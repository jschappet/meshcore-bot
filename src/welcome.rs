// Welcome requests are durable before HTTP submission or radio transmission.
use crate::protocol::{Message, Route};
use reqwest::{blocking::Client, redirect::Policy};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{self, Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub enabled: bool,
    pub channel: u8,
    pub experiment_id: String,
    pub outbox: PathBuf,
    #[serde(default = "template")]
    pub message_template: String,
}

fn template() -> String {
    "Welcome to RegenerateSkagit! {invite_url}".into()
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl Config {
    pub fn validate(&self) -> io::Result<()> {
        let id = self.experiment_id.as_bytes();
        if id.len() != 36
            || !id.iter().enumerate().all(|(i, b)| {
                if [8, 13, 18, 23].contains(&i) {
                    *b == b'-'
                } else {
                    b.is_ascii_hexdigit()
                }
            })
        {
            return Err(io::Error::other(
                "skagit_welcome.experiment_id must be the experiment document UUID",
            ));
        }
        if !self.message_template.contains("{invite_url}")
            || self.message_template.chars().any(char::is_control)
        {
            return Err(io::Error::other(
                "Welcome message_template requires {invite_url} and no control characters",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct Job {
    event_id: String,
    experiment_id: String,
    sender: String,
    channel: u8,
    message_template: String,
    invite_url: Option<String>,
    sent: bool,
    rejected: bool,
    retry_at: u64,
    attempts: u32,
}

struct Queue {
    dir: PathBuf,
    _lock: fs::File,
}

impl Queue {
    fn jobs(&self) -> io::Result<Vec<Job>> {
        let mut jobs = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let job: Job =
                    serde_json::from_slice(&fs::read(path)?).map_err(io::Error::other)?;
                if job.event_id.len() != 64 || !job.event_id.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return Err(io::Error::other("Invalid welcome event ID in outbox"));
                }
                jobs.push(job);
            }
        }
        Ok(jobs)
    }

    fn save(&self, job: &Job) -> io::Result<()> {
        let mut options = fs::OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let temporary = self.dir.join("write.tmp");
        let mut file = options.open(&temporary)?;
        file.write_all(&serde_json::to_vec(job)?)?;
        file.sync_all()?;
        fs::rename(temporary, self.dir.join(format!("{}.json", job.event_id)))?;
        #[cfg(unix)]
        fs::File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Welcome(Arc<Mutex<Queue>>);

impl Welcome {
    pub fn start(config: &Config) -> io::Result<Self> {
        config.validate()?;
        let token = std::env::var("MESHCORE_INVITE_BOT_TOKEN")
            .ok()
            .filter(|token| !token.trim().is_empty())
            .ok_or_else(|| {
                io::Error::other("Enabled welcome invites require MESHCORE_INVITE_BOT_TOKEN")
            })?;
        fs::create_dir_all(&config.outbox)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&config.outbox, fs::Permissions::from_mode(0o700))?;
        }
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(config.outbox.join("queue.lock"))?;
        lock.try_lock()
            .map_err(|_| io::Error::other("Welcome outbox already in use"))?;
        let queue = Queue {
            dir: config.outbox.clone(),
            _lock: lock,
        };
        queue.jobs()?;
        let service = Self(Arc::new(Mutex::new(queue)));
        let worker = service.clone();
        let client = Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(io::Error::other)?;
        std::thread::Builder::new()
            .name("skagit-invites".into())
            .spawn(move || loop {
                if let Err(error) = worker.upload(&client, &token) {
                    log::error!("Welcome outbox: {error}");
                }
                std::thread::sleep(Duration::from_secs(1));
            })?;
        Ok(service)
    }

    pub fn observe(&self, config: &Config, message: &Message<'_>) -> io::Result<()> {
        if message.route != Route::Channel(config.channel) {
            return Ok(());
        }
        let Some((name, _)) = message.text.split_once(':') else {
            return Ok(());
        };
        let name = name.trim();
        if name.is_empty() {
            return Ok(());
        }
        // Channel text supplies a display name, so this identity is provisional.
        let identity = format!(
            "{}:{}:{}",
            config.experiment_id.to_ascii_lowercase(),
            config.channel,
            name
        );
        let id = crate::diagnostics::hex(
            ring::digest::digest(&ring::digest::SHA256, identity.as_bytes()).as_ref(),
        );
        let queue = self.0.lock().unwrap();
        if queue.dir.join(format!("{id}.json")).exists() {
            return Ok(());
        }
        queue.save(&Job {
            event_id: id,
            experiment_id: config.experiment_id.clone(),
            sender: name.into(),
            channel: config.channel,
            message_template: config.message_template.clone(),
            invite_url: None,
            sent: false,
            rejected: false,
            retry_at: 0,
            attempts: 0,
        })?;
        log::info!("Queued a new RegenerateSkagit welcome");
        Ok(())
    }

    fn upload(&self, client: &Client, token: &str) -> io::Result<()> {
        let job = self.0.lock().unwrap().jobs()?.into_iter().find(|job| {
            !job.sent && !job.rejected && job.invite_url.is_none() && job.retry_at <= now()
        });
        let Some(mut job) = job else {
            return Ok(());
        };
        let payload =
            serde_json::json!({"experiment_id":job.experiment_id,"event_id":job.event_id});
        match client
            .post("https://regenerateskagit.org/api/meshcore/invites")
            .bearer_auth(token)
            .header("Idempotency-Key", &job.event_id)
            .json(&payload)
            .send()
        {
            Ok(response) => {
                let status = response.status().as_u16();
                log::info!("Welcome invite response HTTP {status}");
                let mut bytes = Vec::new();
                if response.take(4097).read_to_end(&mut bytes).is_ok() {
                    apply_response(&mut job, status, &bytes);
                }
            }
            // A transport error can contain a URL. Log only the failure category.
            Err(_) => log::warn!("Welcome invite request failed; retrying in background"),
        }
        job.attempts = job.attempts.saturating_add(1);
        job.retry_at = now() + (5_u64 * 2_u64.pow(job.attempts.min(6))).min(300);
        self.0.lock().unwrap().save(&job)
    }

    pub fn reply(&self) -> io::Result<Option<(String, u8, String)>> {
        for job in self.0.lock().unwrap().jobs()? {
            if !job.sent && !job.rejected {
                if let Some(url) = job.invite_url {
                    if let Some(text) = render(&job.message_template, &url) {
                        return Ok(Some((job.event_id, job.channel, text)));
                    }
                }
            }
        }
        Ok(None)
    }

    pub fn mark_sent(&self, id: &str) -> io::Result<()> {
        let queue = self.0.lock().unwrap();
        let mut job = queue
            .jobs()?
            .into_iter()
            .find(|job| job.event_id == id)
            .ok_or_else(|| io::Error::other("Welcome job missing"))?;
        job.sent = true;
        queue.save(&job)
    }
}

fn apply_response(job: &mut Job, status: u16, bytes: &[u8]) {
    if matches!(status, 200 | 201) && bytes.len() <= 4096 {
        if let Ok(body) = serde_json::from_slice::<serde_json::Value>(bytes) {
            if let Some(url) = body["invite_url"].as_str() {
                if valid_invite(url) && render(&job.message_template, url).is_some() {
                    job.invite_url = Some(url.into());
                } else {
                    job.rejected = true;
                }
            }
        }
    } else if matches!(status, 400 | 404 | 409 | 422) {
        job.rejected = true;
        log::warn!("Welcome rejected by Heron; check the configured experiment");
    }
}

fn valid_invite(text: &str) -> bool {
    reqwest::Url::parse(text).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str() == Some("regenerateskagit.org")
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none()
    })
}
fn render(template: &str, url: &str) -> Option<String> {
    let text = template.replace("{invite_url}", url);
    (text.len() <= 100 && !text.chars().any(char::is_control)).then_some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queues_once_and_waits_for_saved_backend_response_before_reply() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            enabled: true,
            channel: 3,
            experiment_id: "12345678-1234-1234-1234-123456789abc".into(),
            outbox: dir.path().into(),
            message_template: template(),
        };
        config.validate().unwrap();
        let service = Welcome(Arc::new(Mutex::new(Queue {
            dir: dir.path().into(),
            _lock: fs::File::create(dir.path().join("lock")).unwrap(),
        })));
        let message = crate::protocol::message(b"\x08\x03\0\0\0\0\0\0Alice: hello").unwrap();
        service.observe(&config, &message).unwrap();
        service.observe(&config, &message).unwrap();
        assert!(service.reply().unwrap().is_none());
        let queue = service.0.lock().unwrap();
        let mut jobs = queue.jobs().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].experiment_id, config.experiment_id);
        let event = jobs[0].event_id.clone();
        apply_response(&mut jobs[0], 503, b"unavailable");
        assert!(jobs[0].invite_url.is_none());
        assert_eq!(jobs[0].event_id, event);
        apply_response(
            &mut jobs[0],
            201,
            br#"{"invite_url":"https://regenerateskagit.org/api/meshcore/token123"}"#,
        );
        queue.save(&jobs[0]).unwrap();
        drop(queue);
        let (id, channel, text) = service.reply().unwrap().unwrap();
        assert_eq!(id, event);
        assert_eq!(channel, 3);
        assert!(text.contains("/token123"));
        service.mark_sent(&id).unwrap();
        service.observe(&config, &message).unwrap();
        assert!(service.reply().unwrap().is_none());
    }

    #[test]
    fn rejects_missing_experiments_and_unknown_backend_experiments() {
        let config = Config {
            enabled: true,
            channel: 3,
            experiment_id: String::new(),
            outbox: PathBuf::from("unused"),
            message_template: template(),
        };
        assert!(config.validate().is_err());
        let mut job = Job {
            event_id: "A".repeat(64),
            experiment_id: "unused".into(),
            sender: "Alice".into(),
            channel: 3,
            message_template: template(),
            invite_url: None,
            sent: false,
            rejected: false,
            retry_at: 0,
            attempts: 0,
        };
        apply_response(&mut job, 404, b"unknown experiment");
        assert!(job.rejected);
        assert!(job.invite_url.is_none());
        assert!(!valid_invite("https://evil.example/invite"));
    }
}
