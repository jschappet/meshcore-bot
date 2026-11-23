// Durable registration verification outbox. Backend calls run off the radio thread.
use crate::protocol::{Message, Route};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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
    #[serde(default, rename = "channel")]
    _legacy_channel: Option<u8>,
    pub outbox: PathBuf,
    #[serde(default = "default_url")]
    pub url: String,
    #[serde(default = "default_template")]
    pub message_template: String,
}

fn default_url() -> String {
    "https://regenerateskagit.org/api/meshcore/{id}".into()
}

fn default_template() -> String {
    "Skagit confirmation: {passphrase}".into()
}

impl Config {
    pub fn validate(&self) -> io::Result<()> {
        let parsed = reqwest::Url::parse(&self.url.replace("{id}", "registration"))
            .map_err(|_| io::Error::other("Invalid Skagit verification URL"))?;
        if !self.url.contains("{id}")
            || parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(io::Error::other(
                "Skagit URL requires HTTPS and an {id} placeholder without credentials or fragment",
            ));
        }
        let rendered = self
            .message_template
            .replace("{passphrase}", &"x".repeat(60))
            .replace("{id}", &"x".repeat(64));
        if !self.message_template.contains("{passphrase}")
            || rendered.len() > 133
            || rendered.chars().any(char::is_control)
            || rendered.contains(['{', '}'])
        {
            return Err(io::Error::other("Skagit message_template requires {passphrase}, supports {id}, and must fit 133 UTF-8 bytes after expansion"));
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct Job {
    #[serde(default = "default_url")]
    url: String,
    #[serde(default = "default_template")]
    message_template: String,
    id: String,
    payload: Value,
    #[serde(default)]
    sender_prefix: Option<[u8; 6]>,
    passphrase: Option<String>,
    sent: bool,
    attempts: u32,
    retry_at: u64,
}

struct Queue {
    directory: PathBuf,
    _lock: fs::File,
}

#[derive(Clone)]
pub struct Verification(Arc<Mutex<Queue>>);

#[derive(Serialize)]
pub struct Status {
    event_id: String,
    registration_id: Option<String>,
    sender_prefix: Option<String>,
    received_at: Option<String>,
    status: &'static str,
    attempts: u32,
    retry_at: Option<u64>,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
}

impl Queue {
    fn save(&self, job: &Job) -> io::Result<()> {
        let temp = self.directory.join("write.tmp");
        let mut options = fs::OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(&serde_json::to_vec(job)?)?;
        file.sync_all()?;
        fs::rename(temp, self.directory.join(format!("{}.json", job.id)))?;
        #[cfg(unix)]
        fs::File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    fn jobs(&self) -> io::Result<Vec<Job>> {
        let mut jobs = Vec::new();
        for entry in fs::read_dir(&self.directory)? {
            let path = entry?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                let job: Job =
                    serde_json::from_slice(&fs::read(&path)?).map_err(io::Error::other)?;
                if !valid(&job.id) {
                    return Err(io::Error::other("Invalid verification job ID"));
                }
                jobs.push(job);
            }
        }
        Ok(jobs)
    }
}

impl Verification {
    pub fn statuses(&self) -> io::Result<Vec<Status>> {
        let mut rows: Vec<_> = self
            .0
            .lock()
            .unwrap()
            .jobs()?
            .into_iter()
            .map(|job| {
                let status = if job.sender_prefix.is_none() {
                    "Legacy channel request held"
                } else if job.sent {
                    "DM accepted by radio"
                } else if job.passphrase.is_some() {
                    "Waiting to send DM"
                } else if job.attempts > 0 {
                    "Waiting for backend retry"
                } else {
                    "Queued for backend"
                };
                Status {
                    event_id: job.id,
                    registration_id: job.payload["registration_id"].as_str().map(str::to_owned),
                    sender_prefix: job.sender_prefix.map(|key| crate::diagnostics::hex(&key)),
                    received_at: job.payload["received_at"].as_str().map(str::to_owned),
                    status,
                    attempts: job.attempts,
                    retry_at: (!job.sent && job.passphrase.is_none() && job.retry_at > 0)
                        .then_some(job.retry_at),
                }
            })
            .collect();
        rows.sort_by(|a, b| b.received_at.cmp(&a.received_at));
        rows.truncate(500);
        Ok(rows)
    }

    pub fn start(config: &Config) -> io::Result<Self> {
        config.validate()?;
        fs::create_dir_all(&config.outbox)?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(config.outbox.join("queue.lock"))?;
        lock.try_lock()
            .map_err(|_| io::Error::other("Skagit outbox is already in use"))?;
        let queue = Queue {
            directory: config.outbox.clone(),
            _lock: lock,
        };
        queue.jobs()?;
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(io::Error::other)?;
        let result = Self(Arc::new(Mutex::new(queue)));
        let worker = result.clone();
        std::thread::Builder::new()
            .name("skagit-verification".into())
            .spawn(move || loop {
                if let Err(error) = worker.upload_one(&client) {
                    log::error!("Skagit verification queue: {error}");
                }
                std::thread::sleep(Duration::from_secs(1));
            })?;
        Ok(result)
    }

    pub fn enqueue(
        &self,
        config: &Config,
        message: &Message<'_>,
        rssi: Option<i8>,
        path: Option<String>,
    ) -> io::Result<bool> {
        let Route::Direct(prefix) = message.route else {
            return Ok(false);
        };
        let content = message.text.trim();
        let fields: Vec<_> = content.split_whitespace().collect();
        let ["skagit", "verify", id, code] = fields.as_slice() else {
            return Ok(false);
        };
        if !valid(id) || !valid(code) {
            return Ok(false);
        }
        let digest = ring::digest::digest(
            &ring::digest::SHA256,
            format!(
                "{}:{}:{}",
                crate::diagnostics::hex(&prefix),
                u32::from_le_bytes(message.timestamp),
                message.text
            )
            .as_bytes(),
        );
        let event_id = crate::diagnostics::hex(digest.as_ref());
        let queue = self.0.lock().unwrap();
        if queue.directory.join(format!("{event_id}.json")).exists() {
            return Ok(true);
        }
        let job = Job {
            url: config.url.clone(),
            message_template: config.message_template.clone(),
            id: event_id.clone(),
            sender_prefix: Some(prefix),
            sent: false,
            passphrase: None,
            attempts: 0,
            retry_at: 0,
            payload: json!({
                "registration_id":id, "code":code, "event_id":event_id,
                "received_at":chrono::Utc::now().to_rfc3339(),
                "sender_timestamp":u32::from_le_bytes(message.timestamp),
                "sender_key_prefix":crate::diagnostics::hex(&prefix),
                "transport":"dm", "snr_db":message.snr_db,
                "rssi_dbm":rssi, "path":path, "route":crate::route_label(message.path_len)
            }),
        };
        queue.save(&job)?;
        log::info!("Skagit verification saved for background upload");
        Ok(true)
    }

    fn upload_one(&self, client: &Client) -> io::Result<()> {
        let job = self.0.lock().unwrap().jobs()?.into_iter().find(|job| {
            !job.sent
                && job.sender_prefix.is_some()
                && job.passphrase.is_none()
                && job.retry_at <= now()
        });
        let Some(mut job) = job else {
            return Ok(());
        };
        let id = job.payload["registration_id"]
            .as_str()
            .ok_or_else(|| io::Error::other("Missing registration ID"))?;
        if !valid(id) {
            return Err(io::Error::other("Invalid registration ID"));
        }
        let url = job.url.replace("{id}", id);
        let response = client
            .post(url)
            .header("Idempotency-Key", &job.id)
            .json(&job.payload)
            .send();
        match response {
            Ok(response) => {
                let status = response.status();
                log::info!("Skagit verification response HTTP {}", status.as_u16());
                if status.is_success() {
                    let mut bytes = Vec::new();
                    response.take(4097).read_to_end(&mut bytes)?;
                    if bytes.len() <= 4096 {
                        if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                            job.passphrase = value["passphrase"]
                                .as_str()
                                .filter(|value| {
                                    !value.trim().is_empty()
                                        && value.len() <= 60
                                        && !value.chars().any(char::is_control)
                                })
                                .map(str::to_owned);
                        }
                    }
                }
            }
            Err(error) => log::warn!("Skagit verification upload failed: {}", error.without_url()),
        }
        job.attempts = job.attempts.saturating_add(1);
        job.retry_at = now() + (5_u64 * 2_u64.pow(job.attempts.min(6))).min(300);
        self.0.lock().unwrap().save(&job)
    }

    pub fn reply(&self) -> io::Result<Option<(String, [u8; 6], String)>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .jobs()?
            .into_iter()
            .find(|job| !job.sent && job.sender_prefix.is_some() && job.passphrase.is_some())
            .map(|job| {
                (
                    job.id,
                    job.sender_prefix.unwrap(),
                    job.message_template
                        .replace("{passphrase}", job.passphrase.as_deref().unwrap())
                        .replace(
                            "{id}",
                            job.payload["registration_id"].as_str().unwrap_or(""),
                        ),
                )
            }))
    }

    pub fn mark_sent(&self, id: &str) -> io::Result<()> {
        let queue = self.0.lock().unwrap();
        let mut job = queue
            .jobs()?
            .into_iter()
            .find(|job| job.id == id)
            .ok_or_else(|| io::Error::other("Verification job missing"))?;
        job.sent = true;
        queue.save(&job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn queues_offline_deduplicates_and_reloads_ready_replies() {
        let dir = tempfile::tempdir().unwrap();
        let queue = Queue {
            directory: dir.path().into(),
            _lock: fs::File::create(dir.path().join("lock")).unwrap(),
        };
        let service = Verification(Arc::new(Mutex::new(queue)));
        let config = Config {
            _legacy_channel: None,
            outbox: dir.path().into(),
            url: default_url(),
            message_template: "Verified {passphrase}".into(),
        };
        let message = crate::protocol::message(
            b"\x07123456\xff\0\0\0\0\0skagit verify registration1 cedar-123",
        )
        .unwrap();
        assert!(service.enqueue(&config, &message, Some(-90), None).unwrap());
        assert!(service.enqueue(&config, &message, None, None).unwrap());
        let summary = serde_json::to_value(service.statuses().unwrap()).unwrap();
        assert_eq!(summary[0]["status"], "Queued for backend");
        assert!(!summary.to_string().contains("cedar-123"));
        let queue = service.0.lock().unwrap();
        let mut jobs = queue.jobs().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].payload["rssi_dbm"], -90);
        assert_eq!(jobs[0].payload["transport"], "dm");
        assert_eq!(jobs[0].payload["sender_key_prefix"], "313233343536");
        let channel_message = crate::protocol::message(
            b"\x08\x03\0\0\0\0\0\0Alice: skagit verify registration1 cedar-123",
        )
        .unwrap();
        drop(queue);
        assert!(!service
            .enqueue(&config, &channel_message, None, None)
            .unwrap());
        let queue = service.0.lock().unwrap();
        jobs[0].passphrase = Some("river".into());
        queue.save(&jobs[0]).unwrap();
        drop(queue);
        let (id, channel, text) = service.reply().unwrap().unwrap();
        assert_eq!(service.statuses().unwrap()[0].status, "Waiting to send DM");
        assert!(!serde_json::to_string(&service.statuses().unwrap())
            .unwrap()
            .contains("river"));
        assert_eq!(channel, *b"123456");
        assert!(text.ends_with("river"));
        assert_eq!(text, "Verified river");
        service.mark_sent(&id).unwrap();
        assert!(service.reply().unwrap().is_none());
        assert_eq!(
            service.statuses().unwrap()[0].status,
            "DM accepted by radio"
        );
    }
}
