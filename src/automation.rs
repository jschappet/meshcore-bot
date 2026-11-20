use crate::protocol::{Message, Route};
use chrono::{DateTime, Local, NaiveDate};
use log::debug;
use serde::Deserialize;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ADVERT_COOLDOWN: Duration = Duration::from_secs(3600);
const DEFAULT_HEARTBEAT_SECONDS: u32 = 300;

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    admin_pubkeys: Vec<String>,
    tracked_repeaters: Vec<String>,
    advert_interval_seconds: Option<u32>,
    scheduled_messages: Vec<Schedule>,
    heartbeat: Option<HeartbeatConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeartbeatConfig {
    #[serde(default)]
    enabled: bool,
    interval_seconds: Option<u32>,
    recipient_pubkey: Option<String>,
}

#[derive(PartialEq, Eq)]
struct HeartbeatSettings {
    enabled: bool,
    interval: Duration,
    recipient: [u8; 32],
}

impl HeartbeatConfig {
    fn resolve(self, admins: &[[u8; 32]]) -> io::Result<HeartbeatSettings> {
        let seconds = self.interval_seconds.unwrap_or(DEFAULT_HEARTBEAT_SECONDS);
        if seconds == 0 {
            return Err(invalid("heartbeat.interval_seconds must be positive"));
        }
        let recipient = match (self.recipient_pubkey, admins) {
            (Some(text), _) => public_key(&text)
                .filter(|key| admins.contains(key))
                .ok_or_else(|| {
                    invalid("heartbeat.recipient_pubkey must be a full key listed in admin_pubkeys")
                })?,
            (None, [only]) => *only,
            (None, _) => {
                return Err(invalid(
                    "heartbeat requires one admin or an explicit recipient_pubkey",
                ))
            }
        };
        Ok(HeartbeatSettings {
            enabled: self.enabled,
            interval: Duration::from_secs(seconds.into()),
            recipient,
        })
    }
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Schedule {
    channel: u8,
    message: String,
    every_seconds: Option<u32>,
    daily_utc: Option<String>,
}

impl Schedule {
    fn next_after(&self, now: SystemTime) -> SystemTime {
        if let Some(seconds) = self.every_seconds {
            return now + Duration::from_secs(u64::from(seconds));
        }
        let second = daily_second(self.daily_utc.as_deref().unwrap()).unwrap();
        let unix = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let today = unix - unix % 86_400 + second;
        let next = if today > unix { today } else { today + 86_400 };
        UNIX_EPOCH + Duration::from_secs(next)
    }

    fn validate(&self, index: usize) -> io::Result<()> {
        let valid_times = match (self.every_seconds, self.daily_utc.as_deref()) {
            (Some(interval), None) => interval > 0,
            (None, Some(time)) => daily_second(time).is_some(),
            _ => false,
        };
        if !valid_times {
            return Err(invalid(format!(
                "scheduled_messages[{index}] needs either positive every_seconds or daily_utc in HH:MM format"
            )));
        }
        if !valid_post(&self.message) {
            return Err(invalid(format!(
                "scheduled_messages[{index}] needs a nonempty message of at most 100 UTF-8 bytes with no NUL"
            )));
        }
        Ok(())
    }
}

fn daily_second(time: &str) -> Option<u64> {
    let (hour, minute) = time.split_once(':')?;
    if hour.len() != 2
        || minute.len() != 2
        || !hour
            .bytes()
            .chain(minute.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let hour = hour.parse::<u64>().ok()?;
    let minute = minute.parse::<u64>().ok()?;
    (hour < 24 && minute < 60).then_some(hour * 3600 + minute * 60)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn public_key(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut key = [0; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * index..2 * index + 2], 16).ok()?;
    }
    Some(key)
}

pub fn valid_post(text: &str) -> bool {
    !text.trim().is_empty() && text.len() <= 100 && !text.contains('\0')
}

struct Job {
    schedule: Schedule,
    next: SystemTime,
}

pub enum Action {
    Post(u8, String),
    Advert,
    Heartbeat {
        recipient: [u8; 32],
        sequence: u64,
        time: DateTime<Local>,
    },
}

struct Heartbeat {
    settings: HeartbeatSettings,
    next: Option<SystemTime>,
    sequence: u64,
    counter_date: Option<NaiveDate>,
}

impl Heartbeat {
    fn set_interval(&mut self, seconds: u32) {
        let interval = Duration::from_secs(seconds.into());
        if self.settings.interval != interval {
            self.settings.interval = interval;
            self.reschedule(SystemTime::now());
        }
    }

    fn reschedule(&mut self, now: SystemTime) {
        self.next = self.settings.enabled.then(|| now + self.settings.interval);
    }

    fn due(&self, now: SystemTime) -> bool {
        self.next.is_some_and(|next| next <= now)
    }

    fn update_counter_date(&mut self, date: NaiveDate) {
        if self.counter_date != Some(date) {
            self.counter_date = Some(date);
            self.sequence = 0;
        }
    }

    fn next_sequence(&mut self, date: NaiveDate) -> u64 {
        self.update_counter_date(date);
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }
}

#[derive(Default)]
pub struct Automation {
    config_file: Option<PathBuf>,
    keys: Vec<[u8; 32]>,
    pub tracked_repeaters: Vec<[u8; 32]>,
    pub channels_paused: bool,
    jobs: Vec<Job>,
    advert_interval: Option<Duration>,
    next_advert: Option<SystemTime>,
    last_advert: Option<Instant>,
    heartbeat: Option<Heartbeat>,
}

impl Automation {
    pub fn new(config_file: Option<PathBuf>) -> io::Result<Self> {
        let mut automation = Self {
            config_file,
            ..Self::default()
        };
        if automation.config_file.is_some() {
            automation.reload()?;
        }
        Ok(automation)
    }

    pub fn reload(&mut self) -> io::Result<()> {
        let path = self
            .config_file
            .as_deref()
            .ok_or_else(|| invalid("No --config file set"))?;
        let (mut config, keys) = load_config(path)?;
        let tracked_repeaters = config
            .tracked_repeaters
            .iter()
            .map(|text| {
                public_key(text).ok_or_else(|| {
                    invalid("tracked_repeaters requires full 64-character public keys")
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        let heartbeat = config
            .heartbeat
            .take()
            .map(|value| value.resolve(&keys))
            .transpose()?;
        let now = SystemTime::now();
        let mut jobs = Vec::new();
        for schedule in config.scheduled_messages {
            let next = self
                .jobs
                .iter()
                .find(|job| job.schedule == schedule)
                .map_or_else(|| schedule.next_after(now), |job| job.next);
            jobs.push(Job { schedule, next });
        }
        let interval = config
            .advert_interval_seconds
            .map(|value| Duration::from_secs(u64::from(value)));
        if self.advert_interval != interval {
            self.next_advert = interval.map(|duration| now + duration);
        }
        self.advert_interval = interval;
        self.jobs = jobs;
        self.keys = keys;
        self.tracked_repeaters = tracked_repeaters;
        self.heartbeat = match (self.heartbeat.take(), heartbeat) {
            (Some(current), Some(settings)) if current.settings == settings => Some(current),
            (current, Some(settings)) => {
                let same_recipient =
                    current.filter(|old| old.settings.recipient == settings.recipient);
                let mut heartbeat = Heartbeat {
                    sequence: same_recipient.as_ref().map_or(0, |old| old.sequence),
                    counter_date: same_recipient.and_then(|old| old.counter_date),
                    settings,
                    next: None,
                };
                heartbeat.reschedule(now);
                Some(heartbeat)
            }
            (_, None) => None,
        };
        debug!(
            "Loaded automation config: {} admin keys, {} scheduled posts, advert interval {:?}",
            self.keys.len(),
            self.jobs.len(),
            self.advert_interval,
        );
        Ok(())
    }

    pub fn admin_candidate(&self, message: &Message<'_>) -> Option<[u8; 6]> {
        let Route::Direct(prefix) = message.route else {
            return None;
        };
        (message.text_type == 0 && self.keys.iter().any(|key| key.starts_with(&prefix)))
            .then_some(prefix)
    }

    pub fn authorized(&self, prefix: [u8; 6], contacts: &[[u8; 32]]) -> bool {
        let mut matches = contacts.iter().filter(|key| key.starts_with(&prefix));
        matches.next().is_some_and(|key| self.keys.contains(key)) && matches.next().is_none()
    }

    pub fn reserve_advert(&mut self) -> Result<(), Duration> {
        let remaining = self.last_advert.map_or(Duration::ZERO, |last| {
            ADVERT_COOLDOWN.saturating_sub(last.elapsed())
        });
        if !remaining.is_zero() {
            return Err(remaining);
        }
        // Reserve before writing. A lost response leaves the transmit outcome unknown.
        self.last_advert = Some(Instant::now());
        self.next_advert = self
            .advert_interval
            .map(|duration| SystemTime::now() + duration);
        Ok(())
    }

    pub fn due(&mut self, now: SystemTime) -> Option<Action> {
        if let Some(heartbeat) = self
            .heartbeat
            .as_mut()
            .filter(|heartbeat| heartbeat.due(now))
        {
            heartbeat.reschedule(now);
            let time = DateTime::<Local>::from(now);
            return Some(Action::Heartbeat {
                recipient: heartbeat.settings.recipient,
                sequence: heartbeat.next_sequence(time.date_naive()),
                time,
            });
        }
        if self.next_advert.is_some_and(|next| next <= now) {
            match self.reserve_advert() {
                Ok(()) => return Some(Action::Advert),
                Err(remaining) => self.next_advert = Some(now + remaining),
            }
        }
        if let Some(job) = self
            .jobs
            .iter_mut()
            .filter(|job| job.next <= now)
            .min_by_key(|job| job.next)
        {
            job.next = job.schedule.next_after(now);
            return Some(Action::Post(
                job.schedule.channel,
                job.schedule.message.clone(),
            ));
        }
        None
    }

    pub fn has_due_work(&self, now: SystemTime) -> bool {
        self.next_advert.is_some_and(|next| next <= now)
            || self
                .heartbeat
                .as_ref()
                .is_some_and(|heartbeat| heartbeat.due(now))
            || self.jobs.iter().any(|job| job.next <= now)
    }

    pub fn skip_missed(&mut self, now: SystemTime) {
        if let Some(heartbeat) = self
            .heartbeat
            .as_mut()
            .filter(|heartbeat| heartbeat.due(now))
        {
            heartbeat.reschedule(now);
        }
        for job in self.jobs.iter_mut().filter(|job| job.next <= now) {
            job.next = job.schedule.next_after(now);
        }
        if self.next_advert.is_some_and(|next| next <= now) {
            self.next_advert = self.advert_interval.map(|duration| now + duration);
        }
    }

    pub fn heartbeat_command(&mut self, command: &str) -> String {
        let Some(heartbeat) = &mut self.heartbeat else {
            return "Heartbeat not configured | add heartbeat to bot.json and reload".into();
        };
        heartbeat.update_counter_date(Local::now().date_naive());
        match command.to_ascii_lowercase().as_str() {
            "" | "status" => {}
            "reset" => {
                heartbeat.sequence = 0;
                return "Heartbeat counter reset | next #1".into();
            }
            "on" | "enable" if !heartbeat.settings.enabled => {
                heartbeat.settings.enabled = true;
                heartbeat.reschedule(SystemTime::now());
            }
            "off" | "disable" => {
                heartbeat.settings.enabled = false;
                heartbeat.next = None;
            }
            "on" | "enable" => {}
            "fast" => heartbeat.set_interval(60),
            "slow" => heartbeat.set_interval(3600),
            "default" => heartbeat.set_interval(DEFAULT_HEARTBEAT_SECONDS),
            _ => return "Usage: hb [status|on|off|reset|fast|slow|default]".into(),
        }
        let status = if heartbeat.settings.enabled {
            "on"
        } else {
            "off"
        };
        format!(
            "Heartbeat {status} | DM every {}s | count {}",
            heartbeat.settings.interval.as_secs(),
            heartbeat.sequence,
        )
    }

    pub fn summary(&self) -> String {
        let advert = self.advert_interval.map_or_else(
            || "off".into(),
            |interval| format!("{}s", interval.as_secs()),
        );
        let mut reply = format!("Schedule | {} posts | advert {advert}", self.jobs.len());
        for job in &self.jobs {
            let when = match job.schedule.every_seconds {
                Some(seconds) => format!("every {seconds}s"),
                None => format!("{} UTC", job.schedule.daily_utc.as_deref().unwrap()),
            };
            let item = format!(" | #{} {when}", job.schedule.channel);
            if reply.len() + item.len() > 96 {
                reply.push_str(" …");
                break;
            }
            reply.push_str(&item);
        }
        reply
    }
}

fn load_config(path: &Path) -> io::Result<(Config, Vec<[u8; 32]>)> {
    let input = std::fs::read_to_string(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("Could not read {}: {error}", path.display()),
        )
    })?;
    let config: Config = serde_json::from_str(&input).map_err(|error| {
        invalid(format!(
            "Invalid config at line {}, column {}: {error}",
            error.line(),
            error.column()
        ))
    })?;
    let keys = config
        .admin_pubkeys
        .iter()
        .map(|text| {
            public_key(text).ok_or_else(|| {
                invalid("Each admin_pubkeys entry must contain 64 hexadecimal characters")
            })
        })
        .collect::<io::Result<_>>()?;
    if config
        .advert_interval_seconds
        .is_some_and(|seconds| u64::from(seconds) < ADVERT_COOLDOWN.as_secs())
    {
        return Err(invalid("advert_interval_seconds must be at least 3600"));
    }
    for (index, job) in config.scheduled_messages.iter().enumerate() {
        job.validate(index)?;
        if config.scheduled_messages[..index].contains(job) {
            return Err(invalid(format!(
                "scheduled_messages[{index}] duplicates an earlier job"
            )));
        }
    }
    Ok((config, keys))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_uses_sole_admin_and_honors_toggle_and_reconnect() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let key = [0x23; 32];
        let config = serde_json::json!({
            "admin_pubkeys": [crate::diagnostics::hex(&key)],
            "heartbeat": {"enabled": false, "interval_seconds": 300}
        });
        std::fs::write(file.path(), config.to_string()).unwrap();
        let mut automation = Automation::new(Some(file.path().into())).unwrap();
        assert_eq!(
            automation.heartbeat_command("status"),
            "Heartbeat off | DM every 300s | count 0"
        );
        assert_eq!(
            automation.heartbeat_command("on"),
            "Heartbeat on | DM every 300s | count 0"
        );
        let due = automation.heartbeat.as_ref().unwrap().next.unwrap();
        assert!(automation.due(due - Duration::from_secs(1)).is_none());
        assert!(
            matches!(automation.due(due), Some(Action::Heartbeat {recipient, sequence: 1, time}) if recipient == key && SystemTime::from(time) == due)
        );
        let next = automation.heartbeat.as_ref().unwrap().next;
        assert_eq!(
            automation.heartbeat_command("reset"),
            "Heartbeat counter reset | next #1"
        );
        assert_eq!(automation.heartbeat.as_ref().unwrap().next, next);
        let reconnected = due + Duration::from_secs(900);
        automation.skip_missed(reconnected);
        assert!(automation.due(reconnected).is_none());
        assert!(matches!(
            automation.due(reconnected + Duration::from_secs(300)),
            Some(Action::Heartbeat { sequence: 1, .. })
        ));
        automation.reload().unwrap();
        assert!(automation
            .heartbeat_command("status")
            .starts_with("Heartbeat off | DM every 300s"));
        assert!(!automation.has_due_work(reconnected + Duration::from_secs(3600)));
    }

    #[test]
    fn heartbeat_starts_at_one_on_each_local_date() {
        let day = NaiveDate::from_ymd_opt(2026, 11, 15).unwrap();
        let mut heartbeat = Heartbeat {
            settings: HeartbeatSettings {
                enabled: true,
                interval: Duration::from_secs(300),
                recipient: [1; 32],
            },
            next: None,
            sequence: 0,
            counter_date: None,
        };
        assert_eq!(heartbeat.next_sequence(day), 1);
        assert_eq!(heartbeat.next_sequence(day), 2);
        let tomorrow = day.succ_opt().unwrap();
        heartbeat.update_counter_date(tomorrow);
        assert_eq!(heartbeat.sequence, 0);
        assert_eq!(heartbeat.next_sequence(tomorrow), 1);
    }

    #[test]
    fn heartbeat_rejects_an_ambiguous_default_or_a_non_admin_recipient() {
        let resolve = |recipient_pubkey| {
            HeartbeatConfig {
                enabled: true,
                interval_seconds: Some(300),
                recipient_pubkey,
            }
            .resolve(&[[1; 32], [2; 32]])
        };
        assert!(resolve(None).is_err());
        assert!(resolve(Some("03".repeat(32))).is_err());
        assert_eq!(resolve(Some("02".repeat(32))).unwrap().recipient, [2; 32]);
    }

    #[test]
    fn heartbeat_presets_preserve_the_counter_and_enabled_state() {
        let settings = HeartbeatConfig {
            enabled: false,
            interval_seconds: None,
            recipient_pubkey: None,
        }
        .resolve(&[[1; 32]])
        .unwrap();
        let mut automation = Automation {
            heartbeat: Some(Heartbeat {
                settings,
                next: None,
                sequence: 4,
                counter_date: Some(Local::now().date_naive()),
            }),
            ..Automation::default()
        };
        assert_eq!(
            automation.heartbeat_command("status"),
            "Heartbeat off | DM every 300s | count 4"
        );
        assert_eq!(
            automation.heartbeat_command("fast"),
            "Heartbeat off | DM every 60s | count 4"
        );
        assert_eq!(automation.heartbeat.as_ref().unwrap().next, None);
        automation.heartbeat_command("on");
        assert_eq!(
            automation.heartbeat_command("slow"),
            "Heartbeat on | DM every 3600s | count 4"
        );
        let next = automation.heartbeat.as_ref().unwrap().next.unwrap();
        assert!(!automation.has_due_work(next - Duration::from_secs(1)));
        assert!(automation.has_due_work(next));
        automation.heartbeat_command("slow");
        assert_eq!(automation.heartbeat.as_ref().unwrap().next, Some(next));
        assert_eq!(
            automation.heartbeat_command("default"),
            "Heartbeat on | DM every 300s | count 4"
        );
    }

    #[test]
    fn reload_preserves_pending_jobs_and_rejects_invalid_replacements() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bot.json");
        let config =
            r#"{"scheduled_messages":[{"channel":3,"every_seconds":30,"message":"Net soon"}]}"#;
        std::fs::write(&path, config).unwrap();
        let mut automation = Automation::new(Some(path.clone())).unwrap();
        let next = automation.jobs[0].next;
        automation.reload().unwrap();
        assert_eq!(automation.jobs[0].next, next);

        std::fs::write(&path, config.replace(":30", ":0")).unwrap();
        assert!(automation.reload().is_err());
        assert_eq!(automation.jobs[0].next, next);
        assert!(matches!(automation.due(next), Some(Action::Post(3, text)) if text == "Net soon"));
        assert!(automation.due(next).is_none());

        let later = next + Duration::from_secs(300);
        automation.skip_missed(later);
        assert!(automation.due(later).is_none());
        assert!(automation.due(later + Duration::from_secs(30)).is_some());
    }

    #[test]
    fn daily_utc_and_flood_advert_use_their_configured_intervals() {
        let schedule = Schedule {
            channel: 0,
            message: "Net check".into(),
            every_seconds: None,
            daily_utc: Some("00:01".into()),
        };
        let due = UNIX_EPOCH + Duration::from_secs(60);
        assert_eq!(schedule.next_after(UNIX_EPOCH), due);
        assert_eq!(schedule.next_after(due), due + Duration::from_secs(86_400));
        assert!(daily_second("24:00").is_none());

        let mut automation = Automation {
            next_advert: Some(UNIX_EPOCH),
            advert_interval: Some(Duration::from_secs(3600)),
            ..Automation::default()
        };
        assert!(matches!(automation.due(UNIX_EPOCH), Some(Action::Advert)));
        assert!(automation.reserve_advert().is_err());
    }
}
