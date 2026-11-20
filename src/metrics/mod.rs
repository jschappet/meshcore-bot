//! Workshop IoT metrics. Enrollment and outbox adapted from flood_monitor (CC0 1.0).
mod store;
mod upload;

use crate::protocol::{self, Route};
use reqwest::{blocking::Client, redirect::Policy, Url};
use ring::{
    rand::{SecureRandom, SystemRandom},
    signature,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use store::{SavedReport, Store};

const WINDOW_MS: u64 = 300_000;
const MAX_PENDING: usize = 2048;
const MAX_BODY: usize = 16 * 1024;
const PROVISIONAL_DELAY: u64 = 10800;
const MAX_NEIGHBORS: usize = 48;

fn hex(bytes: &[u8]) -> String {
    crate::diagnostics::hex(bytes).to_ascii_lowercase()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub url: String,
    pub device_id: String,
    pub name: String,
    pub ip_address: IpAddr,
    pub port: u16,
    #[serde(default)]
    pub public_channel: u8,
    pub spool_dir: PathBuf,
    #[serde(default)]
    pub root_ca_file: Option<PathBuf>,
}
impl Config {
    pub fn load(path: &Path) -> io::Result<Self> {
        let mut config: Self =
            serde_json::from_reader(File::open(path)?).map_err(io::Error::other)?;
        let url = Url::parse(&config.url).map_err(|_| io::Error::other("invalid metrics URL"))?;
        let local = url.host_str().is_some_and(|h| {
            h == "localhost"
                || h.ends_with(".localhost")
                || h.trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(url.scheme() == "https" || url.scheme() == "http" && local)
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.query().is_some()
        {
            return Err(io::Error::other("metrics require HTTPS (HTTP allowed only for localhost development), without URL credentials, query or fragment"));
        }
        if !valid_id(&config.device_id, 80) || !valid_id(&config.name, 160) || config.port == 0 {
            return Err(io::Error::other("invalid metrics device identity or port"));
        }
        let base = path.parent().unwrap_or(Path::new("."));
        if config.spool_dir.is_relative() {
            config.spool_dir = base.join(&config.spool_dir);
        }
        if let Some(ca) = &mut config.root_ca_file {
            if ca.is_relative() {
                *ca = base.join(&*ca);
            }
        }
        Ok(config)
    }
    fn client(&self) -> io::Result<Client> {
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(Policy::none());
        if let Some(path) = &self.root_ca_file {
            let ca = reqwest::Certificate::from_pem(&fs::read(path)?)
                .map_err(|_| io::Error::other("invalid root CA PEM"))?;
            builder = builder.add_root_certificate(ca);
        }
        builder
            .build()
            .map_err(|_| io::Error::other("cannot initialize HTTPS client"))
    }
}
fn valid_id(s: &str, max: usize) -> bool {
    !s.trim().is_empty() && s.len() <= max && !s.chars().any(char::is_control)
}
fn now_unix() -> u64 {
    // A plausible system clock is estimated, not claimed to be NTP-synchronized.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|t| t.as_secs())
        .ok()
        .filter(|t| *t >= 1_577_836_800)
        .unwrap_or(0)
}
fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temp = path.with_extension("tmp");
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&temp)?;
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(temp, path)?;
    #[cfg(unix)]
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}

fn validate_payload(payload: &Value) -> io::Result<()> {
    fn depth(v: &Value) -> usize {
        match v {
            Value::Object(m) => m.values().map(|v| 1 + depth(v)).max().unwrap_or(0),
            Value::Array(a) => a.iter().map(|v| 1 + depth(v)).max().unwrap_or(0),
            _ => 0,
        }
    }
    let r = &payload["reading"];
    let valid_text = |key, limit| r[key].as_str().is_some_and(|s| valid_id(s, limit));
    let schema = r["schema"].as_str().unwrap_or("");
    let start = r["window_start_ms"].as_u64();
    let end = r["window_end_ms"].as_u64();
    let duration = r["window_duration_ms"].as_u64();
    let window = match (start, end, duration) {
        (Some(a), Some(b), Some(d))
            if a <= u32::MAX as u64 && b <= u32::MAX as u64 && d > 0 && d <= u32::MAX as u64 =>
        {
            (b as u32).wrapping_sub(a as u32) == d as u32
        }
        _ => false,
    };
    let time = match r["time_quality"].as_str() {
        Some("unknown") => r["observed_at_unix"].as_i64() == Some(0),
        None | Some("estimated" | "synchronized") => {
            r["observed_at_unix"].as_i64().is_some_and(|t| t > 0)
        }
        _ => false,
    };
    if !payload["device_id"]
        .as_str()
        .is_some_and(|s| valid_id(s, 80))
        || schema.is_empty()
        || schema.len() > 80
        || !schema
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b))
        || !r["schema_version"]
            .as_u64()
            .is_some_and(|v| (1..=i32::MAX as u64).contains(&v))
        || !valid_text("report_id", 160)
        || !valid_text("boot_id", 160)
        || !window
        || !time
        || !r["data"].is_object()
        || serde_json::to_vec(&r["data"])?.len() > 12 * 1024
        || depth(&r["data"]) > 8
        || serde_json::to_vec(payload)?.len() > MAX_BODY
    {
        return Err(io::Error::other(
            "metric exceeds Heron envelope/data limits or has invalid timing/identity",
        ));
    }
    Ok(())
}

#[derive(Serialize)]
struct Neighbor {
    public_key: String,
    name: Option<String>,
    snr_db: f32,
    rssi_dbm: i8,
}

impl Neighbor {
    fn from_raw(frame: &[u8]) -> Option<Self> {
        if frame.first() != Some(&0x88) {
            return None;
        }
        let raw = frame.get(3..)?;
        let header = *raw.first()?;
        if (header >> 2) & 0x0f != 4 {
            return None;
        }
        let length_offset = if matches!(header & 3, 0 | 3) { 5 } else { 1 };
        let path_length = *raw.get(length_offset)?;
        if path_length & 0x3f != 0 || path_length >> 6 == 3 {
            return None;
        }
        let advert = raw.get(length_offset + 1..)?;
        let flags = *advert.get(100)?;
        if flags & 0x0f != 2 {
            return None;
        }
        let key = &advert[..32];
        let mut signed = advert[..36].to_vec();
        signed.extend_from_slice(&advert[100..]);
        signature::UnparsedPublicKey::new(&signature::ED25519, key)
            .verify(&signed, &advert[36..100])
            .ok()?;
        let name_offset = 101
            + if flags & 0x10 != 0 { 8 } else { 0 }
            + if flags & 0x20 != 0 { 2 } else { 0 }
            + if flags & 0x40 != 0 { 2 } else { 0 };
        let fields = advert.get(name_offset..)?;
        let name = if flags & 0x80 != 0 {
            fields
                .split(|byte| *byte == 0)
                .next()
                .and_then(|text| std::str::from_utf8(text).ok())
                .filter(|text| !text.is_empty() && !text.chars().any(char::is_control))
                .map(str::to_owned)
        } else {
            None
        };
        Some(Self {
            public_key: hex(key),
            name,
            snr_db: f32::from(frame[1] as i8) / 4.0,
            rssi_dbm: frame[2] as i8,
        })
    }
}

#[derive(Default)]
struct Window {
    start_ms: u64,
    listening_ms: u64,
    dm_count: u32,
    channel_counts: BTreeMap<u8, u32>,
    neighbors: BTreeMap<String, Neighbor>,
    neighbors_truncated: bool,
}

impl Window {
    fn count(&mut self, packet: &[u8]) {
        if let Some(message) = protocol::message(packet) {
            let count = match message.route {
                Route::Direct(_) => &mut self.dm_count,
                Route::Channel(channel) => self.channel_counts.entry(channel).or_default(),
            };
            *count = count.saturating_add(1);
        } else if let Some(neighbor) = Neighbor::from_raw(packet) {
            if self.neighbors.len() < MAX_NEIGHBORS
                || self.neighbors.contains_key(&neighbor.public_key)
            {
                self.neighbors.insert(neighbor.public_key.clone(), neighbor);
            } else {
                self.neighbors_truncated = true;
            }
        }
    }

    fn report(&self, config: &Config, elapsed: u64, unix: u64, boot: &str) -> SavedReport {
        let end = self.start_ms + WINDOW_MS;
        let observed_at = unix.saturating_sub(elapsed.saturating_sub(end) / 1000);
        let quality = if observed_at > 0 {
            "estimated"
        } else {
            "unknown"
        };
        let neighbors: Vec<_> = self.neighbors.values().collect();
        let payload = json!({
            "device_id": config.device_id,
            "name": config.name,
            "ip_address": config.ip_address.to_string(),
            "port": config.port,
            "deployed_code": {"name": "meshcore-bot-rust", "version": env!("CARGO_PKG_VERSION")},
            "capabilities": ["meshcore_message_count"],
            "reading": {
                "schema": "meshcore_message_count", "schema_version": 2,
                "report_id": format!("{boot}-{}", end / WINDOW_MS), "boot_id": boot,
                "observed_at_unix": observed_at, "time_quality": quality,
                "window_start_ms": self.start_ms as u32, "window_end_ms": end as u32,
                "window_duration_ms": WINDOW_MS,
                "data": {
                    "public_channel": config.public_channel,
                    "public_count": self.channel_counts.get(&config.public_channel).copied().unwrap_or_default(),
                    "dm_count": self.dm_count, "channel_counts": self.channel_counts,
                    "listening_ms": self.listening_ms,
                    "neighbor_count": neighbors.len(), "neighbors": neighbors,
                    "neighbors_truncated": self.neighbors_truncated
                }
            }
        });
        SavedReport {
            payload,
            elapsed_ms: end,
            frozen: false,
        }
    }
}

pub struct Reporter {
    config: Config,
    started: Instant,
    boot_id: String,
    listening: bool,
    last_elapsed: u64,
    window: Window,
    store: Arc<Mutex<Store>>,
}

impl Reporter {
    pub fn start(config: Config, retry_now: bool) -> io::Result<Self> {
        let mut store = Store::open(&config)?;
        let token = store.token()?;
        let client = config.client()?;
        store.save(store.state.clone())?;
        if retry_now {
            if store.retry_provisional_now()? {
                log::info!("Workshop IoT provisional retry delay cleared after operator approval");
            } else {
                log::info!("Workshop IoT retry-now requested; no provisional delay to clear");
            }
        }
        log::info!(
            "Workshop IoT target={} device={} queue_depth={} last_http={:?} enrollment={} retry_in_seconds={}",
            config.url,
            config.device_id,
            store.state.reports.len(),
            store.state.last_http_status,
            store.state.enrollment_status.as_deref().unwrap_or("unknown"),
            store.retry_at.saturating_duration_since(Instant::now()).as_secs(),
        );
        let mut random = [0; 16];
        SystemRandom::new()
            .fill(&mut random)
            .map_err(|_| io::Error::other("secure random generation failed"))?;
        let boot_id = format!("meshcore-{}", hex(&random));
        let started = Instant::now();
        let store = Arc::new(Mutex::new(store));
        let (upload_store, upload_url, upload_boot) =
            (store.clone(), config.url.clone(), boot_id.clone());
        thread::Builder::new()
            .name("metrics-upload".into())
            .spawn(move || {
                upload::run(
                    client,
                    upload_store,
                    upload_url,
                    token,
                    upload_boot,
                    started,
                )
            })?;
        log::info!(
            "Workshop IoT metrics enabled for {} in five-minute windows",
            config.device_id
        );
        Ok(Self {
            config,
            started,
            boot_id,
            listening: false,
            last_elapsed: 0,
            window: Window::default(),
            store,
        })
    }

    pub fn tick(&mut self, packet: Option<&[u8]>) {
        let elapsed = self.started.elapsed().as_millis() as u64;
        self.tick_at(elapsed, packet);
    }

    fn tick_at(&mut self, elapsed: u64, packet: Option<&[u8]>) {
        while self.window.start_ms + WINDOW_MS <= elapsed {
            let end = self.window.start_ms + WINDOW_MS;
            self.account(end);
            if self.window.listening_ms > 0
                || self.window.dm_count > 0
                || !self.window.channel_counts.is_empty()
                || !self.window.neighbors.is_empty()
            {
                let report = self
                    .window
                    .report(&self.config, elapsed, now_unix(), &self.boot_id);
                let mut store = self.store.lock().unwrap();
                if let Err(error) = store.append(report) {
                    store.missed_windows = store.missed_windows.saturating_add(1);
                    log::error!("Metrics window could not be saved: {error}");
                } else {
                    log::debug!(
                        "Workshop IoT queued window: listening_ms={} dms={} active_channels={} neighbors={}",
                        self.window.listening_ms,
                        self.window.dm_count,
                        self.window.channel_counts.len(),
                        self.window.neighbors.len(),
                    );
                    log::debug!("Metrics outbox: {}", store.diagnostics());
                }
            } else {
                log::debug!("Workshop IoT skipped window without radio listening time");
            }
            self.window = Window {
                start_ms: end,
                ..Window::default()
            };
            if !self.listening {
                self.window.start_ms = elapsed - elapsed % WINDOW_MS;
                break;
            }
        }
        self.account(elapsed);
        if let Some(packet) = packet {
            if self.listening {
                self.window.count(packet);
            } else if packet.first() == Some(&protocol::QUEUE_EMPTY) {
                // Exclude messages queued while the bot was offline.
                self.listening = true;
                log::debug!("Workshop IoT radio backlog drained; collecting live metrics");
            }
        }
    }

    fn account(&mut self, through: u64) {
        if self.listening {
            self.window.listening_ms += through.saturating_sub(self.last_elapsed);
        }
        self.last_elapsed = through;
    }

    pub fn disconnect(&mut self) {
        self.tick(None);
        self.listening = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    fn advert(path_length: u8) -> Vec<u8> {
        let signing = Ed25519KeyPair::from_seed_unchecked(&[42; 32]).unwrap();
        let key = signing.public_key().as_ref();
        let timestamp = 1_794_823_000_u32.to_le_bytes();
        let app_data = b"\x82Test repeater";
        let signature = signing.sign(&[key, &timestamp, app_data].concat());
        let mut frame = vec![0x88, 25, (-95_i8) as u8, 0x11, path_length];
        if path_length == 1 {
            frame.push(0xaa);
        }
        frame.extend_from_slice(key);
        frame.extend_from_slice(&timestamp);
        frame.extend_from_slice(signature.as_ref());
        frame.extend_from_slice(app_data);
        frame
    }

    #[test]
    fn windows_count_live_messages_and_verified_neighbors_and_persist_the_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            url: "https://workshop.example/api/iot".into(),
            device_id: "test-bot".into(),
            name: "Test bot".into(),
            ip_address: "127.0.0.1".parse().unwrap(),
            port: 8080,
            public_channel: 3,
            spool_dir: dir.path().to_owned(),
            root_ca_file: None,
        };
        let store = Store::open(&config).unwrap();
        let token = store.token().unwrap();
        assert_eq!(token.len(), 64);
        let mut reporter = Reporter {
            config,
            store: Arc::new(Mutex::new(store)),
            started: Instant::now(),
            boot_id: "test-boot".into(),
            listening: false,
            last_elapsed: 0,
            window: Window::default(),
        };
        let dm = b"\x07123456\xff\0\0\0\0\0private text";
        reporter.tick_at(5_000, Some(dm)); // startup backlog
        reporter.tick_at(10_000, Some(&[protocol::QUEUE_EMPTY]));
        reporter.tick_at(11_000, Some(dm));
        reporter.tick_at(12_000, Some(b"\x08\x03\0\0\0\0\0\0Channel text"));
        reporter.tick_at(13_000, Some(b"\x08\x04\0\0\0\0\0\0Other channel"));
        reporter.tick_at(14_000, Some(b"\x08\x04\0\0\0\0\0\0Other channel"));
        reporter.tick_at(15_000, Some(&advert(0x40)));
        reporter.tick_at(16_000, Some(&advert(0x40))); // the same neighbor only counts once
        reporter.tick_at(17_000, Some(&advert(1))); // relayed
        let mut forged = advert(0);
        forged[41] ^= 1; // signature
        assert!(Neighbor::from_raw(&forged).is_none());
        assert!(Neighbor::from_raw(&advert(1)).is_none());
        reporter.tick_at(18_000, Some(&forged));
        reporter.tick_at(240_000, None);
        reporter.listening = false;
        reporter.tick_at(900_000, None); // wholly offline windows are omitted

        let store = Arc::try_unwrap(reporter.store)
            .ok()
            .unwrap()
            .into_inner()
            .unwrap();
        assert_eq!(store.state.reports.len(), 1);
        drop(store);
        let mut store = Store::open(&reporter.config).unwrap();
        assert_eq!(store.token().unwrap(), token);
        let report = &store.state.reports[0].payload;
        validate_payload(report).unwrap();
        assert_eq!(report["reading"]["schema"], "meshcore_message_count");
        assert_eq!(report["reading"]["schema_version"], 2);
        let data = &report["reading"]["data"];
        assert_eq!(data["dm_count"], 1);
        assert_eq!(data["channel_counts"], json!({"3": 1, "4": 2}));
        assert_eq!(data["public_count"], 1);
        assert_eq!(data["listening_ms"], 230_000);
        assert_eq!(data["neighbor_count"], 1);
        assert_eq!(data["neighbors"][0]["name"], "Test repeater");
        assert_eq!(data["neighbors"][0]["rssi_dbm"], -95);
        assert!(!data.to_string().contains("private text"));

        let now = now_unix();
        let sent = upload::freeze(&mut store, "test-boot", 900_000, now)
            .unwrap()
            .unwrap();
        let retry = upload::classify(503, &Value::Null, None, "test-boot-1");
        upload::apply(&mut store, &sent, Some(503), retry, now).unwrap();
        let resent = upload::freeze(&mut store, "test-boot", 930_000, now + 30)
            .unwrap()
            .unwrap();
        assert_eq!(sent.payload, resent.payload);
        let ack = json!({"status":"approved", "report_id":"test-boot-1", "metric_duplicate":true});
        let approved = upload::classify(201, &ack, None, "test-boot-1");
        upload::apply(&mut store, &resent, Some(201), approved, now + 30).unwrap();
        assert!(store.state.reports.is_empty());
    }

    #[test]
    fn retry_now_only_clears_provisional_enrollment_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            url: "https://workshop.example/api/iot".into(),
            device_id: "test-bot".into(),
            name: "Test bot".into(),
            ip_address: "127.0.0.1".parse().unwrap(),
            port: 8080,
            public_channel: 0,
            spool_dir: dir.path().to_owned(),
            root_ca_file: None,
        };
        let mut store = Store::open(&config).unwrap();
        let token = store.token().unwrap();
        let window = Window {
            listening_ms: WINDOW_MS,
            ..Window::default()
        };
        store
            .append(window.report(&config, WINDOW_MS, now_unix(), "test-boot"))
            .unwrap();

        let mut state = store.state.clone();
        state.last_http_status = Some(429);
        state.enrollment_status = Some("provisional".into());
        state.retry_delay_seconds = PROVISIONAL_DELAY;
        state.retry_not_before_unix = now_unix() + PROVISIONAL_DELAY;
        store.save(state).unwrap();
        drop(store);

        let mut store = Store::open(&config).unwrap();
        assert!(!store.retry_provisional_now().unwrap());
        assert_eq!(store.state.retry_delay_seconds, PROVISIONAL_DELAY);
        let mut state = store.state.clone();
        state.last_http_status = Some(202);
        store.save(state).unwrap();
        assert!(store.retry_provisional_now().unwrap());
        assert!(!store.retry_provisional_now().unwrap());
        assert_eq!(store.state.retry_not_before_unix, 0);
        assert_eq!(store.state.reports.len(), 1);
        assert_eq!(store.token().unwrap(), token);
    }
}
