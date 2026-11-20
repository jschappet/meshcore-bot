mod automation;
mod diagnostics;
mod metrics;
mod probes;
mod protocol;
mod viewer;

use automation::{Action, Automation};
use diagnostics::{Multitest, Observations, Reception, Stats, Trace};
use log::{debug, info, trace, warn};
use protocol::{Frames, Message, Route};
use ring::rand::{SecureRandom, SystemRandom};
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, process::ExitCode, thread};

const USAGE: &str = "Usage: meshcore-bot [--port DEVICE] [--channel INDEX] [--config FILE] [--web-port PORT] [--web-host IP] [--metrics-config FILE] [--metrics-retry-now]
The port defaults to MESHCORE_PORT, then /dev/ttyACM0.
The Web Viewer binds to 127.0.0.1 unless --web-host is set.
Commands: ping, test/t [phrase], path/decode/route, prefix, stats, multitest/mt, trace/tracer, probe.
Send commands by direct message or add --channel INDEX to also reply there.
Public and #test never receive automatic replies.
Admin DMs: schedule, advert, hb, channelpause/channelresume, reload, announcements, repeater.";
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

struct Options {
    port: String,
    channel: Option<u8>,
    config_file: Option<PathBuf>,
    web_address: Option<SocketAddr>,
    metrics_config: Option<PathBuf>,
    metrics_retry_now: bool,
}

fn options() -> Result<Option<Options>, String> {
    let mut args = env::args().skip(1);
    let mut port = env::var("MESHCORE_PORT").unwrap_or_else(|_| "/dev/ttyACM0".into());
    let mut channel = None;
    let mut config_file = None;
    let mut web_port = None;
    let mut web_host = None;
    let mut metrics_config = None;
    let mut metrics_retry_now = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--port" => port = args.next().ok_or("--port requires a device path")?,
            "--config" => {
                config_file = Some(PathBuf::from(
                    args.next().ok_or("--config requires a file path")?,
                ))
            }
            "--metrics-config" => {
                metrics_config = Some(PathBuf::from(
                    args.next().ok_or("--metrics-config requires a file path")?,
                ));
            }
            "--metrics-retry-now" => metrics_retry_now = true,
            "--web-host" => {
                web_host = Some(
                    args.next()
                        .and_then(|value| value.parse::<IpAddr>().ok())
                        .ok_or("--web-host requires an IPv4 or IPv6 address")?,
                );
            }
            "--web-port" => {
                web_port = Some(
                    args.next()
                        .and_then(|value| value.parse::<u16>().ok())
                        .filter(|port| *port != 0)
                        .ok_or("--web-port requires a port from 1 to 65535")?,
                );
            }
            "--channel" => {
                channel = Some(
                    args.next()
                        .and_then(|value| value.parse().ok())
                        .ok_or("--channel requires an index from 0 to 255")?,
                );
            }
            _ => return Err(format!("Unknown argument {arg}")),
        }
    }
    if metrics_retry_now && metrics_config.is_none() {
        return Err("--metrics-retry-now requires --metrics-config".into());
    }
    if web_host.is_some() && web_port.is_none() {
        return Err("--web-host requires --web-port".into());
    }
    Ok(Some(Options {
        port,
        channel,
        config_file,
        web_address: web_port.map(|port| {
            SocketAddr::new(web_host.unwrap_or_else(|| Ipv4Addr::LOCALHOST.into()), port)
        }),
        metrics_config,
        metrics_retry_now,
    }))
}

struct Radio<T> {
    port: T,
    frames: Frames,
    last_command: Option<Instant>,
    observations: Observations,
    channel_key: Option<(u8, [u8; 16])>,
    contacts: Option<(Instant, Vec<diagnostics::Repeater>)>,
    multitest: Option<Multitest>,
    repeater_stats: Option<std::sync::Arc<std::sync::Mutex<diagnostics::RepeaterStats>>>,
    trace: Option<Trace>,
    metrics: Option<std::rc::Rc<std::cell::RefCell<metrics::Reporter>>>,
}

impl<T: Read + Write> Radio<T> {
    fn new(port: T) -> Self {
        Self {
            port,
            frames: Frames::default(),
            last_command: None,
            observations: Observations::default(),
            channel_key: None,
            contacts: None,
            multitest: None,
            repeater_stats: None,
            trace: None,
            metrics: None,
        }
    }

    fn receive(&mut self) -> io::Result<Option<Vec<u8>>> {
        let frame = self.frames.read(&mut self.port)?;
        if let Some(metrics) = &self.metrics {
            metrics.borrow_mut().tick(frame.as_deref());
        }
        if let Some(frame) = &frame {
            trace!("Radio RX opcode=0x{:02x} bytes={}", frame[0], frame.len());
            match frame[0] {
                0x88 => {
                    if let Some(snapshot) = &self.repeater_stats {
                        if let Some(repeater) = diagnostics::Repeater::from_raw(frame) {
                            snapshot.lock().unwrap().observe(repeater);
                        }
                    }
                    if let Some(row) = Reception::parse(frame) {
                        if let Some(session) = &mut self.multitest {
                            session.observe(&row);
                        }
                        self.observations.push(row);
                    }
                }
                0x89 => {
                    if let Some(trace) = &mut self.trace {
                        trace.observe(frame);
                    }
                }
                _ => {}
            }
        }
        Ok(frame)
    }

    fn send_command(&mut self, command: &[u8]) -> io::Result<()> {
        if let Some(last_command) = self.last_command {
            thread::sleep(Duration::from_millis(30).saturating_sub(last_command.elapsed()));
        }
        protocol::send(&mut self.port, command)?;
        trace!(
            "Radio TX opcode=0x{:02x} bytes={}",
            command[0],
            command.len()
        );
        self.last_command = Some(Instant::now());
        Ok(())
    }

    fn command(&mut self, command: &[u8], expected: &[u8]) -> io::Result<Vec<u8>> {
        self.send_command(command)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Some(frame) = self.receive()? {
                if frame[0] == protocol::ERROR || expected.contains(&frame[0]) {
                    return Ok(frame);
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "No response from companion for command 0x{:02x}",
                command[0]
            ),
        ))
    }

    fn handshake(&mut self) -> io::Result<()> {
        let info = self.command(b"\x01\0\0\0\0\0\0\0meshcore-bot", &[protocol::SELF_INFO])?;
        if info[0] != protocol::SELF_INFO || info.len() < 58 {
            return Err(io::Error::other("Invalid APP_START response"));
        }
        let device = self.command(&[0x16, 3], &[protocol::DEVICE_INFO])?;
        if device[0] != protocol::DEVICE_INFO || device.len() < 2 {
            return Err(io::Error::other("Invalid DEVICE_QUERY response"));
        }
        Ok(())
    }

    fn reply(&mut self, route: &Route, text: &str) -> io::Result<bool> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_secs() as u32;
        let (reply, expected) = route.reply(timestamp, text);
        let response = self.command(&reply, &[expected])?;
        if response[0] == protocol::ERROR {
            warn!(
                "Companion rejected outgoing message on {route} (code {:?})",
                response.get(1)
            );
        } else {
            debug!("Companion accepted outgoing message on {route}");
        }
        Ok(response[0] != protocol::ERROR)
    }

    fn send_advert(&mut self) -> io::Result<bool> {
        let accepted = self.command(&[0x07, 1], &[0])?[0] == 0;
        if !accepted {
            warn!("Companion rejected flood advert");
        } else {
            debug!("Companion accepted flood advert");
        }
        Ok(accepted)
    }

    fn diagnostics_ready(&self) -> bool {
        self.trace
            .as_ref()
            .is_some_and(|trace| trace.result.is_some() || Instant::now() >= trace.deadline)
            || self
                .multitest
                .as_ref()
                .is_some_and(|session| Instant::now() >= session.deadline)
    }

    fn finish_diagnostics(&mut self) -> io::Result<()> {
        if self
            .trace
            .as_ref()
            .is_some_and(|trace| trace.result.is_some() || Instant::now() >= trace.deadline)
        {
            let trace = self.trace.take().unwrap();
            self.reply(
                &trace.route,
                trace
                    .result
                    .as_deref()
                    .unwrap_or("Trace timed out | no return heard"),
            )?;
        }
        if self
            .multitest
            .as_ref()
            .is_some_and(|session| Instant::now() >= session.deadline)
        {
            let session = self.multitest.take().unwrap();
            self.reply(&session.route, &session.reply())?;
        }
        Ok(())
    }

    fn serve(
        &mut self,
        channel: Option<u8>,
        stats: &mut Stats,
        automation: &mut Automation,
    ) -> io::Result<()> {
        self.finish_diagnostics()?;
        if let Some(snapshot) = &self.repeater_stats {
            snapshot.lock().unwrap().tracked_repeaters = automation.tracked_repeaters.clone();
        }
        match automation.due(SystemTime::now()) {
            Some(Action::Post(channel, text)) => {
                debug!("Scheduled post due on channel #{channel}");
                self.reply(&Route::Channel(channel), &text)?;
            }
            Some(Action::Advert) => {
                debug!("Scheduled flood advert due");
                self.send_advert()?;
            }
            Some(Action::Heartbeat {
                recipient,
                sequence,
                time,
            }) => {
                self.send_heartbeat(&recipient, sequence, time)?;
            }
            None => {}
        }
        let channel = channel.filter(|_| !automation.channels_paused);
        let frame = self.command(
            &[0x0a],
            &[0x07, 0x08, 0x10, 0x11, 0x1b, protocol::QUEUE_EMPTY],
        )?;
        match frame[0] {
            protocol::ERROR => return Err(io::Error::other("Companion rejected message sync")),
            protocol::QUEUE_EMPTY => {
                let deadline = Instant::now() + Duration::from_secs(2);
                while Instant::now() < deadline
                    && !self.diagnostics_ready()
                    && !automation.has_due_work(SystemTime::now())
                {
                    if self
                        .receive()?
                        .is_some_and(|frame| frame[0] == protocol::MESSAGES_WAITING)
                    {
                        break;
                    }
                }
            }
            _ => {
                if let Some(message) = protocol::message(&frame) {
                    debug!(
                        "Incoming message on {} | text={:?}",
                        message.route, message.text
                    );
                    let command = parse_command(&message, channel);
                    stats.record(&message, command.is_some());
                    if let Some((keyword, argument)) = command {
                        debug!("Received command {keyword} on {}", message.route);
                        let text = if is_admin_command(&keyword) {
                            self.handle_admin(&message, &keyword, argument, automation)?
                        } else {
                            self.handle_command(&message, &keyword, argument, stats)?
                        };
                        if let Some(text) = text {
                            self.reply(&message.route, &text)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_admin(
        &mut self,
        message: &Message<'_>,
        keyword: &str,
        argument: &str,
        automation: &mut Automation,
    ) -> io::Result<Option<String>> {
        let Some(prefix) = automation.admin_candidate(message) else {
            debug!("Admin command {keyword} denied: requires a plain DM from a configured admin");
            return Ok(None);
        };
        let Some(contacts) = self.read_contacts()? else {
            warn!("Admin command {keyword} denied: companion contact list was incomplete");
            return Ok(None);
        };
        let matches: Vec<[u8; 32]> = contacts
            .iter()
            .filter_map(|frame| frame.get(1..33)?.try_into().ok())
            .filter(|key: &[u8; 32]| key.starts_with(&prefix))
            .collect();
        if !automation.authorized(prefix, &matches) {
            debug!("Admin command {keyword} denied: no unique authorized contact match ({} matching contacts)", matches.len());
            return Ok(None);
        }
        debug!("Admin command {keyword} authorized");
        let reply = match (keyword, argument) {
            ("channelpause", "") => {
                automation.channels_paused = true;
                if self
                    .trace
                    .as_ref()
                    .is_some_and(|trace| matches!(trace.route, Route::Channel(_)))
                {
                    self.trace = None;
                }
                if self
                    .multitest
                    .as_ref()
                    .is_some_and(|test| matches!(test.route, Route::Channel(_)))
                {
                    self.multitest = None;
                }
                "Channel replies paused".into()
            }
            ("channelresume", "") => {
                automation.channels_paused = false;
                "Channel replies resumed".into()
            }
            ("schedule", "") => automation.summary(),
            ("heartbeat" | "hb", argument) => automation.heartbeat_command(argument),
            ("reload", "") => match automation.reload() {
                Ok(()) => "Automation configuration reloaded".into(),
                Err(error) => {
                    warn!("Configuration reload failed: {error}");
                    "Reload failed | previous configuration is still active".into()
                }
            },
            ("advert", "") => match automation.reserve_advert() {
                Ok(()) if self.send_advert()? => "Flood advert accepted by radio".into(),
                Ok(()) => "Companion rejected the flood advert".into(),
                Err(remaining) => format!(
                    "Advert cooldown | {} minutes remaining",
                    remaining.as_secs().div_ceil(60).max(1)
                ),
            },
            ("announcements", argument) => {
                if let Some((channel, text)) = announcement(argument) {
                    if self.reply(&Route::Channel(channel), text)? {
                        format!("Announcement accepted for channel #{channel}")
                    } else {
                        format!("Radio rejected announcement for channel #{channel}")
                    }
                } else {
                    "Usage: announcements send CHANNEL MESSAGE (up to 100 UTF-8 bytes)".into()
                }
            }
            ("repeater" | "repeaters" | "rp", argument) => {
                let repeaters: Vec<_> = contacts
                    .iter()
                    .filter_map(|frame| diagnostics::Repeater::parse(frame))
                    .collect();
                let result = if argument.eq_ignore_ascii_case("stats") {
                    self.repeater_stats.as_ref().map_or_else(
                        || diagnostics::repeater_status(&repeaters, contacts.len(), argument),
                        |snapshot| snapshot.lock().unwrap().reply(),
                    )
                } else {
                    diagnostics::repeater_status(&repeaters, contacts.len(), argument)
                };
                self.contacts = Some((Instant::now(), repeaters));
                result
            }
            _ => format!("Usage: {keyword}"),
        };
        Ok(Some(reply))
    }

    fn handle_command(
        &mut self,
        message: &Message<'_>,
        keyword: &str,
        argument: &str,
        stats: &Stats,
    ) -> io::Result<Option<String>> {
        let reply = match keyword {
            "ping" => "Pong!".into(),
            "test" | "t" => test_reply(message, argument),
            "stats" => stats.reply(&argument.to_ascii_lowercase()),
            "probe" => self.handle_probe(message, argument, &stats.probes)?,
            "prefix" => self.lookup_prefix(&argument.to_ascii_lowercase())?,
            "path" | "decode" | "route" => {
                self.load_channel_key(&message.route)?;
                if let Some(row) = self.reception(message) {
                    let label = row.label();
                    if label.len() <= 90 {
                        format!("Path | {label}")
                    } else {
                        format!(
                            "Path | {} repeaters | path too long to display",
                            row.path.len() / row.width
                        )
                    }
                } else {
                    format!(
                        "Path | {} | repeater hashes unavailable",
                        route_label(message.path_len)
                    )
                }
            }
            "multitest" | "mt" => {
                if self.multitest.is_some() {
                    return Ok(Some("Multitest in progress | try again shortly".into()));
                }
                self.load_channel_key(&message.route)?;
                let Some(row) = self.reception(message) else {
                    return Ok(Some("Multitest needs a matching channel RF packet".into()));
                };
                self.multitest = Some(Multitest::new(message.route, row, &self.observations));
                return Ok(None);
            }
            "trace" | "tracer" => return self.start_trace(message, keyword, argument),
            _ => unreachable!("parse_command only returns implemented commands"),
        };
        Ok(Some(reply))
    }

    fn handle_probe(
        &mut self,
        message: &Message<'_>,
        argument: &str,
        probes: &std::sync::Mutex<probes::Probes>,
    ) -> io::Result<String> {
        match probes::parse(argument) {
            None => Ok(probes::USAGE.into()),
            Some(probes::Request::Summary { run }) => {
                let summary = probes.lock().unwrap().summary(message.route, run);
                info!("Probe summary on {} | {summary}", message.route);
                Ok(summary)
            }
            Some(probes::Request::Sample {
                run,
                sequence,
                total,
            }) => {
                self.load_channel_key(&message.route)?;
                let reception = self.reception(message);
                let signal = probes::Signal {
                    snr: message.snr_db,
                    rssi: reception.map(|row| row.rssi_dbm),
                };
                let path =
                    reception.map_or_else(|| route_label(message.path_len), Reception::label);
                let sample = probes::Sample {
                    sequence,
                    signal,
                    received_at: chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    path: path.clone(),
                };
                let duplicate =
                    match probes
                        .lock()
                        .unwrap()
                        .record(message.route, run, total, sample)
                    {
                        Ok(duplicate) => duplicate,
                        Err(error) => return Ok(error.into()),
                    };
                let snr = signal
                    .snr
                    .map_or_else(|| "n/a".into(), |value| format!("{value}dB"));
                let rssi = signal
                    .rssi
                    .map_or_else(|| "n/a".into(), |value| format!("{value}dBm"));
                info!(
                    "Probe {run} {sequence}/{total} on {} | SNR {snr} | RSSI {rssi} | path {path} | duplicate={duplicate}",
                    message.route,
                );
                let marker = if duplicate { " dup" } else { "" };
                let mut reply =
                    format!("ACK {run} {sequence}/{total}{marker} | SNR {snr} | RSSI {rssi}");
                if reply.len() + 3 + path.len() <= 100 {
                    reply.push_str(" | ");
                    reply.push_str(&path);
                }
                Ok(reply)
            }
        }
    }

    fn load_channel_key(&mut self, route: &Route) -> io::Result<()> {
        let Route::Channel(index) = *route else {
            return Ok(());
        };
        if self.channel_key.is_some_and(|(slot, _)| slot == index) {
            return Ok(());
        }
        if let Some((_, key)) = self.channel_details(index)? {
            self.channel_key = Some((index, key));
        }
        Ok(())
    }

    fn channel_details(&mut self, index: u8) -> io::Result<Option<(String, [u8; 16])>> {
        let response = self.command(&[0x1f, index], &[0x12])?;
        if response.get(..2) != Some(&[0x12, index]) {
            return Ok(None);
        }
        let details = response.get(2..50).and_then(|details| {
            let name = details[..32].split(|byte| *byte == 0).next()?;
            let name = std::str::from_utf8(name).ok()?.trim();
            Some((name.to_owned(), details[32..].try_into().ok()?))
        });
        Ok(details)
    }

    fn reply_channel(&mut self, requested: Option<u8>) -> io::Result<Option<u8>> {
        let Some(index) = requested else {
            return Ok(None);
        };
        let Some((name, key)) = self.channel_details(index)? else {
            warn!("Channel replies disabled: could not identify radio slot #{index}");
            return Ok(None);
        };
        let normalized = name.strip_prefix('#').unwrap_or(&name);
        // The well-known Public PSK differs from the key derived for #public.
        const PUBLIC_KEY: [u8; 16] = [
            0x8b, 0x33, 0x87, 0xe9, 0xc5, 0xcd, 0xea, 0x6a, 0xc9, 0xe8, 0xed, 0xba, 0xa1, 0x15,
            0xcd, 0x72,
        ];
        let test_key = ring::digest::digest(&ring::digest::SHA256, b"#test");
        if name.is_empty()
            || normalized.eq_ignore_ascii_case("public")
            || normalized.eq_ignore_ascii_case("test")
            || key == PUBLIC_KEY
            || key == test_key.as_ref()[..16]
        {
            info!("Channel replies disabled on radio slot #{index}");
            return Ok(None);
        }
        self.channel_key = Some((index, key));
        Ok(Some(index))
    }

    fn reception(&self, message: &Message<'_>) -> Option<&Reception> {
        let (index, key) = self.channel_key.as_ref()?;
        if message.route != Route::Channel(*index) {
            return None;
        }
        self.observations.find(message, key)
    }

    fn lookup_prefix(&mut self, argument: &str) -> io::Result<String> {
        if !diagnostics::valid_prefix_argument(argument) {
            return Ok("Usage: prefix XX [all] | prefix free | prefix refresh".into());
        }
        let max_age = Duration::from_secs(if argument == "refresh" { 5 } else { 60 });
        if self
            .contacts
            .as_ref()
            .is_none_or(|(at, _)| at.elapsed() >= max_age)
        {
            let Some(contacts) = self.read_contacts()? else {
                return Ok("Companion contact sync unavailable".into());
            };
            let repeaters = contacts
                .iter()
                .filter_map(|frame| diagnostics::Repeater::parse(frame))
                .collect();
            self.contacts = Some((Instant::now(), repeaters));
        }
        Ok(diagnostics::prefix_reply(
            &self.contacts.as_ref().unwrap().1,
            argument,
        ))
    }

    fn read_contacts(&mut self) -> io::Result<Option<Vec<Vec<u8>>>> {
        self.send_command(&[0x04])?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut contacts = Vec::new();
        let mut count = None;
        let mut valid = true;
        while Instant::now() < deadline {
            if let Some(frame) = self.receive()? {
                match frame[0] {
                    protocol::ERROR => return Ok(None),
                    0x02 if count.is_none() => {
                        count = frame
                            .get(1..5)
                            .and_then(|bytes| bytes.try_into().ok())
                            .map(u32::from_le_bytes);
                        valid &= count.is_some();
                    }
                    0x03 if count.is_some() && frame.len() >= 33 => contacts.push(frame),
                    0x02 | 0x03 => valid = false,
                    0x04 => {
                        if valid && count == Some(contacts.len() as u32) {
                            if let Some(snapshot) = &self.repeater_stats {
                                let repeaters: Vec<_> = contacts
                                    .iter()
                                    .filter_map(|frame| diagnostics::Repeater::parse(frame))
                                    .collect();
                                let mut snapshot = snapshot.lock().unwrap();
                                for repeater in repeaters {
                                    snapshot.observe(repeater);
                                }
                            }
                            return Ok(Some(contacts));
                        }
                        return Ok(None);
                    }
                    _ => {}
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "Contact sync timed out",
        ))
    }

    fn send_heartbeat(
        &mut self,
        recipient: &[u8; 32],
        sequence: u64,
        time: chrono::DateTime<chrono::Local>,
    ) -> io::Result<()> {
        let prefix: [u8; 6] = recipient[..6].try_into().unwrap();
        let Some(contacts) = self.read_contacts()? else {
            warn!("Heartbeat #{sequence} skipped: companion contact list was incomplete");
            return Ok(());
        };
        let mut matches = contacts
            .iter()
            .filter_map(|frame| frame.get(1..33))
            .filter(|key| key.starts_with(&prefix));
        if matches.next() != Some(recipient.as_slice()) || matches.next().is_some() {
            warn!("Heartbeat #{sequence} skipped: recipient has no unique radio contact match");
            return Ok(());
        }
        let message = format!("Heartbeat #{sequence} | {}", time.format("%F %T %:z"));
        if self.reply(&Route::Direct(prefix), &message)? {
            info!("Heartbeat #{sequence} accepted by radio for DM");
        }
        Ok(())
    }

    fn start_trace(
        &mut self,
        message: &Message<'_>,
        keyword: &str,
        argument: &str,
    ) -> io::Result<Option<String>> {
        if self.trace.is_some() {
            return Ok(Some("Trace in progress | try again shortly".into()));
        }
        let path = if argument.is_empty() {
            self.load_channel_key(&message.route)?;
            let Some(row) = self.reception(message) else {
                return Ok(Some(
                    "Incoming path unavailable | try tracer 01,7A,55".into(),
                ));
            };
            row.label()
        } else {
            argument.to_owned()
        };
        let Some((nodes, flags)) =
            diagnostics::trace_path(&path, keyword == "tracer" || argument.is_empty())
        else {
            return Ok(Some(
                "Trace needs a path of up to 12 hops after expansion, e.g. tracer 01,7A,55".into(),
            ));
        };

        let mut tag = [0; 4];
        SystemRandom::new()
            .fill(&mut tag)
            .map_err(|_| io::Error::other("Could not generate a trace tag"))?;
        let hops = (nodes.len() >> flags) as u64;
        let wait = Duration::from_millis(2_000 + 1_500 * hops);
        self.trace = Some(Trace {
            route: message.route,
            tag,
            deadline: Instant::now() + wait + Duration::from_secs(5),
            result: None,
        });
        let mut send = vec![0x24];
        send.extend_from_slice(&tag);
        send.extend_from_slice(&[0; 4]);
        send.push(flags);
        send.extend(nodes);
        let response = self.command(&send, &[protocol::MESSAGE_SENT])?;
        if response.get(2..6) != Some(&tag) || response[0] != protocol::MESSAGE_SENT {
            self.trace = None;
            return Ok(Some("Companion rejected the trace".into()));
        }
        if let Some(trace) = &mut self.trace {
            let estimated_ms = response
                .get(6..10)
                .and_then(|raw| raw.try_into().ok())
                .map(u32::from_le_bytes)
                .unwrap_or(0);
            trace.deadline = Instant::now()
                + wait.max(Duration::from_millis(u64::from(estimated_ms.min(60_000))));
        }
        Ok(None)
    }
}

fn is_admin_command(keyword: &str) -> bool {
    matches!(
        keyword,
        "schedule"
            | "advert"
            | "heartbeat"
            | "hb"
            | "channelpause"
            | "channelresume"
            | "reload"
            | "announcements"
            | "repeater"
            | "repeaters"
            | "rp"
    )
}

fn announcement(argument: &str) -> Option<(u8, &str)> {
    let (verb, rest) = argument.split_once(char::is_whitespace)?;
    let (channel, text) = rest.trim_start().split_once(char::is_whitespace)?;
    let text = text.trim();
    (verb.eq_ignore_ascii_case("send") && automation::valid_post(text))
        .then_some((channel.parse().ok()?, text))
}

fn parse_command<'a>(message: &Message<'a>, channel: Option<u8>) -> Option<(String, &'a str)> {
    let text = message.text;
    let command = match message.route {
        Route::Direct(_) => text,
        Route::Channel(index) if Some(index) == channel => text
            .split_once(':')
            .filter(|(sender, _)| !sender.trim().is_empty())
            .map_or(text, |(_, message)| message),
        Route::Channel(_) => return None,
    };
    let command = command.trim();
    let (keyword, argument) = command
        .split_once(char::is_whitespace)
        .unwrap_or((command, ""));
    let keyword = keyword.to_ascii_lowercase();
    let argument = argument.trim();
    match keyword.as_str() {
        "ping" | "path" | "decode" | "route" | "multitest" | "mt" if argument.is_empty() => {}
        "test" | "t" | "stats" | "prefix" | "trace" | "tracer" | "probe" => {}
        _ if is_admin_command(&keyword) && matches!(message.route, Route::Direct(_)) => {}
        _ => return None,
    }
    Some((keyword, argument))
}

fn route_label(path_len: u8) -> String {
    match path_len {
        0xff => "Direct route".into(),
        // Flooded packets encode the repeater count in the low six bits.
        path_len => match path_len & 0x3f {
            1 => "1 hop".into(),
            hops => format!("{hops} hops"),
        },
    }
}

fn test_reply(message: &Message<'_>, phrase: &str) -> String {
    let path = route_label(message.path_len);
    let snr = message
        .snr_db
        .map_or_else(|| "SNR unavailable".into(), |snr| format!("SNR {snr} dB"));
    let mut reply = format!("ACK | {path} | {snr}");
    if !phrase.is_empty() {
        // Leave room for diagnostics and the radio's channel sender prefix.
        let mut end = phrase.len().min(48);
        while !phrase.is_char_boundary(end) {
            end -= 1;
        }
        reply.push_str(" | ");
        reply.push_str(&phrase[..end]);
    }
    reply
}

fn connect(path: &str) -> io::Result<Radio<Box<dyn serialport::SerialPort>>> {
    debug!("Opening companion serial port {path} at 115200 baud");
    let port = serialport::new(path, 115_200)
        .timeout(Duration::from_millis(200))
        .open()
        .map_err(|error| io::Error::other(format!("Could not open serial port {path}: {error}")))?;
    // As in flood_monitor, let the USB companion settle before APP_START.
    thread::sleep(Duration::from_secs(1));
    let mut radio = Radio::new(port);
    radio.handshake()?;
    Ok(radio)
}

fn run(options: Options) -> io::Result<()> {
    let retries = std::iter::repeat_with(|| thread::sleep(RECONNECT_DELAY));
    run_sessions(&options, connect, retries)
}

fn run_sessions<T: Read + Write>(
    options: &Options,
    mut connect: impl FnMut(&str) -> io::Result<Radio<T>>,
    mut retries: impl Iterator<Item = ()>,
) -> io::Result<()> {
    let mut automation = Automation::new(options.config_file.clone())?;
    let mut stats = Stats::default();
    let repeater_stats =
        std::sync::Arc::new(std::sync::Mutex::new(diagnostics::RepeaterStats::default()));
    let viewer = options
        .web_address
        .map(|address| viewer::start(address, stats.probes.clone(), repeater_stats.clone()))
        .transpose()?;
    let config = options
        .metrics_config
        .as_deref()
        .map(metrics::Config::load)
        .transpose()?;
    let mut connect_session = || -> io::Result<_> {
        let mut radio = connect(&options.port)?;
        let channel = radio.reply_channel(options.channel)?;
        Ok((radio, channel))
    };
    let (mut radio, mut reply_channel) = connect_session()?;
    let metrics = config
        .map(|config| metrics::Reporter::start(config, options.metrics_retry_now))
        .transpose()?
        .map(|reporter| std::rc::Rc::new(std::cell::RefCell::new(reporter)));
    loop {
        radio.metrics = metrics.clone();
        radio.repeater_stats = Some(repeater_stats.clone());
        if let Some(viewer) = &viewer {
            viewer.set_radio(viewer::RadioStatus::Connected);
        }
        info!("MeshCore companion connected on {}", options.port);
        let mut error = loop {
            if let Err(error) = radio.serve(reply_channel, &mut stats, &mut automation) {
                break error;
            }
        };
        // Close the old port and discard incomplete frames and replies before reconnecting.
        drop(radio);
        if let Some(metrics) = &metrics {
            metrics.borrow_mut().disconnect();
        }
        if let Some(viewer) = &viewer {
            viewer.set_radio(viewer::RadioStatus::Reconnecting);
        }
        warn!("Lost companion session on {}: {error}", options.port);
        (radio, reply_channel) = loop {
            if retries.next().is_none() {
                return Err(error);
            }
            match connect_session() {
                Ok(session) => break session,
                Err(reconnect_error) => {
                    warn!("Reconnect on {} failed: {reconnect_error}", options.port);
                    error = reconnect_error;
                }
            }
        };
        automation.skip_missed(SystemTime::now());
    }
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    match options() {
        Ok(Some(options)) => match run(options) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("MeshCore bot: {error}");
                ExitCode::FAILURE
            }
        },
        Ok(None) => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}\n{USAGE}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;

    fn command(device: &mut UnixStream) -> Vec<u8> {
        let mut header = [0; 3];
        device.read_exact(&mut header).unwrap();
        assert_eq!(header[0], b'<');
        let mut payload = vec![0; u16::from_le_bytes([header[1], header[2]]) as usize];
        device.read_exact(&mut payload).unwrap();
        payload
    }

    fn respond(device: &mut UnixStream, payload: &[u8]) {
        device.write_all(&[b'>', payload.len() as u8, 0]).unwrap();
        device.write_all(payload).unwrap();
    }

    fn channel_message(channel: u8, text: &[u8]) -> Vec<u8> {
        let mut frame = vec![0x11, 0xf8, 0, 0, channel, 0xff, 0, 1, 0, 0, 0];
        frame.extend_from_slice(text);
        frame
    }

    #[test]
    fn suppresses_public_and_test_channel_replies_by_name_or_key() {
        let (client, mut device) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        device
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let companion = thread::spawn(move || {
            for (index, name, key) in [
                (3, "Public", [1; 16]),
                (5, "#TeSt", [1; 16]),
                (
                    8,
                    "Renamed",
                    ring::digest::digest(&ring::digest::SHA256, b"#test").as_ref()[..16]
                        .try_into()
                        .unwrap(),
                ),
                (9, "#antenna", [1; 16]),
            ] {
                assert_eq!(command(&mut device), [0x1f, index]);
                let mut frame = [0; 50];
                frame[..2].copy_from_slice(&[0x12, index]);
                frame[2..2 + name.len()].copy_from_slice(name.as_bytes());
                frame[34..].copy_from_slice(&key);
                respond(&mut device, &frame);
            }
        });
        let mut radio = Radio::new(client);
        assert_eq!(radio.reply_channel(Some(3)).unwrap(), None);
        assert_eq!(radio.reply_channel(Some(5)).unwrap(), None);
        assert_eq!(radio.reply_channel(Some(8)).unwrap(), None);
        assert_eq!(radio.reply_channel(Some(9)).unwrap(), Some(9));
        companion.join().unwrap();
    }

    #[test]
    fn reconnects_after_a_failed_open_without_resending_an_unconfirmed_reply() {
        let simulated_session = |prefix: [u8; 6], confirm_reply| {
            let (client, mut device) = UnixStream::pair().unwrap();
            client
                .set_read_timeout(Some(Duration::from_millis(20)))
                .unwrap();
            device
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let companion = thread::spawn(move || {
                assert_eq!(command(&mut device), b"\x01\0\0\0\0\0\0\0meshcore-bot");
                let mut self_info = [0; 58];
                self_info[0] = protocol::SELF_INFO;
                respond(&mut device, &self_info);
                assert_eq!(command(&mut device), [0x16, 3]);
                respond(&mut device, &[protocol::DEVICE_INFO, 3]);
                assert_eq!(command(&mut device), [0x0a]);
                let mut ping = vec![0x07];
                ping.extend_from_slice(&prefix);
                ping.extend_from_slice(b"\xff\0\0\0\0\0ping");
                respond(&mut device, &ping);
                let reply = command(&mut device);
                assert_eq!(&reply[..3], &[0x02, 0, 0]);
                assert_eq!(&reply[7..13], &prefix);
                assert_eq!(&reply[13..], b"Pong!");
                if confirm_reply {
                    respond(&mut device, &[protocol::MESSAGE_SENT]);
                    assert_eq!(command(&mut device), [0x0a]);
                }
            });
            (client, companion)
        };

        let (first, first_companion) = simulated_session(*b"123456", false);
        let (second, second_companion) = simulated_session(*b"abcdef", true);
        let opens: [io::Result<_>; 3] =
            [Ok(first), Err(io::ErrorKind::NotFound.into()), Ok(second)];
        let mut opens = opens.into_iter();
        let error = run_sessions(
            &Options {
                port: "test companion".into(),
                channel: None,
                config_file: None,
                web_address: None,
                metrics_config: None,
                metrics_retry_now: false,
            },
            |_| {
                let mut radio = Radio::new(opens.next().expect("unexpected open")?);
                radio.handshake()?;
                Ok(radio)
            },
            [(), ()].into_iter(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        first_companion.join().unwrap();
        second_companion.join().unwrap();
    }

    #[test]
    fn ping_replies_to_the_sender_and_only_the_enabled_channel() {
        let (client, mut device) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        device
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let companion = thread::spawn(move || {
            assert_eq!(command(&mut device), b"\x01\0\0\0\0\0\0\0meshcore-bot");
            let mut self_info = [0; 58];
            self_info[0] = 0x05;
            device.write_all(b">\x3a").unwrap();
            thread::sleep(Duration::from_millis(40));
            device.write_all(b"\x00").unwrap();
            device.write_all(&self_info).unwrap();
            assert_eq!(command(&mut device), [0x16, 3]);
            respond(&mut device, &[0x0d, 3]);

            assert_eq!(command(&mut device), [0x0a]);
            let prefix = [1, 2, 3, 4, 5, 6];
            let mut dm = vec![0x10, 0xf8, 0, 0];
            dm.extend_from_slice(&prefix);
            dm.extend_from_slice(b"\xff\x02\x01\0\0\0\x01\x02\x03\x04 PiNg \n");
            respond(&mut device, &dm);
            let reply = command(&mut device);
            assert_eq!(&reply[..3], &[0x02, 0, 0]);
            assert_eq!(&reply[7..13], &prefix);
            assert_eq!(&reply[13..], b"Pong!");
            respond(&mut device, &[0x06]);

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &channel_message(2, b"Node: ping"));
            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &channel_message(3, b"Node: ping please"));
            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &channel_message(3, b"Node: ping"));
            let reply = command(&mut device);
            assert_eq!(&reply[..3], &[0x03, 0, 3]);
            assert_eq!(&reply[7..], b"Pong!");
            respond(&mut device, &[0]);

            assert_eq!(command(&mut device), [0x0a]);
            let mut test = channel_message(3, b"Node: T check");
            test[5] = 0x82; // Two repeaters with three-byte hashes.
            respond(&mut device, &test);
            let reply = command(&mut device);
            assert_eq!(&reply[..3], &[0x03, 0, 3]);
            assert_eq!(&reply[7..], b"ACK | 2 hops | SNR -2 dB | check");
            respond(&mut device, &[0]);

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &[0x0a]);
            respond(&mut device, &[0x83]);
        });

        let mut radio = Radio::new(client);
        radio.handshake().unwrap();
        let mut stats = Stats::default();
        let mut automation = Automation::default();
        for _ in 0..6 {
            radio.serve(Some(3), &mut stats, &mut automation).unwrap();
        }
        companion.join().unwrap();
    }

    fn raw_group_text(path: &[u8], text: &[u8], channel_key: &[u8; 16]) -> Vec<u8> {
        use aes::cipher::{BlockEncrypt, KeyInit};
        use ring::{digest, hmac};

        let mut plaintext = b"\x01\0\0\0\0".to_vec();
        plaintext.extend_from_slice(text);
        plaintext.resize(plaintext.len().next_multiple_of(16), 0);
        let cipher = aes::Aes128::new(channel_key.into());
        for block in plaintext.as_chunks_mut::<16>().0 {
            cipher.encrypt_block(block.into());
        }
        let mac = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, channel_key), &plaintext);
        let mut frame = vec![0x88, 0, 0, 0x15, path.len() as u8];
        frame.extend_from_slice(path);
        frame.push(digest::digest(&digest::SHA256, channel_key).as_ref()[0]);
        frame.extend_from_slice(&mac.as_ref()[..2]);
        frame.extend_from_slice(&plaintext);
        frame
    }

    #[test]
    fn diagnoses_authenticated_paths_and_collects_matching_receptions() {
        let key = [0x2a; 16];
        let (client, mut device) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        device
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let companion = thread::spawn(move || {
            assert_eq!(command(&mut device), [0x0a]);
            let logged = raw_group_text(&[1, 0xab], b"Node: path", &key);
            respond(&mut device, &logged);
            let mut tampered = logged.clone();
            tampered[5] = 0xcc;
            *tampered.last_mut().unwrap() ^= 0x01;
            respond(&mut device, &tampered);
            let mut path_message = channel_message(3, b"Node: path");
            path_message[5] = 2;
            respond(&mut device, &path_message);

            assert_eq!(command(&mut device), [0x1f, 3]);
            let mut channel = vec![0; 50];
            channel[..2].copy_from_slice(&[0x12, 3]);
            channel[34..].copy_from_slice(&key);
            respond(&mut device, &channel);
            assert_eq!(&command(&mut device)[7..], b"Path | 01,AB");
            respond(&mut device, &[0]);

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &raw_group_text(&[1], b"Node: mt", &key));
            let mut mt_message = channel_message(3, b"Node: mt");
            mt_message[5] = 1;
            respond(&mut device, &mt_message);

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &raw_group_text(&[0x22], b"Unrelated", &key));
            respond(&mut device, &raw_group_text(&[0xab], b"Node: mt", &key));
            respond(&mut device, &raw_group_text(&[0xab], b"Node: mt", &key));
            respond(&mut device, &[0x0a]);
            respond(&mut device, &[0x83]);

            assert_eq!(
                &command(&mut device)[7..],
                b"Multitest | 2 observed paths | 01 | AB"
            );
            respond(&mut device, &[0]);
            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &channel_message(3, b"Node: stats channels"));
            assert_eq!(
                &command(&mut device)[7..],
                b"Channels 24h | 3 messages | #3: 3"
            );
            respond(&mut device, &[0]);

            assert_eq!(command(&mut device), [0x0a]);
            let mut observed = raw_group_text(&[0xab], b"Node: probe hill-a 2/4", &key);
            observed[2] = (-94i8) as u8;
            respond(&mut device, &observed);
            let mut probe = channel_message(3, b"Node: probe hill-a 2/4");
            probe[5] = 1;
            respond(&mut device, &probe);
            assert_eq!(
                &command(&mut device)[7..],
                b"ACK hill-a 2/4 | SNR -2dB | RSSI -94dBm | AB"
            );
            respond(&mut device, &[0]);

            assert_eq!(command(&mut device), [0x0a]);
            respond(
                &mut device,
                &channel_message(3, b"Node: probe stats hill-a"),
            );
            assert_eq!(
                &command(&mut device)[7..],
                b"hill-a | RX 1/4 (25%) | dup 0 | SNR med -2.0dB | RSSI med -94.0dBm"
            );
            respond(&mut device, &[0]);
        });

        let mut radio = Radio::new(client);
        let mut stats = Stats::default();
        let mut automation = Automation::default();
        for _ in 0..3 {
            radio.serve(Some(3), &mut stats, &mut automation).unwrap();
        }
        radio.multitest.as_mut().unwrap().deadline = Instant::now();
        for _ in 0..3 {
            radio.serve(Some(3), &mut stats, &mut automation).unwrap();
        }
        let run = stats.probes.lock().unwrap().detail(1).unwrap();
        let payload = serde_json::to_value(run).unwrap();
        assert_eq!(payload["samples"][0]["path"], "AB");
        assert_eq!(payload["samples"][0]["rssi"], -94);
        companion.join().unwrap();
    }

    #[test]
    fn queries_repeaters_and_correlates_even_an_early_trace_result() {
        let (client, mut device) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        device
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let companion = thread::spawn(move || {
            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &channel_message(3, b"Node: prefix 01 all"));
            assert_eq!(command(&mut device), [0x04]);
            respond(&mut device, &[0x02, 1, 0, 0, 0]);
            let mut contact = [0; 148];
            contact[..2].copy_from_slice(&[3, 1]);
            contact[33] = 2;
            contact[100..104].copy_from_slice(b"Hill");
            respond(&mut device, &contact);
            respond(&mut device, &[0x04, 0, 0, 0, 0]);
            assert_eq!(
                &command(&mut device)[7..],
                b"Prefix 01 | 1 radio repeaters | Hill (last heard unknown)"
            );
            respond(&mut device, &[0]);

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &channel_message(3, b"Node: tracer 01,AB"));
            let trace = command(&mut device);
            assert_eq!(trace[0], 0x24);
            assert_eq!(&trace[5..], &[0, 0, 0, 0, 0, 1, 0xab, 1]);
            let tag = &trace[1..5];
            let mut data = vec![0x89, 0, 3, 0];
            data.extend_from_slice(tag);
            data.extend_from_slice(&[0, 0, 0, 0, 1, 0xab, 1, 20, 0xf8, 40, 50]);
            let mut unrelated = data.clone();
            unrelated[4] ^= 0x01;
            unrelated[15] = 80;
            respond(&mut device, &unrelated);
            respond(&mut device, &data);
            let mut ack = vec![0x06, 0];
            ack.extend_from_slice(tag);
            ack.extend_from_slice(&[0; 4]);
            respond(&mut device, &ack);

            let expected = format!(
                "Trace {} | 3 hops | SNR dB | 01:5 | AB:-2 | 01:10 | bot:12.5",
                diagnostics::hex(tag)
            );
            assert_eq!(&command(&mut device)[7..], expected.as_bytes());
            respond(&mut device, &[0]);
            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &[0x0a]);
            respond(&mut device, &[0x83]);
        });
        let mut radio = Radio::new(client);
        let mut stats = Stats::default();
        let mut automation = Automation::default();
        for _ in 0..3 {
            radio.serve(Some(3), &mut stats, &mut automation).unwrap();
        }
        companion.join().unwrap();
    }

    fn dm(prefix: &[u8; 6], text: &[u8]) -> Vec<u8> {
        let mut frame = vec![0x07];
        frame.extend_from_slice(prefix);
        frame.extend_from_slice(b"\xff\0\0\0\0\0");
        frame.extend_from_slice(text);
        frame
    }

    fn contacts(device: &mut UnixStream, keys: &[[u8; 32]]) {
        assert_eq!(command(device), [0x04]);
        let mut start = vec![0x02];
        start.extend_from_slice(&(keys.len() as u32).to_le_bytes());
        respond(device, &start);
        for key in keys {
            let mut contact = [0; 148];
            contact[0] = 0x03;
            contact[1..33].copy_from_slice(key);
            respond(device, &contact);
        }
        respond(device, &[0x04, 0, 0, 0, 0]);
    }

    #[test]
    fn heartbeat_sends_only_to_the_unique_admin_contact() {
        let key = [0x12; 32];
        let time = chrono::DateTime::<chrono::Local>::from(UNIX_EPOCH + Duration::from_secs(3_600));
        let expected = format!("Heartbeat #2 | {}", time.format("%F %T %:z"));
        let mut collision = key;
        collision[31] = 0x34;
        let (client, mut device) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        device
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let companion = thread::spawn(move || {
            contacts(&mut device, &[key, collision]);
            // Only the contact query for the second heartbeat should follow.
            contacts(&mut device, &[key]);
            let heartbeat = command(&mut device);
            assert_eq!(&heartbeat[..3], &[0x02, 0, 0]);
            assert_eq!(&heartbeat[7..13], &key[..6]);
            assert_eq!(&heartbeat[13..], expected.as_bytes());
            respond(&mut device, &[0x06]);
        });
        let mut radio = Radio::new(client);
        radio.send_heartbeat(&key, 1, time).unwrap();
        radio.send_heartbeat(&key, 2, time).unwrap();
        companion.join().unwrap();
    }

    #[test]
    fn admin_requires_a_unique_contact_and_keeps_channel_pause_separate_from_dm() {
        let key = [0x12; 32];
        let prefix = [0x12; 6];
        let mut collision = key;
        collision[31] = 0x34;
        let config = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            config.path(),
            serde_json::json!({"admin_pubkeys": [diagnostics::hex(&key)]}).to_string(),
        )
        .unwrap();
        let mut automation = Automation::new(Some(config.path().to_owned())).unwrap();

        let (client, mut device) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        device
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let companion = thread::spawn(move || {
            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &channel_message(3, b"Node: channelpause"));

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &dm(&prefix, b"channelpause"));
            contacts(&mut device, &[key, collision]);

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &channel_message(3, b"Node: ping"));
            assert_eq!(&command(&mut device)[7..], b"Pong!");
            respond(&mut device, &[0]);

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &dm(&prefix, b"CHANNELPAUSE"));
            contacts(&mut device, &[key]);
            assert_eq!(&command(&mut device)[13..], b"Channel replies paused");
            respond(&mut device, &[0x06]);

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &channel_message(3, b"Node: ping"));

            assert_eq!(command(&mut device), [0x0a]);
            respond(
                &mut device,
                &dm(&prefix, b"announcements send 3 Net at noon"),
            );
            contacts(&mut device, &[key]);
            let announcement = command(&mut device);
            assert_eq!(&announcement[..3], &[3, 0, 3]);
            assert_eq!(&announcement[7..], b"Net at noon");
            respond(&mut device, &[0]);
            assert_eq!(
                &command(&mut device)[13..],
                b"Announcement accepted for channel #3"
            );
            respond(&mut device, &[0x06]);

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &dm(&prefix, b"channelresume"));
            contacts(&mut device, &[key]);
            assert_eq!(&command(&mut device)[13..], b"Channel replies resumed");
            respond(&mut device, &[0x06]);

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &dm(&prefix, b"advert"));
            contacts(&mut device, &[key]);
            assert_eq!(command(&mut device), [0x07, 1]);
            respond(&mut device, &[0]);
            assert_eq!(
                &command(&mut device)[13..],
                b"Flood advert accepted by radio"
            );
            respond(&mut device, &[0x06]);

            assert_eq!(command(&mut device), [0x0a]);
            respond(&mut device, &dm(&prefix, b"advert"));
            contacts(&mut device, &[key]);
            assert_eq!(
                &command(&mut device)[13..],
                b"Advert cooldown | 60 minutes remaining"
            );
            respond(&mut device, &[0x06]);
        });

        let mut stats = Stats::default();
        let mut radio = Radio::new(client);
        for _ in 0..9 {
            radio.serve(Some(3), &mut stats, &mut automation).unwrap();
        }
        assert!(!automation.channels_paused);
        companion.join().unwrap();
    }

    #[test]
    fn legacy_direct_test_reports_missing_snr_and_keeps_utf8_intact() {
        let mut frame = b"\x07123456\xff\0\0\0\0\0test ".to_vec();
        let phrase = format!("{}🦀", "a".repeat(47));
        frame.extend_from_slice(phrase.as_bytes());
        let message = protocol::message(&frame).unwrap();
        assert_eq!(
            test_reply(&message, parse_command(&message, None).unwrap().1),
            format!("ACK | Direct route | SNR unavailable | {}", "a".repeat(47))
        );
    }
}
