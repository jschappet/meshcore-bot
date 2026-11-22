use crate::protocol::{Message, Route};
use aes::cipher::{BlockDecrypt, KeyInit};
use ring::{digest, hmac};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const STATS_WINDOW: Duration = Duration::from_secs(86_400);
const RAW_CACHE_AGE: Duration = Duration::from_secs(15);
pub const MULTITEST_DURATION: Duration = Duration::from_secs(6);

pub struct Reception {
    pub at: Instant,
    pub width: usize,
    pub path: Vec<u8>,
    pub rssi_dbm: i8,
    pub payload: Vec<u8>,
    flooded: bool,
    path_len: u8,
}

impl Reception {
    pub fn parse(frame: &[u8]) -> Option<Self> {
        let raw = frame.get(3..)?;
        let header = *raw.first()?;
        // Only group text can be authenticated with the enabled channel's key.
        if (header >> 2) & 0x0f != 5 {
            return None;
        }
        let length_offset = if matches!(header & 3, 0 | 3) { 5 } else { 1 };
        let length = *raw.get(length_offset)?;
        let width = usize::from((length >> 6) + 1);
        if width == 4 {
            return None;
        }
        let start = length_offset + 1;
        let path_bytes = width * usize::from(length & 0x3f);
        if path_bytes > 64 {
            return None;
        }
        let end = start + path_bytes;
        Some(Self {
            at: Instant::now(),
            width,
            path: raw.get(start..end)?.to_vec(),
            rssi_dbm: frame[2] as i8,
            payload: raw.get(end..)?.to_vec(),
            flooded: matches!(header & 3, 0 | 1),
            path_len: length,
        })
    }

    fn matches(&self, message: &Message<'_>, key: &[u8; 16]) -> bool {
        if if self.flooded {
            self.path_len != message.path_len
        } else {
            message.path_len != 0xff || !self.path.is_empty()
        } {
            return false;
        }
        let [hash, mac1, mac2, ciphertext @ ..] = self.payload.as_slice() else {
            return false;
        };
        if ciphertext.is_empty()
            || ciphertext.len() % 16 != 0
            || *hash != digest::digest(&digest::SHA256, key).as_ref()[0]
        {
            return false;
        }
        let tag = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), ciphertext);
        if (tag.as_ref()[0] ^ mac1) | (tag.as_ref()[1] ^ mac2) != 0 {
            return false;
        }
        let cipher = aes::Aes128::new(key.into());
        let mut plaintext = ciphertext.to_vec();
        for block in plaintext.as_chunks_mut::<16>().0 {
            cipher.decrypt_block(block.into());
        }
        plaintext[..4] == message.timestamp
            && plaintext[4] >> 2 == 0
            && plaintext[5..].split(|byte| *byte == 0).next() == Some(message.text.as_bytes())
    }

    pub fn label(&self) -> String {
        if self.flooded {
            path_label(self.width, &self.path)
        } else {
            "Direct route".into()
        }
    }
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

fn path_label(width: usize, path: &[u8]) -> String {
    if path.is_empty() {
        return "0 hops".into();
    }
    path.chunks_exact(width)
        .map(hex)
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Default)]
pub struct Observations(VecDeque<Reception>);

impl Observations {
    pub fn push(&mut self, reception: Reception) {
        self.0.retain(|row| row.at.elapsed() <= RAW_CACHE_AGE);
        if self.0.len() == 512 {
            self.0.pop_front();
        }
        self.0.push_back(reception);
    }

    pub fn find(&self, message: &Message<'_>, key: &[u8; 16]) -> Option<&Reception> {
        self.0
            .iter()
            .find(|row| row.at.elapsed() <= RAW_CACHE_AGE && row.matches(message, key))
    }
}

pub struct Multitest {
    pub route: Route,
    pub deadline: Instant,
    pub payload: Vec<u8>,
    pub paths: BTreeSet<String>,
}

impl Multitest {
    pub fn new(route: Route, reception: &Reception, observations: &Observations) -> Self {
        let mut session = Self {
            route,
            deadline: Instant::now() + MULTITEST_DURATION,
            payload: reception.payload.clone(),
            paths: BTreeSet::new(),
        };
        for row in &observations.0 {
            if row.at >= reception.at {
                session.observe(row);
            }
        }
        session
    }

    pub fn observe(&mut self, reception: &Reception) {
        if reception.at <= self.deadline
            && reception.payload == self.payload
            && self.paths.len() < 32
        {
            self.paths.insert(reception.label());
        }
    }

    pub fn reply(&self) -> String {
        let mut reply = format!("Multitest | {} observed paths", self.paths.len());
        append_items(&mut reply, self.paths.iter().map(String::as_str));
        reply
    }
}

fn append_items<'a>(reply: &mut String, items: impl IntoIterator<Item = &'a str>) {
    for item in items {
        if reply.len() + 3 + item.len() > 100 {
            reply.push_str(" …");
            break;
        }
        reply.push_str(" | ");
        reply.push_str(item);
    }
}

pub struct Trace {
    pub route: Route,
    pub deadline: Instant,
    pub tag: [u8; 4],
    pub result: Option<String>,
}

impl Trace {
    pub fn observe(&mut self, frame: &[u8]) {
        if self.result.is_some()
            || Instant::now() > self.deadline
            || frame.get(4..8) != Some(&self.tag)
            || frame.get(8..12) != Some(&[0; 4])
        {
            return;
        }
        let length = usize::from(frame[2]);
        let width = 1 << (frame[3] & 3);
        if length % width != 0 {
            return;
        }
        let count = length / width;
        let Some(snrs) = frame.get(12 + length..12 + length + count + 1) else {
            return;
        };
        let db = |snr: u8| f32::from(snr as i8) / 4.0;
        let values: Vec<_> = frame[12..12 + length]
            .chunks_exact(width)
            .zip(snrs)
            .map(|(hash, &snr)| format!("{}:{}", hex(hash), db(snr)))
            .chain(std::iter::once(format!("bot:{}", db(snrs[count]))))
            .collect();
        let mut result = format!("Trace {} | {count} hops | SNR dB", hex(&self.tag));
        append_items(&mut result, values.iter().map(String::as_str));
        self.result = Some(result);
    }
}

pub fn trace_path(argument: &str, reciprocal: bool) -> Option<(Vec<u8>, u8)> {
    let mut nodes = Vec::new();
    let width = argument.split(',').next()?.trim().len();
    if !matches!(width, 2 | 4 | 8) {
        return None;
    }
    for node in argument.split(',').map(str::trim) {
        if node.len() != width || !node.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        for offset in (0..width).step_by(2) {
            nodes.push(u8::from_str_radix(&node[offset..offset + 2], 16).ok()?);
        }
    }
    let bytes = width / 2;
    if reciprocal {
        let tail: Vec<_> = nodes
            .chunks_exact(bytes)
            .rev()
            .skip(1)
            .flatten()
            .copied()
            .collect();
        nodes.extend(tail);
    }
    if nodes.len() / bytes > 12 {
        return None;
    }
    Some((nodes, (bytes as u8).trailing_zeros() as u8))
}

#[derive(Default)]
struct Bucket {
    direct_messages: u32,
    channel_messages: BTreeMap<u8, u32>,
    commands: u32,
    direct_routes: u32,
    flooded: u32,
}

#[derive(Default)]
pub struct Stats {
    pub probes: std::sync::Arc<std::sync::Mutex<crate::probes::Probes>>,
    buckets: VecDeque<(Instant, Bucket)>,
}

impl Stats {
    pub fn record(&mut self, message: &Message<'_>, command: bool) {
        let now = Instant::now();
        while self
            .buckets
            .front()
            .is_some_and(|(start, _)| now.duration_since(*start) >= STATS_WINDOW)
        {
            self.buckets.pop_front();
        }
        if self
            .buckets
            .back()
            .is_none_or(|(start, _)| now.duration_since(*start) >= Duration::from_secs(60))
        {
            self.buckets.push_back((now, Bucket::default()));
        }
        let bucket = &mut self.buckets.back_mut().expect("a bucket was just added").1;
        match message.route {
            Route::Direct(_) => bucket.direct_messages += 1,
            Route::Channel(index) => *bucket.channel_messages.entry(index).or_default() += 1,
        }
        bucket.commands += u32::from(command);
        if message.path_len == 0xff {
            bucket.direct_routes += 1;
        } else {
            bucket.flooded += 1;
        }
    }

    pub fn reply(&self, argument: &str) -> String {
        let mut total = Bucket::default();
        for (_, bucket) in &self.buckets {
            total.direct_messages += bucket.direct_messages;
            total.commands += bucket.commands;
            total.direct_routes += bucket.direct_routes;
            total.flooded += bucket.flooded;
            for (&channel, &count) in &bucket.channel_messages {
                *total.channel_messages.entry(channel).or_default() += count;
            }
        }
        let channels: u32 = total.channel_messages.values().sum();
        match argument {
            "" => format!(
                "Stats 24h | {} messages | {} commands",
                channels + total.direct_messages,
                total.commands
            ),
            "messages" => format!(
                "Messages 24h | {} DMs | {channels} channel | {} commands",
                total.direct_messages, total.commands
            ),
            "channels" => {
                let items: Vec<_> = total
                    .channel_messages
                    .iter()
                    .map(|(index, count)| format!("#{index}: {count}"))
                    .collect();
                let mut reply = format!("Channels 24h | {channels} messages");
                append_items(&mut reply, items.iter().map(String::as_str));
                reply
            }
            "paths" => format!(
                "Paths 24h | {} direct routes | {} flooded",
                total.direct_routes, total.flooded
            ),
            _ => "Usage: stats [messages|channels|paths]".into(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Repeater {
    pub(crate) public_key: [u8; 32],
    prefix: u8,
    name: String,
    pub(crate) advertised_at: u32,
    location: Option<(i32, i32)>,
}

impl Repeater {
    pub fn parse(frame: &[u8]) -> Option<Self> {
        if *frame.get(33)? & 0x0f != 2 {
            return None;
        }
        let name = frame.get(100..132)?.split(|byte| *byte == 0).next()?;
        Some(Self {
            public_key: frame.get(1..33)?.try_into().ok()?,
            prefix: frame[1],
            name: String::from_utf8_lossy(name).trim().to_owned(),
            advertised_at: u32::from_le_bytes(frame.get(132..136)?.try_into().ok()?),
            location: coordinates(frame),
        })
    }

    pub fn from_raw(frame: &[u8]) -> Option<Self> {
        if frame.first() != Some(&0x88) {
            return None;
        }
        let raw = frame.get(3..)?;
        let header = *raw.first()?;
        if (header >> 2) & 15 != 4 {
            return None;
        }
        let offset = if matches!(header & 3, 0 | 3) { 5 } else { 1 };
        let length = *raw.get(offset)?;
        let width = usize::from((length >> 6) + 1);
        let size = width * usize::from(length & 63);
        if width == 4 || size > 64 {
            return None;
        }
        let advert = raw.get(offset + 1 + size..)?;
        let flags = *advert.get(100)?;
        if flags & 15 != 2 {
            return None;
        }
        let key: [u8; 32] = advert[..32].try_into().ok()?;
        let mut signed = advert[..36].to_vec();
        signed.extend_from_slice(&advert[100..]);
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, key)
            .verify(&signed, &advert[36..100])
            .ok()?;
        let mut index = 101;
        let location = if flags & 0x10 != 0 {
            let bytes = advert.get(index..index + 8)?;
            index += 8;
            Some((
                i32::from_le_bytes(bytes[..4].try_into().ok()?),
                i32::from_le_bytes(bytes[4..].try_into().ok()?),
            ))
        } else {
            None
        };
        index += if flags & 0x20 != 0 { 2 } else { 0 };
        index += if flags & 0x40 != 0 { 2 } else { 0 };
        let rest = advert.get(index..)?;
        let name = if flags & 0x80 != 0 {
            String::from_utf8_lossy(rest.split(|byte| *byte == 0).next()?).into_owned()
        } else {
            String::new()
        };
        Some(Self {
            public_key: key,
            prefix: key[0],
            name,
            advertised_at: u32::from_le_bytes(advert[32..36].try_into().ok()?),
            location,
        })
    }

    fn age(&self, now: u64) -> Option<u64> {
        (self.advertised_at != 0).then(|| now.saturating_sub(u64::from(self.advertised_at)))
    }
}

fn coordinates(frame: &[u8]) -> Option<(i32, i32)> {
    let lat = i32::from_le_bytes(frame.get(136..140)?.try_into().ok()?);
    let lon = i32::from_le_bytes(frame.get(140..144)?.try_into().ok()?);
    ((lat != 0 || lon != 0)
        && lat.unsigned_abs() <= 90_000_000
        && lon.unsigned_abs() <= 180_000_000)
        .then_some((lat, lon))
}

fn prefix_filter(argument: &str) -> Option<(u8, bool)> {
    let mut parts = argument.split_whitespace();
    let prefix = parts
        .next()
        .filter(|value| value.len() == 2 && value.bytes().all(|b| b.is_ascii_hexdigit()))
        .and_then(|value| u8::from_str_radix(value, 16).ok())?;
    let include_all = match parts.next() {
        None => false,
        Some("all") if parts.next().is_none() => true,
        _ => return None,
    };
    Some((prefix, include_all))
}

pub fn valid_prefix_argument(argument: &str) -> bool {
    matches!(argument, "refresh" | "free" | "available") || prefix_filter(argument).is_some()
}

pub fn prefix_reply(contacts: &[Repeater], argument: &str) -> String {
    if argument == "refresh" {
        return format!("Radio contacts | {} repeaters", contacts.len());
    }
    if matches!(argument, "free" | "available") {
        let used: BTreeSet<_> = contacts.iter().map(|contact| contact.prefix).collect();
        let sample: Vec<_> = (0..=255)
            .filter(|prefix| !used.contains(prefix))
            .take(12)
            .map(|prefix| format!("{prefix:02X}"))
            .collect();
        return format!(
            "{} unused in radio contacts | {}",
            256 - used.len(),
            sample.join(",")
        );
    }
    let Some((prefix, include_all)) = prefix_filter(argument) else {
        return "Usage: prefix XX [all] | prefix free | prefix refresh".into();
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut names: Vec<_> = contacts
        .iter()
        .filter(|contact| contact.prefix == prefix)
        .filter_map(|contact| {
            let age = contact.age(now);
            let active = age.is_some_and(|age| age < 7 * 86_400);
            if !active && !include_all {
                return None;
            }
            let name = if contact.name.is_empty() {
                "(unnamed)"
            } else {
                &contact.name
            };
            let seen = match age {
                Some(age) if age < 3_600 => format!("{}m ago", age / 60),
                Some(age) if age < 86_400 => format!("{}h ago", age / 3_600),
                Some(age) => format!("{}d ago", age / 86_400),
                None => "last heard unknown".into(),
            };
            let location = contact.location.map_or_else(String::new, |(lat, lon)| {
                format!(
                    ", {:.3},{:.3}",
                    f64::from(lat) / 1_000_000.0,
                    f64::from(lon) / 1_000_000.0
                )
            });
            Some(format!("{name} ({seen}{location})"))
        })
        .collect();
    names.sort_unstable();
    let mut reply = format!("Prefix {prefix:02X} | {} radio repeaters", names.len());
    append_items(&mut reply, names.iter().map(String::as_str));
    reply
}

#[derive(Default, serde::Serialize)]
pub struct RepeaterStats {
    pub tracked_repeaters: Vec<[u8; 32]>,
    pub updated_at: Option<String>,
    pub total: usize,
    pub active_24h: usize,
    pub active_7d: usize,
    pub stale: usize,
    pub unknown_last_heard: usize,
    pub with_location: usize,
    pub colliding_prefixes: usize,
    pub repeaters: Vec<Repeater>,
}

impl RepeaterStats {
    pub fn from_contacts(repeaters: &[Repeater]) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut prefixes = BTreeMap::<u8, usize>::new();
        let mut result = Self {
            updated_at: Some(chrono::Utc::now().to_rfc3339()),
            total: repeaters.len(),
            repeaters: repeaters.to_vec(),
            ..Self::default()
        };
        for repeater in repeaters {
            *prefixes.entry(repeater.prefix).or_default() += 1;
            match repeater.age(now) {
                Some(age) => {
                    result.active_24h += usize::from(age < 86_400);
                    result.active_7d += usize::from(age < 7 * 86_400);
                    result.stale += usize::from(age >= 7 * 86_400);
                }
                None => result.unknown_last_heard += 1,
            }
            result.with_location += usize::from(repeater.location.is_some());
        }
        result.colliding_prefixes = prefixes.values().filter(|&&count| count > 1).count();
        result
    }

    pub fn observe(&mut self, repeater: Repeater) {
        let tracked = self.tracked_repeaters.clone();
        let mut repeaters = self.repeaters.clone();
        if let Some(existing) = repeaters
            .iter_mut()
            .find(|old| old.public_key == repeater.public_key)
        {
            if existing.advertised_at > repeater.advertised_at {
                return;
            }
            *existing = repeater;
        } else {
            if repeaters.len() >= 4096 {
                return;
            }
            repeaters.push(repeater);
        }
        *self = Self::from_contacts(&repeaters);
        self.tracked_repeaters = tracked;
    }

    pub fn reply(&self) -> String {
        if !self.tracked_repeaters.is_empty() {
            let selected: Vec<_> = self
                .repeaters
                .iter()
                .filter(|repeater| self.tracked_repeaters.contains(&repeater.public_key))
                .cloned()
                .collect();
            let stats = Self::from_contacts(&selected);
            return format!(
                "Tracked {}/{} | 24h {} | 7d {} | stale {} | GPS {}",
                stats.total,
                self.tracked_repeaters.len(),
                stats.active_24h,
                stats.active_7d,
                stats.stale,
                stats.with_location
            );
        }
        format!(
            "RP {} | 24h {} | 7d {} | stale {} | unknown {} | GPS {} | prefix collisions {}",
            self.total,
            self.active_24h,
            self.active_7d,
            self.stale,
            self.unknown_last_heard,
            self.with_location,
            self.colliding_prefixes
        )
    }
}

pub fn repeater_status(repeaters: &[Repeater], contacts: usize, argument: &str) -> String {
    match argument.to_ascii_lowercase().as_str() {
        "stats" => RepeaterStats::from_contacts(repeaters).reply(),
        "" | "status" | "scan" => format!(
            "Radio contacts | {contacts} total | {} repeaters",
            repeaters.len()
        ),
        "list" => {
            let mut names: Vec<_> = repeaters
                .iter()
                .map(|repeater| {
                    let name = if repeater.name.is_empty() {
                        "(unnamed)"
                    } else {
                        &repeater.name
                    };
                    format!("{:02X} {name}", repeater.prefix)
                })
                .collect();
            names.sort_unstable();
            let mut reply = format!("Radio repeaters | {}", repeaters.len());
            append_items(&mut reply, names.iter().map(String::as_str));
            reply
        }
        _ => "Usage: repeater [scan|status|list|stats]".into(),
    }
}

#[cfg(test)]
mod repeater_tests {
    use super::*;

    #[test]
    fn classifies_contact_ages_and_counts_colliding_prefixes() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        let make = |prefix, age: Option<u32>| Repeater {
            public_key: [prefix; 32],
            prefix,
            name: "Hill".into(),
            advertised_at: age.map_or(0, |age| now - age),
            location: Some((0, 1)),
        };
        let stats = RepeaterStats::from_contacts(&[
            make(1, Some(10)),
            make(1, Some(2 * 86_400)),
            make(2, Some(8 * 86_400)),
            make(3, None),
        ]);
        assert_eq!(stats.total, 4);
        assert_eq!(stats.active_24h, 1);
        assert_eq!(stats.active_7d, 2);
        assert_eq!(stats.stale, 1);
        assert_eq!(stats.unknown_last_heard, 1);
        assert_eq!(stats.colliding_prefixes, 1);
    }
}
