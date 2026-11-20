use crate::protocol::Route;
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

const RUN_IDLE_LIMIT: Duration = Duration::from_secs(7_200);
const MAX_RUNS: usize = 32;
pub const USAGE: &str = "Usage: probe RUN 1/20 | probe stats RUN (up to 100 packets)";

pub enum Request<'a> {
    Sample {
        run: &'a str,
        sequence: u8,
        total: u8,
    },
    Summary {
        run: &'a str,
    },
}

pub fn parse(argument: &str) -> Option<Request<'_>> {
    let mut parts = argument.split_whitespace();
    let first = parts.next()?;
    let second = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let valid_run = |run: &str| {
        !run.is_empty()
            && run.len() <= 16
            && run
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    };
    if first.eq_ignore_ascii_case("stats") && valid_run(second) {
        return Some(Request::Summary { run: second });
    }
    if !valid_run(first) || first.eq_ignore_ascii_case("stats") {
        return None;
    }
    let (sequence, total) = second.split_once('/')?;
    let sequence = sequence.parse().ok()?;
    let total = total.parse().ok()?;
    (sequence >= 1 && sequence <= total && total <= 100).then_some(Request::Sample {
        run: first,
        sequence,
        total,
    })
}

#[derive(Copy, Clone, Serialize)]
pub struct Signal {
    pub snr: Option<f32>,
    pub rssi: Option<i8>,
}

#[derive(Clone, Serialize)]
pub struct Sample {
    pub sequence: u8,
    #[serde(flatten)]
    pub signal: Signal,
    pub received_at: String,
    pub path: String,
}

#[derive(Serialize)]
pub struct RunSummary {
    id: u64,
    name: String,
    route: String,
    total: u8,
    received: usize,
    duplicates: u32,
    median_snr: Option<f32>,
    median_rssi: Option<f32>,
}

#[derive(Serialize)]
pub struct RunDetail {
    #[serde(flatten)]
    summary: RunSummary,
    samples: Vec<Sample>,
}

struct Run {
    id: u64,
    route: Route,
    name: String,
    total: u8,
    last_seen: Instant,
    samples: BTreeMap<u8, Sample>,
    duplicates: u32,
}

#[derive(Default)]
pub struct Probes {
    runs: VecDeque<Run>,
    next_id: u64,
}

impl Run {
    fn summary(&self) -> RunSummary {
        fn median(mut values: Vec<f32>) -> Option<f32> {
            if values.is_empty() {
                return None;
            }
            values.sort_unstable_by(f32::total_cmp);
            Some((values[values.len() / 2] + values[(values.len() - 1) / 2]) / 2.0)
        }
        RunSummary {
            id: self.id,
            name: self.name.clone(),
            route: self.route.to_string(),
            total: self.total,
            received: self.samples.len(),
            duplicates: self.duplicates,
            median_snr: median(
                self.samples
                    .values()
                    .filter_map(|sample| sample.signal.snr)
                    .collect(),
            ),
            median_rssi: median(
                self.samples
                    .values()
                    .filter_map(|sample| sample.signal.rssi.map(f32::from))
                    .collect(),
            ),
        }
    }

    fn is_current(&self) -> bool {
        self.last_seen.elapsed() < RUN_IDLE_LIMIT
    }
}

impl Probes {
    pub fn record(
        &mut self,
        route: Route,
        name: &str,
        total: u8,
        sample: Sample,
    ) -> Result<bool, &'static str> {
        self.runs.retain(Run::is_current);
        let index = match self
            .runs
            .iter()
            .position(|run| run.route == route && run.name == name)
        {
            Some(index) => index,
            None => {
                if self.runs.len() == MAX_RUNS {
                    let oldest = self
                        .runs
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, run)| run.last_seen)
                        .map(|(index, _)| index)
                        .unwrap();
                    self.runs.remove(oldest);
                }
                self.next_id += 1;
                self.runs.push_back(Run {
                    id: self.next_id,
                    route,
                    name: name.into(),
                    total,
                    last_seen: Instant::now(),
                    samples: BTreeMap::new(),
                    duplicates: 0,
                });
                self.runs.len() - 1
            }
        };
        let run = &mut self.runs[index];
        if run.total != total {
            return Err("Probe total differs from this run's original total. Use a new run ID.");
        }
        run.last_seen = Instant::now();
        match run.samples.entry(sample.sequence) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(sample);
                Ok(false)
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                run.duplicates = run.duplicates.saturating_add(1);
                Ok(true)
            }
        }
    }

    pub fn summary(&self, route: Route, name: &str) -> String {
        let Some(run) = self
            .runs
            .iter()
            .find(|run| run.route == route && run.name == name && run.is_current())
        else {
            return format!("Probe {name} | no samples received");
        };
        let RunSummary {
            received,
            median_snr: snr,
            median_rssi: rssi,
            ..
        } = run.summary();
        let percent = 100.0 * received as f64 / f64::from(run.total);
        let mut reply = format!(
            "{name} | RX {received}/{} ({percent:.0}%) | dup {}",
            run.total, run.duplicates
        );
        if let Some(snr) = snr {
            reply.push_str(&format!(" | SNR med {snr:.1}dB"));
        }
        if let Some(rssi) = rssi {
            reply.push_str(&format!(" | RSSI med {rssi:.1}dBm"));
        }
        reply
    }

    pub fn summaries(&self) -> Vec<RunSummary> {
        let mut current: Vec<_> = self.runs.iter().filter(|run| run.is_current()).collect();
        current.sort_unstable_by_key(|run| std::cmp::Reverse(run.last_seen));
        current.into_iter().map(Run::summary).collect()
    }

    pub fn detail(&self, id: u64) -> Option<RunDetail> {
        let run = self
            .runs
            .iter()
            .find(|run| run.id == id && run.is_current())?;
        Some(RunDetail {
            summary: run.summary(),
            samples: run.samples.values().cloned().collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_counts_unique_sequences_and_uses_the_first_signal_sample() {
        let mut probes = Probes::default();
        let route = Route::Channel(0);
        let first = Signal {
            snr: Some(-2.0),
            rssi: Some(-100),
        };
        let second = Signal {
            snr: Some(4.0),
            rssi: Some(-90),
        };
        let sample = |sequence, signal| Sample {
            sequence,
            signal,
            received_at: "2026-11-15T20:15:00Z".into(),
            path: "AB".into(),
        };
        assert_eq!(
            probes.record(route, "point-a", 4, sample(1, first)),
            Ok(false)
        );
        assert_eq!(
            probes.record(route, "point-a", 4, sample(3, second)),
            Ok(false)
        );
        assert_eq!(
            probes.record(route, "point-a", 4, sample(1, second)),
            Ok(true)
        );
        assert!(probes
            .record(route, "point-a", 5, sample(2, second))
            .is_err());
        assert_eq!(
            probes.summary(route, "point-a"),
            "point-a | RX 2/4 (50%) | dup 1 | SNR med 1.0dB | RSSI med -95.0dBm"
        );
        assert_eq!(
            probes.summary(Route::Channel(1), "point-a"),
            "Probe point-a | no samples received"
        );
    }
}
