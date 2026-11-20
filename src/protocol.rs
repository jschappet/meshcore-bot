// Serial framing adapted from flood_monitor (CC0 1.0).
use std::fmt;
use std::io::{self, Read, Write};

pub const MAX_FRAME: usize = 176;

pub fn send(writer: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    if payload.is_empty() || payload.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid command length",
        ));
    }
    writer.write_all(&[b'<', payload.len() as u8, 0])?;
    writer.write_all(payload)?;
    writer.flush()
}

/// Retain partial headers/payloads across serial read timeouts.
#[derive(Default)]
pub struct Frames {
    buffer: Vec<u8>,
}
impl Frames {
    pub fn read(&mut self, reader: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
        loop {
            if let Some(start) = self.buffer.iter().position(|b| *b == b'>') {
                self.buffer.drain(..start);
                if self.buffer.len() >= 3 {
                    let len = u16::from_le_bytes([self.buffer[1], self.buffer[2]]) as usize;
                    if len == 0 || len > MAX_FRAME {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid MeshCore frame length",
                        ));
                    }
                    if self.buffer.len() >= len + 3 {
                        let frame = self.buffer[3..len + 3].to_vec();
                        self.buffer.drain(..len + 3);
                        return Ok(Some(frame));
                    }
                }
            } else {
                self.buffer.clear();
            }
            let mut bytes = [0u8; 512];
            match reader.read(&mut bytes) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "radio disconnected",
                    ));
                }
                Ok(n) => self.buffer.extend_from_slice(&bytes[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                    ) =>
                {
                    return Ok(None);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

pub const ERROR: u8 = 0x01;
pub const SELF_INFO: u8 = 0x05;
pub const MESSAGE_SENT: u8 = 0x06;
pub const QUEUE_EMPTY: u8 = 0x0a;
pub const DEVICE_INFO: u8 = 0x0d;
pub const MESSAGES_WAITING: u8 = 0x83;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Direct([u8; 6]),
    Channel(u8),
}

impl fmt::Display for Route {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct(_) => formatter.write_str("DM"),
            Self::Channel(index) => write!(formatter, "channel #{index}"),
        }
    }
}

impl Route {
    pub fn reply(&self, timestamp: u32, text: &str) -> (Vec<u8>, u8) {
        let (mut frame, response) = match self {
            Self::Direct(_) => (vec![0x02, 0, 0], MESSAGE_SENT),
            Self::Channel(index) => (vec![0x03, 0, *index], 0x00),
        };
        frame.extend_from_slice(&timestamp.to_le_bytes());
        if let Self::Direct(prefix) = self {
            frame.extend_from_slice(prefix);
        }
        frame.extend_from_slice(text.as_bytes());
        (frame, response)
    }
}

pub struct Message<'a> {
    pub route: Route,
    pub text: &'a str,
    pub path_len: u8,
    pub text_type: u8,
    pub timestamp: [u8; 4],
    pub snr_db: Option<f32>,
}

pub fn message(frame: &[u8]) -> Option<Message<'_>> {
    // The protocol v3 receive frames insert SNR and two reserved bytes.
    let kind = *frame.first()?;
    let (route, metadata) = match kind {
        0x07 => (Route::Direct(frame.get(1..7)?.try_into().ok()?), 7),
        0x08 => (Route::Channel(*frame.get(1)?), 2),
        0x10 => (Route::Direct(frame.get(4..10)?.try_into().ok()?), 10),
        0x11 => (Route::Channel(*frame.get(4)?), 5),
        _ => return None,
    };
    let text_type = *frame.get(metadata + 1)?;
    let text_offset = match (&route, text_type) {
        (_, 0) => metadata + 6,
        (Route::Direct(_), 2) => metadata + 10,
        _ => return None,
    };
    let text_bytes = frame.get(text_offset..)?.split(|byte| *byte == 0).next()?;
    Some(Message {
        route,
        text: std::str::from_utf8(text_bytes).ok()?,
        path_len: frame[metadata],
        text_type,
        timestamp: frame[metadata + 2..metadata + 6].try_into().ok()?,
        snr_db: matches!(kind, 0x10 | 0x11).then(|| f32::from(frame[1] as i8) / 4.0),
    })
}
