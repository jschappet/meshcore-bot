use crate::protocol::{Message, Route};
use actix_web::{web, HttpResponse, Scope};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
#[cfg(test)]
use std::sync::Mutex;

const CAPACITY: usize = 1000;

#[derive(Clone, Serialize)]
struct ChannelMessage {
    channel: u8,
    received_at: String,
    sender_timestamp: u32,
    text: String,
    snr_db: Option<f32>,
    route: String,
}

#[derive(Default)]
pub struct Channels {
    messages: VecDeque<ChannelMessage>,
    names: BTreeMap<u8, String>,
    senders: BTreeMap<String, Vec<String>>,
}

impl Channels {
    pub fn has_channel(&self, index: u8) -> bool {
        self.names.contains_key(&index)
    }

    pub fn set_channel(&mut self, index: u8, name: String) {
        self.names.insert(index, name);
    }

    pub fn set_contacts(&mut self, contacts: &[Vec<u8>]) {
        self.senders.clear();
        for contact in contacts {
            let Some(key) = contact.get(1..33) else {
                continue;
            };
            let Some(name) = contact.get(100..132) else {
                continue;
            };
            let name =
                String::from_utf8_lossy(name.split(|byte| *byte == 0).next().unwrap_or_default())
                    .trim()
                    .to_owned();
            if !name.is_empty() {
                let keys = self.senders.entry(name).or_default();
                let key = crate::diagnostics::hex(key);
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
        }
    }

    pub fn observe(&mut self, message: &Message<'_>) {
        let Route::Channel(channel) = message.route else {
            return;
        };
        if self.messages.len() == CAPACITY {
            self.messages.pop_front();
        }
        self.messages.push_back(ChannelMessage {
            channel,
            received_at: chrono::Utc::now().to_rfc3339(),
            sender_timestamp: u32::from_le_bytes(message.timestamp),
            text: message.text.to_owned(),
            snr_db: message.snr_db,
            route: crate::route_label(message.path_len),
        });
    }
}

pub fn scope() -> Scope {
    web::scope("/channels").route("", web::get().to(messages))
}

#[derive(Deserialize)]
struct Query {
    channel: Option<u8>,
}

async fn messages(state: web::Data<super::ViewerState>, query: web::Query<Query>) -> HttpResponse {
    let channels: std::sync::MutexGuard<'_, Channels> = state.channels.lock().unwrap();
    let slots: std::collections::BTreeSet<_> = channels
        .messages
        .iter()
        .map(|message| message.channel)
        .collect();
    let messages: Vec<_> = channels
        .messages
        .iter()
        .rev()
        .filter(|message| {
            query
                .channel
                .is_none_or(|channel| channel == message.channel)
        })
        .take(300)
        .cloned()
        .collect();
    let messages: Vec<_> = messages
        .into_iter()
        .map(|message| {
            let sender = message
                .text
                .split_once(':')
                .map(|(sender, _)| sender.trim());
            let candidates = sender
                .and_then(|name| channels.senders.get(name))
                .cloned()
                .unwrap_or_default();
            serde_json::json!({
                "channel_name": channels.names.get(&message.channel),
                "sender_name": sender,
                "sender_key_candidates": candidates,
                "channel": message.channel, "received_at": message.received_at,
                "sender_timestamp": message.sender_timestamp, "text": message.text,
                "snr_db": message.snr_db, "route": message.route,
            })
        })
        .collect();
    HttpResponse::Ok().json(
        serde_json::json!({"channels":slots, "channel_names":channels.names, "messages":messages}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    use std::sync::Arc;

    #[actix_web::test]
    async fn retains_channel_traffic_filters_by_slot_and_excludes_dms() {
        let mut channels = Channels::default();
        let mut frame = b"\x08\x03\x01\0\0\0\0\0Node: hello".to_vec();
        for _ in 0..1002 {
            channels.observe(&crate::protocol::message(&frame).unwrap());
        }
        frame[1] = 4;
        channels.observe(&crate::protocol::message(&frame).unwrap());
        let dm = crate::protocol::message(b"\x07123456\xff\0\0\0\0\0private").unwrap();
        channels.observe(&dm);
        assert_eq!(channels.messages.len(), CAPACITY);
        channels.set_channel(4, "#antenna".into());
        let mut contact = vec![0; 148];
        contact[1..33].fill(0xab);
        contact[100..104].copy_from_slice(b"Node");
        channels.set_contacts(&[contact]);
        let state = web::Data::new(super::super::ViewerState {
            channels: Arc::new(Mutex::new(channels)),
            radio: Mutex::new(super::super::RadioStatus::Connected),
            started: std::time::Instant::now(),
            probes: Arc::default(),
            repeaters: Arc::default(),
        });
        let app = test::init_service(
            App::new()
                .app_data(state)
                .service(web::scope("/api").service(scope())),
        )
        .await;
        let body: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/channels?channel=4")
                .to_request(),
        )
        .await;
        assert_eq!(body["channels"], serde_json::json!([3, 4]));
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
        assert_eq!(body["messages"][0]["text"], "Node: hello");
        assert_eq!(body["messages"][0]["channel_name"], "#antenna");
        assert_eq!(
            body["messages"][0]["sender_key_candidates"][0],
            "AB".repeat(32)
        );
    }
}
