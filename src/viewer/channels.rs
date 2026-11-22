use crate::protocol::{Message, Route};
use actix_web::{web, HttpResponse, Scope};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
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
}

impl Channels {
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
    HttpResponse::Ok().json(serde_json::json!({"channels":slots, "messages":messages}))
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
    }
}
