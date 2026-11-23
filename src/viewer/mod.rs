pub mod channels;
use crate::probes::Probes;
use actix_web::{middleware, web, App, HttpResponse, HttpServer, Scope};
use serde::Serialize;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RadioStatus {
    Connecting,
    Connected,
    Reconnecting,
}

pub struct ViewerState {
    verification: Option<crate::skagit::Verification>,
    pub channels: Arc<Mutex<channels::Channels>>,
    radio: Mutex<RadioStatus>,
    started: Instant,
    probes: Arc<Mutex<Probes>>,
    pub repeaters: Arc<Mutex<crate::diagnostics::RepeaterStats>>,
}

impl ViewerState {
    pub fn set_radio(&self, status: RadioStatus) {
        *self.radio.lock().unwrap() = status;
    }
}

#[derive(Serialize)]
struct Health {
    radio: RadioStatus,
    uptime_seconds: u64,
}

pub fn start(
    address: SocketAddr,
    probes: Arc<Mutex<Probes>>,
    repeaters: Arc<Mutex<crate::diagnostics::RepeaterStats>>,
    channels: Arc<Mutex<channels::Channels>>,
    verification: Option<crate::skagit::Verification>,
) -> io::Result<web::Data<ViewerState>> {
    let listener = TcpListener::bind(address).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("Web Viewer could not listen on {address}: {error}"),
        )
    })?;
    let state = web::Data::new(ViewerState {
        radio: Mutex::new(RadioStatus::Connecting),
        started: Instant::now(),
        probes,
        repeaters,
        channels,
        verification,
    });
    let server_state = state.clone();
    std::thread::Builder::new()
        .name("web-viewer".into())
        .spawn(move || {
            let result = actix_web::rt::System::new().block_on(async move {
                HttpServer::new(move || {
                    App::new()
                        .wrap(middleware::DefaultHeaders::new().add(("Cache-Control", "no-store")))
                        .app_data(server_state.clone())
                        .configure(configure)
                })
                .workers(1)
                .disable_signals()
                .listen(listener)?
                .run()
                .await
            });
            if let Err(error) = result {
                log::error!("Web Viewer stopped: {error}");
            }
        })?;
    log::info!("Web Viewer listening on {address}");
    Ok(state)
}

fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("/").route(web::get().to(dashboard)))
        .service(web::resource("/repeaters/map").route(web::get().to(repeater_map)))
        .service(api_scope("/api"));
}

async fn repeater_map() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("map.html"))
}

fn api_scope(path: &'static str) -> Scope {
    web::scope(path)
        .route("/health", web::get().to(health))
        .service(probe_scope())
        .service(channels::scope())
        .service(web::scope("/verifications").route("", web::get().to(verifications)))
        .service(web::scope("/repeaters").route("", web::get().to(repeater_stats)))
}

async fn verifications(state: web::Data<ViewerState>) -> HttpResponse {
    match &state.verification {
        Some(queue) => match queue.statuses() {
            Ok(rows) => {
                HttpResponse::Ok().json(serde_json::json!({"enabled":true, "requests":rows}))
            }
            Err(error) => {
                log::error!("Could not read verification status: {error}");
                HttpResponse::InternalServerError().finish()
            }
        },
        None => HttpResponse::Ok().json(serde_json::json!({"enabled":false, "requests":[]})),
    }
}

fn probe_scope() -> Scope {
    web::scope("/probes")
        .route("", web::get().to(probe_runs))
        .route("/{id}", web::get().to(probe_detail))
}

async fn dashboard() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("index.html"))
}

async fn health(state: web::Data<ViewerState>) -> web::Json<Health> {
    web::Json(Health {
        radio: *state.radio.lock().unwrap(),
        uptime_seconds: state.started.elapsed().as_secs(),
    })
}

async fn repeater_stats(state: web::Data<ViewerState>) -> HttpResponse {
    HttpResponse::Ok().json(&*state.repeaters.lock().unwrap())
}

async fn probe_runs(state: web::Data<ViewerState>) -> web::Json<Vec<crate::probes::RunSummary>> {
    web::Json(state.probes.lock().unwrap().summaries())
}

async fn probe_detail(state: web::Data<ViewerState>, id: web::Path<u64>) -> HttpResponse {
    match state.probes.lock().unwrap().detail(id.into_inner()) {
        Some(run) => HttpResponse::Ok().json(run),
        None => HttpResponse::NotFound().finish(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        probes::{Sample, Signal},
        protocol::Route,
    };
    use actix_web::{http::StatusCode, test};

    #[actix_web::test]
    async fn serves_health_and_current_probe_data_under_api_scope() {
        let state = web::Data::new(ViewerState {
            verification: None,
            channels: Arc::default(),
            radio: Mutex::new(RadioStatus::Connecting),
            started: Instant::now(),
            probes: Arc::new(Mutex::new(Probes::default())),
            repeaters: Arc::new(Mutex::new(crate::diagnostics::RepeaterStats::default())),
        });
        let app = test::init_service(App::new().app_data(state.clone()).configure(configure)).await;
        let page = test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;
        assert_eq!(page.status(), StatusCode::OK);
        let health = test::TestRequest::get().uri("/api/health");
        let health_response: serde_json::Value =
            test::call_and_read_body_json(&app, health.to_request()).await;
        assert_eq!(health_response["radio"], "connecting");
        state.set_radio(RadioStatus::Reconnecting);

        state
            .probes
            .lock()
            .unwrap()
            .record(
                Route::Channel(3),
                "hill-a",
                20,
                Sample {
                    sequence: 7,
                    signal: Signal {
                        snr: Some(4.5),
                        rssi: Some(-99),
                    },
                    received_at: "2026-11-15T20:15:00Z".into(),
                    path: "AB,CD".into(),
                },
            )
            .unwrap();
        let runs: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/api/probes").to_request(),
        )
        .await;
        assert_eq!(runs[0]["received"], 1);
        assert_eq!(runs[0]["total"], 20);
        assert_eq!(runs[0]["median_rssi"], -99.0);
        let uri = format!("/api/probes/{}", runs[0]["id"]);
        let detail: serde_json::Value =
            test::call_and_read_body_json(&app, test::TestRequest::get().uri(&uri).to_request())
                .await;
        assert_eq!(detail["samples"][0]["sequence"], 7);
        assert_eq!(detail["samples"][0]["path"], "AB,CD");

        let missing = test::call_service(
            &app,
            test::TestRequest::get().uri("/api/probes/999").to_request(),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let health_response: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/api/health").to_request(),
        )
        .await;
        assert_eq!(health_response["radio"], "reconnecting");
    }
}
