// DAEMON-ORIGINAL (like hooks.rs/events_bridge.rs) — no desktop twin; the
// desktop pairs implicitly through the logged-in account and never serves this
// surface, so there is nothing to converge with.
//
//! Local pairing surface for Qobuz Connect: mDNS advertisement + the three
//! `/streamcore` HTTP endpoints the Qobuz app calls when a user picks this
//! device in the output selector.
//!
//! Protocol (verified against the qobuz-proxy and StreamCore32 receivers):
//!   - the device advertises `_qobuz-connect._tcp.local.` with TXT properties
//!     `path=/streamcore`, `type=SPEAKER`, `sdk_version`, `Name`, `device_uuid`;
//!   - the app then calls `GET /streamcore/get-display-info` (picker metadata),
//!     `GET /streamcore/get-connect-info` (`{current_session_id, app_id}`), and
//!     finally `POST /streamcore/connect-to-qconnect` with
//!     `{session_id, jwt_qconnect: {jwt, exp, endpoint}, jwt_api: {jwt, exp}}`.
//!
//! The handed-over `jwt_qconnect` is the SAME credential class the account path
//! obtains from `/qws/createToken` (`WsTransportConfig.jwt_qws` + endpoint), but
//! minted for the CASTER's session — so any Qobuz account on the LAN can hand
//! this device a session, with last-write-wins takeover semantics. The tokens
//! land in the in-memory [`PairingStore`]; `DaemonQconnectService::connect`
//! prefers a live pairing token over `/qws/createToken` discovery.
//!
//! P0 limits (hybrid mode): `jwt_api` is stored but NOT yet used — stream URLs
//! still come from the daemon's logged-in account (`get_stream_url`), so a
//! login-free device needs the follow-up qbz-qobuz Bearer work. Tokens are
//! never persisted (a daemon restart just waits for the next handoff POST) and
//! never logged (registered with the qbz-log redactor on receipt).
//!
//! This is a SEPARATE tiny_http listener from the `/api` control plane: the
//! Qobuz app is an unauthenticated LAN client, so it must not be subject to
//! the control plane's Origin shield / opt-in Bearer gate — and the pairing
//! surface must not widen the control plane's attack surface either.

use std::io::Cursor;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{SystemTime, UNIX_EPOCH};

use mdns_sd::{ServiceDaemon, ServiceInfo};
use qconnect_transport_ws::WsTransportConfig;
use serde_json::{json, Value};
use tiny_http::{Method, Request, Response, Server};

use super::transport::{default_qconnect_device_info, resolve_qconnect_device_uuid};
use super::DaemonQconnectService;
// The response formatter is shared with the control plane deliberately: it is
// a pure serializer, so reusing it does not couple the two listeners.
use crate::api::json;

pub const MDNS_SERVICE_TYPE: &str = "_qobuz-connect._tcp.local.";
const SDK_VERSION: &str = concat!("qbz-", env!("CARGO_PKG_VERSION"));
/// Treat a token expiring within this window as already expired (clock skew +
/// time to finish the WS handshake).
const EXP_SLACK_SECS: u64 = 60;

/// Tokens handed over by the Qobuz app in `connect-to-qconnect`. `exp` values
/// are unix seconds as sent on the wire; `0` = not provided (treated as
/// non-expiring — nothing in this repo decodes JWTs to find out more).
#[derive(Clone)]
pub struct PairingTokens {
    pub session_id: String,
    pub ws_jwt: String,
    pub ws_exp: u64,
    pub ws_endpoint: String,
    /// Stored for the login-free follow-up; unused in hybrid mode.
    #[allow(dead_code)]
    pub api_jwt: Option<String>,
    #[allow(dead_code)]
    pub api_exp: u64,
}

impl PairingTokens {
    fn ws_token_live(&self, now: u64) -> bool {
        self.ws_exp == 0 || self.ws_exp > now + EXP_SLACK_SECS
    }
}

/// The redactor covers registered values in LOG lines, but a `{:?}` in a panic
/// message or `dbg!` bypasses it — so Debug elides the raw JWTs entirely.
impl std::fmt::Debug for PairingTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PairingTokens")
            .field("session_id", &self.session_id)
            .field("ws_jwt", &"<redacted>")
            .field("ws_exp", &self.ws_exp)
            .field("ws_endpoint", &self.ws_endpoint)
            .field("api_jwt", &self.api_jwt.as_ref().map(|_| "<redacted>"))
            .field("api_exp", &self.api_exp)
            .finish()
    }
}

/// Last handed-over tokens, shared between the pairing listener (writer) and
/// the connect/reconnect paths (readers). In-memory only — see module doc.
pub type PairingStore = Arc<StdMutex<Option<PairingTokens>>>;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A clone of the stored tokens if their WS credential is still usable.
/// Expired tokens are dropped from the store on the way out, so the connect
/// path falls back to `/qws/createToken` instead of retrying a dead JWT.
pub fn valid_ws_tokens(store: &PairingStore) -> Option<PairingTokens> {
    let mut guard = store.lock().ok()?;
    match guard.as_ref() {
        Some(tokens) if tokens.ws_token_live(now_secs()) => Some(tokens.clone()),
        Some(_) => {
            log::info!("[QConnect/Pairing] handed-over WS token expired; dropping it");
            *guard = None;
            None
        }
        None => None,
    }
}

/// Build the WS transport config from handed-over tokens. Field-for-field the
/// hardened defaults `resolve_transport_config` uses (require_jwt, 60s idle
/// retry, the default connectionId/backend/controllers channels).
pub fn transport_config_from(tokens: &PairingTokens) -> WsTransportConfig {
    let mut config = WsTransportConfig::default();
    config.endpoint_url = tokens.ws_endpoint.clone();
    config.jwt_qws = Some(tokens.ws_jwt.clone());
    config.require_jwt = true;
    config.reconnect_idle_retry_ms = 60_000;
    config.subscribe_channels = vec![vec![0x01], vec![0x02], vec![0x03]];
    config
}

/// Parse a `connect-to-qconnect` body. Only the WS connect token is mandatory:
/// without `{jwt, endpoint}` there is nothing to connect to.
fn parse_connect_request(body: &Value) -> Result<PairingTokens, String> {
    let session_id = body
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    let ws = body
        .get("jwt_qconnect")
        .ok_or_else(|| "missing jwt_qconnect".to_string())?;
    let ws_jwt = ws
        .get("jwt")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "missing jwt_qconnect.jwt".to_string())?
        .to_string();
    let ws_endpoint = ws
        .get("endpoint")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "missing jwt_qconnect.endpoint".to_string())?
        .to_string();
    let ws_exp = ws.get("exp").and_then(Value::as_u64).unwrap_or(0);

    let (api_jwt, api_exp) = match body.get("jwt_api") {
        Some(api) => (
            api.get("jwt")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(ToString::to_string),
            api.get("exp").and_then(Value::as_u64).unwrap_or(0),
        ),
        None => (None, 0),
    };

    let tokens = PairingTokens {
        session_id,
        ws_jwt,
        ws_exp,
        ws_endpoint,
        api_jwt,
        api_exp,
    };
    if !tokens.ws_token_live(now_secs()) {
        return Err("jwt_qconnect is already expired".to_string());
    }
    Ok(tokens)
}

/// mDNS instance names cannot carry spaces or exotic characters; keep
/// alphanumerics/`-`/`_`, collapse runs of `-`. The original display name still
/// travels in the TXT `Name` property.
fn sanitize_instance_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        if c.is_alphanumeric() || c == '_' {
            out.push(c);
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        "QBZ".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Owner handle for the pairing surface: the mDNS registration + the
/// `/streamcore` listener thread. Shut down BEFORE the qconnect disconnect so
/// no handoff can land mid-teardown, and joined so its
/// `Arc<DaemonQconnectService>` (→ `Arc<AppRuntime>`) clone drops ahead of
/// `drop(booted)` (the #521 clock-release ordering).
pub struct PairingHandle {
    server: Arc<Server>,
    thread: Option<std::thread::JoinHandle<()>>,
    mdns: Option<(ServiceDaemon, String)>,
}

impl PairingHandle {
    pub fn shutdown(&mut self) {
        if let Some((daemon, fullname)) = self.mdns.take() {
            let _ = daemon.unregister(&fullname);
            let _ = daemon.shutdown();
        }
        self.server.unblock();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Bind the pairing listener, register the mDNS service, and start serving.
/// mDNS failure is non-fatal (the listener still serves for apps that probe by
/// IP, and boot must not die on a flaky avahi/Bonjour) — bind failure is an
/// error because the advertised surface would point at nothing.
pub fn spawn(
    port: u16,
    friendly_name: &str,
    service: Arc<DaemonQconnectService>,
    rt: tokio::runtime::Handle,
) -> Result<PairingHandle, String> {
    let server = Server::http(("0.0.0.0", port))
        .map_err(|err| format!("bind pairing listener on port {port}: {err}"))?;
    let server = Arc::new(server);

    let mdns = match register_mdns(port, friendly_name) {
        Ok(pair) => Some(pair),
        Err(err) => {
            log::warn!("[QConnect/Pairing] mDNS registration failed (serving anyway): {err}");
            None
        }
    };

    let srv = Arc::clone(&server);
    let thread = std::thread::Builder::new()
        .name("qbzd-pairing".into())
        .spawn(move || {
            for mut req in srv.incoming_requests() {
                let resp = handle(&service, &rt, &mut req);
                let _ = req.respond(resp);
            }
        })
        .map_err(|err| format!("spawn pairing thread: {err}"))?;

    log::info!(
        "[QConnect/Pairing] serving /streamcore on port {port} as \"{friendly_name}\"{}",
        if mdns.is_some() { " (mDNS registered)" } else { "" }
    );
    Ok(PairingHandle {
        server,
        thread: Some(thread),
        mdns,
    })
}

fn register_mdns(port: u16, friendly_name: &str) -> Result<(ServiceDaemon, String), String> {
    let daemon = ServiceDaemon::new().map_err(|err| format!("mdns daemon: {err}"))?;
    let instance = sanitize_instance_name(friendly_name);
    let device_uuid = resolve_qconnect_device_uuid();
    let properties = [
        ("path", "/streamcore"),
        ("type", "SPEAKER"),
        ("sdk_version", SDK_VERSION),
        ("Name", friendly_name),
        ("device_uuid", device_uuid.as_str()),
    ];
    // Host label derived from the instance (not the machine hostname) so we
    // never fight avahi/Bonjour over the machine's own `<hostname>.local.`
    // A-record; enable_addr_auto fills in the real interface addresses.
    let info = ServiceInfo::new(
        MDNS_SERVICE_TYPE,
        &instance,
        &format!("{instance}.local."),
        "",
        port,
        &properties[..],
    )
    .map_err(|err| format!("mdns service info: {err}"))?
    .enable_addr_auto();
    let fullname = info.get_fullname().to_string();
    daemon
        .register(info)
        .map_err(|err| format!("mdns register: {err}"))?;
    Ok((daemon, fullname))
}

// ---------------------------------------------------------------------------
// Token refresh (StreamCore32 parity): `POST /qws/refreshToken` with the
// Bearer api credential renews jwt_api (body `jwt=jwt_api`) and jwt_qws (body
// `jwt=jwt_qws`), so a paired, account-less device can outlive its handed-over
// tokens' expiry without the app re-casting.
// ---------------------------------------------------------------------------

type Runtime = Arc<qbz_app::shell::AppRuntime<crate::adapter::DaemonAdapter>>;

const REFRESH_CHECK_SECS: u64 = 30;
/// Refresh once a token is within this window of its expiry.
const REFRESH_LEAD_SECS: u64 = 300;

/// Daemon-lifetime refresh heartbeat: a no-op until a handoff populates the
/// store. Held by `QconnectHandle` and abort+joined at shutdown (it clones
/// `Arc<AppRuntime>` — #521 ordering, same contract as the report scheduler).
pub fn spawn_token_refresh(store: PairingStore, runtime: Runtime) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick =
            tokio::time::interval(std::time::Duration::from_secs(REFRESH_CHECK_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            refresh_if_needed(&store, &runtime).await;
        }
    })
}

async fn refresh_if_needed(store: &PairingStore, runtime: &Runtime) {
    let Some(snapshot) = store.lock().ok().and_then(|guard| guard.clone()) else {
        return;
    };
    let now = now_secs();
    let due = |exp: u64| exp != 0 && exp <= now + REFRESH_LEAD_SECS;
    let api_due = snapshot.api_jwt.is_some() && due(snapshot.api_exp);
    let ws_due = due(snapshot.ws_exp);
    if !api_due && !ws_due {
        return;
    }
    // The refresh endpoint itself authenticates with the Bearer api credential;
    // without one (degenerate handoff) there is nothing we can renew.
    let Some(mut api_jwt) = snapshot.api_jwt.clone() else {
        return;
    };
    let Some(client) = runtime.core().client().read().await.clone() else {
        return;
    };

    if api_due {
        match refresh_jwt(&client, &api_jwt, "jwt_api").await {
            Ok(payload) => {
                let jwt = payload.get("jwt").and_then(Value::as_str).unwrap_or_default();
                if !jwt.is_empty() {
                    qbz_log::register_secret(jwt.to_string());
                    api_jwt = jwt.to_string();
                    let exp = payload.get("exp").and_then(Value::as_u64).unwrap_or(0);
                    if let Ok(mut guard) = store.lock() {
                        if let Some(tokens) = guard.as_mut() {
                            tokens.api_jwt = Some(api_jwt.clone());
                            tokens.api_exp = exp;
                        }
                    }
                    client.set_bearer_api_token(Some(api_jwt.clone())).await;
                    log::info!("[QConnect/Pairing] refreshed jwt_api (exp {exp})");
                }
            }
            Err(err) => log::warn!("[QConnect/Pairing] jwt_api refresh failed: {err}"),
        }
    }

    if ws_due {
        match refresh_jwt(&client, &api_jwt, "jwt_qws").await {
            Ok(payload) => {
                let jwt = payload.get("jwt").and_then(Value::as_str).unwrap_or_default();
                if !jwt.is_empty() {
                    qbz_log::register_secret(jwt.to_string());
                    let exp = payload.get("exp").and_then(Value::as_u64).unwrap_or(0);
                    let endpoint = payload
                        .get("endpoint")
                        .and_then(Value::as_str)
                        .map(ToString::to_string);
                    if let Ok(mut guard) = store.lock() {
                        if let Some(tokens) = guard.as_mut() {
                            tokens.ws_jwt = jwt.to_string();
                            tokens.ws_exp = exp;
                            if let Some(endpoint) = endpoint {
                                tokens.ws_endpoint = endpoint;
                            }
                        }
                    }
                    // The LIVE WS connection keeps its old token; the fresh one
                    // is what the reconnect credential re-resolve picks up.
                    log::info!("[QConnect/Pairing] refreshed jwt_qws (exp {exp})");
                }
            }
            Err(err) => log::warn!("[QConnect/Pairing] jwt_qws refresh failed: {err}"),
        }
    }
}

/// One `POST /qws/refreshToken` round-trip; returns the renewed token payload
/// (`{jwt, exp[, endpoint]}`). Status-before-decode like the createToken path.
async fn refresh_jwt(
    client: &qbz_qobuz::QobuzClient,
    bearer: &str,
    kind: &str,
) -> Result<Value, String> {
    let app_id = client
        .app_id()
        .await
        .map_err(|err| format!("refreshToken requires initialized API client: {err}"))?;
    let url = qbz_qobuz::endpoints::build_url("/qws/refreshToken");
    let response = client
        .get_http()
        .post(&url)
        .header("X-App-Id", app_id)
        .header("Authorization", format!("Bearer {bearer}"))
        .form(&[("jwt", kind)])
        .send()
        .await
        .map_err(|err| format!("refreshToken HTTP request failed: {err}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|err| format!("refreshToken response read failed: {err}"))?;
    if !status.is_success() {
        let preview = body.trim().chars().take(300).collect::<String>();
        return Err(format!("refreshToken status {status}: {preview}"));
    }
    let payload: Value = serde_json::from_str(&body)
        .map_err(|err| format!("refreshToken response decode failed: {err}"))?;
    payload
        .get(kind)
        .cloned()
        .ok_or_else(|| format!("refreshToken response missing {kind} payload"))
}

// ---------------------------------------------------------------------------
// Request handling (runs on the qbzd-pairing thread; async work reaches the
// tokio runtime through the captured Handle, same pattern as ApiState.rt).
// ---------------------------------------------------------------------------

fn handle(
    service: &Arc<DaemonQconnectService>,
    rt: &tokio::runtime::Handle,
    req: &mut Request,
) -> Response<Cursor<Vec<u8>>> {
    let path = req.url().split('?').next().unwrap_or_default().to_owned();
    match (req.method(), path.as_str()) {
        (Method::Get, "/streamcore/get-display-info") => json(200, display_info()),
        (Method::Get, "/streamcore/get-connect-info") => {
            let session_id = valid_ws_tokens(&service.pairing_store())
                .map(|t| t.session_id)
                .unwrap_or_default();
            let app_id = rt
                .block_on(service.current_app_id())
                .unwrap_or_default();
            json(
                200,
                json!({ "current_session_id": session_id, "app_id": app_id }),
            )
        }
        (Method::Post, "/streamcore/connect-to-qconnect") => {
            // Unauthenticated LAN endpoint: cap the body read (a real handoff
            // is well under 8 KB) so a hostile client can't stream gigabytes
            // into daemon memory.
            use std::io::Read as _;
            let mut body = String::new();
            let _ = req.as_reader().take(64 * 1024).read_to_string(&mut body);
            let body: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
            match parse_connect_request(&body) {
                Ok(tokens) => {
                    // Never let a handed-over credential reach a log line.
                    qbz_log::register_secret(tokens.ws_jwt.clone());
                    if let Some(api_jwt) = &tokens.api_jwt {
                        qbz_log::register_secret(api_jwt.clone());
                    }
                    log::info!(
                        "[QConnect/Pairing] handoff received (session {}…); taking over",
                        tokens.session_id.chars().take(8).collect::<String>()
                    );
                    if let Ok(mut guard) = service.pairing_store().lock() {
                        *guard = Some(tokens);
                    }
                    // Ack the app immediately (it watches the cloud session for
                    // the device joining, not this response) and run the
                    // disconnect+connect takeover off the serving thread. The
                    // task is tracked on the service: a newer handoff aborts
                    // it, and shutdown aborts it before the final disconnect.
                    let takeover = Arc::clone(service);
                    let task = rt.spawn(async move {
                        if let Err(err) = takeover.reconnect_for_pairing().await {
                            log::warn!("[QConnect/Pairing] takeover connect failed: {err}");
                        }
                    });
                    service.replace_takeover_task(task);
                    json(200, json!({}))
                }
                Err(err) => json(400, json!({ "error": err })),
            }
        }
        _ => json(404, json!({ "error": "not found" })),
    }
}

/// `get-display-info` — picker metadata, sourced from the same canonical device
/// identity the WS join advertises. The display quality vocabulary is
/// MP3 | LOSSLESS | HIRES_L1 (24/96) | HIRES_L3 (24/192); our wire capability
/// is hires_l2 = 192 kHz decode, i.e. HIRES_L3 in display terms.
fn display_info() -> Value {
    let info = default_qconnect_device_info();
    json!({
        "type": "SPEAKER",
        "friendly_name": info.friendly_name.unwrap_or_default(),
        "model_display_name": info.model.unwrap_or_default(),
        "brand_display_name": info.brand.unwrap_or_default(),
        "serial_number": info.device_uuid.unwrap_or_default(),
        "max_audio_quality": "HIRES_L3",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws_body(jwt: &str, exp: u64, endpoint: &str) -> Value {
        json!({
            "session_id": "sess-1234",
            "jwt_qconnect": { "jwt": jwt, "exp": exp, "endpoint": endpoint },
            "jwt_api": { "jwt": "api-jwt", "exp": exp }
        })
    }

    #[test]
    fn parse_accepts_a_full_handoff() {
        let far_future = now_secs() + 3600;
        let tokens =
            parse_connect_request(&ws_body("ws-jwt", far_future, "wss://qws.example")).unwrap();
        assert_eq!(tokens.session_id, "sess-1234");
        assert_eq!(tokens.ws_jwt, "ws-jwt");
        assert_eq!(tokens.ws_endpoint, "wss://qws.example");
        assert_eq!(tokens.api_jwt.as_deref(), Some("api-jwt"));
    }

    #[test]
    fn parse_rejects_missing_or_expired_ws_token() {
        assert!(parse_connect_request(&json!({})).is_err());
        assert!(parse_connect_request(&json!({
            "jwt_qconnect": { "jwt": "x", "exp": 0 } // no endpoint
        }))
        .is_err());
        // Already expired (exp in the past) must be refused at the door.
        assert!(parse_connect_request(&ws_body("ws-jwt", 1000, "wss://qws.example")).is_err());
    }

    #[test]
    fn parse_treats_missing_exp_as_non_expiring() {
        let body = json!({
            "session_id": "s",
            "jwt_qconnect": { "jwt": "ws-jwt", "endpoint": "wss://e" }
        });
        let tokens = parse_connect_request(&body).unwrap();
        assert_eq!(tokens.ws_exp, 0);
        assert!(tokens.ws_token_live(now_secs()));
        assert!(tokens.api_jwt.is_none());
    }

    #[test]
    fn ws_token_live_boundary_honors_the_slack() {
        let tokens = |exp| PairingTokens {
            session_id: "s".into(),
            ws_jwt: "jwt".into(),
            ws_exp: exp,
            ws_endpoint: "wss://e".into(),
            api_jwt: None,
            api_exp: 0,
        };
        let now = 1_000_000;
        // Strictly greater than now + slack is required: exactly at the slack
        // edge counts as expired.
        assert!(!tokens(now + EXP_SLACK_SECS).ws_token_live(now));
        assert!(tokens(now + EXP_SLACK_SECS + 1).ws_token_live(now));
    }

    #[test]
    fn debug_never_prints_the_jwts() {
        let rendered = format!(
            "{:?}",
            PairingTokens {
                session_id: "s".into(),
                ws_jwt: "SECRET-WS".into(),
                ws_exp: 0,
                ws_endpoint: "wss://e".into(),
                api_jwt: Some("SECRET-API".into()),
                api_exp: 0,
            }
        );
        assert!(!rendered.contains("SECRET-WS"));
        assert!(!rendered.contains("SECRET-API"));
    }

    #[test]
    fn store_drops_expired_tokens_on_read() {
        let store: PairingStore = Arc::new(StdMutex::new(Some(PairingTokens {
            session_id: "s".into(),
            ws_jwt: "jwt".into(),
            ws_exp: 1, // long expired
            ws_endpoint: "wss://e".into(),
            api_jwt: None,
            api_exp: 0,
        })));
        assert!(valid_ws_tokens(&store).is_none());
        assert!(store.lock().unwrap().is_none(), "expired entry must be cleared");
    }

    #[test]
    fn transport_config_mirrors_the_hardened_defaults() {
        let tokens = PairingTokens {
            session_id: "s".into(),
            ws_jwt: "jwt".into(),
            ws_exp: 0,
            ws_endpoint: "wss://e".into(),
            api_jwt: None,
            api_exp: 0,
        };
        let config = transport_config_from(&tokens);
        assert_eq!(config.endpoint_url, "wss://e");
        assert_eq!(config.jwt_qws.as_deref(), Some("jwt"));
        assert!(config.require_jwt);
        assert_eq!(config.reconnect_idle_retry_ms, 60_000);
        assert_eq!(
            config.subscribe_channels,
            vec![vec![0x01], vec![0x02], vec![0x03]]
        );
    }

    #[test]
    fn instance_name_is_mdns_safe() {
        assert_eq!(sanitize_instance_name("QBZ (studio-pi)"), "QBZ-studio-pi");
        assert_eq!(sanitize_instance_name("Living Room"), "Living-Room");
        assert_eq!(sanitize_instance_name("  ***  "), "QBZ");
    }
}
