// TODO(converge: qconnect-glue) — derived from crates/qbz/src/qconnect_service.rs @ 5d50158e
// (the connect/disconnect facade + startup auto-connect, UI stripped);
// do not fix bugs here without fixing the source, and vice versa.
//
//! Daemon QConnect service facade + boot-step-12 entry point.
//!
//! Composes the copied glue (engine / sink / session / report / transport /
//! remote_stream) into a headless connect flow that reproduces the desktop
//! `SlintQconnectService::connect` recipe (build transport -> one shared
//! sync-state Mutex -> sink -> `QconnectApp::new` -> `set_app` -> `connect` ->
//! subscribe transport events BEFORE the spawn -> spawn `run_session_loop` ->
//! `bootstrap_remote_presence`), minus every UI surface. Lifecycle transitions
//! latch into `DaemonShared.qconnect` so `/api/status` stays diagnosable.
//!
//! `start()` mints the daemon's OWN device identity in the daemon-root KV, reads
//! the effective startup mode (cli_override = None — never shadow the KV that
//! T11/T13 write), and, when auto-connect is on, spawns a connect-on-Ready task
//! with the bounded [2s, 5s, 15s, 30s] retry schedule. QConnect reads NOTHING
//! from qbzd.toml — only the daemon-root `qconnect_settings.db`.

pub mod engine;
pub mod pairing;
pub mod publish;
pub mod remote_stream;
pub mod report;
pub mod session;
pub mod sink;
pub mod transport;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use qbz_app::shell::AppRuntime;
use qbz_models::CoreEvent;
use qconnect_app::{
    compute_effective_startup, QconnectApp, QconnectAppEvent, QconnectEventSink,
    QconnectLifecycleState, QconnectRemoteSyncState, QconnectSessionState, SessionLoopHost,
};
use qconnect_transport_ws::{NativeWsTransport, WsTransportConfig};
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;

use crate::adapter::DaemonAdapter;
use crate::paths::ProfileRoots;
use crate::state::{AuthState, DaemonShared};

use self::engine::DaemonRendererEngine;
use self::session::{bootstrap_remote_presence, DaemonSessionLoopHost};
use self::sink::{DaemonEventSink, DaemonQconnectApp};

type Runtime = Arc<AppRuntime<DaemonAdapter>>;
type SharedState = Arc<std::sync::Mutex<DaemonShared>>;

/// The live QConnect runtime for one connected session (app + its config + the
/// spawned event loop + the shared sync accumulator).
pub(crate) struct DaemonQconnectRuntime {
    pub app: Arc<DaemonQconnectApp>,
    /// Re-latched by `bootstrap_after_reconnect` on a credential re-resolve;
    /// consumed by the full-reconnect path (T10) + status/endpoint reporting.
    #[allow(dead_code)]
    pub config: WsTransportConfig,
    pub event_loop: JoinHandle<()>,
    pub sync_state: Arc<Mutex<QconnectRemoteSyncState>>,
}

/// Connect-flow state, mirrored on the desktop `SlintQconnectInner`. `pub(crate)`
/// fields so `session::DaemonSessionLoopHost` can gate lifecycle + re-latch the
/// config + drop the runtime on reconnect-exhausted.
#[derive(Default)]
pub(crate) struct DaemonQconnectInner {
    pub runtime: Option<DaemonQconnectRuntime>,
    /// Latched connect/loop error; surfaced by `qbzd status` QConnect block (T11).
    #[allow(dead_code)]
    pub last_error: Option<String>,
    pub lifecycle_state: QconnectLifecycleState,
    /// Last local queue ids this session pushed to the cloud (the publish.rs
    /// echo latch). Cleared on every connect (parity with the desktop
    /// `qconnect_service.rs` connect reset).
    pub last_pushed_queue_ids: Option<Vec<u64>>,
}

/// Map a lifecycle state to the `/api/status` `qconnect.state` label + the
/// session-active flag, and latch it into `DaemonShared`.
fn latch_lifecycle_into_shared(shared: &SharedState, state: QconnectLifecycleState) {
    let (label, active) = match state {
        QconnectLifecycleState::Off => ("off", false),
        QconnectLifecycleState::Connecting => ("connecting", false),
        QconnectLifecycleState::Connected => ("connected", true),
        QconnectLifecycleState::Reconnecting => ("retrying", false),
        QconnectLifecycleState::Exhausted => ("exhausted", false),
    };
    if let Ok(mut s) = shared.lock() {
        s.qconnect.state = label.to_string();
        s.qconnect.session_active = active;
        if matches!(state, QconnectLifecycleState::Reconnecting) {
            s.qconnect.last_transport_reconnect = Some(unix_seconds_string());
        }
        // 01 §9.3: a live QConnect session (fresh connect OR post-reconnect
        // recovery) is a real network-reachable outcome — latch it true. The
        // reconnect-EXHAUSTED failure side latches false in
        // `session::DaemonSessionLoopHost::on_reconnect_exhausted` (it does
        // not route through this helper — see there).
        if matches!(state, QconnectLifecycleState::Connected) {
            s.set_network_online(true);
        }
        // Surface the transition on the CoreEvent bus (SSE, `qbzd watch`,
        // the event hook) alongside the /api/status latch.
        s.emit_qconnect_session_changed();
    }
}

fn unix_seconds_string() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

/// Dedup + gate a lifecycle transition: only emit while a runtime is alive and
/// the state actually changes. Mirrors the desktop
/// `update_lifecycle_state_if_running`, plus the `DaemonShared` latch.
pub(crate) async fn update_lifecycle_state_if_running(
    inner: &Arc<Mutex<DaemonQconnectInner>>,
    sink: &DaemonEventSink,
    shared: &SharedState,
    next: QconnectLifecycleState,
) {
    let mut guard = inner.lock().await;
    if guard.runtime.is_none() {
        return;
    }
    if guard.lifecycle_state == next {
        return;
    }
    guard.lifecycle_state = next;
    drop(guard);
    latch_lifecycle_into_shared(shared, next);
    sink.on_event(QconnectAppEvent::LifecycleChanged { state: next })
        .await;
}

/// The headless QConnect connect service.
pub struct DaemonQconnectService {
    inner: Arc<Mutex<DaemonQconnectInner>>,
    runtime: Runtime,
    shared: SharedState,
    #[allow(dead_code)] // T11 (settings reload) re-reads the KV through this path.
    settings_db: PathBuf,
    custom_device_name: Arc<tokio::sync::RwLock<Option<String>>>,
    /// Tokens handed over by a Qobuz app via the local pairing surface
    /// (pairing.rs). `connect()` prefers a live entry over `/qws/createToken`.
    pairing_store: pairing::PairingStore,
    /// Serializes connect/disconnect state transitions. Without it a pairing
    /// takeover's `disconnect()` against an in-flight `connect()` (e.g. the
    /// auto-connect watcher mid-handshake) lets the slower connect install its
    /// runtime LAST — leaking the newer session's event loop + WS connection
    /// and stranding the handed-over token on the account session.
    ops: Mutex<()>,
    /// The latest pairing-takeover task (pairing.rs POST handler). Tracked so
    /// a newer handoff aborts the previous takeover and `shutdown()` can abort
    /// it before the final disconnect (#521: it clones this service's
    /// `Arc<AppRuntime>` and must not resurrect a session past shutdown).
    takeover_task: std::sync::Mutex<Option<JoinHandle<()>>>,
    /// One-shot latch: the next deferred renderer join claims the active slot
    /// (is_active=true). Armed by a handoff takeover — the handoff IS the
    /// user's output selection and no SET_ACTIVE_RENDERER follows it (official
    /// receivers join active after a handoff; qobuz-proxy parity). Never armed
    /// on the account path, which keeps the desktop anti-steal join.
    handoff_join_pending: Arc<std::sync::atomic::AtomicBool>,
    /// Which track is still filling its buffer — written by the renderer
    /// engine, read by the report scheduler so the controller is told
    /// BUFFERING instead of being shown a healthy buffer over silence.
    buffering: Arc<engine::BufferingLatch>,
    /// Report-tick wakeup, shared with the engine so a load reports at once.
    report_notify: Arc<tokio::sync::Notify>,
}

impl DaemonQconnectService {
    /// Establish the QConnect session. Gated on an initialized API client (the
    /// qws/createToken discovery needs it). Idempotent: a second call while a
    /// runtime is alive (or a connect is in flight) is a no-op.
    pub async fn connect(&self) -> Result<(), String> {
        let _ops = self.ops.lock().await;
        self.connect_locked().await
    }

    async fn connect_locked(&self) -> Result<(), String> {
        if !self.runtime.core().is_api_initialized().await {
            return Err("Qobuz API is not initialized; cannot start Qobuz Connect".to_string());
        }

        // Claim the connect slot ATOMICALLY before the transport-config await, so
        // two concurrent connect()s can't both build a runtime. A live runtime OR
        // an in-flight `Connecting` both short-circuit to a no-op.
        {
            let mut guard = self.inner.lock().await;
            if guard.runtime.is_some()
                || guard.lifecycle_state == QconnectLifecycleState::Connecting
            {
                log::info!(
                    "[QConnect] connect() called while already {:?}; no-op",
                    guard.lifecycle_state
                );
                return Ok(());
            }
            guard.lifecycle_state = QconnectLifecycleState::Connecting;
            guard.last_error = None;
        }
        latch_lifecycle_into_shared(&self.shared, QconnectLifecycleState::Connecting);

        // A live locally-paired credential (pairing.rs handoff) wins over the
        // account-bound `/qws/createToken` discovery: it carries the CASTER's
        // session, which is the whole point of the pairing surface.
        let config = match pairing::valid_ws_tokens(&self.pairing_store) {
            Some(tokens) => {
                log::info!("[QConnect] connecting with locally paired credentials");
                pairing::transport_config_from(&tokens)
            }
            None => match transport::resolve_transport_config(&self.runtime).await {
                Ok(config) => config,
                Err(err) => {
                    let mut guard = self.inner.lock().await;
                    if guard.runtime.is_none() {
                        guard.lifecycle_state = QconnectLifecycleState::Off;
                    }
                    drop(guard);
                    latch_lifecycle_into_shared(&self.shared, QconnectLifecycleState::Off);
                    return Err(err);
                }
            },
        };

        let transport = Arc::new(NativeWsTransport::new());
        let sync_state = Arc::new(Mutex::new(QconnectRemoteSyncState::default()));
        // T10 (OD4, §7.4): resolve the volume policy from the daemon-root KV at
        // connect, so a later `qbzd settings reload` (T11) is picked up on the
        // next connect. Unset/unknown -> Software (the OD4 default).
        let volume_mode = engine::VolumeMode::from_kv(
            transport::load_volume_mode_at(&self.settings_db).as_deref(),
        );
        let engine = DaemonRendererEngine::new(
            Arc::clone(&self.runtime),
            volume_mode,
            Arc::clone(&self.buffering),
            Arc::clone(&self.report_notify),
            Arc::clone(&self.shared),
        );
        let sink = Arc::new(DaemonEventSink::new(
            engine,
            Arc::clone(&sync_state),
            Arc::clone(&self.shared),
        ));
        let app = Arc::new(QconnectApp::new(
            Arc::clone(&transport),
            Arc::clone(&sink),
            Arc::clone(&sync_state),
        ));
        // Wire the owning app into the sink so it can emit reports + drive
        // session-apply.
        sink.set_app(&app);

        if let Err(err) = app.connect(config.clone()).await {
            let mut guard = self.inner.lock().await;
            guard.lifecycle_state = QconnectLifecycleState::Off;
            let msg = format!("qconnect transport connect failed: {err}");
            guard.last_error = Some(msg.clone());
            drop(guard);
            latch_lifecycle_into_shared(&self.shared, QconnectLifecycleState::Off);
            return Err(msg);
        }

        // T10 (OD4, §7.4): if locked volume mode, pin the player to 100% (1.0)
        // at connect time. Harmless no-op on bit-perfect backends (ALSA-direct
        // hw_volume=false, JACK, DoP/DSD), corrects Rodio backends (PipeWire/
        // Pulse) where Player default is 0.75 (not 1.0).
        if volume_mode == engine::VolumeMode::Locked {
            if let Err(err) = self.runtime.core().set_volume(1.0) {
                log::warn!("[QConnect] failed to pin volume to 100% at connect: {err}");
            }
        }

        // Subscribe to transport events SYNCHRONOUSLY here — after connect()
        // returns and BEFORE the spawn / any further await — so the receiver is
        // live before the WS handshake emits Connected / Subscribed /
        // SessionEstablished / SESSION_STATE. tokio broadcast has no replay; a
        // receiver created inside the spawned loop would race + drop those.
        let transport_rx = app.subscribe_transport_events();
        let idle_retry_active = config.reconnect_idle_retry_ms > 0;
        let host: Arc<dyn SessionLoopHost> = Arc::new(DaemonSessionLoopHost {
            app: Arc::clone(&app),
            sync_state: Arc::clone(&sync_state),
            inner: Arc::clone(&self.inner),
            sink: Arc::clone(&sink),
            runtime: Arc::clone(&self.runtime),
            shared: Arc::clone(&self.shared),
            volume_mode, // T10 (OD4): join-time volume report honors the mode
            // Resolved at connect like the mode above, so a `settings set` is
            // picked up on the next connect.
            initial_volume: transport::load_initial_volume_at(&self.settings_db),
            pairing_store: Arc::clone(&self.pairing_store),
            handoff_join_pending: Arc::clone(&self.handoff_join_pending),
        });
        let app_for_loop = Arc::clone(&app);
        let event_loop = tokio::spawn(async move {
            app_for_loop
                .run_session_loop(host, transport_rx, idle_retry_active)
                .await;
        });

        let runtime_app = Arc::clone(&app);
        {
            let mut guard = self.inner.lock().await;
            guard.last_error = None;
            guard.last_pushed_queue_ids = None; // fresh session: re-arm the publish echo latch
            guard.runtime = Some(DaemonQconnectRuntime {
                app,
                config,
                event_loop,
                sync_state,
            });
        }

        let custom_name = self.custom_device_name.read().await.clone();
        if let Err(err) = bootstrap_remote_presence(&runtime_app, custom_name.clone()).await {
            let _ = self.disconnect().await;
            let mut guard = self.inner.lock().await;
            guard.last_error = Some(format!("qconnect bootstrap failed: {err}"));
            return Err(format!("qconnect bootstrap failed: {err}"));
        }

        // Reflect the resolved device name in `/api/status`.
        let effective_name = transport::resolve_qconnect_friendly_name(custom_name.as_deref());
        if let Ok(mut s) = self.shared.lock() {
            s.qconnect.device_name = effective_name;
        }

        Ok(())
    }

    /// Tear the QConnect session down. Always forces Off. Aborts AND joins the
    /// event loop so its `Arc<AppRuntime>` clone drops before the daemon's
    /// shutdown releases the audio device (§8.2 / #521 ordering).
    pub async fn disconnect(&self) -> Result<(), String> {
        let _ops = self.ops.lock().await;
        self.disconnect_locked().await
    }

    async fn disconnect_locked(&self) -> Result<(), String> {
        let runtime = {
            let mut guard = self.inner.lock().await;
            guard.lifecycle_state = QconnectLifecycleState::Off;
            guard.runtime.take()
        };

        if let Some(runtime) = runtime {
            // Disarm any in-flight liveness watchdog + clear the session topology
            // BEFORE aborting the loop, so a late event can't resurrect a stale
            // active-renderer state.
            {
                let mut state = runtime.sync_state.lock().await;
                state.watchdog_generation = state.watchdog_generation.wrapping_add(1);
                state.session = QconnectSessionState::default();
                state.session_renderer_states.clear();
            }
            if let Err(err) = runtime.app.disconnect().await {
                let mut guard = self.inner.lock().await;
                guard.last_error = Some(format!("qconnect disconnect failed: {err}"));
            }
            runtime.event_loop.abort();
            let _ = runtime.event_loop.await;
        }

        if let Ok(mut s) = self.shared.lock() {
            s.qconnect.state = "off".to_string();
            s.qconnect.session_active = false;
            s.emit_qconnect_session_changed();
        }
        Ok(())
    }

    /// T11 (`POST /api/settings/reload`): re-cache the device-name override from
    /// the daemon-root KV so the NEXT `connect()` (whenever that happens) uses
    /// whatever `qbzd qconnect name` / `settings set qconnect.device_name` most
    /// recently wrote — 03-setup-tui.md §3.4's "applies on the next connection"
    /// rule. Does NOT force a reconnect by itself (a rename alone must not
    /// bounce an active session).
    async fn refresh_device_name(&self, settings_db: &std::path::Path) {
        let name = transport::load_device_name_at(settings_db);
        *self.custom_device_name.write().await = name;
    }

    /// Shared handle onto the pairing-token store (pairing.rs is the writer,
    /// the connect/reconnect paths are the readers).
    pub(crate) fn pairing_store(&self) -> pairing::PairingStore {
        Arc::clone(&self.pairing_store)
    }

    /// The bundle app id for `get-connect-info`, when the API client exists.
    pub(crate) async fn current_app_id(&self) -> Option<String> {
        let client = self.runtime.core().client().read().await.clone()?;
        client.app_id().await.ok()
    }

    /// Handoff takeover: force-drop whatever session is live, then connect —
    /// which now picks up the freshly stored pairing tokens. Holds the ops
    /// lock across BOTH steps so an in-flight connect (auto-connect watcher,
    /// an earlier takeover) finishes and gets torn down first, and nothing can
    /// interleave between our disconnect and connect.
    pub(crate) async fn reconnect_for_pairing(&self) -> Result<(), String> {
        let _ops = self.ops.lock().await;
        // Login-free path: the API client may still be missing after an
        // offline-tolerant boot (no-op when initialized), and the handed-over
        // jwt_api is the client's credential when no account is logged in
        // (stream URLs via `Authorization: Bearer` — the user session, when
        // present, still outranks it inside the client).
        let _ = self.runtime.core().try_init_api().await;
        if let Some(tokens) = pairing::valid_ws_tokens(&self.pairing_store) {
            if let Some(client) = self.runtime.core().client().read().await.clone() {
                client.set_bearer_api_token(tokens.api_jwt.clone()).await;
            }
        }
        let _ = self.disconnect_locked().await;
        // Arm AFTER the disconnect (which tears down the previous session
        // loop) and BEFORE the connect that will consume it on its deferred
        // renderer join.
        self.handoff_join_pending
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.connect_locked().await
    }

    /// Track the latest takeover task, aborting the previous one (a newer
    /// handoff always wins). Returns nothing; the handle is consumed by
    /// [`Self::abort_takeover_task`] at shutdown.
    pub(crate) fn replace_takeover_task(&self, task: JoinHandle<()>) {
        if let Ok(mut guard) = self.takeover_task.lock() {
            if let Some(prev) = guard.replace(task) {
                prev.abort();
            }
        }
    }

    /// Abort any in-flight takeover so it cannot resurrect a session after
    /// shutdown's final disconnect (#521 ordering).
    fn abort_takeover_task(&self) {
        if let Ok(mut guard) = self.takeover_task.lock() {
            if let Some(task) = guard.take() {
                task.abort();
            }
        }
    }

    /// Wait until the daemon is Ready (logged in + API initialized), then attempt
    /// `connect()` with the bounded [2s, 5s, 15s, 30s] retry schedule. Each
    /// `connect()` re-resolves the transport config internally, so a transient
    /// credential/network failure can clear on a later attempt.
    async fn connect_on_ready(self: Arc<Self>) {
        loop {
            let logged_in = self
                .shared
                .lock()
                .map(|s| s.auth == AuthState::LoggedIn)
                .unwrap_or(false);
            if logged_in && self.runtime.core().is_api_initialized().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let schedule: [u64; 4] = [2_000, 5_000, 15_000, 30_000];
        for attempt in 0..=schedule.len() {
            match self.connect().await {
                Ok(()) => {
                    log::info!("[QConnect] auto-connect succeeded");
                    return;
                }
                Err(err) => {
                    log::warn!(
                        "[QConnect] auto-connect attempt {} failed: {err}",
                        attempt + 1
                    );
                }
            }
            match schedule.get(attempt) {
                Some(delay_ms) => tokio::time::sleep(Duration::from_millis(*delay_ms)).await,
                None => {
                    log::warn!(
                        "[QConnect] auto-connect gave up for this session after {} attempts",
                        attempt + 1
                    );
                    return;
                }
            }
        }
    }
}

/// Owner handle held by the daemon boot for the process lifetime. Drives runtime
/// enable/disable (T11) and the ordered shutdown (§8.2-1).
pub struct QconnectHandle {
    service: Arc<DaemonQconnectService>,
    watcher: Option<JoinHandle<()>>,
    /// The local pairing surface (mDNS + /streamcore listener). Shut down FIRST
    /// so no handoff can land while the session is being torn down, and joined
    /// so its `Arc<DaemonQconnectService>` clone drops before `drop(booted)`.
    pairing: Option<pairing::PairingHandle>,
    /// T10: the report-tick scheduler task. Held so shutdown can abort+join it
    /// (it clones `Arc<AppRuntime>`, so it must drop before `drop(booted)` per the
    /// #521 clock-release ordering, exactly like the watcher).
    report_task: Option<JoinHandle<()>>,
    /// The queue-publish subscriber (publish.rs). Same #521 ordering contract as
    /// `report_task` — it clones `Arc<AppRuntime>` + the qconnect inner.
    publish_task: Option<JoinHandle<()>>,
    /// The pairing token-refresh heartbeat (pairing.rs). Same #521 contract.
    refresh_task: Option<JoinHandle<()>>,
}

/// T11: a `Clone`-able, `Send + Sync` handle onto the running
/// [`DaemonQconnectService`] — what the reload route reaches through
/// `ApiState.qconnect_control` (`Arc<std::sync::OnceLock<QconnectControl>>`,
/// populated by `QconnectHandle::control()` right after `start()` returns).
/// `connect`/`disconnect` are already idempotent on the service (a no-op when
/// already in the target state), so the reload path can call either
/// unconditionally without checking current status first.
#[derive(Clone)]
pub struct QconnectControl(Arc<DaemonQconnectService>);

impl QconnectControl {
    pub async fn connect(&self) -> Result<(), String> {
        self.0.connect().await
    }

    pub async fn disconnect(&self) -> Result<(), String> {
        // Operator intent (`qbzd qconnect disable`): drop any handed-over
        // pairing credentials, so a later enable reconnects with the daemon
        // account instead of silently re-joining the last LAN caster's
        // session. Abort any in-flight takeover FIRST and do the clearing
        // under the ops lock — otherwise a takeover that already read the
        // tokens re-installs the caster's Bearer right after we cleared it.
        // The internal disconnect() must NOT clear anything —
        // reconnect_for_pairing depends on the token surviving its own
        // disconnect step.
        self.0.abort_takeover_task();
        let _ops = self.0.ops.lock().await;
        self.0
            .handoff_join_pending
            .store(false, std::sync::atomic::Ordering::SeqCst);
        if let Ok(mut guard) = self.0.pairing_store.lock() {
            *guard = None;
        }
        if let Some(client) = self.0.runtime.core().client().read().await.clone() {
            client.set_bearer_api_token(None).await;
        }
        self.0.disconnect_locked().await
    }

    /// Re-cache the device-name override from the daemon-root KV (§ see
    /// [`DaemonQconnectService::refresh_device_name`] — applies on the NEXT
    /// connect, never forces a reconnect for a rename alone).
    pub async fn refresh_device_name(&self, settings_db: &std::path::Path) {
        self.0.refresh_device_name(settings_db).await;
    }
}

impl QconnectHandle {
    /// Connect on demand (T11 `qbzd qconnect enable`).
    #[allow(dead_code)]
    pub async fn connect(&self) -> Result<(), String> {
        self.service.connect().await
    }

    /// Disconnect on demand (T11 `qbzd qconnect disable`).
    #[allow(dead_code)]
    pub async fn disconnect(&self) -> Result<(), String> {
        self.service.disconnect().await
    }

    /// A cheap, `Clone`-able handle onto the running service — what
    /// `daemon.rs`'s `POST /api/settings/reload` route holds (via an
    /// `Arc<OnceLock<QconnectControl>>` populated right after `start()`
    /// returns, since QConnect boots AFTER the HTTP server per the normative
    /// order, 01-architecture.md §8.1 steps 11/12). Carries none of the
    /// `JoinHandle`s `QconnectHandle` owns — those stay daemon-shutdown-only.
    pub fn control(&self) -> QconnectControl {
        QconnectControl(Arc::clone(&self.service))
    }

    /// §8.2-1: stop the auto-connect watcher and disconnect the session BEFORE
    /// the daemon stops playback. Aborts + joins the watcher and the event loop so
    /// every `Arc<AppRuntime>` clone this handle owns drops ahead of
    /// `drop(booted)` (the #521 clock-release ordering).
    pub async fn shutdown(&mut self) {
        // Stop the pairing surface FIRST (no new handoffs), then kill any
        // in-flight takeover, so nothing can resurrect a session after the
        // final disconnect below. The listener join is a blocking call and the
        // pairing thread may itself be parked in `Handle::block_on`, so move
        // the join off this worker (multi-thread runtime; see main.rs).
        if let Some(mut pairing) = self.pairing.take() {
            tokio::task::block_in_place(move || pairing.shutdown());
        }
        self.service.abort_takeover_task();
        if let Some(watcher) = self.watcher.take() {
            watcher.abort();
            let _ = watcher.await;
        }
        // T10: stop the report scheduler too, so its `Arc<AppRuntime>` clone drops
        // ahead of `drop(booted)`.
        if let Some(report_task) = self.report_task.take() {
            report_task.abort();
            let _ = report_task.await;
        }
        // Same for the queue-publish subscriber (publish.rs).
        if let Some(publish_task) = self.publish_task.take() {
            publish_task.abort();
            let _ = publish_task.await;
        }
        // And the pairing token-refresh heartbeat.
        if let Some(refresh_task) = self.refresh_task.take() {
            refresh_task.abort();
            let _ = refresh_task.await;
        }
        let _ = self.service.disconnect().await;
    }
}

/// Boot step 12: wire QConnect. Mints the daemon's OWN device identity in the
/// daemon-root KV, decides auto-connect from the persisted startup mode
/// (`cli_override = None`, `last_known = None` — P0), latches the initial status,
/// and, when enabled, spawns the connect-on-Ready retry task.
pub fn start(
    runtime: Runtime,
    shared: SharedState,
    roots: &ProfileRoots,
    report_notify: Arc<tokio::sync::Notify>,
    core_events: broadcast::Receiver<CoreEvent>,
) -> QconnectHandle {
    let settings_db = roots.data.join("qconnect_settings.db");
    // Re-point device identity + KV at the daemon root (NEVER the desktop global).
    transport::init_settings_db_path(settings_db.clone());

    // Effective startup decision (Ready-state only). `cli_override` stays None: a
    // `Some` would permanently shadow the KV store that `qbzd qconnect
    // enable|disable` (T11) + the TUI (T13) write, making both dead controls.
    // `last_known` is None in P0 (RememberLast resolves to off).
    let mode = transport::load_startup_mode_at(&settings_db);
    let should_auto_connect = compute_effective_startup(mode, None, None);
    let custom_name = transport::load_device_name_at(&settings_db);
    let effective_name = transport::resolve_qconnect_friendly_name(custom_name.as_deref());

    // Latch the initial status so `/api/status` reflects the config before Ready.
    if let Ok(mut s) = shared.lock() {
        s.qconnect.enabled = should_auto_connect;
        s.qconnect.device_name = effective_name.clone();
        s.qconnect.state = "off".to_string();
        s.qconnect.session_active = false;
    }

    let service = Arc::new(DaemonQconnectService {
        inner: Arc::new(Mutex::new(DaemonQconnectInner::default())),
        runtime,
        shared,
        settings_db: settings_db.clone(),
        custom_device_name: Arc::new(tokio::sync::RwLock::new(custom_name)),
        pairing_store: Arc::new(std::sync::Mutex::new(None)),
        ops: Mutex::new(()),
        takeover_task: std::sync::Mutex::new(None),
        handoff_join_pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        buffering: Arc::new(engine::BufferingLatch::default()),
        report_notify: Arc::clone(&report_notify),
    });

    // Local pairing surface (KV `pairing` = on|off, default on; port from KV
    // `pairing_port`). Fail-open on error: the account-bound cloud path above
    // is independent of it, so a bind conflict must not take the daemon down.
    let (pairing, refresh_task, pairing_port) = if transport::load_pairing_enabled_at(&settings_db)
    {
        let port = transport::load_pairing_port_at(&settings_db);
        match pairing::spawn(
            port,
            &effective_name,
            Arc::clone(&service),
            tokio::runtime::Handle::current(),
        ) {
            Ok(handle) => {
                let refresh = pairing::spawn_token_refresh(
                    service.pairing_store(),
                    Arc::clone(&service.runtime),
                );
                (Some(handle), Some(refresh), Some(port))
            }
            Err(err) => {
                log::warn!("[QConnect/Pairing] disabled for this run: {err}");
                (None, None, None)
            }
        }
    } else {
        log::info!("[QConnect/Pairing] disabled by settings (pairing = off)");
        (None, None, None)
    };
    // Reflect the pairing surface in `/api/status` (static for the process
    // lifetime — the listener is boot-time-only). `pairing_port` is the port
    // the listener actually BOUND, never a re-read of the KV (a settings set
    // racing the boot window must not make status report a port nothing
    // listens on).
    if let Ok(mut s) = service.shared.lock() {
        s.qconnect.pairing = pairing.is_some();
        s.qconnect.pairing_port = pairing_port;
    }

    let watcher = if should_auto_connect {
        log::info!(
            "[QConnect] auto-connect enabled (startup mode = {}); waiting for Ready",
            mode.as_str()
        );
        let svc = Arc::clone(&service);
        Some(tokio::spawn(async move { svc.connect_on_ready().await }))
    } else {
        log::info!(
            "[QConnect] auto-connect disabled (startup mode = {})",
            mode.as_str()
        );
        None
    };

    // T10 (§7.2): spawn the report-tick scheduler. It runs for the daemon
    // lifetime, waking on the driver's ReportEdge signal (via `report_notify`)
    // and its own ~2 s floor, and reports on the LIVE session (a no-op until a
    // connect installs a runtime).
    let scheduler_inner = Arc::clone(&service.inner);
    let scheduler_runtime = Arc::clone(&service.runtime);
    let scheduler_buffering = Arc::clone(&service.buffering);
    let report_task = Some(tokio::spawn(async move {
        report::run_report_scheduler(
            report_notify,
            scheduler_inner,
            scheduler_runtime,
            scheduler_buffering,
        )
        .await;
    }));

    // Queue-publish subscriber (publish.rs): debounced CoreEvent::QueueUpdated ->
    // push the local queue to the cloud when it changed. Runs for the daemon
    // lifetime; a no-op until a connect installs a runtime (and gated to
    // active-local-renderer sessions inside the publish body).
    let publish_task = Some(publish::spawn_queue_cloud_publish(
        Arc::clone(&service.inner),
        Arc::clone(&service.runtime),
        core_events,
    ));

    QconnectHandle {
        service,
        watcher,
        pairing,
        report_task,
        publish_task,
        refresh_task,
    }
}
