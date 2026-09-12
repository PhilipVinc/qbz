// TODO(converge: qconnect-glue) — copied from crates/qbz/src/qconnect_event_sink.rs @ c8ef2a1b;
// do not fix bugs here without fixing the source, and vice versa.
//
//! Daemon `QconnectEventSink`.
//!
//! Receives `QconnectAppEvent`s from the qconnect-app crate and dispatches the
//! renderer-critical arms into the [`DaemonRendererEngine`] (the renderer seam),
//! the shared `QconnectRemoteSyncState` accumulator, and the frontend-agnostic
//! `qconnect_app::renderer` orchestration (materialize / apply / loop-mode /
//! cursor-align). The session-management critical section is delegated to
//! `QconnectApp::apply_session_management_event`; only the post-lock work the
//! returned `SessionApplyOutcome` asks for runs here.
//!
//! Daemon adaptation vs. the Slint copy (§1.4): the FOUR renderer-critical arms
//! (QueueUpdated / RendererCommandApplied / SessionManagementEvent /
//! RendererUpdated) are wired so the daemon functions AS a renderer; every UI arm
//! (device picker, now-playing card, DEV modal, toasts) is dropped, and the three
//! desktop `toast::error_weak` surfaces become `log::warn!`.

use std::sync::{Arc, OnceLock, Weak};

use async_trait::async_trait;
use qconnect_app::{
    build_session_renderer_snapshot, cache_renderer_snapshot, is_peer_renderer_active, QconnectApp,
    QconnectAppEvent, QconnectEventSink, QconnectRemoteSyncState, QconnectRendererEngine,
    RendererCommand, RendererReport, RendererReportType,
};
use qconnect_transport_ws::NativeWsTransport;
use serde_json::Value;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::engine::DaemonRendererEngine;
use super::transport::resolve_local_identity;

/// Concrete `QconnectApp` type used by the daemon adapter.
pub type DaemonQconnectApp = QconnectApp<NativeWsTransport, DaemonEventSink>;

pub struct DaemonEventSink {
    /// Renderer seam — forwards the `qconnect_app::renderer` orchestration onto
    /// `runtime.core()` + the protected player.
    engine: DaemonRendererEngine,
    /// THE shared remote-sync accumulator (one Mutex, shared with `QconnectApp`).
    sync_state: Arc<Mutex<QconnectRemoteSyncState>>,
    /// Daemon status latch, so `/api/status` can say whether the SESSION still
    /// renders HERE — not merely that we hold a cloud connection.
    shared: Arc<std::sync::Mutex<crate::state::DaemonShared>>,
    /// Late-bound weak handle to the owning app, wired via `set_app` after the
    /// app is built FROM this sink. Used to emit renderer reports (e.g.
    /// is_active=true after SetActive(true)) and to drive the session-apply +
    /// freeze/watchdog without an ownership cycle.
    app: Arc<OnceLock<Weak<DaemonQconnectApp>>>,
    /// FIX #13: previous "a peer is the active renderer" state, tracked across
    /// `apply_session_management_event` calls. On a false->true transition (the
    /// daemon becomes a CONTROLLER) we fire one `ask_for_active_renderer_state` to
    /// fetch the peer's full state (incl. `current_queue_item_id`) so the queue
    /// cursor resolves the peer's CURRENT track immediately instead of staying
    /// stale until the peer changes track. Edge-detected to avoid spamming on
    /// every periodic state-update frame.
    last_peer_active: std::sync::atomic::AtomicBool,
    /// DAEMON-ONLY (qconnect.initial_volume): when this renderer last became the
    /// session's active one, and whether the join-time volume has already been
    /// defended once since. See `assert_join_volume`.
    became_active_at: std::sync::Mutex<Option<std::time::Instant>>,
    join_volume_asserted: std::sync::atomic::AtomicBool,
}

/// How long after taking the render a controller's volume still counts as its
/// STALE slider rather than a deliberate change.
///
/// Observed on hardware: the desktop app pushed its own 75 % 0.7 s after
/// SetActive. A few seconds covers that comfortably and is far short of anyone
/// reaching for the slider on purpose.
const JOIN_VOLUME_ASSERT_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);

impl DaemonEventSink {
    pub fn new(
        engine: DaemonRendererEngine,
        sync_state: Arc<Mutex<QconnectRemoteSyncState>>,
        shared: Arc<std::sync::Mutex<crate::state::DaemonShared>>,
    ) -> Self {
        Self {
            engine,
            sync_state,
            shared,
            app: Arc::new(OnceLock::new()),
            last_peer_active: std::sync::atomic::AtomicBool::new(false),
            became_active_at: std::sync::Mutex::new(None),
            join_volume_asserted: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Wire the owning app after construction. Idempotent (OnceLock).
    pub fn set_app(&self, app: &Arc<DaemonQconnectApp>) {
        let _ = self.app.set(Arc::downgrade(app));
    }

    /// Emit a StateUpdated report announcing this renderer is now active. Sent
    /// after SetActive(true) is applied so the controller learns we are ready.
    async fn report_active_renderer_ready(&self) {
        let Some(app) = self.app.get().and_then(Weak::upgrade) else {
            return;
        };
        let queue_version = app.queue_state_snapshot().await.version;
        let report = RendererReport::new(
            RendererReportType::RndrSrvrStateUpdated,
            Uuid::new_v4().to_string(),
            queue_version,
            serde_json::json!({
                "is_active": true,
                // No buffer state: this report announces that we are the active
                // renderer, and it is flushed at almost exactly the moment a
                // takeback's forced stream arms the buffer, so whichever value it
                // sampled was a coin flip — observed announcing OK 13 ms before
                // the report loop announced BUFFERING for the same load. Omitted,
                // it leaves the field untouched and the report loop stays the one
                // place that speaks about the buffer.
                "buffer_state": Option::<i32>::None,
                "queue_version": {
                    "major": queue_version.major,
                    "minor": queue_version.minor
                }
            }),
        );
        if let Err(err) = app.send_renderer_report_command(report).await {
            log::warn!("[QConnect] Failed to report active-renderer-ready: {err}");
        }
    }

    /// DAEMON-ONLY: send this renderer's real volume to the controller.
    async fn report_volume(&self, reason: &str) {
        let Some(app) = self.app.get().and_then(Weak::upgrade) else {
            return;
        };
        let volume_pct = self.engine.reported_volume_pct();
        log::info!("[QConnect] Reporting volume {volume_pct}% ({reason})");
        let report = RendererReport::new(
            RendererReportType::RndrSrvrVolumeChanged,
            Uuid::new_v4().to_string(),
            app.queue_state_snapshot().await.version,
            serde_json::json!({ "volume": volume_pct }),
        );
        if let Err(err) = app.send_renderer_report_command(report).await {
            log::warn!("[QConnect] Failed to report volume: {err}");
        }
    }

    /// DAEMON-ONLY (qconnect.initial_volume): defend the join-time volume, once.
    ///
    /// "Volume on connect" exists so an app that has never spoken to this player
    /// cannot push its own level — near full scale on the phone apps — at a
    /// system that may have no volume control after us. It was being defeated
    /// about a second after it applied, and silently:
    ///
    /// - the join reported our volume BEFORE `SetActive(true)` arrived, and the
    ///   cloud discards a report from a renderer it does not yet consider
    ///   active. Measured: every report sent while active is echoed back as
    ///   CTRL_VOLUME_CHANGED within ~30 ms; the join-time one was never echoed.
    /// - then the controller broadcast its own remembered level for us. That
    ///   arrives as CTRL_VOLUME_CHANGED (87), a controller-facing view, NOT as a
    ///   SetVolume command — so we correctly do not obey it, and the app and the
    ///   player were simply left disagreeing until someone touched the slider.
    ///
    /// So: re-report ours when the cloud's view of US disagrees shortly after we
    /// take the render. ONCE — the latch means a controller that insists still
    /// wins, and after the window volume belongs to whoever is holding the
    /// slider, which is the ordinary case this must not interfere with.
    async fn assert_join_volume(&self, payload: &Value) {
        use std::sync::atomic::Ordering;

        if self.join_volume_asserted.load(Ordering::SeqCst) {
            return;
        }
        let within_window = self
            .became_active_at
            .lock()
            .ok()
            .and_then(|at| *at)
            .is_some_and(|at| at.elapsed() < JOIN_VOLUME_ASSERT_WINDOW);
        if !within_window {
            return;
        }

        let Some(renderer_id) = payload.get("renderer_id").and_then(Value::as_i64) else {
            return;
        };
        let is_us = {
            let state = self.sync_state.lock().await;
            state.session.local_renderer_id == i32::try_from(renderer_id).ok()
        };
        if !is_us {
            return;
        }

        let Some(theirs) = payload.get("volume").and_then(Value::as_i64) else {
            return;
        };
        let ours = i64::from(self.engine.reported_volume_pct());
        if theirs == ours {
            return;
        }

        self.join_volume_asserted.store(true, Ordering::SeqCst);
        log::info!(
            "[QConnect] Controller says {theirs}% for us but we are at {ours}% — \
             re-asserting the join-time volume"
        );
        self.report_volume("join-time volume, re-asserted").await;
    }

    /// Apply a server session-management event by delegating the locked critical
    /// section to qconnect-app, then running the post-lock renderer-engine work
    /// the returned `SessionApplyOutcome` asks for. Mirrors the Tauri
    /// `apply_session_management_event`; the post-lock ordering (loop mode ->
    /// local-playback handoff -> projection -> freeze -> watchdog) is identical.
    /// DAEMON-ONLY: latch whether the session still renders HERE, for
    /// `/api/status`.
    ///
    /// `session_active` only says we hold a cloud connection, which stays true
    /// when the controller moves playback to its own speakers — so moOde kept
    /// the Qobuz overlay up after the app switched to local audio (reported by
    /// Tim Curtis). The renderer ids answer that one: the cloud sends no
    /// `SetActive(false)` for a switch-away, it simply names another renderer.
    ///
    /// But the ids cannot answer while our own renderer id is unresolved, and
    /// reading that window as "not active" would drop moOde's overlay
    /// mid-playback. So `local_renderer_role` keeps the window as `None` and we
    /// fall back to the last SetActive the renderer honoured, which is the
    /// cloud saying it outright.
    async fn latch_render_ownership(&self) {
        let is_active = {
            let state = self.sync_state.lock().await;
            qconnect_app::session::local_renderer_role(&state.session)
                .unwrap_or_else(|| state.local_render_active.unwrap_or(false))
        };
        if let Ok(mut shared) = self.shared.lock() {
            if shared.qconnect.is_active != is_active {
                log::info!(
                    "[QConnect] Render ownership: {}",
                    if is_active {
                        "this device"
                    } else {
                        "elsewhere"
                    }
                );
            }
            shared.qconnect.is_active = is_active;
        }
    }

    async fn apply_session_management_event(&self, message_type: &str, payload: &Value) {
        let Some(app) = self.app.get().and_then(Weak::upgrade) else {
            return;
        };
        let identity = resolve_local_identity();
        let outcome = app
            .apply_session_management_event(message_type, payload, &identity)
            .await;

        if let Some(loop_mode) = outcome.apply_loop_mode {
            if let Err(err) =
                qconnect_app::renderer::apply_remote_loop_mode(&self.engine, loop_mode).await
            {
                log::warn!("[QConnect] Failed to apply remote loop mode: {err}");
            }
        }

        if outcome.sync_local_playback {
            self.sync_local_playback_for_renderer_ownership().await;
        }

        if let Some(renderer_id) = outcome.remote_projection_renderer_id {
            self.sync_active_renderer_projection(renderer_id).await;
        }

        self.latch_render_ownership().await;

        if let Some(renderer_id) = outcome.disconnected_renderer_id {
            app.freeze_active_renderer_projection(
                renderer_id,
                QconnectAppEvent::RendererDisconnected { renderer_id },
            )
            .await;
        }

        if let Some((renderer_id, generation)) = outcome.watchdog_arm {
            app.arm_renderer_watchdog(renderer_id, generation);
        }

        // FIX #13: when the daemon transitions INTO controller mode (a PEER
        // becomes the active renderer), the peer's periodic state-update frames
        // carry `current_queue_item_id: null` (position-only), so on the
        // transition the cursor/projection can't resolve the peer's CURRENT track.
        // Fetch the peer's FULL state once on the false->true edge so the existing
        // align + projection resolve the real current track now.
        let peer_active_now = {
            let state = self.sync_state.lock().await;
            is_peer_renderer_active(&state.session)
        };
        let was_peer_active = self
            .last_peer_active
            .swap(peer_active_now, std::sync::atomic::Ordering::Relaxed);
        if peer_active_now && !was_peer_active {
            if let Err(err) = app.ask_for_active_renderer_state().await {
                log::warn!(
                    "[QConnect] controller entry: ask_for_active_renderer_state failed: {err}"
                );
            }
        }
    }

    /// When an active PEER renderer now owns playback, stop our local playback so
    /// the two don't double-play. Mirrors the Tauri helper, with the engine seam
    /// in place of `CoreBridge`.
    async fn sync_local_playback_for_renderer_ownership(&self) {
        let peer_renderer_active = {
            let state = self.sync_state.lock().await;
            is_peer_renderer_active(&state.session)
        };
        if !peer_renderer_active {
            return;
        }

        let playback_state = self.engine.get_playback_state();
        if playback_state.track_id == 0 {
            return;
        }

        log::info!(
            "[QConnect] Stopping local playback because active renderer is a peer (track_id={})",
            playback_state.track_id
        );
        if let Err(err) = self.engine.stop() {
            log::warn!("[QConnect] Failed to stop local playback after renderer handoff: {err}");
        }
    }

    /// Refresh the cached projection for the active renderer and, when a peer owns
    /// playback, align the local queue cursor to the peer's current track (so a
    /// later takeover lands on the right track). Mirrors the Tauri helper.
    async fn sync_active_renderer_projection(&self, renderer_id: i32) {
        let (queue_state, renderer_state, session_loop_mode, should_align_engine) = {
            let state = self.sync_state.lock().await;
            let Some(active_renderer_id) = state.session.active_renderer_id else {
                return;
            };
            if active_renderer_id != renderer_id {
                return;
            }

            (
                state.last_remote_queue_state.clone(),
                state
                    .session_renderer_states
                    .get(&active_renderer_id)
                    .cloned(),
                state.session_loop_mode,
                state.session.local_renderer_id != Some(active_renderer_id),
            )
        };

        let (Some(queue_state), Some(renderer_state)) = (queue_state, renderer_state) else {
            return;
        };

        let renderer_snapshot =
            build_session_renderer_snapshot(&queue_state, Some(&renderer_state), session_loop_mode);
        {
            let mut state = self.sync_state.lock().await;
            cache_renderer_snapshot(&mut state, &renderer_snapshot);
        }

        if !should_align_engine {
            return;
        }

        let Some(current_track) = renderer_snapshot.current_track.as_ref() else {
            return;
        };

        if let Err(err) =
            qconnect_app::renderer::align_queue_cursor(&self.engine, current_track.track_id).await
        {
            log::warn!("[QConnect] Failed to sync peer renderer cursor into engine: {err}");
        }
    }
}

#[async_trait]
impl QconnectEventSink for DaemonEventSink {
    async fn on_event(&self, event: QconnectAppEvent) {
        match &event {
            QconnectAppEvent::SessionManagementEvent {
                message_type,
                payload,
            } => {
                log::info!(
                    "[QConnect] Session management: {} payload={}",
                    message_type,
                    serde_json::to_string(payload).unwrap_or_else(|_| "?".to_string())
                );
                self.apply_session_management_event(message_type, payload)
                    .await;
                if message_type == "MESSAGE_TYPE_SRVR_CTRL_VOLUME_CHANGED" {
                    self.assert_join_volume(payload).await;
                }
            }
            QconnectAppEvent::RendererUpdated(renderer_state) => {
                log::info!(
                    "[QConnect] Renderer updated: playing_state={:?} volume={:?} position={:?}",
                    renderer_state.playing_state,
                    renderer_state.volume,
                    renderer_state.current_position_ms,
                );
                let mut sync_state = self.sync_state.lock().await;
                cache_renderer_snapshot(&mut sync_state, renderer_state);
            }
            QconnectAppEvent::QueueUpdated(queue_state) => {
                log::debug!(
                    "[QConnect] QueueUpdated: items={} shuffle_mode={} version={}.{}",
                    queue_state.queue_items.len(),
                    queue_state.shuffle_mode,
                    queue_state.version.major,
                    queue_state.version.minor,
                );
                {
                    let mut sync_state = self.sync_state.lock().await;
                    sync_state.last_remote_queue_state = Some(queue_state.clone());
                }
                if let Err(err) = qconnect_app::renderer::materialize_remote_queue(
                    &self.engine,
                    &self.sync_state,
                    queue_state,
                )
                .await
                {
                    log::warn!("[QConnect] Failed to materialize remote queue: {err}");
                }
            }
            QconnectAppEvent::RendererCommandApplied { command, state } => {
                log::info!("[QConnect] Renderer command applied: {:?}", command);
                let became_active = matches!(command, RendererCommand::SetActive { active: true });
                // A real SetVolume means the controller has actually told us a
                // level, so there is nothing stale to correct — stand the
                // join-time assertion down for good. This is what keeps the
                // assertion off the ordinary path entirely: it can only ever
                // fire while the controller has said nothing and merely SHOWS a
                // different number, never against someone moving the slider.
                if matches!(command, RendererCommand::SetVolume { .. }) {
                    self.join_volume_asserted
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
                let sets_active = matches!(command, RendererCommand::SetActive { .. });
                if let Err(err) = qconnect_app::renderer::apply_renderer_command(
                    &self.engine,
                    &self.sync_state,
                    command,
                    state,
                )
                .await
                {
                    log::warn!("[QConnect] Failed to apply renderer command: {err}");
                } else if became_active {
                    self.report_active_renderer_ready().await;
                    // Now, not at join. The join's own volume report goes out
                    // before this command arrives, and the cloud discards a
                    // report from a renderer it does not yet consider active —
                    // which is how qconnect.initial_volume ended up invisible to
                    // the app. See `assert_join_volume`.
                    {
                        use std::sync::atomic::Ordering;
                        if let Ok(mut at) = self.became_active_at.lock() {
                            *at = Some(std::time::Instant::now());
                        }
                        self.join_volume_asserted.store(false, Ordering::SeqCst);
                    }
                    self.report_volume("took the render").await;
                }
                // DAEMON-ONLY: re-latch the published role. The cloud states it
                // outright in this message, and until now the latch ran ONLY on
                // session-management events — so `/api/status` could still say
                // `is_active: true` after a SetActive(false) had already stopped
                // the engine, and moOde kept the overlay over a silent player.
                // Runs on the error path too: whatever the engine did or failed
                // to do, the ids and the honoured flag are the truth we publish.
                if sets_active {
                    self.latch_render_ownership().await;
                }
            }
            QconnectAppEvent::RendererUnreachable { renderer_id } => {
                // Slint copy surfaced a toast here — daemon logs it (§1.4).
                log::warn!("[QConnect] Renderer {renderer_id} unreachable");
            }
            QconnectAppEvent::RendererDisconnected { renderer_id } => {
                // Slint copy surfaced a toast here — daemon logs it (§1.4).
                log::warn!("[QConnect] Renderer {renderer_id} disconnected");
            }
            QconnectAppEvent::PlaybackError {
                queue_item_id,
                error_type,
                ..
            } => {
                // Slint copy surfaced a toast here — daemon logs it (§1.4).
                log::warn!(
                    "[QConnect] Playback error on queue_item {queue_item_id}: {error_type:?}"
                );
            }
            QconnectAppEvent::ResyncComplete => {
                log::info!("[QConnect] Post-reconnect resync complete");
            }
            QconnectAppEvent::LifecycleChanged { state } => {
                log::info!("[QConnect] Lifecycle -> {state:?}");
            }
            QconnectAppEvent::Diagnostic { channel, level, .. } => {
                log::debug!("[QConnect] diagnostic {channel} [{level}]");
            }
            _ => {}
        }
    }
}
