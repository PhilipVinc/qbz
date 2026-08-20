// TODO(converge: qconnect-glue) — copied from crates/qbz/src/qconnect_service.rs @ 5d50158e;
// do not fix bugs here without fixing the source, and vice versa.
//
//! Daemon session-loop host + controller bootstrap + deferred renderer-join.
//!
//! [`DaemonSessionLoopHost`] implements the frontend-agnostic
//! `qconnect_app::SessionLoopHost` so the shared `run_session_loop` drives
//! lifecycle, reconnect bootstrap/resync, deferred renderer-join, and
//! reconnect-exhausted teardown through this adapter — exactly as
//! `SlintSessionLoopHost` does.
//!
//! Two DELIBERATE daemon-copy behavior fixes vs. the desktop (OD2 — desktop call
//! sites untouched; owner may fold these back into the shared path later):
//!   (a) `deferred_renderer_join` reports the player's REAL volume instead of the
//!       hardcoded `json!({"volume": 100})` (qconnect_service.rs:2711-2715; §7.4).
//!   (b) `bootstrap_after_reconnect` RE-RESOLVES transport credentials via
//!       `resolve_transport_config` so a reconnect after JWT expiry recovers
//!       (IV3 — the WS transport reuses `last_config` and never re-resolves
//!       `/qws/createToken`; design-input/qconnect-headless.md §7.6.7).
//!
//! The desktop's D5 offline-mode parks are dropped: qbzd has no offline MODE.

use std::sync::Arc;

use qbz_app::shell::AppRuntime;
use qconnect_app::renderer::PLAYING_STATE_STOPPED;
use qconnect_app::{
    QconnectLifecycleState, QconnectRemoteSyncState, QueueCommandType, RendererReport,
    RendererReportType, SessionLoopHost, JOIN_SESSION_REASON_RECONNECTION,
};
use serde_json::json;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::adapter::DaemonAdapter;
use crate::state::DaemonShared;
use super::engine::VolumeMode; // T10 (OD4): join-time volume report honors the mode
use super::sink::{DaemonEventSink, DaemonQconnectApp};
use super::transport::{
    default_qconnect_device_info, default_qconnect_device_info_with_name, resolve_transport_config,
    QconnectJoinSessionRequest, AUDIO_QUALITY_HIRES_LEVEL2, BUFFER_STATE_OK,
};
use super::{update_lifecycle_state_if_running, DaemonQconnectInner};

type Runtime = Arc<AppRuntime<DaemonAdapter>>;

/// Daemon-side implementation of the shared session-loop seams. Holds the handles
/// the loop reaches back into: the app (renderer join + state reads), the shared
/// sync accumulator, the service inner (lifecycle gating + teardown + config
/// re-latch), the sink (lifecycle emit), the runtime (track-duration + volume
/// reads), and the daemon shared state (status latching for `/api/status`).
pub struct DaemonSessionLoopHost {
    pub app: Arc<DaemonQconnectApp>,
    pub sync_state: Arc<Mutex<QconnectRemoteSyncState>>,
    pub inner: Arc<Mutex<DaemonQconnectInner>>,
    pub sink: Arc<DaemonEventSink>,
    pub runtime: Runtime,
    pub shared: Arc<std::sync::Mutex<DaemonShared>>,
    /// T10 (OD4): resolved volume policy — the deferred renderer join reports 100
    /// in `Locked` mode, the real player volume in `Software`.
    pub volume_mode: VolumeMode,
    /// DAEMON-ONLY (pairing): reconnect credential re-resolve prefers a live
    /// handed-over token, mirroring the preference in `connect()`.
    pub pairing_store: super::pairing::PairingStore,
    /// DAEMON-ONLY (pairing): one-shot latch armed by a handoff takeover. The
    /// handoff IS the user's output selection, so the next renderer join must
    /// claim the active slot (official receivers join with is_active=true
    /// after a handoff — qobuz-proxy/StreamCore32 parity). Joining available
    /// leaves the session's active renderer pointing at the PREVIOUS output
    /// and the app's play command never reaches this device.
    pub handoff_join_pending: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait::async_trait]
impl SessionLoopHost for DaemonSessionLoopHost {
    async fn update_lifecycle(&self, state: QconnectLifecycleState) {
        update_lifecycle_state_if_running(&self.inner, &self.sink, &self.shared, state).await;
    }

    async fn bootstrap_after_reconnect(&self) {
        // FIX (b) (IV3, daemon-copy only): the WS transport reuses `last_config`
        // on in-loop reconnects and never re-resolves `/qws/createToken`, so a
        // reconnect after the JWT expires keeps failing. Re-resolve the transport
        // credentials (fresh `/qws/createToken`) and latch the new config into the
        // runtime so a subsequent full reconnect uses valid credentials. The
        // desktop copy does NOT do this.
        // DAEMON-ONLY (pairing): same source order as `connect()` — a live
        // handed-over token outranks a fresh `/qws/createToken` (the paired
        // session must survive its own reconnects, not fall back to the
        // daemon account's session).
        let fresh = match super::pairing::valid_ws_tokens(&self.pairing_store) {
            Some(tokens) => Ok(super::pairing::transport_config_from(&tokens)),
            None => resolve_transport_config(&self.runtime).await,
        };
        match fresh {
            Ok(fresh) => {
                let mut guard = self.inner.lock().await;
                if let Some(rt) = guard.runtime.as_mut() {
                    rt.config = fresh;
                }
                log::info!("[QConnect] reconnect: re-resolved transport credentials (fresh JWT)");
            }
            Err(err) => {
                log::warn!("[QConnect] reconnect: credential re-resolve failed: {err}");
            }
        }
        if let Err(err) = bootstrap_remote_presence(&self.app, None).await {
            log::error!("[QConnect] Re-bootstrap after reconnect failed: {err}");
        }
    }

    async fn deferred_renderer_join(&self, session_uuid: String, reason: i32) {
        // DAEMON-ONLY (pairing): consume the handoff latch exactly once.
        let force_active = self
            .handoff_join_pending
            .swap(false, std::sync::atomic::Ordering::SeqCst);
        deferred_renderer_join(
            &self.app,
            &self.sync_state,
            &self.runtime,
            self.volume_mode, // T10 (OD4): 100 in Locked, real in Software
            &session_uuid,
            reason,
            force_active,
        )
        .await;
    }

    async fn on_reconnect_exhausted(
        &self,
        attempts: u32,
        last_reason: String,
        idle_retry_active: bool,
    ) -> bool {
        {
            let mut guard = self.inner.lock().await;
            guard.lifecycle_state = QconnectLifecycleState::Exhausted;
            guard.last_error = Some(format!(
                "Reconnect attempts exhausted ({attempts}): {last_reason}"
            ));
            if !idle_retry_active {
                // Legacy terminate path: drop the runtime so a fresh connect()
                // succeeds. Dropping it detaches this task's own JoinHandle (fine
                // — the loop breaks right after).
                guard.runtime = None;
            }
        }
        if let Ok(mut s) = self.shared.lock() {
            s.qconnect.state = "exhausted".to_string();
            s.qconnect.session_active = false;
            // 01 §9.3: reconnect-exhausted is a real network-class failure —
            // latch `network.online` false (the success side latches true in
            // `latch_lifecycle_into_shared` on the next `Connected` transition).
            s.set_network_online(false);
            s.emit_qconnect_session_changed();
        }
        log::warn!("[QConnect] Reconnect exhausted ({attempts}): {last_reason}");
        // Daemon has no offline MODE, so the desktop's idle-retry offline park is
        // dropped: keep idling (the transport's internal 60s idle rearm keeps
        // trying) unless idle-retry is off, in which case break + drop.
        !idle_retry_active
    }

    async fn on_loop_error(&self, message: String) {
        // Slint copy surfaced a toast here — daemon logs it.
        log::error!("[QConnect] session loop error: {message}");
    }
}

/// Controller-side bootstrap: JoinSession (works without a session_uuid) then ask
/// for the current queue state. The renderer-side join is deferred until the
/// server sends SESSION_STATE with a session_uuid (handled in the session loop).
/// Mirrors the Tauri `bootstrap_remote_presence`.
pub async fn bootstrap_remote_presence(
    app: &Arc<DaemonQconnectApp>,
    custom_device_name: Option<String>,
) -> Result<(), String> {
    let device_info = default_qconnect_device_info_with_name(custom_device_name.as_deref());

    let join_payload = serde_json::to_value(QconnectJoinSessionRequest {
        session_uuid: None,
        device_info: Some(device_info),
    })
    .map_err(|err| format!("serialize join_session bootstrap payload: {err}"))?;

    let join_command = app
        .build_queue_command(QueueCommandType::CtrlSrvrJoinSession, join_payload)
        .await;
    let join_action_uuid = app
        .send_queue_command(join_command)
        .await
        .map_err(|err| format!("send bootstrap ctrl_srvr_join_session failed: {err}"))?;
    // JoinSession responds with session/renderer controller events not part of
    // queue reducer correlation. Drop the pending slot so queue ops aren't blocked.
    app.clear_pending_if_matches(&join_action_uuid).await;

    let ask_queue_command = app
        .build_queue_command(QueueCommandType::CtrlSrvrAskForQueueState, json!({}))
        .await;
    let ask_action_uuid = app
        .send_queue_command(ask_queue_command)
        .await
        .map_err(|err| format!("send bootstrap ask_for_queue_state failed: {err}"))?;
    app.clear_pending_if_matches(&ask_action_uuid).await;

    log::info!(
        "[QConnect] Bootstrap complete: controller joined, queue state requested. Renderer join deferred until session_uuid received."
    );
    Ok(())
}

/// Deferred renderer join: called from the session loop when SESSION_STATE with a
/// session_uuid arrives. Idempotent per uuid (P1-8). Mirrors the Tauri
/// `deferred_renderer_join`, reading the current track duration via
/// `runtime.core().get_track` and the current volume via
/// `runtime.core().get_playback_state()`.
pub async fn deferred_renderer_join(
    app: &Arc<DaemonQconnectApp>,
    sync_state: &Arc<Mutex<QconnectRemoteSyncState>>,
    runtime: &Runtime,
    volume_mode: VolumeMode, // T10 (OD4): join-time volume report policy
    session_uuid: &str,
    join_reason: i32,
    force_active: bool, // DAEMON-ONLY (pairing): handoff join claims the render
) {
    let already_joined = {
        let st = sync_state.lock().await;
        st.last_joined_session_uuid.as_deref() == Some(session_uuid)
    };
    if already_joined {
        log::info!(
            "[QConnect] Deferred join skipped (already joined session_uuid={session_uuid}); re-asking renderer state"
        );
        if let Err(err) = app.ask_for_active_renderer_state().await {
            log::warn!("[QConnect] Idempotent-join AskForRendererState failed: {err}");
        }
        return;
    }

    let device_info = default_qconnect_device_info();
    let queue_version_ref = app.queue_state_snapshot().await.version;

    log::info!("[QConnect] Deferred renderer join with session_uuid={session_uuid}");

    // 1. Renderer JoinSession with session_uuid.
    // Do NOT auto-steal the render on a fresh connect: join as an AVAILABLE
    // renderer (is_active=false), not the active one. Only a post-drop
    // RECONNECTION rejoins as active, so a network blip mid-render does not lose
    // the render. DAEMON-ONLY (pairing) exception: a join right after a local
    // handoff claims the render (`force_active`) — the handoff is the user's
    // output selection and no separate SET_ACTIVE_RENDERER ever arrives.
    let join_as_active = force_active || join_reason == JOIN_SESSION_REASON_RECONNECTION;
    let renderer_join_payload = json!({
        "session_uuid": session_uuid,
        "device_info": serde_json::to_value(&device_info).unwrap_or_default(),
        "is_active": join_as_active,
        "reason": join_reason,
        "initial_state": {
            "playing_state": PLAYING_STATE_STOPPED,
            "buffer_state": BUFFER_STATE_OK,
            "current_position": 0,
            "duration": 0,
            "queue_version": {
                "major": queue_version_ref.major,
                "minor": queue_version_ref.minor
            }
        }
    });
    let renderer_join_report = RendererReport::new(
        RendererReportType::RndrSrvrJoinSession,
        Uuid::new_v4().to_string(),
        queue_version_ref,
        renderer_join_payload,
    );
    if let Err(err) = app.send_renderer_report_command(renderer_join_report).await {
        log::error!("[QConnect] Deferred renderer join failed: {err}");
        return;
    }

    // 2. Initial StateUpdated report. At join time (e.g. reconnect mid-playback)
    // we may already have a current track, so resolve the real duration + current/
    // next queue_item_ids instead of hardcoding nulls.
    let renderer = app.renderer_state_snapshot().await;
    let queue = app.queue_state_snapshot().await;
    let current_track_id = renderer.current_track.as_ref().map(|item| item.track_id);
    let (current_qid, next_qid, _) = current_track_id
        .map(|tid| {
            qconnect_app::queue_resolution::resolve_queue_item_ids_from_queue_state(&queue, tid)
        })
        .unwrap_or((None, None, None));
    let duration_secs = match current_track_id {
        Some(track_id) => runtime
            .core()
            .get_track(track_id)
            .await
            .map(|track| u64::from(track.duration))
            .unwrap_or(0),
        None => 0,
    };
    // FIX (c) (daemon-copy only): report the player's REAL state, not a
    // hardcoded stopped-at-zero. A RECONNECTION rejoin (and a pairing takeover
    // that lands mid-playback) happens while audio is live, and announcing
    // "stopped, 0:00" made the controller show 0:00 and treat us as idle until
    // the next report tick corrected it. Same reasoning as FIX (a) below for
    // volume. Position is MILLISECONDS on the wire; the player reports seconds.
    let live = runtime.core().get_playback_state();
    let live_is_current = live.track_id != 0 && Some(live.track_id) == current_track_id;
    // FIX (c2) (daemon-copy only): only state what we actually know, and leave
    // every unknown field out. A PAIRING HANDOFF joins as active with no
    // renderer state at all — the controller's SetState is still in flight — so
    // `live_is_current` is false, and announcing "STOPPED, 0:00 of 0:00" there
    // drew a pause icon at zero over the position the app was already showing
    // (hand off at 2:19 and it flashed 0:00). A null field means "no change", so
    // the app keeps its own view for the ~1 s until the report scheduler sends
    // the real state off the buffering latch. Claiming PLAYING + BUFFERING here
    // instead would be a guess: if the SetState never arrives — an empty queue,
    // a dropped frame — this report is the last word and the spinner never
    // stops.
    let (report_playing_state, report_buffer_state) = if live_is_current && live.is_playing {
        (
            Some(qconnect_app::renderer::PLAYING_STATE_PLAYING),
            Some(BUFFER_STATE_OK),
        )
    } else if force_active {
        (None, None)
    } else {
        (Some(PLAYING_STATE_STOPPED), Some(BUFFER_STATE_OK))
    };
    // `duration` is MILLISECONDS on the wire like `current_position` — it was
    // sent in seconds here, so even a known duration rendered as 0:00.
    let state_report_payload = json!({
        "playing_state": report_playing_state,
        "buffer_state": report_buffer_state,
        "current_position": live_is_current.then(|| (live.position as i64) * 1000),
        "duration": (duration_secs > 0).then(|| (duration_secs as i64) * 1000),
        "current_queue_item_id": current_qid,
        "next_queue_item_id": next_qid,
        "queue_version": {
            "major": queue_version_ref.major,
            "minor": queue_version_ref.minor
        }
    });
    let state_report = RendererReport::new(
        RendererReportType::RndrSrvrStateUpdated,
        Uuid::new_v4().to_string(),
        queue_version_ref,
        state_report_payload,
    );
    if let Err(err) = app.send_renderer_report_command(state_report).await {
        log::error!("[QConnect] Deferred renderer state report failed: {err}");
    }

    // 3. Report volume and max audio quality.
    // FIX (a) (§7.4, daemon-copy only): report the player's REAL volume, not the
    // desktop's hardcoded 100. `get_playback_state().volume` is a 0.0-1.0
    // fraction; the wire wants a 0-100 percent.
    // T10 (OD4): the volume MODE decides — `Software` reports the real percent,
    // `Locked` reports 100 fixed (no software attenuation on the bit-perfect path).
    let volume_pct = volume_mode.reported_volume_pct(runtime.core().get_playback_state().volume);
    let volume_report = RendererReport::new(
        RendererReportType::RndrSrvrVolumeChanged,
        Uuid::new_v4().to_string(),
        queue_version_ref,
        json!({ "volume": volume_pct }),
    );
    if let Err(err) = app.send_renderer_report_command(volume_report).await {
        log::error!("[QConnect] Deferred renderer volume report failed: {err}");
    }

    let max_quality_report = RendererReport::new(
        RendererReportType::RndrSrvrMaxAudioQualityChanged,
        Uuid::new_v4().to_string(),
        queue_version_ref,
        json!({ "max_audio_quality": AUDIO_QUALITY_HIRES_LEVEL2 }),
    );
    if let Err(err) = app.send_renderer_report_command(max_quality_report).await {
        log::error!("[QConnect] Deferred renderer max quality report failed: {err}");
    }

    log::info!("[QConnect] Deferred renderer join complete");

    // Re-request session state so the server sends an updated renderer list
    // (including ourselves).
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let refresh_command = app
        .build_queue_command(QueueCommandType::CtrlSrvrAskForQueueState, json!({}))
        .await;
    if let Ok(action_uuid) = app.send_queue_command(refresh_command).await {
        app.clear_pending_if_matches(&action_uuid).await;
        log::info!("[QConnect] Re-requested session state after renderer join");
    }

    // Resync the active renderer's full state too, so a reconnect rejoin restores
    // renderer state.
    if let Err(err) = app.ask_for_active_renderer_state().await {
        log::warn!("[QConnect] Post-join AskForRendererState failed: {err}");
    }

    // Record this session_uuid so a subsequent SESSION_STATE with the same uuid
    // takes the idempotent fast-path above.
    {
        let mut st = sync_state.lock().await;
        st.last_joined_session_uuid = Some(session_uuid.to_string());
    }
}
