// TODO(converge: qconnect-glue) — ported from crates/qbz/src/qconnect_service.rs
// `sync_local_queue_if_changed` @ f18960ba; do not fix bugs here without fixing
// the source, and vice versa.
//
//! Daemon-side local-queue -> Connect-cloud publish (the desktop's
//! `sync_local_queue_if_changed`, qconnect_service.rs:875).
//!
//! The daemon was queue RECEIVE-ONLY: a daemon-originated queue (CLI/TUI/MPRIS/
//! restored session) never reached the cloud, so controllers rendered a
//! different queue than the one actually playing (design doc had flagged this
//! as knowingly unported — design-input/qconnect-headless.md:250-252).
//!
//! The gates are the desktop's EXACT set, in the same order: live runtime ->
//! `is_local_renderer_active` (a peer owns playback -> the peer publishes) ->
//! offline-only skip -> non-empty -> echo-suppress vs the cloud's last-applied
//! queue -> per-session `last_pushed_queue_ids` latch -> all-or-nothing
//! admission (any local/Plex track refuses the WHOLE push; offline
//! qobuz_download stays eligible — its id is the real Qobuz id). The desktop
//! toasts on refusal; the daemon logs.
//!
//! Trigger: the desktop calls it on every track transition from its poll loop.
//! The daemon instead runs a debounced `CoreEvent::QueueUpdated` subscriber
//! (same pattern as `daemon.rs::spawn_queue_persist`), which ALSO covers queue
//! edits while paused/stopped — a transition-only hook would miss those.

use std::sync::Arc;

use qbz_app::shell::AppRuntime;
use qbz_models::CoreEvent;
use qconnect_app::{is_local_renderer_active, QueueCommandType};
use serde_json::json;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::DaemonQconnectInner;
use crate::adapter::DaemonAdapter;

/// Push the local core queue to the Connect session when it differs from the
/// cloud's. No-op under any of the gates listed in the module docs. Echo-safe
/// by construction: the inbound materialize path sets `last_applied_queue_state`
/// to the very queue it materialized locally, so a controller-pushed queue
/// compares equal and is never bounced back.
pub async fn publish_local_queue_if_changed(
    inner: &Arc<Mutex<DaemonQconnectInner>>,
    runtime: &Arc<AppRuntime<DaemonAdapter>>,
) {
    let (app, sync_state) = {
        let guard = inner.lock().await;
        match guard.runtime.as_ref() {
            Some(rt) => (Arc::clone(&rt.app), Arc::clone(&rt.sync_state)),
            None => return,
        }
    };

    // Only push while WE are the active renderer (the user is driving the
    // daemon). When a peer owns playback, the peer publishes its own queue.
    {
        let state = sync_state.lock().await;
        if !is_local_renderer_active(&state.session) {
            return;
        }
    }

    // A queue built from an OFFLINE-ONLY local playlist never reaches the
    // Connect cloud. Debug level — this runs after every queue mutation and
    // must not spam the log.
    if runtime.core().queue_is_offline_only() {
        log::debug!("[QConnect] queue is from an offline-only playlist; skipping cloud push");
        return;
    }

    let (tracks, current_index) = runtime.core().get_all_queue_tracks().await;
    if tracks.is_empty() {
        return;
    }
    let ordered_ids: Vec<u64> = tracks.iter().map(|track| track.id).collect();

    // Echo-suppress: skip when this is the cloud's current queue (materialized
    // inbound) so our own adoption / a remote queue change never bounces back.
    {
        let state = sync_state.lock().await;
        if let Some(applied) = &state.last_applied_queue_state {
            let applied_ids: Vec<u64> = applied
                .queue_items
                .iter()
                .map(|item| item.track_id)
                .collect();
            if applied_ids == ordered_ids {
                return;
            }
        }
    }
    // ...and skip when we already pushed this exact queue (cloud echo pending).
    {
        let guard = inner.lock().await;
        if guard.last_pushed_queue_ids.as_deref() == Some(ordered_ids.as_slice()) {
            return;
        }
    }

    // Admission: refuse the whole push if any track isn't Qobuz-castable.
    let all_eligible = tracks.iter().all(|track| {
        let source = track
            .source
            .as_deref()
            .unwrap_or("qobuz")
            .to_ascii_lowercase();
        source != "local" && source != "plex" && track.id > 0
    });
    if !all_eligible {
        log::info!("[QConnect] Local queue has non-Qobuz tracks; not casting to Connect");
        // Remember it so we don't re-log on every queue event within this queue.
        let mut guard = inner.lock().await;
        guard.last_pushed_queue_ids = Some(ordered_ids);
        return;
    }

    let count = ordered_ids.len();
    let track_ids: Vec<i64> = ordered_ids.iter().map(|id| *id as i64).collect();
    let start_index = current_index.unwrap_or(0);
    let payload = json!({
        "track_ids": track_ids,
        "queue_position": start_index,
        "shuffle_mode": false,
        "shuffle_pivot_index": start_index,
        "context_uuid": Uuid::new_v4().to_string(),
        "autoplay_reset": true,
        "autoplay_loading": false,
    });
    let command = app
        .build_queue_command(QueueCommandType::CtrlSrvrQueueLoadTracks, payload)
        .await;
    match app.send_queue_command(command).await {
        Ok(_) => {
            log::info!(
                "[QConnect] Pushed local queue to Connect ({count} tracks, start={start_index})"
            );
            let mut guard = inner.lock().await;
            guard.last_pushed_queue_ids = Some(ordered_ids);
        }
        Err(err) => log::warn!("[QConnect] Failed to push local queue: {err}"),
    }
}

/// The queue-publish subscriber: debounces `CoreEvent::QueueUpdated` bursts by
/// 2 s (same ritual as `daemon.rs::spawn_queue_persist`), then runs
/// [`publish_local_queue_if_changed`]. Non-queue events are drained WITHOUT
/// extending the debounce window, so they can never starve the publish. Holds
/// `Arc` clones of the qconnect inner + the runtime, so the handle is
/// aborted+joined in `QconnectHandle::shutdown` ahead of `drop(booted)` (the
/// #521 ordering), exactly like the report scheduler.
pub fn spawn_queue_cloud_publish(
    inner: Arc<Mutex<DaemonQconnectInner>>,
    runtime: Arc<AppRuntime<DaemonAdapter>>,
    mut rx: broadcast::Receiver<CoreEvent>,
) -> JoinHandle<()> {
    use tokio::sync::broadcast::error::RecvError;
    const DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(2);
    tokio::spawn(async move {
        loop {
            // Block until the FIRST queue mutation of a burst.
            match rx.recv().await {
                Ok(CoreEvent::QueueUpdated { .. }) => {}
                Ok(_) => continue,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return,
            }
            // Debounce: a fixed deadline that only a further QueueUpdated extends.
            let mut deadline = tokio::time::Instant::now() + DEBOUNCE;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => break,
                    r = rx.recv() => match r {
                        Ok(CoreEvent::QueueUpdated { .. }) => {
                            deadline = tokio::time::Instant::now() + DEBOUNCE;
                        }
                        Ok(_) => {}
                        Err(RecvError::Lagged(_)) => {}
                        Err(RecvError::Closed) => return,
                    }
                }
            }
            publish_local_queue_if_changed(&inner, &runtime).await;
        }
    })
}
