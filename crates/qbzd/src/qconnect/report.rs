// TODO(converge: qconnect-glue) — copied from crates/qbz/src/qconnect_service.rs @ 5d50158e;
// do not fix bugs here without fixing the source, and vice versa.
//
//! Renderer playback-state report (the UI-free body of the desktop
//! `report_playback_state`, qconnect_service.rs:592).
//!
//! Daemon adaptation vs. the Slint copy (§1.4): the desktop `report_playback_state`
//! is a method on `SlintQconnectService` driven by the Slint playback POLL LOOP;
//! here it is a free function the T10 report tick calls on a tokio interval. No
//! behavior change — it still self-gates on `is_local_renderer_active`, resolves
//! current/next queue_item_id from the playing track, sends a
//! `RndrSrvrStateUpdated`, keeps the app's renderer position in sync, and reports
//! the live output format for the controller's quality badge. `position_ms` /
//! `duration_ms` are MILLISECONDS (the QConnect protocol unit).
#![allow(dead_code)]

use std::sync::Arc;

use qbz_app::shell::AppRuntime;
use qconnect_app::{
    is_local_renderer_active, QconnectFileAudioQualitySnapshot, QconnectRemoteSyncState,
    RendererReport, RendererReportType,
};
use serde_json::json;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::adapter::DaemonAdapter;
use super::sink::DaemonQconnectApp;
use super::transport::{BUFFER_STATE_BUFFERING, BUFFER_STATE_OK};

pub const QCONNECT_RENDERER_CHANNELS: i32 = 2;
const AUDIO_QUALITY_UNKNOWN: i32 = 0;
const AUDIO_QUALITY_MP3: i32 = 1;
const AUDIO_QUALITY_CD: i32 = 2;
const AUDIO_QUALITY_HIRES_L1: i32 = 3;
const AUDIO_QUALITY_HIRES_L2: i32 = 4;
const AUDIO_QUALITY_HIRES_L3: i32 = 5;

/// Report this device's playback state to the cloud while the daemon is the
/// ACTIVE LOCAL renderer. Self-gates on `is_local_renderer_active` (no-op when a
/// PEER owns playback), resolves the current/next queue_item_id from the playing
/// track, sends a `RndrSrvrStateUpdated`, and keeps the app's renderer position
/// in sync.
pub async fn report_playback_state(
    app: &Arc<DaemonQconnectApp>,
    sync_state: &Arc<Mutex<QconnectRemoteSyncState>>,
    runtime: &Arc<AppRuntime<DaemonAdapter>>,
    playing_state: i32,
    position_ms: i64,
    duration_ms: i64,
    track_id: u64,
    buffer_state: i32,
) {
    // Only report when WE are the active renderer. When a peer renderer owns
    // playback (the daemon is acting as a controller) the renderer reports come
    // from the peer, not us.
    {
        let state = sync_state.lock().await;
        if !is_local_renderer_active(&state.session) {
            return;
        }
    }

    let (current_qid, next_qid) =
        resolve_queue_item_ids_by_track_id(app, sync_state, track_id).await;
    let queue_version = app.queue_state_snapshot().await.version;

    let report = RendererReport::new(
        RendererReportType::RndrSrvrStateUpdated,
        Uuid::new_v4().to_string(),
        queue_version,
        json!({
            "playing_state": playing_state,
            "buffer_state": buffer_state,
            "current_position": position_ms,
            "duration": duration_ms,
            "current_queue_item_id": current_qid,
            "next_queue_item_id": next_qid,
            "queue_version": {
                "major": queue_version.major,
                "minor": queue_version.minor
            }
        }),
    );
    if let Err(err) = app.send_renderer_report_command(report).await {
        log::warn!("[QConnect] Failed to report playback state: {err}");
    }

    // Not while buffering: `renderer.current_position_ms` is the HIGHER-priority
    // input to the renderer's load-offset and seek decisions, so publishing the
    // offset a stream is still filling toward makes a later state-only SetState
    // (a bare pause/resume, carrying no position of its own) compare the player's
    // real clock against it and fire a seek to a track that is not playing yet.
    // The in-flight offset is for the CONTROLLER's benefit only.
    if position_ms >= 0 && buffer_state != BUFFER_STATE_BUFFERING {
        app.update_renderer_position(position_ms as u64).await;
    }

    // Report the live output format so the controller shows the correct quality
    // badge (CD / Hi-Res). Reads the player's current output (sample_rate/
    // bit_depth); channels default to stereo. Both reports dedup internally in
    // qconnect-app, so calling them every report tick is cheap.
    let player = runtime.core().player();
    let sample_rate = player.state.get_sample_rate();
    let bit_depth = player.state.get_bit_depth();
    if let Some(snapshot) =
        build_file_audio_quality_snapshot(sample_rate, bit_depth, QCONNECT_RENDERER_CHANNELS)
    {
        if let Err(err) = app
            .report_file_audio_quality_if_changed(queue_version, snapshot)
            .await
        {
            log::warn!("[QConnect] Failed to report file audio quality: {err}");
        }
        // FIX (daemon-copy only): the DEVICE report must describe what the DAC
        // is actually receiving, not the source file. Both reports used to
        // carry the stream format, so a device resampling 24/96 down to 24/48
        // still told the controller it was running 24/96 — the protocol has
        // separate File and Device messages precisely to distinguish them.
        // /proc/asound carries the negotiated hardware rate; fall back to the
        // stream format when nothing is open (nothing better to say).
        let device = qbz_audio::dac_probe::negotiated_active_rate();
        let (device_rate, device_channels) = match &device {
            Some(negotiated) => (
                negotiated.sample_rate as i32,
                negotiated.channels as i32,
            ),
            None => (snapshot.sampling_rate, snapshot.nb_channels),
        };
        // ALSA reports a container format (24-bit audio commonly rides in
        // S32_LE), so the container width would overstate the real depth —
        // keep the stream's bit depth, which is the honest number.
        if let Err(err) = app
            .report_device_audio_quality_if_changed(
                queue_version,
                device_rate,
                snapshot.bit_depth,
                device_channels,
            )
            .await
        {
            log::warn!("[QConnect] Failed to report device audio quality: {err}");
        }
    }
}

/// Classify a (sample_rate, bit_depth) output into the QConnect AudioQuality
/// level. Pure mirror of the Tauri `classify_qconnect_audio_quality`.
fn classify_audio_quality(sample_rate: u32, bit_depth: u32) -> i32 {
    if sample_rate == 0 || bit_depth == 0 {
        AUDIO_QUALITY_UNKNOWN
    } else if sample_rate >= 384_000 {
        AUDIO_QUALITY_HIRES_L3
    } else if sample_rate >= 192_000 {
        AUDIO_QUALITY_HIRES_L2
    } else if bit_depth > 16 || sample_rate > 48_000 {
        AUDIO_QUALITY_HIRES_L1
    } else if sample_rate >= 44_100 {
        AUDIO_QUALITY_CD
    } else {
        AUDIO_QUALITY_MP3
    }
}

/// Build a file-audio-quality snapshot from the live output format, or None when
/// the format isn't known yet. Pure mirror of the Tauri
/// `build_qconnect_file_audio_quality_snapshot`.
fn build_file_audio_quality_snapshot(
    sample_rate: u32,
    bit_depth: u32,
    nb_channels: i32,
) -> Option<QconnectFileAudioQualitySnapshot> {
    if sample_rate == 0 || bit_depth == 0 {
        return None;
    }
    Some(QconnectFileAudioQualitySnapshot {
        sampling_rate: sample_rate as i32,
        bit_depth: bit_depth as i32,
        nb_channels,
        audio_quality: classify_audio_quality(sample_rate, bit_depth),
    })
}

/// Resolve the current + next `queue_item_id` for a playing `track_id` from the
/// cloud queue snapshot, caching the result into the sync accumulator. Mirrors
/// the Tauri `resolve_queue_item_ids_by_track_id`.
async fn resolve_queue_item_ids_by_track_id(
    app: &Arc<DaemonQconnectApp>,
    sync_state: &Arc<Mutex<QconnectRemoteSyncState>>,
    track_id: u64,
) -> (Option<u64>, Option<u64>) {
    let queue = app.queue_state_snapshot().await;
    let (current_qid, next_qid, next_track_id) =
        qconnect_app::queue_resolution::resolve_queue_item_ids_from_queue_state(&queue, track_id);

    if let Some(current_qid) = current_qid {
        let mut state = sync_state.lock().await;
        state.last_renderer_queue_item_id = Some(current_qid);
        state.last_renderer_next_queue_item_id = next_qid;
        state.last_renderer_track_id = Some(track_id);
        state.last_renderer_next_track_id = next_track_id;
        (Some(current_qid), next_qid)
    } else {
        (None, None)
    }
}

// T10 (§7.2, §3.1-7): the report-tick scheduler. The desktop reports from its
// 450 ms Slint poll loop; the daemon has no such loop, so a dedicated tokio task
// owns the cadence. It calls `report_playback_state` on the LIVE session (a no-op
// when not connected or when a peer owns playback, since the body self-gates on
// `is_local_renderer_active`).
//
// Two triggers, per §7.2 ("~2 s tokio interval while playing + edge-triggered on
// track/play-state transitions"):
//   * `notify` — the driver's `DriverAction::ReportEdge` signal (daemon.rs wires
//     `on_edge -> Notify::notify_one`). The landed T4 driver folds the ~2 s
//     periodic cadence AND the transition edges into this one signal
//     (playback.rs:4648, `transition || periodic`).
//   * a ~2 s `interval` — the periodic FLOOR of §3.1-7. Because the driver
//     already supplies the periodic edge, the interval is RESET on every wake so
//     it only elapses when the edge stream goes quiet (no double-reporting during
//     active playback); interval-driven reports are additionally gated on
//     `is_playing`, so a paused/stopped renderer stays silent like the desktop.
pub async fn run_report_scheduler(
    notify: Arc<tokio::sync::Notify>,
    inner: Arc<Mutex<super::DaemonQconnectInner>>,
    runtime: Arc<AppRuntime<DaemonAdapter>>,
    buffering: Arc<super::engine::BufferingLatch>,
) {
    use qconnect_app::renderer::{
        PLAYING_STATE_PAUSED, PLAYING_STATE_PLAYING, PLAYING_STATE_STOPPED,
    };

    // The periodic floor. It tightens to LOADING_FLOOR while a stream is
    // filling, because the two things that end a load — audio becoming audible,
    // and the real position/duration replacing the blank ones in the shared
    // SetState echo — have no event of their own and can only go out on a tick.
    // At the 2 s floor the spinner ran on for up to two seconds past the first
    // sample, and the controller sat on a 0:00 duration just as long.
    const IDLE_FLOOR: std::time::Duration = std::time::Duration::from_millis(2_000);
    const LOADING_FLOOR: std::time::Duration = std::time::Duration::from_millis(300);

    // A fresh interval fires its first tick immediately; start one period out.
    let period_from = |floor: std::time::Duration| {
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + floor, floor);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval
    };

    let mut floor = IDLE_FLOOR;
    let mut interval = period_from(floor);
    let mut was_buffering = false;

    loop {
        let via_interval = tokio::select! {
            _ = notify.notified() => false,
            _ = interval.tick() => true,
        };

        // Read the live player state.
        let ev = runtime.core().player().get_playback_event();
        // The load in flight, if any. Asked of the latch rather than of the
        // player: until the new stream produces audio the player still reports
        // the OUTGOING track, so anything keyed on ev.track_id missed the whole
        // load window and only noticed once audio had started. The MILLISECOND
        // clock is what makes the audible edge prompt — see `in_flight`.
        let player = runtime.core().player();
        let in_flight = buffering.in_flight(ev.track_id, player.state.current_position_ms());
        let is_buffering = in_flight.is_some();
        // Re-arm the floor for whichever phase we are now in, and reset it either
        // way so the floor only elapses after a full period of edge silence.
        let wanted = if is_buffering { LOADING_FLOOR } else { IDLE_FLOOR };
        if wanted == floor {
            interval.reset();
        } else {
            floor = wanted;
            interval = period_from(floor);
        }
        // Whether the last report we sent claimed BUFFERING. A load that FAILS
        // clears the latch without the player ever adopting the track, so
        // without this the falling edge fell into the `continue` below and the
        // controller was left spinning on a load that had already given up.
        let falling_edge = was_buffering && !is_buffering;
        was_buffering = is_buffering;
        // Nothing loaded, nothing loading, and nothing to retract.
        if ev.track_id == 0 && !is_buffering && !falling_edge {
            continue;
        }

        // The periodic floor only fires while actually playing (or buffering);
        // edge notifications (transitions + the driver's periodic) always report.
        if via_interval && !ev.is_playing && !is_buffering && !falling_edge {
            continue;
        }

        // Reconcile the queue cursor with the audible track. A gapless hand-off
        // advances inside the player, and the driver only syncs the cursor on
        // the exact tick the track id changes while playing on BOTH sides of
        // the tick — a playback-state blip during the hand-off ("PlayNext
        // landed after track finished") loses that edge for good, leaving the
        // cursor one track behind: `qbzd status` and the moOde overlay named
        // the previous track while the next one played (title said "Golden
        // Seams" while the reported duration, 213s, was "Pulse"). Skipped while
        // buffering, where the cursor is legitimately AHEAD of the player: the
        // stream for the new track has not started yet, and syncing there would
        // drag the cursor back to the outgoing track. `sync_current_to_id` only
        // moves the pointer (and emits) when it actually differs.
        if ev.is_playing && ev.track_id != 0 && !is_buffering {
            runtime.core().sync_current_to_id(ev.track_id).await;
        }

        // Resolve the LIVE session (app + the shared sync accumulator). No runtime
        // means QConnect is not connected -> a no-op this tick.
        let (app, sync_state) = {
            let guard = inner.lock().await;
            match guard.runtime.as_ref() {
                Some(rt) => (Arc::clone(&rt.app), Arc::clone(&rt.sync_state)),
                None => continue,
            }
        };

        // While buffering, report PLAYING + BUFFERING — the pair the official
        // client itself sends. Observed from the desktop app's own renderer
        // reports while IT buffers: playing_state 2, buffer_state 1, position
        // and duration populated, repeated for the whole load.
        //
        // UNKNOWN was tried (StreamCore32 uses it) and is worse here: for a
        // fresh track the position is 0, which the wire omits, so an UNKNOWN
        // report carries neither a state nor a position and the controller drew
        // nothing at all during a next-track load. The loading state comes from
        // buffer_state; the playing state's job is to say we intend to play.
        let playing_state = if ev.is_playing || is_buffering {
            PLAYING_STATE_PLAYING
        } else if ev.track_id == 0 {
            // Nothing loaded at all — only reachable on the falling edge of a
            // load that failed, where PAUSED would invite the controller to
            // offer a resume for audio that was never there.
            PLAYING_STATE_STOPPED
        } else {
            PLAYING_STATE_PAUSED
        };
        let buffer_state = if is_buffering {
            BUFFER_STATE_BUFFERING
        } else {
            BUFFER_STATE_OK
        };
        // While a load is in flight the report must describe the track being
        // loaded, taken from the latch — the player is still on the OUTGOING
        // track (or on nothing at all, freshly after a hand-off, where it has
        // neither a duration nor a track id). Reporting `ev` there named the
        // previous song during a next-track load, and showed "0:00 of 0:00"
        // for the whole wait when switching output from another device.
        //
        // The offset the stream opened at is the honest position: on a resume
        // at 2:19 the controller should draw the scrubber there while it fills,
        // not at zero.
        let (report_track_id, position_secs, duration_secs) = match in_flight {
            Some((track_id, start_secs, duration_secs)) => (track_id, start_secs, duration_secs),
            None => (ev.track_id, ev.position, ev.duration),
        };
        // `report_playback_state` wants MILLISECONDS; the player reports seconds.
        let position_ms = (position_secs as i64) * 1000;
        let duration_ms = (duration_secs as i64) * 1000;
        report_playback_state(
            &app,
            &sync_state,
            &runtime,
            playing_state,
            position_ms,
            duration_ms,
            report_track_id,
            buffer_state,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_audio_quality_matches_the_desktop_thresholds() {
        assert_eq!(classify_audio_quality(0, 0), AUDIO_QUALITY_UNKNOWN);
        assert_eq!(classify_audio_quality(44_100, 16), AUDIO_QUALITY_CD);
        assert_eq!(classify_audio_quality(48_000, 16), AUDIO_QUALITY_CD);
        assert_eq!(classify_audio_quality(96_000, 24), AUDIO_QUALITY_HIRES_L1);
        assert_eq!(classify_audio_quality(192_000, 24), AUDIO_QUALITY_HIRES_L2);
        assert_eq!(classify_audio_quality(384_000, 24), AUDIO_QUALITY_HIRES_L3);
        assert_eq!(classify_audio_quality(22_050, 16), AUDIO_QUALITY_MP3);
    }

    #[test]
    fn snapshot_is_none_until_format_known() {
        assert!(build_file_audio_quality_snapshot(0, 0, 2).is_none());
        let snap = build_file_audio_quality_snapshot(96_000, 24, 2).expect("known format");
        assert_eq!(snap.sampling_rate, 96_000);
        assert_eq!(snap.bit_depth, 24);
        assert_eq!(snap.nb_channels, 2);
        assert_eq!(snap.audio_quality, AUDIO_QUALITY_HIRES_L1);
    }
}
