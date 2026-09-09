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
/// The player's clock has reached the end of the track it is playing.
///
/// Not a proxy for anything subtle: if we are playing and the position has
/// caught up with the duration, the next track has not started yet.
fn clock_is_at_the_end(is_playing: bool, position_secs: u64, duration_secs: u64) -> bool {
    is_playing && duration_secs > 0 && position_secs >= duration_secs
}

/// Debounce [`clock_is_at_the_end`] into "a load is in progress".
///
/// `at_end_since` is the caller's memory of when the clock first arrived there,
/// cleared as soon as it moves again. The grace exists so an ordinary gapless
/// hand-off, which passes through this state for a fraction of a second, never
/// flashes a spinner at the controller.
fn awaiting_next_track(
    at_end: bool,
    at_end_since: &mut Option<std::time::Instant>,
    grace: std::time::Duration,
) -> bool {
    if !at_end {
        *at_end_since = None;
        return false;
    }
    let since = *at_end_since.get_or_insert_with(std::time::Instant::now);
    since.elapsed() >= grace
}

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
    // The duration IS published while buffering, unlike the position: it belongs
    // to the track being loaded and is what the SetState echo needs in order to
    // avoid blanking the controller's display. Nothing seeks on a duration.
    if duration_ms > 0 {
        app.update_renderer_duration(duration_ms as u64).await;
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

    // How long the clock must sit at the end of a track before we call it a
    // load. Long enough that an ordinary gapless hand-off — where this is true
    // for a fraction of a second — never flashes a spinner at the controller.
    const AWAITING_NEXT_GRACE: std::time::Duration = std::time::Duration::from_millis(1_000);

    let mut floor = IDLE_FLOOR;
    let mut interval = period_from(floor);
    let mut was_buffering = false;
    // When the clock first reached the end of the current track.
    let mut at_end_since: Option<std::time::Instant> = None;
    // (playing_state, buffer_state, track) of the last report we sent, so a
    // transition can be told from a routine position update.
    let mut last_signature: Option<(i32, i32, u64)> = None;

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
        // Between tracks. The clock has reached the end of the track and the
        // next one has not started, which is a load in progress that the latch
        // never hears about: a gapless prefetch runs inside the player, with no
        // `begin`/`finish` around it. Without this the controller sat at
        // "4:18 / 4:18", PLAYING and buffer OK, for as long as the prefetch
        // took — the spinner only appeared later, when the cloud happened to
        // send a SetState for the next track and armed the latch.
        let awaiting_next = awaiting_next_track(
            clock_is_at_the_end(ev.is_playing, ev.position, ev.duration),
            &mut at_end_since,
            AWAITING_NEXT_GRACE,
        );
        let is_buffering = in_flight.is_some() || awaiting_next;
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

        // A state TRANSITION just went out (play/pause, a new track, buffering
        // starting or ending). Schedule a prompt follow-up rather than waiting
        // out the 2 s floor.
        //
        // Every controller command is echoed by the shared SetState handler,
        // which cannot know a duration — there is none in the renderer state to
        // read — so it reports `duration: null`. The controller blanks its
        // progress display on that, which is why hitting pause or play made the
        // time and total length flick to 0:00 and stay there until our next
        // report. That echo lands a few milliseconds AFTER ours, so being fast
        // is not enough; the fix is to speak again right behind it.
        let signature = (playing_state, buffer_state, report_track_id);
        if last_signature != Some(signature) {
            last_signature = Some(signature);
            floor = LOADING_FLOOR;
            interval = period_from(floor);
        }
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

#[cfg(test)]
mod awaiting_next_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn the_clock_is_at_the_end_only_while_playing_a_track_of_known_length() {
        assert!(clock_is_at_the_end(true, 258, 258));
        assert!(clock_is_at_the_end(true, 259, 258), "past the end counts too");
        assert!(!clock_is_at_the_end(true, 257, 258));
        // Paused at the end is not a load, it is a paused track.
        assert!(!clock_is_at_the_end(false, 258, 258));
        // A stream with no duration yet would otherwise read as permanently
        // finished at position 0.
        assert!(!clock_is_at_the_end(true, 0, 0));
    }

    #[test]
    fn a_gapless_handoff_passes_through_without_arming() {
        let mut since = None;
        // A whole grace period has not elapsed on the first observation, so the
        // fraction of a second a hand-off spends here reports nothing.
        assert!(!awaiting_next_track(true, &mut since, Duration::from_secs(3600)));
        assert!(since.is_some(), "the wait is now being timed");
        // The next track starts: forget it happened.
        assert!(!awaiting_next_track(false, &mut since, Duration::from_secs(3600)));
        assert!(since.is_none());
    }

    #[test]
    fn a_wait_that_outlasts_the_grace_arms_and_stays_armed() {
        let mut since = None;
        assert!(awaiting_next_track(true, &mut since, Duration::ZERO));
        let first = since;
        assert!(awaiting_next_track(true, &mut since, Duration::ZERO));
        assert_eq!(
            since, first,
            "the timer keeps its original start across ticks"
        );
    }
}
