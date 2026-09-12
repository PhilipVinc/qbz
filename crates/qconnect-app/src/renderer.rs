//! Frontend-agnostic renderer-side pure helpers (slice 6).
//!
//! Pure protocol/format math used by the renderer orchestration (queue
//! materialize / cursor-align). No engine, no I/O, no Tauri. Relocated here so
//! both the Tauri adapter and the Slint adapter share one definition; the
//! src-tauri side re-exports these. The load-dedup predicates and the
//! audio-quality report helpers move here alongside their orchestration /
//! report consumers in the later slice-6 steps.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use qbz_models::{QueueTrack, RepeatMode, Track};
use qbz_player::PlaybackState;
use qconnect_core::QueueItem;
use tokio::sync::Mutex;

use crate::queue_resolution::{
    dedupe_track_ids, resolve_core_shuffle_order, resolve_remote_start_index,
};
use crate::renderer_engine::QconnectRendererEngine;
use crate::session::quality_from_max_audio_quality;
use crate::{QConnectQueueState, QConnectRendererState, QconnectRemoteSyncState, RendererCommand};

/// QConnect protocol `playing_state` wire values. Single source of truth for the
/// renderer orchestration; the Tauri adapter re-exports these from here.
pub const PLAYING_STATE_UNKNOWN: i32 = 0;
pub const PLAYING_STATE_STOPPED: i32 = 1;
pub const PLAYING_STATE_PLAYING: i32 = 2;
pub const PLAYING_STATE_PAUSED: i32 = 3;

/// Dedup window: an echoed SetState for a track whose load was registered within
/// this window does not re-trigger the load. The audio thread updates
/// `playback_state.track_id` only after the engine appends the source, so a bare
/// `track_id` comparison would re-fire during that buffer/decode gap.
const LOAD_ATTEMPT_DEDUP_WINDOW: Duration = Duration::from_secs(5);

/// A stop/pause landing within this window of our own load is the previous
/// renderer's handoff echo rather than a user intent — see `is_handoff_echo` in
/// `apply_renderer_command`. Kept tight so a real stop or pause shortly after a
/// track starts is still honored.
const HANDOFF_ECHO_WINDOW: Duration = Duration::from_millis(1_500);

/// Source tag stamped on remote queue tracks materialized from a QConnect cloud
/// queue. Matches the Tauri adapter's prior `QCONNECT_REMOTE_QUEUE_SOURCE`.
pub const QCONNECT_REMOTE_QUEUE_SOURCE: &str = "qobuz_connect_remote";

pub fn qconnect_repeat_mode_from_loop_mode(loop_mode: i32) -> Option<RepeatMode> {
    // QConnect protocol loop mode values: 1 = off, 2 = repeat one, 3 = repeat all.
    match loop_mode {
        0 | 1 => Some(RepeatMode::Off),
        2 => Some(RepeatMode::One),
        3 => Some(RepeatMode::All),
        _ => None,
    }
}

pub fn normalize_volume_to_fraction(volume: i32) -> f32 {
    volume.clamp(0, 100) as f32 / 100.0
}

pub fn model_track_to_core_queue_track(track: &Track) -> QueueTrack {
    let artwork_url = track
        .album
        .as_ref()
        .and_then(|album| album.image.best().cloned());
    let artist = track
        .performer
        .as_ref()
        .map(|performer| performer.name.clone())
        .unwrap_or_else(|| "Unknown Artist".to_string());
    let album = track
        .album
        .as_ref()
        .map(|album| album.title.clone())
        .unwrap_or_else(|| "Unknown Album".to_string());
    let album_id = track.album.as_ref().and_then(|album| {
        let trimmed = album.id.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let artist_id = track.performer.as_ref().map(|performer| performer.id);

    QueueTrack {
        id: track.id,
        title: track.title.clone(),
        version: track.version.clone(),
        artist,
        album,
        album_version: None,
        duration_secs: track.duration as u64,
        artwork_url,
        hires: track.hires,
        bit_depth: track.maximum_bit_depth,
        sample_rate: track.maximum_sampling_rate,
        is_local: false,
        album_id: album_id.clone(),
        artist_id,
        streamable: track.streamable,
        source: Some(QCONNECT_REMOTE_QUEUE_SOURCE.to_string()),
        parental_warning: track.parental_warning,
        source_item_id_hint: album_id,
        context_kind: None,
        context_id: None,
    }
}

// ===================== Renderer orchestration (slice 6, step 6) =====================
//
// Engine-agnostic: written ONLY against `QconnectRendererEngine` + the shared
// `QconnectRemoteSyncState`. The Tauri/Slint adapters obtain a concrete engine
// (`&CoreBridge` / `&SlintEngine`) — including any "not initialized yet" guard —
// and dispatch here, so the hard-won echo/cursor/materialize/shuffle logic is
// never re-derived per frontend. Ported byte-for-byte from the prior Tauri
// `corebridge.rs` / `track_loading.rs`; only `bridge` -> `engine` and the
// guard-unwrap (which stays adapter-side) changed.

pub fn queue_state_needs_materialization(
    previous: Option<&QConnectQueueState>,
    next: &QConnectQueueState,
) -> bool {
    let Some(previous) = previous else {
        return true;
    };

    previous.version != next.version
        || previous.queue_items != next.queue_items
        || previous.shuffle_mode != next.shuffle_mode
        || previous.shuffle_order != next.shuffle_order
        || previous.autoplay_mode != next.autoplay_mode
        || previous.autoplay_loading != next.autoplay_loading
        || previous.autoplay_items != next.autoplay_items
}

pub fn should_reload_remote_track(playback_state: &PlaybackState, track_id: u64) -> bool {
    // Only reload when the track ID actually changed. The previous
    // !has_loaded_audio gate fired during the buffering window of an
    // initial load (qbz already started fetching but the audio engine
    // hasn't reported the track as loaded yet) — when the cloud echo
    // SetState arrived for the same track, this caused a redundant
    // load that interrupted the in-progress one. That was the residual
    // first-track hiccup.
    playback_state.track_id != track_id
}

/// Returns true if a load attempt for `track_id` was registered within the
/// dedup window (see `LOAD_ATTEMPT_DEDUP_WINDOW`).
fn is_recent_load_attempt(state: &QconnectRemoteSyncState, track_id: u64) -> bool {
    match state.last_load_attempt {
        Some((tid, ts)) => tid == track_id && ts.elapsed() < LOAD_ATTEMPT_DEDUP_WINDOW,
        None => false,
    }
}

/// The position a `SetState` frame actually specifies, in milliseconds.
///
/// The command's own `current_position_ms` is authoritative: it arrives in the
/// same frame as the track it names. The cloud's retained renderer view
/// (`renderer_state.current_position_ms`) is only a fallback, and preferring it
/// was a bug — `qconnect_core::apply_renderer_command` updates `current_track`
/// and `current_position_ms` INDEPENDENTLY, each only when the command carries
/// it. So an ordinary track change (new `current_track`, no position) leaves the
/// retained position sitting at the OUTGOING track's: playing 20 s of a track
/// and pressing next started the next track 20 s in, both by loading the stream
/// there and by seeking there right afterwards (moOde forum, the_bertrum, three
/// 10.3.3 boxes).
///
/// So the retained position counts only for a frame that names neither a track
/// nor a position — the state-only shape, which is the takeback this fallback
/// exists for: `SetActive` lands before the cloud knows our `current_track`, and
/// the renderer view is then the only thing carrying where the peer left off.
///
/// A frame that names a NEW track without a position means "from the start".
fn frame_position_ms(
    command_position_ms: Option<u64>,
    command_track: Option<&QueueItem>,
    renderer_state: &QConnectRendererState,
) -> Option<u64> {
    match (command_position_ms, command_track) {
        (Some(ms), _) => Some(ms),
        (None, Some(_)) => None,
        (None, None) => renderer_state.current_position_ms,
    }
}

/// Load a remote track into the engine, deduped against echoed SetState frames.
/// Records the attempt BEFORE dispatching the load (the audio thread updates
/// `playback_state.track_id` only after the engine appends the source, so the
/// recording must precede the load to close the echo window).
///
/// `start_position_secs` is the position to resume the stream at. For a normal
/// peer track-change the cloud sends position ~0, so this is 0 (a fresh track).
/// On a TAKEBACK whose first load lands here (SetActive arrived before the cloud
/// knew our current_track, so the force-stream couldn't fire), the SetState
/// carries the peer's real position and we resume there instead of streaming
/// from 0 and then trying to seek forward — a forward seek past the buffered
/// watermark is silently ignored by the audio thread, so streaming from 0 left
/// the first takeback playing from the start (bad for audiobooks). The protected
/// bit-perfect seams + the HTTP feeder live behind `start_track_stream`.
pub async fn ensure_remote_track_loaded(
    engine: &impl QconnectRendererEngine,
    sync_state: &Arc<Mutex<QconnectRemoteSyncState>>,
    track_id: u64,
    max_audio_quality: Option<i32>,
    start_position_secs: u64,
) -> Result<bool, String> {
    {
        let state = sync_state.lock().await;
        if is_recent_load_attempt(&state, track_id) {
            return Ok(false);
        }
    }
    let playback_state = engine.get_playback_state();
    if !should_reload_remote_track(&playback_state, track_id) {
        return Ok(false);
    }

    {
        let mut state = sync_state.lock().await;
        state.last_load_attempt = Some((track_id, Instant::now()));
    }

    let quality = quality_from_max_audio_quality(max_audio_quality);
    let duration_secs = engine
        .get_track(track_id)
        .await
        .map(|track| u64::from(track.duration))
        .unwrap_or(0);
    engine
        .start_track_stream(track_id, quality, duration_secs, start_position_secs)
        .await
        .map(|()| true)
}

/// Force a (re)stream of `track_id` at `start_position_secs` when BECOMING the
/// active renderer (takeback). Unlike [`ensure_remote_track_loaded`], this does
/// NOT short-circuit on a matching `playback_state.track_id`: a prior
/// controller->renderer handoff tore the local stream down via `engine.stop()`
/// (audio buffer cleared, `has_loaded_audio` false) while `current_track_id`
/// still reports the old track, so the plain track-id guard would skip the load
/// and the following `resume()` would fail with "no audio data available".
///
/// It DOES skip when the engine is already streaming this exact track with audio
/// loaded (`track_id` matches AND `has_loaded_audio`), so a spurious SetActive
/// during live renderer playback never restarts the current track; and it keeps
/// the dedup window so the SetActive->SetState echo doesn't double-load.
///
/// `start_position_secs` resumes at the handed-off position (the cloud carries
/// the peer's last position in `renderer_state.current_position_ms`), so a long
/// track / audiobook does not restart from 0. Resume is honored by the protected
/// `play_streaming_dynamic` session-resume path behind `start_track_stream`.
pub async fn force_remote_track_stream(
    engine: &impl QconnectRendererEngine,
    sync_state: &Arc<Mutex<QconnectRemoteSyncState>>,
    track_id: u64,
    max_audio_quality: Option<i32>,
    start_position_secs: u64,
) -> Result<bool, String> {
    let playback_state = engine.get_playback_state();
    if playback_state.track_id == track_id && engine.has_loaded_audio() {
        return Ok(false);
    }

    {
        let state = sync_state.lock().await;
        if is_recent_load_attempt(&state, track_id) {
            return Ok(false);
        }
    }
    {
        let mut state = sync_state.lock().await;
        state.last_load_attempt = Some((track_id, Instant::now()));
    }

    let quality = quality_from_max_audio_quality(max_audio_quality);
    let duration_secs = engine
        .get_track(track_id)
        .await
        .map(|track| u64::from(track.duration))
        .unwrap_or(0);
    engine
        .start_track_stream(track_id, quality, duration_secs, start_position_secs)
        .await
        .map(|()| true)
}

pub async fn apply_remote_loop_mode(
    engine: &impl QconnectRendererEngine,
    loop_mode: i32,
) -> Result<(), String> {
    let repeat_mode = qconnect_repeat_mode_from_loop_mode(loop_mode)
        .ok_or_else(|| format!("unsupported qconnect loop mode: {loop_mode}"))?;
    engine.set_repeat_mode(repeat_mode).await;
    Ok(())
}

pub async fn apply_renderer_command(
    engine: &impl QconnectRendererEngine,
    sync_state: &Arc<Mutex<QconnectRemoteSyncState>>,
    command: &RendererCommand,
    renderer_state: &QConnectRendererState,
) -> Result<(), String> {
    match command {
        RendererCommand::SetState {
            playing_state,
            current_position_ms,
            current_track,
            next_track,
            ..
        } => {
            let resolved_playing_state = renderer_state.playing_state.or(*playing_state);
            // Position this command started a fresh stream at, if it did. The
            // protected streaming path already begins playback there (it waits
            // for the buffer and pre-skips), so the seek block below must not
            // ALSO seek to it: the engine's reported position is still the
            // previous track's at that moment, making the comparison
            // meaningless, and the seek either gets dropped for being past the
            // buffered watermark or lands later and rebuilds the engine,
            // redoing the whole skip.
            let mut stream_started_at: Option<u64> = None;
            // Resolved once and used by BOTH the load below and the seek at the
            // end of this arm: they must agree, or the load starts the track in
            // the right place and the seek immediately drags it elsewhere.
            let frame_position_ms =
                frame_position_ms(*current_position_ms, current_track.as_ref(), renderer_state);
            let mut projection_renderer_state = renderer_state.clone();
            if projection_renderer_state.current_track.is_none() {
                projection_renderer_state.current_track = current_track.clone();
            }
            if projection_renderer_state.next_track.is_none() {
                projection_renderer_state.next_track = next_track.clone();
            }
            let resolved_current_track = projection_renderer_state.current_track.as_ref();
            if let Some(projected_track) = resolved_current_track {
                let queue_state = {
                    let state = sync_state.lock().await;
                    state.last_remote_queue_state.clone()
                };
                let projection_applied = if let Some(queue_state) = queue_state.as_ref() {
                    sync_remote_shuffle_projection(
                        engine,
                        sync_state,
                        queue_state,
                        &projection_renderer_state,
                    )
                    .await?
                } else {
                    false
                };

                // Track-manipulation operations (cursor align, force-restart,
                // ensure_remote_track_loaded) only run when the COMMAND
                // explicitly specifies a current_track. The projection's
                // resolved_current_track can be stale: when the cloud sends
                // a state-only update (pause/resume) with command.current_track=null,
                // the projection falls back to renderer_state.current_track,
                // which is the cloud's last-known view of qbz's playback —
                // potentially behind qbz's actual local advance. Using that
                // stale value to align/load causes spurious track switches
                // (e.g., pause from iOS made qbz jump back to a previous
                // track). The outer renderer_state-based projection is still
                // used for shuffle sync above and downstream playing_state /
                // seek operations, which remain safe because they don't
                // change the queue cursor or load tracks.
                let _ = projected_track; // retained for shuffle projection above
                if let Some(command_track) = current_track.as_ref() {
                    if !projection_applied {
                        if let Err(err) = align_queue_cursor(engine, command_track.track_id).await {
                            log::warn!("[QConnect] Failed to align CoreBridge queue cursor: {err}");
                        }
                    }

                    if matches!(
                        resolved_playing_state,
                        Some(PLAYING_STATE_PLAYING | PLAYING_STATE_PAUSED)
                    ) {
                        // Force-restart removed: the cloud routinely re-emits
                        // SetState with current_position_ms=0 for the same
                        // track when only secondary fields change (e.g.,
                        // next_track corrections, queue_item_id refreshes).
                        // Reloading the stream on every echo caused first-
                        // track hiccup on album change and "needs several
                        // taps" on prev/next. Track-change cases are handled
                        // by align_queue_cursor + ensure_remote_track_loaded
                        // below; legitimate seek-to-start from a peer
                        // controller can use the seek path with target>1s.
                        // Resume the load at the position this frame specifies
                        // (same source the seek block below uses). For a normal
                        // peer track-change that is 0; on a takeback whose first
                        // load lands here it is the peer's position, so we stream
                        // from there instead of from 0 + an ignored forward seek.
                        let start_position_secs =
                            frame_position_ms.map(|ms| ms / 1000).unwrap_or(0);
                        match ensure_remote_track_loaded(
                            engine,
                            sync_state,
                            command_track.track_id,
                            projection_renderer_state.max_audio_quality,
                            start_position_secs,
                        )
                        .await
                        {
                            Ok(true) => stream_started_at = Some(start_position_secs),
                            Ok(false) => {}
                            Err(err) => log::warn!(
                                "[QConnect] Failed to load remote track {}: {err}",
                                command_track.track_id
                            ),
                        }
                    }
                }
            }

            // Handoff echo: claiming the render from a peer (the phone/desktop
            // Qobuz app) makes that peer stop ITS local playback, and the cloud
            // relays the result to whoever is now the active renderer — us,
            // milliseconds after it told us to play that very track. Observed
            // in both shapes: `stopped` at position 0 naming the track, and a
            // state-only `paused` carrying no track or position at all.
            // Honoring either killed the stream we had just started, leaving
            // the controller spinning until the user pressed play again.
            //
            // Keyed on our OWN load having just happened, and on the command
            // not naming a real position to hold at (position 0, or absent in
            // the state-only shape). Deliberately NOT keyed on the track: the
            // peer resets its own cursor to the head of the queue as it stops,
            // so the echo can name a different track than the one we just
            // started — observed naming queue item 0 while we were loading
            // item 4, which killed the stream ("play superseded, abandoning").
            // A stop naming a track we are not playing is not a coherent
            // instruction to stop our playback anyway.
            //
            // The window is deliberately tight, so stopping or pausing a
            // second or more after a track starts still works normally.
            let is_handoff_echo = {
                let just_loaded = {
                    let state = sync_state.lock().await;
                    state
                        .last_load_attempt
                        .map(|(_, ts)| ts.elapsed() < HANDOFF_ECHO_WINDOW)
                        .unwrap_or(false)
                };
                just_loaded && (*current_position_ms).map(|ms| ms <= 1_000).unwrap_or(true)
            };

            if let Some(value) = resolved_playing_state {
                match value {
                    PLAYING_STATE_PLAYING => {
                        // A state-only resume (current_track = null, e.g. a
                        // mid-track handoff from a peer renderer after the
                        // engine restarted) can land on an engine whose queue
                        // cursor is set but which holds NO loaded audio — the
                        // session store restores the queue paused and
                        // unloaded. A bare resume() then dies in the audio
                        // thread ("cannot resume - no audio data available")
                        // while the cloud keeps reporting paused 0:00 to the
                        // controller forever. Cold-load the cloud's current
                        // track at its position first, exactly like the
                        // SetActive takeback path; the has_loaded_audio gate
                        // keeps echoes and live playback on the plain resume.
                        let cold_engine = !engine.has_loaded_audio();
                        let cold_track = projection_renderer_state.current_track.as_ref();
                        if cold_engine && cold_track.is_some() {
                            let track_id = cold_track.map(|t| t.track_id).unwrap_or(0);
                            let start_position_secs =
                                frame_position_ms.map(|ms| ms / 1000).unwrap_or(0);
                            match force_remote_track_stream(
                                engine,
                                sync_state,
                                track_id,
                                projection_renderer_state.max_audio_quality,
                                start_position_secs,
                            )
                            .await
                            {
                                Ok(true) => stream_started_at = Some(start_position_secs),
                                Ok(false) => {}
                                Err(err) => log::warn!(
                                    "[QConnect] Cold-start load of remote track {track_id} failed: {err}"
                                ),
                            }
                        } else {
                            engine.resume()?;
                        }
                    }
                    PLAYING_STATE_PAUSED => {
                        if is_handoff_echo {
                            log::info!(
                                "[QConnect] SetState pause ignored: handoff echo for the track just started"
                            );
                        } else {
                            engine.pause()?;
                        }
                    }
                    PLAYING_STATE_STOPPED => {
                        if is_handoff_echo {
                            log::info!(
                                "[QConnect] SetState stop ignored: handoff echo for the track just started"
                            );
                        } else {
                            engine.stop()?;
                        }
                    }
                    PLAYING_STATE_UNKNOWN => {}
                    _ => {
                        log::debug!("[QConnect] Unknown playing state received: {value}");
                    }
                }
            }

            if let Some(position_ms) = frame_position_ms {
                let playback_state = engine.get_playback_state();
                let current_pos_secs = playback_state.position;
                let target_secs = position_ms / 1000;
                // Reject echo seeks: when the command targets the same track
                // qbz is already playing AND target<=1s while local is well
                // ahead, this is the cloud re-emitting a stale SetState
                // (frequently fires on next_track corrections and queue_
                // item_id refreshes). A real peer "go to start" intent
                // would target the same track as the local one but the
                // round-trip to qbz is already a few seconds, making this
                // case indistinguishable from echo — favor stability.
                let is_echo_reset = current_track
                    .as_ref()
                    .map(|cmd_track| cmd_track.track_id == playback_state.track_id)
                    .unwrap_or(false)
                    && target_secs <= 1
                    && current_pos_secs > 2;
                // Issue #387: honor seeks regardless of which device is the
                // active renderer. The previous gate (`peer_renderer_active`)
                // skipped seeks entirely when local was the active renderer,
                // breaking the case where a peer controller (e.g. official
                // Qobuz mobile app) sends a real seek to qbz acting as the
                // renderer — the audio thread never moved while the cloud
                // state advanced, so the controller's progress bar locked.
                // The is_echo_reset + abs_diff > 2 gates already filter the
                // cloud-echo case the peer_renderer_active check was added
                // to defend against in commit 147bcbd7. If hiccups return,
                // revert this change and reintroduce a more targeted echo
                // detector (UUID-based) instead of the all-or-nothing gate.
                // The stream this command just started already begins at
                // `target_secs` (see `stream_started_at`), so a seek here is
                // pure waste — and harmful: it either logs "past buffered
                // watermark" and is dropped, or applies later and rebuilds the
                // engine, re-running a multi-second sample pre-skip.
                let redundant_after_load = stream_started_at
                    .map(|started| started.abs_diff(target_secs) <= 2)
                    .unwrap_or(false);
                // The frame's position belongs to the track the frame NAMES. If
                // the engine is not on that track yet, `current_pos_secs` is
                // some other track's clock and every comparison below is
                // meaningless — so is the seek.
                //
                // This is the ordinary shape of a queue push: the app replaces
                // the queue, `materialize_remote_queue` starts track 0, and the
                // cloud's SetState for that same track arrives while the load
                // is still in flight. Observed exactly once per album change:
                //
                //   24.042  materialize_remote_queue: starting position 0 (350617010)
                //   24.765  Player: Starting dynamic streaming ... start=0s
                //   24.766  SetState seek: current=62s target=0s   <-- the previous track
                //   24.968  Stop requested / Stopping PCM
                //   25.106  Acquired new source from queue
                //
                // 140 ms of dead air and a PCM stop+prepare at the top of every
                // freshly started track — the faint click at the start of each
                // one. The `stream_started_at` guard above cannot catch it:
                // this call did not perform the load, so it has nothing to
                // compare, and the racing snapshots the two guards read need
                // not even agree with each other.
                //
                // A frame carrying no track (the state-only shape) does refer
                // to what we are playing, and still seeks.
                let position_is_for_another_track = current_track
                    .as_ref()
                    .is_some_and(|cmd_track| cmd_track.track_id != playback_state.track_id);
                if redundant_after_load {
                    log::info!(
                        "[QConnect] SetState seek skipped: stream already started at {target_secs}s"
                    );
                } else if position_is_for_another_track {
                    log::info!(
                        "[QConnect] SetState seek skipped: target {target_secs}s belongs to track {}, engine is on {} at {current_pos_secs}s",
                        current_track.as_ref().map(|t| t.track_id).unwrap_or(0),
                        playback_state.track_id
                    );
                } else if !is_echo_reset && current_pos_secs.abs_diff(target_secs) > 2 {
                    log::info!(
                        "[QConnect] SetState seek: current={}s target={}s",
                        current_pos_secs,
                        target_secs
                    );
                    engine.seek(target_secs)?;
                }
            }
        }
        RendererCommand::SetVolume { volume, .. } => {
            if let Some(resolved) = renderer_state.volume.or(*volume) {
                engine.set_volume(normalize_volume_to_fraction(resolved))?;
            }
        }
        RendererCommand::MuteVolume { value } => {
            if *value {
                engine.set_volume(0.0)?;
            } else if let Some(resolved) = renderer_state.volume {
                engine.set_volume(normalize_volume_to_fraction(resolved))?;
            }
        }
        RendererCommand::SetLoopMode { loop_mode } => {
            let resolved_loop_mode = renderer_state.loop_mode.unwrap_or(*loop_mode);
            let repeat_mode = qconnect_repeat_mode_from_loop_mode(resolved_loop_mode)
                .ok_or_else(|| format!("unsupported qconnect loop mode: {resolved_loop_mode}"))?;
            engine.set_repeat_mode(repeat_mode).await;
        }
        RendererCommand::SetActive { active } => {
            if *active {
                // Do NOT load here. At SetActive time `renderer_state` still
                // describes what WE last played: the cloud has not yet told us
                // the session's current track, and while a peer held the render
                // it may have moved on. Loading from that stale view resumed
                // the wrong track (observed: took the render back onto our old
                // track at the peer's position, 77s of a track the peer was not
                // even playing).
                //
                // The authoritative SetState follows within a few hundred ms
                // carrying the real track AND position, and its cold-engine
                // path loads from there — this is also how StreamCore32 is
                // structured: SetActive only flips the flag, playback starts in
                // the SetState handler.
                log::info!(
                    "[QConnect] SetActive(true): awaiting the session's SetState before loading"
                );
                sync_state.lock().await.local_render_active = Some(true);
            } else {
                // Standing down — but not if we just started a load. Joining a
                // live session replays it as SetActive(true) -> SetState(PLAYING)
                // -> SetActive(false) within ~10 ms, and obeying that last frame
                // literally kills the track the SetState just started. Other
                // Connect receivers hit the same replay and guard it with a
                // timing window; ours already tracks the load, so key on that.
                let just_loaded = {
                    let state = sync_state.lock().await;
                    state
                        .last_load_attempt
                        .is_some_and(|(_, at)| at.elapsed() < LOAD_ATTEMPT_DEDUP_WINDOW)
                };
                if just_loaded {
                    log::info!(
                        "[QConnect] SetActive(false) within the load window — join replay, not a handoff"
                    );
                } else {
                    log::info!(
                        "[QConnect] SetActive(false): stopping, the session renders elsewhere"
                    );
                    // Recorded only on the branch that actually stands down, so
                    // the published role never contradicts the engine.
                    sync_state.lock().await.local_render_active = Some(false);
                    engine.stop()?;
                }
            }
        }
        RendererCommand::SetMaxAudioQuality { max_audio_quality } => {
            // Applied on the next load via renderer_state.max_audio_quality
            // (recorded by the core reducer). No immediate re-fetch.
            log::info!("[QConnect] SetMaxAudioQuality => {max_audio_quality}");
        }
        RendererCommand::SetShuffleMode { shuffle_mode } => {
            let enabled = renderer_state.shuffle_mode.unwrap_or(*shuffle_mode);
            // WS-authoritative: flip ONLY the flag here. Never generate a local
            // shuffle order — the cloud owns queue order, which arrives separately
            // via `sync_remote_shuffle_projection` / `materialize_remote_queue`.
            // Calling the order-generating `set_shuffle` would produce a divergent
            // local random order ("es un infierno" — the documented failure mode).
            engine.set_shuffle_flag(enabled).await;
        }
    }

    Ok(())
}

async fn sync_remote_shuffle_projection(
    engine: &impl QconnectRendererEngine,
    sync_state: &Arc<Mutex<QconnectRemoteSyncState>>,
    queue_state: &QConnectQueueState,
    renderer_state: &QConnectRendererState,
) -> Result<bool, String> {
    if !queue_state.shuffle_mode || queue_state.queue_items.is_empty() {
        return Ok(false);
    }

    let start_index = resolve_remote_start_index(
        queue_state,
        renderer_state
            .current_track
            .as_ref()
            .map(|item| item.queue_item_id),
        renderer_state
            .current_track
            .as_ref()
            .map(|item| item.track_id),
    );
    let Some(start_index) = start_index else {
        return Ok(false);
    };

    let core_shuffle_order = resolve_core_shuffle_order(
        queue_state,
        renderer_state
            .current_track
            .as_ref()
            .map(|item| item.queue_item_id),
        renderer_state
            .current_track
            .as_ref()
            .map(|item| item.track_id),
        renderer_state
            .next_track
            .as_ref()
            .map(|item| item.queue_item_id),
        renderer_state.next_track.as_ref().map(|item| item.track_id),
    );

    // Same deferral rule as materialize_remote_queue: do not invent an
    // identity shuffle when the cloud hasn't yet sent the authoritative
    // shuffle_order. Wait for the second QueueUpdated.
    if core_shuffle_order.is_none() {
        return Ok(false);
    }

    let should_apply = {
        let state = sync_state.lock().await;
        state.last_materialized_start_index != Some(start_index)
            || state.last_materialized_core_shuffle_order != core_shuffle_order
    };
    if !should_apply {
        return Ok(false);
    }

    let (tracks, _) = engine.get_all_queue_tracks().await;
    if tracks.len() != queue_state.queue_items.len() || tracks.is_empty() {
        return Ok(false);
    }

    engine
        .set_queue_with_order(
            tracks,
            Some(start_index),
            queue_state.shuffle_mode,
            core_shuffle_order.clone(),
        )
        .await;

    let mut state = sync_state.lock().await;
    state.last_materialized_start_index = Some(start_index);
    state.last_materialized_core_shuffle_order = core_shuffle_order;
    Ok(true)
}

pub async fn materialize_remote_queue(
    engine: &impl QconnectRendererEngine,
    sync_state: &Arc<Mutex<QconnectRemoteSyncState>>,
    queue_state: &QConnectQueueState,
) -> Result<(), String> {
    let (
        renderer_queue_item_id,
        renderer_track_id,
        renderer_next_queue_item_id,
        renderer_next_track_id,
        renderer_playing_state,
        renderer_max_audio_quality,
        should_skip,
    ) = {
        let mut state = sync_state.lock().await;
        if !queue_state_needs_materialization(state.last_applied_queue_state.as_ref(), queue_state)
        {
            (
                state.last_renderer_queue_item_id,
                state.last_renderer_track_id,
                state.last_renderer_next_queue_item_id,
                state.last_renderer_next_track_id,
                state.last_renderer_playing_state,
                state.last_renderer_max_audio_quality,
                true,
            )
        } else {
            state.last_applied_queue_state = Some(queue_state.clone());
            (
                state.last_renderer_queue_item_id,
                state.last_renderer_track_id,
                state.last_renderer_next_queue_item_id,
                state.last_renderer_next_track_id,
                state.last_renderer_playing_state,
                state.last_renderer_max_audio_quality,
                false,
            )
        }
    };

    if should_skip {
        log::debug!(
            "[QConnect] materialize_remote_queue: skipped (identical snapshot {}.{})",
            queue_state.version.major,
            queue_state.version.minor
        );
        return Ok(());
    }

    log::info!(
        "[QConnect] materialize_remote_queue: version={}.{} items={} renderer_qid={:?} renderer_tid={:?} renderer_next_qid={:?} renderer_next_tid={:?} playing_state={:?}",
        queue_state.version.major,
        queue_state.version.minor,
        queue_state.queue_items.len(),
        renderer_queue_item_id,
        renderer_track_id,
        renderer_next_queue_item_id,
        renderer_next_track_id,
        renderer_playing_state
    );

    if queue_state.queue_items.is_empty() {
        // Preserve legacy behavior: keep current track on qconnect sync clears.
        engine.clear_queue(true).await;
        engine.set_shuffle(false).await;
        let mut state = sync_state.lock().await;
        state.last_materialized_start_index = None;
        state.last_materialized_core_shuffle_order = None;
        return Ok(());
    }

    let unique_track_ids = dedupe_track_ids(queue_state);
    let fetched_tracks = engine
        .get_tracks_batch(&unique_track_ids)
        .await
        .map_err(|err| format!("fetch tracks batch for remote queue: {err}"))?;

    let mut tracks_by_id = HashMap::with_capacity(fetched_tracks.len());
    for track in fetched_tracks {
        tracks_by_id.insert(track.id, model_track_to_core_queue_track(&track));
    }

    let mut queue_tracks = Vec::with_capacity(queue_state.queue_items.len());
    for item in &queue_state.queue_items {
        if let Some(queue_track) = tracks_by_id.get(&item.track_id) {
            queue_tracks.push(queue_track.clone());
            continue;
        }

        match engine.get_track(item.track_id).await {
            Ok(track) => {
                let mapped = model_track_to_core_queue_track(&track);
                tracks_by_id.insert(item.track_id, mapped.clone());
                queue_tracks.push(mapped);
            }
            Err(err) => {
                log::warn!(
                    "[QConnect] Unable to hydrate remote queue track {}: {}",
                    item.track_id,
                    err
                );
            }
        }
    }

    if queue_tracks.is_empty() {
        return Err("remote queue materialization produced zero playable tracks".to_string());
    }

    // Resolve start index from remote state first, then from the local playback
    // cursor only if that track is still part of the remote queue.
    let current_playback_track_id = match engine.get_playback_state().track_id {
        0 => None,
        track_id => Some(track_id),
    };
    // The controller's own selection, when the push carried one, outranks every
    // projection below it: `selected_queue_position` says which entry the user
    // just tapped in THIS queue, while the renderer projection only says what we
    // were playing before it. They agree on a cast (the position is the playing
    // track), and where they disagree the selection is the newer fact.
    let selected_index = queue_state
        .selected_queue_position
        .and_then(|position| usize::try_from(position).ok())
        .filter(|index| *index < queue_state.queue_items.len());
    let mut start_index = selected_index;
    if start_index.is_none() {
        start_index =
            resolve_remote_start_index(queue_state, renderer_queue_item_id, renderer_track_id);
    }
    if start_index.is_none() {
        start_index = resolve_remote_start_index(
            queue_state,
            renderer_next_queue_item_id,
            renderer_next_track_id,
        )
        .map(|index| index.saturating_sub(1));
    }
    if start_index.is_none() {
        start_index = current_playback_track_id.and_then(|track_id| {
            queue_state
                .queue_items
                .iter()
                .position(|item| item.track_id == track_id)
        });
    }
    if start_index.is_none() && !queue_tracks.is_empty() {
        start_index = Some(0);
    }
    let core_shuffle_order = resolve_core_shuffle_order(
        queue_state,
        renderer_queue_item_id,
        renderer_track_id,
        renderer_next_queue_item_id,
        renderer_next_track_id,
    );
    // The cloud sends two QueueUpdated events during a shuffle toggle:
    // first with shuffle_mode=true and shuffle_order=null (the flag
    // broadcasts immediately), then ~400ms later with the computed
    // shuffle_order. If we mark shuffle_enabled=true on the first event
    // with an absent order, set_queue_with_order falls into its identity
    // path (0,1,2,...) — that is qbz inventing a sequence that diverges
    // from the order the cloud is about to authorize. Defer the engine
    // shuffle activation until the authoritative order is present.
    let effective_shuffle_enabled = queue_state.shuffle_mode && core_shuffle_order.is_some();
    log::info!(
        "[QConnect] materialize_remote_queue: setting queue with {} tracks, start_index={:?}, local_track_id={:?}, remote_shuffle_mode={}, shuffle_order_present={}, engine_shuffle_enabled={}",
        queue_tracks.len(),
        start_index,
        current_playback_track_id,
        queue_state.shuffle_mode,
        core_shuffle_order.is_some(),
        effective_shuffle_enabled,
    );
    engine
        .set_queue_with_order(
            queue_tracks,
            start_index,
            effective_shuffle_enabled,
            core_shuffle_order.clone(),
        )
        .await;

    {
        let mut state = sync_state.lock().await;
        state.last_materialized_start_index = start_index;
        state.last_materialized_core_shuffle_order = core_shuffle_order;
    }

    let local_track_missing_from_remote = current_playback_track_id
        .map(|track_id| {
            !queue_state
                .queue_items
                .iter()
                .any(|item| item.track_id == track_id)
        })
        .unwrap_or(true);

    if let Some(index) = start_index {
        if local_track_missing_from_remote {
            log::info!(
                "[QConnect] materialize_remote_queue: aligning queue cursor to remote index {}",
                index
            );
            let _ = engine.play_index(index).await;
        }
    }

    if current_playback_track_id.is_some()
        && current_playback_track_id != renderer_track_id
        && local_track_missing_from_remote
        && matches!(
            renderer_playing_state,
            Some(PLAYING_STATE_STOPPED | PLAYING_STATE_UNKNOWN)
        )
    {
        log::info!(
            "[QConnect] materialize_remote_queue: stopping stale local playback track {:?} after remote queue replacement",
            current_playback_track_id
        );
        let _ = engine.stop();
    }

    // Start the selection when nothing else will. A push that carries
    // `selected_queue_position` is not always followed by a SetState naming the
    // track — selecting the last entry with repeat off is the case that isn't
    // (see `QConnectQueueState::selected_queue_position`) — and the cursor move
    // above only re-points the queue, so without this the tap plays nothing and
    // the outgoing track keeps going.
    //
    // Gated on the cloud reporting the session PLAYING: a selection made while
    // paused or stopped is the controller staging a track, not asking for audio.
    // `ensure_remote_track_loaded` keeps the shared dedup window, so a SetState
    // that DOES name this track right after cannot load it twice, and it
    // short-circuits when the engine already plays it.
    // A queue REPLACEMENT that names nothing at all. Picking an album the
    // renderer is not playing from pushes a whole new queue with
    // `autoplay_reset` and `queue_position: null`, and every SetState that
    // follows is state-only (`current_track: null`) while the cloud keeps
    // reporting the OUTGOING track's position. The cursor move above re-points
    // the queue, `qbzd status` and the overlay read that cursor — and the engine
    // plays on. That is the "app shows the track I picked, speakers play the
    // previous one" report, in its sticky form (the reconciler in `report.rs`
    // cannot heal it: the audible track is no longer IN the queue).
    //
    // The renderer view naming a track that is ALSO absent from the pushed queue
    // is what makes this safe. In a handoff the peer's track IS in the queue, so
    // we stand down and let the SetState that follows resume it at the handed-off
    // position instead of restarting it at 0.
    let start_target = selected_index.or_else(|| {
        let renderer_track_missing_from_remote = renderer_track_id
            .map(|track_id| {
                !queue_state
                    .queue_items
                    .iter()
                    .any(|item| item.track_id == track_id)
            })
            .unwrap_or(false);
        if local_track_missing_from_remote && renderer_track_missing_from_remote {
            start_index
        } else {
            None
        }
    });

    // Gated on the cloud reporting the session PLAYING: a queue staged while
    // paused or stopped is the controller preparing something, not asking for
    // audio. `ensure_remote_track_loaded` keeps the shared dedup window, so a
    // SetState that DOES name this track right after cannot load it twice, and it
    // short-circuits when the engine already plays it.
    if let Some(index) = start_target {
        let target_track_id = queue_state.queue_items[index].track_id;
        // When the cloud's own view already names this track, the SetState that
        // follows owns the load — and it carries a position (a takeback resumes
        // mid-track). Starting it here would restart it at 0 and the dedup
        // window would then swallow the frame that knew better.
        let cloud_already_names_it = renderer_track_id == Some(target_track_id);
        if !cloud_already_names_it
            && current_playback_track_id != Some(target_track_id)
            && matches!(renderer_playing_state, Some(PLAYING_STATE_PLAYING))
        {
            log::info!(
                "[QConnect] materialize_remote_queue: starting queue position {index} (track {target_track_id}); {}",
                if selected_index.is_some() {
                    "the push selected it"
                } else {
                    "the push replaced the queue and named no track"
                }
            );
            // A freshly selected track starts at its beginning — the same rule
            // `frame_position_ms` applies to a frame that names a track and no
            // position.
            ensure_remote_track_loaded(
                engine,
                sync_state,
                target_track_id,
                renderer_max_audio_quality,
                0,
            )
            .await?;
        }
    }

    Ok(())
}

pub async fn align_queue_cursor(
    engine: &impl QconnectRendererEngine,
    track_id: u64,
) -> Result<(), String> {
    let (tracks, current_index) = engine.get_all_queue_tracks().await;
    log::info!(
        "[QConnect] align_queue_cursor: track_id={track_id} queue_len={} current_index={:?}",
        tracks.len(),
        current_index
    );
    if let Some(target_index) = tracks.iter().position(|track| track.id == track_id) {
        if current_index != Some(target_index) {
            log::info!(
                "[QConnect] align_queue_cursor: moving cursor from {:?} to {target_index}",
                current_index
            );
            let _ = engine.play_index(target_index).await;
        }
        return Ok(());
    }

    log::info!(
        "[QConnect] align_queue_cursor: track {track_id} not in queue, fetching and creating single-track queue"
    );
    let track = engine
        .get_track(track_id)
        .await
        .map_err(|err| format!("fetch current remote track {track_id}: {err}"))?;
    let queue_track = model_track_to_core_queue_track(&track);
    engine.set_queue(vec![queue_track], Some(0)).await;
    Ok(())
}

// ===================== Mock-engine trait tests (slice 6, step 8) =====================
//
// These exercise the renderer orchestration end-to-end against a recording mock
// engine — the hard-won behavior that previously could only be tested through the
// Tauri adapter. A passing test here proves the logic is engine-independent: any
// future Slint regression is a wiring bug in its trait impl, not a re-derivation
// bug in the shared logic.

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex as StdMutex};

    use async_trait::async_trait;
    use qbz_models::{Quality, QueueTrack, RepeatMode, Track};
    use qbz_player::PlaybackState;
    use qconnect_core::{QueueItem, QueueVersion};
    use tokio::sync::Mutex;

    use crate::renderer_engine::QconnectRendererEngine;
    use crate::{
        QConnectQueueState, QConnectRendererState, QconnectRemoteSyncState, RendererCommand,
    };

    #[derive(Default)]
    struct MockCalls {
        resumes: u32,
        pauses: u32,
        stops: u32,
        seeks: Vec<u64>,
        set_volumes: Vec<f32>,
        set_repeat_modes: u32,
        set_shuffles: Vec<bool>,
        set_shuffle_flags: Vec<bool>,
        set_queue_with_order: Vec<(bool, Option<Vec<usize>>)>,
        set_queues: u32,
        clear_queues: Vec<bool>,
        play_indexes: Vec<usize>,
        get_tracks_batch: u32,
        start_track_streams: Vec<u64>,
        start_positions: Vec<u64>,
    }

    /// Records every engine call; serves canned `PlaybackState` + queue snapshot.
    struct MockEngine {
        calls: Arc<StdMutex<MockCalls>>,
        playback: PlaybackState,
        queue_tracks: Vec<QueueTrack>,
        queue_index: Option<usize>,
        loaded_audio: bool,
    }

    impl MockEngine {
        fn new() -> Self {
            Self {
                calls: Arc::new(StdMutex::new(MockCalls::default())),
                playback: PlaybackState::default(),
                queue_tracks: Vec::new(),
                queue_index: None,
                loaded_audio: false,
            }
        }

        fn calls(&self) -> std::sync::MutexGuard<'_, MockCalls> {
            self.calls.lock().unwrap()
        }
    }

    #[async_trait]
    impl QconnectRendererEngine for MockEngine {
        fn resume(&self) -> Result<(), String> {
            self.calls().resumes += 1;
            Ok(())
        }
        fn pause(&self) -> Result<(), String> {
            self.calls().pauses += 1;
            Ok(())
        }
        fn stop(&self) -> Result<(), String> {
            self.calls().stops += 1;
            Ok(())
        }
        fn seek(&self, position_secs: u64) -> Result<(), String> {
            self.calls().seeks.push(position_secs);
            Ok(())
        }
        fn set_volume(&self, fraction: f32) -> Result<(), String> {
            self.calls().set_volumes.push(fraction);
            Ok(())
        }
        fn get_playback_state(&self) -> PlaybackState {
            self.playback.clone()
        }
        fn has_loaded_audio(&self) -> bool {
            self.loaded_audio
        }
        async fn set_repeat_mode(&self, _mode: RepeatMode) {
            self.calls().set_repeat_modes += 1;
        }
        async fn set_shuffle(&self, enabled: bool) {
            self.calls().set_shuffles.push(enabled);
        }
        async fn set_shuffle_flag(&self, enabled: bool) {
            self.calls().set_shuffle_flags.push(enabled);
        }
        async fn get_all_queue_tracks(&self) -> (Vec<QueueTrack>, Option<usize>) {
            (self.queue_tracks.clone(), self.queue_index)
        }
        async fn set_queue(&self, _tracks: Vec<QueueTrack>, _start_index: Option<usize>) {
            self.calls().set_queues += 1;
        }
        async fn set_queue_with_order(
            &self,
            _tracks: Vec<QueueTrack>,
            _start_index: Option<usize>,
            shuffle_enabled: bool,
            shuffle_order: Option<Vec<usize>>,
        ) {
            self.calls()
                .set_queue_with_order
                .push((shuffle_enabled, shuffle_order));
        }
        async fn clear_queue(&self, keep_current: bool) {
            self.calls().clear_queues.push(keep_current);
        }
        async fn play_index(&self, index: usize) -> Option<QueueTrack> {
            self.calls().play_indexes.push(index);
            None
        }
        async fn get_track(&self, track_id: u64) -> Result<Track, String> {
            Ok(mock_track(track_id))
        }
        async fn get_tracks_batch(&self, track_ids: &[u64]) -> Result<Vec<Track>, String> {
            self.calls().get_tracks_batch += 1;
            Ok(track_ids.iter().map(|&id| mock_track(id)).collect())
        }
        async fn start_track_stream(
            &self,
            track_id: u64,
            _quality: Quality,
            _duration_secs: u64,
            start_position_secs: u64,
        ) -> Result<(), String> {
            let mut calls = self.calls();
            calls.start_track_streams.push(track_id);
            calls.start_positions.push(start_position_secs);
            Ok(())
        }
        fn current_output_format(&self) -> Option<(u32, u32)> {
            Some((44_100, 16))
        }
    }

    fn qi(track_id: u64, queue_item_id: u64) -> QueueItem {
        QueueItem {
            track_context_uuid: "ctx".to_string(),
            track_id,
            queue_item_id,
        }
    }

    fn mock_track(id: u64) -> Track {
        serde_json::from_value(serde_json::json!({ "id": id, "title": "t", "duration": 100 }))
            .expect("mock track")
    }

    fn mock_queue_track(id: u64) -> QueueTrack {
        model_track_to_core_queue_track(&mock_track(id))
    }

    fn queue_state(
        version: QueueVersion,
        items: Vec<QueueItem>,
        shuffle_mode: bool,
        shuffle_order: Option<Vec<usize>>,
    ) -> QConnectQueueState {
        QConnectQueueState {
            version,
            queue_items: items,
            shuffle_mode,
            shuffle_order,
            autoplay_mode: false,
            autoplay_loading: false,
            autoplay_items: Vec::new(),
            updated_at_ms: 0,
            last_server_queue_hash: None,
            selected_queue_position: None,
        }
    }

    /// A queue the controller pushed with a selection, i.e. what
    /// CTRL_SRVR_QUEUE_TRACKS_LOADED carries when the user taps a track.
    fn pushed_queue_state(
        version: QueueVersion,
        items: Vec<QueueItem>,
        selected_queue_position: u64,
    ) -> QConnectQueueState {
        QConnectQueueState {
            selected_queue_position: Some(selected_queue_position),
            ..queue_state(version, items, false, None)
        }
    }

    fn sync() -> Arc<Mutex<QconnectRemoteSyncState>> {
        Arc::new(Mutex::new(QconnectRemoteSyncState::default()))
    }

    /// #2 — two loads for the same track within the dedup window trigger exactly
    /// one `start_track_stream`; the second is swallowed by the 5s window even
    /// though the audio thread hasn't reported the track yet.
    #[tokio::test]
    async fn ensure_remote_track_loaded_dedups_within_window() {
        let engine = MockEngine::new(); // playback track_id 0 != 42 → would reload
        let sync = sync();
        ensure_remote_track_loaded(&engine, &sync, 42, None, 0)
            .await
            .unwrap();
        ensure_remote_track_loaded(&engine, &sync, 42, None, 0)
            .await
            .unwrap();
        assert_eq!(engine.calls().start_track_streams, vec![42]);
    }

    /// #2 — no reload when the audio thread already plays the requested track.
    #[tokio::test]
    async fn ensure_remote_track_loaded_skips_when_track_unchanged() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 42,
            ..Default::default()
        };
        let sync = sync();
        ensure_remote_track_loaded(&engine, &sync, 42, None, 0)
            .await
            .unwrap();
        assert!(engine.calls().start_track_streams.is_empty());
    }

    /// #1 / #387 — a SetState targeting the SAME track at <=1s while local is well
    /// ahead is a cloud echo: the seek is rejected.
    #[tokio::test]
    async fn apply_renderer_command_rejects_echo_seek() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 7,
            position: 30,
            ..Default::default()
        };
        engine.queue_tracks = vec![mock_queue_track(7)];
        engine.queue_index = Some(0);
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: None,
            current_position_ms: Some(0),
            current_track: Some(qi(7, 0)),
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &cmd, &QConnectRendererState::default())
            .await
            .unwrap();
        assert!(
            engine.calls().seeks.is_empty(),
            "echo seek must be rejected (#387 is_echo_reset)"
        );
    }

    /// A queue push starts track 0 while the engine still reports the previous
    /// track's clock; the cloud's SetState for the NEW track then arrives with
    /// position 0. Seeking on that comparison tore the engine down 140 ms into
    /// every freshly started track (an audible click). The frame's position
    /// belongs to a track we are not on yet, so it must not seek.
    #[tokio::test]
    async fn apply_renderer_command_ignores_a_position_meant_for_another_track() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 62589635, // the track that is still playing out
            position: 62,
            ..Default::default()
        };
        // Already loaded and playing, so the load path short-circuits and
        // leaves `stream_started_at` unset — exactly as observed.
        engine.queue_tracks = vec![mock_queue_track(350617010)];
        engine.queue_index = Some(0);
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: None,
            current_position_ms: Some(0),
            current_track: Some(qi(350617010, 0)),
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &cmd, &QConnectRendererState::default())
            .await
            .unwrap();
        assert!(
            engine.calls().seeks.is_empty(),
            "a position for track 350617010 must not seek an engine on 62589635"
        );
    }

    /// The state-only shape (no track named) DOES refer to what we are playing,
    /// so the guard above must not swallow a real seek from a peer controller.
    #[tokio::test]
    async fn apply_renderer_command_still_honors_a_trackless_seek() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 7,
            position: 10,
            ..Default::default()
        };
        engine.queue_tracks = vec![mock_queue_track(7)];
        engine.queue_index = Some(0);
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: None,
            current_position_ms: Some(90_000),
            current_track: None,
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &cmd, &QConnectRendererState::default())
            .await
            .unwrap();
        assert_eq!(engine.calls().seeks, vec![90]);
    }

    /// #1 / #387 — a genuine peer seek (target far from local) IS honored, even
    /// for the same track (the bug the all-or-nothing peer gate caused).
    #[tokio::test]
    async fn apply_renderer_command_honors_genuine_seek() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 7,
            position: 10,
            ..Default::default()
        };
        engine.queue_tracks = vec![mock_queue_track(7)];
        engine.queue_index = Some(0);
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: None,
            current_position_ms: Some(40_000),
            current_track: Some(qi(7, 0)),
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &cmd, &QConnectRendererState::default())
            .await
            .unwrap();
        assert_eq!(engine.calls().seeks, vec![40]);
    }

    /// A state-only resume (current_track = null) on a COLD engine — queue
    /// cursor restored but no audio loaded, as after a daemon restart — must
    /// cold-load the cloud's current track at the handed-off position instead
    /// of issuing a bare resume that dies in the audio thread ("cannot resume
    /// - no audio data available") and wedges the controller at paused 0:00.
    #[tokio::test]
    async fn apply_renderer_command_cold_resume_loads_current_track() {
        let mut engine = MockEngine::new();
        // Cursor reports the restored track, but nothing is loaded: the plain
        // track-id guard would skip, which is why the force path is used.
        engine.playback = PlaybackState {
            track_id: 7,
            ..Default::default()
        };
        engine.queue_tracks = vec![mock_queue_track(7)];
        engine.queue_index = Some(0);
        engine.loaded_audio = false;
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PLAYING),
            current_position_ms: None,
            current_track: None,
            next_track: None,
        };
        // The cloud's view carries the session's current track + position.
        let renderer_state = QConnectRendererState {
            current_track: Some(qi(7, 2)),
            current_position_ms: Some(242_491),
            ..Default::default()
        };
        apply_renderer_command(&engine, &sync, &cmd, &renderer_state)
            .await
            .unwrap();
        let calls = engine.calls();
        assert_eq!(
            calls.start_track_streams,
            vec![7],
            "cold resume must load the session's current track"
        );
        assert_eq!(
            calls.start_positions,
            vec![242],
            "load must resume at the handed-off position"
        );
        assert_eq!(calls.resumes, 0, "no bare resume on a cold engine");
        assert!(
            calls.seeks.is_empty(),
            "the load already starts at the position; a seek here is dropped for \
             being past the buffered watermark, or rebuilds the engine and \
             re-runs a multi-second sample pre-skip"
        );
    }

    /// A peer track-change (position 0) must not seek either: the fresh stream
    /// already starts at 0, while the engine still reports the PREVIOUS track's
    /// position, so the comparison that drives the seek is meaningless.
    #[tokio::test]
    async fn apply_renderer_command_track_change_does_not_seek_after_load() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 7,
            position: 84, // still the outgoing track's position
            ..Default::default()
        };
        engine.queue_tracks = vec![mock_queue_track(7), mock_queue_track(8)];
        engine.queue_index = Some(0);
        engine.loaded_audio = true;
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PLAYING),
            current_position_ms: Some(0),
            current_track: Some(qi(8, 1)),
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &cmd, &QConnectRendererState::default())
            .await
            .unwrap();
        let calls = engine.calls();
        assert_eq!(calls.start_track_streams, vec![8], "loads the new track");
        assert_eq!(calls.start_positions, vec![0]);
        assert!(calls.seeks.is_empty(), "no redundant seek after the load");
    }

    /// moOde forum (the_bertrum, three 10.3.3 boxes): "play 20 seconds of a
    /// track then skip to the next and the next track starts at 20 seconds in".
    ///
    /// The cloud's reducer updates `current_track` and `current_position_ms`
    /// independently, each only when the command carries it. A next-track frame
    /// names the new track and NO position, so the renderer view handed to us
    /// already says track 8 while still holding track 7's 20 s. Preferring that
    /// retained position broke the new track twice over — the load started
    /// there, and the seek block then drove it there again.
    #[tokio::test]
    async fn apply_renderer_command_track_change_ignores_the_outgoing_position() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 7,
            position: 20,
            ..Default::default()
        };
        engine.queue_tracks = vec![mock_queue_track(7), mock_queue_track(8)];
        engine.queue_index = Some(0);
        engine.loaded_audio = true;
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PLAYING),
            current_position_ms: None, // a track change carries no position
            current_track: Some(qi(8, 1)),
            next_track: None,
        };
        // What the reducer leaves behind: the new track, the OLD position.
        let renderer_state = QConnectRendererState {
            current_position_ms: Some(20_000),
            current_track: Some(qi(8, 1)),
            ..Default::default()
        };
        apply_renderer_command(&engine, &sync, &cmd, &renderer_state)
            .await
            .unwrap();
        let calls = engine.calls();
        assert_eq!(calls.start_track_streams, vec![8], "loads the new track");
        assert_eq!(
            calls.start_positions,
            vec![0],
            "a track change with no position of its own starts from the beginning"
        );
        assert!(
            calls.seeks.is_empty(),
            "and nothing drags it to the outgoing track's position afterwards"
        );
    }

    /// The other half of that rule: a frame that names NEITHER a track nor a
    /// position is the takeback shape — SetActive landed before the cloud knew
    /// our current_track — and there the retained renderer position is the only
    /// thing carrying where the peer left off, so it must still be honored.
    #[tokio::test]
    async fn apply_renderer_command_state_only_resume_uses_the_retained_position() {
        let engine = MockEngine::new(); // cold: no loaded audio
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PLAYING),
            current_position_ms: None,
            current_track: None,
            next_track: None,
        };
        let renderer_state = QConnectRendererState {
            current_position_ms: Some(106_000),
            current_track: Some(qi(9, 0)),
            ..Default::default()
        };
        apply_renderer_command(&engine, &sync, &cmd, &renderer_state)
            .await
            .unwrap();
        let calls = engine.calls();
        assert_eq!(calls.start_track_streams, vec![9]);
        assert_eq!(
            calls.start_positions,
            vec![106],
            "resume the peer's position rather than restarting the track"
        );
    }

    /// The peer whose render we just took over stops its own local playback,
    /// and the cloud relays that stopped@0 to us right after telling us to
    /// play. Honoring it killed the stream we had just started.
    #[tokio::test]
    async fn apply_renderer_command_ignores_the_handoff_stop_echo() {
        let engine = MockEngine::new();
        let sync = sync();
        // Play the track: records the load attempt the echo check keys on.
        let play = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PLAYING),
            current_position_ms: Some(60_000),
            current_track: Some(qi(9, 0)),
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &play, &QConnectRendererState::default())
            .await
            .unwrap();
        // The peer's stop lands milliseconds later: same track, position 0.
        let echo = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_STOPPED),
            current_position_ms: Some(0),
            current_track: Some(qi(9, 0)),
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &echo, &QConnectRendererState::default())
            .await
            .unwrap();
        let calls = engine.calls();
        assert_eq!(calls.start_track_streams, vec![9], "the play still loads");
        assert_eq!(calls.stops, 0, "the handoff stop echo must not stop us");
    }

    /// SetActive(true) must NOT load: at that moment the renderer state still
    /// describes what WE last played, and a peer holding the render may have
    /// moved on. Loading from that stale view took the render back onto our old
    /// track (…974) while the peer was actually on another (…969). The
    /// authoritative SetState follows within a few hundred ms.
    #[tokio::test]
    async fn apply_renderer_command_setactive_does_not_load_from_stale_state() {
        let engine = MockEngine::new();
        let sync = sync();
        let cmd = RendererCommand::SetActive { active: true };
        // Stale: our previous track and the position we handed off at.
        let renderer_state = QConnectRendererState {
            current_track: Some(qi(410251974, 4)),
            current_position_ms: Some(30_000),
            ..Default::default()
        };
        apply_renderer_command(&engine, &sync, &cmd, &renderer_state)
            .await
            .unwrap();
        let calls = engine.calls();
        assert!(
            calls.start_track_streams.is_empty(),
            "must wait for the session's own SetState instead of guessing"
        );
        assert_eq!(calls.stops, 0, "and must not stop anything either");
    }

    /// The SetState that follows carries the real track and position, and that
    /// is what loads — the takeback lands on the peer's track, not ours.
    #[tokio::test]
    async fn apply_renderer_command_setstate_after_setactive_loads_the_peer_track() {
        let engine = MockEngine::new();
        let sync = sync();
        let activate = RendererCommand::SetActive { active: true };
        let stale = QConnectRendererState {
            current_track: Some(qi(410251974, 4)),
            current_position_ms: Some(30_000),
            ..Default::default()
        };
        apply_renderer_command(&engine, &sync, &activate, &stale)
            .await
            .unwrap();
        // The cloud's SetState: the session is on track …969 at 1:19.
        let set_state = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PLAYING),
            current_position_ms: Some(79_818),
            current_track: Some(qi(410251969, 0)),
            next_track: None,
        };
        // The core reducer has already folded the command in by this point.
        let reduced = QConnectRendererState {
            current_track: Some(qi(410251969, 0)),
            current_position_ms: Some(79_818),
            ..Default::default()
        };
        apply_renderer_command(&engine, &sync, &set_state, &reduced)
            .await
            .unwrap();
        let calls = engine.calls();
        assert_eq!(
            calls.start_track_streams,
            vec![410251969],
            "the peer's track"
        );
        assert_eq!(calls.start_positions, vec![79], "at the peer's position");
    }

    /// The echo can also name a DIFFERENT track than the one we just started:
    /// the peer resets its own cursor to the head of the queue as it stops.
    /// Observed naming queue item 0 while item 4 was loading, which killed the
    /// stream ("play superseded, abandoning") and left the app silent.
    #[tokio::test]
    async fn apply_renderer_command_ignores_a_handoff_stop_naming_another_track() {
        let engine = MockEngine::new();
        let sync = sync();
        let play = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PLAYING),
            current_position_ms: Some(168_000),
            current_track: Some(qi(442682701, 4)),
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &play, &QConnectRendererState::default())
            .await
            .unwrap();
        // The peer stops, reporting the queue head rather than our track.
        let echo = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_STOPPED),
            current_position_ms: Some(0),
            current_track: Some(qi(442682697, 0)),
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &echo, &QConnectRendererState::default())
            .await
            .unwrap();
        let calls = engine.calls();
        assert_eq!(calls.start_track_streams, vec![442682701]);
        assert_eq!(
            calls.stops, 0,
            "a stop naming a track we are not playing must not stop us mid-handoff"
        );
    }

    /// The same echo also arrives as a STATE-ONLY pause (no track, no
    /// position) — the shape observed when switching output mid-track from the
    /// desktop app, which left the device spinning until the user pressed play.
    #[tokio::test]
    async fn apply_renderer_command_ignores_a_state_only_pause_echo() {
        let engine = MockEngine::new();
        let sync = sync();
        let play = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PLAYING),
            current_position_ms: Some(139_691),
            current_track: Some(qi(9, 0)),
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &play, &QConnectRendererState::default())
            .await
            .unwrap();
        let echo = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PAUSED),
            current_position_ms: None,
            current_track: None,
            next_track: None,
        };
        // The cloud's view still carries the handed-off position; the echo check
        // must not read it as "hold here".
        let renderer_state = QConnectRendererState {
            current_position_ms: Some(139_691),
            ..Default::default()
        };
        apply_renderer_command(&engine, &sync, &echo, &renderer_state)
            .await
            .unwrap();
        assert_eq!(
            engine.calls().pauses,
            0,
            "the state-only pause echo must not pause the stream we just started"
        );
    }

    /// A pause that is not part of a handoff burst still pauses.
    #[tokio::test]
    async fn apply_renderer_command_honors_a_genuine_pause() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 9,
            position: 45,
            ..Default::default()
        };
        engine.loaded_audio = true;
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PAUSED),
            current_position_ms: None,
            current_track: None,
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &cmd, &QConnectRendererState::default())
            .await
            .unwrap();
        assert_eq!(
            engine.calls().pauses,
            1,
            "a real pause must reach the engine"
        );
    }

    /// A stop for a track we did NOT just load is a real stop.
    #[tokio::test]
    async fn apply_renderer_command_honors_a_genuine_stop() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 9,
            position: 45,
            ..Default::default()
        };
        engine.loaded_audio = true;
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_STOPPED),
            current_position_ms: Some(0),
            current_track: Some(qi(9, 0)),
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &cmd, &QConnectRendererState::default())
            .await
            .unwrap();
        assert_eq!(engine.calls().stops, 1, "a real stop must reach the engine");
    }

    /// A genuine mid-track seek (no load this command) still reaches the engine.
    #[tokio::test]
    async fn apply_renderer_command_real_seek_still_applies() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 7,
            position: 10,
            ..Default::default()
        };
        engine.queue_tracks = vec![mock_queue_track(7)];
        engine.queue_index = Some(0);
        engine.loaded_audio = true;
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: None,
            current_position_ms: Some(90_000),
            current_track: None,
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &cmd, &QConnectRendererState::default())
            .await
            .unwrap();
        let calls = engine.calls();
        assert!(
            calls.start_track_streams.is_empty(),
            "no load: the track is already playing"
        );
        assert_eq!(calls.seeks, vec![90], "a real seek must still be honored");
    }

    /// The cold-start load never fires while audio is loaded: a resume during
    /// live playback (or a cloud echo) stays a plain resume, no re-stream.
    #[tokio::test]
    async fn apply_renderer_command_warm_resume_stays_plain() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 7,
            position: 30,
            ..Default::default()
        };
        engine.queue_tracks = vec![mock_queue_track(7)];
        engine.queue_index = Some(0);
        engine.loaded_audio = true;
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PLAYING),
            current_position_ms: None,
            current_track: None,
            next_track: None,
        };
        let renderer_state = QConnectRendererState {
            current_track: Some(qi(7, 2)),
            ..Default::default()
        };
        apply_renderer_command(&engine, &sync, &cmd, &renderer_state)
            .await
            .unwrap();
        let calls = engine.calls();
        assert!(
            calls.start_track_streams.is_empty(),
            "no re-stream while audio is loaded"
        );
        assert_eq!(calls.resumes, 1, "plain resume on a warm engine");
    }

    /// #4 — WS-authoritative shuffle: a standalone SetShuffleMode must flip the
    /// flag ONLY (set_shuffle_flag), NEVER generate a local order. It must not
    /// call the order-generating set_shuffle, and must not apply any queue order
    /// (set_queue_with_order) — the cloud's order arrives separately.
    #[tokio::test]
    async fn apply_renderer_command_setshufflemode_is_flag_only() {
        let mut engine = MockEngine::new();
        engine.queue_tracks = vec![
            mock_queue_track(1),
            mock_queue_track(2),
            mock_queue_track(3),
        ];
        engine.queue_index = Some(0);
        let sync = sync();
        let cmd = RendererCommand::SetShuffleMode { shuffle_mode: true };
        apply_renderer_command(&engine, &sync, &cmd, &QConnectRendererState::default())
            .await
            .unwrap();
        let calls = engine.calls();
        assert_eq!(
            calls.set_shuffle_flags,
            vec![true],
            "SetShuffleMode must take the flag-only path"
        );
        assert!(
            calls.set_shuffles.is_empty(),
            "SetShuffleMode must NEVER call the order-generating set_shuffle (WS-authoritative rule)"
        );
        assert!(
            calls.set_queue_with_order.is_empty(),
            "SetShuffleMode must not apply any local order; the cloud's order arrives separately"
        );
    }

    /// #3 — a state-only SetState (command.current_track = None) must NOT align the
    /// cursor or load a track from the renderer_state's stale current_track (the
    /// iOS-pause-jumped-back fix); the playing_state is still applied.
    #[tokio::test]
    async fn apply_renderer_command_skips_track_ops_on_state_only_update() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 7,
            position: 5,
            ..Default::default()
        };
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PAUSED),
            current_position_ms: None,
            current_track: None,
            next_track: None,
        };
        // renderer_state carries a STALE current_track the projection would fall
        // back to — it must not drive a load/align.
        let renderer_state = QConnectRendererState {
            current_track: Some(qi(99, 0)),
            ..Default::default()
        };
        apply_renderer_command(&engine, &sync, &cmd, &renderer_state)
            .await
            .unwrap();
        let calls = engine.calls();
        assert!(
            calls.start_track_streams.is_empty(),
            "no load on state-only update"
        );
        assert!(
            calls.play_indexes.is_empty(),
            "no cursor align on state-only update"
        );
        assert_eq!(calls.pauses, 1, "pause still applied");
    }

    /// #5 — shuffle deferral: the first event (shuffle_mode=true, order=None)
    /// materializes with shuffle_enabled=false (no invented identity order); the
    /// second event (authoritative order present) enables shuffle.
    #[tokio::test]
    async fn materialize_defers_shuffle_until_authoritative_order() {
        let engine = MockEngine::new();
        let sync = sync();
        let items = vec![qi(10, 0), qi(11, 1)];

        let q1 = queue_state(QueueVersion::new(1, 0), items.clone(), true, None);
        materialize_remote_queue(&engine, &sync, &q1).await.unwrap();
        {
            let calls = engine.calls();
            assert_eq!(calls.set_queue_with_order.len(), 1);
            assert!(
                !calls.set_queue_with_order[0].0,
                "shuffle deferred while order absent"
            );
        }

        let q2 = queue_state(QueueVersion::new(1, 1), items, true, Some(vec![1, 0]));
        materialize_remote_queue(&engine, &sync, &q2).await.unwrap();
        {
            let calls = engine.calls();
            assert_eq!(calls.set_queue_with_order.len(), 2);
            assert!(
                calls.set_queue_with_order[1].0,
                "shuffle enabled once authoritative order present"
            );
        }
    }

    /// The 13-track album from the "last track of a playlist never plays" trace:
    /// the user taps the LAST entry while entry 1 is playing. The push carries
    /// `queue_position: 12` and the only frame that follows is a state-only
    /// SetState (`current_track: null`), so this materialization is the sole
    /// chance to start track 12 — and the renderer projection still names the
    /// outgoing track, which is what used to win.
    #[tokio::test]
    async fn materialize_starts_the_pushed_selection_when_no_setstate_names_it() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 410609158,
            ..Default::default()
        };
        let sync = sync();
        {
            let mut state = sync.lock().await;
            state.last_renderer_queue_item_id = Some(1);
            state.last_renderer_track_id = Some(410609158);
            state.last_renderer_playing_state = Some(PLAYING_STATE_PLAYING);
        }
        let items: Vec<QueueItem> = (0..13).map(|i| qi(410609157 + i, i)).collect();
        let pushed = pushed_queue_state(QueueVersion::new(4, 1), items, 12);

        materialize_remote_queue(&engine, &sync, &pushed)
            .await
            .unwrap();

        assert_eq!(
            sync.lock().await.last_materialized_start_index,
            Some(12),
            "the selection outranks the outgoing track's projection"
        );
        assert_eq!(
            engine.calls().start_track_streams,
            vec![410609169],
            "the selected track is started, since no SetState will name it"
        );
    }

    /// A selection made while the session is not playing is the controller
    /// staging a track, not asking for audio: point the cursor at it, start
    /// nothing.
    #[tokio::test]
    async fn materialize_stages_a_pushed_selection_without_playing_it() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 410609158,
            ..Default::default()
        };
        let sync = sync();
        {
            let mut state = sync.lock().await;
            state.last_renderer_queue_item_id = Some(1);
            state.last_renderer_track_id = Some(410609158);
            state.last_renderer_playing_state = Some(PLAYING_STATE_PAUSED);
        }
        let items: Vec<QueueItem> = (0..13).map(|i| qi(410609157 + i, i)).collect();
        let pushed = pushed_queue_state(QueueVersion::new(4, 1), items, 12);

        materialize_remote_queue(&engine, &sync, &pushed)
            .await
            .unwrap();

        assert_eq!(sync.lock().await.last_materialized_start_index, Some(12));
        assert!(
            engine.calls().start_track_streams.is_empty(),
            "a paused session gets no unrequested audio"
        );
    }

    /// Picking an album the renderer is not playing from: the controller pushes
    /// a whole new queue with `queue_position: null` and never names a current
    /// track, while the cloud keeps reporting the OUTGOING track's position.
    /// Moving the cursor is not enough — that is the "app shows the track I
    /// picked, speakers play the previous one" report.
    #[tokio::test]
    async fn materialize_starts_the_head_when_a_replacement_names_no_track() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 29450960, // still playing, and absent from the new queue
            ..Default::default()
        };
        let sync = sync();
        {
            let mut state = sync.lock().await;
            state.last_renderer_queue_item_id = Some(6);
            state.last_renderer_track_id = Some(29450960);
            state.last_renderer_playing_state = Some(PLAYING_STATE_PLAYING);
        }
        let replacement = queue_state(
            QueueVersion::new(14, 1),
            vec![qi(3001, 0), qi(3002, 1), qi(3003, 2)],
            false,
            None,
        );

        materialize_remote_queue(&engine, &sync, &replacement)
            .await
            .unwrap();

        assert_eq!(
            engine.calls().start_track_streams,
            vec![3001],
            "the new queue's head is played, not just pointed at"
        );
    }

    /// The handoff shape must NOT be caught by the rule above: the peer's track
    /// IS in the pushed queue, so the SetState that follows owns the load — and
    /// it carries the handed-off position, which starting here would discard.
    #[tokio::test]
    async fn materialize_leaves_a_handoff_queue_to_the_following_setstate() {
        let engine = MockEngine::new(); // nothing playing locally yet
        let sync = sync();
        {
            let mut state = sync.lock().await;
            state.last_renderer_queue_item_id = Some(1);
            state.last_renderer_track_id = Some(3002);
            state.last_renderer_playing_state = Some(PLAYING_STATE_PLAYING);
        }
        let handoff = queue_state(
            QueueVersion::new(2, 0),
            vec![qi(3001, 0), qi(3002, 1), qi(3003, 2)],
            false,
            None,
        );

        materialize_remote_queue(&engine, &sync, &handoff)
            .await
            .unwrap();

        assert!(
            engine.calls().start_track_streams.is_empty(),
            "the peer's track is in this queue; SetState resumes it at its position"
        );
    }

    /// A cast pushes the queue with `queue_position` naming the very track the
    /// cloud already reports as current — that is a handoff, not a new
    /// selection. Starting it here would restart it at 0 and the dedup window
    /// would then swallow the SetState carrying the handed-off position.
    #[tokio::test]
    async fn materialize_leaves_a_selection_the_cloud_already_names_to_setstate() {
        let engine = MockEngine::new();
        let sync = sync();
        {
            let mut state = sync.lock().await;
            state.last_renderer_queue_item_id = Some(1);
            state.last_renderer_track_id = Some(3002);
            state.last_renderer_playing_state = Some(PLAYING_STATE_PLAYING);
        }
        let cast = pushed_queue_state(
            QueueVersion::new(2, 0),
            vec![qi(3001, 0), qi(3002, 1), qi(3003, 2)],
            1, // the position the peer was playing
        );

        materialize_remote_queue(&engine, &sync, &cast)
            .await
            .unwrap();

        assert!(
            engine.calls().start_track_streams.is_empty(),
            "SetState owns this load; it knows the handed-off position"
        );
    }

    /// A queue event that carries no selection (every mutation other than
    /// TracksLoaded) keeps the previous behaviour exactly: the start index comes
    /// from the renderer projection and nothing is started.
    #[tokio::test]
    async fn materialize_without_a_selection_keeps_the_projection_start_index() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 410609158,
            ..Default::default()
        };
        let sync = sync();
        {
            let mut state = sync.lock().await;
            state.last_renderer_queue_item_id = Some(1);
            state.last_renderer_track_id = Some(410609158);
            state.last_renderer_playing_state = Some(PLAYING_STATE_PLAYING);
        }
        let items: Vec<QueueItem> = (0..13).map(|i| qi(410609157 + i, i)).collect();
        let plain = queue_state(QueueVersion::new(4, 1), items, false, None);

        materialize_remote_queue(&engine, &sync, &plain)
            .await
            .unwrap();

        assert_eq!(sync.lock().await.last_materialized_start_index, Some(1));
        assert!(engine.calls().start_track_streams.is_empty());
    }

    /// #1 (takeback) — the prior controller->renderer stop() cleared the audio
    /// buffer but left a stale track id, so a plain track-id guard would skip
    /// the reload and a later resume() would die with "no audio data
    /// available". That reload still happens, but on the STATE-ONLY resume
    /// (whose cold-engine path force-streams) rather than on SetActive, which
    /// has no trustworthy view of the session's current track yet.
    #[tokio::test]
    async fn takeback_reloads_torn_down_audio_on_the_following_resume() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 7, // stale id left by stop(); audio is gone
            ..Default::default()
        };
        engine.loaded_audio = false;
        let sync = sync();
        let renderer_state = QConnectRendererState {
            current_track: Some(qi(7, 0)),
            current_position_ms: Some(45_000),
            ..Default::default()
        };
        apply_renderer_command(
            &engine,
            &sync,
            &RendererCommand::SetActive { active: true },
            &renderer_state,
        )
        .await
        .unwrap();
        assert!(
            engine.calls().start_track_streams.is_empty(),
            "SetActive alone must not guess a track to load"
        );
        // The cloud's resume for this session lands next.
        apply_renderer_command(
            &engine,
            &sync,
            &RendererCommand::SetState {
                playing_state: Some(PLAYING_STATE_PLAYING),
                current_position_ms: None,
                current_track: None,
                next_track: None,
            },
            &renderer_state,
        )
        .await
        .unwrap();
        let calls = engine.calls();
        assert_eq!(
            calls.start_track_streams,
            vec![7],
            "the cold engine must be reloaded rather than bare-resumed"
        );
        assert_eq!(
            calls.start_positions,
            vec![45],
            "and resume at the handed-off position, not 0"
        );
        assert_eq!(calls.resumes, 0, "no bare resume onto an empty buffer");
    }

    /// #1 (no-interrupt) — a SetActive(true) while the renderer is ALREADY
    /// streaming this exact track with audio loaded must NOT restart it (guards
    /// against a spurious activation tearing down live playback).
    #[tokio::test]
    async fn set_active_does_not_restart_when_already_streaming() {
        let mut engine = MockEngine::new();
        engine.playback = PlaybackState {
            track_id: 7,
            ..Default::default()
        };
        engine.loaded_audio = true; // live playback in progress
        let sync = sync();
        let cmd = RendererCommand::SetActive { active: true };
        let renderer_state = QConnectRendererState {
            current_track: Some(qi(7, 0)),
            current_position_ms: Some(45_000),
            ..Default::default()
        };
        apply_renderer_command(&engine, &sync, &cmd, &renderer_state)
            .await
            .unwrap();
        assert!(
            engine.calls().start_track_streams.is_empty(),
            "must not restart an already-streaming track on a spurious SetActive"
        );
    }

    /// #1 (takeback first-load via SetState) — when the FIRST load on a takeback
    /// lands in the SetState path (SetActive arrived before current_track was
    /// known, so the force-stream couldn't fire), the load must stream at the
    /// cloud's reported position, not 0 — so a mid-track takeback resumes where
    /// the peer was instead of restarting (a forward seek past the buffered
    /// watermark is silently ignored, so streaming from 0 stuck at the start).
    #[tokio::test]
    async fn apply_renderer_command_setstate_streams_at_reported_position() {
        let engine = MockEngine::new(); // playback track_id 0 → fresh load
        let sync = sync();
        let cmd = RendererCommand::SetState {
            playing_state: Some(PLAYING_STATE_PLAYING),
            current_position_ms: Some(118_000),
            current_track: Some(qi(7, 1)),
            next_track: None,
        };
        apply_renderer_command(&engine, &sync, &cmd, &QConnectRendererState::default())
            .await
            .unwrap();
        let calls = engine.calls();
        assert_eq!(calls.start_track_streams, vec![7], "fresh takeback load");
        assert_eq!(
            calls.start_positions,
            vec![118],
            "takeback load must resume at the cloud position (118s), not 0"
        );
    }
}
