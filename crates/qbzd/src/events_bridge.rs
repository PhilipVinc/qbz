// crates/qbzd/src/events_bridge.rs — bridge playback-driver edges onto the
// CoreEvent bus.
//
// The 450 ms playback driver detects track/play-state transitions
// (`DriverAction::ReportEdge` -> `deps.on_edge`), but nothing translated those
// edges into the playback CoreEvent variants the bus consumers were written
// for: mpris.rs and scrobble_engine.rs both carry TrackStarted /
// PlaybackStateChanged / PositionUpdated / VolumeChanged arms that never
// fired, and `/api/events` (SSE) / `qbzd watch` carried no playback traffic at
// all. This task closes that gap: it wakes on the same edge Notify pulse the
// QConnect report scheduler uses (with a 2 s poll fallback, because
// `plan_tick` only reports edges while a track id is present — a stop that
// clears the track id would otherwise go unseen), reads the live player state,
// dedups against what it last published, and sends the delta to the bus.
//
// Holds only a Weak<AppRuntime> (upgraded per wake, dropped before the next
// wait), so it sits outside the #521 audio-release ordering — the caller
// aborts it for a clean shutdown, same as the queue-persist subscriber.
use std::sync::{Arc, Weak};
use std::time::Duration;

use qbz_app::shell::AppRuntime;
use qbz_models::{CoreEvent, PlaybackState};
use tokio::sync::{broadcast, Notify};
use tokio::task::JoinHandle;

use crate::adapter::DaemonAdapter;

type Runtime = Arc<AppRuntime<DaemonAdapter>>;

/// The poll fallback cadence: only exercised when no edge pulse arrives (e.g.
/// a stop cleared the track id, which suppresses `ReportEdge`).
const FALLBACK_POLL: Duration = Duration::from_secs(2);

/// Spawn the bridge task. Emits, deduped against the last published values:
/// * `TrackStarted` on a track-id change while playing (with the queue's
///   current-track metadata),
/// * `PlaybackStateChanged` on a playing/paused/stopped change,
/// * `PositionUpdated` on every wake while playing (~2 s cadence, the
///   scrobbler's timing source and the MPRIS progress feed),
/// * `VolumeChanged` on a volume change.
pub fn spawn(
    runtime: &Runtime,
    bus: broadcast::Sender<CoreEvent>,
    edge: Arc<Notify>,
) -> JoinHandle<()> {
    let weak: Weak<AppRuntime<DaemonAdapter>> = Arc::downgrade(runtime);
    tokio::spawn(async move {
        let mut last_track_id: u64 = 0;
        let mut last_state: Option<PlaybackState> = None;
        let mut last_volume: Option<f32> = None;
        loop {
            // Wake on a driver edge, or fall back to a slow poll so a
            // track-id-clearing stop still surfaces as a transition.
            let _ = tokio::time::timeout(FALLBACK_POLL, edge.notified()).await;

            let Some(rt) = weak.upgrade() else { return };
            let core = rt.core();
            let player = core.player();
            let ev = player.get_playback_event();
            let has_audio = player.has_loaded_audio();

            if ev.track_id != 0 && ev.track_id != last_track_id && ev.is_playing {
                // The queue's current track carries the metadata (title/artist/
                // artwork). Skip when the cursor hasn't caught up yet — the next
                // wake retries because last_track_id only advances on a send.
                let queue = core.get_queue_state().await;
                if let Some(track) = queue.current_track.as_ref().filter(|t| t.id == ev.track_id) {
                    let _ = bus.send(CoreEvent::TrackStarted {
                        track: track.clone(),
                        position_secs: ev.position,
                    });
                    last_track_id = ev.track_id;
                }
            }

            // Same three-way mapping as the MPRIS seed: loaded-but-not-playing
            // audio is Paused, nothing loaded is Stopped. Dedup on the MAPPED
            // state, not `is_playing` — paused and stopped are both
            // not-playing, and a pause -> stop transition must still emit.
            let state = if ev.is_playing {
                PlaybackState::Playing
            } else if has_audio {
                PlaybackState::Paused
            } else {
                PlaybackState::Stopped
            };
            if last_state != Some(state) {
                let _ = bus.send(CoreEvent::PlaybackStateChanged { state });
                last_state = Some(state);
                // A stop ends the "current track": forget it so replaying the
                // same track emits a fresh TrackStarted (scrobbling, hooks).
                if state == PlaybackState::Stopped {
                    last_track_id = 0;
                }
            }

            if ev.is_playing {
                let _ = bus.send(CoreEvent::PositionUpdated {
                    position_secs: ev.position,
                    duration_secs: ev.duration,
                });
            }

            if last_volume.is_none_or(|v| (v - ev.volume).abs() > 0.001) {
                let _ = bus.send(CoreEvent::VolumeChanged { volume: ev.volume });
                last_volume = Some(ev.volume);
            }
        }
    })
}
