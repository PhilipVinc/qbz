// TODO(converge: qconnect-glue) — copied from crates/qbz/src/qconnect_engine.rs @ c8ef2a1b;
// do not fix bugs here without fixing the source, and vice versa.
//
//! Qobuz Connect renderer engine for the qbzd daemon.
//!
//! Implements [`qconnect_app::QconnectRendererEngine`] over the daemon
//! `AppRuntime`'s `QbzCore` + `Player`, so qbzd becomes a QConnect renderer
//! that inherits the shared echo/cursor/materialize/shuffle orchestration in
//! `qconnect_app::renderer` instead of re-deriving it.
//!
//! The protected bit-perfect seams (`play_streaming_dynamic` / `play_data`) and
//! the HTTP feeder live here, impl-side, exactly as the Tauri `CoreBridge` impl
//! does; the probe-derived sample_rate/channels/bit_depth flow STRAIGHT into
//! `play_streaming_dynamic` (never defaulted, or hi-res remote playback silently
//! resamples). The feeder body is a near-verbatim port of the Tauri
//! `track_loading.rs` feeder, with `bridge.player()` -> `self.core().player()`;
//! the only deviation is the TLS backend — the crates workspace `reqwest` ships
//! `rustls-tls` (not `native-tls`), so the `.use_native_tls()` calls are dropped.
//! TLS is transport encryption only; the decoded audio bytes are identical, so
//! bit-perfect is unaffected. (If the Qobuz streaming CDN ever presents a cert
//! rustls rejects, add `native-tls` to qbzd's reqwest features.)
#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use qbz_app::shell::AppRuntime;
use qbz_core::QbzCore;
use qbz_models::{Quality, QueueTrack, RepeatMode, Track};
use qbz_player::PlaybackState;
use qconnect_app::QconnectRendererEngine;

use crate::adapter::DaemonAdapter;

// T10 (OD4, §7.4): daemon-only volume policy. The desktop has no equivalent —
// it always applies remote volume. The mode is read from the daemon-root
// `qconnect_settings.db` `volume_mode` KV key (transport::load_volume_mode_at)
// at connect time and injected into the engine + session host.
/// How the daemon treats a controller's remote volume command (01 §7.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VolumeMode {
    /// OD4 DEFAULT. Remote `SetVolume` is applied to the player via the core,
    /// and the player's real volume is reported back to the controller.
    #[default]
    Software,
    /// Bit-perfect purist. The player stays at 100 % (no software attenuation);
    /// remote `SetVolume` is acknowledged-but-ignored (logged at info) and 100
    /// is reported. For DACs feeding power amps where software gain is unwanted.
    Locked,
}

impl VolumeMode {
    /// Parse the `volume_mode` KV value. Anything but the literal `"locked"`
    /// (unset, empty, unknown) falls back to `Software` — the OD4 default.
    pub fn from_kv(value: Option<&str>) -> Self {
        match value.map(str::trim) {
            Some("locked") => VolumeMode::Locked,
            _ => VolumeMode::Software,
        }
    }

    /// Whether a controller's remote `SetVolume` should reach the player. True
    /// only in `Software`; `Locked` acknowledges-but-ignores.
    pub fn applies_remote_volume(self) -> bool {
        matches!(self, VolumeMode::Software)
    }

    /// The volume (0-100 percent) to REPORT to the controller given the player's
    /// real 0.0-1.0 fraction. `Software` reports the real (rounded) percent;
    /// `Locked` always reports 100 regardless of the player's actual level.
    pub fn reported_volume_pct(self, real_fraction: f32) -> i32 {
        match self {
            VolumeMode::Software => (real_fraction.clamp(0.0, 1.0) * 100.0).round() as i32,
            VolumeMode::Locked => 100,
        }
    }
}

/// QConnect renderer engine backed by the daemon `AppRuntime`. Holds the shared
/// runtime and forwards every trait method through `runtime.core()`; the async
/// feeder spawns on the ambient tokio runtime (`start_track_stream` is always
/// awaited from a runtime task).
pub struct DaemonRendererEngine {
    runtime: Arc<AppRuntime<DaemonAdapter>>,
    /// T10 (OD4): resolved volume policy for this session (from the KV at connect).
    volume_mode: VolumeMode,
    /// DAEMON-ONLY: the current track's progressive-download feeder. A track
    /// change MUST abort the previous feeder — left alone it downloads the
    /// full file at line speed to the very end, and a few quick skips stack
    /// concurrent hi-res downloads that starve the new track's startup buffer
    /// (5-8 s starts observed on a Pi).
    current_feeder: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// DAEMON-ONLY: the track whose buffer is still filling, so renderer
    /// reports can say BUFFERING (see `BufferingLatch`).
    buffering: Arc<BufferingLatch>,
    /// Daemon status + event bus, so a starting stream can announce itself to
    /// the host BEFORE it takes the audio device (see `start_track_stream`).
    shared: Arc<std::sync::Mutex<crate::state::DaemonShared>>,
    /// Pulsed when buffering starts so the report scheduler tells the
    /// controller within milliseconds instead of at its next 2 s tick.
    report_notify: Arc<tokio::sync::Notify>,
}

/// Which track is filling its buffer, shared between the renderer engine (the
/// writer) and the report scheduler (the reader). A stream is "buffering" from
/// the moment its feeder opens until the player actually starts producing
/// audio — on a deep resume that is several seconds of downloading plus a
/// sample pre-skip, during which the controller deserves a loading state.
#[derive(Default)]
pub struct BufferingLatch(std::sync::Mutex<Option<Buffering>>);

struct Buffering {
    track_id: u64,
    /// Where the stream was opened. The playback clock sits here until audio
    /// flows, so a position past it is the audible edge.
    start_position_secs: u64,
    /// The loading track's duration, so a report sent BEFORE the player has
    /// switched to it can still describe it.
    duration_secs: u64,
    since: std::time::Instant,
}

/// A load that never becomes audible must not report BUFFERING forever.
const BUFFERING_MAX: std::time::Duration = std::time::Duration::from_secs(90);

impl BufferingLatch {
    /// Mark `track_id` as buffering from `start_position_secs` (replacing any
    /// previous track).
    pub fn begin(&self, track_id: u64, start_position_secs: u64, duration_secs: u64) {
        if let Ok(mut guard) = self.0.lock() {
            *guard = Some(Buffering {
                track_id,
                start_position_secs,
                duration_secs,
                since: std::time::Instant::now(),
            });
        }
    }

    /// Clear the latch for `track_id`. Ignores a stale clear for a track that
    /// has already been superseded.
    pub fn finish(&self, track_id: u64) {
        if let Ok(mut guard) = self.0.lock() {
            if guard.as_ref().map(|b| b.track_id) == Some(track_id) {
                *guard = None;
            }
        }
    }

    /// The load in flight, as `(track_id, start_position_secs, duration_secs)`,
    /// or `None` once audio is flowing. Self-clearing on the audible edge.
    ///
    /// Deliberately NOT keyed on the player's track id: the player keeps
    /// reporting the OUTGOING track until the new stream produces audio, so a
    /// check keyed on it saw "not buffering" for the whole load and only turned
    /// true once audio had already started — the controller got no loading
    /// state during the wait and a stray spinner just after playback began.
    ///
    /// Nor on `is_playing`: during a next-track load it is simply still `true`
    /// from the OUTGOING track, so it says nothing about the new stream.
    ///
    /// The audible edge is the player arriving on the loading track AND its
    /// clock moving past where the stream opened — the clock only advances once
    /// audio actually flows.
    ///
    /// `position_ms` is MILLISECONDS on purpose. With whole seconds, a track
    /// loading at 0 needed the clock to reach a full 1 s before it counted as
    /// audible, so the controller kept a spinner up for a second of music it
    /// was already playing.
    pub fn in_flight(&self, player_track_id: u64, position_ms: u64) -> Option<(u64, u64, u64)> {
        let Ok(mut guard) = self.0.lock() else {
            return None;
        };
        let Some(b) = guard.as_ref() else {
            return None;
        };
        let audible = player_track_id == b.track_id
            && position_ms > b.start_position_secs.saturating_mul(1000);
        if audible || b.since.elapsed() > BUFFERING_MAX {
            *guard = None;
            return None;
        }
        Some((b.track_id, b.start_position_secs, b.duration_secs))
    }

}

impl DaemonRendererEngine {
    pub fn new(
        runtime: Arc<AppRuntime<DaemonAdapter>>,
        volume_mode: VolumeMode,
        buffering: Arc<BufferingLatch>,
        report_notify: Arc<tokio::sync::Notify>,
        shared: Arc<std::sync::Mutex<crate::state::DaemonShared>>,
    ) -> Self {
        Self {
            runtime,
            volume_mode,
            current_feeder: std::sync::Mutex::new(None),
            buffering,
            shared,
            report_notify,
        }
    }

    /// Abort the previous track's feeder (no-op when none). The dropped
    /// FailGuard marks the OLD writer errored, which is correct — that buffer
    /// belongs to the abandoned source.
    fn abort_current_feeder(&self) {
        if let Ok(mut guard) = self.current_feeder.lock() {
            if let Some(prev) = guard.take() {
                prev.abort();
            }
        }
    }

    fn core(&self) -> &Arc<QbzCore<DaemonAdapter>> {
        self.runtime.core()
    }

    /// Last-resort load for tracks the raw-URL path cannot fetch (the CDN
    /// header flood defeats every reqwest attempt — see
    /// `remote_stream::is_header_flood_error`): the CMAF path is unaffected by
    /// the h1 header cap. `play_track_resolved` does NOT move the queue cursor
    /// (nothing on the QConnect path does — the shared driver's cursor sync
    /// only fires on a playing->playing track edge), so sync it explicitly or
    /// `qbzd status` / the local now-playing truth keep showing the PREVIOUS
    /// track while the recovered one plays.
    async fn play_via_cmaf(
        &self,
        track_id: u64,
        quality: Quality,
        start_position_secs: u64,
    ) -> Result<(), String> {
        self.core()
            .play_track_resolved(track_id, quality, None, None, start_position_secs)
            .await
            .map_err(|err| format!("CMAF fallback for remote track {track_id}: {err}"))?;
        self.core().sync_current_to_id(track_id).await;
        Ok(())
    }
}

#[async_trait]
impl QconnectRendererEngine for DaemonRendererEngine {
    // ---- transport (sync) ----
    fn resume(&self) -> Result<(), String> {
        self.core().resume().map_err(|err| err.to_string())
    }
    fn pause(&self) -> Result<(), String> {
        self.core().pause().map_err(|err| err.to_string())
    }
    fn stop(&self) -> Result<(), String> {
        self.core().stop().map_err(|err| err.to_string())
    }
    fn seek(&self, position_secs: u64) -> Result<(), String> {
        self.core().seek(position_secs).map_err(|err| err.to_string())
    }
    fn set_volume(&self, fraction: f32) -> Result<(), String> {
        // T10 (OD4, §7.4): volume-mode gate. In `Locked` mode the player stays
        // at 100 % and a controller's remote SetVolume is acknowledged-but-
        // ignored (logged at info), so the DAC keeps receiving full-scale,
        // bit-perfect samples. `Software` (default) applies it via the core.
        if !self.volume_mode.applies_remote_volume() {
            log::info!(
                "[QConnect] volume_mode=locked: ignoring remote SetVolume({:.3}); player stays at 100%",
                fraction
            );
            return Ok(());
        }
        self.core().set_volume(fraction).map_err(|err| err.to_string())
    }
    fn get_playback_state(&self) -> PlaybackState {
        self.core().get_playback_state()
    }
    fn has_loaded_audio(&self) -> bool {
        self.core().player().has_loaded_audio()
    }

    // ---- queue / mode (async) ----
    async fn set_repeat_mode(&self, mode: RepeatMode) {
        self.core().set_repeat_mode(mode).await
    }
    async fn set_shuffle(&self, enabled: bool) {
        self.core().set_shuffle(enabled).await
    }
    async fn set_shuffle_flag(&self, enabled: bool) {
        self.core().set_shuffle_with_order(enabled, None).await
    }
    async fn get_all_queue_tracks(&self) -> (Vec<QueueTrack>, Option<usize>) {
        self.core().get_all_queue_tracks().await
    }
    async fn set_queue(&self, tracks: Vec<QueueTrack>, start_index: Option<usize>) {
        self.core().set_queue(tracks, start_index).await
    }
    async fn set_queue_with_order(
        &self,
        tracks: Vec<QueueTrack>,
        start_index: Option<usize>,
        shuffle_enabled: bool,
        shuffle_order: Option<Vec<usize>>,
    ) {
        self.core()
            .set_queue_with_order(tracks, start_index, shuffle_enabled, shuffle_order)
            .await
    }
    async fn clear_queue(&self, keep_current: bool) {
        self.core().clear_queue(keep_current).await
    }
    async fn play_index(&self, index: usize) -> Option<QueueTrack> {
        self.core().play_index(index).await
    }

    // ---- catalog (async) ----
    async fn get_track(&self, track_id: u64) -> Result<Track, String> {
        self.core()
            .get_track(track_id)
            .await
            .map_err(|err| err.to_string())
    }
    async fn get_tracks_batch(&self, track_ids: &[u64]) -> Result<Vec<Track>, String> {
        self.core()
            .get_tracks_batch(track_ids)
            .await
            .map_err(|err| err.to_string())
    }

    // ---- protected audio seam (the only protected touch) ----
    async fn start_track_stream(
        &self,
        track_id: u64,
        quality: Quality,
        duration_secs: u64,
        start_position_secs: u64,
    ) -> Result<(), String> {
        // Announce the load BEFORE anything touches the audio device.
        //
        // A host integration has to free the card for us: on moOde the event
        // hook stops MPD, and MPD keeps its ALSA device for seconds after that.
        // Until now the first thing the host heard about a cast was
        // PlaybackError — the daemon went straight from `paused` to opening an
        // exclusive device, failed because MPD still held it, and only then
        // said so. Casting to a player that was already playing something
        // simply did not work, which is the "Qobuz will not start while moOde
        // is playing" report.
        //
        // Emitting `loading` here gives the host the whole stream-URL resolve
        // (a network round-trip) plus the buffer fill to get out of the way,
        // and it is also the honest state to show a controller that is
        // otherwise told the renderer is paused while a track loads.
        if let Ok(shared) = self.shared.lock() {
            shared.emit(qbz_models::CoreEvent::PlaybackStateChanged {
                state: qbz_models::PlaybackState::Loading,
            });
        }

        let stream_url = self
            .core()
            .get_stream_url(track_id, quality)
            .await
            .map_err(|err| format!("resolve stream url for remote track {track_id}: {err}"))?;

        // DAEMON-ONLY: stop the previous track's download before starting the
        // next one (see `current_feeder`).
        self.abort_current_feeder();

        // DAEMON-ONLY: tell the controller we are loading. The stream is not
        // audible until the feeder reaches `start_position_secs` and the
        // pre-skip completes; the report scheduler clears this once the player
        // starts producing audio.
        self.buffering
            .begin(track_id, start_position_secs, duration_secs);
        self.report_notify.notify_one();

        let player = self.core().player();
        let stream_result = super::remote_stream::stream_remote_track_into_player(
            &player,
            track_id,
            duration_secs,
            start_position_secs,
            &stream_url.url,
            "QConnect",
        )
        .await;

        let stream_err = match stream_result {
            Ok(feeder) => {
                if let Ok(mut guard) = self.current_feeder.lock() {
                    *guard = Some(feeder);
                }
                return Ok(());
            }
            Err(err) => err,
        };

        // DAEMON-ONLY: past this point the raw stream is gone and the latch,
        // armed above, no longer necessarily describes what is happening. Its
        // only other exits are the audible edge and a 90 s safety expiry, so a
        // stale entry means a minute and a half of spinner for audio that will
        // never arrive. Clear it whenever a fallback ends in Err.
        //
        // The CMAF fallback keeps the latch on success: it streams the same
        // track from the same offset, so the audible edge still fits.
        let clear_on_failure = |result: Result<(), String>| {
            if result.is_err() {
                self.buffering.finish(track_id);
                self.report_notify.notify_one();
            }
            result
        };

        // Akamai small-object header flood: SMALL raw-url objects come back
        // with ~106 headers, over hyper's hard-coded 100-header h1 cap, so
        // EVERY reqwest fetch of this URL fails — the full download would die
        // the same death. Skip it and go straight to the CMAF last resort.
        if super::remote_stream::is_header_flood_error(&stream_err) {
            log::warn!(
                "[QConnect] Raw-URL streaming hit the CDN header flood for track {track_id}: {stream_err}. Skipping full download; last resort: CMAF."
            );
            return clear_on_failure(
                self.play_via_cmaf(track_id, quality, start_position_secs)
                    .await,
            );
        }

        log::warn!(
            "[QConnect] Streaming handoff unavailable for track {}: {}. Falling back to full download.",
            track_id,
            stream_err
        );
        match download_remote_audio(&stream_url.url).await {
            Ok(audio_data) => {
                let played = self
                    .core()
                    .player()
                    .play_data(audio_data, track_id)
                    .map(|_| ())
                    .map_err(|err| format!("play remote track {track_id}: {err}"));
                // Clear either way: on success the complete file is in hand, so
                // nothing is filling — and `play_data` restarts from 0, so the
                // latch's start offset (82 s on a resume) would never be passed
                // and it would sit on BUFFERING until the safety expiry.
                self.buffering.finish(track_id);
                self.report_notify.notify_one();
                played
            }
            Err(download_err) if super::remote_stream::is_header_flood_error(&download_err) => {
                log::warn!(
                    "[QConnect] Full download hit the CDN header flood for track {track_id}: {download_err}. Last resort: CMAF."
                );
                clear_on_failure(
                    self.play_via_cmaf(track_id, quality, start_position_secs)
                        .await,
                )
            }
            Err(download_err) => clear_on_failure(Err(download_err)),
        }
    }

    fn current_output_format(&self) -> Option<(u32, u32)> {
        let player = self.core().player();
        Some((
            player.state.get_sample_rate(),
            player.state.get_bit_depth(),
        ))
    }
}


async fn download_remote_audio(url: &str) -> Result<Vec<u8>, String> {
    let response = reqwest::Client::new()
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await
        .map_err(|err| {
            format!(
                "download remote audio request failed: {}",
                super::remote_stream::describe_reqwest_error(&err)
            )
        })?;

    if !response.status().is_success() {
        return Err(format!(
            "download remote audio failed with status {}",
            response.status()
        ));
    }

    let bytes = response.bytes().await.map_err(|err| {
        format!(
            "read remote audio bytes failed: {}",
            super::remote_stream::describe_reqwest_error(&err)
        )
    })?;
    Ok(bytes.to_vec())
}

// T10 (OD4, §7.4): volume-mode policy tests. These pin the decision the engine's
// `set_volume` gate and the session host's join-time volume report consult — the
// two enforcement points of the software|locked contract.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffering_latch_tracks_one_track_at_a_time() {
        let latch = BufferingLatch::default();
        assert!(
            latch.in_flight(7, 0).is_none(),
            "nothing is buffering initially"
        );

        latch.begin(7, 0, 200);
        assert_eq!(latch.in_flight(7, 0), Some((7, 0, 200)));

        // A track change supersedes: the old track's late clear must not
        // release the new track's buffering state.
        latch.begin(8, 0, 300);
        latch.finish(7);
        assert_eq!(
            latch.in_flight(8, 0),
            Some((8, 0, 300)),
            "stale clear must be ignored"
        );

        latch.finish(8);
        assert!(
            latch.in_flight(8, 0).is_none(),
            "explicit clear releases it"
        );
    }

    #[test]
    fn buffering_latch_reports_the_loading_track_not_the_players() {
        // The whole point of the latch: during a load the PLAYER still names the
        // outgoing track (11) — or nothing at all, right after a hand-off — while
        // the load in flight is track 12 at 139s. The report must describe 12, or
        // the controller names the previous song and draws 0:00 of 0:00.
        let latch = BufferingLatch::default();
        latch.begin(12, 139, 254);
        assert_eq!(
            latch.in_flight(11, 42_000),
            Some((12, 139, 254)),
            "outgoing track playing: still the new track's load"
        );
        assert_eq!(
            latch.in_flight(0, 0),
            Some((12, 139, 254)),
            "player empty after a hand-off: still the new track's load"
        );
    }

    #[test]
    fn buffering_latch_clears_only_once_the_clock_moves() {
        // A resume at 80s: the player parks the clock at 80 while it downloads
        // to that offset and pre-skips, and reports itself "playing" long
        // before the first sample — so only a position PAST 80 means audible.
        let latch = BufferingLatch::default();
        latch.begin(9, 80, 200);
        assert!(
            latch.in_flight(9, 80_000).is_some(),
            "still filling at the start offset"
        );
        assert!(
            latch.in_flight(9, 80_000).is_some(),
            "repeated ticks stay buffering"
        );
        // Milliseconds, so the very first sample past the offset counts —
        // whole seconds kept the spinner up for a second of audible music.
        assert!(
            latch.in_flight(9, 80_050).is_none(),
            "clock moved: audio is flowing"
        );
        assert!(latch.in_flight(9, 80_050).is_none(), "and it stays cleared");
    }

    #[test]
    fn buffering_latch_gives_up_on_a_load_that_never_starts() {
        let latch = BufferingLatch::default();
        latch.begin(9, 0, 200);
        // Backdate past the safety window: a load that never becomes audible
        // must not report BUFFERING forever.
        if let Ok(mut guard) = latch.0.lock() {
            if let Some(b) = guard.as_mut() {
                b.since = std::time::Instant::now() - (BUFFERING_MAX + std::time::Duration::from_secs(1));
            }
        }
        assert!(latch.in_flight(9, 0).is_none());
    }

    #[test]
    fn software_mode_applies_and_reports_real() {
        // remote SetVolume 0.4 -> engine.set_volume(0.4); report reads real volume.
        let mode = VolumeMode::from_kv(Some("software"));
        assert_eq!(mode, VolumeMode::Software);
        assert!(mode.applies_remote_volume());
        assert_eq!(mode.reported_volume_pct(0.4), 40);
        assert_eq!(mode.reported_volume_pct(1.0), 100);
    }

    #[test]
    fn locked_mode_ignores_and_reports_100() {
        // remote SetVolume -> acknowledged-but-ignored; player stays 1.0; 100 reported.
        let mode = VolumeMode::from_kv(Some("locked"));
        assert_eq!(mode, VolumeMode::Locked);
        assert!(!mode.applies_remote_volume());
        // 100 reported regardless of the player's actual level.
        assert_eq!(mode.reported_volume_pct(0.4), 100);
        assert_eq!(mode.reported_volume_pct(1.0), 100);
    }

    #[test]
    fn default_mode_is_software_od4() {
        // Unset / empty / unknown all resolve to the OD4 default (software).
        assert_eq!(VolumeMode::default(), VolumeMode::Software);
        assert_eq!(VolumeMode::from_kv(None), VolumeMode::Software);
        assert_eq!(VolumeMode::from_kv(Some("")), VolumeMode::Software);
        assert_eq!(VolumeMode::from_kv(Some("  ")), VolumeMode::Software);
        assert_eq!(VolumeMode::from_kv(Some("garbage")), VolumeMode::Software);
        // Whitespace around the real value is tolerated.
        assert_eq!(VolumeMode::from_kv(Some(" locked ")), VolumeMode::Locked);
    }
}
