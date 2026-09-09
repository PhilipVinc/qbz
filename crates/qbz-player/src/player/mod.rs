//! Audio player module
//!
//! Handles audio playback with support for:
//! - HTTP streaming from Qobuz
//! - FLAC, MP3 decoding via symphonia
//! - Gapless playback
//! - Volume control
//! - Real-time position tracking via events
//!
//! Uses a dedicated audio thread since rodio's OutputStream is not Send.
//! Supports both rodio (PipeWire/Pulse) and direct ALSA (hw: devices).

mod playback_engine;
mod streaming_source;

pub use streaming_source::{
    max_initial_buffer_bytes, set_max_initial_buffer_bytes, BufferWriter, BufferedMediaSource,
    FetchPlan, InMemorySource, IncrementalStreamingSource, StreamSeekMode, StreamingConfig,
};

use rodio::buffer::SamplesBuffer;
use rodio::cpal::traits::{DeviceTrait, HostTrait};
use rodio::cpal::{
    BufferSize, SampleFormat, StreamConfig, SupportedBufferSize, SupportedStreamConfig,
};
use rodio::{Decoder, DeviceSinkBuilder, MixerDeviceSink, Source};
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::DecoderOptions;
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use symphonia::default::{get_codecs, get_probe};

use playback_engine::PlaybackEngine;
use qbz_audio::{
    calculate_gain_factor, db_to_linear, extract_replaygain, AnalyzerMessage, AnalyzerTap,
    AudioBackendType, AudioDiagnostic, AudioSettings, BackendConfig, BackendManager,
    BitPerfectMode, DiagnosticSource, DynamicAmplify, LoudnessAnalyzer, LoudnessCache,
    TappedSource, VisualizerTap,
};
use qbz_cache::TrackBytes;
use qbz_models::{AssetOrigin, ExternalStreamAsset, Quality, StreamQualityInfo};
use qbz_qobuz::QobuzClient;

/// Commands sent to the audio thread
enum AudioCommand {
    /// Play audio data with track ID, duration, and audio specs
    Play {
        /// Resident bytes, or a file in the L2 cache — see [`TrackAudio`].
        audio: TrackAudio,
        track_id: u64,
        duration_secs: u64,
        sample_rate: u32,
        channels: u16,
    },
    /// Play from streaming source (BufferedMediaSource)
    /// The download task should already be running and pushing to the source
    PlayStreaming {
        source: Arc<BufferedMediaSource>,
        track_id: u64,
        sample_rate: u32,
        channels: u16,
        duration_secs: u64,
        /// Resume offset in seconds (#315). When > 0, the audio thread
        /// waits for enough buffer to cover the offset and pre-skips
        /// decoder output up to that point before engaging audio.
        start_position_secs: u64,
        /// Total content length in bytes. Combined with `duration_secs`
        /// to estimate bytes-per-second when sizing the resume buffer.
        content_length: u64,
        /// Play generation this command belongs to (PR #583 counter,
        /// snapshotted at send time). The audio thread stops waiting for the
        /// initial buffer as soon as a newer play intent bumps the counter,
        /// instead of blocking up to 60s on a superseded download (#591).
        play_gen: u64,
    },
    /// Pause playback
    Pause,
    /// Resume playback
    Resume,
    /// Stop playback
    Stop,
    /// Set volume (0.0 - 1.0)
    SetVolume(f32),
    /// Seek to position in seconds
    Seek(u64),
    /// Reinitialize audio device (releases and re-acquires)
    ReinitDevice { device_name: Option<String> },
    /// Release the output device WITHOUT reopening it: drops the active
    /// stream (freeing an exclusive ALSA `hw:` grab + its D-Bus reservation)
    /// and un-suspends / un-forces anything QBZ parked, so PipeWire can
    /// reclaim a device QBZ was holding. User-triggered from settings.
    ReleaseDevice,
    /// Append next track to current engine for gapless playback (Rodio only)
    PlayNext {
        audio: TrackAudio,
        track_id: u64,
        sample_rate: u32,
        channels: u16,
    },
    /// Play a local DSD file via DoP (DSD over PCM) on ALSA direct (DSD plan
    /// Phase 2). The audio thread opens the demuxer + an S32 stream at the
    /// DoP carrier rate and feeds pre-packed words through the DoP engine.
    PlayDsdDop { path: std::path::PathBuf, track_id: u64 },
    /// Play a local DSD file NATIVELY (ALSA DSD_U32, DSD plan Phase 3) —
    /// requires the kernel to grant the device a DSD format (quirk table).
    PlayDsdNative { path: std::path::PathBuf, track_id: u64 },
    /// Queue the next DSD track on the ACTIVE DoP engine (gapless DSD).
    /// Ignored (with gapless_ready reset) when the engine isn't DoP or the
    /// carrier rate differs — the normal track-end advance then handles it.
    PlayNextDsdDop { path: std::path::PathBuf, track_id: u64 },
}

/// Where a gapless-queued track's audio lives between the prefetch and the
/// transition.
///
/// `File` exists because the prefetch has ALREADY written the track to the L2
/// disk cache by the time it is queued. A Hi-Res track is 120–220 MB, so holding
/// a second copy in RAM for the minutes until the transition — beside the
/// current track's own buffer — is what took a 1 GB Pi to 570 MB RSS and into a
/// reboot. Decoding from the file costs the decoder's own buffers instead, and
/// the page cache can evict what it likes under pressure.
#[derive(Clone)]
/// Where a track's audio comes from: resident bytes, or a file in the L2 cache.
///
/// Used by BOTH the gapless hand-off and an ordinary play. The file case is not
/// an optimisation, it is what makes a 116 MB Hi-Res track playable on a 1 GB
/// host at all: the decoder reads it off the card as it goes instead of the
/// player holding the whole thing in RAM.
pub enum TrackAudio {
    Memory(TrackBytes),
    File(std::path::PathBuf),
}

impl TrackAudio {
    fn describe(&self) -> String {
        match self {
            Self::Memory(data) => format!("{} bytes in memory", data.len()),
            Self::File(path) => format!("file {}", path.display()),
        }
    }
}

/// Pending gapless track data (queued for seamless transition)
struct GaplessPending {
    track_id: u64,
    duration_secs: u64,
    audio: TrackAudio,
    normalization_gain: Option<f32>,
    /// The format this track was accepted for. Carried so a hand-off that has
    /// to be re-queued (a seek replaces the engine) is checked against exactly
    /// what it was checked against the first time.
    sample_rate: u32,
    channels: u16,
}

struct CursorMediaSource {
    inner: Cursor<TrackBytes>,
    len: u64,
}

impl CursorMediaSource {
    fn new(data: TrackBytes) -> Self {
        let len = data.len() as u64;
        Self {
            inner: Cursor::new(data),
            len,
        }
    }
}

impl MediaSource for CursorMediaSource {
    fn is_seekable(&self) -> bool {
        true
    }

    fn byte_len(&self) -> Option<u64> {
        Some(self.len)
    }
}

impl Read for CursorMediaSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Seek for CursorMediaSource {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

/// Audio specifications extracted from decoded audio
#[allow(dead_code)]
struct AudioSpecs {
    samples: SamplesBuffer,
    sample_rate: u32,
    channels: u16,
}

fn cpal_device_name(device: &rodio::cpal::Device) -> Option<String> {
    device
        .description()
        .ok()
        .map(|description| description.name().to_string())
}

fn decode_with_symphonia(data: &TrackBytes) -> Result<AudioSpecs, String> {
    let source = Box::new(CursorMediaSource::new(data.clone())) as Box<dyn MediaSource>;
    let mss = MediaSourceStream::new(source, Default::default());

    let mut hint = Hint::new();
    hint.with_extension("m4a");

    let format_opts = FormatOptions {
        enable_gapless: true,
        ..Default::default()
    };
    let metadata_opts: MetadataOptions = Default::default();
    let mut probed = get_probe()
        .format(&hint, mss, &format_opts, &metadata_opts)
        .map_err(|err| format!("Symphonia probe failed: {}", err))?;

    let track = probed
        .format
        .default_track()
        .ok_or_else(|| "Symphonia: no supported audio tracks".to_string())?;
    let track_id = track.id;
    let codec_params = track.codec_params.clone();

    let mut decoder = get_codecs()
        .make(&codec_params, &DecoderOptions::default())
        .map_err(|err| format!("Symphonia decoder init failed: {}", err))?;

    let mut sample_rate = 0;
    let mut channels = 0u16;
    let mut samples: Vec<f32> = Vec::new();

    loop {
        let packet = match probed.format.next_packet() {
            Ok(packet) => packet,
            Err(SymphoniaError::IoError(_)) => break,
            Err(err) => return Err(format!("Symphonia read error: {}", err)),
        };

        if packet.track_id() != track_id {
            continue;
        }

        match decoder.decode(&packet) {
            Ok(audio_buf) => {
                let spec = *audio_buf.spec();
                if sample_rate == 0 {
                    sample_rate = spec.rate;
                    channels = spec.channels.count() as u16;
                }

                let mut sample_buf = SampleBuffer::<f32>::new(audio_buf.frames() as u64, spec);
                sample_buf.copy_interleaved_ref(audio_buf);
                samples.extend_from_slice(sample_buf.samples());
            }
            Err(SymphoniaError::DecodeError(_)) => continue,
            Err(SymphoniaError::ResetRequired) => {
                decoder.reset();
                continue;
            }
            Err(err) => return Err(format!("Symphonia decode error: {}", err)),
        }
    }

    if samples.is_empty() || sample_rate == 0 || channels == 0 {
        return Err("Symphonia decode produced no audio".to_string());
    }

    Ok(AudioSpecs {
        samples: SamplesBuffer::new(
            std::num::NonZero::new(channels).unwrap(),
            std::num::NonZero::new(sample_rate).unwrap(),
            samples,
        ),
        sample_rate,
        channels,
    })
}

fn is_isomp4(data: &[u8]) -> bool {
    if data.len() < 12 {
        return false;
    }

    &data[4..8] == b"ftyp"
}

/// Extract audio metadata (sample rate, channels) without full decode.
/// This is much faster than decode_with_symphonia as it only reads headers.
/// Audio metadata extracted from file headers
#[allow(dead_code)]
struct AudioMetadata {
    sample_rate: u32,
    channels: u16,
    bit_depth: Option<u32>,
}

#[allow(dead_code)]
fn extract_audio_metadata(data: &TrackBytes) -> Result<(u32, u16), String> {
    let meta = extract_audio_metadata_full(data)?;
    Ok((meta.sample_rate, meta.channels))
}

fn extract_audio_metadata_full(data: &TrackBytes) -> Result<AudioMetadata, String> {
    // For non-isomp4 files (FLAC, etc.), try symphonia directly to get all metadata
    // Symphonia gives us bits_per_sample which rodio doesn't expose

    // Use symphonia probe for codec params (no decode needed)
    let source = Box::new(CursorMediaSource::new(data.clone())) as Box<dyn MediaSource>;
    let mss = MediaSourceStream::new(source, Default::default());

    let mut hint = Hint::new();
    if is_isomp4(data) {
        hint.with_extension("m4a");
    }

    let format_opts = FormatOptions {
        enable_gapless: true,
        ..Default::default()
    };
    let metadata_opts: MetadataOptions = Default::default();
    let probed = get_probe()
        .format(&hint, mss, &format_opts, &metadata_opts)
        .map_err(|err| format!("Symphonia probe failed: {}", err))?;

    let track = probed
        .format
        .default_track()
        .ok_or_else(|| "Symphonia: no supported audio tracks".to_string())?;

    let sample_rate = track
        .codec_params
        .sample_rate
        .ok_or_else(|| "No sample rate in codec params".to_string())?;

    // ALAC and some other formats don't include channel info in initial codec params
    // Default to stereo (2 channels) which is the most common case
    let channels = track
        .codec_params
        .channels
        .map(|c| c.count() as u16)
        .unwrap_or(2);

    // Get bits per sample for bit depth
    let bit_depth = track.codec_params.bits_per_sample;

    Ok(AudioMetadata {
        sample_rate,
        channels,
        bit_depth,
    })
}

/// True when a cached FLAC is a lower quality than `requested`, so the
/// cache entry should be bypassed and the track re-fetched. Ported from
/// the Tauri `cached_quality_below_requested` helper: an unparseable
/// buffer is assumed compatible.
/// How much of a cache file the header-only readers get.
///
/// Rate, bit depth and ReplayGain all live in FLAC metadata blocks at the very
/// start of the stream. 256 KB is far more than they need and far less than a
/// Hi-Res track.
const HEAD_BYTES: usize = 256 * 1024;

/// The first `len` bytes of `path`, for the readers that take bytes.
///
/// `None` if the file cannot be read at all; a SHORT read is returned as-is,
/// since every caller either parses a header out of it or gives up gracefully.
fn read_head(path: &std::path::Path, len: usize) -> Option<TrackBytes> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).ok()?;
    let mut buf = vec![0u8; len];
    let mut filled = 0usize;
    while filled < len {
        match file.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(_) => return None,
        }
    }
    buf.truncate(filled);
    Some(TrackBytes::from(buf))
}

fn cached_quality_below_requested(data: &TrackBytes, requested: Quality) -> bool {
    let meta = match extract_audio_metadata_full(data) {
        Ok(m) => m,
        Err(_) => return false,
    };
    let sample_rate = meta.sample_rate;
    let bit_depth = meta.bit_depth.unwrap_or(16);
    match requested {
        // Hi-Res+: expect 24-bit AND > 96 kHz.
        Quality::UltraHiRes => bit_depth < 24 || sample_rate <= 96000,
        // Hi-Res: expect 24-bit.
        Quality::HiRes => bit_depth < 24,
        // Lossless / Mp3: any FLAC satisfies the request.
        _ => false,
    }
}

/// Decode fully-buffered audio and land on `position` using Symphonia's
/// native seek, so the cost is a seek-table lookup (or, for the Qobuz FLACs
/// that ship STREAMINFO and nothing else, an in-memory bisection) rather
/// than decoding and discarding every sample before it — which for a 124s
/// offset at 96kHz is 23.8M samples and seconds of CPU on a Pi.
///
/// `None` when Symphonia cannot probe the format (rodio-only MP4/AAC) or
/// the seek fails; the caller then falls back to `decode_with_fallback` +
/// `skip_duration`, which always works.
fn seek_in_memory(
    data: &TrackBytes,
    position: Duration,
) -> Option<Box<dyn Source<Item = f32> + Send>> {
    let mut source = match InMemorySource::new(data.clone()) {
        Ok(source) => source,
        Err(err) => {
            log::warn!("Native seek: InMemorySource probe failed ({err})");
            return None;
        }
    };
    if let Err(err) = source.seek_to(position) {
        log::warn!("Native seek: in-memory seek to {position:?} failed ({err})");
        return None;
    }
    Some(Box::new(source))
}

/// Decode straight from a file on disk, for a track that is already in the L2
/// cache (see [`TrackAudio::File`]). The decoder reads through a `BufReader`,
/// so the resident cost is its own buffers rather than the whole track.
///
/// Only the primary decoder is tried: the L2 cache holds decrypted FLAC written
/// by this player, never the isomp4 shapes `decode_with_fallback` exists to
/// rescue. A caller that gets `Err` here still has the in-memory path.
/// Should a track of `total` bytes be kept on DISK rather than in memory?
///
/// Yes exactly when two of them will not fit the L1 budget — the playing track
/// and the prefetched next one is the pair the cache exists to hold, and when it
/// cannot hold both, holding them anyway is what took a 1 GB Pi into swap.
///
/// Asked twice, so it lives in one place: once before a download starts (the
/// CMAF segment table gives the size up front, so a big track is written
/// straight to the card as it decrypts) and once about bytes already in hand.
/// Reset everything that must not survive into a NEW track, whatever path the
/// track arrived on.
///
/// There is more than one way a track starts — `Play` for a buffer, and
/// `PlayStreaming` for everything the Qobuz app selects — and each one used to
/// carry its own copy of this ritual. `PlayStreaming` was missing it entirely,
/// which meant selecting a track from another album left the PREVIOUS album's
/// prepared successor in place: the prefetch for the new track could never arm
/// (`request_armed` still true), and the stale `pending` named a track the
/// freshly-rebuilt engine did not have queued, so the transition at the end of
/// the new track swapped to it and stalled.
///
/// One function, called by both, so the next path that starts a track cannot
/// quietly forget half of it.
/// Every gate on "ask for the next track now".
///
/// Eight conditions that were only ever readable as one inline `&&` chain, which
/// is how a stale `request_armed` — left set by a track change that forgot to
/// clear it — could silently stop all prefetching with nothing in the log to say
/// so. Named and tested here so each gate's purpose survives.
/// Every gate on "ask for the next track now", in order:
///
/// * `enabled` — the user's setting AND the host being able to afford a second
///   track at all.
/// * `transition_consumed_pending` — a transition already fired this tick, so
///   `pos`/`dur` still describe the OUTGOING track and arming would use stale
///   numbers.
/// * `duration_secs` — zero means the duration is not known yet.
/// * `has_pending` — a successor is already prepared.
/// * `request_armed` — a request is already outstanding, including one issued
///   and not yet fulfilled. Cleared by the transition that consumes it, and by
///   starting a new track (see [`begin_new_track`]).
/// * `ready_flag` — the flag the driver watches; set means it has been told.
/// * `next_track_id` — non-zero means a successor is queued on the engine.
/// * `current_stream_complete` — a stream still DOWNLOADING has no tail to hand
///   over; a COMPLETE one does.
///
/// Named and tested because as one inline `&&` chain it was unreadable, and a
/// stale `request_armed` left by a track change that forgot to clear it stopped
/// all prefetching with nothing in the log to say so.
#[allow(clippy::too_many_arguments)]
fn should_arm_prefetch(
    enabled: bool,
    transition_consumed_pending: bool,
    duration_secs: u64,
    has_pending: bool,
    request_armed: bool,
    ready_flag: bool,
    next_track_id: u64,
    current_stream_complete: bool,
) -> bool {
    enabled
        && !transition_consumed_pending
        && duration_secs > 0
        && !has_pending
        && !request_armed
        && !ready_flag
        && next_track_id == 0
        && current_stream_complete
}

fn begin_new_track(
    thread_state: &SharedState,
    gapless_pending: &mut Option<GaplessPending>,
    gapless_request_armed: &mut bool,
    pause_suspend_deadline: &mut Option<Instant>,
) {
    *pause_suspend_deadline = None;
    *gapless_pending = None;
    *gapless_request_armed = false;
    thread_state.set_gapless_ready(false);
    thread_state.set_gapless_next_track_id(0);
}

pub(crate) fn spill_to_disk(total: usize, budget: usize) -> bool {
    total.saturating_mul(2) > budget
}

fn decode_file_with_fallback(
    path: &std::path::Path,
) -> Result<Box<dyn Source<Item = f32> + Send>, String> {
    let file = std::fs::File::open(path)
        .map_err(|e| format!("open cached track {}: {e}", path.display()))?;

    let attempt =
        panic::catch_unwind(AssertUnwindSafe(|| Decoder::new(BufReader::new(file))));

    match attempt {
        Ok(Ok(decoder)) => Ok(Box::new(decoder)),
        Ok(Err(err)) => Err(format!("decode cached track {}: {err}", path.display())),
        Err(_) => Err(format!("decode of {} panicked", path.display())),
    }
}

/// The head of a cached file, for tag reads (ReplayGain lives in the FLAC
/// metadata blocks at the start). Bounded so a 220 MB track costs kilobytes.
fn read_tag_head(path: &std::path::Path) -> Option<TrackBytes> {
    use std::io::Read;
    const TAG_HEAD_BYTES: usize = 1024 * 1024;

    let mut file = std::fs::File::open(path).ok()?;
    let mut head = Vec::with_capacity(TAG_HEAD_BYTES.min(1024 * 64));
    file.take(TAG_HEAD_BYTES as u64).read_to_end(&mut head).ok()?;
    Some(TrackBytes::from(head.as_slice()))
}

fn decode_with_fallback(data: &TrackBytes) -> Result<Box<dyn Source<Item = f32> + Send>, String> {
    if is_isomp4(data) {
        return decode_with_symphonia(data).map(|specs| {
            log::info!("Decoded audio using symphonia fallback (isomp4)");
            Box::new(specs.samples) as Box<dyn Source<Item = f32> + Send>
        });
    }

    let primary = panic::catch_unwind(AssertUnwindSafe(|| {
        Decoder::new(BufReader::new(Cursor::new(data.clone())))
    }));

    match primary {
        Ok(Ok(decoder)) => return Ok(Box::new(decoder)),
        Ok(Err(err)) => {
            log::warn!("Primary decode failed, attempting mp4 fallback: {}", err);
        }
        Err(_) => {
            log::warn!("Primary decode panicked, attempting mp4 fallback");
        }
    }

    // Try mp4 fallback (rodio 0.22 removed Mp4Type hint)
    {
        let attempt = panic::catch_unwind(AssertUnwindSafe(|| {
            Decoder::new_mp4(BufReader::new(Cursor::new(data.clone())))
        }));

        match attempt {
            Ok(Ok(decoder)) => {
                log::info!("Decoded audio using mp4 fallback");
                return Ok(Box::new(decoder));
            }
            Ok(Err(err)) => {
                log::warn!("mp4 fallback failed: {}", err);
            }
            Err(_) => {
                log::warn!("mp4 fallback panicked");
            }
        }
    }

    match decode_with_symphonia(data) {
        Ok(specs) => {
            log::info!("Decoded audio using symphonia fallback");
            Ok(Box::new(specs.samples))
        }
        Err(err) => Err(err),
    }
}

/// Create MixerDeviceSink with custom sample rate configuration
fn create_output_stream_with_config(
    device: rodio::cpal::Device,
    sample_rate: u32,
    channels: u16,
    exclusive_mode: bool,
    state: SharedState,
) -> Result<MixerDeviceSink, String> {
    log::info!(
        "Creating MixerDeviceSink: {}Hz, {} channels, exclusive: {}",
        sample_rate,
        channels,
        exclusive_mode
    );

    // Create StreamConfig with desired sample rate
    // Note: buffer_size here is unused — with_supported_config() resets it.
    // The actual buffer size is set via with_buffer_size() below.
    let config = StreamConfig {
        channels,
        sample_rate,
        buffer_size: BufferSize::Default,
    };

    // Check if device supports this configuration
    let supported_configs = device
        .supported_output_configs()
        .map_err(|e| format!("Failed to get supported configs: {}", e))?;

    let mut found_matching = false;
    for range in supported_configs {
        if range.channels() == channels
            && sample_rate >= range.min_sample_rate()
            && sample_rate <= range.max_sample_rate()
        {
            found_matching = true;
            log::info!(
                "Device supports {}Hz (range: {}-{}Hz)",
                sample_rate,
                range.min_sample_rate(),
                range.max_sample_rate()
            );
            break;
        }
    }

    if !found_matching {
        log::warn!(
            "Device may not support {}Hz, attempting anyway",
            sample_rate
        );
    }

    // Create SupportedStreamConfig
    let supported_config = SupportedStreamConfig::new(
        config.channels,
        config.sample_rate,
        SupportedBufferSize::Range { min: 64, max: 8192 },
        SampleFormat::F32,
    );

    // Compute buffer size — must be applied AFTER with_supported_config()
    // because that method resets buffer_size to Default via ..Default::default().
    // MixerDeviceSink has zero internal buffering, so CPAL's buffer is the
    // ONLY buffer between the mixer and audio hardware.
    let cpal_buffer_size = if exclusive_mode {
        BufferSize::Fixed(512) // Low latency for exclusive mode
    } else {
        // ~100ms buffer, matching old vendored cpal period size.
        // Prevents underruns at high sample rates (192kHz = 19200 frames).
        BufferSize::Fixed(sample_rate / 10)
    };
    log::info!("Buffer size: {:?}", cpal_buffer_size);

    // Create MixerDeviceSink with custom config
    match DeviceSinkBuilder::from_device(device) {
        Ok(builder) => {
            // rodio's default error callback only eprintln!s a live stream
            // error (e.g. an ALSA buffer underrun mid-playback), so playback
            // dies without the driver ever seeing a message to latch. Record
            // it on the shared state instead so it drains to the event bus as
            // a PlaybackError. Rate-limited: ALSA can repeat EPIPE every
            // period on a wedged device, and each recorded message is one
            // bus event (and one forked hook script) after the drain.
            let mut last_reported: Option<std::time::Instant> = None;
            match builder
                .with_supported_config(&supported_config)
                .with_buffer_size(cpal_buffer_size)
                .with_error_callback(move |err| {
                    log::error!("Audio stream error: {err}");
                    let now = std::time::Instant::now();
                    if last_reported.map_or(true, |t| now.duration_since(t).as_secs() >= 5) {
                        last_reported = Some(now);
                        state.record_stream_error(format!("Audio stream error: {err}"));
                    }
                })
                .open_stream()
            {
                Ok(mixer_sink) => {
                    log::info!("MixerDeviceSink created successfully at {}Hz", sample_rate);
                    Ok(mixer_sink)
                }
                Err(e) => {
                    log::error!("Failed to open stream at {}Hz: {}", sample_rate, e);
                    Err(format!("Failed to create output stream: {}", e))
                }
            }
        }
        Err(e) => {
            log::error!("Failed to create device sink builder: {}", e);
            Err(format!("Failed to create output stream: {}", e))
        }
    }
}

/// Output stream type - either rodio or ALSA Direct.
///
/// On macOS, the Rodio variant carries an optional `exclusive_guard`
/// whose `Drop` releases CoreAudio Hog Mode and is otherwise inert —
/// so exclusive and shared modes share a single code path.
enum StreamType {
    Rodio {
        sink: MixerDeviceSink,
        /// Holds CoreAudio Hog Mode for the lifetime of the stream.
        /// Load-bearing via `Drop`; reads happen through pattern matches
        /// (e.g., `set_coreaudio_hardware_volume`).
        #[cfg(target_os = "macos")]
        exclusive_guard: Option<qbz_audio::CoreAudioExclusiveGuard>,
    },
    #[cfg(target_os = "linux")]
    AlsaDirect(Arc<qbz_audio::AlsaDirectStream>),
    /// Native JACK output (#263 Tier 3). QBZ as a JACK client with stable ports;
    /// NOT bit-perfect (resampled to the graph rate).
    #[cfg(target_os = "linux")]
    Jack(Arc<qbz_audio::JackStream>),
}

impl StreamType {
    /// Construct a shared-mode Rodio stream (no exclusive guard).
    fn rodio(sink: MixerDeviceSink) -> Self {
        StreamType::Rodio {
            sink,
            #[cfg(target_os = "macos")]
            exclusive_guard: None,
        }
    }

    /// Apply the volume to CoreAudio hardware if the device supports it.
    ///
    /// Returns `true` only when the hardware accepted the change so the
    /// caller can pin the software stream to unity gain. Returns `false`
    /// for shared mode and for knob-only DACs (no settable volume
    /// property), letting the caller fall back to software volume.
    #[cfg(target_os = "macos")]
    fn set_coreaudio_hardware_volume(&self, volume: f32) -> bool {
        match self {
            StreamType::Rodio {
                exclusive_guard: Some(guard),
                ..
            } => match guard.set_hardware_volume(volume) {
                Ok(()) => true,
                Err(e) => {
                    log::warn!(
                        "[CoreAudio] Hardware volume failed; falling back to software: {}",
                        e
                    );
                    false
                }
            },
            _ => false,
        }
    }

    /// Actual output stream rate. On macOS shared mode this is the rate that
    /// must match CoreAudio's current nominal device rate; decoded track rates
    /// may differ and are resampled by Rodio.
    #[cfg(target_os = "macos")]
    fn output_sample_rate(&self) -> u32 {
        match self {
            StreamType::Rodio { sink, .. } => sink.config().sample_rate().get(),
        }
    }
}

fn apply_engine_volume(
    #[cfg_attr(not(target_os = "macos"), allow(unused_variables))] stream_opt: &Option<StreamType>,
    engine: &PlaybackEngine,
    volume: f32,
) {
    #[cfg(target_os = "macos")]
    if stream_opt
        .as_ref()
        .map(|stream| stream.set_coreaudio_hardware_volume(volume))
        .unwrap_or(false)
    {
        engine.set_volume(1.0);
        return;
    }

    engine.set_volume(volume);
}

#[cfg(target_os = "macos")]
fn uses_coreaudio_system_default(settings: &AudioSettings) -> bool {
    settings
        .backend_type
        .unwrap_or(AudioBackendType::SystemDefault)
        == AudioBackendType::SystemDefault
}

/// `evaluate_stream_recreate` runs every track change on the audio thread,
/// and `coreaudio_nominal_rate` issues two CoreAudio HAL queries per call
/// (`resolve_output_device_id` + `get_nominal_sample_rate`). A short-lived
/// cache absorbs the bulk of those repeats — long enough to skip duplicate
/// queries within an album, short enough to notice when the user changes the
/// system audio rate.
#[cfg(target_os = "macos")]
const COREAUDIO_RATE_CACHE_TTL: std::time::Duration = std::time::Duration::from_millis(750);

#[cfg(target_os = "macos")]
struct CachedNominalRate {
    cached_at: std::time::Instant,
    device_name: Option<String>,
    rate: Option<u32>,
}

#[cfg(target_os = "macos")]
std::thread_local! {
    static COREAUDIO_NOMINAL_RATE_CACHE: std::cell::RefCell<Option<CachedNominalRate>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(target_os = "macos")]
fn query_macos_nominal_rate(device_name: Option<&str>) -> Option<u32> {
    let device_id = qbz_audio::coreaudio_direct::resolve_output_device_id(device_name)
        .inspect_err(|e| {
            log::warn!(
                "[CoreAudio] Failed to resolve output device for rate check: {}",
                e
            )
        })
        .ok()?;

    qbz_audio::coreaudio_direct::get_nominal_sample_rate(device_id)
        .inspect_err(|e| log::warn!("[CoreAudio] Failed to query nominal rate: {}", e))
        .ok()
}

#[cfg(target_os = "macos")]
fn coreaudio_nominal_rate(settings: &AudioSettings) -> Option<u32> {
    if !uses_coreaudio_system_default(settings) {
        return None;
    }

    let device_name = settings.output_device.clone();
    let now = std::time::Instant::now();

    COREAUDIO_NOMINAL_RATE_CACHE.with(|cell| {
        let mut cell = cell.borrow_mut();
        if let Some(cached) = cell.as_ref() {
            if cached.device_name == device_name
                && now.duration_since(cached.cached_at) < COREAUDIO_RATE_CACHE_TTL
            {
                return cached.rate;
            }
        }
        let rate = query_macos_nominal_rate(device_name.as_deref());
        *cell = Some(CachedNominalRate {
            cached_at: now,
            device_name,
            rate,
        });
        rate
    })
}

#[cfg(target_os = "macos")]
fn coreaudio_shared_rate_mismatch(
    settings: &AudioSettings,
    stream_opt: &Option<StreamType>,
) -> Option<(u32, u32)> {
    if settings.exclusive_mode || !uses_coreaudio_system_default(settings) {
        return None;
    }

    let stream_rate = stream_opt.as_ref().map(StreamType::output_sample_rate)?;
    let nominal_rate = coreaudio_nominal_rate(settings)?;

    (stream_rate != nominal_rate).then_some((stream_rate, nominal_rate))
}

/// Inputs needed by both `Play` and `Stream` handlers to decide whether the
/// current output stream must be torn down and recreated.
struct StreamRecreateDecision {
    needs_new_stream: bool,
    format_changed: bool,
    dac_passthrough: bool,
    using_alsa_direct: bool,
    using_coreaudio_exclusive: bool,
    coreaudio_shared_rate_mismatch: Option<(u32, u32)>,
}

/// Read settings once and evaluate every condition that forces a stream
/// rebuild: any decoded-format change (sample rate / channels — the output
/// stream must follow the track's native rate on every backend, #449) and
/// CoreAudio shared-mode rate drift (CPAL caches the device rate at open
/// time, so when the OS nominal rate moves we must rebuild or playback runs
/// at the wrong speed).
///
/// `current_track_sample_rate` / `current_track_channels` describe the last
/// *decoded source* format and are compared to the incoming `sample_rate` /
/// `channels` — never to the output stream's hardware rate, which on macOS
/// shared mode may differ and is tracked separately via Rodio's sink config.
fn evaluate_stream_recreate(
    thread_settings: &Arc<Mutex<AudioSettings>>,
    stream_opt: &Option<StreamType>,
    current_track_sample_rate: Option<u32>,
    current_track_channels: Option<u16>,
    sample_rate: u32,
    channels: u16,
    context: &str,
) -> StreamRecreateDecision {
    let format_changed =
        current_track_sample_rate != Some(sample_rate) || current_track_channels != Some(channels);

    let settings_guard = thread_settings.lock().ok();

    let dac_passthrough = settings_guard
        .as_ref()
        .map(|s| cfg!(target_os = "linux") && s.dac_passthrough)
        .unwrap_or(false);

    let using_alsa_direct = settings_guard
        .as_ref()
        .and_then(|s| s.backend_type)
        .map(|b| b == AudioBackendType::Alsa)
        .unwrap_or(false);

    let using_coreaudio_exclusive = settings_guard
        .as_ref()
        .map(|s| {
            cfg!(target_os = "macos")
                && s.backend_type.unwrap_or(AudioBackendType::SystemDefault)
                    == AudioBackendType::SystemDefault
                && s.exclusive_mode
        })
        .unwrap_or(false);

    #[cfg(target_os = "macos")]
    let coreaudio_shared_rate_mismatch = settings_guard
        .as_ref()
        .and_then(|s| coreaudio_shared_rate_mismatch(s, stream_opt))
        .inspect(|(stream_rate, nominal_rate)| {
            log::warn!(
                "[CoreAudio] {} shared-mode output rate changed: stream {}Hz, device nominal {}Hz. Recreating stream to avoid wrong-speed playback.",
                context,
                stream_rate,
                nominal_rate
            );
        });
    #[cfg(not(target_os = "macos"))]
    let coreaudio_shared_rate_mismatch: Option<(u32, u32)> = {
        let _ = (stream_opt, context);
        None
    };

    drop(settings_guard);

    let needs_new_stream = compute_needs_new_stream(
        stream_opt.is_some(),
        format_changed,
        dac_passthrough,
        using_alsa_direct,
        using_coreaudio_exclusive,
        coreaudio_shared_rate_mismatch.is_some(),
    );

    StreamRecreateDecision {
        needs_new_stream,
        format_changed,
        dac_passthrough,
        using_alsa_direct,
        using_coreaudio_exclusive,
        coreaudio_shared_rate_mismatch,
    }
}

/// Pure decision rule for whether the output stream must be rebuilt.
///
/// Split out so the truth table can be unit-tested without faking a real
/// `MixerDeviceSink` or `AudioSettings` mutex.
fn compute_needs_new_stream(
    has_stream: bool,
    format_changed: bool,
    _dac_passthrough: bool,
    _using_alsa_direct: bool,
    _using_coreaudio_exclusive: bool,
    coreaudio_shared_rate_mismatch: bool,
) -> bool {
    // A decoded-format change (sample rate or channel count) requires a fresh
    // output stream on EVERY backend so the device follows the track's native
    // rate (#449). The bit-perfect flags used to gate this, which was correct
    // only while Stop dropped the stream; once that drop was deferred to avoid
    // a track-change click (e93fcaec), the default/PipeWire path stopped
    // switching rates and stayed locked to the first track. Same-rate tracks
    // keep reusing the stream (format_changed == false), preserving the click
    // fix.
    !has_stream || format_changed || coreaudio_shared_rate_mismatch
}

/// Try to create output stream using the backend system (if configured)
/// Returns None if backend system is not configured (backend_type = None)
///
/// For ALSA backend with hw: devices, may return AlsaDirect instead of Rodio stream.
fn try_init_stream_with_backend(
    audio_settings: &AudioSettings,
    sample_rate: u32,
    channels: u16,
    state: &SharedState,
) -> Option<Result<StreamType, String>> {
    // A None backend_type means "Auto" / unset. Resolve it to SystemDefault on
    // every platform instead of returning None — returning None made the caller
    // fall through to the legacy CPAL path, which forced the track rate onto the
    // shared default device: that froze the seekbar with no audio AND left a
    // process-wide stuck audio handle that survived Reset (#470). "Auto" is
    // resolved to a concrete backend in the UI; this is the backend-side safety
    // net for any remaining None (legacy installs, headless callers).
    let backend_type = audio_settings
        .backend_type
        .unwrap_or(qbz_audio::AudioBackendType::SystemDefault);

    log::info!(
        "Using backend system: {:?} (device: {:?}, plugin: {:?})",
        backend_type,
        audio_settings.output_device,
        audio_settings.alsa_plugin
    );

    // Create backend
    let backend = match BackendManager::create_backend(backend_type) {
        Ok(b) => b,
        Err(e) => {
            log::error!("Failed to create backend {:?}: {}", backend_type, e);
            return Some(Err(e));
        }
    };

    // Check availability
    if !backend.is_available() {
        let msg = format!("Backend {:?} is not available on this system", backend_type);
        log::error!("{}", msg);
        return Some(Err(msg));
    }

    // Build backend config
    let config = BackendConfig {
        backend_type,
        device_id: audio_settings.output_device.clone(),
        sample_rate,
        channels,
        exclusive_mode: audio_settings.exclusive_mode,
        alsa_plugin: audio_settings.alsa_plugin,
        pw_force_bitperfect: audio_settings.pw_force_bitperfect,
        skip_sink_switch: audio_settings.skip_sink_switch,
        alsa_mixer_device: Some(audio_settings.alsa_mixer_device.clone())
            .filter(|d| !d.trim().is_empty()),
    };

    // For ALSA backend with a directly-openable device, try direct ALSA first
    // (Linux only)
    #[cfg(target_os = "linux")]
    if backend_type == AudioBackendType::Alsa {
        // hw:/plughw:/front:, or a PCM somebody named in an ALSA config
        if let Some(ref device_id) = config.device_id {
            if qbz_audio::AlsaDirectStream::supports_direct_open(device_id) {
                log::info!(
                    "Device '{device_id}' can be opened directly, using ALSA Direct for bit-perfect playback"
                );

                // Downcast backend to AlsaBackend to access try_create_direct_stream
                if let Some(alsa_backend) = backend
                    .as_any()
                    .downcast_ref::<qbz_audio::alsa_backend::AlsaBackend>()
                {
                    if let Some(result) = alsa_backend.try_create_direct_stream(&config) {
                        return Some(result.map(|(stream, mode)| {
                            log::info!("ALSA Direct stream created with mode: {:?}", mode);
                            state.set_bit_perfect_mode(Some(mode));
                            state.set_output_sample_rate(stream.sample_rate());
                            StreamType::AlsaDirect(Arc::new(stream))
                        }));
                    }
                }
            }
        }
    }

    // JACK (#263 Tier 3): create the JACK client/stream directly (not via the
    // MixerDeviceSink trait). Opt-in routing-freedom mode, NOT bit-perfect.
    #[cfg(target_os = "linux")]
    if backend_type == AudioBackendType::Jack {
        match qbz_audio::JackStream::new(config.channels) {
            Ok(stream) => {
                state.set_bit_perfect_mode(Some(qbz_audio::BitPerfectMode::Disabled));
                state.set_output_sample_rate(stream.sample_rate());
                return Some(Ok(StreamType::Jack(Arc::new(stream))));
            }
            Err(e) => return Some(Err(format!("JACK backend unavailable: {e}"))),
        }
    }

    // Fallback to regular rodio stream (PipeWire, Pulse, ALSA via CPAL)
    match backend.create_output_stream_with_exclusive_guard(&config) {
        Ok((mixer_sink, _exclusive_guard)) => {
            let output_sample_rate = mixer_sink.config().sample_rate().get();
            log::info!(
                "Stream created via {:?} backend (requested {}Hz, output {}Hz)",
                backend_type,
                sample_rate,
                output_sample_rate
            );
            state.set_bit_perfect_mode(Some(BitPerfectMode::Disabled));
            state.set_output_sample_rate(output_sample_rate);
            #[cfg(target_os = "macos")]
            let stream = if backend_type == AudioBackendType::SystemDefault {
                StreamType::Rodio {
                    sink: mixer_sink,
                    exclusive_guard: _exclusive_guard,
                }
            } else {
                StreamType::rodio(mixer_sink)
            };
            #[cfg(not(target_os = "macos"))]
            let stream = StreamType::rodio(mixer_sink);
            Some(Ok(stream))
        }
        Err(e) => {
            log::error!("Backend stream creation failed: {}", e);
            Some(Err(e))
        }
    }
}

/// Event payload for playback state updates
#[derive(Debug, Clone, serde::Serialize)]
pub struct PlaybackEvent {
    pub is_playing: bool,
    pub position: u64,
    pub duration: u64,
    pub track_id: u64,
    pub volume: f32,
    /// Actual sample rate of the current stream (Hz) — the rate the file
    /// was encoded at and the decoder produces.
    pub sample_rate: Option<u32>,
    /// Actual bit depth of the current stream
    pub bit_depth: Option<u32>,
    /// Rate the output device actually runs at (Hz). Differs from
    /// `sample_rate` when something in the chain resamples — a shared
    /// PipeWire/Pulse/CPAL path, or an ALSA config that pins a rate. None
    /// before any stream has been created.
    #[serde(default)]
    pub output_sample_rate: Option<u32>,
    /// Queue shuffle state
    pub shuffle: Option<bool>,
    /// Queue repeat mode ("off", "all", "one")
    pub repeat: Option<String>,
    /// Normalization gain factor being applied (None = normalization not active)
    pub normalization_gain: Option<f32>,
    /// True when backend wants the next track pre-queued for gapless playback
    #[serde(default)]
    pub gapless_ready: bool,
    /// Track ID of the gapless-queued next track (0 = none queued)
    #[serde(default)]
    pub gapless_next_track_id: u64,
    /// Bit-perfect mode of the current stream. None when no stream is active.
    /// Lets the UI show whether playback is direct-hardware bit-perfect, going
    /// through plughw software resample, or running on a shared system path
    /// (pipewire/pulse/cpal) where bit-perfect is not guaranteed.
    #[serde(default)]
    pub bit_perfect_mode: Option<BitPerfectMode>,
    /// Streaming buffer progress (0.0..1.0). `None` when not streaming or
    /// the track is fully buffered — drives the seek-bar cache overlay.
    #[serde(default)]
    pub buffer_progress: Option<f32>,
}

/// Shared state between main thread and audio thread
#[derive(Clone)]
pub struct SharedState {
    /// Is currently playing
    is_playing: Arc<AtomicBool>,
    /// Current position in seconds
    position: Arc<AtomicU64>,
    /// Total duration in seconds
    duration: Arc<AtomicU64>,
    /// Current track ID
    current_track_id: Arc<AtomicU64>,
    /// DSD-direct mode: 0 = none, 1 = DoP, 2 = native DSD_U32_BE,
    /// 3 = native DSD_U32_LE. Non-zero means volume is fixed and seek is
    /// unsupported; the gapless arm uses it to build the matching packing.
    dsd_direct: Arc<std::sync::atomic::AtomicU8>,
    /// True when audio data/source is available for playback or resume
    has_loaded_audio: Arc<AtomicBool>,
    /// Volume (0.0 - 1.0, f32 stored as u32 bits — same idiom as
    /// `normalization_gain`; integer-percent storage quantized the volume
    /// to 1% on every re-apply)
    volume: Arc<AtomicU32>,
    /// Playback start time (Unix timestamp millis when started/resumed)
    playback_start_millis: Arc<AtomicU64>,
    /// Position when playback was started/resumed (in seconds)
    position_at_start: Arc<AtomicU64>,
    /// Current output device name
    current_device: Arc<std::sync::RwLock<Option<String>>>,
    /// Stream error flag (set when ALSA/audio errors are detected)
    stream_error: Arc<AtomicBool>,
    /// Optional user-readable explanation paired with `stream_error`.
    /// Drained by the Tauri polling loop to emit a frontend toast and then
    /// cleared, so the UI fires the notification exactly once per error.
    stream_error_message: Arc<std::sync::RwLock<Option<String>>>,
    /// Actual sample rate of the current stream (Hz) — what the decoder
    /// produces, i.e. the rate the file was encoded at.
    sample_rate: Arc<AtomicU32>,
    /// Actual bit depth of the current stream
    bit_depth: Arc<AtomicU32>,
    /// Rate the output device actually runs at (Hz), which is NOT always the
    /// stream's: a shared path (PipeWire/Pulse/CPAL, or moOde's ALSA config)
    /// can hand back 44100 for a 96000 stream and resample in between. 0
    /// until a stream has been created. Reflects the most recent stream —
    /// the value survives the pause-suspend teardown, which is what we want,
    /// since Resume re-opens the device at the same rate.
    output_sample_rate: Arc<AtomicU32>,
    /// Current normalization gain factor (f32 stored as u32 bits, 0 = not applied)
    normalization_gain: Arc<AtomicU32>,
    /// True when the audio thread wants the next track pre-queued for gapless
    gapless_ready: Arc<AtomicBool>,
    /// Track ID of the gapless-queued next track (0 = none)
    gapless_next_track_id: Arc<AtomicU64>,
    /// Streaming buffer progress (0.0-1.0 stored as f32 bits, 0 = not streaming)
    buffer_progress: Arc<AtomicU32>,
    /// Current bit-perfect mode encoded as u8 (see `bit_perfect_mode_from_u8`).
    /// 0 = Unknown (no stream active yet), 1 = Disabled (CPAL/Rodio / shared
    /// system path), 2 = DirectHardware (ALSA hw:), 3 = PluginFallback (plughw:).
    bit_perfect_mode: Arc<AtomicU8>,
    /// Monotonic play generation (PR #583). Bumped by `Player::begin_play` on
    /// every new play intent. Lives in the shared state so the audio thread
    /// can detect that a queued `PlayStreaming` was superseded by a newer play
    /// and stop waiting on its initial buffer instead of blocking ~60s (#591).
    play_generation: Arc<AtomicU64>,
}

impl Default for SharedState {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            is_playing: Arc::new(AtomicBool::new(false)),
            position: Arc::new(AtomicU64::new(0)),
            duration: Arc::new(AtomicU64::new(0)),
            current_track_id: Arc::new(AtomicU64::new(0)),
            dsd_direct: Arc::new(std::sync::atomic::AtomicU8::new(0)),
            has_loaded_audio: Arc::new(AtomicBool::new(false)),
            volume: Arc::new(AtomicU32::new(0.75f32.to_bits())),
            playback_start_millis: Arc::new(AtomicU64::new(0)),
            position_at_start: Arc::new(AtomicU64::new(0)),
            current_device: Arc::new(std::sync::RwLock::new(None)),
            stream_error: Arc::new(AtomicBool::new(false)),
            stream_error_message: Arc::new(std::sync::RwLock::new(None)),
            sample_rate: Arc::new(AtomicU32::new(0)),
            bit_depth: Arc::new(AtomicU32::new(0)),
            output_sample_rate: Arc::new(AtomicU32::new(0)),
            normalization_gain: Arc::new(AtomicU32::new(0)),
            gapless_ready: Arc::new(AtomicBool::new(false)),
            gapless_next_track_id: Arc::new(AtomicU64::new(0)),
            buffer_progress: Arc::new(AtomicU32::new(0)),
            bit_perfect_mode: Arc::new(AtomicU8::new(0)),
            play_generation: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Clearing (`error = false`) also drops any pending
    /// `stream_error_message`. This is intentional: if init recovers before
    /// the Tauri polling loop drains the message, we'd rather swallow the
    /// toast than surface a notification for a transient failure the user
    /// never perceived. The trade-off is that a fast record→clear→drain
    /// sequence loses the message — accepted because a recovered error is
    /// not a user-actionable event.
    /// 0 = none, 1 = DoP, 2 = native BE, 3 = native LE.
    pub fn set_dsd_mode(&self, mode: u8) {
        self.dsd_direct.store(mode, Ordering::SeqCst);
    }

    pub fn dsd_mode(&self) -> u8 {
        self.dsd_direct.load(Ordering::SeqCst)
    }

    pub fn is_dsd_direct(&self) -> bool {
        self.dsd_direct.load(Ordering::SeqCst) != 0
    }

    pub fn set_stream_error(&self, error: bool) {
        self.stream_error.store(error, Ordering::SeqCst);
        if !error {
            if let Ok(mut m) = self.stream_error_message.write() {
                *m = None;
            }
        }
    }

    pub fn has_stream_error(&self) -> bool {
        self.stream_error.load(Ordering::SeqCst)
    }

    /// Record a user-readable error explanation alongside `stream_error=true`.
    /// The message is drained once via `take_stream_error_message` so the UI
    /// fires the toast exactly once per error.
    pub fn record_stream_error(&self, message: impl Into<String>) {
        self.stream_error.store(true, Ordering::SeqCst);
        if let Ok(mut m) = self.stream_error_message.write() {
            *m = Some(message.into());
        }
    }

    /// Atomically take the pending stream-error message (if any). Returns
    /// `None` when no message is pending or has already been read.
    pub fn take_stream_error_message(&self) -> Option<String> {
        self.stream_error_message
            .write()
            .ok()
            .and_then(|mut m| m.take())
    }

    /// Start a new play intent; returns the generation token for this intent
    /// (see `Player::begin_play`).
    pub(crate) fn begin_play(&self) -> u64 {
        self.play_generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// The most recent play generation (the token a play command sent right
    /// now would carry).
    pub(crate) fn current_play_generation(&self) -> u64 {
        self.play_generation.load(Ordering::SeqCst)
    }

    /// True while `gen` is still the newest play intent. The audio thread
    /// uses this to abandon buffer waits for superseded plays (#591).
    pub(crate) fn is_current_play(&self, gen: u64) -> bool {
        self.current_play_generation() == gen
    }

    pub fn set_stream_quality(&self, sample_rate: u32, bit_depth: u32) {
        self.sample_rate.store(sample_rate, Ordering::SeqCst);
        self.bit_depth.store(bit_depth, Ordering::SeqCst);
    }

    /// Record the rate the freshly-created output device actually runs at.
    /// Called from the stream-creation paths, which are the only places that
    /// learn it — the requested rate is only a request.
    pub fn set_output_sample_rate(&self, sample_rate: u32) {
        self.output_sample_rate.store(sample_rate, Ordering::SeqCst);
    }

    /// Rate the output device runs at, or None before any stream exists.
    /// Compare against `get_sample_rate()`: a mismatch means something in
    /// the chain is resampling.
    pub fn get_output_sample_rate(&self) -> Option<u32> {
        match self.output_sample_rate.load(Ordering::SeqCst) {
            0 => None,
            rate => Some(rate),
        }
    }

    /// Set the current bit-perfect mode for the active stream.
    /// Pass None when no stream is active (e.g., after stop).
    pub fn set_bit_perfect_mode(&self, mode: Option<BitPerfectMode>) {
        let code = match mode {
            None => 0,
            Some(BitPerfectMode::Disabled) => 1,
            Some(BitPerfectMode::DirectHardware) => 2,
            Some(BitPerfectMode::PluginFallback) => 3,
            Some(BitPerfectMode::DirectNamedDevice) => 4,
        };
        self.bit_perfect_mode.store(code, Ordering::SeqCst);
    }

    /// Get the current bit-perfect mode for the active stream.
    /// Returns None when no stream has been initialized yet.
    pub fn get_bit_perfect_mode(&self) -> Option<BitPerfectMode> {
        match self.bit_perfect_mode.load(Ordering::SeqCst) {
            0 => None,
            1 => Some(BitPerfectMode::Disabled),
            2 => Some(BitPerfectMode::DirectHardware),
            3 => Some(BitPerfectMode::PluginFallback),
            4 => Some(BitPerfectMode::DirectNamedDevice),
            _ => None,
        }
    }

    pub fn get_sample_rate(&self) -> u32 {
        self.sample_rate.load(Ordering::SeqCst)
    }

    pub fn get_bit_depth(&self) -> u32 {
        self.bit_depth.load(Ordering::SeqCst)
    }

    /// Set the current normalization gain factor.
    /// Stores f32 as u32 bits. Pass None (or 0.0) to indicate no normalization.
    pub fn set_normalization_gain(&self, gain: Option<f32>) {
        let bits = gain.unwrap_or(0.0).to_bits();
        self.normalization_gain.store(bits, Ordering::SeqCst);
    }

    /// Get the current normalization gain factor.
    /// Returns None if normalization is not active (gain is 0.0).
    pub fn get_normalization_gain(&self) -> Option<f32> {
        let bits = self.normalization_gain.load(Ordering::SeqCst);
        let gain = f32::from_bits(bits);
        if gain == 0.0 {
            None
        } else {
            Some(gain)
        }
    }

    /// Set streaming buffer progress (0.0 to 1.0). Pass 0.0 when not streaming.
    pub fn set_buffer_progress(&self, progress: f32) {
        self.buffer_progress
            .store(progress.to_bits(), Ordering::SeqCst);
    }

    /// Get streaming buffer progress (0.0 to 1.0). Returns None if not streaming.
    pub fn get_buffer_progress(&self) -> Option<f32> {
        let bits = self.buffer_progress.load(Ordering::SeqCst);
        let progress = f32::from_bits(bits);
        if progress <= 0.0 || progress >= 1.0 {
            None
        } else {
            Some(progress)
        }
    }

    pub fn set_current_device(&self, device: Option<String>) {
        if let Ok(mut d) = self.current_device.write() {
            *d = device;
        }
    }

    pub fn current_device(&self) -> Option<String> {
        self.current_device.read().ok().and_then(|d| d.clone())
    }

    pub fn set_gapless_ready(&self, ready: bool) {
        self.gapless_ready.store(ready, Ordering::SeqCst);
    }

    pub fn is_gapless_ready(&self) -> bool {
        self.gapless_ready.load(Ordering::SeqCst)
    }

    pub fn set_gapless_next_track_id(&self, track_id: u64) {
        self.gapless_next_track_id.store(track_id, Ordering::SeqCst);
    }

    pub fn get_gapless_next_track_id(&self) -> u64 {
        self.gapless_next_track_id.load(Ordering::SeqCst)
    }

    /// Get current position based on elapsed time since playback started
    pub fn current_position(&self) -> u64 {
        if !self.is_playing.load(Ordering::SeqCst) {
            return self.position.load(Ordering::SeqCst);
        }

        let start_millis = self.playback_start_millis.load(Ordering::SeqCst);
        if start_millis == 0 {
            return self.position.load(Ordering::SeqCst);
        }

        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let elapsed_secs = (now_millis.saturating_sub(start_millis)) / 1000;
        let position_at_start = self.position_at_start.load(Ordering::SeqCst);
        let duration = self.duration.load(Ordering::SeqCst);

        // Clamp to duration
        (position_at_start + elapsed_secs).min(duration)
    }

    /// Millisecond-precision companion to [`Self::current_position`] — the
    /// exact same derivation WITHOUT the whole-second truncation. READ-ONLY
    /// state derivation from the existing anchors (`playback_start_millis`
    /// is already epoch-ms; `position_at_start`/`position`/`duration` are
    /// seconds): no stream, seek, format or device path is touched.
    ///
    /// Added for the lyrics sync engine (karaoke needs sub-second
    /// resolution); semantics mirror `current_position` line by line:
    /// paused / no anchor → stored coarse position ×1000; playing →
    /// `position_at_start*1000 + (now_ms - start_millis)`, clamped to
    /// `duration*1000`.
    pub fn current_position_ms(&self) -> u64 {
        if !self.is_playing.load(Ordering::SeqCst) {
            return self.position.load(Ordering::SeqCst).saturating_mul(1000);
        }

        let start_millis = self.playback_start_millis.load(Ordering::SeqCst);
        if start_millis == 0 {
            return self.position.load(Ordering::SeqCst).saturating_mul(1000);
        }

        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let elapsed_ms = now_millis.saturating_sub(start_millis);
        let position_at_start_ms = self
            .position_at_start
            .load(Ordering::SeqCst)
            .saturating_mul(1000);
        let duration_ms = self.duration.load(Ordering::SeqCst).saturating_mul(1000);

        // Clamp to duration (same rule as current_position)
        position_at_start_ms.saturating_add(elapsed_ms).min(duration_ms)
    }

    /// Mark playback as started/resumed at current position
    fn start_playback_timer(&self, position: u64) {
        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.playback_start_millis
            .store(now_millis, Ordering::SeqCst);
        self.position_at_start.store(position, Ordering::SeqCst);
    }

    /// Mark playback as paused, saving current position
    fn pause_playback_timer(&self) {
        let current_pos = self.current_position();
        self.position.store(current_pos, Ordering::SeqCst);
        self.playback_start_millis.store(0, Ordering::SeqCst);
    }

    pub fn is_playing(&self) -> bool {
        self.is_playing.load(Ordering::SeqCst)
    }

    pub fn position(&self) -> u64 {
        self.position.load(Ordering::SeqCst)
    }

    pub fn duration(&self) -> u64 {
        self.duration.load(Ordering::SeqCst)
    }

    pub fn current_track_id(&self) -> u64 {
        self.current_track_id.load(Ordering::SeqCst)
    }

    pub fn set_loaded_audio(&self, loaded: bool) {
        self.has_loaded_audio.store(loaded, Ordering::SeqCst);
    }

    pub fn has_loaded_audio(&self) -> bool {
        self.has_loaded_audio.load(Ordering::SeqCst)
    }

    pub fn volume(&self) -> f32 {
        f32::from_bits(self.volume.load(Ordering::SeqCst))
    }
}

/// Audio player that handles streaming playback
/// Uses a dedicated thread for audio output
pub struct Player {
    /// Channel to send commands to the audio thread
    tx: Sender<AudioCommand>,
    /// Shared state accessible from any thread
    pub state: SharedState,
    /// Audio settings (exclusive mode, DAC passthrough, etc.)
    audio_settings: Arc<Mutex<AudioSettings>>,
    /// Visualizer tap for audio sample capture (optional)
    #[allow(dead_code)]
    visualizer_tap: Option<VisualizerTap>,
    /// Bit-depth diagnostic capture (always available, zero-cost when idle)
    pub diagnostic: AudioDiagnostic,
    /// Two-level playback cache (L1 memory + optional L2 disk). A track is
    /// cached after its first play so replays start instantly.
    audio_cache: Arc<qbz_cache::AudioCache>,
}

impl Default for Player {
    fn default() -> Self {
        Self::new(None, AudioSettings::default(), None, AudioDiagnostic::new())
    }
}

impl Player {
    /// Create a new player with an optional specific output device and audio settings
    /// If device_name is None, uses the system default device
    /// visualizer_tap is optional - if provided, audio samples are captured for visualization
    pub fn new(
        device_name: Option<String>,
        audio_settings: AudioSettings,
        visualizer_tap: Option<VisualizerTap>,
        diagnostic: AudioDiagnostic,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<AudioCommand>();
        // The audio thread sends to ITSELF in one place: after a seek rebuilds
        // the engine, it re-issues the gapless hand-off it was holding (see the
        // Seek arm). Re-running `PlayNext` is what keeps that path single —
        // decode, ReplayGain, format check and append all stay in one handler.
        let self_tx = tx.clone();
        let state = SharedState::new();
        let thread_state = state.clone();

        // Clone settings for thread
        let settings = Arc::new(Mutex::new(audio_settings.clone()));
        let thread_settings = settings.clone();

        // Clone visualizer tap and diagnostic for audio thread
        let thread_viz_tap = visualizer_tap.clone();
        let thread_diagnostic = diagnostic.clone();

        // Spawn dedicated audio thread
        thread::spawn(move || {
            log::info!("Audio thread starting...");

            // Initialize loudness analysis system
            let (analyzer_tx, analyzer_rx) = mpsc::sync_channel::<AnalyzerMessage>(64);
            let loudness_cache = match LoudnessCache::new() {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    log::error!("Failed to create loudness cache: {}. Normalization will work without caching.", e);
                    // Create a fallback in-memory cache (will be lost on restart)
                    // For now, just panic — this should not fail in practice
                    panic!("LoudnessCache creation failed: {}", e);
                }
            };
            let _analyzer_handle = LoudnessAnalyzer::spawn(analyzer_rx, loudness_cache.clone());
            let analyzer_enabled = Arc::new(AtomicBool::new(false));

            // Helper to wrap source with visualizer tap, normalization, and diagnostic capture
            // Pipeline order (normalization ON):
            //   Diagnostic (raw) → AnalyzerTap → DynamicAmplify → Visualizer
            // Pipeline order (normalization OFF — bit-perfect):
            //   Diagnostic (raw) → Visualizer
            let wrap_source = |source: Box<dyn Source<Item = f32> + Send>,
                               normalization_gain: Option<f32>,
                               gain_atomic: Option<Arc<AtomicU32>>,
                               analyzer_tx: &SyncSender<AnalyzerMessage>,
                               analyzer_enabled: &Arc<AtomicBool>|
             -> Box<dyn Source<Item = f32> + Send> {
                // Diagnostic tap (innermost — captures raw decoded samples)
                let source: Box<dyn Source<Item = f32> + Send> =
                    Box::new(DiagnosticSource::new(source, thread_diagnostic.clone()));

                // Normalization: dynamic (Phase 2) > static (Phase 1 fallback) > none (bit-perfect)
                let source: Box<dyn Source<Item = f32> + Send> =
                    if let Some(gain_atomic) = gain_atomic {
                        let initial_gain = normalization_gain.unwrap_or(1.0);
                        log::info!(
                            "Audio thread: dynamic normalization enabled (initial gain {:.4})",
                            initial_gain
                        );
                        analyzer_enabled.store(true, Ordering::SeqCst);
                        let source: Box<dyn Source<Item = f32> + Send> = Box::new(
                            AnalyzerTap::new(source, analyzer_tx.clone(), analyzer_enabled.clone()),
                        );
                        Box::new(DynamicAmplify::new(source, gain_atomic, initial_gain))
                    } else if let Some(gain) = normalization_gain {
                        log::info!(
                            "Audio thread: applying static normalization gain factor {:.4}",
                            gain
                        );
                        Box::new(source.amplify(gain))
                    } else {
                        source
                    };

                // Visualizer tap (outermost)
                if let Some(ref tap) = thread_viz_tap {
                    Box::new(TappedSource::new(
                        source,
                        tap.ring_buffer.clone(),
                        tap.enabled.clone(),
                    ))
                } else {
                    source
                }
            };

            // Get the audio host
            let host = rodio::cpal::default_host();

            // Helper to validate a device has supported output configs
            let is_device_valid = |d: &rodio::cpal::Device| -> bool {
                d.supported_output_configs()
                    .map(|configs| configs.count() > 0)
                    .unwrap_or(false)
            };

            // Helper to find and initialize audio device
            // Try backend system first, fall back to legacy CPAL
            // Takes desired sample_rate and channels to maintain DAC passthrough
            let init_device = |name: &Option<String>,
                               state: &SharedState,
                               sample_rate: u32,
                               channels: u16|
             -> Option<StreamType> {
                // Try backend system if configured
                if let Ok(settings) = thread_settings.lock() {
                    if settings.backend_type.is_some() || cfg!(target_os = "macos") {
                        // Use provided sample rate/channels to maintain DAC passthrough
                        log::info!(
                            "Initializing backend system with {}Hz/{}ch",
                            sample_rate,
                            channels
                        );
                        match try_init_stream_with_backend(&settings, sample_rate, channels, state)
                        {
                            Some(Ok(stream_type)) => {
                                // Set device name from settings for backend system
                                let device_name = settings
                                    .output_device
                                    .clone()
                                    .unwrap_or_else(|| "Default".to_string());
                                log::info!("Audio output initialized via backend system at {}Hz (device: {})", sample_rate, device_name);
                                state.set_current_device(Some(device_name));
                                return Some(stream_type);
                            }
                            Some(Err(e)) => {
                                // On macOS, the backend path is the only one that
                                // understands CoreAudio ownership and nominal-rate
                                // validation. Falling through to legacy CPAL can
                                // create a stream at a stale/source rate; in
                                // shared mode that can produce wrong-speed audio,
                                // and in Exclusive Mode it would silently drop Hog
                                // Mode. Surface the failure instead.
                                #[cfg(target_os = "macos")]
                                if settings
                                    .backend_type
                                    .unwrap_or(AudioBackendType::SystemDefault)
                                    == AudioBackendType::SystemDefault
                                {
                                    log::error!(
                                        "Could not start macOS audio output — {}. Not falling back to the legacy CPAL path because it would either play at the wrong speed (shared mode) or silently drop Exclusive Mode.",
                                        e
                                    );
                                    state.set_current_device(None);
                                    state.record_stream_error(e.clone());
                                    return None;
                                }
                                log::warn!(
                                    "Backend system init failed: {}, falling back to legacy",
                                    e
                                );
                            }
                            None => {
                                // Backend not configured, continue to legacy path
                            }
                        }
                    }
                }

                // Legacy CPAL path
                let device = if let Some(ref name) = name {
                    log::info!("Looking for audio device: {}", name);
                    let found = host.output_devices().ok().and_then(|mut devices| {
                        devices.find(|d| cpal_device_name(d).as_deref() == Some(name.as_str()))
                    });

                    match found {
                        Some(d) if is_device_valid(&d) => {
                            log::info!("Found and validated device: {}", name);
                            Some(d)
                        }
                        Some(_) => {
                            log::warn!(
                                "Device '{}' found but has no valid output configs, using default",
                                name
                            );
                            host.default_output_device()
                        }
                        None => {
                            log::warn!("Device '{}' not found, using default", name);
                            host.default_output_device()
                        }
                    }
                } else {
                    log::info!("Using default audio device");
                    host.default_output_device()
                };

                let device = match device {
                    Some(d) => {
                        if let Some(name) = cpal_device_name(&d) {
                            log::info!("Using audio device: {}", name);
                            state.set_current_device(Some(name));
                        }
                        d
                    }
                    None => {
                        log::error!("No audio output device available");
                        state.set_current_device(None);
                        return None;
                    }
                };

                match DeviceSinkBuilder::from_device(device).and_then(|b| b.open_sink_or_fallback())
                {
                    Ok(mixer_sink) => {
                        log::info!("Audio output initialized successfully");
                        Some(StreamType::rodio(mixer_sink))
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to create audio output on device: {}. Trying default...",
                            e
                        );
                        match DeviceSinkBuilder::open_default_sink() {
                            Ok(mixer_sink) => {
                                log::info!("Fallback to default audio output succeeded");
                                Some(StreamType::rodio(mixer_sink))
                            }
                            Err(e2) => {
                                log::error!("Failed to create default audio output: {}", e2);
                                state.set_current_device(None);
                                None
                            }
                        }
                    }
                }
            };

            // Initialize audio device lazily on first playback to avoid idle CPU usage.
            let mut current_device_name = device_name.clone();
            let mut stream_opt: Option<StreamType> = None;
            let mut current_track_sample_rate: Option<u32> = None;
            let mut current_track_channels: Option<u16> = None;

            #[allow(dead_code)]
            const MAX_INIT_RETRIES: u32 = 5;
            #[allow(dead_code)]
            const RETRY_DELAY_MS: u64 = 500;

            let mut current_engine: Option<PlaybackEngine> = None;
            // Store audio data for seeking (we need to re-decode from the beginning)
            let mut current_audio_data: Option<TrackBytes> = None;
            // Set instead of `current_audio_data` when the track arrived through a
            // gapless hand-off that decoded straight from the L2 cache file
            // (see `TrackAudio::File`). Resume re-opens it rather than holding
            // 120-220 MB of Hi-Res in RAM for the whole track.
            let mut current_audio_file: Option<std::path::PathBuf> = None;
            // Store streaming source for resume (when download completes, we can get the data)
            let mut current_streaming_source: Option<Arc<BufferedMediaSource>> = None;
            // Track consecutive sink creation failures to detect broken streams
            let mut consecutive_sink_failures: u32 = 0;
            const MAX_SINK_FAILURES: u32 = 3;
            // Delay dropping the audio stream after pause to reduce CPU usage.
            const PAUSE_SUSPEND_DELAY_MS: u64 = 2000;
            let mut pause_suspend_deadline: Option<Instant> = None;
            let mut last_empty_check = Instant::now();
            // Current track's normalization gain factor (stored for reuse on resume/seek)
            let mut current_normalization_gain: Option<f32> = None;
            // Current track's dynamic gain atomic (shared with DynamicAmplify + LoudnessAnalyzer)
            let mut current_gain_atomic: Option<Arc<AtomicU32>> = None;
            // Gapless: pending next track that has been appended to the Sink
            let mut gapless_pending: Option<GaplessPending> = None;
            // Gapless request guard: once we request "next" for a track, do not re-arm
            // until track changes or playback state is reset.
            let mut gapless_request_armed = false;

            log::info!("Audio thread ready and waiting for commands");

            let handle_command =
                |command: AudioCommand,
                 current_engine: &mut Option<PlaybackEngine>,
                 current_audio_data: &mut Option<TrackBytes>,
                 current_audio_file: &mut Option<std::path::PathBuf>,
                 current_streaming_source: &mut Option<Arc<BufferedMediaSource>>,
                 stream_opt: &mut Option<StreamType>,
                 current_device_name: &mut Option<String>,
                 consecutive_sink_failures: &mut u32,
                 pause_suspend_deadline: &mut Option<Instant>,
                 current_track_sample_rate: &mut Option<u32>,
                 current_track_channels: &mut Option<u16>,
                 current_normalization_gain: &mut Option<f32>,
                 current_gain_atomic: &mut Option<Arc<AtomicU32>>,
                 gapless_pending: &mut Option<GaplessPending>,
                 gapless_request_armed: &mut bool| {
                    match command {
                        AudioCommand::Play {
                            audio,
                            track_id,
                            duration_secs,
                            sample_rate,
                            channels,
                        } => {
                            log::info!(
                                "Audio thread: playing track {} ({}Hz, {} channels)",
                                track_id,
                                sample_rate,
                                channels
                            );
                            begin_new_track(
                                &thread_state,
                                gapless_pending,
                                gapless_request_armed,
                                pause_suspend_deadline,
                            );
                            thread_state.set_dsd_mode(0);

                            let StreamRecreateDecision {
                                needs_new_stream,
                                format_changed,
                                dac_passthrough,
                                using_alsa_direct,
                                using_coreaudio_exclusive,
                                coreaudio_shared_rate_mismatch,
                            } = evaluate_stream_recreate(
                                &thread_settings,
                                stream_opt,
                                *current_track_sample_rate,
                                *current_track_channels,
                                sample_rate,
                                channels,
                                "Play",
                            );

                            if needs_new_stream {
                                if stream_opt.is_some() {
                                    // CoreAudio rate-mismatch case already
                                    // logged at warn level by
                                    // `evaluate_stream_recreate`.
                                    if coreaudio_shared_rate_mismatch.is_none()
                                        && (dac_passthrough
                                            || using_alsa_direct
                                            || using_coreaudio_exclusive)
                                        && format_changed
                                    {
                                        let mode = if using_coreaudio_exclusive {
                                            "CoreAudio exclusive"
                                        } else if using_alsa_direct {
                                            "ALSA Direct"
                                        } else {
                                            "DAC passthrough"
                                        };
                                        log::info!(
                                        "Sample rate/channels changed from {:?}Hz/{:?}ch to {}Hz/{}ch - recreating audio stream ({})",
                                        *current_track_sample_rate,
                                        *current_track_channels,
                                        sample_rate,
                                        channels,
                                        mode
                                    );
                                    }
                                    // Stop engine FIRST so its writer thread releases its
                                    // Arc<AlsaDirectStream> reference before we drop the stream.
                                    // Without this, snd_pcm_open() races against the old PCM
                                    // handle and fails with EBUSY.
                                    if let Some(engine) = current_engine.take() {
                                        engine.stop();
                                        std::thread::sleep(Duration::from_millis(50));
                                    }
                                    // Now this drop is the last Arc ref — PCM actually closes
                                    drop(stream_opt.take());
                                    // Give kernel time to fully release the ALSA device
                                    std::thread::sleep(Duration::from_millis(50));
                                }

                                log::info!(
                                    "DAC passthrough: {}, ALSA Direct: {}, CoreAudio exclusive: {}",
                                    dac_passthrough,
                                    using_alsa_direct,
                                    using_coreaudio_exclusive
                                );

                                // Legacy CPAL path for Play, shared by the
                                // "backend not configured" and "backend init failed"
                                // arms below (#591/#592 follow-up — same shape as the
                                // streaming handler). Uses the track's native
                                // rate/channels unchanged — no resampling is
                                // introduced here.
                                let legacy_stream = || -> Result<StreamType, String> {
                                    // Get the audio device via CPAL
                                    let device = if let Some(ref name) = *current_device_name {
                                        log::info!("Looking for audio device: {}", name);
                                        let found =
                                            host.output_devices().ok().and_then(|mut devices| {
                                                devices.find(|d| {
                                                    cpal_device_name(d).as_deref()
                                                        == Some(name.as_str())
                                                })
                                            });

                                        match found {
                                            Some(d) if is_device_valid(&d) => {
                                                log::info!(
                                                    "Found and validated device: {}",
                                                    name
                                                );
                                                Some(d)
                                            }
                                            Some(_) => {
                                                log::warn!("Device '{}' found but has no valid output configs, using default", name);
                                                host.default_output_device()
                                            }
                                            None => {
                                                log::warn!(
                                                    "Device '{}' not found, using default",
                                                    name
                                                );
                                                host.default_output_device()
                                            }
                                        }
                                    } else {
                                        log::info!("Using default audio device");
                                        host.default_output_device()
                                    };

                                    let Some(device) = device else {
                                        return Err(
                                            "No audio output device available".to_string()
                                        );
                                    };

                                    // Set current device name
                                    if let Some(name) = cpal_device_name(&device) {
                                        log::info!("Using audio device: {}", name);
                                        thread_state.set_current_device(Some(name));
                                    }

                                    create_output_stream_with_config(
                                        device,
                                        sample_rate,
                                        channels,
                                        dac_passthrough,
                                        thread_state.clone(),
                                    )
                                    .map(StreamType::rodio)
                                };

                                // Try backend system first (if configured), then fall back to legacy CPAL
                                // This avoids unnecessary CPAL device enumeration for PipeWire DAC and ALSA Direct
                                let stream_result = if let Some(settings) =
                                    thread_settings.lock().ok()
                                {
                                    match try_init_stream_with_backend(
                                        &settings,
                                        sample_rate,
                                        channels,
                                        &thread_state,
                                    ) {
                                        Some(Ok(stream)) => {
                                            // Backend system handled it - set device name from settings
                                            let device_name = settings
                                                .output_device
                                                .clone()
                                                .unwrap_or_else(|| "Default".to_string());
                                            log::info!(
                                                "Backend system using device: {}",
                                                device_name
                                            );
                                            thread_state.set_current_device(Some(device_name));
                                            Ok(stream)
                                        }
                                        // macOS: the backend path is authoritative
                                        // (CoreAudio ownership + nominal-rate handling);
                                        // surface the failure unchanged — see init_device
                                        // for the rationale.
                                        #[cfg(target_os = "macos")]
                                        Some(Err(e)) => Err(e),
                                        // Linux/other: a backend init failure must not
                                        // dead-end the Play (#591/#592 follow-up). Fall
                                        // back to the legacy CPAL path immediately, same
                                        // as the streaming handler and init_device do.
                                        #[cfg(not(target_os = "macos"))]
                                        Some(Err(e)) => {
                                            log::warn!(
                                                "Backend system init failed for Play: {}, falling back to legacy",
                                                e
                                            );
                                            legacy_stream()
                                        }
                                        None => {
                                            // Backend system not configured, use legacy CPAL path
                                            log::info!("Backend system not configured, using legacy CPAL path");
                                            legacy_stream()
                                        }
                                    }
                                } else {
                                    // Failed to lock settings, use legacy path with CPAL device search
                                    let device = if let Some(ref name) = *current_device_name {
                                        log::info!("Looking for audio device: {}", name);
                                        host.output_devices()
                                            .ok()
                                            .and_then(|mut devices| {
                                                devices.find(|d| {
                                                    cpal_device_name(d).as_deref()
                                                        == Some(name.as_str())
                                                })
                                            })
                                            .or_else(|| {
                                                log::warn!(
                                                    "Device '{}' not found, using default",
                                                    name
                                                );
                                                host.default_output_device()
                                            })
                                    } else {
                                        host.default_output_device()
                                    };

                                    let Some(device) = device else {
                                        log::error!("No audio output device available");
                                        thread_state.set_current_device(None);
                                        thread_state.set_stream_error(true);
                                        return;
                                    };

                                    if let Some(name) = cpal_device_name(&device) {
                                        thread_state.set_current_device(Some(name));
                                    }

                                    create_output_stream_with_config(
                                        device,
                                        sample_rate,
                                        channels,
                                        dac_passthrough,
                                        thread_state.clone(),
                                    )
                                    .map(StreamType::rodio)
                                };

                                // Handle stream creation result
                                match stream_result {
                                    Ok(stream) => {
                                        *stream_opt = Some(stream);
                                        thread_state.set_stream_error(false);

                                        // Set current device name from settings (for backend system)
                                        if let Some(settings) = thread_settings.lock().ok() {
                                            if let Some(ref device_name) = settings.output_device {
                                                thread_state
                                                    .set_current_device(Some(device_name.clone()));
                                                log::info!(
                                                    "Audio stream ready at {}Hz on device: {}",
                                                    sample_rate,
                                                    device_name
                                                );
                                            } else {
                                                thread_state.set_current_device(Some(
                                                    "Default".to_string(),
                                                ));
                                                log::info!(
                                                    "Audio stream ready at {}Hz on default device",
                                                    sample_rate
                                                );
                                            }
                                        } else {
                                            log::info!("Audio stream ready at {}Hz", sample_rate);
                                        }

                                        // Delay to ensure stream is fully initialized before decoder starts
                                        // This prevents sync gaps and allows hardware to stabilize after sample rate changes
                                        // Extra time needed for large sample rate changes (e.g., 88.2kHz → 44.1kHz)
                                        std::thread::sleep(Duration::from_millis(150));
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "❌ Failed to create stream at {}Hz: {}",
                                            sample_rate,
                                            e
                                        );
                                        thread_state.set_stream_error(true);
                                        thread_state.set_current_device(None);
                                        return;
                                    }
                                }
                            } else if format_changed {
                                // Format changed but DAC passthrough is disabled - reuse existing stream
                                log::info!(
                                "Audio format changed from {:?}Hz/{:?}ch to {}Hz/{}ch - reusing audio stream (DAC passthrough disabled, gapless enabled)",
                                *current_track_sample_rate,
                                *current_track_channels,
                                sample_rate,
                                channels
                            );
                            }

                            // Track the decoded source format separately from
                            // the OS output stream rate. In shared mode the
                            // stream can stay at the CoreAudio nominal rate
                            // while each track has its own decoded rate.
                            *current_track_sample_rate = Some(sample_rate);
                            *current_track_channels = Some(channels);

                            let Some(ref stream) = *stream_opt else {
                                log::error!("Audio thread: no audio device available");
                                return;
                            };

                            // Stop previous engine and wait for sink to release resources.
                            // The 50ms sleep is an ALSA-only workaround for snd_pcm_open()
                            // racing the previous PCM handle's release. On CoreAudio (macOS)
                            // and WASAPI (Windows) the host stream stays open across track
                            // changes, so the sleep just feeds 50ms of silence into the
                            // mixer — audible as a click at each end of the gap.
                            if let Some(engine) = current_engine.take() {
                                engine.stop();
                                #[cfg(target_os = "linux")]
                                std::thread::sleep(Duration::from_millis(50));
                            }

                            // Exactly one of the two is live: the seek arm and
                            // the engine rebuild both pick their source by
                            // asking which one is set.
                            match &audio {
                                TrackAudio::Memory(data) => {
                                    *current_audio_data = Some(data.clone());
                                    *current_audio_file = None;
                                }
                                TrackAudio::File(path) => {
                                    *current_audio_data = None;
                                    *current_audio_file = Some(path.clone());
                                }
                            }
                            *current_streaming_source = None; // Clear streaming source for non-streaming playback
                            thread_state.set_loaded_audio(true);

                            // Create PlaybackEngine from StreamType
                            let mut engine = match stream {
                                StreamType::Rodio { sink: mixer_sink, .. } => {
                                    match PlaybackEngine::new_rodio(&mixer_sink.mixer()) {
                                        Ok(e) => {
                                            *consecutive_sink_failures = 0;
                                            thread_state.set_stream_error(false);
                                            e
                                        }
                                        Err(e) => {
                                            *consecutive_sink_failures += 1;
                                            log::error!(
                                                "Failed to create engine (attempt {}): {}",
                                                *consecutive_sink_failures,
                                                e
                                            );

                                            if *consecutive_sink_failures >= MAX_SINK_FAILURES {
                                                log::warn!(
                                                "Audio stream appears broken after {} failures. Auto-reinitializing...",
                                                *consecutive_sink_failures
                                            );
                                                thread_state.set_stream_error(true);

                                                drop(stream_opt.take());
                                                std::thread::sleep(Duration::from_millis(200));

                                                // Use last known sample rate/channels to maintain DAC passthrough
                                                let sr = current_track_sample_rate.unwrap_or(48000);
                                                let ch = current_track_channels.unwrap_or(2);
                                                *stream_opt = init_device(
                                                    current_device_name,
                                                    &thread_state,
                                                    sr,
                                                    ch,
                                                );
                                                if stream_opt.is_some() {
                                                    log::info!("Audio stream auto-reinitialized successfully at {}Hz", sr);
                                                    *consecutive_sink_failures = 0;
                                                    thread_state.set_stream_error(false);
                                                } else {
                                                    log::error!("Auto-reinit failed. Audio device unavailable.");
                                                    thread_state
                                                        .is_playing
                                                        .store(false, Ordering::SeqCst);
                                                    thread_state.set_current_device(None);
                                                }
                                            }
                                            return;
                                        }
                                    }
                                }
                                #[cfg(target_os = "linux")]
                                StreamType::AlsaDirect(alsa_stream) => {
                                    *consecutive_sink_failures = 0;
                                    thread_state.set_stream_error(false);
                                    let hardware_volume = thread_settings
                                        .lock()
                                        .ok()
                                        .map(|s| s.alsa_hardware_volume)
                                        .unwrap_or(false);
                                    PlaybackEngine::new_alsa_direct(
                                        alsa_stream.clone(),
                                        hardware_volume,
                                    )
                                }
                                #[cfg(target_os = "linux")]
                                StreamType::Jack(jack_stream) => {
                                    *consecutive_sink_failures = 0;
                                    thread_state.set_stream_error(false);
                                    PlaybackEngine::new_jack(jack_stream.clone())
                                }
                            };

                            let volume = f32::from_bits(thread_state.volume.load(Ordering::SeqCst));
                            apply_engine_volume(&stream_opt, &engine, volume);

                            let decoded = match &audio {
                                TrackAudio::Memory(data) => decode_with_fallback(data),
                                TrackAudio::File(path) => decode_file_with_fallback(path),
                            };
                            let source = match decoded {
                                Ok(s) => s,
                                Err(e) => {
                                    log::error!("Failed to decode audio: {}", e);
                                    return;
                                }
                            };

                            let actual_duration = source
                                .total_duration()
                                .map(|d| d.as_secs())
                                .unwrap_or(duration_secs);
                            thread_state
                                .duration
                                .store(actual_duration, Ordering::SeqCst);

                            // Calculate normalization gain if enabled
                            let norm_settings = thread_settings
                                .lock()
                                .ok()
                                .filter(|s| s.normalization_enabled)
                                .map(|s| s.normalization_target_lufs);

                            let (normalization, gain_atomic) =
                                if let Some(target_lufs) = norm_settings {
                                    // Check for ReplayGain metadata first (initial gain hint)
                                    // A file is read head-first for this: FLAC
                                    // carries ReplayGain in a metadata block at
                                    // the very start, so there is no reason to
                                    // pull 116 MB off the card to find it.
                                    let rg_gain = match &audio {
                                        TrackAudio::Memory(data) => extract_replaygain(data),
                                        TrackAudio::File(path) => {
                                            read_head(path, HEAD_BYTES).and_then(|head| extract_replaygain(&head))
                                        }
                                    }
                                    .map(|rg| calculate_gain_factor(&rg, target_lufs));

                                    // Create shared atomic for dynamic normalization
                                    let atomic =
                                        Arc::new(AtomicU32::new(rg_gain.unwrap_or(1.0).to_bits()));

                                    // Check loudness cache for pre-computed EBU R128 gain
                                    if let Some(cached) = loudness_cache.get(track_id) {
                                        let cached_gain = db_to_linear(cached.gain_db.min(6.0));
                                        atomic.store(cached_gain.to_bits(), Ordering::Relaxed);
                                        log::info!(
                                            "Normalization: cache hit for track {}, gain {:.4}",
                                            track_id,
                                            cached_gain
                                        );
                                    }

                                    // Notify analyzer of new track
                                    let _ = analyzer_tx.try_send(AnalyzerMessage::NewTrack {
                                        track_id,
                                        sample_rate,
                                        channels,
                                        target_lufs,
                                        gain_atomic: atomic.clone(),
                                    });

                                    (rg_gain, Some(atomic))
                                } else {
                                    (None, None)
                                };

                            *current_normalization_gain = normalization;
                            *current_gain_atomic = gain_atomic.clone();
                            thread_state.set_normalization_gain(normalization);

                            // Wrap source with diagnostic, normalization, and visualizer
                            let source = wrap_source(
                                source,
                                normalization,
                                gain_atomic,
                                &analyzer_tx,
                                &analyzer_enabled,
                            );
                            if let Err(e) = engine.append(source) {
                                log::error!("Failed to append source to engine: {}", e);
                                return;
                            }

                            thread_state.is_playing.store(true, Ordering::SeqCst);
                            thread_state.position.store(0, Ordering::SeqCst);
                            thread_state
                                .current_track_id
                                .store(track_id, Ordering::SeqCst);
                            thread_state.start_playback_timer(0);

                            *current_engine = Some(engine);
                            log::info!(
                                "Audio thread: playback started, duration: {}s, normalization: {}",
                                actual_duration,
                                normalization
                                    .map(|g| format!("{:.4}x", g))
                                    .unwrap_or_else(|| "off".to_string())
                            );
                        }
                        AudioCommand::PlayStreaming {
                            source,
                            track_id,
                            sample_rate,
                            channels,
                            duration_secs,
                            start_position_secs,
                            content_length,
                            play_gen,
                        } => {
                            log::info!(
                            "Audio thread: starting streaming playback for track {} ({}Hz, {} channels, {}s, start={}s)",
                            track_id,
                            sample_rate,
                            channels,
                            duration_secs,
                            start_position_secs
                        );
                            begin_new_track(
                                &thread_state,
                                gapless_pending,
                                gapless_request_armed,
                                pause_suspend_deadline,
                            );

                            // Store streaming source for resume capability
                            // When download completes, we can extract the data for resume
                            *current_streaming_source = Some(source.clone());
                            *current_audio_data = None;
                            *current_audio_file = None; // Clear regular audio data
                            thread_state.set_loaded_audio(true);

                            // Track metadata is known-good at command time. Store the
                            // duration NOW — current_position() clamps to it, so leaving
                            // it 0 until after the buffer wait freezes the bar at 0:00,
                            // and a Resume that recovers this source after a failed setup
                            // would pin the bar at 0:00 forever (#508 follow-up, 2.0.1
                            // Flatpak report 2026-07-15).
                            thread_state.duration.store(duration_secs, Ordering::SeqCst);

                            let StreamRecreateDecision {
                                needs_new_stream,
                                format_changed,
                                dac_passthrough,
                                using_alsa_direct,
                                using_coreaudio_exclusive,
                                coreaudio_shared_rate_mismatch,
                            } = evaluate_stream_recreate(
                                &thread_settings,
                                stream_opt,
                                *current_track_sample_rate,
                                *current_track_channels,
                                sample_rate,
                                channels,
                                "Streaming",
                            );

                            if needs_new_stream {
                                if stream_opt.is_some() {
                                    // CoreAudio rate-mismatch case already
                                    // logged at warn level by
                                    // `evaluate_stream_recreate`.
                                    if coreaudio_shared_rate_mismatch.is_none()
                                        && (dac_passthrough
                                            || using_alsa_direct
                                            || using_coreaudio_exclusive)
                                        && format_changed
                                    {
                                        let mode = if using_coreaudio_exclusive {
                                            "CoreAudio exclusive"
                                        } else if using_alsa_direct {
                                            "ALSA Direct"
                                        } else {
                                            "DAC passthrough"
                                        };
                                        log::info!(
                                        "Streaming: Sample rate/channels changed to {}Hz/{}ch - recreating audio stream ({})",
                                        sample_rate,
                                        channels,
                                        mode
                                    );
                                    }
                                    // Stop engine FIRST so its writer thread releases its
                                    // Arc<AlsaDirectStream> reference before we drop the stream.
                                    // Without this, snd_pcm_open() races against the old PCM
                                    // handle and fails with EBUSY.
                                    if let Some(engine) = current_engine.take() {
                                        engine.stop();
                                        std::thread::sleep(Duration::from_millis(50));
                                    }
                                    // Now this drop is the last Arc ref — PCM actually closes
                                    drop(stream_opt.take());
                                    // Give kernel time to fully release the ALSA device
                                    std::thread::sleep(Duration::from_millis(50));
                                }

                                // Legacy CPAL path for streaming, shared by the
                                // "backend not configured" and "backend init failed"
                                // arms below. Uses the track's native rate/channels
                                // unchanged — no resampling is introduced here.
                                let legacy_stream = || -> Result<StreamType, String> {
                                    let device = if let Some(ref name) = *current_device_name {
                                        host.output_devices()
                                            .ok()
                                            .and_then(|mut devices| {
                                                devices.find(|d| {
                                                    cpal_device_name(d).as_deref()
                                                        == Some(name.as_str())
                                                })
                                            })
                                            .or_else(|| host.default_output_device())
                                    } else {
                                        host.default_output_device()
                                    };

                                    let Some(device) = device else {
                                        return Err(
                                            "No audio output device available for streaming"
                                                .to_string(),
                                        );
                                    };

                                    if let Some(name) = cpal_device_name(&device) {
                                        thread_state.set_current_device(Some(name));
                                    }

                                    create_output_stream_with_config(
                                        device,
                                        sample_rate,
                                        channels,
                                        dac_passthrough,
                                        thread_state.clone(),
                                    )
                                    .map(StreamType::rodio)
                                };

                                let stream_result = if let Some(settings) =
                                    thread_settings.lock().ok()
                                {
                                    match try_init_stream_with_backend(
                                        &settings,
                                        sample_rate,
                                        channels,
                                        &thread_state,
                                    ) {
                                        Some(Ok(stream)) => {
                                            // Set device name from settings for backend system
                                            let device_name = settings
                                                .output_device
                                                .clone()
                                                .unwrap_or_else(|| "Default".to_string());
                                            log::info!(
                                                "Streaming backend using device: {}",
                                                device_name
                                            );
                                            thread_state.set_current_device(Some(device_name));
                                            Ok(stream)
                                        }
                                        // macOS: the backend path is authoritative
                                        // (CoreAudio ownership + nominal-rate handling);
                                        // surface the failure unchanged — see init_device
                                        // for the rationale.
                                        #[cfg(target_os = "macos")]
                                        Some(Err(e)) => Err(e),
                                        // Linux/other: a backend init failure must not
                                        // leave the track never-starting (#591/#592 — the
                                        // 45s loading watchdog was the only exit). Fall
                                        // back to the legacy CPAL path immediately, same
                                        // as the Play/Resume paths do via init_device.
                                        #[cfg(not(target_os = "macos"))]
                                        Some(Err(e)) => {
                                            log::warn!(
                                                "Backend system init failed for streaming: {}, falling back to legacy",
                                                e
                                            );
                                            legacy_stream()
                                        }
                                        None => {
                                            log::info!("Backend system not configured, using legacy CPAL path");
                                            legacy_stream()
                                        }
                                    }
                                } else {
                                    let device = host.default_output_device();
                                    let Some(device) = device else {
                                        log::error!(
                                            "No audio output device available for streaming"
                                        );
                                        // Same cleanup as the failure arms below: the
                                        // streaming source was already stored at
                                        // command-accept, and a Resume that finds it
                                        // would replay a track that never started.
                                        *current_streaming_source = None;
                                        *current_audio_data = None;
                                        *current_audio_file = None;
                                        thread_state.set_loaded_audio(false);
                                        thread_state.is_playing.store(false, Ordering::SeqCst);
                                        thread_state.record_stream_error(
                                            "No audio output device available for streaming",
                                        );
                                        return;
                                    };
                                    create_output_stream_with_config(
                                        device,
                                        sample_rate,
                                        channels,
                                        dac_passthrough,
                                        thread_state.clone(),
                                    )
                                    .map(StreamType::rodio)
                                };

                                match stream_result {
                                    Ok(stream) => {
                                        *stream_opt = Some(stream);
                                        thread_state.set_stream_error(false);
                                        log::info!(
                                            "Streaming audio stream ready at {}Hz",
                                            sample_rate
                                        );
                                        std::thread::sleep(Duration::from_millis(150));
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "❌ Failed to create stream for streaming at {}Hz: {}",
                                            sample_rate,
                                            e
                                        );
                                        // Backend AND legacy CPAL both failed — a real
                                        // error the user must see. Clear the zombie
                                        // streaming state (same cleanup as the sibling
                                        // failure arms below); leaving it set lets a
                                        // later Resume replay the completed download
                                        // with duration still 0, pinning the bar at
                                        // 0:00 forever (#508/#592).
                                        *current_streaming_source = None;
                                        *current_audio_data = None;
                                        *current_audio_file = None;
                                        thread_state.set_loaded_audio(false);
                                        thread_state.is_playing.store(false, Ordering::SeqCst);
                                        thread_state.record_stream_error(e);
                                        return;
                                    }
                                }
                            }

                            // Keep decoded source format current even when
                            // macOS shared mode reuses the same output stream.
                            *current_track_sample_rate = Some(sample_rate);
                            *current_track_channels = Some(channels);

                            let Some(ref stream) = *stream_opt else {
                                log::error!(
                                    "Audio thread: no audio device available for streaming"
                                );
                                return;
                            };

                            // Stop previous engine. ALSA-only sleep — see Play handler
                            // above for rationale (CoreAudio/WASAPI don't race here and the
                            // 50ms gap is audible as a click).
                            if let Some(engine) = current_engine.take() {
                                engine.stop();
                                #[cfg(target_os = "linux")]
                                std::thread::sleep(Duration::from_millis(50));
                            }

                            // Create PlaybackEngine
                            let mut engine = match stream {
                                StreamType::Rodio { sink: mixer_sink, .. } => {
                                    match PlaybackEngine::new_rodio(&mixer_sink.mixer()) {
                                        Ok(e) => {
                                            *consecutive_sink_failures = 0;
                                            thread_state.set_stream_error(false);
                                            e
                                        }
                                        Err(e) => {
                                            log::error!(
                                                "Failed to create engine for streaming: {}",
                                                e
                                            );
                                            *current_streaming_source = None;
                                            *current_audio_data = None;
                                            *current_audio_file = None;
                                            thread_state.set_loaded_audio(false);
                                            thread_state
                                                .is_playing
                                                .store(false, Ordering::SeqCst);
                                            thread_state.record_stream_error(
                                                "Failed to create the playback engine for streaming",
                                            );
                                            return;
                                        }
                                    }
                                }
                                #[cfg(target_os = "linux")]
                                StreamType::AlsaDirect(alsa_stream) => {
                                    let hardware_volume = thread_settings
                                        .lock()
                                        .ok()
                                        .map(|s| s.alsa_hardware_volume)
                                        .unwrap_or(false);
                                    PlaybackEngine::new_alsa_direct(
                                        alsa_stream.clone(),
                                        hardware_volume,
                                    )
                                }
                                #[cfg(target_os = "linux")]
                                StreamType::Jack(jack_stream) => {
                                    PlaybackEngine::new_jack(jack_stream.clone())
                                }
                            };

                            let volume = f32::from_bits(thread_state.volume.load(Ordering::SeqCst));
                            apply_engine_volume(&stream_opt, &engine, volume);

                            // Wait for minimum buffer before starting playback.
                            //
                            // A session resume (start_position_secs > 0) needs
                            // the bytes AT the resume offset, and how we get
                            // them depends on the feeder. A range-capable
                            // feeder only needs the file header here — format
                            // plus seek table — because the jump below turns
                            // the offset into one HTTP request. A sequential
                            // feeder has no such option: the eager pre-skip
                            // decodes-and-discards from byte zero up to the
                            // offset, so it has to wait for the offset plus an
                            // 8s headroom to be downloaded before the audio
                            // device can be fed without underrunning.
                            log::info!("Streaming: waiting for initial buffer...");
                            let start_wait = Instant::now();
                            let max_wait = Duration::from_secs(60);

                            // Smallest window that reliably holds a FLAC
                            // STREAMINFO plus SEEKTABLE — the same
                            // format-detection floor StreamingConfig uses.
                            const HEADER_PROBE_BYTES: u64 = 256 * 1024;

                            let ranged_resume =
                                start_position_secs > 0 && source.supports_range_requests();

                            let bytes_per_sec_estimate: u64 = if duration_secs > 0
                                && content_length > 0
                            {
                                content_length / duration_secs
                            } else {
                                200_000
                            };
                            let resume_buffer_target: u64 =
                                if start_position_secs > 0 && !ranged_resume {
                                    bytes_per_sec_estimate
                                        .saturating_mul(start_position_secs.saturating_add(8))
                                } else {
                                    0
                                };

                            let buffer_sufficient = |src: &Arc<BufferedMediaSource>| -> bool {
                                if ranged_resume {
                                    // Enough to probe the format and read the
                                    // seek table; the playback buffer is filled
                                    // after the jump, at the resume point.
                                    return src.head_bytes() >= HEADER_PROBE_BYTES
                                        || src.is_complete();
                                }
                                if !src.has_min_buffer() {
                                    return false;
                                }
                                if resume_buffer_target == 0 {
                                    return true;
                                }
                                (src.buffer_size() as u64) >= resume_buffer_target
                            };

                            while !buffer_sufficient(&source)
                                && source.download_error().is_none()
                                && start_wait.elapsed() < max_wait
                                && thread_state.is_current_play(play_gen)
                            {
                                std::thread::sleep(Duration::from_millis(50));
                            }
                            let buffer_ready = buffer_sufficient(&source);

                            // A newer play intent superseded this one while we
                            // were waiting (user clicked another track). Bail
                            // out immediately — the newer PlayStreaming command
                            // is queued right behind us — instead of blocking
                            // the audio thread up to 60s on a download that no
                            // longer matters (#591). Not an error: no
                            // stream_error, quiet return (same shape as the
                            // timeout return below).
                            if !thread_state.is_current_play(play_gen) {
                                log::info!(
                                    "Streaming: play of track {} superseded after {}ms buffer wait — abandoning",
                                    track_id,
                                    start_wait.elapsed().as_millis()
                                );
                                return;
                            }

                            if !buffer_ready && !source.has_min_buffer() {
                                // #595 attributes feeder-death vs plain timeout;
                                // #594 clears the transport state so the UI does
                                // not sit on a phantom "playing" with no engine.
                                let err_msg = match source.download_error() {
                                    Some(err) => {
                                        log::error!(
                                            "Streaming: feeder failed before initial buffer: {}",
                                            err
                                        );
                                        format!("Stream feeder failed before buffering: {err}")
                                    }
                                    None => {
                                        log::error!(
                                            "Streaming: timeout waiting for initial buffer"
                                        );
                                        "Timed out waiting for the stream buffer to fill"
                                            .to_string()
                                    }
                                };
                                *current_streaming_source = None;
                                *current_audio_data = None;
                                *current_audio_file = None;
                                thread_state.set_loaded_audio(false);
                                thread_state.is_playing.store(false, Ordering::SeqCst);
                                thread_state.record_stream_error(err_msg);
                                return;
                            }
                            if resume_buffer_target > 0
                                && (source.buffer_size() as u64) < resume_buffer_target
                            {
                                log::warn!(
                                    "Streaming: timed out waiting for resume buffer (got {} bytes, wanted {}); pre-skip may underrun briefly",
                                    source.buffer_size(),
                                    resume_buffer_target
                                );
                            }

                            let buffer_wait_ms = start_wait.elapsed().as_millis();
                            log::info!(
                            "Streaming: buffer ready in {}ms ({} bytes, target {}), creating incremental decoder...",
                            buffer_wait_ms, source.buffer_size(), resume_buffer_target
                        );

                            // Create incremental streaming source - this starts playback IMMEDIATELY
                            // while continuing to decode/download in background
                            let mut incremental_source =
                                match IncrementalStreamingSource::new(source.clone()) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        log::error!(
                                            "Failed to create incremental streaming source: {}",
                                            e
                                        );
                                        *current_streaming_source = None;
                                        *current_audio_data = None;
                                        *current_audio_file = None;
                                        thread_state.set_loaded_audio(false);
                                        thread_state.is_playing.store(false, Ordering::SeqCst);
                                        thread_state.record_stream_error(
                                            "Failed to start the streaming decoder",
                                        );
                                        return;
                                    }
                                };

                            // Verify sample rate/channels match what we expected
                            let actual_sr = incremental_source.get_sample_rate();
                            let actual_ch = incremental_source.get_channels();
                            if actual_sr != sample_rate || actual_ch != channels {
                                log::warn!(
                                "Streaming: detected format {}Hz/{}ch differs from expected {}Hz/{}ch",
                                actual_sr, actual_ch, sample_rate, channels
                            );
                            }

                            // Session resume over a range-capable feeder: ask
                            // Symphonia to land on the offset directly. The
                            // FLAC seek table turns the timestamp into a byte
                            // offset, the buffer turns that into a `Range`
                            // request, and the decoder starts there — instead
                            // of downloading every byte before the offset and
                            // then decoding-and-discarding all of it.
                            let mut resumed_by_seek = false;
                            if ranged_resume {
                                let seek_start = Instant::now();
                                match incremental_source
                                    .seek_to(Duration::from_secs(start_position_secs))
                                {
                                    Ok(()) => {
                                        resumed_by_seek = true;
                                        log::info!(
                                            "Resume: landed on {}s in {}ms (byte offset {}, {} bytes held)",
                                            start_position_secs,
                                            seek_start.elapsed().as_millis(),
                                            source.primary_offset(),
                                            source.buffer_size()
                                        );
                                    }
                                    Err(e) => {
                                        // No usable seek table, or the request
                                        // failed. Fall back to the pre-skip
                                        // below, which only needs bytes in
                                        // order — slow, but it always works.
                                        log::warn!(
                                            "Resume: native seek to {}s failed ({}); falling back to pre-skip",
                                            start_position_secs,
                                            e
                                        );
                                    }
                                }
                            }

                            // The jump moved playback to a part of the file
                            // nothing has buffered yet: refill there before
                            // handing the source to the engine, or the first
                            // pull underruns the device.
                            if resumed_by_seek {
                                let refill = Instant::now();
                                while !source.has_min_buffer()
                                    && source.download_error().is_none()
                                    && refill.elapsed() < Duration::from_secs(30)
                                    && thread_state.is_current_play(play_gen)
                                {
                                    std::thread::sleep(Duration::from_millis(20));
                                }
                                if !thread_state.is_current_play(play_gen) {
                                    log::info!(
                                        "Streaming: play of track {} superseded while refilling after the resume seek",
                                        track_id
                                    );
                                    return;
                                }
                                if let Some(err) = source.download_error() {
                                    log::error!("Resume: feeder failed after the seek: {}", err);
                                    *current_streaming_source = None;
                                    *current_audio_data = None;
                                    *current_audio_file = None;
                                    thread_state.set_loaded_audio(false);
                                    thread_state.is_playing.store(false, Ordering::SeqCst);
                                    thread_state.record_stream_error(format!(
                                        "Stream feeder failed after the resume seek: {err}"
                                    ));
                                    return;
                                }
                                log::info!(
                                    "Resume: buffered {} bytes from the resume point in {}ms",
                                    source.buffer_size(),
                                    refill.elapsed().as_millis()
                                );
                            }

                            // Set duration from track metadata (passed from frontend)
                            // This allows the seekbar to show progress even during streaming
                            thread_state.duration.store(duration_secs, Ordering::SeqCst);

                            // Normalization for streaming: try ReplayGain from buffered data,
                            // then fall back to real-time EBU R128 analysis
                            let norm_settings = thread_settings
                                .lock()
                                .ok()
                                .filter(|s| s.normalization_enabled)
                                .map(|s| s.normalization_target_lufs);

                            let (normalization, gain_atomic) = if let Some(target_lufs) =
                                norm_settings
                            {
                                // Try ReplayGain metadata from buffered data
                                let rg_gain = source.get_buffered_data().and_then(|data| {
                                    extract_replaygain(&data)
                                        .map(|rg| calculate_gain_factor(&rg, target_lufs))
                                });

                                // Create shared atomic for dynamic normalization
                                let atomic =
                                    Arc::new(AtomicU32::new(rg_gain.unwrap_or(1.0).to_bits()));

                                // Check loudness cache
                                if let Some(cached) = loudness_cache.get(track_id) {
                                    let cached_gain = db_to_linear(cached.gain_db.min(6.0));
                                    atomic.store(cached_gain.to_bits(), Ordering::Relaxed);
                                    log::info!("Streaming normalization: cache hit for track {}, gain {:.4}", track_id, cached_gain);
                                }

                                // Notify analyzer of new track
                                let _ = analyzer_tx.try_send(AnalyzerMessage::NewTrack {
                                    track_id,
                                    sample_rate,
                                    channels,
                                    target_lufs,
                                    gain_atomic: atomic.clone(),
                                });

                                (rg_gain, Some(atomic))
                            } else {
                                (None, None)
                            };

                            *current_normalization_gain = normalization;
                            *current_gain_atomic = gain_atomic.clone();
                            thread_state.set_normalization_gain(normalization);

                            // Box the incremental source to match the expected type
                            let mut source_to_play: Box<dyn Source<Item = f32> + Send> =
                                Box::new(incremental_source);

                            // Eager pre-skip for session resume. Decode and
                            // discard samples here so the engine's first pull
                            // doesn't have to do the work synchronously, which
                            // would underrun the audio device for multi-second
                            // offsets. The buffer wait above guarantees
                            // there's enough downloaded data to feed this loop.
                            if start_position_secs > 0 && !resumed_by_seek {
                                let target_samples: u64 = (start_position_secs)
                                    .saturating_mul(actual_sr as u64)
                                    .saturating_mul(actual_ch as u64);
                                let skip_start = Instant::now();
                                let mut skipped: u64 = 0;
                                while skipped < target_samples {
                                    if source_to_play.next().is_none() {
                                        log::warn!(
                                            "Resume: source ended before reaching {}s (pre-skipped {} samples)",
                                            start_position_secs,
                                            skipped
                                        );
                                        break;
                                    }
                                    skipped += 1;
                                }
                                log::info!(
                                    "Resume: pre-skipped {} samples ({}s) in {}ms",
                                    skipped,
                                    start_position_secs,
                                    skip_start.elapsed().as_millis()
                                );
                            }

                            // Wrap source with diagnostic, normalization, and visualizer
                            let source_to_play = wrap_source(
                                source_to_play,
                                normalization,
                                gain_atomic,
                                &analyzer_tx,
                                &analyzer_enabled,
                            );
                            if let Err(e) = engine.append(source_to_play) {
                                log::error!("Failed to append streaming source to engine: {}", e);
                                return;
                            }

                            thread_state.set_dsd_mode(0);
                            thread_state.is_playing.store(true, Ordering::SeqCst);
                            thread_state
                                .position
                                .store(start_position_secs, Ordering::SeqCst);
                            thread_state
                                .current_track_id
                                .store(track_id, Ordering::SeqCst);
                            thread_state.start_playback_timer(start_position_secs);

                            *current_engine = Some(engine);
                            log::info!(
                            "Audio thread: streaming playback STARTED in {}ms at {}s (incremental decode active)",
                            start_wait.elapsed().as_millis(),
                            start_position_secs
                        );
                        }
                        AudioCommand::PlayDsdDop { path, track_id } => {
                            #[cfg(target_os = "linux")]
                            {
                                log::info!(
                                    "Audio thread: DoP playback for track {} ({})",
                                    track_id,
                                    path.display()
                                );
                                *pause_suspend_deadline = None;
                                *gapless_pending = None;
                                *gapless_request_armed = false;
                                thread_state.set_gapless_ready(false);
                                thread_state.set_gapless_next_track_id(0);

                                let demux = match qbz_dsd::open_dsd(&path) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        log::error!("DoP: cannot open DSD file: {}", e);
                                        thread_state.set_stream_error(true);
                                        return;
                                    }
                                };
                                let dop = match qbz_dsd::DopStream::new(demux) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        log::error!("DoP: cannot build DoP stream: {}", e);
                                        thread_state.set_stream_error(true);
                                        return;
                                    }
                                };
                                let carrier = dop.carrier_rate();
                                let dsd_rate = dop.dsd_rate();
                                let duration =
                                    dop.total_frames() / (carrier.max(1) as u64);

                                // DoP always needs a fresh S32 stream at the
                                // carrier rate — tear down whatever is open.
                                if let Some(engine) = current_engine.take() {
                                    engine.stop();
                                    std::thread::sleep(Duration::from_millis(50));
                                }
                                drop(stream_opt.take());
                                std::thread::sleep(Duration::from_millis(50));

                                let device = thread_settings
                                    .lock()
                                    .ok()
                                    .and_then(|s| s.output_device.clone());
                                let Some(device) = device else {
                                    log::error!("DoP: no output device configured");
                                    thread_state.set_stream_error(true);
                                    return;
                                };
                                let stream = match qbz_audio::alsa_backend::create_dop_stream(
                                    &device, carrier, 2,
                                ) {
                                    Ok(st) => Arc::new(st),
                                    Err(e) => {
                                        log::error!("DoP: stream open failed: {}", e);
                                        thread_state.set_stream_error(true);
                                        thread_state.set_current_device(None);
                                        return;
                                    }
                                };
                                log::info!(
                                    "DoP: {} locked at {} Hz carrier on {}",
                                    qbz_dsd::dsd_label(dsd_rate),
                                    carrier,
                                    device
                                );
                                *stream_opt = Some(StreamType::AlsaDirect(stream.clone()));
                                thread_state.set_current_device(Some(device));
                                *current_track_sample_rate = Some(carrier);
                                *current_track_channels = Some(2);
                                *current_audio_data = None;
                                *current_audio_file = None;
                                *current_streaming_source = None;

                                let mut engine = PlaybackEngine::new_alsa_dop(stream, false);
                                if let Err(e) = engine.append_dop(Box::new(DsdErrorReport::new(
                                    dop,
                                    thread_state.clone(),
                                ))) {
                                    log::error!("DoP: append failed: {}", e);
                                    thread_state.set_stream_error(true);
                                    return;
                                }
                                *current_engine = Some(engine);
                                thread_state.set_loaded_audio(true);
                                thread_state.set_stream_error(false);
                                thread_state.set_stream_quality(dsd_rate, 1);
                                thread_state.duration.store(duration, Ordering::SeqCst);
                                thread_state.set_dsd_mode(1);
                                thread_state.is_playing.store(true, Ordering::SeqCst);
                                thread_state.position.store(0, Ordering::SeqCst);
                                thread_state
                                    .current_track_id
                                    .store(track_id, Ordering::SeqCst);
                                thread_state.start_playback_timer(0);
                                log::info!("Audio thread: DoP playback STARTED");
                            }
                            #[cfg(not(target_os = "linux"))]
                            {
                                let _ = (path, track_id);
                                log::error!("DoP playback is Linux-only");
                                thread_state.set_stream_error(true);
                            }
                        }
                        AudioCommand::PlayDsdNative { path, track_id } => {
                            #[cfg(target_os = "linux")]
                            {
                                log::info!(
                                    "Audio thread: native DSD playback for track {} ({})",
                                    track_id,
                                    path.display()
                                );
                                *pause_suspend_deadline = None;
                                *gapless_pending = None;
                                *gapless_request_armed = false;
                                thread_state.set_gapless_ready(false);
                                thread_state.set_gapless_next_track_id(0);

                                let info = match qbz_dsd::open_dsd(&path) {
                                    Ok(d) => d.info().clone(),
                                    Err(e) => {
                                        log::error!("Native DSD: cannot open file: {}", e);
                                        thread_state.set_stream_error(true);
                                        return;
                                    }
                                };
                                if info.channels != 2 {
                                    log::error!("Native DSD: stereo only");
                                    thread_state.set_stream_error(true);
                                    return;
                                }
                                let rate = qbz_dsd::native_u32_rate(info.dsd_rate);
                                let duration = (info.sample_count / 32) / (rate.max(1) as u64);

                                if let Some(engine) = current_engine.take() {
                                    engine.stop();
                                    std::thread::sleep(Duration::from_millis(50));
                                }
                                drop(stream_opt.take());
                                std::thread::sleep(Duration::from_millis(50));

                                let device = thread_settings
                                    .lock()
                                    .ok()
                                    .and_then(|s| s.output_device.clone());
                                let Some(device) = device else {
                                    log::error!("Native DSD: no output device configured");
                                    thread_state.set_stream_error(true);
                                    return;
                                };
                                let (stream, little_endian) =
                                    match qbz_audio::alsa_backend::create_native_dsd_stream(
                                        &device,
                                        info.dsd_rate,
                                        2,
                                    ) {
                                        Ok(pair) => pair,
                                        Err(e) => {
                                            log::error!("Native DSD: stream open failed: {}", e);
                                            thread_state.set_stream_error(true);
                                            thread_state.set_current_device(None);
                                            return;
                                        }
                                    };
                                let stream = Arc::new(stream);
                                let native_src = match qbz_dsd::open_dsd(&path)
                                    .map_err(|e| e.to_string())
                                    .and_then(|d| {
                                        qbz_dsd::NativeDsdStream::new(d, little_endian)
                                            .map_err(|e| e.to_string())
                                    }) {
                                    Ok(n) => n,
                                    Err(e) => {
                                        log::error!("Native DSD: source build failed: {}", e);
                                        thread_state.set_stream_error(true);
                                        return;
                                    }
                                };
                                log::info!(
                                    "Native DSD: {} locked at {} Hz U32 ({}) on {}",
                                    qbz_dsd::dsd_label(info.dsd_rate),
                                    rate,
                                    if little_endian { "LE" } else { "BE" },
                                    device
                                );
                                *stream_opt = Some(StreamType::AlsaDirect(stream.clone()));
                                thread_state.set_current_device(Some(device));
                                *current_track_sample_rate = Some(rate);
                                *current_track_channels = Some(2);
                                *current_audio_data = None;
                                *current_audio_file = None;
                                *current_streaming_source = None;

                                let mut engine = PlaybackEngine::new_alsa_dop(stream, true);
                                if let Err(e) = engine.append_dop(Box::new(DsdErrorReport::new(
                                    native_src,
                                    thread_state.clone(),
                                ))) {
                                    log::error!("Native DSD: append failed: {}", e);
                                    thread_state.set_stream_error(true);
                                    return;
                                }
                                *current_engine = Some(engine);
                                thread_state.set_loaded_audio(true);
                                thread_state.set_stream_error(false);
                                thread_state.set_stream_quality(info.dsd_rate, 1);
                                thread_state.duration.store(duration, Ordering::SeqCst);
                                thread_state.set_dsd_mode(if little_endian { 3 } else { 2 });
                                thread_state.is_playing.store(true, Ordering::SeqCst);
                                thread_state.position.store(0, Ordering::SeqCst);
                                thread_state
                                    .current_track_id
                                    .store(track_id, Ordering::SeqCst);
                                thread_state.start_playback_timer(0);
                                log::info!("Audio thread: native DSD playback STARTED");
                            }
                            #[cfg(not(target_os = "linux"))]
                            {
                                let _ = (path, track_id);
                                log::error!("Native DSD playback is Linux-only");
                                thread_state.set_stream_error(true);
                            }
                        }
                        AudioCommand::PlayNextDsdDop { path, track_id } => {
                            #[cfg(target_os = "linux")]
                            {
                                let Some(engine) = current_engine.as_mut() else {
                                    thread_state.set_gapless_ready(false);
                                    return;
                                };
                                if !engine.is_dop() {
                                    log::info!("Gapless DoP: engine is not DoP, ignoring");
                                    thread_state.set_gapless_ready(false);
                                    return;
                                }
                                // Build the packing matching the ACTIVE
                                // direct mode (1 = DoP, 2/3 = native BE/LE).
                                let mode = thread_state.dsd_mode();
                                let built: Result<
                                    (Box<dyn Iterator<Item = i32> + Send>, u32, u64),
                                    String,
                                > = qbz_dsd::open_dsd(&path)
                                    .map_err(|e| e.to_string())
                                    .and_then(|d| match mode {
                                        1 => qbz_dsd::DopStream::new(d)
                                            .map_err(|e| e.to_string())
                                            .map(|st| {
                                                let rate = st.carrier_rate();
                                                let frames = st.total_frames();
                                                (
                                                    Box::new(DsdErrorReport::new(
                                                        st,
                                                        thread_state.clone(),
                                                    ))
                                                        as Box<
                                                            dyn Iterator<Item = i32> + Send,
                                                        >,
                                                    rate,
                                                    frames,
                                                )
                                            }),
                                        2 | 3 => qbz_dsd::NativeDsdStream::new(d, mode == 3)
                                            .map_err(|e| e.to_string())
                                            .map(|st| {
                                                let rate = st.rate();
                                                let frames = st.total_frames();
                                                (
                                                    Box::new(DsdErrorReport::new(
                                                        st,
                                                        thread_state.clone(),
                                                    ))
                                                        as Box<
                                                            dyn Iterator<Item = i32> + Send,
                                                        >,
                                                    rate,
                                                    frames,
                                                )
                                            }),
                                        _ => Err("no DSD-direct mode active".to_string()),
                                    });
                                let (src, rate, total_frames) = match built {
                                    Ok(v) => v,
                                    Err(e) => {
                                        log::warn!("Gapless DSD: cannot open next track: {}", e);
                                        thread_state.set_gapless_ready(false);
                                        return;
                                    }
                                };
                                if *current_track_sample_rate != Some(rate) {
                                    log::info!(
                                        "Gapless DSD: rate mismatch ({:?} vs {}), ignoring",
                                        *current_track_sample_rate,
                                        rate
                                    );
                                    thread_state.set_gapless_ready(false);
                                    return;
                                }
                                let duration = total_frames / (rate.max(1) as u64);
                                match engine.append_dop(src) {
                                    Ok(()) => {
                                        // data stays empty: the DoP engine never
                                        // resumes from current_audio_data (the
                                        // pause-suspend teardown is gated off in
                                        // DoP mode).
                                        *gapless_pending = Some(GaplessPending {
                                            track_id,
                                            duration_secs: duration,
                                            audio: TrackAudio::Memory(TrackBytes::default()),
                                            normalization_gain: None,
                                            // Never re-queued: seeking is
                                            // refused outright during DoP, and
                                            // the audio here is deliberately
                                            // empty anyway.
                                            sample_rate: rate,
                                            channels: 2,
                                        });
                                        thread_state.set_gapless_next_track_id(track_id);
                                        thread_state.set_gapless_ready(false);
                                        log::info!(
                                            "Gapless DoP: queued track {} for seamless DSD transition",
                                            track_id
                                        );
                                    }
                                    Err(e) => {
                                        log::warn!("Gapless DoP: append failed: {}", e);
                                        thread_state.set_gapless_ready(false);
                                    }
                                }
                            }
                            #[cfg(not(target_os = "linux"))]
                            {
                                let _ = (path, track_id);
                                thread_state.set_gapless_ready(false);
                            }
                        }
                        AudioCommand::Pause => {
                            if let Some(ref engine) = *current_engine {
                                engine.pause();
                                thread_state.pause_playback_timer();
                                thread_state.is_playing.store(false, Ordering::SeqCst);
                                *pause_suspend_deadline = Some(
                                    Instant::now() + Duration::from_millis(PAUSE_SUSPEND_DELAY_MS),
                                );
                                log::info!(
                                    "Audio thread: paused at {}s",
                                    thread_state.position.load(Ordering::SeqCst)
                                );
                            }
                        }
                        AudioCommand::Resume => {
                            *pause_suspend_deadline = None;
                            if current_engine.is_none() {
                                // Where the rebuilt engine gets its samples.
                                // A pause longer than PAUSE_SUSPEND_DELAY_MS
                                // dropped the engine to release the device, so
                                // resuming means decoding from the start of
                                // something and landing on the paused position
                                // again.
                                enum ResumeInput {
                                    /// Full file in memory: cache hit, local
                                    /// library, or a stream that finished
                                    /// downloading.
                                    Memory(TrackBytes),
                                    /// A still-streaming source whose feeder
                                    /// serves range requests, so the resume
                                    /// position is one request away instead of
                                    /// a whole-file download away.
                                    Ranged(Arc<BufferedMediaSource>),
                                }

                                let resume_input = if let Some(ref data) = *current_audio_data {
                                    ResumeInput::Memory(data.clone())
                                } else if let Some(ref path) = *current_audio_file {
                                    // A track that arrived through a gapless
                                    // hand-off decoded from the L2 cache file and
                                    // deliberately kept no copy in RAM. Reading it
                                    // back here is a one-off on a user action, not
                                    // a resident cost.
                                    match std::fs::read(path) {
                                        Ok(bytes) => {
                                            log::info!(
                                                "Resume: re-reading {} ({} bytes) for the gapless-loaded track",
                                                path.display(),
                                                bytes.len()
                                            );
                                            ResumeInput::Memory(TrackBytes::from(bytes.as_slice()))
                                        }
                                        Err(e) => {
                                            log::warn!(
                                                "Audio thread: cannot resume - cached file {} unreadable: {e}",
                                                path.display()
                                            );
                                            return;
                                        }
                                    }
                                } else if let Some(ref streaming_src) = *current_streaming_source {
                                    if streaming_src.is_complete() {
                                        match streaming_src.take_complete_data() {
                                            Some(data) => {
                                                log::info!("Resume: using complete streaming data ({} bytes)", data.len());
                                                // Store it in current_audio_data for future use
                                                *current_audio_data = Some(data.clone());
                                                *current_audio_file = None;
                                                ResumeInput::Memory(data)
                                            }
                                            None => {
                                                log::warn!("Audio thread: cannot resume - streaming source complete but data unavailable");
                                                return;
                                            }
                                        }
                                    } else if streaming_src.supports_range_requests() {
                                        log::info!(
                                            "Resume: streaming incomplete ({} bytes buffered) but the feeder serves ranges - seeking instead of waiting",
                                            streaming_src.buffer_size()
                                        );
                                        ResumeInput::Ranged(streaming_src.clone())
                                    } else {
                                        log::warn!("Audio thread: cannot resume - streaming not complete yet ({} bytes buffered)",
                                        streaming_src.buffer_size());
                                        return;
                                    }
                                } else {
                                    log::warn!(
                                        "Audio thread: cannot resume - no audio data available"
                                    );
                                    return;
                                };

                                if stream_opt.is_none() {
                                    // Use last known sample rate/channels to maintain DAC passthrough
                                    let sr = current_track_sample_rate.unwrap_or(48000);
                                    let ch = current_track_channels.unwrap_or(2);
                                    log::info!(
                                        "Resume: reinitializing stream at {}Hz/{}ch",
                                        sr,
                                        ch
                                    );
                                    *stream_opt =
                                        init_device(current_device_name, &thread_state, sr, ch);
                                }

                                let Some(ref stream) = *stream_opt else {
                                    log::error!(
                                        "Audio thread: cannot resume - no audio device available"
                                    );
                                    return;
                                };

                                let mut engine = match stream {
                                    StreamType::Rodio { sink: mixer_sink, .. } => {
                                        match PlaybackEngine::new_rodio(&mixer_sink.mixer()) {
                                            Ok(e) => e,
                                            Err(e) => {
                                                log::error!(
                                                    "Failed to create engine for resume: {}",
                                                    e
                                                );
                                                return;
                                            }
                                        }
                                    }
                                    #[cfg(target_os = "linux")]
                                    StreamType::AlsaDirect(alsa_stream) => {
                                        let hardware_volume = thread_settings
                                            .lock()
                                            .ok()
                                            .map(|s| s.alsa_hardware_volume)
                                            .unwrap_or(false);
                                        PlaybackEngine::new_alsa_direct(
                                            alsa_stream.clone(),
                                            hardware_volume,
                                        )
                                    }
                                    #[cfg(target_os = "linux")]
                                    StreamType::Jack(jack_stream) => {
                                        PlaybackEngine::new_jack(jack_stream.clone())
                                    }
                                };

                                let volume =
                                    f32::from_bits(thread_state.volume.load(Ordering::SeqCst));
                                apply_engine_volume(&stream_opt, &engine, volume);

                                let resume_pos = thread_state.position.load(Ordering::SeqCst);
                                let skipped_source: Box<dyn Source<Item = f32> + Send> =
                                    match resume_input {
                                        ResumeInput::Memory(audio_data) => {
                                            // A Resume that rebuilds from completed streaming data
                                            // can run after a PlayStreaming that failed BEFORE
                                            // storing the duration, and only the decoded source
                                            // knows what it is (#508) — InMemorySource reports no
                                            // total_duration, so that case has to take the
                                            // decode_with_fallback path below.
                                            let needs_duration =
                                                thread_state.duration.load(Ordering::SeqCst) == 0;

                                            // Otherwise seek natively, the way the Seek handler
                                            // already does: skip_duration decodes and discards
                                            // every sample before the position, which for a 124s
                                            // offset at 96kHz is 23.8M samples and ~2.8s of Pi CPU
                                            // between "device ready" and audible.
                                            let seeked = (resume_pos > 0 && !needs_duration)
                                                .then(|| {
                                                    seek_in_memory(
                                                        &audio_data,
                                                        Duration::from_secs(resume_pos),
                                                    )
                                                })
                                                .flatten();

                                            match seeked {
                                                Some(source) => source,
                                                None => {
                                                    let source = match decode_with_fallback(
                                                        &audio_data,
                                                    ) {
                                                        Ok(s) => s,
                                                        Err(e) => {
                                                            log::error!(
                                                                "Failed to decode audio for resume: {}",
                                                                e
                                                            );
                                                            return;
                                                        }
                                                    };

                                                    // Must read total_duration() BEFORE
                                                    // skip_duration consumes `source`.
                                                    if needs_duration {
                                                        if let Some(d) = source.total_duration() {
                                                            thread_state.duration.store(
                                                                d.as_secs(),
                                                                Ordering::SeqCst,
                                                            );
                                                        }
                                                    }

                                                    if resume_pos > 0 {
                                                        let skip = Duration::from_secs(resume_pos);
                                                        Box::new(source.skip_duration(skip))
                                                    } else {
                                                        source
                                                    }
                                                }
                                            }
                                        }
                                        ResumeInput::Ranged(streaming_src) => {
                                            // Rebuild the incremental decoder over the
                                            // same buffer and let Symphonia's native
                                            // seek land on the paused position: the
                                            // FLAC seek table gives the byte offset and
                                            // the range request fetches it. No
                                            // whole-file download to wait out, and no
                                            // decode-and-discard pass over everything
                                            // before the position.
                                            let buffer = streaming_src.clone();
                                            let rebuilt =
                                                IncrementalStreamingSource::new(streaming_src);
                                            let mut source = match rebuilt {
                                                Ok(s) => s,
                                                Err(e) => {
                                                    log::error!(
                                                        "Resume: failed to rebuild the streaming decoder: {}",
                                                        e
                                                    );
                                                    return;
                                                }
                                            };
                                            if resume_pos > 0 {
                                                if let Err(e) =
                                                    source.seek_to(Duration::from_secs(resume_pos))
                                                {
                                                    log::error!(
                                                        "Resume: streaming seek to {}s failed: {}",
                                                        resume_pos,
                                                        e
                                                    );
                                                    return;
                                                }
                                            }
                                            // The seek returns as soon as the
                                            // first ranged bytes land. Give the
                                            // feeder a moment to build a real
                                            // buffer there before the engine
                                            // starts pulling, or the resume
                                            // stutters on its first samples.
                                            let refill = Instant::now();
                                            while !buffer.has_min_buffer()
                                                && buffer.download_error().is_none()
                                                && refill.elapsed() < Duration::from_secs(10)
                                            {
                                                std::thread::sleep(Duration::from_millis(20));
                                            }
                                            log::info!(
                                                "Resume: {} bytes buffered at {}s in {}ms",
                                                buffer.buffer_size(),
                                                resume_pos,
                                                refill.elapsed().as_millis()
                                            );
                                            Box::new(source)
                                        }
                                    };

                                // Wrap source with diagnostic, normalization, and visualizer
                                // Reuse the gain + atomic from the original Play
                                let skipped_source = wrap_source(
                                    skipped_source,
                                    *current_normalization_gain,
                                    current_gain_atomic.clone(),
                                    &analyzer_tx,
                                    &analyzer_enabled,
                                );
                                if let Err(e) = engine.append(skipped_source) {
                                    log::error!("Failed to append source for resume: {}", e);
                                    return;
                                }
                                thread_state.start_playback_timer(resume_pos);
                                thread_state.is_playing.store(true, Ordering::SeqCst);
                                *current_engine = Some(engine);

                                log::info!("Audio thread: resumed from {}s", resume_pos);
                                return;
                            }

                            if let Some(ref engine) = *current_engine {
                                engine.play();
                                let current_pos = thread_state.position.load(Ordering::SeqCst);
                                thread_state.start_playback_timer(current_pos);
                                thread_state.is_playing.store(true, Ordering::SeqCst);
                                log::info!("Audio thread: resumed");
                            }
                        }
                        AudioCommand::Stop => {
                            if let Some(engine) = current_engine.take() {
                                engine.stop();
                            }
                            *current_audio_data = None;
                            *current_audio_file = None;
                            *current_streaming_source = None;
                            *current_normalization_gain = None;
                            *current_gain_atomic = None;
                            *gapless_pending = None;
                            *gapless_request_armed = false;
                            thread_state.set_gapless_ready(false);
                            thread_state.set_gapless_next_track_id(0);
                            analyzer_enabled.store(false, Ordering::SeqCst);
                            thread_state.set_normalization_gain(None);
                            thread_state.is_playing.store(false, Ordering::SeqCst);
                            thread_state.position.store(0, Ordering::SeqCst);
                            thread_state.set_loaded_audio(false);
                            thread_state
                                .playback_start_millis
                                .store(0, Ordering::SeqCst);
                            thread_state.position_at_start.store(0, Ordering::SeqCst);
                            // Defer dropping the stream so a Play immediately following Stop
                            // (the frontend's track-change pattern is Stop → Play, not append)
                            // can reuse the open device. Tearing CoreAudio down between every
                            // track was producing the audible click on track change. The idle
                            // loop's pause-suspend handler (below) drops the stream when this
                            // deadline fires; Play / Resume / Seek / ReinitDevice all clear
                            // the deadline so they reuse or replace the stream as needed.
                            *pause_suspend_deadline = Some(
                                Instant::now() + Duration::from_millis(PAUSE_SUSPEND_DELAY_MS),
                            );
                            // Reset the PipeWire clock if WE forced it. The call is
                            // self-gating (reset_pipewire_clock no-ops unless QBZ set
                            // the force), so it is now unconditional: the previous
                            // pw_force_bitperfect gate missed plain (no-passthrough)
                            // PipeWire users and leaked a forced rate after stop (#263).
                            #[cfg(target_os = "linux")]
                            qbz_audio::pipewire_backend::PipeWireBackend::reset_pipewire_clock();
                            log::info!("Audio thread: stopped");
                        }
                        AudioCommand::SetVolume(volume) => {
                            thread_state
                                .volume
                                .store(volume.to_bits(), Ordering::SeqCst);
                            if let Some(ref engine) = *current_engine {
                                apply_engine_volume(&stream_opt, &engine, volume);
                            }
                            // debug: a slider drag delivers dozens of these per
                            // second — at info they dominated a field log (#555,
                            // 758 of 1025 lines) and each one is a formatted
                            // write from the AUDIO thread.
                            log::debug!("Audio thread: volume set to {}", volume);
                        }
                        AudioCommand::Seek(position_secs) => {
                            if current_engine.as_ref().map(|e| e.is_dop()).unwrap_or(false) {
                                // v1 limitation: no seek inside a DoP stream
                                // (demuxer-level seek + marker re-phase later).
                                log::info!("Seek ignored during DoP playback ({}s)", position_secs);
                                return;
                            }
                            *pause_suspend_deadline = None;
                            // A seek builds a NEW engine, so the hand-off queued
                            // into the old one is gone and this state has to be
                            // cleared or it would claim a next track the engine
                            // does not have.
                            //
                            // But seeking WITHIN a track does not change which
                            // track comes next, and its bytes are still in hand
                            // — so take them with us and re-queue them once the
                            // new engine exists, instead of throwing away work
                            // that is already done. Without this, seeking cost
                            // the transition its gapless hand-off: observed on
                            // hardware as `Starting dynamic streaming for track
                            // N` where a seamless swap should have been, and if
                            // the seek landed near the end of the track there
                            // was not enough time left to prepare it again at
                            // all.
                            //
                            // DoP is not a concern: a seek is refused above.

                            // Four cases reach this handler:
                            //   * full-file playback (current_audio_data set)
                            //   * streaming, download complete (buffered
                            //     source holds the full file)
                            //   * streaming over a range-capable feeder —
                            //     anywhere in the track is one HTTP request
                            //     away, so no watermark applies
                            //   * sequential streaming (CMAF segment assembly),
                            //     download IN PROGRESS — only allowed if the
                            //     target position falls inside the
                            //     already-buffered region. Such a feeder can
                            //     only produce bytes in order, so seeking past
                            //     the watermark would block the audio thread
                            //     waiting for the rest of the download.
                            //     Cache, offline-cache, and local-library
                            //     playback reach this handler with
                            //     current_audio_data Some and skip the
                            //     streaming branch entirely (issue #335).
                            //   * a track handed over as a FILE (the disk
                            //     gapless path) — `current_audio_file` set,
                            //     `current_audio_data` empty by design, because
                            //     the whole point was not to hold the bytes.
                            if current_audio_data.is_none()
                                && current_streaming_source.is_none()
                                && current_audio_file.is_none()
                            {
                                log::warn!("Audio thread: cannot seek - no audio data available");
                                return;
                            }
                            if let Some(ref stream_src) = *current_streaming_source {
                                if !stream_src.is_complete() {
                                    if stream_src.supports_range_requests() {
                                        log::info!(
                                            "Audio thread: seek to {}s over a range-capable stream ({} bytes held)",
                                            position_secs,
                                            stream_src.buffer_size()
                                        );
                                    } else {
                                        // Approximate bytes-to-seconds mapping via
                                        // download fraction × total duration. Exact
                                        // for CBR, close-enough for FLAC/VBR; the
                                        // 0.90 margin covers the error band so the
                                        // decoder never reads past the watermark.
                                        let duration_secs = thread_state.duration();
                                        let progress = stream_src.progress().unwrap_or(0.0);
                                        if duration_secs == 0 || progress <= 0.0 {
                                            log::warn!(
                                                "Audio thread: seek to {}s ignored — streaming progress unknown",
                                                position_secs
                                            );
                                            return;
                                        }
                                        let max_seekable_secs =
                                            (progress * 0.90 * duration_secs as f32) as u64;
                                        if position_secs > max_seekable_secs {
                                            log::warn!(
                                                "Audio thread: seek to {}s ignored — past buffered watermark ({}s, progress {:.1}%)",
                                                position_secs,
                                                max_seekable_secs,
                                                progress * 100.0
                                            );
                                            return;
                                        }
                                        log::info!(
                                            "Audio thread: seek to {}s within buffered zone (watermark {}s, progress {:.1}%)",
                                            position_secs,
                                            max_seekable_secs,
                                            progress * 100.0
                                        );
                                    }
                                }
                            }

                            let Some(ref stream) = *stream_opt else {
                                log::error!(
                                    "Audio thread: cannot seek - no audio device available"
                                );
                                return;
                            };

                            log::info!("Audio thread: seeking to {}s", position_secs);

                            // ONLY NOW, past every early return above. This used
                            // to run before them, so a REFUSED seek -- past the
                            // buffered watermark, unknown progress, no audio for
                            // the shape we were given -- still threw the prepared
                            // next track away and returned without re-queueing it.
                            // Worse, it left `gapless_request_armed` set, so the
                            // driver never re-armed either: the hand-off was gone
                            // for good and the transition drained. Observed as a
                            // gap several minutes after a handful of rejected
                            // seeks.
                            //
                            // The clear itself is still required: a seek builds a
                            // NEW engine, so the hand-off queued into the old one
                            // is gone and claiming otherwise would stall the
                            // transition. But seeking WITHIN a track does not
                            // change which track comes next, and its bytes are
                            // still in hand, so they come with us and are
                            // re-queued once the new engine exists.
                            //
                            // ARMED stays set while that re-queue is in flight:
                            // the re-issued PlayNext is delivered asynchronously,
                            // and until it lands, "no pending, not armed, ready
                            // false, next id 0" is exactly what the prefetch check
                            // looks for -- a tick there would start a SECOND
                            // prefetch, and PlayNext appends unconditionally, so
                            // two sources would be queued with only the later one
                            // described. The transition clears the flag, as it
                            // does for any normal arm.
                            let requeue_gapless = gapless_pending.as_ref().map(|pending| {
                                (
                                    pending.audio.clone(),
                                    pending.track_id,
                                    pending.sample_rate,
                                    pending.channels,
                                )
                            });
                            *gapless_pending = None;
                            *gapless_request_armed = requeue_gapless.is_some();
                            thread_state.set_gapless_ready(false);
                            thread_state.set_gapless_next_track_id(0);

                            // After take(), every failure path must clear playing
                            // state — otherwise UI can show "playing" with no engine.
                            if let Some(engine) = current_engine.take() {
                                engine.stop();
                            }
                            let seek_abort = |thread_state: &SharedState, why: &str| {
                                log::error!("Audio thread: seek aborted: {why}");
                                thread_state.is_playing.store(false, Ordering::SeqCst);
                                thread_state.set_stream_error(true);
                            };

                            let mut engine = match stream {
                                StreamType::Rodio { sink: mixer_sink, .. } => {
                                    match PlaybackEngine::new_rodio(&mixer_sink.mixer()) {
                                        Ok(e) => e,
                                        Err(e) => {
                                            seek_abort(
                                                &thread_state,
                                                &format!("rodio engine create failed: {e}"),
                                            );
                                            return;
                                        }
                                    }
                                }
                                #[cfg(target_os = "linux")]
                                StreamType::AlsaDirect(alsa_stream) => {
                                    let hardware_volume = thread_settings
                                        .lock()
                                        .ok()
                                        .map(|s| s.alsa_hardware_volume)
                                        .unwrap_or(false);
                                    PlaybackEngine::new_alsa_direct(
                                        alsa_stream.clone(),
                                        hardware_volume,
                                    )
                                }
                                #[cfg(target_os = "linux")]
                                StreamType::Jack(jack_stream) => {
                                    PlaybackEngine::new_jack(jack_stream.clone())
                                }
                            };

                            let volume = f32::from_bits(thread_state.volume.load(Ordering::SeqCst));
                            apply_engine_volume(&stream_opt, &engine, volume);

                            // Build the decoded source for the seek. Both
                            // streaming and cached paths use Symphonia's native
                            // seek — FLAC seek table / MP3 TOC jumps straight
                            // to the target byte, then decodes forward to the
                            // exact sample. Avoids skip_duration's
                            // decode-every-sample-from-zero loop, which stalls
                            // the audio thread on long seeks (especially FLAC
                            // Hi-Res). Cached path falls back to decode_with_
                            // fallback + skip_duration if Symphonia can't
                            // probe the format (e.g., rodio-only MP4/AAC),
                            // preserving existing behavior for those cases.
                            let skip_duration = Duration::from_secs(position_secs);
                            let skipped_source: Box<dyn Source<Item = f32> + Send> = if let Some(
                                ref stream_src,
                            ) =
                                *current_streaming_source
                            {
                                match IncrementalStreamingSource::new(stream_src.clone()) {
                                    Ok(mut s) => {
                                        if let Err(e) = s.seek_to(skip_duration) {
                                            seek_abort(
                                                &thread_state,
                                                &format!("streaming native seek failed: {e}"),
                                            );
                                            return;
                                        }
                                        Box::new(s)
                                    }
                                    Err(e) => {
                                        seek_abort(
                                            &thread_state,
                                            &format!("streaming source for seek failed: {e}"),
                                        );
                                        return;
                                    }
                                }
                            } else if let Some(path) = current_audio_file.as_ref() {
                                // Disk gapless hand-off: the bytes are a file, not
                                // a buffer. Before this, the guard above refused
                                // the seek outright ("cannot seek - no audio data")
                                // because it only knew about the in-memory and
                                // streaming cases — the file case postdates it.
                                //
                                // Symphonia over the file first. rodio's generic
                                // `try_seek` also "succeeds" here, but with no
                                // SEEKTABLE (and Qobuz FLACs carry none) it decodes
                                // forward to the target: 7.4 s measured to reach
                                // 676 s of a 96 kHz track on a Pi, against ~200 ms
                                // for the same seek on the streaming path.
                                // Symphonia bisects the file instead.
                                let native = match InMemorySource::from_file(path) {
                                    Ok(mut s) => match s.seek_to(skip_duration) {
                                        Ok(()) => Some(
                                            Box::new(s) as Box<dyn Source<Item = f32> + Send>
                                        ),
                                        Err(e) => {
                                            log::warn!(
                                                "Native seek on cached file failed ({e}); falling back to a decoding seek"
                                            );
                                            None
                                        }
                                    },
                                    Err(e) => {
                                        log::warn!(
                                            "Symphonia could not open the cached file ({e}); falling back to a decoding seek"
                                        );
                                        None
                                    }
                                };
                                match native {
                                    Some(src) => src,
                                    // rodio's decoders cover a few formats
                                    // Symphonia cannot probe (MP4/AAC), so keep the
                                    // slow path rather than refusing the seek.
                                    None => match decode_file_with_fallback(path) {
                                        Ok(mut src) => match src.try_seek(skip_duration) {
                                            Ok(()) => src,
                                            Err(_) => match decode_file_with_fallback(path) {
                                                Ok(fb) => Box::new(fb.skip_duration(skip_duration)),
                                                Err(e) => {
                                                    seek_abort(
                                                        &thread_state,
                                                        &format!("decode of cached file for seek failed: {e}"),
                                                    );
                                                    return;
                                                }
                                            },
                                        },
                                        Err(e) => {
                                            seek_abort(
                                                &thread_state,
                                                &format!("open of cached file for seek failed: {e}"),
                                            );
                                            return;
                                        }
                                    },
                                }
                            } else {
                                let audio_data = current_audio_data
                                    .as_ref()
                                    .expect("current_audio_data was checked Some above");
                                match InMemorySource::new(audio_data.clone()) {
                                    Ok(mut s) => match s.seek_to(skip_duration) {
                                        Ok(()) => Box::new(s),
                                        Err(e) => {
                                            log::warn!(
                                                    "Native seek on cached source failed ({}); falling back to skip_duration",
                                                    e
                                                );
                                            match decode_with_fallback(audio_data) {
                                                Ok(fb) => Box::new(fb.skip_duration(skip_duration)),
                                                Err(e) => {
                                                    seek_abort(
                                                        &thread_state,
                                                        &format!(
                                                            "decode for seek failed: {e}"
                                                        ),
                                                    );
                                                    return;
                                                }
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        log::warn!(
                                                "InMemorySource probe failed ({}); falling back to skip_duration",
                                                e
                                            );
                                        match decode_with_fallback(audio_data) {
                                            Ok(fb) => Box::new(fb.skip_duration(skip_duration)),
                                            Err(e) => {
                                                seek_abort(
                                                    &thread_state,
                                                    &format!(
                                                        "decode for seek failed: {e}"
                                                    ),
                                                );
                                                return;
                                            }
                                        }
                                    }
                                }
                            };

                            // Send Reset to analyzer (seek invalidates accumulated samples)
                            let _ = analyzer_tx.try_send(AnalyzerMessage::Reset);

                            // Wrap source with diagnostic, normalization, and visualizer
                            // Reuse the gain + atomic from the current track
                            let skipped_source = wrap_source(
                                skipped_source,
                                *current_normalization_gain,
                                current_gain_atomic.clone(),
                                &analyzer_tx,
                                &analyzer_enabled,
                            );
                            if let Err(e) = engine.append(skipped_source) {
                                seek_abort(
                                    &thread_state,
                                    &format!("append source for seek failed: {e}"),
                                );
                                return;
                            }

                            let was_playing = thread_state.is_playing.load(Ordering::SeqCst);
                            if !was_playing {
                                engine.pause();
                            }

                            thread_state.position.store(position_secs, Ordering::SeqCst);
                            if was_playing {
                                thread_state.start_playback_timer(position_secs);
                            }

                            *current_engine = Some(engine);
                            thread_state.set_stream_error(false);
                            log::info!(
                                "Audio thread: seeked to {}s (was_playing: {})",
                                position_secs,
                                was_playing
                            );

                            // Hand the prepared next track back, now that there
                            // is an engine to hand it to. Re-issuing the command
                            // rather than inlining the work keeps decode,
                            // ReplayGain, the format check and the append in the
                            // single handler that owns them — and that handler
                            // re-validates everything, so a re-queue that is no
                            // longer appropriate is refused exactly as a fresh
                            // one would be.
                            if let Some((audio, track_id, sample_rate, channels)) = requeue_gapless
                            {
                                log::info!(
                                    "Gapless: re-queueing track {track_id} after the seek rebuilt the engine"
                                );
                                if let Err(e) = self_tx.send(AudioCommand::PlayNext {
                                    audio,
                                    track_id,
                                    sample_rate,
                                    channels,
                                }) {
                                    // Not fatal: the driver re-arms the prefetch
                                    // on its own, which is what used to happen
                                    // every time.
                                    log::warn!("Gapless: could not re-queue after seek: {e}");
                                }
                            }
                        }
                        AudioCommand::ReinitDevice {
                            device_name: new_device,
                        } => {
                            log::info!(
                                "Audio thread: reinitializing device (new: {:?})",
                                new_device
                            );
                            *pause_suspend_deadline = None;

                            if let Some(engine) = current_engine.take() {
                                engine.stop();
                            }

                            drop(stream_opt.take());
                            log::info!("Audio thread: previous stream dropped, device released");

                            std::thread::sleep(Duration::from_millis(100));

                            *current_device_name = new_device;
                            // Use last known sample rate/channels to maintain DAC passthrough
                            let sr = current_track_sample_rate.unwrap_or(48000);
                            let ch = current_track_channels.unwrap_or(2);
                            log::info!("ReinitDevice: reinitializing at {}Hz/{}ch", sr, ch);
                            *stream_opt = init_device(current_device_name, &thread_state, sr, ch);

                            if stream_opt.is_some() {
                                log::info!("Audio thread: device reinitialized successfully");
                                *consecutive_sink_failures = 0;
                            } else {
                                log::error!("Audio thread: failed to reinitialize device");
                            }

                            // Preserve position so Resume can seek back to it.
                            // pause_playback_timer() captures the real-time position
                            // into thread_state.position before clearing the timer.
                            thread_state.pause_playback_timer();
                            thread_state.is_playing.store(false, Ordering::SeqCst);
                            // Keep current_audio_data and current_streaming_source
                            // intact so Resume can recreate the engine and seek.
                        }
                        AudioCommand::ReleaseDevice => {
                            log::info!("Audio thread: releasing output device (user-requested)");
                            // Cancel any deferred drop and tear the stream down NOW so
                            // the device is freed immediately (no warm-stream lingering).
                            *pause_suspend_deadline = None;
                            if let Some(engine) = current_engine.take() {
                                engine.stop();
                            }
                            drop(stream_opt.take());
                            // Undo anything QBZ parked so PipeWire / WirePlumber can
                            // reclaim the device (e.g. a DAC left invisible to other
                            // apps after bit-perfect ALSA Direct held it exclusively).
                            // Both calls are self-gating no-ops if QBZ didn't set them.
                            #[cfg(target_os = "linux")]
                            {
                                qbz_audio::alsa_backend::resume_suspended_sink();
                                qbz_audio::pipewire_backend::PipeWireBackend::reset_pipewire_clock();
                            }
                            thread_state.pause_playback_timer();
                            thread_state.is_playing.store(false, Ordering::SeqCst);
                            // Keep current_audio_data / current_streaming_source intact
                            // so a later Play / Resume reopens and continues.
                            log::info!("Audio thread: output device released");
                        }
                        AudioCommand::PlayNext {
                            audio,
                            track_id,
                            sample_rate,
                            channels,
                        } => {
                            // Gapless: append next track to existing Rodio Sink
                            let engine = match current_engine.as_mut() {
                                Some(e) => e,
                                None => {
                                    log::warn!(
                                        "Gapless: no engine, ignoring PlayNext for track {}",
                                        track_id
                                    );
                                    thread_state.set_gapless_ready(false);
                                    return;
                                }
                            };

                            // Verify format compatibility (same sample rate and channels)
                            if let (Some(cur_sr), Some(cur_ch)) =
                                (*current_track_sample_rate, *current_track_channels)
                            {
                                if sample_rate != cur_sr || channels != cur_ch {
                                    log::info!(
                                    "Gapless: format mismatch (current {}Hz/{}ch vs next {}Hz/{}ch), ignoring PlayNext for track {}",
                                    cur_sr, cur_ch, sample_rate, channels, track_id
                                );
                                    thread_state.set_gapless_ready(false);
                                    return;
                                }
                            }

                            // Don't queue while the current track is still
                            // DOWNLOADING — its tail is not in memory, so there is
                            // nothing to hand the writer thread after it.
                            //
                            // A COMPLETE source is fine: its buffer holds the whole
                            // track, which is precisely what promotion used to copy
                            // out before clearing it. On a low-memory host promotion
                            // is skipped (the copy is what puts a 1 GB Pi in swap),
                            // so this is the only path left to gapless there —
                            // refusing it outright silently traded gapless for
                            // memory. The transition clears the source, freeing the
                            // finished track's buffer.
                            if current_streaming_source
                                .as_ref()
                                .is_some_and(|source| !source.is_complete())
                            {
                                log::info!("Gapless: streaming source still downloading, ignoring PlayNext for track {}", track_id);
                                thread_state.set_gapless_ready(false);
                                return;
                            }

                            // Decode the next track's audio — from memory, or
                            // straight off the L2 cache file when the prefetch
                            // left it there (see `TrackAudio`). `tag_bytes` is
                            // whatever ReplayGain can be read from: the whole
                            // track in the memory case, the head of the file
                            // otherwise (FLAC tags live at the start).
                            let (decoded, tag_bytes) = match &audio {
                                TrackAudio::Memory(data) => {
                                    (decode_with_fallback(data), Some(data.clone()))
                                }
                                TrackAudio::File(path) => {
                                    (decode_file_with_fallback(path), read_tag_head(path))
                                }
                            };
                            let source = match decoded {
                                Ok(s) => s,
                                Err(e) => {
                                    log::error!(
                                        "Gapless: failed to decode track {}: {}",
                                        track_id,
                                        e
                                    );
                                    thread_state.set_gapless_ready(false);
                                    return;
                                }
                            };

                            let actual_duration =
                                source.total_duration().map(|d| d.as_secs()).unwrap_or(0);

                            // Calculate normalization for the next track
                            let norm_settings = thread_settings
                                .lock()
                                .ok()
                                .filter(|s| s.normalization_enabled)
                                .map(|s| s.normalization_target_lufs);

                            let (normalization, gain_atomic) =
                                if let Some(target_lufs) = norm_settings {
                                    let rg_gain = tag_bytes
                                        .as_ref()
                                        .and_then(|bytes| extract_replaygain(bytes))
                                        .map(|rg| calculate_gain_factor(&rg, target_lufs));
                                    let atomic =
                                        Arc::new(AtomicU32::new(rg_gain.unwrap_or(1.0).to_bits()));
                                    if let Some(cached) = loudness_cache.get(track_id) {
                                        let cached_gain = db_to_linear(cached.gain_db.min(6.0));
                                        atomic.store(cached_gain.to_bits(), Ordering::Relaxed);
                                    }
                                    let _ = analyzer_tx.try_send(AnalyzerMessage::NewTrack {
                                        track_id,
                                        sample_rate,
                                        channels,
                                        target_lufs,
                                        gain_atomic: atomic.clone(),
                                    });
                                    (rg_gain, Some(atomic))
                                } else {
                                    (None, None)
                                };

                            // Wrap source with normalization/visualizer pipeline
                            let source = wrap_source(
                                source,
                                normalization,
                                gain_atomic,
                                &analyzer_tx,
                                &analyzer_enabled,
                            );

                            // Append to existing Sink (gapless queue).
                            // Late-gapless race: this append can arrive AFTER
                            // the engine went empty at natural end ("track
                            // finished" already set thread_state.is_playing =
                            // false). The engine's own append re-opens its
                            // writer gate and the track PLAYS, but the
                            // player-level flag stays false — freezing the
                            // position monitor, the next gapless arm, the
                            // QConnect reports and the queue cursor on the
                            // finished track while the next one plays (the
                            // "invisible track" desync). Capture emptiness
                            // BEFORE the append: a user-paused engine still
                            // holds its current source (NOT empty), so a
                            // paused session is never resumed by this.
                            let engine_was_empty = engine.empty();
                            if let Err(e) = engine.append(source) {
                                log::error!(
                                    "Gapless: failed to append track {} to engine: {}",
                                    track_id,
                                    e
                                );
                                thread_state.set_gapless_ready(false);
                                return;
                            }
                            if engine_was_empty
                                && !thread_state.is_playing.load(Ordering::SeqCst)
                            {
                                log::info!(
                                    "Gapless: PlayNext landed after track finished — resuming playback-state tracking"
                                );
                                thread_state.is_playing.store(true, Ordering::SeqCst);
                            }

                            // Store pending gapless data for transition detection
                            log::info!(
                                "Gapless: track {track_id} queued from {}",
                                audio.describe()
                            );
                            *gapless_pending = Some(GaplessPending {
                                track_id,
                                duration_secs: actual_duration,
                                audio,
                                normalization_gain: normalization,
                                sample_rate,
                                channels,
                            });
                            thread_state.set_gapless_next_track_id(track_id);
                            thread_state.set_gapless_ready(false); // Request fulfilled

                            log::info!(
                                "Gapless: queued track {} (duration: {}s) for seamless transition",
                                track_id,
                                actual_duration
                            );
                        }
                    }
                };

            loop {
                if thread_state.is_playing.load(Ordering::SeqCst) {
                    match rx.recv_timeout(Duration::from_millis(100)) {
                        Ok(command) => handle_command(
                            command,
                            &mut current_engine,
                            &mut current_audio_data,
                            &mut current_audio_file,
                            &mut current_streaming_source,
                            &mut stream_opt,
                            &mut current_device_name,
                            &mut consecutive_sink_failures,
                            &mut pause_suspend_deadline,
                            &mut current_track_sample_rate,
                            &mut current_track_channels,
                            &mut current_normalization_gain,
                            &mut current_gain_atomic,
                            &mut gapless_pending,
                            &mut gapless_request_armed,
                        ),
                        Err(RecvTimeoutError::Timeout) => {
                            let now = Instant::now();
                            if now.duration_since(last_empty_check) >= Duration::from_millis(500) {
                                last_empty_check = now;

                                // Update streaming buffer progress for UI seekbar
                                if let Some(streaming_src) = current_streaming_source.as_ref() {
                                    let progress = streaming_src.progress().unwrap_or(1.0);
                                    thread_state.set_buffer_progress(progress);
                                } else {
                                    thread_state.set_buffer_progress(0.0);
                                }

                                // Streaming -> cached promotion:
                                // once streaming download completes, persist full data and clear streaming marker.
                                // This unlocks normal gapless pre-queue for the current track's tail.
                                let mut clear_streaming_source = false;
                                if let Some(streaming_src) = current_streaming_source.as_ref() {
                                    if streaming_src.is_complete() {
                                        // Promotion copies the whole track so the
                                        // streaming buffer can be dropped. That is a
                                        // fair trade on a desktop and a fatal one on a
                                        // 1 GB Pi: for the seconds it takes, BOTH
                                        // copies of a 200 MB+ Hi-Res track are resident.
                                        // The buffer we already hold serves resume and
                                        // seek perfectly well, so on a low-memory host
                                        // we keep it and skip the copy entirely.
                                        let profile =
                                            qbz_models::system_capabilities::memory_profile();
                                        let promote = profile.class
                                            != qbz_models::system_capabilities::MemoryClass::LowMemory;
                                        if promote {
                                            if current_audio_data.is_none() {
                                                if let Some(full_data) =
                                                    streaming_src.take_complete_data()
                                                {
                                                    log::info!(
                                                        "Streaming promotion: full track buffered ({} bytes), enabling cached transition path",
                                                        full_data.len()
                                                    );
                                                    current_audio_data = Some(full_data);
                                                }
                                            }
                                            clear_streaming_source = true;
                                        } else {
                                            log::debug!(
                                                "Streaming promotion skipped ({} bytes): low-memory host keeps the buffer instead of copying it",
                                                streaming_src.buffer_size()
                                            );
                                        }
                                    }
                                }
                                if clear_streaming_source {
                                    current_streaming_source = None;
                                }

                                let pos = thread_state.current_position();
                                let dur = thread_state.duration.load(Ordering::SeqCst);

                                // Track whether a gapless transition fired in this iteration
                                // so the "approaching end" check below can skip itself.
                                // Without this, the stale pos/dur snapshot above would arm
                                // gapless_request_armed=true for the new track immediately
                                // (because pos/dur still point at the outgoing track at the
                                // moment of swap), and the flag never resets during the new
                                // track's playback — so the real "approaching end" trigger
                                // for the new track never fires and gapless playback stalls
                                // out at engine-empty.
                                let mut transition_consumed_pending = false;

                                // Gapless transition detection: when position exceeds current
                                // track duration, the queued next track has started playing.
                                //
                                // ORDERING NOTE: the polling loop in lib.rs reads `track_id`,
                                // `gapless_next_track_id`, and other fields as separate atomic
                                // loads, so it can observe an inconsistent intermediate state
                                // if these stores aren't ordered carefully. The frontend's
                                // `isGaplessTransition` predicate requires
                                // `event.gapless_next_track_id === 0` AND
                                // `event.track_id !== currentTrack.id`. If the polling loop
                                // reads `track_id` post-swap but `gapless_next_track_id`
                                // pre-reset, both conditions can't be satisfied simultaneously
                                // and the frontend mis-classifies the gapless transition as an
                                // external track change — leaving the UI stuck on the previous
                                // title while the audio plays the new track.
                                //
                                // To eliminate that race, clear the "transition complete"
                                // markers (`gapless_next_track_id`, `gapless_ready`) BEFORE
                                // mutating `track_id`. Any racing reader either sees the old
                                // track_id with cleared slots (no transition observed yet, will
                                // catch up on next tick) or the new track_id with cleared slots
                                // (clean gapless transition), but never the inconsistent
                                // mid-swap mix.
                                // ONE transition, whichever signal reports it.
                                //
                                // There were two copies of this swap: a
                                // position-based one (`pos >= dur`) and an
                                // ALSA-direct one keyed on the writer thread's
                                // atomic flag. They had drifted -- only the
                                // ALSA copy released the finished track's
                                // streaming buffer -- and the position copy runs
                                // FIRST, so on this hardware it consumed the
                                // pending and the ALSA copy found nothing to do.
                                // The buffer was never released on the very
                                // hosts that skip promotion to avoid holding it:
                                // a Hi-Res track is 120-220 MB, kept until some
                                // later non-gapless load replaced it.
                                //
                                // The flag is ALWAYS drained, pending or not, so
                                // a stale one cannot fire later against a
                                // different track.
                                let engine_signalled = current_engine
                                    .as_ref()
                                    .is_some_and(|engine| engine.take_source_transition());
                                let position_signalled = dur > 0 && pos >= dur;

                                // `if let` rather than a checked `expect`: this
                                // runs on the audio thread, where a panic is
                                // silence with no recovery, and the guard buys
                                // nothing the pattern match does not.
                                let transition = if position_signalled || engine_signalled {
                                    gapless_pending.take()
                                } else {
                                    None
                                };
                                if let Some(pending) = transition {
                                    if position_signalled {
                                        log::info!(
                                            "Gapless transition: track {} -> {} (pos {}s >= dur {}s)",
                                            thread_state.current_track_id.load(Ordering::SeqCst),
                                            pending.track_id,
                                            pos,
                                            dur
                                        );
                                    } else {
                                        log::info!(
                                            "ALSA Direct gapless transition: track {} -> {}",
                                            thread_state.current_track_id.load(Ordering::SeqCst),
                                            pending.track_id
                                        );
                                    }

                                    // ORDERING NOTE: the polling loop in lib.rs reads
                                    // `track_id`, `gapless_next_track_id` and the rest as
                                    // separate atomic loads, so it can observe an inconsistent
                                    // intermediate state if these stores are not ordered
                                    // carefully. The frontend's `isGaplessTransition` predicate
                                    // requires `gapless_next_track_id === 0` AND
                                    // `track_id !== currentTrack.id`; if the loop read
                                    // `track_id` post-swap but `gapless_next_track_id`
                                    // pre-reset, neither could hold and the transition was
                                    // mis-read as an external track change, leaving the UI on
                                    // the previous title while the audio played the new one.
                                    //
                                    // So clear the "transition complete" markers BEFORE
                                    // mutating `track_id`. A racing reader then sees either the
                                    // old id with cleared slots (catches up next tick) or the
                                    // new id with cleared slots (a clean transition), never the
                                    // mid-swap mix.
                                    thread_state.set_gapless_next_track_id(0);
                                    thread_state.set_gapless_ready(false);
                                    thread_state
                                        .current_track_id
                                        .store(pending.track_id, Ordering::SeqCst);
                                    thread_state
                                        .duration
                                        .store(pending.duration_secs, Ordering::SeqCst);
                                    thread_state.start_playback_timer(0);
                                    match &pending.audio {
                                        TrackAudio::Memory(data) => {
                                            current_audio_data = Some(data.clone());
                                            current_audio_file = None;
                                        }
                                        TrackAudio::File(path) => {
                                            // Nothing to hold: the decoder is reading
                                            // this file, and Resume re-opens it.
                                            current_audio_data = None;
                                            current_audio_file = Some(path.clone());
                                        }
                                    }
                                    current_normalization_gain = pending.normalization_gain;
                                    thread_state.set_normalization_gain(pending.normalization_gain);
                                    gapless_request_armed = false;
                                    transition_consumed_pending = true;
                                    // The track that just ended owned this buffer; the new one
                                    // plays from `current_audio_data` / `current_audio_file`.
                                    // Dropping it frees a finished Hi-Res track on a host where
                                    // promotion was skipped, and leaves the next PlayNext an
                                    // empty slot rather than a stale one.
                                    current_streaming_source = None;
                                }

                                // Gapless readiness: signal the frontend that it
                                // is time to prepare the next track.
                                //
                                // The trigger is THIS track being fully
                                // buffered, not a countdown to its end.
                                //
                                // It used to be a fixed lead: 5s, then 10s once
                                // an offline-cache HiRes decrypt was found to
                                // need ~6s on a CPU without AES-NI. But a fixed
                                // number is the wrong shape for a job whose cost
                                // scales with the track. Measured on a Pi 3B
                                // over WiFi at ~2.8 MB/s: a 22 MB track took
                                // 8.1s to fetch plus 1.8s to stage on the card,
                                // and missed a 10s lead by 0.85s; an 80 MB track
                                // on the same playlist missed it by 15s. Hi-Res
                                // is 21 MB/min, so a 10-minute movement (~210 MB)
                                // could never have made a 10s window on any Pi.
                                // The lead did not fail at the margin — it never
                                // won.
                                //
                                // Arming on completion instead costs no PEAK
                                // memory: either way two whole tracks are
                                // resident at the hand-off, and the lead only
                                // decides how LONG that peak is held, not how
                                // high it is. On a low-memory host it is cheaper
                                // still, because the prefetch is staged to disk
                                // and dropped from L1 immediately. What it does
                                // cost is a wasted download when the listener
                                // jumps somewhere else in the queue — a plain
                                // "next" is not waste, it plays exactly the
                                // track just fetched.
                                //
                                // `is_complete()` on the current stream is the
                                // signal, and it was already required here: a
                                // stream still DOWNLOADING has no tail to hand
                                // over, a COMPLETE one does. On a low-memory
                                // host promotion is skipped precisely so the
                                // buffer is not copied, so the slot is never
                                // cleared and `is_none()` would never fire.
                                // Same rule as the PlayNext guard.
                                // The host floor is not a preference and is not
                                // overridable by the setting: a prefetch is a
                                // whole extra track allocated in RAM, and a
                                // board that cannot afford one is not slow, it
                                // is dead. See `GAPLESS_MIN_TOTAL_KB`.
                                let gapless_enabled = thread_settings
                                    .lock()
                                    .ok()
                                    .map(|s| s.gapless_enabled)
                                    .unwrap_or(false)
                                    && qbz_models::system_capabilities::memory_profile()
                                        .allow_gapless_prefetch;
                                if should_arm_prefetch(
                                    gapless_enabled,
                                    transition_consumed_pending,
                                    dur,
                                    gapless_pending.is_some(),
                                    gapless_request_armed,
                                    thread_state.is_gapless_ready(),
                                    thread_state.get_gapless_next_track_id(),
                                    current_streaming_source
                                        .as_ref()
                                        .is_none_or(|source| source.is_complete()),
                                ) {
                                    log::info!(
                                        "Gapless: this track is buffered ({}s/{}s), fetching the next one",
                                        pos, dur
                                    );
                                    thread_state.set_gapless_ready(true);
                                    gapless_request_armed = true;
                                }

                                // Original: check if ALL sources are done (engine empty)
                                if let Some(ref engine) = current_engine {
                                    if engine.empty()
                                        && thread_state.is_playing.load(Ordering::SeqCst)
                                    {
                                        log::info!("Audio thread: track finished (engine empty)");
                                        thread_state.is_playing.store(false, Ordering::SeqCst);
                                        let duration = thread_state.duration.load(Ordering::SeqCst);
                                        thread_state.position.store(duration, Ordering::SeqCst);
                                        thread_state
                                            .playback_start_millis
                                            .store(0, Ordering::SeqCst);
                                        // Clear gapless state on track end
                                        thread_state.set_gapless_ready(false);
                                        thread_state.set_gapless_next_track_id(0);
                                        gapless_pending = None;
                                        gapless_request_armed = false;
                                    }
                                }
                            }
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            log::info!("Audio thread: channel closed, exiting");
                            break;
                        }
                    }
                } else {
                    if let Some(deadline) = pause_suspend_deadline {
                        // DoP streams are NEVER suspended on pause: the writer
                        // keeps the DAC locked in DSD mode with 0x69 silence,
                        // and there is no current_audio_data to resume from.
                        let dop_active = current_engine
                            .as_ref()
                            .map(|e| e.is_dop())
                            .unwrap_or(false);
                        if stream_opt.is_some() && !dop_active {
                            let now = Instant::now();
                            if now >= deadline {
                                if let Some(engine) = current_engine.take() {
                                    engine.stop();
                                }
                                drop(stream_opt.take());
                                pause_suspend_deadline = None;
                                // Reset the PipeWire clock if WE forced it (self-gating;
                                // see reset_pipewire_clock). Unconditional now — the
                                // previous pw_force_bitperfect gate leaked a forced rate
                                // for plain (no-passthrough) PipeWire users (#263).
                                #[cfg(target_os = "linux")]
                                qbz_audio::pipewire_backend::PipeWireBackend::reset_pipewire_clock();
                                // The exclusive device is now released (stream dropped
                                // above), so resume any PipeWire sink we suspended for
                                // exclusive access — self-gating no-op otherwise (#263).
                                #[cfg(target_os = "linux")]
                                qbz_audio::alsa_backend::resume_suspended_sink();
                                log::info!("Audio thread: suspended stream after pause");
                                continue;
                            }

                            let wait = deadline.saturating_duration_since(now);
                            let wait = std::cmp::min(wait, Duration::from_millis(250));
                            match rx.recv_timeout(wait) {
                                Ok(command) => handle_command(
                                    command,
                                    &mut current_engine,
                                    &mut current_audio_data,
                                    &mut current_audio_file,
                                    &mut current_streaming_source,
                                    &mut stream_opt,
                                    &mut current_device_name,
                                    &mut consecutive_sink_failures,
                                    &mut pause_suspend_deadline,
                                    &mut current_track_sample_rate,
                                    &mut current_track_channels,
                                    &mut current_normalization_gain,
                                    &mut current_gain_atomic,
                                    &mut gapless_pending,
                                    &mut gapless_request_armed,
                                ),
                                Err(RecvTimeoutError::Timeout) => {}
                                Err(RecvTimeoutError::Disconnected) => {
                                    log::info!("Audio thread: channel closed, exiting");
                                    break;
                                }
                            }
                            continue;
                        }
                        pause_suspend_deadline = None;
                    }

                    match rx.recv() {
                        Ok(command) => handle_command(
                            command,
                            &mut current_engine,
                            &mut current_audio_data,
                            &mut current_audio_file,
                            &mut current_streaming_source,
                            &mut stream_opt,
                            &mut current_device_name,
                            &mut consecutive_sink_failures,
                            &mut pause_suspend_deadline,
                            &mut current_track_sample_rate,
                            &mut current_track_channels,
                            &mut current_normalization_gain,
                            &mut current_gain_atomic,
                            &mut gapless_pending,
                            &mut gapless_request_armed,
                        ),
                        Err(_) => {
                            log::info!("Audio thread: channel closed, exiting");
                            break;
                        }
                    }
                }
            }
        });

        // Two-level playback cache: L1 in memory, L2 on disk (~800 MB). The
        // L1 budget comes from the host's memory profile rather than a flat
        // 400 MB: that figure is a desktop's, and on a 1 GB Pi it reserved
        // 40 % of RAM for one subsystem, which is how qbzd ended up swapping
        // to the SD card during ordinary playback. A disk-cache failure
        // degrades to L1-only rather than aborting player creation.
        // The dynamic initial-buffer sizing (`StreamingConfig::from_speed_mbps`)
        // grows to 2 MB on a slow connection. On a memory-pressured Pi that is
        // the wrong direction — a slow download there is itself a symptom of
        // swap thrash — so the host's profile caps it (256 KB on LowMemory).
        // Process-wide and idempotent; the cap is uncapped by default.
        {
            let profile = qbz_models::system_capabilities::memory_profile();
            log::info!(
                "[Player] memory profile: {:?} ({} MB RAM) — initial buffer <= {} KB, hi-res prefetch {}",
                profile.class,
                profile.mem_total_kb / 1024,
                profile.max_initial_buffer_bytes / 1024,
                if profile.allow_hires_prefetch { "allowed" } else { "not allowed" },
            );
            // Say it plainly and say why: on a board below the floor, gapless
            // silently not happening is otherwise indistinguishable from gapless
            // being broken, and the next bug report is about the wrong thing.
            if !profile.allow_gapless_prefetch {
                log::warn!(
                    "[Player] Gapless: OFF — this host has {} MB of RAM and prefetching the next \
                     track needs at least {} MB. Tracks will still follow one another, with a gap.",
                    profile.mem_total_kb / 1024,
                    qbz_models::system_capabilities::GAPLESS_MIN_TOTAL_KB / 1024,
                );
            }
            set_max_initial_buffer_bytes(profile.max_initial_buffer_bytes);
        }

        // ALSA direct-path buffer length. Read here so it applies from the next
        // stream open; 0 keeps the rate-derived default.
        if audio_settings.alsa_buffer_ms > 0 {
            log::info!(
                "[Player] ALSA buffer: {} ms (audio.alsa_buffer_ms)",
                audio_settings.alsa_buffer_ms
            );
        }
        qbz_audio::alsa_direct::set_alsa_buffer_ms(u32::from(audio_settings.alsa_buffer_ms));

        // Whether to hold the DAC's clock up with silence during gaps. Read
        // here for the same reason as the buffer above: it is a property of
        // this host, and the writer thread asks for it on every idle turn.
        if audio_settings.dac_keepalive_ms > 0 {
            log::info!(
                "[Player] DAC keep-alive: {} ms of silence during gaps (audio.dac_keepalive_ms)",
                audio_settings.dac_keepalive_ms
            );
        }
        qbz_audio::alsa_direct::set_dac_keepalive_ms(u32::from(audio_settings.dac_keepalive_ms));

        // How a volume percentage becomes an amplitude multiplier. Read once
        // here: the curve is a property of the host's configuration, and every
        // engine variant applies it at the same point.
        match qbz_audio::volume_curve::VolumeCurve::from_key(&audio_settings.volume_curve) {
            Some(curve) => {
                log::info!("[Player] volume curve: {}", curve.as_key());
                qbz_audio::volume_curve::set_volume_curve(curve);
            }
            None => {
                log::warn!(
                    "[Player] unknown audio.volume_curve '{}' — using perceptual",
                    audio_settings.volume_curve
                );
                qbz_audio::volume_curve::set_volume_curve(
                    qbz_audio::volume_curve::VolumeCurve::Perceptual,
                );
            }
        }

        let l1_max_bytes = match audio_settings.memory_cache_mb {
            0 => {
                let profile = qbz_models::system_capabilities::memory_profile();
                log::info!(
                    "[Player] L1 audio cache: {} MB (auto, from {} MB of RAM)",
                    profile.audio_cache_l1_max_bytes / (1024 * 1024),
                    profile.mem_total_kb / 1024
                );
                profile.audio_cache_l1_max_bytes
            }
            mb => {
                log::info!("[Player] L1 audio cache: {mb} MB (audio.memory_cache_mb)");
                usize::from(mb) * 1024 * 1024
            }
        };
        // L2 on disk unless the host asked to stay in memory. Caching in RAM
        // alone keeps gapless working with no card writes at all, which is worth
        // having on a player with room for the current AND next track (~450 MB
        // for a Hi-Res pair) — and is why the host, not the daemon, decides.
        let audio_cache = if !audio_settings.cache_to_disk {
            log::info!("[Player] L2 disk cache: off (audio.cache_to_disk) — nothing is written to the card");
            Arc::new(qbz_cache::AudioCache::new(l1_max_bytes))
        } else {
            match qbz_cache::PlaybackCache::new(800 * 1024 * 1024) {
                Ok(pc) => Arc::new(qbz_cache::AudioCache::with_playback_cache(
                    l1_max_bytes,
                    Arc::new(pc),
                )),
                Err(e) => {
                    log::warn!("Playback disk cache unavailable: {e}; memory cache only");
                    Arc::new(qbz_cache::AudioCache::new(l1_max_bytes))
                }
            }
        };

        Self {
            tx,
            state,
            audio_settings: settings,
            visualizer_tap,
            diagnostic,
            audio_cache,
        }
    }

    /// Start a new play intent; returns the generation token for this intent.
    /// Any earlier in-flight `play_track` whose generation no longer matches
    /// must not start audio. The monotonic counter lives in `SharedState`
    /// (bumped on every new play intent) so a slow `play_track`
    /// (network/CMAF) that finishes after a newer play cannot restart the
    /// previous track (last-writer race on `AudioCommand::Play`), and so the
    /// audio thread can abandon superseded buffer waits (#591).
    fn begin_play(&self) -> u64 {
        self.state.begin_play()
    }

    /// A passing check is a snapshot, not a lock: a newer intent can still
    /// begin between this check and the subsequent AudioCommand send. That
    /// residual window is accepted; closing it would require the generation
    /// to travel inside the audio command itself.
    fn is_current_play(&self, gen: u64) -> bool {
        self.state.is_current_play(gen)
    }

    /// Play a track by ID.
    ///
    /// First attempts the CMAF streaming pipeline (Akamai CDN, encrypted
    /// segments): only the init segment is fetched synchronously to derive
    /// stream parameters, playback starts immediately, and audio segments are
    /// fetched + decrypted + pushed to the streaming buffer in a background
    /// task. If the CMAF setup fails for any reason, falls back to the legacy
    /// `/track/getFileUrl` path (full FLAC download, then `play_data`).
    pub async fn play_track(
        &self,
        client: &QobuzClient,
        track_id: u64,
        quality: Quality,
        start_position_secs: u64,
    ) -> Result<(), String> {
        // Supersede any earlier in-flight play_track for a different intent.
        let gen = self.begin_play();
        log::info!(
            "Player: Starting playback for track {} with quality {:?} (start {}s, gen {gen})",
            track_id,
            quality,
            start_position_secs
        );

        // Cache hit: replay instantly from L1/L2 unless the cached copy is
        // a lower quality than now requested.
        if let Some(cached) = self.audio_cache.get(track_id) {
            if cached_quality_below_requested(&cached.data, quality) {
                log::info!(
                    "[CACHE] Track {} cached below requested {:?} — re-fetching",
                    track_id,
                    quality
                );
            } else {
                if !self.is_current_play(gen) {
                    log::info!(
                        "Player: cache-hit play for track {track_id} superseded (gen {gen})"
                    );
                    return Ok(());
                }
                log::info!(
                    "[CACHE HIT] Track {} ({} bytes) — playing from cache",
                    track_id,
                    cached.size_bytes
                );
                // Use apply_play_data so we do not bump generation again.
                let r = self.apply_play_data(cached.data, track_id);
                // Cached tracks play from in-memory data (no streaming resume
                // offset); honor a session-resume position with a best-effort
                // seek once playback has been handed to the audio thread.
                if r.is_ok() && start_position_secs > 0 && self.is_current_play(gen) {
                    let _ = self.seek(start_position_secs);
                }
                return r;
            }
        }

        // L1 missed. Before going to the network, ask the DISK cache — the
        // comment above has always claimed "L1/L2" but `AudioCache::get` only
        // ever looked in memory, so a track already on the card was downloaded
        // again. Observed end to end on the test Pi:
        //
        //   23:42:07  CMAF-FULL complete: 111.15 MB
        //   23:42:16  Cached track 344750247 (116547479 bytes)   <- L1
        //   23:42:28  Gapless: format mismatch, ignoring PlayNext
        //   23:42:37  Saved track 344750247 to playback cache    <- L2
        //   23:46:31  Starting dynamic streaming ... 111.15 MB   <- fetched AGAIN
        //
        // One 116 MB track nearly fills a 153 MB L1 budget, so the following
        // prefetch evicted it; the file sat on the card untouched. That cost
        // 111 MB of bandwidth on a host whose prefetches already starve the
        // audio thread, and 1.7 s of the gap between the two tracks.
        //
        // Played as a FILE, not read back into memory: the decoder streams it
        // off the card, which is both faster to first sample than loading
        // 116 MB and the only version that fits on a 1 GB host.
        if let Some(path) = self
            .audio_cache
            .get_playback_cache()
            .and_then(|l2| l2.path_if_present(track_id))
        {
            // Same quality gate as the L1 hit above, from the header alone.
            let head_ok = read_head(&path, HEAD_BYTES)
                .map(|head| !cached_quality_below_requested(&head, quality))
                .unwrap_or(false);
            if head_ok {
                if !self.is_current_play(gen) {
                    log::info!("Player: L2-hit play for track {track_id} superseded (gen {gen})");
                    return Ok(());
                }
                match self.apply_play_file(&path, track_id) {
                    Ok(()) => {
                        log::info!(
                            "[CACHE HIT] Track {} — playing from the disk cache ({})",
                            track_id,
                            path.display()
                        );
                        if start_position_secs > 0 && self.is_current_play(gen) {
                            let _ = self.seek(start_position_secs);
                        }
                        return Ok(());
                    }
                    // A file we cannot decode is not a reason to fail the play:
                    // fall through to the network exactly as before.
                    Err(e) => log::warn!(
                        "[CACHE] Track {track_id} is on disk but would not decode ({e}); streaming instead"
                    ),
                }
            } else {
                log::info!(
                    "[CACHE] Track {track_id} on disk is below the requested {quality:?} — re-fetching"
                );
            }
        }

        // `streaming_only` suppresses writing the track into the cache.
        let skip_cache = self
            .audio_settings
            .lock()
            .map(|s| s.streaming_only)
            .unwrap_or(false);

        // Try CMAF streaming pipeline first.
        // Only the init segment is fetched synchronously; audio segments
        // stream in a background task.
        log::info!("[CMAF] Attempting CMAF streaming for track {}", track_id);
        match qbz_qobuz::cmaf::setup_streaming(client, track_id, quality).await {
            Ok(cmaf_info) => {
                if !self.is_current_play(gen) {
                    log::info!(
                        "Player: CMAF setup for track {track_id} superseded (gen {gen})"
                    );
                    return Ok(());
                }
                // Derive stream parameters from init segment metadata.
                let sample_rate = cmaf_info.sampling_rate.unwrap_or(44100);
                let channels = 2u16; // FLAC from Qobuz is always stereo
                let bit_depth = cmaf_info.bit_depth.unwrap_or(16);
                let total_flac_size = cmaf_info.flac_header.len() as u64
                    + cmaf_info
                        .segment_table
                        .iter()
                        .map(|s| s.byte_len as u64)
                        .sum::<u64>();

                // Track duration from the CMAF segment table. The streaming
                // path's position timer clamps `current_position` to the
                // duration it was given, so a zero here freezes the seek bar
                // at 0:00 and blocks auto-advance — derive the real value
                // from the per-segment sample counts.
                let total_samples: u64 = cmaf_info
                    .segment_table
                    .iter()
                    .map(|s| s.sample_count as u64)
                    .sum();
                let duration_secs = if sample_rate > 0 {
                    total_samples / sample_rate as u64
                } else {
                    0
                };

                // Estimate speed from the init segment fetch (conservative:
                // assume ~10 MB/s if init was too fast to measure reliably).
                let speed_mbps = if cmaf_info.init_fetch_ms > 0 {
                    let init_bytes = cmaf_info.flac_header.len() as f64 + 4096.0; // rough init size
                    (init_bytes / (cmaf_info.init_fetch_ms as f64 / 1000.0))
                        / (1024.0 * 1024.0)
                } else {
                    10.0
                };

                log::info!(
                    "[CMAF] Streaming setup: {}Hz, {}-bit, {:.2} MB total, {:.1} MB/s est, {} segments",
                    sample_rate,
                    bit_depth,
                    total_flac_size as f64 / (1024.0 * 1024.0),
                    speed_mbps,
                    cmaf_info.n_segments
                );

                // Create the streaming buffer and start playback immediately.
                // Non-bumping variant: this intent already holds `gen`.
                let buffer_writer = self.apply_play_streaming_dynamic(
                    track_id,
                    sample_rate,
                    channels,
                    bit_depth,
                    total_flac_size,
                    speed_mbps,
                    duration_secs,
                    start_position_secs, // session-resume offset (0 = from start)
                    // CMAF frames are decrypted and concatenated in segment
                    // order, so the feeder cannot start mid-file.
                    StreamSeekMode::Sequential,
                )?;

                // Spawn the background task that fetches + decrypts + pushes
                // audio segments to the buffer.
                let url_template = cmaf_info.url_template.clone();
                let content_key = cmaf_info.content_key;
                let flac_header = cmaf_info.flac_header;
                let n_segments = cmaf_info.n_segments;
                let cache = self.audio_cache.clone();

                tokio::spawn(async move {
                    match Self::cmaf_stream_segments(
                        &url_template,
                        n_segments,
                        content_key,
                        flac_header,
                        buffer_writer,
                        track_id,
                        cache,
                        skip_cache,
                    )
                    .await
                    {
                        Ok(()) => log::info!(
                            "[CMAF-STREAM COMPLETE] Track {}",
                            track_id
                        ),
                        Err(e) => log::error!(
                            "[CMAF-STREAM ERROR] Track {}: {}",
                            track_id,
                            e
                        ),
                    }
                });

                return Ok(());
            }
            Err(e) => {
                log::warn!(
                    "[CMAF] Streaming setup failed: {}, falling back to legacy download",
                    e
                );
                // Fall through to the legacy download path.
            }
        }

        if !self.is_current_play(gen) {
            log::info!(
                "Player: legacy path for track {track_id} superseded before URL fetch (gen {gen})"
            );
            return Ok(());
        }

        // Legacy fallback: get the stream URL
        log::info!("Player: Getting stream URL...");
        let stream_url = client
            .get_stream_url_with_fallback(track_id, quality)
            .await
            .map_err(|e| {
                log::error!("Player: Failed to get stream URL: {}", e);
                format!("Failed to get stream URL: {}", e)
            })?;

        if !self.is_current_play(gen) {
            log::info!(
                "Player: legacy download for track {track_id} superseded after URL (gen {gen})"
            );
            return Ok(());
        }

        log::info!(
            "Player: Got stream URL: {} (format: {})",
            stream_url.url,
            stream_url.mime_type
        );

        // Download the audio data
        log::info!("Player: Starting audio caching...");
        let audio_data = self.download_audio(&stream_url.url).await.map_err(|e| {
            log::error!("Player: Caching failed: {}", e);
            e
        })?;
        log::info!("Player: Cached {} bytes of audio data", audio_data.len());

        if !self.is_current_play(gen) {
            log::info!(
                "Player: legacy play for track {track_id} superseded after download (gen {gen})"
            );
            return Ok(());
        }

        // Store the legacy download in the cache for instant replay. Converted
        // once here, then shared with both the cache and the audio thread.
        let audio_data: TrackBytes = audio_data.into();
        if !skip_cache {
            self.audio_cache.insert(track_id, audio_data.clone());
        }

        // Send to audio thread (do not re-bump generation)
        let r = self.apply_play_data(audio_data, track_id);
        if r.is_ok() && start_position_secs > 0 && self.is_current_play(gen) {
            let _ = self.seek(start_position_secs);
        }
        r
    }

    /// Download a track fully into the L1/L2 cache **without** starting
    /// playback.
    ///
    /// Gapless playback requires upcoming tracks to be cache hits so they
    /// play via `play_data` (fully in-memory) rather than the streaming
    /// path — the audio engine's `PlayNext` handler ignores gapless
    /// requests while a streaming source is active. This method is the
    /// prefetch primitive the controller drives for the next 1-2 queue
    /// tracks.
    ///
    /// Mirrors the Tauri V2 prefetch download: CMAF `download_full` first
    /// (Akamai CDN), legacy `/track/getFileUrl` full download as fallback.
    /// No-ops when `streaming_only` is set, when the track is already
    /// cached, or when another fetch for the same id is in flight.
    pub async fn prefetch_into_cache(
        &self,
        client: &QobuzClient,
        track_id: u64,
        quality: Quality,
    ) -> Result<(), String> {
        // Honor streaming_only — never warm the cache when the user has
        // opted out of caching.
        let skip_cache = self
            .audio_settings
            .lock()
            .map(|s| s.streaming_only)
            .unwrap_or(false);
        if skip_cache {
            log::debug!("[PREFETCH] Skipped track {track_id} — streaming_only mode active");
            return Ok(());
        }

        // Already cached, or another prefetch for this id is already
        // running — nothing to do.
        if self.audio_cache.contains(track_id) {
            log::debug!("[PREFETCH] Track {track_id} already cached");
            return Ok(());
        }
        if self.audio_cache.is_fetching(track_id) {
            log::debug!("[PREFETCH] Track {track_id} already being fetched");
            return Ok(());
        }
        // Back off a track whose last prefetch failed recently (e.g. the account
        // is being 403'd post-outage) instead of re-hammering it on every queue
        // tick — that no-backoff loop is what escalated into an edge/IP block in
        // issue #637. The client-side 403 breaker is the primary guard; this
        // just keeps us from even spinning up the task/log spam.
        const PREFETCH_FAIL_COOLDOWN: Duration = Duration::from_secs(20);
        if self
            .audio_cache
            .recently_failed(track_id, PREFETCH_FAIL_COOLDOWN)
        {
            log::debug!("[PREFETCH] Track {track_id} recently failed — backing off");
            return Ok(());
        }

        self.audio_cache.mark_fetching(track_id);
        log::info!("[PREFETCH] Prefetching track {track_id} at {quality:?}");

        // Try CMAF full download first (Akamai CDN), legacy full download
        // as fallback (nginx CDN).
        let result = match qbz_qobuz::cmaf::download_full(client, track_id, quality).await {
            Ok(data) => Ok(data),
            Err(e) => {
                log::warn!(
                    "[PREFETCH] CMAF failed for track {track_id}: {e}, trying legacy"
                );
                match client.get_stream_url_with_fallback(track_id, quality).await {
                    Ok(stream_url) => self.download_audio(&stream_url.url).await,
                    Err(e) => Err(format!("Failed to get stream URL: {e}")),
                }
            }
        };

        match result {
            Ok(data) => {
                // Brief delay before the cache write to avoid racing the
                // audio thread, matching the Tauri prefetch path.
                tokio::time::sleep(Duration::from_millis(50)).await;
                let len = data.len();
                self.audio_cache.insert(track_id, data);
                self.audio_cache.unmark_fetching(track_id);
                self.audio_cache.clear_failed(track_id);
                log::info!("[PREFETCH] Complete for track {track_id} ({len} bytes)");
                Ok(())
            }
            Err(e) => {
                self.audio_cache.unmark_fetching(track_id);
                self.audio_cache.mark_failed(track_id);
                log::warn!("[PREFETCH] Failed for track {track_id}: {e}");
                Err(e)
            }
        }
    }

    /// True if `track_id` is present in the L1/L2 playback cache. Used by
    /// the gapless controller to decide whether a track can be queued for
    /// a seamless handoff.
    pub fn is_track_cached(&self, track_id: u64) -> bool {
        self.audio_cache.contains(track_id)
    }

    /// Should a track of `bytes` be handed to the gapless path as a FILE rather
    /// than kept in memory?
    ///
    /// Yes exactly when two of them will not fit in the L1 budget — the playing
    /// track and the prefetched next one is the pair the cache exists to hold,
    /// and when it cannot hold both, holding them anyway is what took a 1 GB Pi
    /// into swap.
    ///
    /// This used to key on the host's MEMORY CLASS instead, which was wrong in
    /// both directions: a 22 MB CD track on a 1 GB player paid 1.8 s of
    /// `sync_all` to the SD card for a detour it did not need (and missed its
    /// gapless hand-off by 0.85 s doing it), while a 220 MB Hi-Res track on a
    /// 4 GB player skipped the detour and pinned a pair against the budget.
    pub fn should_hand_over_as_file(&self, bytes: usize) -> bool {
        spill_to_disk(bytes, self.audio_cache.budget_bytes())
    }

    /// Drop every cached audio byte (L1 memory + L2 disk). Called when the
    /// streaming-quality preference changes so the new tier takes effect on the
    /// next play/cast instead of on the next cache miss — the cache is keyed by
    /// track id alone and carries no quality dimension.
    pub fn clear_audio_cache(&self) {
        self.audio_cache.clear();
    }

    /// The L2 cache file holding `track_id`, when the prefetch left one there.
    ///
    /// For the gapless hand-off: with a file in hand the caller can use
    /// [`Self::play_next_file`] and skip the resident copy entirely.
    pub fn cached_track_file(&self, track_id: u64) -> Option<std::path::PathBuf> {
        self.audio_cache
            .get_playback_cache()?
            .path_if_present(track_id)
    }

    /// Put `bytes` on disk and return the file, so a caller can hand the track
    /// over as a path instead of keeping it resident.
    ///
    /// The L2 cache is normally written only as SPILL when L1 evicts something,
    /// and L1 refuses a track larger than its budget outright — so on the very
    /// host that needs this, a 217 MB Hi-Res track never reaches disk on its
    /// own and there is no file to hand over. Writing it here is what makes the
    /// disk hand-off possible at all.
    ///
    /// Returns `None` when there is no L2 cache configured, or the write did not
    /// produce a readable file; the caller then keeps the in-memory path.
    pub fn stage_track_on_disk(
        &self,
        track_id: u64,
        bytes: &[u8],
    ) -> Option<std::path::PathBuf> {
        let cache = self.audio_cache.get_playback_cache()?;
        if let Some(existing) = cache.path_if_present(track_id) {
            return Some(existing);
        }
        cache.insert(track_id, bytes);
        let path = cache.path_if_present(track_id);
        match &path {
            Some(p) => log::info!(
                "Staged track {} on disk for a gapless hand-off ({} bytes -> {})",
                track_id,
                bytes.len(),
                p.display()
            ),
            None => log::warn!(
                "Could not stage track {track_id} on disk; keeping it in memory"
            ),
        }
        path
    }

    /// Drop `track_id` from the L1 memory cache, unconditionally.
    ///
    /// Distinct from [`Self::release_finished_track`], which is about a track
    /// that has finished PLAYING and only fires on a memory-constrained host.
    /// This one is for bytes we have decided to read from disk instead: keeping
    /// the L1 copy would defeat the point.
    pub fn drop_cached_track(&self, track_id: u64) -> bool {
        self.audio_cache.release(track_id)
    }

    /// A track just finished playing — free its bytes early, but only where the
    /// memory actually matters.
    ///
    /// On a **LowMemory** host the previous track (~100 MB for Hi-Res) staying
    /// resident beside the current one and the gapless prefetch is what puts a
    /// 1 GB Pi into swap, so it goes now; it spills to the L2 disk cache, so a
    /// back-skip re-reads it from disk rather than the network.
    ///
    /// On a **Normal** host there is nothing to gain by freeing early: keeping
    /// it means an instant back-skip, and LRU already evicts it the moment
    /// something else needs the room. So this is a no-op there.
    ///
    /// Returns whether the bytes were actually released.
    pub fn release_finished_track(&self, track_id: u64) -> bool {
        let profile = qbz_models::system_capabilities::memory_profile();
        if profile.class != qbz_models::system_capabilities::MemoryClass::LowMemory {
            log::debug!(
                "Keeping finished track {} in the memory cache ({:?} host, {} MB RAM)",
                track_id,
                profile.class,
                profile.mem_total_kb / 1024
            );
            return false;
        }
        self.audio_cache.release(track_id)
    }

    /// Fetch a track's audio bytes for a gapless handoff: L1 memory →
    /// L2 disk → CMAF `download_full` (legacy full download as fallback).
    /// Does not start playback — the caller passes the bytes to
    /// `play_next`. Returns `None` only when every tier fails.
    ///
    /// Ports the L1/L2/CMAF tiers of Tauri's `v2_play_next_gapless`; the
    /// ephemeral / offline-cache / local-library tiers are intentionally
    /// omitted as they do not exist in the Slint MVP.
    pub async fn fetch_for_gapless(
        &self,
        client: &QobuzClient,
        track_id: u64,
        quality: Quality,
    ) -> Option<TrackAudio> {
        // L1: in-memory cache.
        if let Some(cached) = self.audio_cache.get(track_id) {
            log::info!(
                "[GAPLESS] Track {track_id} from MEMORY cache ({} bytes)",
                cached.size_bytes
            );
            return Some(TrackAudio::Memory(cached.data));
        }

        // L2: already on disk — hand over the PATH. Reading it back into a
        // buffer would put the whole track in RAM again, which is the cost the
        // disk copy exists to avoid.
        if let Some(playback_cache) = self.audio_cache.get_playback_cache() {
            if let Some(path) = playback_cache.path_if_present(track_id) {
                log::info!(
                    "[GAPLESS] Track {track_id} from DISK cache ({})",
                    path.display()
                );
                return Some(TrackAudio::File(path));
            }
        }

        // Not cached anywhere. Decide WHERE the download lands before it starts:
        // the segment table gives the exact size up front, so a track too big to
        // sit in memory beside the one playing is written straight to the card as
        // it decrypts. Assembling it in RAM and writing it out afterwards was a
        // whole-track `Arc` copy plus an 11-15 s fsync'd burst that underran ALSA
        // audibly, twice, measured mid-write.
        let spill_cache = self.audio_cache.get_playback_cache().cloned();
        let budget = self.audio_cache.budget_bytes();
        let choose = move |total: usize| {
            if spill_to_disk(total, budget) {
                if let Some(cache) = spill_cache.as_ref() {
                    if let Some(part) = cache.begin_write(track_id, total as u64) {
                        log::info!(
                            "[GAPLESS] Track {track_id} is {:.1} MB — streaming it to disk (L1 budget {:.1} MB)",
                            total as f64 / (1024.0 * 1024.0),
                            budget as f64 / (1024.0 * 1024.0),
                        );
                        return qbz_qobuz::TrackDestination::File(part);
                    }
                }
            }
            qbz_qobuz::TrackDestination::Memory
        };

        match qbz_qobuz::cmaf::download_full_sized(client, track_id, quality, None, choose).await {
            Ok(qbz_qobuz::DownloadedTrack::File(_)) => {
                return match self
                    .audio_cache
                    .get_playback_cache()
                    .and_then(|c| c.commit_write(track_id))
                {
                    Some(path) => Some(TrackAudio::File(path)),
                    None => {
                        log::warn!("[GAPLESS] Track {track_id} could not be published to disk");
                        None
                    }
                };
            }
            Ok(qbz_qobuz::DownloadedTrack::Memory(data)) => {
                let bytes: TrackBytes = data.into();
                log::info!(
                    "[GAPLESS] Track {track_id} downloaded for gapless ({} bytes)",
                    bytes.len()
                );
                self.audio_cache.insert(track_id, bytes.clone());
                return Some(TrackAudio::Memory(bytes));
            }
            Err(e) => {
                log::warn!("[GAPLESS] CMAF failed for track {track_id}: {e}, trying legacy");
                if let Some(cache) = self.audio_cache.get_playback_cache() {
                    cache.abort_write(track_id);
                }
            }
        }

        // Legacy fallback: a plain URL download, always in memory — CMAF failed
        // outright, so there is no segment table and no size to decide on.
        match client.get_stream_url_with_fallback(track_id, quality).await {
            Ok(stream_url) => match self.download_audio(&stream_url.url).await {
                Ok(data) => {
                    let bytes: TrackBytes = data.into();
                    log::info!(
                        "[GAPLESS] Track {track_id} downloaded for gapless ({} bytes)",
                        bytes.len()
                    );
                    self.audio_cache.insert(track_id, bytes.clone());
                    Some(TrackAudio::Memory(bytes))
                }
                Err(e) => {
                    log::warn!("[GAPLESS] Legacy download failed for {track_id}: {e}");
                    None
                }
            },
            Err(e) => {
                log::warn!("[GAPLESS] No stream URL for {track_id}: {e}");
                None
            }
        }
    }

    /// Resolve a fully-materialized audio asset for an EXTERNAL renderer
    /// (Chromecast / DLNA), carrying the bytes verbatim plus the MIME and the
    /// quality. Used by the Cast path through the local media server.
    ///
    /// Cache-first (P1, matches the fast Tauri cast path + consumes the gapless
    /// prefetch): L1 in-memory -> L2 on-disk playback cache (both decrypted
    /// FLAC) -> network. A prefetched/replayed track is served instantly; only a
    /// cold track pays the CMAF download. On a cache hit the delivered quality is
    /// not known here (no metadata stored with the bytes) — the caller derives
    /// the quality label from the track's catalog metadata; the network path
    /// returns the precise resolved tier.
    pub async fn fetch_for_external_stream(
        &self,
        client: &QobuzClient,
        track_id: u64,
        quality: Quality,
    ) -> Option<ExternalStreamAsset> {
        // L1: in-memory cache (warmed by the gapless prefetch / a prior play).
        if let Some(cached) = self.audio_cache.get(track_id) {
            log::info!(
                "[CAST-FETCH] Track {track_id} from MEMORY cache ({} bytes)",
                cached.size_bytes
            );
            return Some(ExternalStreamAsset {
                bytes: cached.data.to_vec(),
                content_type: "audio/flac".to_string(),
                quality: StreamQualityInfo::from_raw(0, None, None),
                duration_secs: None,
                origin: AssetOrigin::Cache,
            });
        }
        // L2: on-disk plain-FLAC playback cache; warm L1 on the way out.
        if let Some(playback_cache) = self.audio_cache.get_playback_cache() {
            if let Some(audio_data) = playback_cache.get(track_id) {
                log::info!(
                    "[CAST-FETCH] Track {track_id} from DISK cache ({} bytes)",
                    audio_data.len()
                );
                self.audio_cache.insert(track_id, audio_data.as_slice());
                return Some(ExternalStreamAsset {
                    bytes: audio_data,
                    content_type: "audio/flac".to_string(),
                    quality: StreamQualityInfo::from_raw(0, None, None),
                    duration_secs: None,
                    origin: AssetOrigin::Cache,
                });
            }
        }

        // Cold: CMAF full download (Akamai CDN) -> decrypted FLAC.
        match qbz_qobuz::cmaf::download_full_with_quality(client, track_id, quality).await {
            Ok((bytes, q)) => {
                log::info!(
                    "[CAST-FETCH] Track {track_id} via CMAF: {} bytes, format_id={}, {:?} kHz/{:?}-bit",
                    bytes.len(),
                    q.format_id,
                    q.sampling_rate_khz,
                    q.bit_depth
                );
                // Warm L1 so a subsequent local replay skips the network.
                self.audio_cache.insert(track_id, bytes.as_slice());
                return Some(ExternalStreamAsset {
                    bytes,
                    content_type: "audio/flac".to_string(),
                    quality: q,
                    duration_secs: None,
                    origin: AssetOrigin::Network,
                });
            }
            Err(e) => {
                log::warn!("[CAST-FETCH] CMAF failed for track {track_id}: {e}, trying legacy");
            }
        }

        // Fallback: legacy stream URL + plain HTTP download. Quality and MIME
        // come from the resolved StreamUrl (which carries the granted tier).
        match client.get_stream_url_with_fallback(track_id, quality).await {
            Ok(stream_url) => {
                let content_type =
                    external_content_type(&stream_url.mime_type, stream_url.format_id);
                let q = StreamQualityInfo::from_raw(
                    stream_url.format_id,
                    Some(stream_url.sampling_rate),
                    stream_url.bit_depth,
                );
                match self.download_audio(&stream_url.url).await {
                    Ok(bytes) => {
                        log::info!(
                            "[CAST-FETCH] Track {track_id} via legacy: {} bytes, format_id={}, ct={}",
                            bytes.len(),
                            q.format_id,
                            content_type
                        );
                        self.audio_cache.insert(track_id, bytes.as_slice());
                        Some(ExternalStreamAsset {
                            bytes,
                            content_type,
                            quality: q,
                            duration_secs: None,
                            origin: AssetOrigin::Network,
                        })
                    }
                    Err(e) => {
                        log::warn!("[CAST-FETCH] Legacy download failed for {track_id}: {e}");
                        None
                    }
                }
            }
            Err(e) => {
                log::warn!("[CAST-FETCH] No stream URL for {track_id}: {e}");
                None
            }
        }
    }

    /// Stream CMAF segments to the player's buffer, decrypting on the fly.
    ///
    /// Writes the FLAC header first so the decoder can identify the format,
    /// then fetches each audio segment, decrypts encrypted frames, and pushes
    /// the resulting FLAC frame data to the streaming buffer. The player
    /// starts playing as soon as enough data is buffered.
    async fn cmaf_stream_segments(
        url_template: &str,
        n_segments: u8,
        content_key: [u8; 16],
        flac_header: Vec<u8>,
        writer: BufferWriter,
        track_id: u64,
        cache: Arc<qbz_cache::AudioCache>,
        skip_cache: bool,
    ) -> Result<(), String> {
        struct FailGuard {
            writer: BufferWriter,
            armed: bool,
        }
        impl Drop for FailGuard {
            fn drop(&mut self) {
                if self.armed {
                    let _ = self
                        .writer
                        .error("CMAF stream aborted before completion".into());
                }
            }
        }
        let mut guard = FailGuard {
            writer,
            armed: true,
        };
        let writer = &guard.writer;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| format!("CMAF client error: {}", e))?;

        // Write the FLAC header first so the decoder can identify the format.
        if let Err(e) = writer.push_chunk(&flac_header) {
            let msg = format!("Failed to write FLAC header to buffer: {e}");
            let _ = writer.error(msg.clone());
            return Err(msg);
        }

        let mut total_written: u64 = flac_header.len() as u64;
        // Accumulate the assembled FLAC (header + decrypted frames) so the
        // finished track can be cached for instant replay. Empty when
        // `skip_cache` (streaming_only) is set.
        let mut cache_data: Vec<u8> = if skip_cache {
            Vec::new()
        } else {
            flac_header.clone()
        };
        let start = Instant::now();

        for seg_idx in 1..=n_segments {
            let seg_url = url_template.replace("$SEGMENT$", &seg_idx.to_string());
            let seg_data = client
                .get(&seg_url)
                .header("User-Agent", "Mozilla/5.0")
                .send()
                .await
                .map_err(|e| format!("CMAF segment {} fetch: {}", seg_idx, e))?
                .bytes()
                .await
                .map_err(|e| format!("CMAF segment {} read: {}", seg_idx, e))?;

            let crypto = qbz_cmaf::parse_segment_crypto(&seg_data)
                .map_err(|e| format!("CMAF segment {} parse: {}", seg_idx, e))?;

            let mut data_pos = crypto.data_offset;
            for entry in &crypto.entries {
                let frame_end = data_pos + entry.size as usize;
                if frame_end > seg_data.len() {
                    let _ = writer.error(format!("CMAF segment {} frame overflow", seg_idx));
                    return Err(format!("CMAF segment {} frame overflow", seg_idx));
                }
                let mut frame = seg_data[data_pos..frame_end].to_vec();
                if entry.flags != 0 {
                    qbz_cmaf::decrypt_frame(&content_key, &entry.iv, &mut frame);
                }
                // Write decrypted frame to the streaming buffer.
                if let Err(e) = writer.push_chunk(&frame) {
                    let msg = format!("Failed to push frame: {e}");
                    let _ = writer.error(msg.clone());
                    return Err(msg);
                }
                if !skip_cache {
                    cache_data.extend_from_slice(&frame);
                }
                total_written += frame.len() as u64;
                data_pos = frame_end;
            }

            // Trailing unencrypted data after all frame entries.
            if data_pos < crypto.mdat_end && crypto.mdat_end <= seg_data.len() {
                let trailing = &seg_data[data_pos..crypto.mdat_end];
                if let Err(e) = writer.push_chunk(trailing) {
                    let msg = format!("Failed to push trailing data: {e}");
                    let _ = writer.error(msg.clone());
                    return Err(msg);
                }
                if !skip_cache {
                    cache_data.extend_from_slice(trailing);
                }
                total_written += trailing.len() as u64;
            }

            // Progress logging every 5 segments or on the last segment.
            if seg_idx % 5 == 0 || seg_idx == n_segments {
                let elapsed = start.elapsed().as_secs_f64();
                let mbps = if elapsed > 0.0 {
                    total_written as f64 / (1024.0 * 1024.0) / elapsed
                } else {
                    0.0
                };
                // Feed the adaptive prefetch throttle with the cumulative MB/s
                // the log already reports; also the offline detector's
                // positive liveness signal (#467, #591).
                qbz_audio::network_throttle::state().record_segment_bandwidth(mbps);
                log::info!(
                    "[CMAF-STREAM] Segment {}/{} ({:.1} MB, {:.1} MB/s)",
                    seg_idx,
                    n_segments - 1,
                    total_written as f64 / (1024.0 * 1024.0),
                    mbps
                );
            }
        }

        // Signal end of stream (disarm fail-guard so Drop does not error).
        guard.armed = false;
        if let Err(e) = writer.complete() {
            log::error!("[CMAF-STREAM] Failed to mark buffer complete: {}", e);
            let _ = writer.error(format!("Failed to mark buffer complete: {e}"));
            return Err(format!("Failed to mark buffer complete: {e}"));
        }

        log::info!(
            "[CMAF-STREAM] Complete: {:.2} MB written in {:.1}s for track {}, segments fetched: 1..{}",
            total_written as f64 / (1024.0 * 1024.0),
            start.elapsed().as_secs_f64(),
            track_id,
            n_segments - 1
        );

        // Cache the assembled FLAC (header + decrypted frames) for instant
        // replay on the next play of this track.
        if !skip_cache && !cache_data.is_empty() {
            let bytes = cache_data.len();
            cache.insert(track_id, cache_data);
            log::info!("[CMAF-STREAM] Track {} cached ({} bytes)", track_id, bytes);
        }

        Ok(())
    }

    /// Play from raw audio data (for cached tracks)
    /// Play from raw audio data (for cached / offline tracks).
    ///
    /// Bumps play generation so this call supersedes any in-flight
    /// `play_track` that has not yet applied audio.
    ///
    /// Takes anything convertible into [`TrackBytes`]: a `Vec<u8>` straight off
    /// the network is converted once here, while bytes already shared with the
    /// cache are passed through without a copy.
    pub fn play_data(&self, data: impl Into<TrackBytes>, track_id: u64) -> Result<(), String> {
        let _gen = self.begin_play();
        self.apply_play_data(data, track_id)
    }

    /// Send `Play` without bumping generation (used by `play_track` after
    /// its own `begin_play` + supersede checks).
    /// Start `track_id` from a file in the L2 cache.
    ///
    /// The file twin of [`Self::apply_play_data`]: same command, same audio
    /// thread, the decoder just reads the card instead of a buffer. The format
    /// is probed here — through the very decoder that will play it — so a file
    /// we cannot read fails before the engine is torn down, leaving the caller
    /// free to fall back to the network.
    fn apply_play_file(&self, path: &std::path::Path, track_id: u64) -> Result<(), String> {
        let probe = decode_file_with_fallback(path)?;
        let sample_rate: u32 = probe.sample_rate().into();
        let channels: u16 = probe.channels().into();
        drop(probe);

        // Bit depth for the reported stream quality comes from the header, the
        // same place the in-memory path reads it; 24 is not assumed.
        let bit_depth = read_head(path, HEAD_BYTES)
            .and_then(|head| extract_audio_metadata_full(&head).ok())
            .and_then(|meta| meta.bit_depth)
            .unwrap_or(16);
        self.state.set_stream_quality(sample_rate, bit_depth);

        log::info!(
            "Player: Playing track {} from disk - {}Hz, {} channels, {}-bit",
            track_id,
            sample_rate,
            channels,
            bit_depth
        );

        self.tx
            .send(AudioCommand::Play {
                audio: TrackAudio::File(path.to_path_buf()),
                track_id,
                duration_secs: 0, // Will be determined by decoder
                sample_rate,
                channels,
            })
            .map_err(|e| {
                format!("Failed to send play command (audio thread may have crashed): {e}")
            })?;
        Ok(())
    }

    fn apply_play_data(&self, data: impl Into<TrackBytes>, track_id: u64) -> Result<(), String> {
        let data: TrackBytes = data.into();
        log::info!(
            "Player: Playing {} bytes of audio data for track {}",
            data.len(),
            track_id
        );

        // Extract audio metadata (sample rate, channels, bit depth) - fast header-only read
        let meta = extract_audio_metadata_full(&data)
            .map_err(|e| format!("Failed to extract audio metadata: {}", e))?;

        let sample_rate = meta.sample_rate;
        let channels = meta.channels;
        let bit_depth = meta.bit_depth.unwrap_or(16);

        log::info!(
            "Player: Detected audio format - {}Hz, {} channels, {}-bit",
            sample_rate,
            channels,
            bit_depth
        );

        // Update shared state with actual stream quality
        self.state.set_stream_quality(sample_rate, bit_depth);

        self.tx
            .send(AudioCommand::Play {
                audio: TrackAudio::Memory(data),
                track_id,
                duration_secs: 0, // Will be determined by decoder
                sample_rate,
                channels,
            })
            .map_err(|e| {
                log::error!("Player: Failed to send to audio thread: {}", e);
                format!(
                    "Failed to send play command (audio thread may have crashed): {}",
                    e
                )
            })?;

        log::info!("Player: Playback initiated successfully");
        Ok(())
    }

    /// Queue next track for gapless playback (appends to current Sink without stopping)
    /// Queue the next track for a gapless transition, decoding it from its L2
    /// cache FILE instead of from bytes in memory.
    ///
    /// Same hand-off as [`Self::play_next`], minus the resident copy: the
    /// prefetch has already written the track to disk, so holding 120–220 MB of
    /// Hi-Res in RAM until the transition buys nothing. The caller picks this
    /// when the file exists and the host is short of memory; on failure it can
    /// still fall back to the in-memory path.
    pub fn play_next_file(&self, path: std::path::PathBuf, track_id: u64) -> Result<(), String> {
        // Probe the format the same way the decoder will read it, so a file we
        // cannot decode fails HERE rather than at the transition, where the
        // fallback is a gap.
        let probe = decode_file_with_fallback(&path)?;
        let sample_rate: u32 = probe.sample_rate().into();
        let channels: u16 = probe.channels().into();
        drop(probe);

        log::info!(
            "Player: Queueing gapless track {} from disk ({}Hz, {}ch, {})",
            track_id,
            sample_rate,
            channels,
            path.display()
        );

        self.tx
            .send(AudioCommand::PlayNext {
                audio: TrackAudio::File(path),
                track_id,
                sample_rate,
                channels,
            })
            .map_err(|e| {
                log::error!("Player: Failed to send PlayNext (file) to audio thread: {e}");
                format!("Failed to send gapless command: {e}")
            })
    }

    pub fn play_next(&self, data: impl Into<TrackBytes>, track_id: u64) -> Result<(), String> {
        let data: TrackBytes = data.into();
        let meta = extract_audio_metadata_full(&data)
            .map_err(|e| format!("Failed to extract audio metadata for gapless: {}", e))?;

        log::info!(
            "Player: Queueing gapless track {} ({}Hz, {}ch, {} bytes)",
            track_id,
            meta.sample_rate,
            meta.channels,
            data.len()
        );

        self.tx
            .send(AudioCommand::PlayNext {
                audio: TrackAudio::Memory(data),
                track_id,
                sample_rate: meta.sample_rate,
                channels: meta.channels,
            })
            .map_err(|e| {
                log::error!("Player: Failed to send PlayNext to audio thread: {}", e);
                format!("Failed to send gapless command: {}", e)
            })
    }

    /// Play a local DSD file (.dsf/.dff) by converting it on the fly to
    /// 176.4 kHz / 24-bit PCM (qbz-dsd, Phase 1 of the DSD plan — see
    /// qbz-nix-docs/dsd-support/). The converted stream rides the existing
    /// `play_streaming` path as an ordinary finite WAV: a background thread
    /// demuxes + decimates and pushes into the BufferWriter, so the whole
    /// PCM pipeline (engines, bit-perfect ALSA at 176.4 kHz, volume,
    /// normalization, seek-in-buffer) behaves exactly as for any hi-res
    /// track. DST-compressed DFF and >2ch files are rejected with a
    /// readable error before anything starts.
    pub fn play_dsd_file(&self, path: std::path::PathBuf, track_id: u64) -> Result<(), String> {
        let _gen = self.begin_play();
        let demux = qbz_dsd::open_dsd(&path).map_err(|e| e.to_string())?;
        let dsd_rate = demux.info().dsd_rate;

        // DoP resolution (Phase 2): user opt-in + ALSA direct backend +
        // stereo + carrier rate supported by the device. Anything else falls
        // through to the universal DSD→PCM conversion below.
        #[cfg(target_os = "linux")]
        {
            let info = demux.info().clone();
            let resolved = self
                .audio_settings
                .lock()
                .ok()
                .map(|s| {
                    (
                        s.dsd_mode.clone(),
                        matches!(s.backend_type, Some(qbz_audio::AudioBackendType::Alsa)),
                        s.output_device.clone(),
                    )
                })
                .unwrap_or(("convert".to_string(), false, None));
            if let (mode, true, Some(device)) = resolved {
                if info.channels == 2 && mode != "convert" {
                    if mode == "native" {
                        // The open itself validates (kernel quirk / rates);
                        // failure surfaces as a stream error toast.
                        log::info!(
                            "Player: DSD track {} — {} via NATIVE DSD",
                            track_id,
                            qbz_dsd::dsd_label(info.dsd_rate)
                        );
                        drop(demux);
                        return self
                            .tx
                            .send(AudioCommand::PlayDsdNative { path, track_id })
                            .map_err(|e| {
                                format!("Failed to send native DSD play command: {}", e)
                            });
                    }
                    let carrier = qbz_dsd::dop_carrier_rate(info.dsd_rate);
                    let rate_ok =
                        qbz_audio::alsa_backend::get_device_supported_rates(&device)
                            .map(|r| r.contains(&carrier))
                            .unwrap_or(true);
                    if rate_ok {
                        log::info!(
                            "Player: DSD track {} — {} via DoP ({} Hz carrier)",
                            track_id,
                            qbz_dsd::dsd_label(info.dsd_rate),
                            carrier
                        );
                        drop(demux);
                        return self
                            .tx
                            .send(AudioCommand::PlayDsdDop { path, track_id })
                            .map_err(|e| format!("Failed to send DoP play command: {}", e));
                    }
                    log::info!(
                        "Player: DoP selected but device lacks the {} Hz carrier — converting to PCM",
                        carrier
                    );
                } else if info.channels != 2 && mode != "convert" {
                    log::info!(
                        "Player: {} selected but track has {} channels — downmix-converting to PCM",
                        mode,
                        info.channels
                    );
                }
            }
        }
        let mut conv = qbz_dsd::DsdPcmConverter::new(demux, qbz_dsd::DEFAULT_GAIN_DB)
            .map_err(|e| e.to_string())?;
        let channels = conv.channels();
        let rate = conv.output_rate();
        let total_frames = conv.total_frames();
        let duration_secs = total_frames / rate as u64;
        let content_length = qbz_dsd::wav_total_size(total_frames, channels);
        log::info!(
            "Player: DSD track {} — {} ({} Hz) → PCM {} Hz/24-bit, {}s, {} bytes WAV",
            track_id,
            qbz_dsd::dsd_label(dsd_rate),
            dsd_rate,
            rate,
            duration_secs,
            content_length
        );
        self.state.set_stream_quality(rate, 24);

        let writer = self.apply_play_streaming(
            track_id,
            rate,
            channels,
            content_length,
            3,
            duration_secs,
            0,
        )?;

        std::thread::spawn(move || {
            if writer
                .push_chunk(&qbz_dsd::wav_header(total_frames, channels, rate))
                .is_err()
            {
                return;
            }
            let mut pcm = Vec::new();
            loop {
                match conv.next_block() {
                    Ok(Some(frames)) => {
                        pcm.clear();
                        qbz_dsd::frames_to_pcm24(&frames, &mut pcm);
                        if writer.push_chunk(&pcm).is_err() {
                            // Reader gone (track changed/stopped) — just stop.
                            return;
                        }
                    }
                    Ok(None) => {
                        let _ = writer.complete();
                        return;
                    }
                    Err(e) => {
                        log::error!("Player: DSD conversion failed mid-track: {}", e);
                        let _ = writer.error(format!("DSD conversion failed: {}", e));
                        return;
                    }
                }
            }
        });
        Ok(())
    }

    /// Whole-file DSD→PCM conversion into an in-memory WAV, for the gapless
    /// prefetch path: the result feeds `play_next` like any cached track, so
    /// consecutive converted-DSD tracks hand off seamlessly. CPU-bound
    /// (~10-30x realtime) — call from a blocking context.
    pub fn prepare_dsd_gapless_wav(path: &std::path::Path) -> Result<Vec<u8>, String> {
        let demux = qbz_dsd::open_dsd(path).map_err(|e| e.to_string())?;
        let mut conv = qbz_dsd::DsdPcmConverter::new(demux, qbz_dsd::DEFAULT_GAIN_DB)
            .map_err(|e| e.to_string())?;
        let channels = conv.channels();
        let rate = conv.output_rate();
        let total = conv.total_frames();
        let mut out = qbz_dsd::wav_header(total, channels, rate);
        out.reserve(total as usize * channels as usize * 3);
        while let Some(frames) = conv.next_block().map_err(|e| e.to_string())? {
            qbz_dsd::frames_to_pcm24(&frames, &mut out);
        }
        Ok(out)
    }

    /// Queue the next DSD track for a gapless transition: appends to the DoP
    /// engine when one is active (seamless native DSD), otherwise converts to
    /// an in-memory WAV and rides the normal `play_next` gapless path.
    pub fn play_next_dsd(&self, path: std::path::PathBuf, track_id: u64) -> Result<(), String> {
        if self.state.is_dsd_direct() {
            return self
                .tx
                .send(AudioCommand::PlayNextDsdDop { path, track_id })
                .map_err(|e| format!("Failed to send DoP gapless command: {}", e));
        }
        let wav = Self::prepare_dsd_gapless_wav(&path)?;
        self.play_next(wav, track_id)
    }

    /// True while a DoP stream is active (volume fixed, seek unsupported).
    pub fn is_dsd_direct_active(&self) -> bool {
        self.state.is_dsd_direct()
    }

    /// Play from streaming source (starts playback before full download).
    /// Returns the BufferWriter so caller can push data as it downloads.
    /// `start_position_secs` > 0 turns this into a session-resume play
    /// (#315): the audio thread waits for enough buffer to cover the
    /// offset and pre-skips decoder output up to that point.
    pub fn play_streaming(
        &self,
        track_id: u64,
        sample_rate: u32,
        channels: u16,
        content_length: u64,
        buffer_seconds: u8,
        duration_secs: u64,
        start_position_secs: u64,
    ) -> Result<BufferWriter, String> {
        let _gen = self.begin_play();
        self.apply_play_streaming(
            track_id,
            sample_rate,
            channels,
            content_length,
            buffer_seconds,
            duration_secs,
            start_position_secs,
        )
    }

    /// `play_streaming` without bumping generation (used by play paths that
    /// already did their own `begin_play` + supersede checks, like
    /// `play_dsd_file`; a mid-intent bump here would invalidate a strictly
    /// newer play that started in the meantime).
    fn apply_play_streaming(
        &self,
        track_id: u64,
        sample_rate: u32,
        channels: u16,
        content_length: u64,
        buffer_seconds: u8,
        duration_secs: u64,
        start_position_secs: u64,
    ) -> Result<BufferWriter, String> {
        log::info!(
            "Player: Starting streaming playback for track {} ({}Hz, {}ch, {} bytes total, {}s, start={}s)",
            track_id,
            sample_rate,
            channels,
            content_length,
            duration_secs,
            start_position_secs
        );

        // Use StreamingConfig::from_seconds for proper buffer sizing
        let config = StreamingConfig::from_seconds(buffer_seconds);

        let (source, writer) = BufferedMediaSource::new(config, Some(content_length));
        let source = Arc::new(source);

        self.tx
            .send(AudioCommand::PlayStreaming {
                source: source.clone(),
                track_id,
                sample_rate,
                channels,
                duration_secs,
                start_position_secs,
                content_length,
                play_gen: self.state.current_play_generation(),
            })
            .map_err(|e| {
                log::error!("Player: Failed to send streaming command: {}", e);
                format!("Failed to send streaming play command: {}", e)
            })?;

        log::info!("Player: Streaming playback initiated");
        Ok(writer)
    }

    /// Play from streaming source with dynamic buffer based on measured speed.
    /// `start_position_secs` > 0 signals session resume (see `play_streaming`).
    ///
    /// `seek_mode` describes what the caller's feeder can do. Pass
    /// [`StreamSeekMode::RangeRequests`] only when it honors the buffer's
    /// range requests (see `BufferWriter::next_plan`): that is what lets a
    /// resume or a seek fetch from the offset instead of downloading
    /// everything before it. A feeder that assembles its bytes in order —
    /// CMAF segments, DSD-to-WAV — must pass
    /// [`StreamSeekMode::Sequential`].
    #[allow(clippy::too_many_arguments)]
    pub fn play_streaming_dynamic(
        &self,
        track_id: u64,
        sample_rate: u32,
        channels: u16,
        bit_depth: u32,
        content_length: u64,
        speed_mbps: f64,
        duration_secs: u64,
        start_position_secs: u64,
        seek_mode: StreamSeekMode,
    ) -> Result<BufferWriter, String> {
        let _gen = self.begin_play();
        self.apply_play_streaming_dynamic(
            track_id,
            sample_rate,
            channels,
            bit_depth,
            content_length,
            speed_mbps,
            duration_secs,
            start_position_secs,
            seek_mode,
        )
    }

    /// `play_streaming_dynamic` without bumping generation (used by
    /// `play_track`'s CMAF path, which already holds a generation token; a
    /// mid-intent bump here would invalidate a strictly newer play that
    /// started in the meantime).
    #[allow(clippy::too_many_arguments)]
    fn apply_play_streaming_dynamic(
        &self,
        track_id: u64,
        sample_rate: u32,
        channels: u16,
        bit_depth: u32,
        content_length: u64,
        speed_mbps: f64,
        duration_secs: u64,
        start_position_secs: u64,
        seek_mode: StreamSeekMode,
    ) -> Result<BufferWriter, String> {
        log::info!(
            "Player: Starting dynamic streaming for track {} ({}Hz, {}ch, {}-bit, {:.2} MB, {:.1} MB/s, {}s, start={}s, {:?})",
            track_id,
            sample_rate,
            channels,
            bit_depth,
            content_length as f64 / (1024.0 * 1024.0),
            speed_mbps,
            duration_secs,
            start_position_secs,
            seek_mode
        );

        // Update shared state with actual stream quality
        self.state.set_stream_quality(sample_rate, bit_depth);

        // Use StreamingConfig::from_speed_mbps for dynamic buffer sizing
        let mut config = StreamingConfig::from_speed_mbps(speed_mbps);

        // Floor the initial buffer at the user's `stream_buffer_seconds` worth
        // of REAL audio bytes (#591). content_length / duration is the track's
        // true average byterate (both derive from the CMAF segment table),
        // unlike `speed_mbps`, which is estimated from the tiny init fetch and
        // is latency-dominated — it lands on the slowest ladder rung for every
        // connection. Clamped to 256KB (format-detection minimum) .. 8MB (the
        // process-wide ladder cap has no desktop caller, so this is the
        // effective ceiling protecting low-memory hosts).
        let user_secs = self
            .audio_settings
            .lock()
            .map(|s| s.stream_buffer_seconds)
            .unwrap_or(2);
        let bps = content_length / duration_secs.max(1);
        let floor = (user_secs as u64).saturating_mul(bps) as usize;
        let ladder_bytes = config.initial_buffer_bytes;
        config.initial_buffer_bytes = ladder_bytes
            .max(floor)
            .clamp(256 * 1024, 8 * 1024 * 1024);
        if config.initial_buffer_bytes != ladder_bytes {
            log::info!(
                "Dynamic buffer: user floor {}s x {} B/s → {}KB initial buffer (ladder gave {}KB)",
                user_secs,
                bps,
                config.initial_buffer_bytes / 1024,
                ladder_bytes / 1024
            );
        }

        let (source, writer) = match seek_mode {
            StreamSeekMode::RangeRequests => {
                BufferedMediaSource::new_seekable(config, Some(content_length))
            }
            StreamSeekMode::Sequential => {
                BufferedMediaSource::new(config, Some(content_length))
            }
        };
        let source = Arc::new(source);

        self.tx
            .send(AudioCommand::PlayStreaming {
                source: source.clone(),
                track_id,
                sample_rate,
                channels,
                duration_secs,
                start_position_secs,
                content_length,
                play_gen: self.state.current_play_generation(),
            })
            .map_err(|e| {
                log::error!("Player: Failed to send streaming command: {}", e);
                format!("Failed to send streaming play command: {}", e)
            })?;

        log::info!("Player: Dynamic streaming playback initiated");
        Ok(writer)
    }

    /// Download audio from URL with timeout
    async fn download_audio(&self, url: &str) -> Result<Vec<u8>, String> {
        use std::time::Duration;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

        log::info!("Caching audio from URL...");

        let response = client
            .get(url)
            .header("User-Agent", "Mozilla/5.0")
            .send()
            .await
            .map_err(|e| format!("Failed to fetch audio: {}", e))?;

        if !response.status().is_success() {
            return Err(format!("HTTP error: {}", response.status()));
        }

        log::info!("Response received, reading bytes...");

        let bytes = response
            .bytes()
            .await
            .map_err(|e| format!("Failed to read audio bytes: {}", e))?;

        log::info!("Cached {} bytes", bytes.len());
        Ok(bytes.to_vec())
    }

    /// Pause playback
    pub fn pause(&self) -> Result<(), String> {
        self.tx
            .send(AudioCommand::Pause)
            .map_err(|e| format!("Failed to send pause command: {}", e))
    }

    /// Resume playback
    pub fn resume(&self) -> Result<(), String> {
        self.tx
            .send(AudioCommand::Resume)
            .map_err(|e| format!("Failed to send resume command: {}", e))
    }

    pub fn has_loaded_audio(&self) -> bool {
        self.state.has_loaded_audio()
    }

    /// Stop playback
    pub fn stop(&self) -> Result<(), String> {
        // Supersede any in-flight play_track so a slow CMAF/legacy fetch cannot
        // restart audio after the user stopped.
        let _ = self.begin_play();
        self.tx
            .send(AudioCommand::Stop)
            .map_err(|e| format!("Failed to send stop command: {}", e))
    }

    /// Set volume (0.0 - 1.0)
    pub fn set_volume(&self, volume: f32) -> Result<(), String> {
        let clamped = volume.clamp(0.0, 1.0);

        // Skip if volume is already at this value (prevents MPRIS/PipeWire feedback loop)
        let current = self.state.volume();
        if (clamped - current).abs() < 0.001 {
            return Ok(());
        }

        self.tx
            .send(AudioCommand::SetVolume(clamped))
            .map_err(|e| format!("Failed to send volume command: {}", e))
    }

    /// Seek to position in seconds
    pub fn seek(&self, position: u64) -> Result<(), String> {
        // Clamp to duration if known
        let duration = self.state.duration();
        let clamped_position = if duration > 0 {
            position.min(duration)
        } else {
            position
        };

        self.tx
            .send(AudioCommand::Seek(clamped_position))
            .map_err(|e| format!("Failed to send seek command: {}", e))
    }

    /// Reinitialize audio device (releases and re-acquires the device)
    /// Use this when changing audio settings like exclusive mode
    pub fn reinit_device(&self, device_name: Option<String>) -> Result<(), String> {
        self.tx
            .send(AudioCommand::ReinitDevice { device_name })
            .map_err(|e| format!("Failed to send reinit command: {}", e))
    }

    /// Release the output device without reopening it. Drops the active
    /// stream — freeing an exclusive ALSA `hw:` grab and its D-Bus
    /// reservation — and un-suspends / un-forces anything QBZ parked, so
    /// PipeWire/WirePlumber can reclaim a device QBZ was holding (e.g. a DAC
    /// left invisible to other apps after bit-perfect ALSA Direct). Pair
    /// with a device re-enumeration in the UI to surface a freed or
    /// hot-plugged DAC without restarting the app.
    pub fn release_device(&self) -> Result<(), String> {
        self.tx
            .send(AudioCommand::ReleaseDevice)
            .map_err(|e| format!("Failed to send release command: {}", e))
    }

    /// Reload audio settings from fresh config (e.g., after database update)
    /// Call this before reinit_device() to ensure Player uses latest settings
    pub fn reload_settings(&self, settings: AudioSettings) -> Result<(), String> {
        if let Ok(mut current_settings) = self.audio_settings.lock() {
            *current_settings = settings;
            Ok(())
        } else {
            Err("Failed to lock audio settings".to_string())
        }
    }

    /// Get current playback state with real-time position
    pub fn get_state(&self) -> Result<PlaybackState, String> {
        Ok(PlaybackState {
            is_playing: self.state.is_playing(),
            position: self.state.current_position(),
            duration: self.state.duration(),
            track_id: self.state.current_track_id(),
            volume: self.state.volume(),
        })
    }

    /// Get playback event for emitting to frontend
    pub fn get_playback_event(&self) -> PlaybackEvent {
        let sample_rate = self.state.get_sample_rate();
        let bit_depth = self.state.get_bit_depth();
        PlaybackEvent {
            is_playing: self.state.is_playing(),
            position: self.state.current_position(),
            duration: self.state.duration(),
            track_id: self.state.current_track_id(),
            volume: self.state.volume(),
            sample_rate: if sample_rate > 0 {
                Some(sample_rate)
            } else {
                None
            },
            bit_depth: if bit_depth > 0 { Some(bit_depth) } else { None },
            output_sample_rate: self.state.get_output_sample_rate(),
            shuffle: None, // Set by caller with access to queue state
            repeat: None,  // Set by caller with access to queue state
            normalization_gain: self.state.get_normalization_gain(),
            gapless_ready: self.state.is_gapless_ready(),
            gapless_next_track_id: self.state.get_gapless_next_track_id(),
            bit_perfect_mode: self.state.get_bit_perfect_mode(),
            buffer_progress: self.state.get_buffer_progress(),
        }
    }
}

/// Playback state snapshot
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PlaybackState {
    pub is_playing: bool,
    pub position: u64,
    pub duration: u64,
    pub track_id: u64,
    pub volume: f32,
}

/// Surfaces a DSD stream's mid-file demux I/O error as a stream error when
/// the stream runs out. The DoP writer consumes these streams as boxed
/// `Iterator<Item = i32>`, which can only say "no more words", so without
/// this wrapper a read failure is indistinguishable from a clean end of
/// track and the user gets a silent early stop.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct DsdErrorReport<S: qbz_dsd::DsdWordSource> {
    inner: S,
    // SharedState is a #[derive(Clone)] handle of Arc<Atomic..> fields, so a
    // clone already shares the same state — no extra Arc wrapper (that is also
    // the type `thread_state` is passed around as at the call sites).
    state: SharedState,
    reported: bool,
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
impl<S: qbz_dsd::DsdWordSource> DsdErrorReport<S> {
    fn new(inner: S, state: SharedState) -> Self {
        Self {
            inner,
            state,
            reported: false,
        }
    }
}

impl<S: qbz_dsd::DsdWordSource> Iterator for DsdErrorReport<S> {
    type Item = i32;
    fn next(&mut self) -> Option<i32> {
        let item = self.inner.next();
        if item.is_none() && !self.reported {
            self.reported = true;
            if let Some(e) = self.inner.io_error() {
                self.state
                    .record_stream_error(format!("DSD playback failed: {e}"));
            }
        }
        item
    }
}

/// Pick the MIME to advertise to an external renderer for a legacy stream-URL
/// download. Prefer the server-provided `mime_type`; when it is empty (Qobuz
/// can return `""`), fall back by format id so the renderer is never handed an
/// empty content type (which some Chromecast/DLNA renderers reject).
pub fn external_content_type(mime: &str, format_id: u32) -> String {
    let trimmed = mime.trim();
    if !trimmed.is_empty() {
        return trimmed.to_string();
    }
    match qbz_models::Quality::from_id(format_id) {
        Some(qbz_models::Quality::Mp3) => "audio/mpeg".to_string(),
        // Lossless / HiRes / UltraHiRes are FLAC over the file/url path.
        Some(_) => "audio/flac".to_string(),
        None => "audio/flac".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::compute_needs_new_stream;
    use super::external_content_type;
    use super::{DsdErrorReport, SharedState};

    struct FakeDsdSource {
        words: std::vec::IntoIter<i32>,
        error: Option<String>,
    }

    impl Iterator for FakeDsdSource {
        type Item = i32;
        fn next(&mut self) -> Option<i32> {
            self.words.next()
        }
    }

    impl qbz_dsd::DsdWordSource for FakeDsdSource {
        fn io_error(&self) -> Option<&str> {
            self.error.as_deref()
        }
    }

    #[test]
    fn dsd_error_report_surfaces_io_error_at_stream_end() {
        let state = SharedState::new();
        let source = FakeDsdSource {
            words: vec![1, 2].into_iter(),
            error: Some("read failed".to_string()),
        };
        let mut wrapped = DsdErrorReport::new(source, state.clone());
        assert_eq!(wrapped.next(), Some(1));
        assert!(!state.has_stream_error());
        assert_eq!(wrapped.next(), Some(2));
        assert_eq!(wrapped.next(), None);
        assert!(state.has_stream_error());
        let msg = state.take_stream_error_message().unwrap();
        assert!(msg.contains("read failed"));
        // Repeated polls after exhaustion must not re-record the message.
        assert_eq!(wrapped.next(), None);
        assert_eq!(state.take_stream_error_message(), None);
    }

    #[test]
    fn dsd_error_report_clean_eof_is_not_an_error() {
        let state = SharedState::new();
        let source = FakeDsdSource {
            words: vec![1].into_iter(),
            error: None,
        };
        let mut wrapped = DsdErrorReport::new(source, state.clone());
        assert_eq!(wrapped.next(), Some(1));
        assert_eq!(wrapped.next(), None);
        assert!(!state.has_stream_error());
        assert_eq!(state.take_stream_error_message(), None);
    }

    #[test]
    fn no_stream_always_needs_new() {
        // Without an existing stream there is nothing to reuse — every
        // other flag is irrelevant.
        assert!(compute_needs_new_stream(
            false, false, false, false, false, false
        ));
    }

    #[test]
    fn unchanged_format_on_default_backend_reuses_stream() {
        // Default rodio backend resamples internally, so an unchanged
        // decoded format on an existing stream needs no rebuild.
        assert!(!compute_needs_new_stream(
            true, false, false, false, false, false
        ));
    }

    #[test]
    fn format_change_on_default_backend_rebuilds_for_native_rate() {
        // #449 regression guard: a decoded sample-rate/channel change must
        // rebuild the output stream on EVERY backend, not just the bit-perfect
        // ones, so the device follows the track's native rate. 1.2.10 got this
        // "for free" because Stop dropped the stream; once that drop was
        // deferred to avoid a track-change click (e93fcaec), reusing the stream
        // on the default/PipeWire backend left the node locked to the first
        // track's rate for every subsequent track. Same-rate tracks still
        // reuse (format_changed == false), preserving the click fix.
        assert!(compute_needs_new_stream(
            true, true, false, false, false, false
        ));
    }

    #[test]
    fn format_change_with_dac_passthrough_rebuilds() {
        assert!(compute_needs_new_stream(
            true, true, true, false, false, false
        ));
    }

    #[test]
    fn format_change_with_alsa_direct_rebuilds() {
        assert!(compute_needs_new_stream(
            true, true, false, true, false, false
        ));
    }

    #[test]
    fn format_change_with_coreaudio_exclusive_rebuilds() {
        assert!(compute_needs_new_stream(
            true, true, false, false, true, false
        ));
    }

    #[test]
    fn bit_perfect_backends_without_format_change_reuse_stream() {
        // Bit-perfect flags only force a rebuild *together with* a format
        // change. On their own they should not.
        assert!(!compute_needs_new_stream(
            true, false, true, false, false, false
        ));
        assert!(!compute_needs_new_stream(
            true, false, false, true, false, false
        ));
        assert!(!compute_needs_new_stream(
            true, false, false, false, true, false
        ));
    }

    #[test]
    fn coreaudio_shared_rate_mismatch_rebuilds_regardless_of_format_change() {
        // The CoreAudio shared-mode rate-drift case has nothing to do with
        // track format; it must rebuild whenever detected.
        assert!(compute_needs_new_stream(
            true, false, false, false, false, true
        ));
    }

    #[test]
    fn current_position_ms_is_a_pure_anchor_derivation() {
        use std::sync::atomic::Ordering;

        let state = super::SharedState::new();
        state.duration.store(300, Ordering::SeqCst);

        // Paused: coarse stored position scaled to ms (current_position parity).
        state.position.store(12, Ordering::SeqCst);
        assert_eq!(state.current_position_ms(), 12_000);

        // Playing without an anchor yet: same coarse fallback.
        state.is_playing.store(true, Ordering::SeqCst);
        assert_eq!(state.current_position_ms(), 12_000);

        // Playing with anchors: position_at_start*1000 + wall-clock elapsed ms.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        state
            .playback_start_millis
            .store(now_ms - 1_500, Ordering::SeqCst);
        state.position_at_start.store(10, Ordering::SeqCst);
        let pos = state.current_position_ms();
        assert!(
            (11_450..=11_900).contains(&pos),
            "expected ~11500ms, got {pos}"
        );

        // Clamped to duration*1000 (same rule current_position applies).
        state
            .playback_start_millis
            .store(now_ms - 10_000, Ordering::SeqCst);
        state.position_at_start.store(299, Ordering::SeqCst);
        assert_eq!(state.current_position_ms(), 300_000);
    }

    #[test]
    fn external_content_type_prefers_server_mime() {
        assert_eq!(external_content_type("audio/flac", 7), "audio/flac");
        assert_eq!(external_content_type("audio/mpeg", 5), "audio/mpeg");
        // Whitespace-only is treated as empty.
        assert_eq!(external_content_type("  ", 6), "audio/flac");
    }

    #[test]
    fn external_content_type_falls_back_by_format_id() {
        // Empty MIME (Qobuz can return "") -> derive from format id.
        assert_eq!(external_content_type("", 5), "audio/mpeg"); // Mp3
        assert_eq!(external_content_type("", 6), "audio/flac"); // Lossless
        assert_eq!(external_content_type("", 7), "audio/flac"); // HiRes
        assert_eq!(external_content_type("", 27), "audio/flac"); // UltraHiRes
        assert_eq!(external_content_type("", 999), "audio/flac"); // unknown -> flac
    }

    #[test]
    fn stream_quality_normalizes_units() {
        use qbz_models::StreamQualityInfo;
        // kHz input stays kHz.
        let khz = StreamQualityInfo::from_raw(7, Some(96.0), Some(24));
        assert_eq!(khz.sampling_rate_khz, Some(96.0));
        // Hz input is converted to kHz.
        let hz = StreamQualityInfo::from_raw(27, Some(192000.0), Some(24));
        assert_eq!(hz.sampling_rate_khz, Some(192.0));
        // Zero / unknown -> None.
        let zero = StreamQualityInfo::from_raw(6, Some(0.0), Some(16));
        assert_eq!(zero.sampling_rate_khz, None);
        // Tier label from format id.
        assert_eq!(khz.tier_label(), "FLAC 24-bit/≤96kHz");
        assert_eq!(hz.tier_label(), "FLAC 24-bit/>96kHz");
    }
}

#[cfg(test)]
mod read_head_tests {
    use super::{read_head, HEAD_BYTES};

    fn temp_file(name: &str, bytes: &[u8]) -> std::path::PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("qbz-head-{name}-{n}.bin"));
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn a_long_file_gives_exactly_the_head() {
        let path = temp_file("long", &vec![7u8; HEAD_BYTES * 2]);
        let head = read_head(&path, 1024).unwrap();
        assert_eq!(head.len(), 1024);
        assert!(head.iter().all(|&b| b == 7));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_short_file_gives_all_of_itself_rather_than_failing() {
        // Every caller parses a header out of this or gives up gracefully, so
        // a file shorter than the window is not an error — and a cache file
        // still being written is exactly that.
        let path = temp_file("short", b"only twenty-four bytes!!");
        let head = read_head(&path, HEAD_BYTES).unwrap();
        assert_eq!(head.len(), 24);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn an_empty_file_reads_as_empty_not_as_an_error() {
        let path = temp_file("empty", b"");
        assert_eq!(read_head(&path, HEAD_BYTES).unwrap().len(), 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_missing_file_is_none_so_the_caller_falls_back_to_the_network() {
        let missing = std::env::temp_dir().join("qbz-head-definitely-not-here.bin");
        assert!(read_head(&missing, HEAD_BYTES).is_none());
    }
}

#[cfg(test)]
mod spill_rule_tests {
    use super::spill_to_disk;

    /// The 1 GB player this was measured on: 17 % of 905 MB.
    const PI_1GB: usize = 161_342_914;
    /// A 4 GB player, where the 512 MB ceiling binds.
    const PI_4GB: usize = 512 * 1024 * 1024;

    /// Real tracks from the test player, against the budget it actually had.
    /// The pair is what matters — a 68 MB track fits twice in 153 MB, a 100 MB
    /// one does not.
    #[test]
    fn the_rule_is_whether_two_of_them_fit() {
        assert!(!spill_to_disk(22_400_000, PI_1GB), "22 MB CD track stays in RAM");
        assert!(!spill_to_disk(68_348_026, PI_1GB), "68 MB fits twice, just");
        assert!(spill_to_disk(100_589_129, PI_1GB), "100 MB does not");
        assert!(spill_to_disk(204_683_730, PI_1GB), "a 195 MB Hi-Res movement does not");
    }

    /// The same tracks on a roomier board: the ones that had to spill no longer
    /// do. The rule scales with the host instead of keying on its memory class,
    /// which is what sent a 22 MB track to the card for no reason.
    #[test]
    fn a_bigger_budget_keeps_more_in_memory() {
        assert!(!spill_to_disk(100_589_129, PI_4GB));
        assert!(!spill_to_disk(204_683_730, PI_4GB), "195 MB fits twice in 512 MB");
        assert!(spill_to_disk(300_000_000, PI_4GB), "but 286 MB does not");
    }

    /// Exactly at the line: two must FIT, so equal-to-budget stays in memory.
    #[test]
    fn the_boundary_is_inclusive() {
        assert!(!spill_to_disk(50, 100), "two of these are exactly the budget");
        assert!(spill_to_disk(51, 100), "one byte over and it spills");
    }

    /// A absurd size must not wrap into "fits".
    #[test]
    fn an_overflowing_size_still_spills() {
        assert!(spill_to_disk(usize::MAX, PI_1GB));
    }
}

#[cfg(test)]
mod new_track_and_prefetch_tests {
    use super::{begin_new_track, should_arm_prefetch, TrackAudio, GaplessPending, SharedState};
    use qbz_cache::TrackBytes;

    fn a_pending(track_id: u64) -> GaplessPending {
        GaplessPending {
            track_id,
            duration_secs: 300,
            audio: TrackAudio::Memory(TrackBytes::from(vec![0u8; 4])),
            normalization_gain: None,
            sample_rate: 44_100,
            channels: 2,
        }
    }

    /// THE regression. Starting a new track must leave nothing of the previous
    /// track's prepared successor behind — on EVERY path a track can start.
    ///
    /// `Play` always did this; `PlayStreaming`, which is how every track picked
    /// in the Qobuz app starts, never did. The result was that selecting a track
    /// from another album left the old album's successor pending: the prefetch
    /// for the new track could not arm, and the transition at the end of it
    /// swapped to a track the rebuilt engine did not have and stalled.
    #[test]
    fn starting_a_track_clears_every_gapless_slot() {
        let state = SharedState::new();
        let mut pending = Some(a_pending(2_944_914));
        let mut armed = true;
        let mut deadline = Some(std::time::Instant::now());
        state.set_gapless_ready(true);
        state.set_gapless_next_track_id(2_944_914);

        begin_new_track(&state, &mut pending, &mut armed, &mut deadline);

        assert!(pending.is_none(), "a successor from the previous track must not survive");
        assert!(!armed, "a stale armed flag blocks the new track's prefetch forever");
        assert!(!state.is_gapless_ready());
        assert_eq!(state.get_gapless_next_track_id(), 0);
        assert!(deadline.is_none());
    }

    /// Idempotent: starting a track when nothing was prepared is a no-op, not a
    /// surprise.
    #[test]
    fn starting_a_track_from_clean_state_changes_nothing() {
        let state = SharedState::new();
        let mut pending: Option<GaplessPending> = None;
        let mut armed = false;
        let mut deadline = None;

        begin_new_track(&state, &mut pending, &mut armed, &mut deadline);

        assert!(pending.is_none());
        assert!(!armed);
        assert!(!state.is_gapless_ready());
        assert_eq!(state.get_gapless_next_track_id(), 0);
    }

    /// The happy path: buffered, nothing prepared, nothing outstanding.
    #[test]
    fn prefetch_arms_once_the_current_track_is_buffered() {
        assert!(should_arm_prefetch(true, false, 300, false, false, false, 0, true));
    }

    /// Each gate refuses on its own. The `request_armed` line is the one that
    /// bit: left set by a track change, it stops all prefetching silently.
    #[test]
    fn every_gate_can_refuse_alone() {
        let cases: [(&str, bool); 8] = [
            ("disabled", should_arm_prefetch(false, false, 300, false, false, false, 0, true)),
            ("transition just fired", should_arm_prefetch(true, true, 300, false, false, false, 0, true)),
            ("duration unknown", should_arm_prefetch(true, false, 0, false, false, false, 0, true)),
            ("already prepared", should_arm_prefetch(true, false, 300, true, false, false, 0, true)),
            ("request outstanding", should_arm_prefetch(true, false, 300, false, true, false, 0, true)),
            ("driver already told", should_arm_prefetch(true, false, 300, false, false, true, 0, true)),
            ("successor queued", should_arm_prefetch(true, false, 300, false, false, false, 7, true)),
            ("still downloading", should_arm_prefetch(true, false, 300, false, false, false, 0, false)),
        ];
        for (why, armed) in cases {
            assert!(!armed, "should not arm: {why}");
        }
    }

    /// The two together, as they run in sequence: a track change clears the
    /// slots, and the prefetch for the NEW track can then arm. Before the fix
    /// the first half did not happen on the streaming path, so the second half
    /// never became true.
    #[test]
    fn a_track_change_unblocks_the_next_prefetch() {
        let state = SharedState::new();
        let mut pending = Some(a_pending(2_944_914));
        let mut armed = true;
        let mut deadline = None;

        // While the stale state stands, nothing can arm.
        assert!(!should_arm_prefetch(
            true, false, 774, pending.is_some(), armed,
            state.is_gapless_ready(), state.get_gapless_next_track_id(), true
        ));

        begin_new_track(&state, &mut pending, &mut armed, &mut deadline);

        assert!(should_arm_prefetch(
            true, false, 774, pending.is_some(), armed,
            state.is_gapless_ready(), state.get_gapless_next_track_id(), true
        ));
    }
}
