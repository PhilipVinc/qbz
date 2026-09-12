//! Buffered media source for streaming playback.
//!
//! Provides two main components:
//! 1. `BufferedMediaSource` - Wraps an async HTTP response to provide a synchronous
//!    `Read + Seek` interface required by symphonia decoders.
//! 2. `IncrementalStreamingSource` - A rodio Source that decodes audio packets
//!    incrementally as they become available, allowing playback to start before
//!    the entire file is downloaded.
//!
//! # Design
//!
//! The source uses a growing buffer that accumulates data from the HTTP response.
//! - Reads block if requesting data not yet buffered
//! - Seek forward blocks until data is available
//! - Seek backward works within buffered data
//! - Seek beyond current buffer position blocks until data arrives
//!
//! # Thread Safety
//!
//! The buffer state is shared between:
//! - The reader (audio thread, synchronous)
//! - The writer (download task, async)
//!
//! Communication uses `Mutex` + `Condvar` for blocking synchronization.

use std::collections::VecDeque;
use std::io::{Cursor, Error as IoError, ErrorKind, Read, Result as IoResult, Seek, SeekFrom};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use qbz_cache::TrackBytes;
use rodio::Source;
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{Decoder, DecoderOptions};
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::{FormatOptions, FormatReader, SeekMode, SeekTo};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use symphonia::default::{get_codecs, get_probe};
use tokio::sync::Notify;

/// Configuration for the streaming buffer
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Minimum bytes to buffer before allowing reads (for format detection)
    pub initial_buffer_bytes: usize,
    /// Maximum buffer size before backpressure (not enforced, just for info)
    pub max_buffer_bytes: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            // 512KB default - enough for format headers and ~2-5 seconds of audio
            // This allows playback to start quickly while still having enough
            // buffer to handle network jitter
            initial_buffer_bytes: 512 * 1024,
            // 100MB max buffer
            max_buffer_bytes: 100 * 1024 * 1024,
        }
    }
}

impl StreamingConfig {
    /// Create config from buffer seconds and approximate bitrate
    ///
    /// For Hi-Res FLAC at 192kHz/24bit stereo, bitrate is roughly 9.2 Mbps
    /// We estimate ~1MB per second as a conservative approximation
    pub fn from_seconds(seconds: u8) -> Self {
        // Minimum 256KB to ensure format detection works
        let bytes = ((seconds as usize) * 1024 * 1024).max(256 * 1024);
        Self {
            initial_buffer_bytes: bytes,
            max_buffer_bytes: 100 * 1024 * 1024,
        }
    }

    /// Create a minimal config for fastest startup
    /// Uses smallest buffer that still allows format detection (~256KB)
    pub fn fast_start() -> Self {
        Self {
            initial_buffer_bytes: 256 * 1024,
            max_buffer_bytes: 100 * 1024 * 1024,
        }
    }

    /// Create config dynamically based on measured download speed
    ///
    /// - Very fast (>10 MB/s): 256KB (instant start)
    /// - Fast (5-10 MB/s): 384KB
    /// - Normal (2-5 MB/s): 512KB
    /// - Slow (1-2 MB/s): 1MB (more buffer to prevent stutter)
    /// - Very slow (<1 MB/s): 2MB
    ///
    /// Result is clamped to the process-wide cap configured via
    /// [`set_max_initial_buffer_bytes`] (typically derived from the host's
    /// memory profile — see qbz-core's system_capabilities). On
    /// memory-constrained hosts the slow-connection branches would
    /// otherwise inflate to 2 MB, which is exactly the wrong direction
    /// when "slow connection" is itself a symptom of swap thrash
    /// (issue #331, Pi 3B).
    pub fn from_speed_mbps(speed_mbps: f64) -> Self {
        let cap = MAX_INITIAL_BUFFER_BYTES.load(std::sync::atomic::Ordering::Relaxed);
        let cfg = Self::from_speed_mbps_with_cap(speed_mbps, cap);

        if cfg.initial_buffer_bytes < raw_initial_buffer_for_speed(speed_mbps) {
            log::info!(
                "Dynamic buffer: {:.1} MB/s detected → {}KB (capped from {}KB by host memory profile)",
                speed_mbps,
                cfg.initial_buffer_bytes / 1024,
                raw_initial_buffer_for_speed(speed_mbps) / 1024
            );
        } else {
            log::info!(
                "Dynamic buffer: {:.1} MB/s detected → {}KB initial buffer",
                speed_mbps,
                cfg.initial_buffer_bytes / 1024
            );
        }

        cfg
    }

    /// Pure variant of [`from_speed_mbps`] — derives the speed-based
    /// initial buffer and clamps to `cap` without touching global state
    /// or logging. Exposed for unit tests; production callers should use
    /// `from_speed_mbps`, which reads the process-wide cap.
    pub fn from_speed_mbps_with_cap(speed_mbps: f64, cap: usize) -> Self {
        let raw_initial_buffer = raw_initial_buffer_for_speed(speed_mbps);
        Self {
            initial_buffer_bytes: raw_initial_buffer.min(cap),
            max_buffer_bytes: 100 * 1024 * 1024,
        }
    }
}

/// Speed-driven initial buffer size, before any cap is applied.
/// Pure function — used by both `from_speed_mbps` and
/// `from_speed_mbps_with_cap` so they share the same ladder.
fn raw_initial_buffer_for_speed(speed_mbps: f64) -> usize {
    if speed_mbps >= 10.0 {
        256 * 1024 // 256KB - instant start for very fast connections
    } else if speed_mbps >= 5.0 {
        384 * 1024 // 384KB
    } else if speed_mbps >= 2.0 {
        512 * 1024 // 512KB - default
    } else if speed_mbps >= 1.0 {
        1024 * 1024 // 1MB - more buffer for slower connections
    } else {
        2 * 1024 * 1024 // 2MB - maximum buffer for very slow connections
    }
}

/// Process-wide cap for dynamically-derived initial buffer sizes.
/// Defaults to `usize::MAX` (no cap) so behavior is unchanged unless the
/// host explicitly configures it via [`set_max_initial_buffer_bytes`] —
/// typically once at process start, derived from the detected memory
/// profile.
static MAX_INITIAL_BUFFER_BYTES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(usize::MAX);

/// Set the process-wide cap for `StreamingConfig::from_speed_mbps`.
/// Subsequent calls to that constructor clamp their result to this cap.
pub fn set_max_initial_buffer_bytes(bytes: usize) {
    MAX_INITIAL_BUFFER_BYTES.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

/// Read the current cap. Mainly useful for tests.
pub fn max_initial_buffer_bytes() -> usize {
    MAX_INITIAL_BUFFER_BYTES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Whether the feeder behind a [`BufferedMediaSource`] can restart its body
/// at an arbitrary byte offset.
///
/// This is what makes a seek cheap. A `Sequential` feeder only ever
/// produces bytes in the order it generates them (CMAF segment assembly,
/// DSD-to-WAV synthesis), so a read ahead of the write head can do nothing
/// but wait for the download to walk there. A `RangeRequests` feeder
/// re-opens the HTTP body with a `Range` header wherever a reader asks, so
/// resuming at 2:30 costs one request instead of two and a half minutes of
/// hi-res FLAC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSeekMode {
    /// Bytes arrive in order from wherever the feeder started; reads ahead
    /// of the download head block until it gets there.
    Sequential,
    /// The feeder honors range requests: a read anywhere in the file makes
    /// it re-open the body at that offset.
    RangeRequests,
}

/// How far ahead of the download head a read may sit before we prefer a
/// fresh range request over waiting for the bytes already on their way.
///
/// Sized from a measured resume, because the cost is lopsided and one bad
/// call cascades. Qobuz FLACs carry STREAMINFO as their only metadata
/// block — no SEEKTABLE — so Symphonia cannot look a timestamp up and
/// bisects instead, probing a few offsets that converge on the target. In
/// a 106s resume trace the first probe landed 33 MB ahead (a genuine jump,
/// worth a request) and the second only 803 KB ahead. At 512 KB that
/// second probe re-opened the body, which moved its start past the third
/// and fourth probes and turned both into holes needing their own
/// requests: one avoidable re-open became three, and two of the four cost
/// ~5 s of CDN time-to-first-byte on a cold offset. Waiting out those
/// 803 KB would have cost ~280 ms and left the following probes inside
/// already-buffered data.
///
/// So: generous enough to swallow a bisection (its window halves each
/// step, starting under a megabyte once the byte-rate estimate is close),
/// while a real jump is orders of magnitude further out and still gets its
/// request. 2 MB is under a second of download on any link fast enough to
/// stream hi-res at all.
const FORWARD_WAIT_BYTES: u64 = 2 * 1024 * 1024;

/// How far *before* the target a re-opened body starts when the reader is
/// walking backwards.
///
/// A probe behind the body we are already streaming can only be a decoder
/// bisecting for a frame boundary — nothing else reads backwards into a
/// region it just skipped. Symphonia's walk then continues downward, and
/// each step would strand the next one in another hole: a measured 91s
/// resume went 32.31 MB, 31.47 MB, 31.06 MB, 30.85 MB, four requests where
/// two of them cost ~5 s of CDN time-to-first-byte. Starting the body a
/// megabyte early puts the rest of the walk inside bytes we are about to
/// hold anyway, and `FORWARD_WAIT_BYTES` covers the gap between the body's
/// start and the probe that asked for it.
///
/// Kept below `FORWARD_WAIT_BYTES` so the reader waits for its own target
/// rather than immediately asking again, and small enough that streaming
/// through it costs a fraction of a second on any link that can carry
/// hi-res.
const SEEK_LOOKBEHIND_BYTES: u64 = 1024 * 1024;

/// One contiguous run of downloaded bytes, covering
/// `[offset, offset + data.len())` of the source file.
struct BufferSegment {
    offset: u64,
    data: Vec<u8>,
}

impl BufferSegment {
    fn end(&self) -> u64 {
        self.offset + self.data.len() as u64
    }
}

/// Internal state shared between reader and writer
struct BufferState {
    /// Downloaded runs, sorted by offset and never overlapping. A
    /// sequential stream keeps exactly one run starting at 0 — the same
    /// layout the single `Vec<u8>` had before range requests. A second run
    /// (and the hole in front of it) only appears once a reader jumps.
    segments: Vec<BufferSegment>,
    /// Offset the feeder appends at: the end of the run it is filling.
    write_pos: u64,
    /// Offset the feeder's current body started at. With `write_pos` this
    /// says which bytes are already on their way, so a read just ahead of
    /// the head waits instead of pointlessly re-requesting.
    range_start: u64,
    /// True when the feeder has nothing left to fetch. A range feeder only
    /// reports this once no gap remains, so a complete buffer is always a
    /// whole file.
    download_complete: bool,
    /// Error from download, if any
    download_error: Option<String>,
    /// Total expected size (from Content-Length), if known
    total_size: Option<u64>,
    /// Where playback reads from: 0 normally, the seek target after a range
    /// request. Buffer-fill checks measure from here, so "2 seconds
    /// buffered" means two seconds of what is about to play, not two
    /// seconds of a part of the track already behind us.
    primary_offset: u64,
    /// Offset a reader wants the feeder to restart at, until it takes it.
    pending_request: Option<u64>,
    /// Whether the feeder can honor `pending_request` at all.
    seek_mode: StreamSeekMode,
    /// Live `BufferedMediaSource` handles. The feeder stops looking for
    /// backfill work once the last reader goes away.
    readers: usize,
    /// Id of the newest reader. Only that one may steer the feeder: a seek
    /// builds a second decoder over the same buffer and the outgoing one can
    /// still take a read or two before it is dropped, and letting it ask for
    /// its own old offset would fight the new one for the connection.
    reader_epoch: u64,
}

impl BufferState {
    /// Index of the run holding `pos`, if it is buffered.
    fn segment_at(&self, pos: u64) -> Option<usize> {
        self.segments
            .iter()
            .position(|seg| pos >= seg.offset && pos < seg.end())
    }

    /// Bytes readable contiguously starting at `pos`.
    fn contiguous_from(&self, pos: u64) -> u64 {
        match self.segment_at(pos) {
            Some(idx) => self.segments[idx].end() - pos,
            None => 0,
        }
    }

    /// Total bytes held, across every run.
    fn downloaded(&self) -> u64 {
        self.segments.iter().map(|seg| seg.data.len() as u64).sum()
    }

    /// First byte range still missing, as `(start, exclusive end)`. The end
    /// is `None` for the file's tail, which the feeder reads to EOF.
    /// Needs `total_size`; with an unknown length only the feeder knows
    /// when it is done.
    fn first_gap(&self) -> Option<(u64, Option<u64>)> {
        let total = self.total_size?;
        let mut cursor = 0u64;
        for seg in &self.segments {
            if seg.offset > cursor {
                return Some((cursor, Some(seg.offset)));
            }
            cursor = cursor.max(seg.end());
        }
        (cursor < total).then_some((cursor, None))
    }

    /// Should the feeder be asked to re-open at `pos`, or are those bytes
    /// close enough behind the download head to be worth waiting for?
    fn should_request(&self, pos: u64) -> bool {
        if self.seek_mode != StreamSeekMode::RangeRequests || self.download_complete {
            return false;
        }
        if self.pending_request.is_some() {
            // A restart is already in flight; a second would only cancel it.
            return false;
        }
        let inbound =
            pos >= self.range_start && pos <= self.write_pos.saturating_add(FORWARD_WAIT_BYTES);
        !inbound
    }

    /// True once `pos` is past everything the feeder will ever produce, so
    /// a read there is EOF rather than a wait.
    ///
    /// `total_size` settles this only for a range stream, where it came from
    /// `Content-Length` and is exact. A sequential stream's total is an
    /// estimate — CMAF derives it by summing a segment table — so trusting
    /// it would cut playback off early; there, only the feeder saying it is
    /// done makes an unbuffered offset EOF.
    fn at_eof(&self, pos: u64) -> bool {
        if self.seek_mode == StreamSeekMode::RangeRequests {
            if let Some(total) = self.total_size {
                if pos >= total {
                    return true;
                }
            }
        }
        self.download_complete && self.segment_at(pos).is_none() && pos >= self.write_pos
    }

    /// Point the write head at `offset`: the feeder is about to push the
    /// body it opened there.
    fn begin(&mut self, offset: u64) {
        self.range_start = offset;
        self.write_pos = offset;
    }

    /// Merge `chunk` in at `offset`, keeping runs sorted and disjoint.
    fn insert(&mut self, offset: u64, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        // By far the common case: the feeder appending to the run it fills.
        if let Some(seg) = self.segments.iter_mut().find(|seg| seg.end() == offset) {
            seg.data.extend_from_slice(chunk);
        } else {
            let at = self
                .segments
                .iter()
                .position(|seg| seg.offset > offset)
                .unwrap_or(self.segments.len());
            self.segments.insert(
                at,
                BufferSegment {
                    offset,
                    data: chunk.to_vec(),
                },
            );
        }
        self.coalesce();
    }

    /// Fold adjacent or overlapping runs together. Re-fetching a byte we
    /// already hold is normal (a decoder bisecting for a seek point, a gap
    /// fill overrunning its end), and the overlap is dropped rather than
    /// duplicated.
    fn coalesce(&mut self) {
        let mut i = 0;
        while i + 1 < self.segments.len() {
            let cur_end = self.segments[i].end();
            if self.segments[i + 1].offset <= cur_end {
                let next = self.segments.remove(i + 1);
                let skip = (cur_end - next.offset) as usize;
                if skip < next.data.len() {
                    self.segments[i].data.extend_from_slice(&next.data[skip..]);
                }
            } else {
                i += 1;
            }
        }
    }

    /// The run starting at byte 0, if there is one. Both the cache
    /// promotion path (which needs the whole file) and the ReplayGain
    /// probe (which needs the header) read from here.
    fn head_run(&self) -> Option<&BufferSegment> {
        self.segments.first().filter(|seg| seg.offset == 0)
    }

    /// Drop buffered bytes well BEHIND the read head, so a long track cannot
    /// grow the buffer without bound.
    ///
    /// The buffer used to hold the entire file. At 44.1 kHz that is 30-60 MB and
    /// nobody notices; at 24/192 a long movement is 450-500 MB, and on a 1 GB
    /// player that pushes the daemon into swap on the SD card — measured at
    /// 118 MB paged out, with bursts of ALSA underruns as the audio thread
    /// faulted pages back in. Audible as hiccups.
    ///
    /// What survives:
    /// * the first `keep_head` bytes, always — FLAC tags (ReplayGain) live at
    ///   the start of the file and `get_buffered_data` reads them from there.
    /// * everything from `read_pos - keep_behind` onward, so a small backward
    ///   seek is still served from memory. A jump further back than that
    ///   re-requests over HTTP, which the range-capable seek path already does
    ///   when the target is outside the buffer.
    ///
    /// Trimming leaves a hole, so `take_complete_data` stops returning the whole
    /// file and promotion is skipped. That is the same path a low-memory host has
    /// always taken and is handled: gapless keys on the DOWNLOAD being complete
    /// (`is_complete`), not on still holding every byte.
    ///
    /// Returns the bytes released.
    fn trim_behind(&mut self, read_pos: u64, keep_behind: u64, keep_head: u64) -> u64 {
        let cutoff = read_pos.saturating_sub(keep_behind);
        if cutoff == 0 {
            return 0;
        }
        let before = self.downloaded();

        for seg in &mut self.segments {
            // The head run keeps its first `keep_head` bytes for tags.
            let protected_until = if seg.offset == 0 { keep_head } else { 0 };
            let drop_until = cutoff.min(seg.end()).max(protected_until);
            if drop_until <= seg.offset {
                continue;
            }
            let drop_len = (drop_until - seg.offset) as usize;
            if drop_len >= seg.data.len() {
                seg.data.clear();
                seg.data.shrink_to_fit();
                seg.offset = seg.end();
                continue;
            }
            seg.data.drain(..drop_len);
            seg.data.shrink_to_fit();
            seg.offset += drop_len as u64;
        }
        self.segments.retain(|seg| !seg.data.is_empty());

        before.saturating_sub(self.downloaded())
    }
}

/// Buffer plus the two wake-ups around it: `ready` for the synchronous
/// readers on the audio thread, `wanted` for the async feeder task.
struct SharedBuffer {
    state: Mutex<BufferState>,
    ready: Condvar,
    wanted: Notify,
}

impl SharedBuffer {
    fn lock(&self) -> IoResult<std::sync::MutexGuard<'_, BufferState>> {
        self.state
            .lock()
            .map_err(|_| IoError::new(ErrorKind::Other, "Failed to acquire buffer lock"))
    }

    /// Block until the buffer changes: data pushed, an error recorded, or
    /// the feeder reporting itself done.
    fn wait<'a>(
        &'a self,
        guard: std::sync::MutexGuard<'a, BufferState>,
    ) -> IoResult<std::sync::MutexGuard<'a, BufferState>> {
        self.ready
            .wait(guard)
            .map_err(|_| IoError::new(ErrorKind::Other, "Condition variable wait failed"))
    }
}

/// A byte range the feeder should fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchPlan {
    /// First byte to fetch, inclusive.
    pub offset: u64,
    /// One past the last byte to fetch, or `None` to read to EOF.
    pub end: Option<u64>,
}

impl FetchPlan {
    /// Value for an HTTP `Range` header: `bytes=1234-` or `bytes=10-99`.
    pub fn range_header(&self) -> String {
        match self.end {
            Some(end) if end > self.offset => format!("bytes={}-{}", self.offset, end - 1),
            _ => format!("bytes={}-", self.offset),
        }
    }

    /// True when a plain unranged GET would satisfy this plan.
    pub fn is_whole_file(&self) -> bool {
        self.offset == 0 && self.end.is_none()
    }

    /// Bytes this plan asks for, or `None` when it runs to EOF.
    pub fn byte_len(&self) -> Option<u64> {
        self.end.map(|end| end.saturating_sub(self.offset))
    }
}

/// A media source that buffers from an async HTTP stream.
///
/// Provides `Read + Seek` interface for decoders while data is still downloading.
/// The source is created with a `BufferWriter` that receives chunks from the
/// download task.
pub struct BufferedMediaSource {
    shared: Arc<SharedBuffer>,
    config: StreamingConfig,
    /// Each reader has its own read position
    read_pos: AtomicU64,
    /// This reader's id, against `BufferState::reader_epoch`.
    reader_id: u64,
}

impl BufferedMediaSource {
    /// Create a new buffered source whose feeder can only produce bytes in
    /// order (see [`StreamSeekMode::Sequential`]).
    ///
    /// Returns the source and a writer for pushing downloaded chunks.
    /// The writer should be used from the async download task.
    pub fn new(config: StreamingConfig, total_size: Option<u64>) -> (Self, BufferWriter) {
        Self::with_seek_mode(config, total_size, StreamSeekMode::Sequential)
    }

    /// Create a new buffered source whose feeder honors range requests, so
    /// reads may jump anywhere in the file for the cost of one request.
    pub fn new_seekable(config: StreamingConfig, total_size: Option<u64>) -> (Self, BufferWriter) {
        Self::with_seek_mode(config, total_size, StreamSeekMode::RangeRequests)
    }

    fn with_seek_mode(
        config: StreamingConfig,
        total_size: Option<u64>,
        seek_mode: StreamSeekMode,
    ) -> (Self, BufferWriter) {
        let shared = Arc::new(SharedBuffer {
            state: Mutex::new(BufferState {
                segments: Vec::new(),
                write_pos: 0,
                range_start: 0,
                download_complete: false,
                download_error: None,
                total_size,
                primary_offset: 0,
                pending_request: None,
                seek_mode,
                readers: 1,
                reader_epoch: 0,
            }),
            ready: Condvar::new(),
            wanted: Notify::new(),
        });

        let source = Self {
            shared: Arc::clone(&shared),
            config: config.clone(),
            read_pos: AtomicU64::new(0),
            reader_id: 0,
        };

        let writer = BufferWriter { shared };

        (source, writer)
    }

    /// Create a new reader that shares the same buffer but has its own read position.
    /// This is used to pass to symphonia which needs ownership of the reader.
    pub fn create_reader(&self) -> Self {
        let mut reader_id = 0;
        if let Ok(mut state) = self.shared.state.lock() {
            state.readers += 1;
            state.reader_epoch += 1;
            reader_id = state.reader_epoch;
        }
        Self {
            shared: Arc::clone(&self.shared),
            config: self.config.clone(),
            read_pos: AtomicU64::new(0),
            reader_id,
        }
    }

    /// Whether a seek on this source can jump straight to its target
    /// instead of waiting for the download to reach it.
    pub fn supports_range_requests(&self) -> bool {
        self.shared
            .state
            .lock()
            .map(|state| state.seek_mode == StreamSeekMode::RangeRequests)
            .unwrap_or(false)
    }

    /// Wait until initial buffer is filled or download completes.
    ///
    /// This should be called before passing the source to the decoder,
    /// to ensure enough data is available for format detection.
    ///
    /// Returns error if download fails before initial buffer is filled.
    pub fn wait_for_initial_buffer(&self) -> IoResult<()> {
        let mut state = self.shared.lock()?;

        while state.contiguous_from(state.primary_offset) < self.config.initial_buffer_bytes as u64
            && !state.download_complete
            && state.download_error.is_none()
        {
            state = self.shared.wait(state)?;
        }

        if let Some(ref err) = state.download_error {
            return Err(IoError::new(ErrorKind::Other, err.clone()));
        }

        Ok(())
    }

    /// Check if the feeder is done and left nothing missing (full file in
    /// buffer). A range feeder only reports completion once every gap is
    /// filled, so this keeps meaning "the whole track is here".
    pub fn is_complete(&self) -> bool {
        if let Ok(state) = self.shared.state.lock() {
            state.download_complete && state.download_error.is_none()
        } else {
            false
        }
    }

    /// Get total buffered bytes, across every downloaded run.
    pub fn buffer_size(&self) -> usize {
        if let Ok(state) = self.shared.state.lock() {
            state.downloaded() as usize
        } else {
            0
        }
    }

    /// Bytes buffered contiguously from the start of the file. This is what
    /// the format probe reads, so it is the gate for building a decoder —
    /// as opposed to [`has_min_buffer`], which measures from wherever
    /// playback is about to start.
    pub fn head_bytes(&self) -> u64 {
        if let Ok(state) = self.shared.state.lock() {
            state.contiguous_from(0)
        } else {
            0
        }
    }

    /// Offset playback reads from: 0, or the target of the last range
    /// request. Buffer-fill checks are measured from here.
    pub fn primary_offset(&self) -> u64 {
        if let Ok(state) = self.shared.state.lock() {
            state.primary_offset
        } else {
            0
        }
    }

    /// Get the complete data if download finished successfully.
    ///
    /// Used to store in cache after streaming playback completes.
    /// Returns None if the download is not complete, failed, or (after a
    /// range jump the feeder never backfilled) has a hole in it.
    ///
    /// IMPORTANT: This clones the buffer rather than moving it. Earlier
    /// attempts to use `mem::take` here regressed playback — the
    /// `Source` impl is still actively reading from the buffer when
    /// the promotion path calls this, and zeroing the buffer out from
    /// under the reader caused immediate EOF (tracks ending 10s into a
    /// 104s file). Cloning is the safe choice; the audible hiccup at
    /// promotion that the move attempted to fix needs a different
    /// approach (shared `Arc<Vec<u8>>` ownership, or off-thread copy).
    /// The whole track's bytes, as the shared [`TrackBytes`] every consumer
    /// downstream expects.
    ///
    /// Returns `TrackBytes` rather than `Vec<u8>` on purpose. A Hi-Res track is
    /// 200 MB+, and the old signature cost TWO full copies of it: the `Vec`
    /// clone here, then `Arc<[u8]>` allocating and memcpying that `Vec` at the
    /// call site (`Arc::from(Vec)` never adopts the buffer). Building the Arc
    /// once, straight from the buffer's slice, halves the peak — which on a 1 GB
    /// Pi is the difference between promoting a track and being OOM-killed.
    pub fn take_complete_data(&self) -> Option<TrackBytes> {
        let state = self.shared.state.lock().ok()?;
        if !state.download_complete || state.download_error.is_some() {
            return None;
        }
        if state.segments.len() != 1 {
            // Holes left behind by a range jump: not a file we can hand to
            // the cache or replay in memory.
            return None;
        }
        state
            .head_run()
            .map(|seg| TrackBytes::from(seg.data.as_slice()))
    }

    /// Get a copy of the buffered file header (for metadata extraction).
    ///
    /// Returns the contiguous run from byte 0, even if incomplete. Useful
    /// for extracting file-level metadata (e.g., ReplayGain tags) which are
    /// typically in the first few KB of the file.
    pub fn get_buffered_data(&self) -> Option<Vec<u8>> {
        let state = self.shared.state.lock().ok()?;
        state
            .head_run()
            .filter(|seg| !seg.data.is_empty())
            .map(|seg| seg.data.clone())
    }

    /// Get download progress as a fraction (0.0 to 1.0)
    ///
    /// Returns None if total size is unknown
    pub fn progress(&self) -> Option<f32> {
        let state = self.shared.state.lock().ok()?;
        state.total_size.map(|total| {
            if total == 0 {
                1.0
            } else {
                state.downloaded() as f32 / total as f32
            }
        })
    }

    /// Check if minimum buffer for playback is available
    ///
    /// Returns true when `initial_buffer_bytes` are buffered contiguously
    /// from the offset playback starts at, or the download is complete.
    pub fn has_min_buffer(&self) -> bool {
        if let Ok(state) = self.shared.state.lock() {
            state.contiguous_from(state.primary_offset) >= self.config.initial_buffer_bytes as u64
                || state.download_complete
        } else {
            false
        }
    }

    /// Error reported by the feeder, if any. Lets waiters (the initial
    /// buffer fill loop) bail out immediately instead of sitting through
    /// the full buffer timeout when the feeder has already died.
    pub fn download_error(&self) -> Option<String> {
        self.shared
            .state
            .lock()
            .ok()
            .and_then(|state| state.download_error.clone())
    }

    /// Ask the feeder to re-open the body at `pos`, when that is both
    /// possible and cheaper than waiting for the bytes already on their
    /// way. Called with the state lock held.
    fn request_range(&self, state: &mut BufferState, pos: u64) {
        if self.reader_id != state.reader_epoch {
            // A superseded decoder, still draining before it is dropped.
            return;
        }
        if !state.should_request(pos) {
            return;
        }
        // Reading backwards into a region we skipped means the decoder is
        // bisecting, and the walk carries on downward from here — so fetch
        // from a little earlier and let the rest of it land in buffered
        // bytes. See SEEK_LOOKBEHIND_BYTES.
        let walking_back = pos < state.range_start;
        let fetch_from = if walking_back {
            pos.saturating_sub(SEEK_LOOKBEHIND_BYTES)
        } else {
            pos
        };
        log::info!(
            "Streaming buffer: requesting range from byte {}{} (download head at {}, {} bytes held)",
            fetch_from,
            if walking_back {
                format!(" for a backward probe at {pos}")
            } else {
                String::new()
            },
            state.write_pos,
            state.downloaded()
        );
        state.primary_offset = pos;
        state.pending_request = Some(fetch_from);
        self.shared.wanted.notify_one();
    }
}

impl Drop for BufferedMediaSource {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.readers = state.readers.saturating_sub(1);
            if state.readers == 0 {
                // Let a feeder parked on `next_plan` notice it has nobody
                // left to serve and stop backfilling.
                self.shared.wanted.notify_one();
            }
        }
    }
}

impl Read for BufferedMediaSource {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut state = self.shared.lock()?;
        let read_pos = self.read_pos.load(Ordering::SeqCst);

        loop {
            if let Some(ref err) = state.download_error {
                return Err(IoError::new(ErrorKind::Other, err.clone()));
            }

            if let Some(idx) = state.segment_at(read_pos) {
                let seg = &state.segments[idx];
                let from = (read_pos - seg.offset) as usize;
                let to_read = buf.len().min(seg.data.len() - from);
                buf[..to_read].copy_from_slice(&seg.data[from..from + to_read]);
                self.read_pos
                    .store(read_pos + to_read as u64, Ordering::SeqCst);
                return Ok(to_read);
            }

            if state.at_eof(read_pos) {
                return Ok(0);
            }

            if state.download_complete {
                // A hole no feeder will ever fill: completion is only
                // reported once nothing is missing, so the buffer must have
                // been torn down under us.
                return Err(IoError::new(
                    ErrorKind::UnexpectedEof,
                    format!("stream byte {read_pos} is not buffered and the feeder has stopped"),
                ));
            }

            // Either wait for the download head to reach us, or (range
            // feeders only) make it come to us.
            self.request_range(&mut state, read_pos);
            state = self.shared.wait(state)?;
        }
    }
}

impl Seek for BufferedMediaSource {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        let mut state = self.shared.lock()?;

        let current_pos = self.read_pos.load(Ordering::SeqCst) as i64;

        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::Current(offset) => current_pos + offset,
            SeekFrom::End(offset) => {
                // For End seeks, we need to know total size or have complete download
                if let Some(total) = state.total_size {
                    total as i64 + offset
                } else if state.download_complete {
                    state.write_pos as i64 + offset
                } else {
                    // Can't seek from end without knowing size
                    return Err(IoError::new(
                        ErrorKind::Unsupported,
                        "Cannot seek from end while streaming without known size",
                    ));
                }
            }
        };

        if new_pos < 0 {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Seek position before start of stream",
            ));
        }

        let new_pos = new_pos as u64;

        // Block until the target byte is readable. On a range feeder the
        // first iteration turns that into a request for exactly this
        // offset, so the wait is one round trip instead of however much
        // file sits in between.
        while state.segment_at(new_pos).is_none()
            && !state.at_eof(new_pos)
            && !state.download_complete
            && state.download_error.is_none()
        {
            self.request_range(&mut state, new_pos);
            state = self.shared.wait(state)?;
        }

        if let Some(ref err) = state.download_error {
            return Err(IoError::new(ErrorKind::Other, err.clone()));
        }

        // After download complete, check bounds
        if state.download_complete && state.segment_at(new_pos).is_none() && !state.at_eof(new_pos)
        {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Seek position beyond end of stream",
            ));
        }

        self.read_pos.store(new_pos, Ordering::SeqCst);
        Ok(new_pos)
    }
}

// Required for symphonia MediaSource trait
impl MediaSource for BufferedMediaSource {
    fn is_seekable(&self) -> bool {
        // We support seeking within buffered data
        true
    }

    fn byte_len(&self) -> Option<u64> {
        if let Ok(state) = self.shared.state.lock() {
            state.total_size
        } else {
            None
        }
    }
}

/// Writer half for pushing downloaded chunks from the async download task.
///
/// This is the sender side that receives data from the HTTP response
/// and makes it available to the `BufferedMediaSource` reader.
///
/// A range-capable feeder drives its loop from this half: [`initial_plan`]
/// for the first body, [`take_request`] polled between chunks to notice a
/// reader jumping, and [`next_plan`] awaited when a body ends to pick up
/// the next request or backfill a hole.
#[derive(Clone)]
pub struct BufferWriter {
    shared: Arc<SharedBuffer>,
}

impl BufferWriter {
    /// Push a chunk of downloaded data at the write head.
    ///
    /// This wakes up any readers waiting for data.
    pub fn push_chunk(&self, chunk: &[u8]) -> Result<(), String> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| "Failed to acquire buffer lock")?;

        let at = state.write_pos;
        state.insert(at, chunk);
        state.write_pos = at + chunk.len() as u64;
        self.shared.ready.notify_all();

        Ok(())
    }

    /// Point the write head at `offset` before pushing a body's bytes.
    ///
    /// The plan-handing calls already do this, so a feeder only needs it to
    /// correct course — a server that answers a ranged GET with `200` and
    /// the whole file has handed us bytes starting at 0, not at the offset
    /// we asked for.
    pub fn begin_at(&self, offset: u64) -> Result<(), String> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| "Failed to acquire buffer lock")?;
        state.begin(offset);
        Ok(())
    }

    /// The first body a range feeder should open: the whole file, so the
    /// format header and seek table land before anything else.
    pub fn initial_plan(&self) -> FetchPlan {
        FetchPlan {
            offset: 0,
            end: None,
        }
    }

    /// A reader's range request, if one arrived. Feeders poll this between
    /// chunks and restart their body when it fires; the returned plan is
    /// already registered as the write head, so readers stop asking.
    pub fn take_request(&self) -> Option<FetchPlan> {
        let mut state = self.shared.state.lock().ok()?;
        let offset = state.pending_request.take()?;
        state.begin(offset);
        Some(FetchPlan { offset, end: None })
    }

    /// The next body to fetch once the current one ends: a reader's range
    /// request first, then any hole left behind by an earlier jump so the
    /// track still ends up whole (and cacheable).
    ///
    /// Returns `None` when there is nothing left to fetch — at which point
    /// readers are told the download is complete — or when the last reader
    /// has gone away. No reader can ask for a byte it could already read,
    /// so "no holes left" really is the end of the feeder's work.
    pub fn next_plan(&self) -> Option<FetchPlan> {
        let mut state = self.shared.state.lock().ok()?;
        if state.readers == 0 || state.download_error.is_some() {
            return None;
        }
        if let Some(offset) = state.pending_request.take() {
            state.begin(offset);
            return Some(FetchPlan { offset, end: None });
        }
        if state.seek_mode == StreamSeekMode::RangeRequests {
            if let Some((offset, end)) = state.first_gap() {
                state.begin(offset);
                return Some(FetchPlan { offset, end });
            }
        }
        if !state.download_complete {
            state.download_complete = true;
            self.shared.ready.notify_all();
        }
        None
    }

    /// Resolves when a reader posts a range request, or when the last
    /// reader goes away. A feeder awaiting its next chunk selects on this
    /// so a jump is honored immediately instead of after the next chunk —
    /// which matters most on exactly the slow connection where a stalled
    /// body would otherwise pin the seek.
    pub async fn request_notified(&self) {
        self.shared.wanted.notified().await;
    }

    /// Mark download as complete
    ///
    /// After this is called, readers will receive EOF after reading all buffered data.
    pub fn complete(&self) -> Result<(), String> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| "Failed to acquire buffer lock")?;

        state.download_complete = true;
        self.shared.ready.notify_all();

        Ok(())
    }

    /// Mark download as failed
    ///
    /// After this is called, readers will receive the error on next read.
    /// The first recorded error wins: it is the root cause, and the feeder
    /// fail-guards fire a generic "aborted" error on drop after a specific
    /// failure has already been recorded, which must not overwrite it.
    pub fn error(&self, err: String) -> Result<(), String> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| "Failed to acquire buffer lock")?;

        if state.download_error.is_none() {
            state.download_error = Some(err);
        }
        self.shared.ready.notify_all();
        self.shared.wanted.notify_one();

        Ok(())
    }

    /// Get total buffered bytes, across every downloaded run.
    pub fn buffer_size(&self) -> usize {
        if let Ok(state) = self.shared.state.lock() {
            state.downloaded() as usize
        } else {
            0
        }
    }
}

// =============================================================================
// IncrementalStreamingSource - A rodio Source that decodes on-demand
// =============================================================================

/// A rodio Source that decodes audio packets incrementally from a BufferedMediaSource.
///
/// This allows playback to start immediately after the initial buffer is filled,
/// while the rest of the file continues downloading in the background.
///
/// The source maintains an internal queue of decoded samples and decodes more
/// packets on-demand as samples are consumed.
pub struct IncrementalStreamingSource {
    /// Sample rate of the audio
    sample_rate: u32,
    /// Number of channels
    channels: u16,
    /// Queue of decoded samples ready to play
    sample_queue: VecDeque<f32>,
    /// The format reader (demuxer)
    format: Box<dyn FormatReader>,
    /// The audio decoder
    decoder: Box<dyn Decoder>,
    /// Track ID we're decoding
    track_id: u32,
    /// Whether we've reached end of stream
    finished: bool,
    /// Number of packets decoded (for stats)
    packets_decoded: u64,
    /// True while inside a WouldBlock stall episode (playback caught up
    /// with the download). Set on the first WouldBlock after at least one
    /// decoded packet, cleared on the next successful decode — so each
    /// episode records exactly one underrun with the network throttle.
    stalled: bool,
    /// Reference to the buffered source (for cache retrieval after playback)
    buffered_source: Arc<BufferedMediaSource>,
}

impl IncrementalStreamingSource {
    /// Create a new incremental streaming source.
    ///
    /// This initializes the symphonia decoder and prepares for incremental decoding.
    /// The BufferedMediaSource should already have its initial buffer filled.
    ///
    /// Returns the source along with detected sample_rate and channels.
    pub fn new(buffered_source: Arc<BufferedMediaSource>) -> Result<Self, String> {
        // Create a reader from the buffered source
        let reader = buffered_source.create_reader();
        let media_source = Box::new(reader) as Box<dyn MediaSource>;
        let mss = MediaSourceStream::new(media_source, Default::default());

        let mut hint = Hint::new();
        hint.with_extension("flac"); // Most Qobuz Hi-Res is FLAC

        let format_opts = FormatOptions {
            enable_gapless: true,
            ..Default::default()
        };
        let metadata_opts: MetadataOptions = Default::default();

        let probed = get_probe()
            .format(&hint, mss, &format_opts, &metadata_opts)
            .map_err(|err| format!("Symphonia probe failed for streaming: {}", err))?;

        let track = probed
            .format
            .default_track()
            .ok_or_else(|| "Symphonia: no supported audio tracks in stream".to_string())?;

        let track_id = track.id;
        let codec_params = track.codec_params.clone();

        // Extract sample rate and channels from codec params
        let sample_rate = codec_params
            .sample_rate
            .ok_or_else(|| "No sample rate in codec params".to_string())?;
        let channels = codec_params.channels.map(|c| c.count() as u16).unwrap_or(2);

        let decoder = get_codecs()
            .make(&codec_params, &DecoderOptions::default())
            .map_err(|err| format!("Symphonia decoder init failed for streaming: {}", err))?;

        log::info!(
            "IncrementalStreamingSource initialized: {}Hz, {} channels",
            sample_rate,
            channels
        );

        Ok(Self {
            sample_rate,
            channels,
            sample_queue: VecDeque::with_capacity(sample_rate as usize * channels as usize), // ~1s buffer
            format: probed.format,
            decoder,
            track_id,
            finished: false,
            packets_decoded: 0,
            stalled: false,
            buffered_source,
        })
    }

    /// Get the sample rate
    pub fn get_sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Get the number of channels
    pub fn get_channels(&self) -> u16 {
        self.channels
    }

    /// Get reference to the buffered source for cache retrieval
    pub fn buffered_source(&self) -> &Arc<BufferedMediaSource> {
        &self.buffered_source
    }

    /// Seek the decoder to the given time using Symphonia's native seek.
    ///
    /// For FLAC this uses the seek table to jump directly to the nearest
    /// seek point, then decodes forward to the exact sample — far cheaper
    /// than skip_duration's decode-every-sample-from-zero path. For MP3
    /// with Xing/VBRI headers it uses the TOC; without headers, Symphonia
    /// falls back to a binary search, still much cheaper than linear decode.
    ///
    /// The underlying BufferedMediaSource::seek is the I/O target. On a
    /// range-capable source (see [`StreamSeekMode`]) an unbuffered offset
    /// turns into a `Range` request and the block lasts one round trip, so
    /// any time in the track is fair game. On a sequential source it blocks
    /// until the download walks there — callers must only invoke it for
    /// times within the downloaded watermark.
    pub fn seek_to(&mut self, time: Duration) -> Result<(), String> {
        self.format
            .seek(
                SeekMode::Accurate,
                SeekTo::Time {
                    time: time.into(),
                    track_id: Some(self.track_id),
                },
            )
            .map_err(|e| format!("Symphonia seek failed: {}", e))?;
        self.decoder.reset();
        self.sample_queue.clear();
        self.packets_decoded = 0;
        self.finished = false;
        Ok(())
    }

    /// Decode more packets to fill the sample queue.
    ///
    /// This is called when the sample queue is running low.
    /// It will decode packets until the queue has at least `min_samples` or EOF is reached.
    fn decode_more(&mut self, min_samples: usize) {
        if self.finished {
            return;
        }

        while self.sample_queue.len() < min_samples {
            let packet = match self.format.next_packet() {
                Ok(packet) => packet,
                Err(SymphoniaError::IoError(ref e))
                    if e.kind() == std::io::ErrorKind::WouldBlock =>
                {
                    // Not enough data buffered yet - wait briefly and retry
                    // This happens when playback catches up with download
                    if !self.stalled && self.packets_decoded > 0 {
                        // Mid-playback stall, not initial buffering (≥1 packet
                        // already decoded): put the prefetch throttle in panic
                        // mode so the live stream gets the pipe to itself (#591).
                        self.stalled = true;
                        qbz_audio::network_throttle::state().record_underrun();
                    }
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(SymphoniaError::IoError(_)) => {
                    // EOF or other IO error
                    log::info!(
                        "IncrementalStreamingSource: EOF reached after {} packets",
                        self.packets_decoded
                    );
                    self.finished = true;
                    return;
                }
                Err(err) => {
                    log::error!("Symphonia read error in stream: {}", err);
                    self.finished = true;
                    return;
                }
            };

            if packet.track_id() != self.track_id {
                continue;
            }

            match self.decoder.decode(&packet) {
                Ok(audio_buf) => {
                    let spec = *audio_buf.spec();
                    let mut sample_buf = SampleBuffer::<f32>::new(audio_buf.frames() as u64, spec);
                    sample_buf.copy_interleaved_ref(audio_buf);

                    // Add samples to queue
                    self.sample_queue
                        .extend(sample_buf.samples().iter().copied());
                    self.packets_decoded += 1;
                    // Successful decode ends any stall episode; the next
                    // WouldBlock streak records a fresh underrun.
                    self.stalled = false;
                }
                Err(SymphoniaError::DecodeError(e)) => {
                    log::warn!("Decode error (skipping packet): {}", e);
                    continue;
                }
                Err(SymphoniaError::ResetRequired) => {
                    self.decoder.reset();
                    continue;
                }
                Err(err) => {
                    log::error!("Symphonia decode error: {}", err);
                    self.finished = true;
                    return;
                }
            }
        }
    }
}

impl Source for IncrementalStreamingSource {
    fn current_span_len(&self) -> Option<usize> {
        // We don't know frame boundaries in the queue
        None
    }

    fn channels(&self) -> std::num::NonZero<u16> {
        std::num::NonZero::new(self.channels).unwrap()
    }

    fn sample_rate(&self) -> std::num::NonZero<u32> {
        std::num::NonZero::new(self.sample_rate).unwrap()
    }

    fn total_duration(&self) -> Option<Duration> {
        // We don't know total duration until download completes
        // Could estimate from content-length if available
        None
    }
}

impl Iterator for IncrementalStreamingSource {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        // If queue is running low, decode more
        // Keep at least 0.5 seconds of audio buffered
        let min_buffer = (self.sample_rate as usize * self.channels as usize) / 2;
        if self.sample_queue.len() < min_buffer {
            self.decode_more(min_buffer);
        }

        self.sample_queue.pop_front()
    }
}

/// Cursor-backed MediaSource for in-memory audio data.
struct InMemoryMediaSource {
    inner: Cursor<TrackBytes>,
    len: u64,
}

impl InMemoryMediaSource {
    fn new(data: TrackBytes) -> Self {
        let len = data.len() as u64;
        Self {
            inner: Cursor::new(data),
            len,
        }
    }
}

impl Read for InMemoryMediaSource {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.inner.read(buf)
    }
}

impl Seek for InMemoryMediaSource {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.inner.seek(pos)
    }
}

impl MediaSource for InMemoryMediaSource {
    fn is_seekable(&self) -> bool {
        true
    }

    fn byte_len(&self) -> Option<u64> {
        Some(self.len)
    }
}

/// Symphonia-backed decoder for fully-in-memory audio bytes, with native
/// seek support.
///
/// Exists because rodio's `skip_duration` decodes every sample from the
/// start of the track when seeking, which on FLAC Hi-Res costs several
/// seconds of CPU for long jumps and stalls the audio thread. This
/// source uses `FormatReader::seek(Accurate, SeekTo::Time)` — FLAC seek
/// table, MP3 Xing/VBRI TOC — to jump straight to the target sample, so
/// the post-seek decode window is ~O(seek point density) instead of
/// O(position).
///
/// Non-Symphonia formats (notably rodio's native MP4/AAC path) aren't
/// supported here; callers must fall back to `decode_with_fallback` +
/// `skip_duration` when `new` returns `Err`.
pub struct InMemorySource {
    sample_rate: u32,
    channels: u16,
    sample_queue: VecDeque<f32>,
    format: Box<dyn FormatReader>,
    decoder: Box<dyn Decoder>,
    track_id: u32,
    finished: bool,
}

impl InMemorySource {
    pub fn new(data: TrackBytes) -> Result<Self, String> {
        let source = Box::new(InMemoryMediaSource::new(data)) as Box<dyn MediaSource>;
        Self::from_media_source(source, "in-memory source")
    }

    /// The same decoder over a FILE, for a track handed over as a path rather
    /// than a buffer (the disk gapless path, taken whenever two of the track
    /// would not fit the L1 budget).
    ///
    /// Exists so seeking such a track is a SEEK. rodio's generic `try_seek`
    /// succeeds on these files but, with no SEEKTABLE — and Qobuz FLACs carry
    /// none — it estimates and then DECODES FORWARD to the target: measured at
    /// 7.4 s to reach 676 s of a 96 kHz track on a Pi, against 130-215 ms for
    /// the same seek on the in-memory streaming path. Symphonia over a seekable
    /// `MediaSource` bisects instead, which on local storage is a handful of
    /// small reads.
    pub fn from_file(path: &std::path::Path) -> Result<Self, String> {
        let file = std::fs::File::open(path)
            .map_err(|e| format!("open {} for seeking: {e}", path.display()))?;
        // symphonia implements `MediaSource` for `File`, and reports it
        // seekable — which is the whole point.
        Self::from_media_source(Box::new(file) as Box<dyn MediaSource>, "cached file")
    }

    fn from_media_source(source: Box<dyn MediaSource>, what: &str) -> Result<Self, String> {
        let mss = MediaSourceStream::new(source, Default::default());

        let hint = Hint::new();

        let format_opts = FormatOptions {
            enable_gapless: true,
            ..Default::default()
        };
        let metadata_opts: MetadataOptions = Default::default();

        let probed = get_probe()
            .format(&hint, mss, &format_opts, &metadata_opts)
            .map_err(|err| format!("Symphonia probe failed for {what}: {err}"))?;

        let track = probed
            .format
            .default_track()
            .ok_or_else(|| "Symphonia: no supported audio tracks".to_string())?;

        let track_id = track.id;
        let codec_params = track.codec_params.clone();

        let sample_rate = codec_params
            .sample_rate
            .ok_or_else(|| "No sample rate in codec params".to_string())?;
        let channels = codec_params.channels.map(|c| c.count() as u16).unwrap_or(2);

        let decoder = get_codecs()
            .make(&codec_params, &DecoderOptions::default())
            .map_err(|err| format!("Symphonia decoder init failed: {}", err))?;

        Ok(Self {
            sample_rate,
            channels,
            sample_queue: VecDeque::with_capacity(sample_rate as usize * channels as usize),
            format: probed.format,
            decoder,
            track_id,
            finished: false,
        })
    }

    pub fn seek_to(&mut self, time: Duration) -> Result<(), String> {
        self.format
            .seek(
                SeekMode::Accurate,
                SeekTo::Time {
                    time: time.into(),
                    track_id: Some(self.track_id),
                },
            )
            .map_err(|e| format!("Symphonia in-memory seek failed: {}", e))?;
        self.decoder.reset();
        self.sample_queue.clear();
        self.finished = false;
        Ok(())
    }

    fn decode_more(&mut self, min_samples: usize) {
        if self.finished {
            return;
        }

        while self.sample_queue.len() < min_samples {
            let packet = match self.format.next_packet() {
                Ok(packet) => packet,
                Err(SymphoniaError::IoError(_)) => {
                    self.finished = true;
                    return;
                }
                Err(err) => {
                    log::error!("Symphonia read error in in-memory source: {}", err);
                    self.finished = true;
                    return;
                }
            };

            if packet.track_id() != self.track_id {
                continue;
            }

            match self.decoder.decode(&packet) {
                Ok(audio_buf) => {
                    let spec = *audio_buf.spec();
                    let mut sample_buf = SampleBuffer::<f32>::new(audio_buf.frames() as u64, spec);
                    sample_buf.copy_interleaved_ref(audio_buf);
                    self.sample_queue
                        .extend(sample_buf.samples().iter().copied());
                }
                Err(SymphoniaError::DecodeError(e)) => {
                    log::warn!("Decode error (skipping packet): {}", e);
                    continue;
                }
                Err(SymphoniaError::ResetRequired) => {
                    self.decoder.reset();
                    continue;
                }
                Err(err) => {
                    log::error!("Symphonia decode error: {}", err);
                    self.finished = true;
                    return;
                }
            }
        }
    }
}

impl Source for InMemorySource {
    fn current_span_len(&self) -> Option<usize> {
        None
    }

    fn channels(&self) -> std::num::NonZero<u16> {
        std::num::NonZero::new(self.channels).unwrap()
    }

    fn sample_rate(&self) -> std::num::NonZero<u32> {
        std::num::NonZero::new(self.sample_rate).unwrap()
    }

    fn total_duration(&self) -> Option<Duration> {
        None
    }
}

impl Iterator for InMemorySource {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        let min_buffer = (self.sample_rate as usize * self.channels as usize) / 2;
        if self.sample_queue.len() < min_buffer {
            self.decode_more(min_buffer);
        }
        self.sample_queue.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    /// A mono 16-bit PCM WAV whose samples ramp linearly from 0 to `peak`
    /// across the whole file, so a decoded sample's VALUE says where in the
    /// file it came from — which is what makes a seek testable.
    fn ramp_wav(sample_rate: u32, total_frames: u32, peak: i16) -> Vec<u8> {
        let data_len = total_frames * 2;
        let mut w = Vec::with_capacity(44 + data_len as usize);
        w.extend_from_slice(b"RIFF");
        w.extend_from_slice(&(36 + data_len).to_le_bytes());
        w.extend_from_slice(b"WAVEfmt ");
        w.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk size
        w.extend_from_slice(&1u16.to_le_bytes()); // PCM
        w.extend_from_slice(&1u16.to_le_bytes()); // mono
        w.extend_from_slice(&sample_rate.to_le_bytes());
        w.extend_from_slice(&(sample_rate * 2).to_le_bytes()); // byte rate
        w.extend_from_slice(&2u16.to_le_bytes()); // block align
        w.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
        w.extend_from_slice(b"data");
        w.extend_from_slice(&data_len.to_le_bytes());
        for i in 0..total_frames {
            let v = (i as f64 / total_frames as f64 * peak as f64) as i16;
            w.extend_from_slice(&v.to_le_bytes());
        }
        w
    }

    fn temp_wav(name: &str, bytes: &[u8]) -> std::path::PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("qbz-{name}-{n}.wav"));
        std::fs::write(&path, bytes).unwrap();
        path
    }

    const RAMP_RATE: u32 = 8_000;
    const RAMP_SECS: u32 = 4;
    const RAMP_PEAK: i16 = 30_000;

    #[test]
    fn a_file_backed_source_decodes_from_the_start() {
        let path = temp_wav(
            "start",
            &ramp_wav(RAMP_RATE, RAMP_RATE * RAMP_SECS, RAMP_PEAK),
        );
        let mut src = InMemorySource::from_file(&path).unwrap();
        assert_eq!(src.sample_rate, RAMP_RATE);
        assert_eq!(src.channels, 1);
        // The ramp starts at zero, and the whole file is there.
        let first = src.next().unwrap();
        assert!(first.abs() < 0.01, "first sample was {first}");
        let total = 1 + src.count();
        assert_eq!(total as u32, RAMP_RATE * RAMP_SECS);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_file_backed_seek_lands_at_the_target_and_not_by_decoding_forward() {
        let path = temp_wav(
            "seek",
            &ramp_wav(RAMP_RATE, RAMP_RATE * RAMP_SECS, RAMP_PEAK),
        );
        let mut src = InMemorySource::from_file(&path).unwrap();
        src.seek_to(Duration::from_secs(2)).unwrap();

        // A seek lands on a packet boundary, so allow a quarter second of
        // slack — far tighter than the failure this guards against, which is
        // decoding forward from zero and therefore landing at 0.0.
        let total_frames = RAMP_RATE * RAMP_SECS;
        let slack_frames = RAMP_RATE / 4;
        let expected = (RAMP_PEAK as f32 / 2.0) / 32768.0;
        let tolerance = (slack_frames as f32 / total_frames as f32) * (RAMP_PEAK as f32 / 32768.0);
        let landed = src.next().unwrap();
        assert!(
            (landed - expected).abs() < tolerance,
            "seek to 2s of a 4s ramp gave {landed}, expected ~{expected} (+-{tolerance})"
        );
        // And only the remaining half of the file is left to play.
        let remaining = 1 + src.count();
        let want = (total_frames / 2) as usize;
        assert!(
            remaining.abs_diff(want) < slack_frames as usize,
            "{remaining} samples left after the seek, expected ~{want}"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_missing_or_unprobeable_file_is_an_error_not_a_panic() {
        let missing = std::env::temp_dir().join("qbz-does-not-exist-at-all.wav");
        let err = InMemorySource::from_file(&missing).err().unwrap();
        assert!(err.contains("qbz-does-not-exist-at-all"), "{err}");

        // Not audio at all: the caller falls back to rodio on this, so it has
        // to come back as an Err naming the file case.
        let path = temp_wav("garbage", b"this is not a media file");
        let err = InMemorySource::from_file(&path).err().unwrap();
        assert!(err.contains("cached file"), "{err}");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn first_error_wins_over_later_generic_abort() {
        let (source, writer) = BufferedMediaSource::new(StreamingConfig::from_seconds(1), None);
        writer.error("root cause".to_string()).unwrap();
        writer
            .error("CMAF stream aborted before completion".to_string())
            .unwrap();
        assert_eq!(source.download_error().as_deref(), Some("root cause"));
    }

    #[test]
    fn feeder_error_is_visible_to_waiters_before_min_buffer() {
        let (source, writer) = BufferedMediaSource::new(StreamingConfig::from_seconds(1), None);
        assert!(!source.has_min_buffer());
        assert!(source.download_error().is_none());
        writer.error("feeder died".to_string()).unwrap();
        // The initial-buffer wait loop polls this instead of sleeping out
        // the full buffer timeout.
        assert_eq!(source.download_error().as_deref(), Some("feeder died"));
        assert!(!source.has_min_buffer());
    }

    #[test]
    fn raw_initial_buffer_for_speed_follows_documented_ladder() {
        // Each band of the documented speed ladder produces its own size.
        assert_eq!(raw_initial_buffer_for_speed(20.0), 256 * 1024);
        assert_eq!(raw_initial_buffer_for_speed(10.0), 256 * 1024);
        assert_eq!(raw_initial_buffer_for_speed(7.0), 384 * 1024);
        assert_eq!(raw_initial_buffer_for_speed(5.0), 384 * 1024);
        assert_eq!(raw_initial_buffer_for_speed(3.0), 512 * 1024);
        assert_eq!(raw_initial_buffer_for_speed(2.0), 512 * 1024);
        assert_eq!(raw_initial_buffer_for_speed(1.5), 1024 * 1024);
        assert_eq!(raw_initial_buffer_for_speed(1.0), 1024 * 1024);
        assert_eq!(raw_initial_buffer_for_speed(0.5), 2 * 1024 * 1024);
        assert_eq!(raw_initial_buffer_for_speed(0.0), 2 * 1024 * 1024);
    }

    #[test]
    fn from_speed_mbps_with_cap_passes_through_when_under_cap() {
        // Cap above the raw value: result equals the raw ladder.
        let cfg = StreamingConfig::from_speed_mbps_with_cap(0.0, 4 * 1024 * 1024);
        assert_eq!(cfg.initial_buffer_bytes, 2 * 1024 * 1024);
    }

    #[test]
    fn from_speed_mbps_with_cap_clamps_slow_connection_to_low_memory_cap() {
        // The case from issue #331: Pi 3B, slow connection because of swap
        // thrash, would otherwise inflate the buffer to 2 MB. With the
        // LowMemory profile's 256KB cap applied, we stay at 256KB.
        let cfg = StreamingConfig::from_speed_mbps_with_cap(0.0, 256 * 1024);
        assert_eq!(cfg.initial_buffer_bytes, 256 * 1024);

        let cfg = StreamingConfig::from_speed_mbps_with_cap(1.5, 256 * 1024);
        assert_eq!(cfg.initial_buffer_bytes, 256 * 1024);
    }

    #[test]
    fn from_speed_mbps_with_cap_no_op_for_normal_profile() {
        // Normal profile cap is 2 MB — equal to the slowest raw band, so
        // any raw value passes through unchanged.
        let cap = 2 * 1024 * 1024;
        for speed in [0.0, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0] {
            let cfg = StreamingConfig::from_speed_mbps_with_cap(speed, cap);
            assert_eq!(
                cfg.initial_buffer_bytes,
                raw_initial_buffer_for_speed(speed),
                "cap should not bind for speed={}",
                speed
            );
        }
    }

    #[test]
    fn from_speed_mbps_with_cap_max_buffer_unchanged() {
        // Whatever the cap, the secondary max_buffer_bytes stays at its
        // module default; we are only clamping the initial fill target.
        let cfg = StreamingConfig::from_speed_mbps_with_cap(0.5, 64 * 1024);
        assert_eq!(cfg.max_buffer_bytes, 100 * 1024 * 1024);
    }

    #[test]
    fn test_basic_read_write() {
        let config = StreamingConfig {
            initial_buffer_bytes: 10,
            max_buffer_bytes: 100,
        };
        let (mut source, writer) = BufferedMediaSource::new(config, Some(20));

        // Write some data
        writer.push_chunk(b"Hello").unwrap();
        writer.push_chunk(b"World").unwrap();

        // Read it back
        let mut buf = [0u8; 5];
        assert_eq!(source.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"Hello");

        assert_eq!(source.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"World");
    }

    #[test]
    fn test_seek_within_buffer() {
        let config = StreamingConfig {
            initial_buffer_bytes: 5,
            max_buffer_bytes: 100,
        };
        let (mut source, writer) = BufferedMediaSource::new(config, Some(10));

        writer.push_chunk(b"0123456789").unwrap();
        writer.complete().unwrap();

        // Read first 5 bytes
        let mut buf = [0u8; 5];
        source.read(&mut buf).unwrap();
        assert_eq!(&buf, b"01234");

        // Seek back to start
        source.seek(SeekFrom::Start(0)).unwrap();
        source.read(&mut buf).unwrap();
        assert_eq!(&buf, b"01234");

        // Seek to middle
        source.seek(SeekFrom::Start(3)).unwrap();
        source.read(&mut buf).unwrap();
        assert_eq!(&buf, b"34567");
    }

    #[test]
    fn test_complete_data_retrieval() {
        let config = StreamingConfig {
            initial_buffer_bytes: 5,
            max_buffer_bytes: 100,
        };
        let (source, writer) = BufferedMediaSource::new(config, Some(10));

        writer.push_chunk(b"Hello").unwrap();
        assert!(source.take_complete_data().is_none()); // Not complete yet

        writer.push_chunk(b"World").unwrap();
        writer.complete().unwrap();

        let data = source.take_complete_data().unwrap();
        assert_eq!(data.as_ref(), b"HelloWorld");
    }

    #[test]
    fn test_blocking_read() {
        let config = StreamingConfig {
            initial_buffer_bytes: 5,
            max_buffer_bytes: 100,
        };
        let (mut source, writer) = BufferedMediaSource::new(config, None);

        // Spawn thread to write after delay
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            writer.push_chunk(b"Delayed").unwrap();
            writer.complete().unwrap();
        });

        // This should block until data arrives
        let mut buf = [0u8; 7];
        let n = source.read(&mut buf).unwrap();
        assert_eq!(n, 7);
        assert_eq!(&buf, b"Delayed");
    }

    #[test]
    fn error_unblocks_reader_with_io_error() {
        use std::io::ErrorKind;
        let config = StreamingConfig {
            initial_buffer_bytes: 5,
            max_buffer_bytes: 100,
        };
        let (mut source, writer) = BufferedMediaSource::new(config, None);
        writer.error("cdn failed".into()).unwrap();
        let mut buf = [0u8; 8];
        let err = source.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Other);
        let msg = err.to_string();
        assert!(msg.contains("cdn failed"), "{msg}");
    }

    // --- Range requests -----------------------------------------------------

    /// An open-ended plan, the shape a live body always has.
    fn to_eof(offset: u64) -> FetchPlan {
        FetchPlan { offset, end: None }
    }

    /// Drive a range-capable buffer the way a feeder does: take the plan,
    /// point the write head at it, push the bytes.
    fn feed(writer: &BufferWriter, plan: FetchPlan, data: &[u8]) {
        writer.begin_at(plan.offset).unwrap();
        writer.push_chunk(data).unwrap();
    }

    #[test]
    fn sequential_source_never_asks_for_a_range() {
        let config = StreamingConfig {
            initial_buffer_bytes: 4,
            max_buffer_bytes: 100,
        };
        let (source, writer) = BufferedMediaSource::new(config, Some(100));
        assert!(!source.supports_range_requests());
        writer.push_chunk(b"0123").unwrap();
        // A read far past the write head has nothing to ask for: this feeder
        // can only produce bytes in order, so the reader must wait.
        assert!(writer.take_request().is_none());
        assert!(writer.next_plan().is_none());
    }

    #[test]
    fn read_far_ahead_asks_the_feeder_to_re_open_there() {
        let config = StreamingConfig {
            initial_buffer_bytes: 4,
            max_buffer_bytes: 8 * 1024 * 1024,
        };
        let (source, writer) = BufferedMediaSource::new_seekable(config, Some(64 * 1024 * 1024));
        assert!(source.supports_range_requests());
        writer.push_chunk(&[7u8; 1024]).unwrap();

        // The reader jumps 32 MB ahead — a real seek, orders of magnitude
        // past FORWARD_WAIT_BYTES.
        let target = 32 * 1024 * 1024;
        let mut reader = source.create_reader();
        let handle = thread::spawn(move || {
            let mut buf = [0u8; 4];
            reader.seek(SeekFrom::Start(target)).unwrap();
            reader.read(&mut buf).unwrap();
            buf
        });

        // The feeder sees the request and serves it.
        let plan = loop {
            if let Some(plan) = writer.take_request() {
                break plan;
            }
            thread::sleep(Duration::from_millis(5));
        };
        assert_eq!(plan.offset, target);
        assert_eq!(plan.range_header(), format!("bytes={target}-"));
        feed(&writer, plan, b"jump");

        assert_eq!(&handle.join().unwrap(), b"jump");
        // Reads are measured from the resume point now, not from byte 0.
        assert_eq!(source.primary_offset(), target);
        assert_eq!(source.head_bytes(), 1024);
        assert_eq!(source.buffer_size(), 1024 + 4);
    }

    #[test]
    fn short_forward_hop_waits_instead_of_re_opening() {
        let config = StreamingConfig {
            initial_buffer_bytes: 4,
            max_buffer_bytes: 8 * 1024 * 1024,
        };
        let (source, writer) = BufferedMediaSource::new_seekable(config, Some(4 * 1024 * 1024));
        writer.push_chunk(&[1u8; 1024]).unwrap();

        // 64KB ahead of the download head: a round trip costs more than the
        // wait, so no request is posted.
        let mut reader = source.create_reader();
        let handle = thread::spawn(move || {
            let mut buf = [0u8; 2];
            reader.seek(SeekFrom::Start(64 * 1024)).unwrap();
            reader.read(&mut buf).unwrap();
            buf
        });

        thread::sleep(Duration::from_millis(50));
        assert!(
            writer.take_request().is_none(),
            "a 64KB hop should be waited out, not re-requested"
        );

        // The sequential download reaches the reader on its own.
        writer.push_chunk(&[2u8; 64 * 1024]).unwrap();
        assert_eq!(&handle.join().unwrap(), &[2u8, 2u8]);
    }

    #[test]
    fn bisection_probe_just_ahead_of_the_head_waits_for_it() {
        // The offsets are the ones a real 106s resume produced. Symphonia
        // has no seek table to consult, so after jumping to 34,165,359 it
        // probes 35,002,642 — 803 KB past the download head. Re-opening
        // there would move the body's start past the probes that follow
        // (which converge back down) and turn each into its own request, so
        // this hop has to be waited out.
        let config = StreamingConfig {
            initial_buffer_bytes: 4,
            max_buffer_bytes: 100 * 1024 * 1024,
        };
        let (source, writer) = BufferedMediaSource::new_seekable(config, Some(72_332_363));
        feed(&writer, to_eof(34_165_359), &[9u8; 15_619]);
        assert_eq!(source.primary_offset(), 0);

        let mut reader = source.create_reader();
        let handle = thread::spawn(move || {
            let mut buf = [0u8; 1];
            reader.seek(SeekFrom::Start(35_002_642)).unwrap();
            reader.read(&mut buf).unwrap();
            buf[0]
        });

        thread::sleep(Duration::from_millis(50));
        assert!(
            writer.take_request().is_none(),
            "an 803 KB hop must be waited out - re-opening there strands the probes behind it"
        );

        // The live body reaches it on its own, and the probes that converge
        // back down are then served from the buffer, not the network.
        writer.push_chunk(&[8u8; 900_000]).unwrap();
        assert_eq!(handle.join().unwrap(), 8);
        let mut back = source.create_reader();
        assert_eq!(back.seek(SeekFrom::Start(34_584_000)).unwrap(), 34_584_000);
        assert_eq!(back.seek(SeekFrom::Start(34_374_679)).unwrap(), 34_374_679);
        assert!(writer.take_request().is_none());
    }

    #[test]
    fn a_backward_probe_fetches_from_before_itself() {
        // The offsets a real 91s resume produced. After jumping to
        // 32,311,596 Symphonia walks back down — 31,475,334 then
        // 31,057,203 then 30,848,137 — and each step used to strand the
        // next one in a hole of its own.
        let config = StreamingConfig {
            initial_buffer_bytes: 4,
            max_buffer_bytes: 200 * 1024 * 1024,
        };
        let (source, writer) = BufferedMediaSource::new_seekable(config, Some(108_323_214));
        feed(&writer, to_eof(32_311_596), &[9u8; 19_801]);

        let mut reader = source.create_reader();
        let handle = thread::spawn(move || {
            let mut buf = [0u8; 1];
            reader.seek(SeekFrom::Start(31_475_334)).unwrap();
            reader.read(&mut buf).unwrap();
            buf[0]
        });

        let plan = loop {
            if let Some(plan) = writer.take_request() {
                break plan;
            }
            thread::sleep(Duration::from_millis(5));
        };
        // Fetched from a megabyte before the probe, not from the probe.
        assert_eq!(plan.offset, 31_475_334 - 1024 * 1024);
        // But the buffer-fill gate still measures from where playback is.
        assert_eq!(source.primary_offset(), 31_475_334);

        // Streaming that body through the probe covers the rest of the
        // walk, so nothing below it asks for the network again.
        feed(&writer, plan, &[7u8; 1024 * 1024 + 64]);
        assert_eq!(handle.join().unwrap(), 7);
        let mut back = source.create_reader();
        assert_eq!(back.seek(SeekFrom::Start(31_057_203)).unwrap(), 31_057_203);
        assert_eq!(back.seek(SeekFrom::Start(30_848_137)).unwrap(), 30_848_137);
        assert!(
            writer.take_request().is_none(),
            "the descending walk must be served from the buffer, not re-opened"
        );
    }

    #[test]
    fn holes_are_backfilled_before_the_stream_reports_complete() {
        let config = StreamingConfig {
            initial_buffer_bytes: 4,
            max_buffer_bytes: 100,
        };
        let (source, writer) = BufferedMediaSource::new_seekable(config, Some(30));

        // Header, then a jump to byte 20 that runs to EOF: bytes 10..20 are
        // the hole a resume leaves behind.
        writer.push_chunk(b"0123456789").unwrap();
        feed(&writer, to_eof(20), b"UUUUUUUUUU");

        assert!(!source.is_complete());
        assert!(source.take_complete_data().is_none());

        // The feeder is handed the hole, bounded so it stops at the data we
        // already hold.
        let plan = writer.next_plan().unwrap();
        assert_eq!(plan.offset, 10);
        assert_eq!(plan.end, Some(20));
        assert_eq!(plan.range_header(), "bytes=10-19");
        assert_eq!(plan.byte_len(), Some(10));
        feed(&writer, plan, b"__________");

        // Nothing missing: the buffer is a whole file again, so it can be
        // promoted to the in-memory cache like any completed download.
        assert!(writer.next_plan().is_none());
        assert!(source.is_complete());
        assert_eq!(
            source.take_complete_data().as_deref(),
            Some(&b"0123456789__________UUUUUUUUUU"[..])
        );
    }

    #[test]
    fn a_pending_request_outranks_backfill() {
        const MB: u64 = 1024 * 1024;
        let config = StreamingConfig {
            initial_buffer_bytes: 4,
            max_buffer_bytes: 16 * MB as usize,
        };
        let (source, writer) = BufferedMediaSource::new_seekable(config, Some(8 * MB));
        // Header plus a jumped-to tail: 1 MB .. 6 MB is the hole waiting to
        // be backfilled.
        writer.push_chunk(&[1u8; MB as usize]).unwrap();
        feed(&writer, to_eof(6 * MB), &[6u8; 2 * MB as usize]);

        // A reader now needs a byte inside that hole.
        let mut reader = source.create_reader();
        let handle = thread::spawn(move || {
            let _ = reader.seek(SeekFrom::Start(4 * MB));
        });
        thread::sleep(Duration::from_millis(50));

        // Playback comes first: the feeder is sent to the probe (a
        // lookbehind early, since it reads backwards) and runs to EOF, not
        // to the bounded gap that starts at 1 MB.
        let plan = writer.next_plan().unwrap();
        assert_eq!(plan.offset, 3 * MB);
        assert_eq!(plan.end, None, "the live body runs to EOF, not to a bound");
        assert_eq!(source.primary_offset(), 4 * MB);
        feed(&writer, plan, &[3u8; MB as usize + 16]);
        handle.join().unwrap();

        // What is left of the hole is what backfill picks up next.
        let gap = writer.next_plan().unwrap();
        assert_eq!((gap.offset, gap.end), (MB, Some(3 * MB)));
    }

    #[test]
    fn overlapping_refetch_does_not_duplicate_bytes() {
        let config = StreamingConfig {
            initial_buffer_bytes: 4,
            max_buffer_bytes: 100,
        };
        let (source, writer) = BufferedMediaSource::new_seekable(config, Some(20));
        writer.push_chunk(b"0123456789").unwrap();
        // A decoder bisecting for a seek point can send us back over bytes we
        // already hold; the overlap is dropped, not appended twice.
        feed(&writer, to_eof(5), b"56789ABCDE");
        assert_eq!(source.buffer_size(), 15);
        let mut reader = source.create_reader();
        let mut buf = [0u8; 15];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"0123456789ABCDE");
    }

    #[test]
    fn feeder_stops_once_the_last_reader_is_gone() {
        let config = StreamingConfig {
            initial_buffer_bytes: 4,
            max_buffer_bytes: 100,
        };
        let (source, writer) = BufferedMediaSource::new_seekable(config, Some(30));
        writer.push_chunk(b"0123456789").unwrap();
        // A hole remains, so there is still work...
        feed(&writer, to_eof(20), b"UUUUUUUUUU");
        assert!(writer.next_plan().is_some());
        // ...but nobody left to play it for.
        drop(source);
        assert!(writer.next_plan().is_none());
    }

    #[test]
    fn min_buffer_is_measured_from_the_resume_point() {
        let config = StreamingConfig {
            initial_buffer_bytes: 8,
            max_buffer_bytes: 8 * 1024 * 1024,
        };
        let (source, writer) = BufferedMediaSource::new_seekable(config, Some(64 * 1024 * 1024));
        writer.push_chunk(&[0u8; 1024]).unwrap();
        // 1024 bytes from byte 0 clears the 8-byte floor.
        assert!(source.has_min_buffer());

        let target = 32 * 1024 * 1024;
        let mut reader = source.create_reader();
        let handle = thread::spawn(move || {
            let _ = reader.seek(SeekFrom::Start(target));
        });
        let plan = loop {
            if let Some(plan) = writer.take_request() {
                break plan;
            }
            thread::sleep(Duration::from_millis(5));
        };
        // The jump moved the playback offset: the megabyte behind us no
        // longer counts, only what is buffered at the resume point.
        assert!(!source.has_min_buffer());
        feed(&writer, plan, &[1u8; 4]);
        assert!(!source.has_min_buffer());
        writer.push_chunk(&[1u8; 4]).unwrap();
        assert!(source.has_min_buffer());
        handle.join().unwrap();
    }

    #[test]
    fn range_header_covers_bounded_and_open_ended_plans() {
        assert_eq!(to_eof(0).range_header(), "bytes=0-");
        assert_eq!(
            FetchPlan {
                offset: 100,
                end: Some(200)
            }
            .range_header(),
            "bytes=100-199"
        );
        // A degenerate bound reads to EOF rather than emitting "bytes=100-99".
        assert_eq!(
            FetchPlan {
                offset: 100,
                end: Some(100)
            }
            .range_header(),
            "bytes=100-"
        );
        assert!(to_eof(0).is_whole_file());
        assert!(!to_eof(1).is_whole_file());
    }
}
