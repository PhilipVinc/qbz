//! L2 Disk Cache - File-based playback cache
//!
//! Secondary cache for audio data evicted from memory.
//! Provides faster access than re-downloading from network.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::SystemTime;

/// Entry metadata for tracking cache usage
#[derive(Debug, Clone)]
struct CacheEntry {
    #[allow(dead_code)]
    track_id: u64,
    size_bytes: u64,
    last_accessed: SystemTime,
}

/// Disk-based playback cache state
struct PlaybackCacheState {
    /// Track metadata keyed by track ID
    entries: HashMap<u64, CacheEntry>,
    /// Current total size in bytes
    current_size: u64,
}

/// Disk-based playback cache for evicted tracks
///
/// Stores audio data as files on disk with LRU eviction.
/// Files are named `{track_id}.audio` in the cache directory.
pub struct PlaybackCache {
    state: Mutex<PlaybackCacheState>,
    /// Cache directory path
    cache_dir: PathBuf,
    /// Maximum cache size in bytes
    max_size_bytes: u64,
}

impl PlaybackCache {
    /// Create a new playback cache with default location
    ///
    /// Default path: `~/.cache/qbz/playback/`
    pub fn new(max_size_bytes: u64) -> Result<Self, String> {
        let cache_dir = dirs::cache_dir()
            .ok_or("Could not determine cache directory")?
            .join("qbz")
            .join("playback");

        Self::with_path(cache_dir, max_size_bytes)
    }

    /// Create a new playback cache at a specific path
    pub fn with_path(cache_dir: PathBuf, max_size_bytes: u64) -> Result<Self, String> {
        // Create directory
        fs::create_dir_all(&cache_dir)
            .map_err(|e| format!("Failed to create playback cache directory: {}", e))?;

        let cache = Self {
            state: Mutex::new(PlaybackCacheState {
                entries: HashMap::new(),
                current_size: 0,
            }),
            cache_dir,
            max_size_bytes,
        };

        // Scan existing files to rebuild state
        cache.rebuild_state();

        log::info!(
            "Playback cache initialized at {:?} (max {} MB)",
            cache.cache_dir,
            max_size_bytes / (1024 * 1024)
        );

        Ok(cache)
    }

    /// Rebuild cache state from existing files on disk
    fn rebuild_state(&self) {
        let mut state = self.state.lock().unwrap();
        state.entries.clear();
        state.current_size = 0;

        if let Ok(entries) = fs::read_dir(&self.cache_dir) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() {
                        // Sweep a write that a crash or power cut interrupted.
                        if entry.file_name().to_str().is_some_and(|n| n.ends_with(".part")) {
                            let _ = fs::remove_file(entry.path());
                            continue;
                        }
                        // Parse track ID from filename (format: {track_id}.audio)
                        if let Some(filename) = entry.file_name().to_str() {
                            if let Some(id_str) = filename.strip_suffix(".audio") {
                                if let Ok(track_id) = id_str.parse::<u64>() {
                                    let size = metadata.len();
                                    let last_accessed =
                                        metadata.accessed().unwrap_or_else(|_| SystemTime::now());

                                    state.entries.insert(
                                        track_id,
                                        CacheEntry {
                                            track_id,
                                            size_bytes: size,
                                            last_accessed,
                                        },
                                    );
                                    state.current_size += size;
                                }
                            }
                        }
                    }
                }
            }
        }

        log::info!(
            "Playback cache rebuilt: {} tracks, {} MB",
            state.entries.len(),
            state.current_size / (1024 * 1024)
        );
    }

    /// Get file path for a track
    fn track_path(&self, track_id: u64) -> PathBuf {
        self.cache_dir.join(format!("{}.audio", track_id))
    }

    /// Check if a track is in the cache
    pub fn contains(&self, track_id: u64) -> bool {
        self.state.lock().unwrap().entries.contains_key(&track_id)
    }

    /// The cached file for `track_id`, when one is actually on disk.
    ///
    /// For a caller that can decode from a file instead of a buffer: a Hi-Res
    /// track is 120–220 MB, and once the prefetch has written it here, holding a
    /// second copy in RAM until the gapless transition is pure cost. Checks the
    /// filesystem rather than only the index, since [`Self::get`] documents that
    /// entries can be deleted underneath us.
    pub fn path_if_present(&self, track_id: u64) -> Option<PathBuf> {
        if !self.contains(track_id) {
            return None;
        }
        let path = self.track_path(track_id);
        path.exists().then_some(path)
    }

    /// Get a track from the cache
    pub fn get(&self, track_id: u64) -> Option<Vec<u8>> {
        let path = self.track_path(track_id);

        // Check if file exists and read it
        if !path.exists() {
            // File was deleted externally, update state
            let mut state = self.state.lock().unwrap();
            if let Some(entry) = state.entries.remove(&track_id) {
                state.current_size = state.current_size.saturating_sub(entry.size_bytes);
            }
            return None;
        }

        match fs::File::open(&path) {
            Ok(mut file) => {
                let mut data = Vec::new();
                if file.read_to_end(&mut data).is_ok() {
                    // Update last accessed time
                    let mut state = self.state.lock().unwrap();
                    if let Some(entry) = state.entries.get_mut(&track_id) {
                        entry.last_accessed = SystemTime::now();
                    }

                    // Touch file to update filesystem access time
                    let _ = filetime::set_file_atime(&path, filetime::FileTime::now());

                    log::debug!(
                        "Playback cache hit for track {} ({} bytes)",
                        track_id,
                        data.len()
                    );
                    Some(data)
                } else {
                    log::warn!("Failed to read playback cache file for track {}", track_id);
                    None
                }
            }
            Err(e) => {
                log::warn!(
                    "Failed to open playback cache file for track {}: {}",
                    track_id,
                    e
                );
                None
            }
        }
    }

    /// Reserve room for a track that will be written STRAIGHT INTO the cache,
    /// and return the temporary path to write to.
    ///
    /// [`Self::insert`] takes bytes that are already assembled in memory, which
    /// means a big track is materialised whole in RAM and then pushed to the
    /// card in one blocking, fsync'd burst — 117 MB took 8.6 s on a Pi's card,
    /// long enough to starve the audio writer thread and underrun ALSA. A caller
    /// that knows the size UP FRONT (the CMAF segment table gives it before a
    /// single audio byte is fetched) can instead stream the track here as it
    /// downloads, so the same bytes trickle out over the ~20 s the download
    /// takes and never form a burst at all.
    ///
    /// The cache keeps ownership of what it should own: eviction happens here,
    /// the write goes to `<id>.part`, and only [`Self::commit_write`] renames it
    /// into place and indexes it — so a crash mid-write leaves a `.part` that
    /// `rebuild_index` sweeps, exactly as for [`Self::insert`].
    ///
    /// `None` when the track cannot fit the cache at all.
    pub fn begin_write(&self, track_id: u64, expected_size: u64) -> Option<PathBuf> {
        if expected_size > self.max_size_bytes {
            log::debug!(
                "Track {} too large for playback cache ({} MB > {} MB)",
                track_id,
                expected_size / (1024 * 1024),
                self.max_size_bytes / (1024 * 1024)
            );
            return None;
        }
        self.evict_if_needed(expected_size);
        let part = self.track_path(track_id).with_extension("part");
        if let Some(parent) = part.parent() {
            let _ = fs::create_dir_all(parent);
        }
        Some(part)
    }

    /// Publish a track written via [`Self::begin_write`]: rename `<id>.part`
    /// into place and index it. Returns the final path.
    ///
    /// The rename is the atomicity guarantee — whatever sits under the real name
    /// is always a whole file — so the caller must have flushed and synced the
    /// `.part` before calling this.
    pub fn commit_write(&self, track_id: u64, actual_size: u64) -> Option<PathBuf> {
        let part = self.track_path(track_id).with_extension("part");
        let path = self.track_path(track_id);
        if let Err(e) = fs::rename(&part, &path) {
            log::warn!("Failed to publish streamed track {}: {}", track_id, e);
            let _ = fs::remove_file(&part);
            return None;
        }
        let mut state = self.state.lock().unwrap();
        if let Some(old) = state.entries.remove(&track_id) {
            state.current_size = state.current_size.saturating_sub(old.size_bytes);
        }
        state.entries.insert(
            track_id,
            CacheEntry {
                track_id,
                size_bytes: actual_size,
                last_accessed: SystemTime::now(),
            },
        );
        state.current_size += actual_size;
        log::info!(
            "Streamed track {} into the playback cache ({} KB). Total: {} MB / {} MB",
            track_id,
            actual_size / 1024,
            state.current_size / (1024 * 1024),
            self.max_size_bytes / (1024 * 1024)
        );
        Some(path)
    }

    /// Discard a partial write. Safe to call when there is nothing to discard.
    pub fn abort_write(&self, track_id: u64) {
        let part = self.track_path(track_id).with_extension("part");
        if part.exists() {
            let _ = fs::remove_file(&part);
            log::debug!("Discarded partial cache write for track {}", track_id);
        }
    }

    /// Insert a track into the cache (called when evicting from memory cache)
    pub fn insert(&self, track_id: u64, data: &[u8]) {
        let size = data.len() as u64;

        // Don't cache if larger than max size
        if size > self.max_size_bytes {
            log::debug!(
                "Track {} too large for playback cache ({} MB > {} MB)",
                track_id,
                size / (1024 * 1024),
                self.max_size_bytes / (1024 * 1024)
            );
            return;
        }

        // Already on the card, byte-for-byte? Then this write is pure wear.
        //
        // A prefetched track reaches here TWICE: `stage_track_on_disk` writes it
        // for the gapless hand-off, and a second later `drop_cached_track` ->
        // `AudioCache::release` spills the same bytes again. Re-writing 23 MB
        // (220 MB for Hi-Res) through `sync_all` to an SD card, to land on a
        // file that is already identical, costs seconds of I/O and real card
        // life. Touch the LRU and return.
        //
        // Size-matched, not just present: a short `<id>.audio` from an older
        // build is exactly the corruption the temp-file-and-rename below exists
        // to prevent, so a length mismatch must still overwrite.
        {
            let path = self.track_path(track_id);
            let same_size = fs::metadata(&path).is_ok_and(|m| m.len() == size);
            if same_size {
                let mut state = self.state.lock().unwrap();
                if let Some(entry) = state.entries.get_mut(&track_id) {
                    entry.last_accessed = SystemTime::now();
                    log::debug!(
                        "Track {track_id} already in the playback cache at {size} bytes — not rewritten"
                    );
                    return;
                }
            }
        }

        // Evict old entries if needed
        self.evict_if_needed(size);

        let path = self.track_path(track_id);

        // Write through a temp file and rename into place. A track is 30-220 MB
        // and the write is not instant: a daemon killed mid-write used to leave
        // a SHORT `<id>.audio` behind, which the startup rebuild then adopted as
        // a complete entry — so that track decoded as garbage on every later
        // play until something evicted it. A rename is atomic, so the cache only
        // ever contains whole files; a crash leaves a `.part` that
        // `rebuild_index` sweeps.
        let temp_path = path.with_extension("part");
        match fs::File::create(&temp_path) {
            Ok(mut file) => {
                let written = file.write_all(data).is_ok()
                    // Reach the platter before the rename: the point of the
                    // rename is that whatever is under the real name is
                    // complete, and on a power cut an unsynced write is not.
                    && file.sync_all().is_ok()
                    && {
                        drop(file);
                        fs::rename(&temp_path, &path).is_ok()
                    };
                if !written {
                    let _ = fs::remove_file(&temp_path);
                }
                if written {
                    let mut state = self.state.lock().unwrap();

                    // Remove old entry if exists
                    if let Some(old) = state.entries.remove(&track_id) {
                        state.current_size = state.current_size.saturating_sub(old.size_bytes);
                    }

                    // Add new entry
                    state.entries.insert(
                        track_id,
                        CacheEntry {
                            track_id,
                            size_bytes: size,
                            last_accessed: SystemTime::now(),
                        },
                    );
                    state.current_size += size;

                    log::info!(
                        "Saved track {} to playback cache ({} KB). Total: {} MB / {} MB",
                        track_id,
                        size / 1024,
                        state.current_size / (1024 * 1024),
                        self.max_size_bytes / (1024 * 1024)
                    );
                } else {
                    log::warn!("Failed to write playback cache file for track {}", track_id);
                }
            }
            Err(e) => {
                log::warn!(
                    "Failed to create playback cache file for track {}: {}",
                    track_id,
                    e
                );
            }
        }
    }

    /// Evict oldest entries to make room for new data
    fn evict_if_needed(&self, needed_bytes: u64) {
        let mut state = self.state.lock().unwrap();

        while state.current_size + needed_bytes > self.max_size_bytes && !state.entries.is_empty() {
            // Find oldest entry
            let oldest_id = state
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_accessed)
                .map(|(id, _)| *id);

            if let Some(track_id) = oldest_id {
                if let Some(entry) = state.entries.remove(&track_id) {
                    state.current_size = state.current_size.saturating_sub(entry.size_bytes);

                    // Delete file
                    let path = self.cache_dir.join(format!("{}.audio", track_id));
                    if let Err(e) = fs::remove_file(&path) {
                        log::debug!("Failed to delete playback cache file: {}", e);
                    } else {
                        log::debug!(
                            "Evicted track {} from playback cache ({} KB)",
                            track_id,
                            entry.size_bytes / 1024
                        );
                    }
                }
            } else {
                break;
            }
        }
    }

    /// Clear the entire cache
    pub fn clear(&self) {
        let mut state = self.state.lock().unwrap();

        for track_id in state.entries.keys() {
            let path = self.cache_dir.join(format!("{}.audio", track_id));
            let _ = fs::remove_file(&path);
        }

        state.entries.clear();
        state.current_size = 0;

        log::info!("Playback cache cleared");
    }

    /// Get cache statistics
    pub fn stats(&self) -> PlaybackCacheStats {
        let state = self.state.lock().unwrap();
        PlaybackCacheStats {
            cached_tracks: state.entries.len(),
            current_size_bytes: state.current_size,
            max_size_bytes: self.max_size_bytes,
        }
    }

    /// Get the cache directory path
    pub fn cache_dir(&self) -> &PathBuf {
        &self.cache_dir
    }
}

/// Playback cache statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct PlaybackCacheStats {
    pub cached_tracks: usize,
    pub current_size_bytes: u64,
    pub max_size_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A COUNTER, not a timestamp. `SystemTime::now()` is not fine-grained
    /// enough on every platform to separate two tests that start together, and
    /// a shared directory makes `insert_leaves_no_partial_file` — which scans
    /// the whole directory — fail on another test's in-flight `.part`.
    static TEMP_DIR_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn temp_cache(max_bytes: u64) -> (PlaybackCache, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "qbz-l2-test-{}-{}",
            std::process::id(),
            TEMP_DIR_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let cache = PlaybackCache::with_path(dir.clone(), max_bytes).expect("cache");
        (cache, dir)
    }

    /// A whole track lands under its real name, and nothing is left behind.
    #[test]
    fn insert_leaves_no_partial_file() {
        let (cache, dir) = temp_cache(10 * 1024 * 1024);
        cache.insert(42, &vec![7u8; 4096]);

        assert_eq!(cache.get(42).map(|d| d.len()), Some(4096));
        let leftovers: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".part"))
            .collect();
        assert!(leftovers.is_empty(), "a .part file survived the insert");
        fs::remove_dir_all(&dir).ok();
    }

    /// The startup scan sweeps an interrupted write instead of adopting it. A
    /// truncated `<id>.audio` used to be indexed as a complete track and decoded
    /// as garbage on every later play.
    #[test]
    fn rebuild_sweeps_an_interrupted_write() {
        let (cache, dir) = temp_cache(10 * 1024 * 1024);
        cache.insert(1, &vec![1u8; 1024]);
        fs::write(dir.join("999.part"), vec![0u8; 512]).unwrap();

        let reopened = PlaybackCache::with_path(dir.clone(), 10 * 1024 * 1024).expect("reopen");

        assert!(reopened.contains(1), "the complete track is adopted");
        assert!(!dir.join("999.part").exists(), "the partial file is swept");
        fs::remove_dir_all(&dir).ok();
    }

    /// The budget is enforced by evicting the least recently used file, so the
    /// directory cannot grow without bound.
    #[test]
    fn insert_evicts_to_stay_under_the_budget() {
        let (cache, dir) = temp_cache(8192);
        cache.insert(1, &vec![0u8; 4096]);
        cache.insert(2, &vec![0u8; 4096]);
        cache.insert(3, &vec![0u8; 4096]);

        assert!(!cache.contains(1), "the oldest track is evicted");
        assert!(cache.contains(3));
        assert!(cache.stats().current_size_bytes <= 8192);
        assert!(!dir.join("1.audio").exists(), "its file is deleted too");
        fs::remove_dir_all(&dir).ok();
    }

    /// A prefetched track arrives here twice — once staged for the gapless
    /// hand-off, once spilled when its L1 entry is dropped — and rewriting an
    /// identical 220 MB file through `sync_all` is seconds of I/O and real SD
    /// card life for nothing.
    #[test]
    fn insert_does_not_rewrite_an_identical_file() {
        let (cache, dir) = temp_cache(10 * 1024 * 1024);
        let data = vec![7u8; 4096];
        cache.insert(1, &data);
        let first = fs::metadata(dir.join("1.audio")).unwrap().modified().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(20));
        cache.insert(1, &data);

        let second = fs::metadata(dir.join("1.audio")).unwrap().modified().unwrap();
        assert_eq!(first, second, "the file was not rewritten");
        assert!(cache.contains(1));
        assert_eq!(cache.stats().current_size_bytes, 4096, "counted once");
        fs::remove_dir_all(&dir).ok();
    }

    /// Present but the WRONG length is the truncated-write case, and it must
    /// still be overwritten — that file decodes as garbage.
    #[test]
    fn insert_overwrites_a_file_of_a_different_size() {
        let (cache, dir) = temp_cache(10 * 1024 * 1024);
        cache.insert(1, &vec![7u8; 4096]);
        cache.insert(1, &vec![9u8; 2048]);

        assert_eq!(fs::read(dir.join("1.audio")).unwrap(), vec![9u8; 2048]);
        assert_eq!(cache.stats().current_size_bytes, 2048);
        fs::remove_dir_all(&dir).ok();
    }
}
