//! Session persistence store.
//!
//! The playback queue/session state is portable application state. The current
//! Tauri/Svelte shell also stores view restoration fields in the same DB table;
//! those fields are modeled here only so the existing schema can round-trip
//! unchanged during the extraction.

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

fn default_streamable() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedQueueTrack {
    pub id: u64,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration_secs: u64,
    pub artwork_url: Option<String>,
    #[serde(default)]
    pub hires: bool,
    pub bit_depth: Option<u32>,
    pub sample_rate: Option<f64>,
    #[serde(default)]
    pub is_local: bool,
    pub album_id: Option<String>,
    pub artist_id: Option<u64>,
    #[serde(default = "default_streamable")]
    pub streamable: bool,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub parental_warning: bool,
    #[serde(default)]
    pub source_item_id_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedPlaybackSession {
    pub queue_tracks: Vec<PersistedQueueTrack>,
    pub current_index: Option<usize>,
    pub current_position_secs: u64,
    pub volume: f32,
    pub shuffle_enabled: bool,
    pub repeat_mode: String,
    pub was_playing: bool,
    pub saved_at: i64,
}

impl Default for PersistedPlaybackSession {
    fn default() -> Self {
        Self {
            queue_tracks: Vec::new(),
            current_index: None,
            current_position_secs: 0,
            volume: 0.75,
            shuffle_enabled: false,
            repeat_mode: "off".to_string(),
            was_playing: false,
            saved_at: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedShellViewState {
    #[serde(default = "default_last_view")]
    pub last_view: String,
    #[serde(default)]
    pub view_context_id: Option<String>,
    #[serde(default)]
    pub view_context_type: Option<String>,
}

fn default_last_view() -> String {
    "home".to_string()
}

impl Default for PersistedShellViewState {
    fn default() -> Self {
        Self {
            last_view: "home".to_string(),
            view_context_id: None,
            view_context_type: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct PersistedSessionSnapshot {
    pub playback: PersistedPlaybackSession,
    pub shell_view: PersistedShellViewState,
}

pub struct SessionStore {
    conn: Connection,
}

impl SessionStore {
    pub fn new() -> Result<Self, String> {
        let data_dir = dirs::data_dir()
            .ok_or("Could not determine data directory")?
            .join("qbz");
        Self::open_at(&data_dir, "session.db")
    }

    pub fn new_at(base_dir: &Path) -> Result<Self, String> {
        Self::open_at(base_dir, "session.db")
    }

    fn open_at(dir: &Path, db_name: &str) -> Result<Self, String> {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("Failed to create data directory: {}", e))?;

        let db_path = dir.join(db_name);
        let conn = Connection::open(&db_path)
            .map_err(|e| format!("Failed to open session database: {}", e))?;

        // WAL mode for non-blocking reads/writes (ADR-002). synchronous=FULL,
        // not NORMAL: the session DB must survive hard reboots (issue #440).
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(|e| format!("Failed to set WAL mode: {}", e))?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS player_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                current_index INTEGER,
                current_position_secs INTEGER NOT NULL DEFAULT 0,
                volume REAL NOT NULL DEFAULT 0.75,
                shuffle_enabled INTEGER NOT NULL DEFAULT 0,
                repeat_mode TEXT NOT NULL DEFAULT 'off',
                was_playing INTEGER NOT NULL DEFAULT 0,
                saved_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS queue_tracks (
                position INTEGER PRIMARY KEY,
                track_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                artist TEXT NOT NULL,
                album TEXT NOT NULL,
                duration_secs INTEGER NOT NULL,
                artwork_url TEXT,
                hires INTEGER NOT NULL DEFAULT 0,
                bit_depth INTEGER,
                sample_rate REAL,
                source TEXT
            );

            INSERT OR IGNORE INTO player_state (id, current_position_secs, volume, shuffle_enabled, repeat_mode, was_playing, saved_at)
            VALUES (1, 0, 0.75, 0, 'off', 0, 0);
            ",
        )
        .map_err(|e| format!("Failed to create session tables: {}", e))?;

        let has_hires: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('queue_tracks') WHERE name = 'hires'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0)
            > 0;

        if !has_hires {
            let _ = conn.execute_batch(
                "
                ALTER TABLE queue_tracks ADD COLUMN hires INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE queue_tracks ADD COLUMN bit_depth INTEGER;
                ALTER TABLE queue_tracks ADD COLUMN sample_rate REAL;
                ",
            );
        }

        let has_is_local: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('queue_tracks') WHERE name = 'is_local'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0)
            > 0;

        if !has_is_local {
            let _ = conn.execute_batch(
                "
                ALTER TABLE queue_tracks ADD COLUMN is_local INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE queue_tracks ADD COLUMN album_id TEXT;
                ALTER TABLE queue_tracks ADD COLUMN artist_id INTEGER;
                ",
            );
        }

        let has_source: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('queue_tracks') WHERE name = 'source'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0)
            > 0;

        if !has_source {
            let _ = conn.execute_batch(
                "
                ALTER TABLE queue_tracks ADD COLUMN source TEXT;
                ",
            );
        }

        let has_streamable: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('queue_tracks') WHERE name = 'streamable'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0)
            > 0;

        if !has_streamable {
            let _ = conn.execute_batch(
                "
                ALTER TABLE queue_tracks ADD COLUMN streamable INTEGER NOT NULL DEFAULT 1;
                ALTER TABLE queue_tracks ADD COLUMN parental_warning INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE queue_tracks ADD COLUMN source_item_id_hint TEXT;
                ",
            );
        }

        let has_last_view: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('player_state') WHERE name = 'last_view'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0)
            > 0;

        if !has_last_view {
            let _ = conn.execute_batch(
                "
                ALTER TABLE player_state ADD COLUMN last_view TEXT NOT NULL DEFAULT 'home';
                ALTER TABLE player_state ADD COLUMN view_context_id TEXT;
                ALTER TABLE player_state ADD COLUMN view_context_type TEXT;
                ",
            );
        }

        Ok(Self { conn })
    }

    pub fn save_session(&self, session: &PersistedSessionSnapshot) -> Result<(), String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        self.conn
            .execute("BEGIN TRANSACTION", [])
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        if let Err(e) = self.conn.execute("DELETE FROM queue_tracks", []) {
            let _ = self.conn.execute("ROLLBACK", []);
            return Err(format!("Failed to clear queue: {}", e));
        }

        for (pos, track) in session.playback.queue_tracks.iter().enumerate() {
            if let Err(e) = self.conn.execute(
                "INSERT INTO queue_tracks (position, track_id, title, artist, album, duration_secs, artwork_url, hires, bit_depth, sample_rate, is_local, album_id, artist_id, source, streamable, parental_warning, source_item_id_hint)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
                params![
                    pos as i64,
                    track.id as i64,
                    track.title,
                    track.artist,
                    track.album,
                    track.duration_secs as i64,
                    track.artwork_url,
                    track.hires as i64,
                    track.bit_depth.map(|v| v as i64),
                    track.sample_rate,
                    track.is_local as i64,
                    track.album_id,
                    track.artist_id.map(|v| v as i64),
                    track.source,
                    track.streamable as i64,
                    track.parental_warning as i64,
                    track.source_item_id_hint,
                ],
            ) {
                let _ = self.conn.execute("ROLLBACK", []);
                return Err(format!("Failed to insert queue track: {}", e));
            }
        }

        if let Err(e) = self.conn.execute(
            "UPDATE player_state SET
                current_index = ?1,
                current_position_secs = ?2,
                volume = ?3,
                shuffle_enabled = ?4,
                repeat_mode = ?5,
                was_playing = ?6,
                saved_at = ?7,
                last_view = ?8,
                view_context_id = ?9,
                view_context_type = ?10
             WHERE id = 1",
            params![
                session.playback.current_index.map(|i| i as i64),
                session.playback.current_position_secs as i64,
                session.playback.volume as f64,
                session.playback.shuffle_enabled as i64,
                session.playback.repeat_mode,
                session.playback.was_playing as i64,
                now,
                session.shell_view.last_view,
                session.shell_view.view_context_id,
                session.shell_view.view_context_type,
            ],
        ) {
            let _ = self.conn.execute("ROLLBACK", []);
            return Err(format!("Failed to update player state: {}", e));
        }

        self.conn
            .execute("COMMIT", [])
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        Ok(())
    }

    pub fn load_session(&self) -> Result<PersistedSessionSnapshot, String> {
        let (
            current_index,
            current_position_secs,
            volume,
            shuffle_enabled,
            repeat_mode,
            was_playing,
            saved_at,
            last_view,
            view_context_id,
            view_context_type,
        ): (
            Option<i64>,
            i64,
            f64,
            i64,
            String,
            i64,
            i64,
            String,
            Option<String>,
            Option<String>,
        ) = self
            .conn
            .query_row(
                "SELECT current_index, current_position_secs, volume, shuffle_enabled, repeat_mode, was_playing, saved_at, last_view, view_context_id, view_context_type
                 FROM player_state WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get::<_, String>(7)
                            .unwrap_or_else(|_| "home".to_string()),
                        row.get(8)?,
                        row.get(9)?,
                    ))
                },
            )
            .map_err(|e| format!("Failed to load player state: {}", e))?;

        let mut stmt = self.conn
            .prepare("SELECT track_id, title, artist, album, duration_secs, artwork_url, hires, bit_depth, sample_rate, is_local, album_id, artist_id, source, streamable, parental_warning, source_item_id_hint FROM queue_tracks ORDER BY position")
            .map_err(|e| format!("Failed to prepare queue query: {}", e))?;

        let tracks: Vec<PersistedQueueTrack> = stmt
            .query_map([], |row| {
                Ok(PersistedQueueTrack {
                    id: row.get::<_, i64>(0)? as u64,
                    title: row.get(1)?,
                    artist: row.get(2)?,
                    album: row.get(3)?,
                    duration_secs: row.get::<_, i64>(4)? as u64,
                    artwork_url: row.get(5)?,
                    hires: row.get::<_, i64>(6).unwrap_or(0) != 0,
                    bit_depth: row.get::<_, Option<i64>>(7)?.map(|v| v as u32),
                    sample_rate: row.get(8)?,
                    is_local: row.get::<_, i64>(9).unwrap_or(0) != 0,
                    album_id: row.get(10)?,
                    artist_id: row.get::<_, Option<i64>>(11)?.map(|v| v as u64),
                    source: row.get(12)?,
                    streamable: row.get::<_, i64>(13).unwrap_or(1) != 0,
                    parental_warning: row.get::<_, i64>(14).unwrap_or(0) != 0,
                    source_item_id_hint: row.get(15)?,
                })
            })
            .map_err(|e| format!("Failed to query queue tracks: {}", e))?
            .filter_map(|result| result.ok())
            .collect();

        Ok(PersistedSessionSnapshot {
            playback: PersistedPlaybackSession {
                queue_tracks: tracks,
                current_index: current_index.map(|i| i as usize),
                current_position_secs: current_position_secs as u64,
                volume: volume as f32,
                shuffle_enabled: shuffle_enabled != 0,
                repeat_mode,
                was_playing: was_playing != 0,
                saved_at,
            },
            shell_view: PersistedShellViewState {
                last_view,
                view_context_id,
                view_context_type,
            },
        })
    }

    pub fn save_position(&self, position_secs: u64) -> Result<(), String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        self.conn
            .execute(
                "UPDATE player_state SET current_position_secs = ?1, saved_at = ?2 WHERE id = 1",
                params![position_secs as i64, now],
            )
            .map_err(|e| format!("Failed to save position: {}", e))?;

        Ok(())
    }

    pub fn save_volume(&self, volume: f32) -> Result<(), String> {
        self.conn
            .execute(
                "UPDATE player_state SET volume = ?1 WHERE id = 1",
                params![volume as f64],
            )
            .map_err(|e| format!("Failed to save volume: {}", e))?;

        Ok(())
    }

    pub fn save_playback_mode(&self, shuffle: bool, repeat_mode: &str) -> Result<(), String> {
        self.conn
            .execute(
                "UPDATE player_state SET shuffle_enabled = ?1, repeat_mode = ?2 WHERE id = 1",
                params![shuffle as i64, repeat_mode],
            )
            .map_err(|e| format!("Failed to save playback mode: {}", e))?;

        Ok(())
    }

    pub fn clear_session(&self) -> Result<(), String> {
        self.conn
            .execute("DELETE FROM queue_tracks", [])
            .map_err(|e| format!("Failed to clear queue: {}", e))?;

        self.conn.execute(
            "UPDATE player_state SET current_index = NULL, current_position_secs = 0, was_playing = 0, last_view = 'home', view_context_id = NULL, view_context_type = NULL WHERE id = 1",
            [],
        ).map_err(|e| format!("Failed to reset player state: {}", e))?;

        Ok(())
    }

    #[cfg(test)]
    fn pragma_synchronous(&self) -> Result<i64, String> {
        self.conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .map_err(|e| format!("Failed to read synchronous pragma: {}", e))
    }

    #[cfg(test)]
    fn pragma_journal_mode(&self) -> Result<String, String> {
        self.conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(|e| format!("Failed to read journal mode pragma: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_test_dir(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("qbz-app-{name}-{}-{nonce}", std::process::id()))
    }

    fn sample_track() -> PersistedQueueTrack {
        PersistedQueueTrack {
            id: 42,
            title: "Track".to_string(),
            artist: "Artist".to_string(),
            album: "Album".to_string(),
            duration_secs: 300,
            artwork_url: Some("https://example.test/art.jpg".to_string()),
            hires: true,
            bit_depth: Some(24),
            sample_rate: Some(96_000.0),
            is_local: true,
            album_id: Some("album-1".to_string()),
            artist_id: Some(7),
            streamable: false,
            source: Some("mixtape".to_string()),
            parental_warning: true,
            source_item_id_hint: Some("item-1".to_string()),
        }
    }

    #[test]
    fn default_session_values_are_stable() {
        let session = PersistedSessionSnapshot::default();

        assert!(session.playback.queue_tracks.is_empty());
        assert_eq!(session.playback.current_index, None);
        assert_eq!(session.playback.current_position_secs, 0);
        assert_eq!(session.playback.volume, 0.75);
        assert!(!session.playback.shuffle_enabled);
        assert_eq!(session.playback.repeat_mode, "off");
        assert!(!session.playback.was_playing);
        assert_eq!(session.shell_view.last_view, "home");
        assert_eq!(session.shell_view.view_context_id, None);
        assert_eq!(session.shell_view.view_context_type, None);
    }

    #[test]
    fn session_store_uses_wal_and_full_synchronous() {
        let dir = unique_test_dir("session-pragmas");
        let store = SessionStore::new_at(&dir).expect("open store");

        assert_eq!(store.pragma_journal_mode().expect("journal mode"), "wal");
        assert_eq!(store.pragma_synchronous().expect("synchronous"), 2);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn session_store_round_trips_queue_and_shell_view_state() {
        let dir = unique_test_dir("session-round-trip");
        let store = SessionStore::new_at(&dir).expect("open store");
        let session = PersistedSessionSnapshot {
            playback: PersistedPlaybackSession {
                queue_tracks: vec![sample_track()],
                current_index: Some(0),
                current_position_secs: 123,
                volume: 0.42,
                shuffle_enabled: true,
                repeat_mode: "all".to_string(),
                was_playing: true,
                saved_at: 0,
            },
            shell_view: PersistedShellViewState {
                last_view: "album".to_string(),
                view_context_id: Some("album-1".to_string()),
                view_context_type: Some("album".to_string()),
            },
        };

        store.save_session(&session).expect("save session");
        let loaded = store.load_session().expect("load session");

        assert_eq!(loaded.playback.queue_tracks, vec![sample_track()]);
        assert_eq!(loaded.playback.current_index, Some(0));
        assert_eq!(loaded.playback.current_position_secs, 123);
        assert_eq!(loaded.playback.volume, 0.42);
        assert!(loaded.playback.shuffle_enabled);
        assert_eq!(loaded.playback.repeat_mode, "all");
        assert!(loaded.playback.was_playing);
        assert!(loaded.playback.saved_at > 0);
        assert_eq!(loaded.shell_view.last_view, "album");
        assert_eq!(
            loaded.shell_view.view_context_id.as_deref(),
            Some("album-1")
        );
        assert_eq!(
            loaded.shell_view.view_context_type.as_deref(),
            Some("album")
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn quick_saves_update_only_targeted_playback_fields() {
        let dir = unique_test_dir("session-quick-save");
        let store = SessionStore::new_at(&dir).expect("open store");

        store.save_position(77).expect("save position");
        store.save_volume(0.25).expect("save volume");
        store
            .save_playback_mode(true, "one")
            .expect("save playback mode");

        let loaded = store.load_session().expect("load session");

        assert_eq!(loaded.playback.current_position_secs, 77);
        assert_eq!(loaded.playback.volume, 0.25);
        assert!(loaded.playback.shuffle_enabled);
        assert_eq!(loaded.playback.repeat_mode, "one");
        assert_eq!(loaded.shell_view.last_view, "home");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn clear_session_resets_playback_and_shell_view_fields() {
        let dir = unique_test_dir("session-clear");
        let store = SessionStore::new_at(&dir).expect("open store");
        let session = PersistedSessionSnapshot {
            playback: PersistedPlaybackSession {
                queue_tracks: vec![sample_track()],
                current_index: Some(0),
                current_position_secs: 55,
                volume: 0.9,
                shuffle_enabled: true,
                repeat_mode: "all".to_string(),
                was_playing: true,
                saved_at: 0,
            },
            shell_view: PersistedShellViewState {
                last_view: "artist".to_string(),
                view_context_id: Some("7".to_string()),
                view_context_type: Some("artist".to_string()),
            },
        };

        store.save_session(&session).expect("save session");
        store.clear_session().expect("clear session");
        let loaded = store.load_session().expect("load session");

        assert!(loaded.playback.queue_tracks.is_empty());
        assert_eq!(loaded.playback.current_index, None);
        assert_eq!(loaded.playback.current_position_secs, 0);
        assert_eq!(loaded.playback.volume, 0.9);
        assert!(loaded.playback.shuffle_enabled);
        assert_eq!(loaded.playback.repeat_mode, "all");
        assert!(!loaded.playback.was_playing);
        assert_eq!(loaded.shell_view.last_view, "home");
        assert_eq!(loaded.shell_view.view_context_id, None);
        assert_eq!(loaded.shell_view.view_context_type, None);

        let _ = std::fs::remove_dir_all(dir);
    }
}
