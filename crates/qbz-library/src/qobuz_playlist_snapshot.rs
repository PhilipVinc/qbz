//! Local snapshot of the user's QOBUZ playlists (offline-mode port, B7/B8).
//!
//! Spec D11 left an HONEST LIMIT: playlist names and membership live only in
//! the Qobuz API, so offline a mixed playlist falls back to a synthesized
//! "Playlist (N local)" name and shows zero Qobuz rows. This module stores a
//! point-in-time snapshot captured opportunistically from data the app
//! ALREADY fetches while online (no new API traffic):
//!
//! - NAMES: every user-playlist list load (sidebar / playlist manager)
//!   upserts id + name (+ owner, track_count) for ALL listed playlists —
//!   cheap names-only rows, no track membership.
//! - MEMBERSHIP: opening a playlist DETAIL online full-replaces its snapshot
//!   track ids (the detail fetch already returns the full track list).
//!   Membership is recorded ONLY for playlists already captured by the
//!   names producer (the user's own list) — a merely-viewed public playlist
//!   never lands in the snapshot, so the offline surfaces stay the user's.
//!
//! Rows are point-in-time: offline consumers show them as-is (no staleness
//! UI in v1); `snapped_at` is stamped for the future.
//!
//! All functions take `&Connection` (the local_playlists idiom): no Tauri
//! state, no async runtime — testable with in-memory SQLite.

use std::collections::HashMap;

use rusqlite::{params, Connection, OptionalExtension, Result};

/// One snapshot header row.
#[derive(Debug, Clone)]
pub struct SnapshotHeader {
    pub qobuz_playlist_id: u64,
    pub name: String,
    pub owner: Option<String>,
    /// The playlist's TOTAL Qobuz track count at snapshot time (not the
    /// offline-playable subset).
    pub track_count: Option<u32>,
    /// Unix ms when this header was last written.
    pub snapped_at: i64,
}

/// Names-producer input (one listed playlist).
#[derive(Debug, Clone)]
pub struct SnapshotNameEntry {
    pub qobuz_playlist_id: u64,
    pub name: String,
    pub owner: Option<String>,
    pub track_count: Option<u32>,
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Create the snapshot tables. Idempotent (`IF NOT EXISTS`), run by
/// `LibraryDatabase::open` next to the rest of the schema.
pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS qobuz_playlist_snapshot (
            qobuz_playlist_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            owner TEXT,
            track_count INTEGER,
            snapped_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS qobuz_playlist_snapshot_tracks (
            qobuz_playlist_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            track_id INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_qobuz_playlist_snapshot_tracks
            ON qobuz_playlist_snapshot_tracks(qobuz_playlist_id, position);
        "#,
    )
}

/// NAMES producer: upsert a header row for every listed playlist. Never
/// touches the membership table. Stamps `snapped_at` = now on each row.
pub fn upsert_names(conn: &Connection, entries: &[SnapshotNameEntry]) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let ts = now_ms();
    let mut stmt = conn.prepare(
        "INSERT INTO qobuz_playlist_snapshot
             (qobuz_playlist_id, name, owner, track_count, snapped_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(qobuz_playlist_id) DO UPDATE SET
             name = excluded.name,
             owner = excluded.owner,
             track_count = excluded.track_count,
             snapped_at = excluded.snapped_at",
    )?;
    for e in entries {
        stmt.execute(params![
            e.qobuz_playlist_id as i64,
            e.name,
            e.owner,
            e.track_count,
            ts
        ])?;
    }
    Ok(())
}

/// MEMBERSHIP producer: full-replace the snapshot track ids of ONE playlist
/// (detail load) and refresh its header (name / owner / track_count /
/// snapped_at). Returns `false` (writing NOTHING) when the playlist has no
/// header row — i.e. it was never captured by the names producer, so it is
/// not one of the user's listed playlists.
pub fn replace_tracks(
    conn: &Connection,
    qobuz_playlist_id: u64,
    name: &str,
    owner: Option<&str>,
    track_ids: &[u64],
) -> Result<bool> {
    let updated = conn.execute(
        "UPDATE qobuz_playlist_snapshot
            SET name = ?2, owner = ?3, track_count = ?4, snapped_at = ?5
          WHERE qobuz_playlist_id = ?1",
        params![
            qobuz_playlist_id as i64,
            name,
            owner,
            track_ids.len() as u32,
            now_ms()
        ],
    )?;
    if updated == 0 {
        return Ok(false);
    }
    conn.execute(
        "DELETE FROM qobuz_playlist_snapshot_tracks WHERE qobuz_playlist_id = ?1",
        params![qobuz_playlist_id as i64],
    )?;
    let mut stmt = conn.prepare(
        "INSERT INTO qobuz_playlist_snapshot_tracks (qobuz_playlist_id, position, track_id)
         VALUES (?1, ?2, ?3)",
    )?;
    for (pos, tid) in track_ids.iter().enumerate() {
        stmt.execute(params![qobuz_playlist_id as i64, pos as i64, *tid as i64])?;
    }
    Ok(true)
}

fn row_to_header(r: &rusqlite::Row) -> Result<SnapshotHeader> {
    Ok(SnapshotHeader {
        qobuz_playlist_id: r.get::<_, i64>("qobuz_playlist_id")? as u64,
        name: r.get("name")?,
        owner: r.get("owner")?,
        track_count: r.get("track_count")?,
        snapped_at: r.get("snapped_at")?,
    })
}

/// One snapshot header, or None.
pub fn get_header(conn: &Connection, qobuz_playlist_id: u64) -> Result<Option<SnapshotHeader>> {
    conn.query_row(
        "SELECT qobuz_playlist_id, name, owner, track_count, snapped_at
           FROM qobuz_playlist_snapshot WHERE qobuz_playlist_id = ?1",
        params![qobuz_playlist_id as i64],
        row_to_header,
    )
    .optional()
}

/// All snapshot headers.
pub fn all_headers(conn: &Connection) -> Result<Vec<SnapshotHeader>> {
    let mut stmt = conn.prepare(
        "SELECT qobuz_playlist_id, name, owner, track_count, snapped_at
           FROM qobuz_playlist_snapshot",
    )?;
    let mut out = Vec::new();
    for r in stmt.query_map([], row_to_header)? {
        out.push(r?);
    }
    Ok(out)
}

/// One playlist's snapshot track ids in snapshot (position) order.
pub fn track_ids(conn: &Connection, qobuz_playlist_id: u64) -> Result<Vec<u64>> {
    let mut stmt = conn.prepare(
        "SELECT track_id FROM qobuz_playlist_snapshot_tracks
          WHERE qobuz_playlist_id = ?1 ORDER BY position",
    )?;
    let mut out = Vec::new();
    for r in stmt.query_map(params![qobuz_playlist_id as i64], |r| r.get::<_, i64>(0))? {
        out.push(r? as u64);
    }
    Ok(out)
}

/// playlist id -> snapshot track ids in position order, for every playlist
/// with membership rows (availability intersection, B8).
pub fn all_track_ids(conn: &Connection) -> Result<HashMap<u64, Vec<u64>>> {
    let mut stmt = conn.prepare(
        "SELECT qobuz_playlist_id, track_id FROM qobuz_playlist_snapshot_tracks
          ORDER BY qobuz_playlist_id, position",
    )?;
    let mut out: HashMap<u64, Vec<u64>> = HashMap::new();
    for r in stmt.query_map([], |r| {
        Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64))
    })? {
        let (pid, tid) = r?;
        out.entry(pid).or_default().push(tid);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        init_schema(&c).unwrap();
        c
    }

    fn name_entry(id: u64, name: &str, count: u32) -> SnapshotNameEntry {
        SnapshotNameEntry {
            qobuz_playlist_id: id,
            name: name.to_string(),
            owner: Some("me".to_string()),
            track_count: Some(count),
        }
    }

    #[test]
    fn roundtrip_header_and_tracks() {
        let c = conn();
        upsert_names(&c, &[name_entry(42, "Road Trip", 3)]).unwrap();
        let wrote = replace_tracks(&c, 42, "Road Trip", Some("me"), &[30, 10, 20]).unwrap();
        assert!(wrote);

        let h = get_header(&c, 42).unwrap().unwrap();
        assert_eq!(h.name, "Road Trip");
        assert_eq!(h.owner.as_deref(), Some("me"));
        assert_eq!(h.track_count, Some(3));
        assert!(h.snapped_at > 0);

        // Snapshot order preserved, not sorted by id.
        assert_eq!(track_ids(&c, 42).unwrap(), vec![30, 10, 20]);
        let all = all_track_ids(&c).unwrap();
        assert_eq!(all.get(&42).unwrap(), &vec![30, 10, 20]);
    }

    #[test]
    fn replace_is_full_replace() {
        let c = conn();
        upsert_names(&c, &[name_entry(7, "Mix", 3)]).unwrap();
        replace_tracks(&c, 7, "Mix", None, &[1, 2, 3]).unwrap();
        replace_tracks(&c, 7, "Mix renamed", None, &[9]).unwrap();

        assert_eq!(track_ids(&c, 7).unwrap(), vec![9]);
        let h = get_header(&c, 7).unwrap().unwrap();
        assert_eq!(h.name, "Mix renamed");
        assert_eq!(h.track_count, Some(1));
        // No leftover rows from the first write.
        let total: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM qobuz_playlist_snapshot_tracks",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(total, 1);
    }

    #[test]
    fn names_only_rows_without_tracks() {
        let c = conn();
        upsert_names(&c, &[name_entry(1, "A", 10), name_entry(2, "B", 0)]).unwrap();

        assert_eq!(all_headers(&c).unwrap().len(), 2);
        assert!(track_ids(&c, 1).unwrap().is_empty());
        assert!(all_track_ids(&c).unwrap().is_empty());

        // Re-upserting updates the name in place without creating track rows.
        upsert_names(&c, &[name_entry(1, "A renamed", 11)]).unwrap();
        let h = get_header(&c, 1).unwrap().unwrap();
        assert_eq!(h.name, "A renamed");
        assert_eq!(h.track_count, Some(11));
        assert!(track_ids(&c, 1).unwrap().is_empty());
    }

    #[test]
    fn replace_refuses_unknown_playlist() {
        let c = conn();
        // No names row -> the detail producer writes NOTHING (a merely
        // viewed public playlist must not land in the snapshot).
        let wrote = replace_tracks(&c, 99, "Someone's list", None, &[1, 2]).unwrap();
        assert!(!wrote);
        assert!(get_header(&c, 99).unwrap().is_none());
        assert!(track_ids(&c, 99).unwrap().is_empty());
    }
}
