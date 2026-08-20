// TODO(converge: qconnect-glue) — copied from crates/qbz/src/remote_stream.rs @ b4450965;
// do not fix bugs here without fixing the source, and vice versa.
//! Shared HTTP streaming feeder.
//!
//! Probe a remote audio URL for size + FLAC format, open the player's
//! progressive streaming sink (`Player::play_streaming_dynamic`), then push the
//! body to the returned `BufferWriter` chunk-by-chunk as it arrives. Playback
//! starts as soon as the initial buffer fills — not after the whole file lands.
//!
//! The feeder is range-aware: it opens the body wherever the buffer asks and
//! re-opens with a `Range` header when a reader jumps, so a seek or a session
//! resume costs one request instead of a download of everything before the
//! offset. See `download_and_stream_remote_track`.
//!
//! `reqwest + BufferWriter` bound only, so it stays frontend-side and never
//! crosses the qconnect-app boundary. Used by BOTH the QConnect renderer
//! (`qconnect_engine.rs`) and the Plex playback path (`playback.rs`), so there
//! is exactly one feeder.
//!
//! BIT-PERFECT: `play_streaming_dynamic` decodes the same original bytes and
//! drives the PROTECTED device init from the decoded stream. The
//! `sample_rate`/`bit_depth` parsed here are only the streaming-config hints;
//! the audio backend (`pipewire_backend.rs`, `init_device`, `audio_settings.rs`)
//! is untouched.

use std::time::Duration;

use qbz_player::{BufferWriter, FetchPlan, Player, StreamSeekMode};

/// Format/size facts sniffed from a remote audio URL before streaming.
pub struct RemoteStreamInfo {
    pub content_length: u64,
    pub sample_rate: u32,
    pub channels: u16,
    pub bit_depth: u32,
    pub speed_mbps: f64,
}

/// Probe + open the progressive sink + spawn the background feeder.
///
/// On success the player has begun buffering and `play_streaming_dynamic` will
/// start audio once the initial buffer fills; the body download runs in a
/// spawned task. Errors here mean the caller should fall back to a full
/// download (the probe or the sink open failed).
///
/// DAEMON-ONLY divergence from the desktop copy: returns the feeder's
/// JoinHandle so the caller can abort a superseded download on track change
/// (the qconnect engine keeps exactly one feeder alive).
pub async fn stream_remote_track_into_player(
    player: &Player,
    track_id: u64,
    duration_secs: u64,
    start_position_secs: u64,
    url: &str,
    log_tag: &str,
) -> Result<tokio::task::JoinHandle<()>, String> {
    let stream_info = probe_remote_stream_info(url).await?;
    log::info!(
        "[{}/STREAMING] Track {} - {:.2} MB, {}Hz, {} ch, {}-bit, {:.1} MB/s",
        log_tag,
        track_id,
        stream_info.content_length as f64 / (1024.0 * 1024.0),
        stream_info.sample_rate,
        stream_info.channels,
        stream_info.bit_depth,
        stream_info.speed_mbps
    );

    let writer = player
        .play_streaming_dynamic(
            track_id,
            stream_info.sample_rate,
            stream_info.channels,
            stream_info.bit_depth,
            stream_info.content_length,
            stream_info.speed_mbps,
            duration_secs,
            start_position_secs,
            // The feeder below re-opens the body wherever the buffer asks, so
            // the decoder may seek anywhere in the track for the cost of one
            // request.
            StreamSeekMode::RangeRequests,
        )
        .map_err(|err| format!("start streaming remote track {track_id}: {err}"))?;

    let url = url.to_string();
    let content_length = stream_info.content_length;
    let log_tag = log_tag.to_string();
    let feeder = tokio::spawn(async move {
        if let Err(err) =
            download_and_stream_remote_track(&url, writer, track_id, content_length, &log_tag).await
        {
            log::error!(
                "[{}/STREAMING] Track {} failed while streaming: {}",
                log_tag,
                track_id,
                err
            );
        }
    });

    Ok(feeder)
}

/// HEAD for content-length, then a small `Range: bytes=0-65535` GET to (a)
/// measure throughput and (b) parse the FLAC `STREAMINFO` block for the real
/// sample rate / channels / bit depth. Never defaults silently for FLAC (a
/// wrong sample rate would silently resample hi-res).
pub async fn probe_remote_stream_info(url: &str) -> Result<RemoteStreamInfo, String> {
    use std::time::Instant;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| format!("create stream probe client: {err}"))?;

    let head_response = client
        .head(url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await
        .map_err(|err| format!("probe HEAD request failed: {}", describe_reqwest_error(&err)))?;

    if !head_response.status().is_success() {
        return Err(format!(
            "probe HEAD request failed with status {}",
            head_response.status()
        ));
    }

    let content_length = head_response
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| "probe missing content-length header".to_string())?;

    let start_time = Instant::now();
    let range_response = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Range", "bytes=0-65535")
        .send()
        .await
        .map_err(|err| format!("probe range request failed: {}", describe_reqwest_error(&err)))?;

    if !range_response.status().is_success() {
        return Err(format!(
            "probe range request failed with status {}",
            range_response.status()
        ));
    }

    let initial_bytes = range_response
        .bytes()
        .await
        .map_err(|err| format!("read probe bytes failed: {}", describe_reqwest_error(&err)))?;

    let elapsed = start_time.elapsed();
    let speed_mbps = if elapsed.as_secs_f64() > 0.0 {
        (initial_bytes.len() as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
    } else {
        10.0
    };

    let (sample_rate, channels, bit_depth) =
        if initial_bytes.len() >= 26 && initial_bytes.starts_with(b"fLaC") {
            let sample_rate = ((initial_bytes[18] as u32) << 12)
                | ((initial_bytes[19] as u32) << 4)
                | ((initial_bytes[20] as u32) >> 4);
            let channels = ((initial_bytes[20] >> 1) & 0x07) + 1;
            let bit_depth = ((initial_bytes[20] & 0x01) << 4) | ((initial_bytes[21] >> 4) & 0x0F);
            (sample_rate, channels as u16, (bit_depth + 1) as u32)
        } else {
            log::warn!("[remote-stream] Non-FLAC probe for remote handoff, using defaults");
            (44_100, 2, 16)
        };

    Ok(RemoteStreamInfo {
        content_length,
        sample_rate,
        channels,
        bit_depth,
        speed_mbps,
    })
}

/// One response header as a string for logging, or `-` when absent.
fn header_str(headers: &reqwest::header::HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
        .to_string()
}

/// Why the body loop stopped.
enum BodyEnd {
    /// The body ran out: the plan is filled, or the server closed early.
    Ended,
    /// A reader wants a different offset.
    Restart(FetchPlan),
    /// The body broke mid-flight.
    Failed(String),
}

/// Range-aware body pump: open the body at the offset the buffer asks for,
/// push it chunk-by-chunk, and re-open somewhere else the moment a reader
/// jumps.
///
/// `BufferWriter::take_request` fires when the decoder seeks — a session
/// resume, a scrub, the engine rebuild after a long pause — and the body is
/// re-opened with `Range: bytes=<offset>-` instead of walking there from
/// wherever we are. When a body ends, `next_plan` hands back either a
/// pending request or the first hole an earlier jump left behind, so the
/// track still ends up whole (and promotable to the in-memory cache) even
/// though it was not downloaded in order.
///
/// Two consequences of being able to re-open anywhere:
///
/// * A broken body is no longer fatal. It is re-opened at the byte it died
///   on, so a CDN dropping a long-lived connection costs a round trip
///   instead of the track. Only a streak of bodies that yield nothing at
///   all gives up.
/// * A server that ignores `Range` and answers `200` with the whole file is
///   handled: the write head is re-pointed at byte 0 and the stream
///   degrades to the old sequential behavior, rather than filling the
///   buffer with bytes attributed to the wrong offsets.
pub async fn download_and_stream_remote_track(
    url: &str,
    writer: BufferWriter,
    track_id: u64,
    content_length: u64,
    log_tag: &str,
) -> Result<(), String> {
    use futures_util::StreamExt;
    use std::time::Instant;

    struct FailGuard {
        writer: BufferWriter,
        armed: bool,
    }
    impl Drop for FailGuard {
        fn drop(&mut self) {
            if self.armed {
                let _ = self
                    .writer
                    .error("remote stream aborted before completion".into());
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
        .timeout(Duration::from_secs(300))
        .build()
        .map_err(|err| format!("create remote streaming client: {err}"))?;

    let mut plan = writer.initial_plan();
    let mut bytes_received = 0u64;
    let start_time = Instant::now();
    let mut last_log_time = Instant::now();
    // Bodies that produced nothing since the last one that did. A single
    // one is a hiccup worth re-opening for; a streak means the offset is
    // not being served and we would spin forever.
    let mut barren_bodies = 0u32;

    'plans: loop {
        let mut request = client.get(url).header("User-Agent", "Mozilla/5.0");
        if !plan.is_whole_file() {
            request = request.header("Range", plan.range_header());
        }

        let sent = Instant::now();
        let response = request.send().await.map_err(|err| {
            format!(
                "start remote streaming request failed: {}",
                describe_reqwest_error(&err)
            )
        })?;
        let opened_ms = sent.elapsed().as_millis();

        if !response.status().is_success() {
            return Err(format!(
                "remote streaming request failed with status {}",
                response.status()
            ));
        }

        // Only a 206 actually starts where we asked. Anything else is the
        // whole file, so its bytes belong at 0.
        let honors_range =
            plan.is_whole_file() || response.status() == reqwest::StatusCode::PARTIAL_CONTENT;
        if !honors_range {
            log::warn!(
                "[{}/STREAMING] Track {} asked for {} and got {} (not partial content) - feeding from byte 0",
                log_tag,
                track_id,
                plan.range_header(),
                response.status()
            );
            writer
                .begin_at(0)
                .map_err(|err| format!("re-point write head to 0: {err}"))?;
        }
        let body_start = if honors_range { plan.offset } else { 0 };
        // A bounded plan is a gap fill; stop once it is filled. A body that
        // came back unranged has no such bound - it is the whole file.
        let plan_limit = if honors_range { plan.byte_len() } else { None };

        // A ranged read at an offset the edge does not hold has measured
        // ~5 s of time-to-first-byte on this CDN, while a fresh connection
        // to it costs 150 ms — so the wait is the edge filling its cache,
        // not our client. `x-cache` says which it was (TCP_HIT vs
        // TCP_MISS), and pairing it with the elapsed time is what makes
        // that attributable rather than guessed at.
        if !plan.is_whole_file() || opened_ms > 1000 {
            log::info!(
                "[{}/STREAMING] Track {} body open at byte {} ({}) in {}ms [x-cache: {}]",
                log_tag,
                track_id,
                plan.offset,
                plan.range_header(),
                opened_ms,
                header_str(response.headers(), "x-cache")
            );
        }

        let mut body_bytes = 0u64;
        let mut stream = response.bytes_stream();

        let body_end = loop {
            let chunk = tokio::select! {
                biased;
                // A reader jumped. Honor it now rather than after the next
                // chunk: on a stalled connection that wait is the whole
                // latency the range request exists to remove.
                _ = writer.request_notified() => {
                    match writer.take_request() {
                        Some(next) => break BodyEnd::Restart(next),
                        None => continue,
                    }
                }
                item = stream.next() => match item {
                    Some(Ok(chunk)) => chunk,
                    Some(Err(err)) => {
                        break BodyEnd::Failed(describe_reqwest_error(&err));
                    }
                    None => break BodyEnd::Ended,
                },
            };

            bytes_received += chunk.len() as u64;
            body_bytes += chunk.len() as u64;

            if let Err(err) = writer.push_chunk(&chunk) {
                log::error!(
                    "[{}/STREAMING] Failed to push chunk for track {}: {}",
                    log_tag,
                    track_id,
                    err
                );
                guard.armed = false;
                let _ = writer.error(format!("push_chunk failed: {err}"));
                return Err(format!("push_chunk failed: {err}"));
            }

            if let Some(limit) = plan_limit {
                if body_bytes >= limit {
                    break BodyEnd::Ended;
                }
            }

            if let Some(next) = writer.take_request() {
                break BodyEnd::Restart(next);
            }

            let now = Instant::now();
            if now.duration_since(last_log_time) >= Duration::from_secs(2) && content_length > 0 {
                let held = writer.buffer_size() as f64;
                let avg_speed = (bytes_received as f64 / start_time.elapsed().as_secs_f64())
                    / (1024.0 * 1024.0);
                log::info!(
                    "[{}/STREAMING] Track {} {:.1}% ({:.2}/{:.2} MB) @ {:.2} MB/s",
                    log_tag,
                    track_id,
                    (held / content_length as f64) * 100.0,
                    held / (1024.0 * 1024.0),
                    content_length as f64 / (1024.0 * 1024.0),
                    avg_speed
                );
                last_log_time = now;
            }
        };

        if body_bytes > 0 {
            barren_bodies = 0;
        } else {
            barren_bodies += 1;
        }

        match body_end {
            BodyEnd::Restart(next) => {
                log::info!(
                    "[{}/STREAMING] Track {} seek re-opens the body at byte {} ({} bytes read from the previous one)",
                    log_tag,
                    track_id,
                    next.offset,
                    body_bytes
                );
                plan = next;
                continue 'plans;
            }
            BodyEnd::Failed(err) => {
                if barren_bodies >= 3 {
                    return Err(format!(
                        "remote streaming body failed at byte {} with no progress: {err}",
                        plan.offset
                    ));
                }
                // Pick up where it died. A reader jump still wins over
                // resuming a body nobody is waiting on.
                let resume_from = body_start + body_bytes;
                let filled = plan.end.is_some_and(|end| resume_from >= end);
                if !filled {
                    log::warn!(
                        "[{}/STREAMING] Track {} body broke at byte {} ({}) - re-opening there",
                        log_tag,
                        track_id,
                        resume_from,
                        err
                    );
                    plan = match writer.take_request() {
                        Some(next) => next,
                        None => {
                            let resumed = FetchPlan {
                                offset: resume_from,
                                end: plan.end,
                            };
                            writer
                                .begin_at(resumed.offset)
                                .map_err(|err| format!("re-point write head: {err}"))?;
                            resumed
                        }
                    };
                    continue 'plans;
                }
                log::warn!(
                    "[{}/STREAMING] Track {} body broke at byte {} after filling its range ({})",
                    log_tag,
                    track_id,
                    resume_from,
                    err
                );
            }
            BodyEnd::Ended => {
                if barren_bodies > 0 && barren_bodies < 3 {
                    log::warn!(
                        "[{}/STREAMING] Track {} body at byte {} closed without data - retrying",
                        log_tag,
                        track_id,
                        plan.offset
                    );
                    continue 'plans;
                }
                if barren_bodies >= 3 {
                    return Err(format!(
                        "remote stream made no progress at byte {} after {} empty bodies",
                        plan.offset, barren_bodies
                    ));
                }
            }
        }

        match writer.next_plan() {
            Some(next) => plan = next,
            // Nothing left to fetch, or no reader left to fetch it for.
            None => break 'plans,
        }
    }

    guard.armed = false;
    if let Err(err) = writer.complete() {
        log::error!(
            "[{}/STREAMING] Failed to mark stream complete for track {}: {}",
            log_tag,
            track_id,
            err
        );
        let _ = writer.error(format!("complete failed: {err}"));
        return Err(format!("complete failed: {err}"));
    }

    log::info!(
        "[{}/STREAMING] Track {} complete: {:.2} MB fetched in {:.1}s",
        log_tag,
        track_id,
        bytes_received as f64 / (1024.0 * 1024.0),
        start_time.elapsed().as_secs_f64()
    );

    Ok(())
}

/// reqwest's `Display` hides the source chain — which is exactly where the
/// diagnosis lives (Akamai's >100-header small-object flood surfaces as hyper's
/// "message head is too large" two levels down). Walk `source()` and join the
/// chain so logs AND signature matching see the real cause.
pub fn describe_reqwest_error(err: &reqwest::Error) -> String {
    use std::error::Error as _;
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

/// True when an error message (already chain-expanded by
/// [`describe_reqwest_error`]) shows hyper's hard-coded h1 100-header cap.
/// Akamai answers SMALL raw-url objects with ~106 headers (the `X-AK-GRN` /
/// `X-AK-FWD-ERROR: ERR_POC_FWD_OBJ_TOO_SMALL` flood), so EVERY reqwest fetch
/// of such an URL fails this way — streaming probe and full download alike.
pub fn is_header_flood_error(message: &str) -> bool {
    let haystack = message.to_ascii_lowercase();
    haystack.contains("message head is too large") || haystack.contains("too many headers")
}
