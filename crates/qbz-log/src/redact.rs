//! Write-time secret redaction — the single most important safety layer in this crate.
//!
//! Two layers, applied in this order by [`redact`]:
//!   1. **Literal live-secret layer:** exact-string replacement of values registered via
//!      [`register_secret`] right after login (the real `user_auth_token`, `app_secret`, …).
//!      Catches tokens logged without a labeled key, which the regexes can't anticipate.
//!   2. **Regex layer:** a fixed set of labeled-key patterns (auth tokens, request_sig,
//!      app_secret, password, bearer/authorization, access/refresh tokens, URL `token=`).
//!      A cheap `.contains` pre-check short-circuits lines with no candidate substring.
//!
//! Every match collapses the secret VALUE to `***REDACTED***` while preserving the
//! labeled key prefix (capture group 1) so the line stays debuggable.

use std::sync::{OnceLock, RwLock};

use regex::Regex;

const REPLACEMENT: &str = "***REDACTED***";
/// Live secret values shorter than this are ignored (too generic to scrub safely).
const MIN_SECRET_LEN: usize = 6;

/// Compiled redaction patterns. Group 1 captures the labeled-key prefix that is kept;
/// the trailing value is what gets replaced. Patterns are case-insensitive.
fn patterns() -> &'static Vec<Regex> {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            // Qobuz user auth token (labeled key, JSON/query/assignment forms)
            r#"(?i)(user_auth_token["':=\s]+)[A-Za-z0-9._\-]+"#,
            // …and the request header form
            r#"(?i)(x-user-auth-token:\s*)\S+"#,
            // request_sig MD5 hex (labeled key form, hex >= 8)
            r#"(?i)(request_sig["':=\s]+)[a-f0-9]{8,}"#,
            // request_sig as a bare URL query param
            r#"(?i)(request_sig=)[a-f0-9]+"#,
            // app secret (app_secret / appsecret)
            r#"(?i)(app_?secret["':=\s]+)[A-Za-z0-9]+"#,
            // password
            r#"(?i)(password["':=\s]+)[^\s"',&]+"#,
            // authorization: Bearer <token>
            r#"(?i)(authorization:\s*bearer\s+)\S+"#,
            // bare bearer token
            r#"(?i)(bearer\s+)[A-Za-z0-9._\-]+"#,
            // OAuth access/refresh tokens (labeled key form)
            r#"(?i)((access|refresh)_token["':=\s]+)[A-Za-z0-9._\-]+"#,
            // generic URL token param
            r#"(?i)(token=)[^&\s"']+"#,
        ]
        .iter()
        .filter_map(|p| Regex::new(p).ok())
        .collect()
    })
}

fn secrets() -> &'static RwLock<Vec<String>> {
    static SECRETS: OnceLock<RwLock<Vec<String>>> = OnceLock::new();
    SECRETS.get_or_init(|| RwLock::new(Vec::new()))
}

/// Register a live secret value so the literal layer scrubs it everywhere, even when
/// logged without a labeled key. Empty / very short values (< [`MIN_SECRET_LEN`]) are
/// ignored, and duplicates are skipped.
pub fn register_secret(value: String) {
    if value.len() < MIN_SECRET_LEN {
        return;
    }
    if let Ok(mut guard) = secrets().write() {
        if !guard.iter().any(|s| s == &value) {
            guard.push(value);
        }
    }
}

/// Cheap pre-check: does the line contain any substring that one of the regexes could
/// match? Avoids running the whole pattern set on the overwhelming majority of lines.
fn has_redaction_candidate(lower: &str) -> bool {
    const NEEDLES: [&str; 6] = ["token", "secret", "password", "bearer", "auth", "sig"];
    NEEDLES.iter().any(|n| lower.contains(n))
}

/// Redact secrets from a single log line. Literal live-secret layer first, then regex.
pub fn redact(line: &str) -> String {
    let mut out = line.to_string();

    // Layer 1 — literal live secrets.
    if let Ok(guard) = secrets().read() {
        for secret in guard.iter() {
            if out.contains(secret.as_str()) {
                out = out.replace(secret.as_str(), REPLACEMENT);
            }
        }
    }

    // Layer 2 — labeled-key regexes (guarded by a cheap substring pre-check).
    let lower = out.to_ascii_lowercase();
    if has_redaction_candidate(&lower) {
        for re in patterns() {
            if re.is_match(&out) {
                out = re
                    .replace_all(&out, format!("${{1}}{REPLACEMENT}").as_str())
                    .into_owned();
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_all_known_shapes() {
        for s in [
            "GET /track?request_sig=ab12cd34ef56 user_auth_token=SEKRET_TOKEN_123",
            "X-User-Auth-Token: SEKRET_TOKEN_123",
            r#"{"app_secret":"abc123def","password":"hunter2"}"#,
            "authorization: Bearer eyJ.aaa.bbb",
        ] {
            let r = redact(s);
            assert!(!r.contains("SEKRET_TOKEN_123"), "leaked auth token: {r}");
            assert!(!r.contains("ab12cd34ef56"), "leaked request_sig: {r}");
            assert!(!r.contains("abc123def"), "leaked app_secret: {r}");
            assert!(!r.contains("hunter2"), "leaked password: {r}");
            assert!(!r.contains("eyJ.aaa.bbb"), "leaked bearer token: {r}");
        }
    }

    #[test]
    fn literal_registry_scrubs_unlabeled_value() {
        register_secret("LIVE_TOKEN_xyz".into());
        let r = redact("blah LIVE_TOKEN_xyz blah");
        assert!(
            !r.contains("LIVE_TOKEN_xyz"),
            "literal secret survived: {r}"
        );
        assert!(r.contains(REPLACEMENT), "no redaction marker: {r}");
    }

    #[test]
    fn short_secret_is_ignored() {
        register_secret("abc".into()); // < MIN_SECRET_LEN -> not registered
        let r = redact("value abc here");
        assert!(r.contains("abc"), "short value should not be scrubbed: {r}");
    }
}
