# Security

## Reporting

Report vulnerabilities privately through GitHub's **Report a vulnerability**
button on the Security tab, not as a public issue. Please include what an
attacker can reach and a way to reproduce it.

## What μqbzd exposes

It is a daemon, so it listens. Worth knowing when assessing a report:

- **HTTP control API**, default `127.0.0.1:8182`. Loopback by default.
- **Pairing / LAN receiver**, default port `8183` when Qobuz Connect is enabled.
  This one is reachable from the local network by design — that is how a phone
  on the same Wi-Fi casts to the box, handing it session tokens. Anyone who can
  reach that port can take over playback and, depending on configuration, have
  the daemon stream with tokens they supplied.
- **Credentials at rest** under the profile's data root, and **Qobuz session
  tokens in memory**. Pairing tokens are never persisted.

The threat model assumes a trusted LAN. Exposing either port to the internet is
not a supported configuration; findings that depend on doing so are unlikely to
be treated as vulnerabilities, but do report them if the impact is worse than
"an untrusted network can control playback".

Secrets are redacted from logs through `qbz_log::register_secret`. **A leak of a
token or password into a log file is a valid report** — that path is meant to be
airtight.

## Supported versions

The latest release only. This is a small project; there are no backport branches.
