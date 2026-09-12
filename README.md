# muqbzd — headless Qobuz Connect daemon

This is a **daemon-only fork** of [QBZ](https://github.com/vicrodh/qbz). It carries `qbzd`
and exactly the crates `qbzd` depends on; the Slint desktop UI, its packaging and its
release tooling have been removed. For the desktop player, use upstream.

> **The name.** *mu* reads three ways, all of them true: **μ**, because what's left is a
> ~25 MB binary where the desktop build wanted ~30 GB of RAM to link; **無**, *nothing* —
> this is the one with no interface; and plain *mu*sic. The binary is still `qbzd`, so
> every existing unit file, hook and `qbzd ...` command keeps working.

`qbzd` is a standalone ~25 MB binary that turns any Linux box — a Raspberry Pi, a NAS, the
living-room mini-PC — into a bit-perfect **Qobuz Connect endpoint** that appears in the
official Qobuz apps like a hardware streamer.

- Daemon + full CLI + terminal setup wizard (TUI) in one binary
- Browser-based login that works over SSH; one-file settings hand-off from desktop QBZ
- HiFi wizard with copyable audio-stack config blocks (clipboard works over SSH)
- MPRIS out of the box, live JSON events (`qbzd watch`), service files for systemd/OpenRC/runit
- Event hooks: `qbzd settings set hooks.script /path/to/script` runs your script on
  playback/session events with `QBZ_*` environment variables — push integration for
  audio-box distros (moOde, Volumio, DIY setups), no polling required
- Local pairing (no login required): the daemon advertises itself on the LAN like a
  hardware streamer, so ANY Qobuz account in the household can cast to it from the
  official app — the app hands the device its own session tokens on selection
  (last cast wins, exactly like a Spotify Connect box). A logged-in account is
  optional: when present the daemon streams with its own account, otherwise it
  streams with the token the casting app handed over. Toggle with
  `qbzd settings set qconnect.pairing on|off` (port: `qconnect.pairing_port`, default 8183);
  both apply on the next daemon start

Upstream's manual still applies:
**[Headless Daemon (qbzd) — Wiki](https://github.com/vicrodh/qbz/wiki/Headless-Daemon)**

## Legal / Branding

- This application uses the Qobuz API but is not certified by Qobuz.
- Qobuz is a trademark of Qobuz. QBZ is not affiliated with, endorsed by, or certified by Qobuz.
- **Offline cache** is a temporary playback store for listening without an internet connection while you have a valid subscription. If your subscription becomes invalid, QBZ will remove all cached content after 3 days.
- Qobuz Terms of Service: https://www.qobuz.com/us-en/legal/terms

## Building

Pure Rust workspace. The manifest is `crates/Cargo.toml` (there is none at the repo root)
and build artifacts land in `crates/target`.

```bash
cd crates
cargo build --release -p qbzd        # -> crates/target/release/qbzd
```

System dependencies (Debian/Ubuntu): `build-essential pkg-config libasound2-dev
libjack-jackd2-dev libdbus-1-dev libssl-dev`.

Tests (whole workspace):

```bash
./scripts/cargo-test.sh
```

### aarch64 (Raspberry Pi)

```bash
./scripts/build-aarch64-qbzd.sh      # native on ARM, or cross via Docker on x86-64
./scripts/qbzd-to-pi.sh              # copy the binary to the Pi and restart the service
```

With no UI crate in the graph, a 4 GB Pi can build the daemon natively. The cross path
uses `crates/Cross.toml` to supply the arm64 dev libs inside the `cross` image.

## Repository layout

```
crates/
  qbzd/                  The daemon: CLI, TUI, HTTP API, hooks, MPRIS, Qobuz Connect glue
  qbz-app/               Application-level orchestration (non-UI)
  qbz-core/              Orchestrator (player + audio + API)
  qbz-player/            Playback engine, streaming, queue
  qbz-audio/             Audio backends, loudness, device management
  qbz-qobuz/             Qobuz API client and auth
  qbz-models/            Shared domain types
  qbz-cmaf/ qbz-dsd/     CMAF demux; DSD (DSF/DFF) decoding, DoP, native DSD packing
  qbz-cache/             L1 memory + L2 disk audio caching
  qbz-offline-cache/     Encrypted offline store
  qbz-library/           Local library scanning and metadata
  qbz-radio/ qbz-reco/   Radio and recommendations
  qbz-integrations/      Last.fm, ListenBrainz, MusicBrainz, Discogs
  qbz-media-controls/    MPRIS
  qbz-credentials/ qbz-secrets/  Auth/token storage
  qbz-log/               Logging with secret redaction
  qconnect-protocol/     Qobuz Connect protobuf wire format
  qconnect-core/         Queue and renderer domain models
  qconnect-app/          Application logic and concurrency
  qconnect-transport-ws/ WebSocket transport with qcloud framing
docs/openapi.yaml        The daemon's HTTP API
packaging/linux/         qbzd standalone tarball README
scripts/                 Build, deploy and acceptance scripts
```

The HTTP API is `docs/openapi.yaml`.

## Known Issues

- **Hi-Res seeking** — seeking in tracks >96kHz can take 10-20s (decoder must scan from start). Use prev/next for instant navigation.
- **ALSA Direct** — exclusive access blocks other apps. Use DAC/amplifier physical volume control.
- **DSD DoP / native mode** — seeking is disabled and volume is fixed while a DoP or native-DSD stream is active (any sample manipulation would corrupt the DSD stream). Convert-to-PCM mode has no such limits.

## Contributing

See `CONTRIBUTING.md`. UI changes belong upstream at
[vicrodh/qbz](https://github.com/vicrodh/qbz).

## License

MIT. Upstream QBZ is by [@vicrodh](https://github.com/vicrodh) and its contributors.
