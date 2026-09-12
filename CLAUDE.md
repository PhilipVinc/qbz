# μqbzd — notes for coding agents

Headless Qobuz Connect daemon. The binary is `qbzd`; the project is `μqbzd`
(`muqbzd` in ASCII). It began as a fork of vicrodh/qbz and has diverged into a
daemon-only tree — **there is no desktop UI here and none is planned**. Outside
`README.md`, the repo does not refer to the project it forked from; keep it that
way when editing docs and CI.

## Layout

A standard Cargo workspace: manifest at the repo root, members under `crates/`,
artifacts in `target/`. Plain `cargo build` / `cargo test` from the root work.
(It was not always so — the manifest used to live in `crates/`, so older commit
messages and comments may mention `--manifest-path crates/Cargo.toml`.)

The workspace is `qbzd` plus **exactly** its dependency closure — 23 crates, the
set `cargo tree -p qbzd` resolves. If you find yourself adding a workspace member,
check whether `qbzd` really needs it.

## Commands

```bash
cargo build --release -p qbzd
./scripts/cargo-test.sh          # whole workspace, same command CI runs
./scripts/build-aarch64-qbzd.sh  # Pi binary: native on ARM, cross via Docker on x86-64
./scripts/qbzd-to-pi.sh          # copy to the Pi, restart the service
./scripts/qbzd-acceptance.sh     # end-to-end against a running daemon
```

## Tests and lints: the suite is GREEN, keep it that way

`./scripts/cargo-test.sh` passes clean — 47 suites, 0 failures — on macOS and
linux/arm64. `cargo clippy --workspace --all-targets -- -D warnings` is clean on
both too, and CI enforces fmt + clippy + tests. **A failure is a regression.**

(Historical note, because older commit messages say otherwise: there used to be
"9 known failures" written off as macOS quirks. Eight were real and failed on
Linux too — nothing installed the rustls `CryptoProvider` outside `qbzd`'s
`main`, so any test building a reqwest client panicked. The ninth asserted XDG
paths on a platform that does not use them. All fixed.)

## Formatting and lints

Run `cargo fmt --all`; the tree is rustfmt-clean and CI checks it. (Older
comments say to format only touched lines — that rule is retired.)

For clippy, prefer fixing over silencing. When a lint is genuinely wrong for the
code, allow it **at the item**, with the reason in a comment beside it — not with
a blanket module allow. Two lints are allowed workspace-wide in the root
`Cargo.toml` `[workspace.lints.clippy]`, each with its rationale.

### VERIFY ON LINUX — this is not optional

Large parts of the audio stack are `#[cfg(target_os = "linux")]`. A macOS
`cargo check`/`clippy`/`test` **never compiles them**, so it cannot tell you the
truth about them. This has already bitten once: `cargo clippy --fix` on macOS saw
`stream` as unused in `PlaybackEngine::set_volume` — because the only reader is
inside a Linux cfg block — and rewrote it to `stream: _`. That compiles on macOS
and breaks ALSA hardware volume on the Pi.

The container is the check:

```bash
docker run --rm --platform linux/arm64 \
  -v "$PWD:/src:ro" -v qbzd-aarch64-target:/target \
  -v qbzd-aarch64-registry:/usr/local/cargo/registry \
  -w /src qbzd-aarch64-build:ubuntu22.04 \
  bash -c 'cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'
```

(Image: `docker build --platform linux/arm64 -t qbzd-aarch64-build:ubuntu22.04 \
-f packaging/docker/qbzd-aarch64.Dockerfile packaging/docker`. On Apple silicon
this runs natively, no QEMU. Needs colima or Docker Desktop up.)

Also treat clippy's suggestions as drafts, not patches: in this tree three were
wrong — one pasted a literal `<item>` placeholder, one mangled a counter loop into
assigning to an immutable binding, one swapped `&PathBuf` for `&Path` without
adding the import.

## Hard rules in the code

- **Secrets go through `qbz_log::register_secret`** before anything can log them —
  see `crates/qbz-log/src/redact.rs`. Registering after the first log line is too
  late.
- **Every task holding an `Arc<AppRuntime>` must be abort-and-joined in
  `QconnectHandle::shutdown()`** before `drop(booted)` — `crates/qbzd/src/qconnect/mod.rs`.
  This is the #521 clock-release ordering: a surviving clone holds the ALSA device
  open and the next start fails. Adding a task means adding its teardown.
- **`qbzd` must never resolve Slint.** CI gates on it in both workflows. Nothing in
  the tree pulls it today; the gate exists so a crates.io dependency cannot
  reintroduce it.
- **The release asset shape is an API.** `muqbzd-<version>-linux-<arch>.tar.gz`
  unpacks to one versioned directory holding `qbzd`, `qbzd.service`,
  `completions/` and `README.md`, with a `.sha256` beside it. Installers pin
  this; reshaping it breaks them.

## Versioning

Plain semver, no distro suffixes. `[workspace.package] version` in the root
`Cargo.toml` is the source of truth (2.1.0) and release tags are `vX.Y.Z` matching
it. The release workflow stamps the tag's version through the `QBZD_BUILD_ID` env
var at compile time, which `crates/qbzd/src/main.rs` reads into `VERSION` — that is
what `qbzd version`, `--version` and `/api/status` report. Without it you get the
Cargo version, so a plain `cargo build` is unchanged.

## Branches, CI and releases

`main` is the trunk. CI (`test-crates`) runs on PRs into main and pushes to main,
path-filtered to `crates/**`. Releases are **tags on main**: pushing a `vX.Y.Z` tag
triggers `release.yml`, whose first job refuses any tag whose commit is not an
ancestor of `origin/main`. `build-arm64.yml` is manual-dispatch and publishes
nothing — use it for a Pi test binary without tagging.

## Stale markers you will run into

`crates/qbzd/src/qconnect/*` and `src/tui/wizard_core.rs` carry headers like
`TODO(converge: qconnect-glue) — copied from crates/qbz/src/... ; do not fix bugs
here without fixing the source`, and `DAEMON-ONLY` annotations marking deltas against
that copy. **The file they point at is not in this tree any more** — it went with the
GUI. Treat them as provenance, not as an instruction to go edit a second copy. The
code they head is now the only copy.
