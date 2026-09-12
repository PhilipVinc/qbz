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

## Tests: 9 failures on macOS are NOT regressions

On this macOS machine the suite always ends with the same 9 failures, on a clean
tree, with no changes applied:

- 6 × `qbz-app` `shell::tests::*`
- 2 × `qbz-qobuz` `client::tests::offline_gate_*`
- 1 × `qbzd` `paths::tests::defaults_resolve_under_xdg_roots_without_touching_real_home`

All are the same rustls "no provider set" panic plus an XDG-path assumption. Do
not "fix" them as part of an unrelated change, and do not report them as broken by
your work — diff the failure list against this one before concluding anything.

## Formatting

The tree **is** rustfmt-clean as of the workspace-wide format, and CI enforces it
(`cargo fmt --all --check` in `test-crates.yml`). Just run `cargo fmt --all`.

Historical note, because older comments still say the opposite: formatting used to
be restricted to touched lines only, because a crate-wide run buried real diffs and
created rebase conflicts against upstream. Neither applies now.

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
- **Do not reshape the release tarball.** `qbzd-<version>-linux-<arch>.tar.gz`
  containing `qbzd-<version>-linux-<arch>/qbzd` is pinned by moOde's
  `qobuz-installer.sh`; changing the layout breaks installs in the field.

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
triggers `fork-qbzd-release.yml`, whose first job refuses any tag whose commit is not
an ancestor of `origin/main`. (`qbzd-v*` also fires it, for the deployed moOde
installer that still pins that shape.) `build-qbzd-arm64.yml` is manual-dispatch and publishes
nothing — use it for a Pi test binary without tagging.

## Stale markers you will run into

`crates/qbzd/src/qconnect/*` and `src/tui/wizard_core.rs` carry headers like
`TODO(converge: qconnect-glue) — copied from crates/qbz/src/... ; do not fix bugs
here without fixing the source`, and `DAEMON-ONLY` annotations marking deltas against
that copy. **The file they point at is not in this tree any more** — it went with the
GUI. Treat them as provenance, not as an instruction to go edit a second copy. The
code they head is now the only copy.
