# Changelog

Notable changes per release. Versions are plain semver; releases are `vX.Y.Z`
tags on `main`.

## 2.1.0 — unreleased

First release of μqbzd as its own project rather than a fork branch.

### Changed

- **The tree is daemon-only.** The Slint desktop UI and everything that served
  it — 14 crates, the vendored Slint/femtovg forks, the flatpak/snap/AUR/gentoo/
  AppImage packaging, the GUI release workflows and the desktop release notes —
  are gone. What remains is `qbzd` plus exactly its dependency closure, 23
  crates. Tracked files went 1418 → 364; the lockfile went 1008 → 627 packages.
- **Standard Cargo layout.** The workspace manifest moved from
  `crates/Cargo.toml` to the repo root, members are `crates/*`, and artifacts
  land in `target/`. Plain `cargo build` / `cargo test` from the root now work.
- **Plain semver.** No more `.moodeN` build suffixes in the version scheme.
  Release tags are `vX.Y.Z` matching `Cargo.toml`. `QBZD_BUILD_ID` still stamps
  a tag's version into the binary at compile time, which is what makes a
  prerelease tag report itself accurately.
- **Release assets renamed** to `muqbzd-<version>-linux-<arch>.tar.gz`, each
  with a `.sha256` beside it. The archive layout is unchanged: one versioned
  directory holding `qbzd`, `qbzd.service`, `completions/` and `README.md`.
- The daemon calls itself μqbzd where it names itself — `qbzd status`,
  `qbzd version`, `--help`, the setup TUI, and the service units it generates.
  `qbzd --version` still prints `qbzd <version>`, which is what installers parse.
- `main` is the trunk and the only branch releases are cut from; CI refuses a
  release tag whose commit is not an ancestor of `main`.
- The workspace is rustfmt-clean, and CI enforces it.

### Fixed

Everything the fork accumulated before it became its own project — Qobuz
Connect handoff and reporting accuracy, HTTP range requests for seek and
resume, gapless prefetch, ALSA clock and buffer handling, and a large memory
footprint reduction — is in this tree. Several of those changes were also sent
to the project this forked from.
