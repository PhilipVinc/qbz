# Contributing to muqbzd

This project is actively evolving. Contributions are welcome, but we have a few rules to keep releases stable and avoid regressions (especially around audio output).

## Where the code lives

This fork is **daemon-only**: the Rust workspace under `crates/` carries `qbzd`
and exactly the crates it depends on. The Slint desktop UI (`crates/qbz`,
`crates/qbz-ui`, the theme/i18n/cast/lyrics/mixtape/plex/playlist-import crates)
lives upstream at [vicrodh/qbz](https://github.com/vicrodh/qbz) and was removed
here — port UI changes there, not to this tree.

## Quick rules

- Write clear, concise English (no emojis in code, comments, or commit messages).
- Keep PRs focused and small when possible.
- Do not change app branding or legal disclaimers without discussing it first.
- Do not modify protected audio-backend behavior unless explicitly requested by the maintainer.

## Branch naming

We use a consistent branch naming scheme:

`<type>/<origin>/<branch_name>`

- `type`: `feature` | `bugfix` | `hotfix` | `refactor` | `release` | `chore` | `docs`
- `origin`:
  - `internal`: created/owned by maintainers
  - `external`: branches/commits authored by third-party contributors (PRs)

Examples:

- `feature/internal/offline-cache-encryption`
- `bugfix/internal/login-footer-alignment`
- `docs/internal/contributing-process`
- `feature/external/add-album-to-playlist`

## Branch workflow

We use a **pre-release integration branch** to keep `main` stable and release-ready at all times.

```
feature/xyz ──┐
bugfix/abc  ──┼──> pre-release ──> main (tagged release)
hotfix/123  ──┘
```

### Branch hierarchy

1. **`main`** - Releases ONLY. Protected branch. Merging here triggers a tagged release.
2. **`pre-release`** - Integration branch. All features and fixes merge here first.
3. **`feature/*`, `bugfix/*`, etc.** - Individual work branches.

### For contributors

**All PRs must target `pre-release`, not `main`.**

PRs targeting `main` will be closed and asked to retarget to `pre-release`.

### Procedure (maintainer)

1. **Triage**
   - Confirm scope and that it does not touch protected areas (audio routing/backends, credential storage, etc.) unless requested.
   - Verify PR targets `pre-release` (not `main`).
2. **Check out the PR**
   - `gh pr checkout <PR_NUMBER>`
3. **Rename the checked-out branch (local)**
   - Use an `external` branch name so it's obvious these commits are third-party authored:
   - `git branch -m <type>/external/<topic>`
4. **Merge to pre-release**
   - `git checkout pre-release`
   - `git merge --no-ff <type>/external/<topic>`
5. **Run checks**
   - Build/validate a touched crate: `cargo check -p <crate>` (run from
     `crates/`), or the whole workspace with `./scripts/cargo-test.sh`.
6. **Push pre-release**
   - `git push origin pre-release`
7. **Close the PR with a comment** explaining it was merged to `pre-release`.

### Releasing to main

When ready to release:

```bash
git checkout main
git merge pre-release
git push origin main
git tag vX.Y.Z
git push origin vX.Y.Z
```

This is done exclusively by maintainers.

### Merge strategy note (to preserve “external” authorship)

If you want the git history to clearly show third-party authored commits, avoid “squash merge”.
Prefer:

- **Create a merge commit**, or
- **Rebase and merge** (preserves individual commits/authors)

## What to include in PRs

- A short description of the problem and solution.
- Notes about any breaking changes or migrations.

## What not to include

- Large refactors mixed with feature work.
- Desktop-UI changes — this fork has no UI; send those upstream.
