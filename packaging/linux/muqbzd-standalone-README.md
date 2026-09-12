# μqbzd — headless Qobuz Connect daemon (standalone download)

This tarball is the independent daemon download (no dependency on the desktop
`qbz` app, no deb/rpm needed). Install it on the box wired to your DAC — a
Raspberry Pi, an LXC, a living-room NUC — run `qbzd setup` once, and it
appears in the official Qobuz app as a Qobuz Connect device.

## Contents

- `qbzd` — the daemon binary (also its own CLI client and setup TUI)
- `qbzd.service` — a systemd user unit (shipped, not enabled)
- `completions/` — bash/zsh/fish shell completions

## Install

Recommended — matches the shipped unit's `ExecStart=/usr/bin/qbzd run`:

```bash
sudo install -Dm755 qbzd /usr/bin/qbzd
sudo install -Dm644 qbzd.service /usr/lib/systemd/user/qbzd.service
systemctl --user daemon-reload
```

Prefer a user-local install instead? Copy `qbzd` anywhere on your `$PATH`
(e.g. `~/.local/bin/qbzd`), then edit `qbzd.service`'s `ExecStart=` line to
point at that path before copying it to `~/.config/systemd/user/qbzd.service`
and running `systemctl --user daemon-reload`.

Shell completions (optional):

```bash
sudo cp completions/qbzd.bash /usr/share/bash-completion/completions/qbzd
# zsh: copy completions/qbzd.zsh into a directory on your $fpath
# fish: copy completions/qbzd.fish into ~/.config/fish/completions/
```

## Required: enable linger

Without linger, the user unit stops the moment you log out of SSH and the
device vanishes from the Qobuz app:

```bash
sudo loginctl enable-linger $USER
```

`qbzd status` warns when linger is off.

## First run

```bash
qbzd setup
```

`qbzd setup` is the six-screen configurator: log in to Qobuz, pick the audio
device, name the Connect device. It edits the same stores `qbzd run` reads,
so one pass is enough (revisit any time to change a setting).

Then enable and start the daemon:

```bash
systemctl --user enable --now qbzd
systemctl --user status qbzd
```

## Why glibc 2.35

This binary is built on ubuntu-22.04 (glibc 2.35) specifically so it runs on
Raspberry Pi OS bookworm (glibc 2.36) and similarly-aged distros without a
rebuild — see `qbz-nix-docs/qbz-daemon/01-architecture.md` (D13).
