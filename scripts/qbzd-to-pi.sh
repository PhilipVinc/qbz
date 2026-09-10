#!/usr/bin/env bash
#
# qbzd-to-pi.sh — build the aarch64 qbzd HERE and install it on the Pi,
# without going through GitHub Actions.
#
# The CI round trip is ~7 minutes: tag, push, wait for the runner, download the
# release tarball. This is ~35 s for an incremental build plus the copy, which
# is the difference between testing an idea and abandoning it. CI stays the
# thing that builds what other people install; this is for the loop where the
# only listener is the person running it.
#
# HOW IT BUILDS
#   A native arm64 Linux container under colima — not a cross toolchain. qbzd
#   links ALSA, D-Bus and OpenSSL, and matching the Pi's own libraries is far
#   easier than persuading a cross linker to find aarch64 copies of all three.
#   `zig cc` was tried and rejected for exactly that reason.
#
#   Rust artifacts live in a docker VOLUME, never on the virtiofs mount: cargo
#   writes tens of thousands of small files and doing that across the mount is
#   slower than the compile.
#
# REQUIREMENTS
#   colima running, with a mount covering this checkout. The mount is the usual
#   failure: colima only shares the paths in ~/.colima/default/colima.yaml, and
#   a path outside them appears INSIDE the VM as an empty directory rather than
#   as an error — a build that "succeeds" against no source.
#
# USAGE
#   ./scripts/qbzd-to-pi.sh              # build and install on $QBZD_PI (default: moode)
#   ./scripts/qbzd-to-pi.sh --build-only # leave the binary in dist/, install nothing
set -euo pipefail

PI="${QBZD_PI:-moode}"
IMAGE="${QBZD_BUILD_IMAGE:-qbzd-aarch64-build:ubuntu22.04}"
HOST_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The directory colima shares, and this checkout's path within it.
MOUNT_ROOT="$(dirname "$HOST_ROOT")"
REL="$(basename "$HOST_ROOT")"
BUILD_ONLY=0
[[ "${1:-}" == "--build-only" ]] && BUILD_ONLY=1

say() { printf '\n[qbzd-to-pi] %s\n' "$*"; }

docker info >/dev/null 2>&1 || { echo "docker is not reachable — is colima running?" >&2; exit 1; }

# Stamp the build so `qbzd --version`, /api/status and the Connect device name
# say where the binary came from. CI sets this from the tag; a local build says
# "local" and the commit, because a Pi running an untagged binary you cannot
# identify later is worse than no local build loop at all.
SHA="$(git -C "$HOST_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
DIRTY=""
git -C "$HOST_ROOT" diff --quiet 2>/dev/null || DIRTY="-dirty"
BASE="$(grep -m1 '^version = ' "$HOST_ROOT/crates/Cargo.toml" | sed 's/version = "\(.*\)"/\1/')"
export QBZD_BUILD_ID="${BASE}.local-${SHA}${DIRTY}"

say "building qbzd $QBZD_BUILD_ID (release, aarch64) in $IMAGE"
docker run --rm \
  -e QBZD_BUILD_ID \
  -v "$MOUNT_ROOT:/work" \
  -v qbzd-cargo-registry:/usr/local/cargo/registry \
  -v qbzd-target-aarch64:/target \
  -w "/work/$REL/crates" \
  "$IMAGE" \
  bash -lc '
    set -e
    # The mount trap: an unshared host path shows up as an empty directory.
    test -f Cargo.toml || { echo "no Cargo.toml at $PWD — colima is not sharing this checkout"; exit 1; }
    CARGO_TARGET_DIR=/target QBZD_BUILD_ID="$QBZD_BUILD_ID" cargo build --release -p qbzd
    mkdir -p "/work/'"$REL"'/dist"
    cp /target/release/qbzd "/work/'"$REL"'/dist/qbzd-aarch64-linux"
  '

BIN="$HOST_ROOT/dist/qbzd-aarch64-linux"
say "built $(ls -lh "$BIN" | awk '{print $5}') -> $BIN"
file "$BIN" | grep -q aarch64 || { echo "not an aarch64 binary — refusing to install" >&2; exit 1; }

if [[ $BUILD_ONLY == 1 ]]; then
  say "--build-only: stopping here"
  exit 0
fi

say "installing on $PI"
scp -q "$BIN" "$PI:/tmp/qbzd.new"
# moOde starts qbzd from renderer.php, NOT from systemd, so there is no unit to
# restart: stop the process (and the sudo that owns it) and relaunch it the way
# startQobuz() does. `pkill -f 'qbzd run'` is wrong here — the pattern matches
# the ssh command line running it and kills the shell mid-script.
ssh "$PI" "
  set -e
  pid=\$(pgrep -x qbzd | head -1) || true
  if [ -n \"\$pid\" ]; then
    ppid=\$(ps -o ppid= -p \$pid | tr -d ' ')
    sudo kill \$ppid \$pid 2>/dev/null || true
    sleep 3
    sudo kill -9 \$ppid \$pid 2>/dev/null || true
    sleep 1
  fi
  sudo install -m755 /tmp/qbzd.new /usr/local/bin/qbzd
  rm -f /tmp/qbzd.new
  sudo sh -c 'LC_ALL=C QBZD_HOOK=/var/local/www/commandw/qbzevent.sh setsid nohup qbzd run >> /var/log/moode_qbzd.log 2>&1 < /dev/null &'
"
# The daemon needs a moment before the control API answers.
for _ in $(seq 1 20); do
  v=$(ssh "$PI" "curl -s --max-time 2 http://127.0.0.1:8182/api/status" 2>/dev/null | tr ',' '\n' | sed -n 's/.*"version":"\([^"]*\)".*/\1/p') || true
  [[ -n "${v:-}" ]] && { say "running on $PI: qbzd $v"; exit 0; }
  sleep 1
done
echo "[qbzd-to-pi] installed, but the control API did not answer — check /var/log/moode_qbzd.log" >&2
exit 1
