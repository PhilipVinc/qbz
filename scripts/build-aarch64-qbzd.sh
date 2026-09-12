#!/usr/bin/env bash
#
# build-aarch64-qbzd.sh — build the Linux aarch64 (ARM64) `qbzd` daemon binary,
# e.g. for a Raspberry Pi 4 + HiFiBerry/USB DAC streamer.
#
# ── Why this is cheap ────────────────────────────────────────────────────────
# `qbzd` is slint-free: no UI crate, no fonts, no GPU libs. (Upstream's desktop
# `qbz` binary generates ONE ~1.6M-line `qbz_ui` module needing ~30 GB for a
# single rustc; this fork does not carry it.) qbzd builds in minutes and fits on
# modest hardware, so:
#   • a 4 GB Pi CAN build it natively;
#   • the container caps here are small — no 48 GB swap headroom needed.
#
# Three modes, auto-selected by host OS+arch (override: QBZD_BUILD_MODE):
#
#   1. NATIVE    (on aarch64 Linux: the Pi itself, an ARM VM/runner)
#   2. CROSS     (on x86-64 Linux with Docker, via `cross`; Cross.toml
#                supplies the arm64 dev libs inside the image)
#   3. CONTAINER (on macOS: an arm64 Linux container, which on Apple silicon
#                runs natively — see packaging/docker/qbzd-aarch64.Dockerfile)
#
# Output: dist/qbzd-aarch64-linux (an aarch64 ELF — verify with `file`).
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

cd "$(dirname "$0")/.."          # repo root (qbz-nix)
REPO="$(pwd)"
TARGET="aarch64-unknown-linux-gnu"
OUT="$REPO/dist/qbzd-aarch64-linux"

# Native deps for the DAEMON only. Audio (ALSA/JACK; PipeWire is reached through
# its ALSA shim), D-Bus for MPRIS, TLS, and the usual -sys toolchain. The GUI
# stack the desktop needs (fontconfig, freetype, xkbcommon, wayland, xcb, GL,
# EGL) is deliberately absent — qbzd links none of it.
DEPS=(
  build-essential pkg-config cmake clang libclang-dev
  libasound2-dev libjack-jackd2-dev
  libdbus-1-dev libssl-dev
)

# Mode selection. Keyed on the OS *first*: `uname -m` alone reports "arm64" on
# an Apple-silicon Mac, which used to fall into the NATIVE branch and quietly
# build a macOS binary. Override with QBZD_BUILD_MODE=native|cross|container.
os="$(uname -s)"
arch="$(uname -m)"
mode="${QBZD_BUILD_MODE:-auto}"

if [ "$mode" = auto ]; then
  case "$os" in
    Darwin) mode=container ;;
    Linux)
      case "$arch" in
        aarch64 | arm64) mode=native ;;
        x86_64 | amd64)  mode=cross ;;
        *) echo "[qbzd-aarch64] ERROR: unsupported build host arch: $arch" >&2; exit 1 ;;
      esac
      ;;
    *) echo "[qbzd-aarch64] ERROR: unsupported build host OS: $os" >&2; exit 1 ;;
  esac
fi

case "$mode" in
  native)
    echo "[qbzd-aarch64] NATIVE build on $arch"
    if command -v apt-get >/dev/null; then
      sudo apt-get update
      sudo apt-get install -y --no-install-recommends "${DEPS[@]}"
    else
      echo "[qbzd-aarch64] non-apt distro: install the equivalents of: ${DEPS[*]}" >&2
    fi
    cargo build --release -p qbzd
    install -Dm755 "target/release/qbzd" "$OUT"
    ;;
  cross)
    echo "[qbzd-aarch64] CROSS-compile from $arch via cross (Docker)"
    if ! docker info >/dev/null 2>&1; then
      echo "[qbzd-aarch64] ERROR: Docker is not running. cross needs it." >&2
      exit 1
    fi
    command -v cross >/dev/null || cargo install cross --locked
    # Modest caps: the heaviest qbzd rustc is a fraction of the desktop's, so
    # this can be capped tightly and still never OOM. The point of the cap is
    # the same as the desktop script's — a runaway gets killed instead of
    # swap-thrashing this box (30 GB, no hibernation) into a hard freeze.
    export CROSS_CONTAINER_OPTS="${CROSS_CONTAINER_OPTS:---memory=8g --memory-swap=12g}"
    # Cross.toml injects the arm64 dev libs into the image.
    cross build --release --target "$TARGET" -p qbzd
    install -Dm755 "target/$TARGET/release/qbzd" "$OUT"
    ;;
  container)
    # NATIVE-IN-A-BOX. On an Apple-silicon host, linux/arm64 containers run
    # natively (no QEMU), so this is the aarch64 native build — not a
    # macOS -> linux-gnu cross. That distinction is the whole point: qbzd
    # pulls aws-lc-sys (cmake + C), alsa-sys and jack-sys, each of which a
    # real cross would have to fight. See packaging/docker/qbzd-aarch64.Dockerfile.
    echo "[qbzd-aarch64] CONTAINER build (native linux/arm64) on $os/$arch"
    if ! docker info >/dev/null 2>&1; then
      echo "[qbzd-aarch64] ERROR: no container runtime. Start one, e.g.:" >&2
      echo "  colima start --vm-type vz --mount-type virtiofs --cpu 6 --memory 8 --mount \"$REPO:w\"" >&2
      exit 1
    fi
    # QBZD_BUILD_ID stamps the version the binary self-reports (`qbzd version`,
    # /api/status, the Connect device softwareVersion) — the same knob
    # release.yml sets. Without it a local build claims the bare Cargo version
    # and is indistinguishable on the Pi from a released build, which breaks
    # A/B-ing. Default marks it local + the sha.
    if [ -z "${QBZD_BUILD_ID:-}" ]; then
      _ver="$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')"
      _sha="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
      git diff --quiet 2>/dev/null || _sha="$_sha-dirty"
      QBZD_BUILD_ID="$_ver.local.$_sha"
    fi
    echo "[qbzd-aarch64] QBZD_BUILD_ID=$QBZD_BUILD_ID"

    IMAGE="${QBZD_IMAGE:-qbzd-aarch64-build:ubuntu22.04}"
    DOCKERFILE="$REPO/packaging/docker/qbzd-aarch64.Dockerfile"
    # Named volumes, NOT bind mounts: the target dir and the crates.io registry
    # stay on the VM's own disk, which is what keeps rebuilds fast (virtiofs is
    # fine for reading source, poor for a million small target/ writes).
    TARGET_VOL="${QBZD_TARGET_VOLUME:-qbzd-aarch64-target}"
    REGISTRY_VOL="${QBZD_REGISTRY_VOLUME:-qbzd-aarch64-registry}"

    docker build --platform linux/arm64 -t "$IMAGE" -f "$DOCKERFILE" \
      "$REPO/packaging/docker"

    mkdir -p "$REPO/dist"
    # Source is mounted READ-ONLY (with --locked) so a container build can
    # never mutate the host tree or the committed lockfile.
    docker run --rm --platform linux/arm64 \
      -v "$REPO:/src:ro" \
      -v "$TARGET_VOL:/target" \
      -v "$REGISTRY_VOL:/usr/local/cargo/registry" \
      -v "$REPO/dist:/out" \
      -w /src \
      -e QBZD_BUILD_ID="$QBZD_BUILD_ID" \
      "$IMAGE" \
      bash -euo pipefail -c '
        cargo build --release --locked -p qbzd

        # Same gates release.yml runs, kept here so a local build fails for
        # the same reasons CI would.
        hits=$(cargo tree -p qbzd -e normal | grep -E "\bslint[a-z-]* v" || true)
        [ -z "$hits" ] || { echo "ERROR: qbzd graph resolves Slint:"; echo "$hits"; exit 1; }

        floor=$(objdump -T /target/release/qbzd \
                | grep -oE "GLIBC_[0-9]+\.[0-9]+" | sort -Vu | tail -1)
        echo "[qbzd-aarch64] glibc floor: $floor"
        highest=$(printf "%s\n" "$floor" "GLIBC_2.35" | sort -V | tail -1)
        [ "$highest" = "GLIBC_2.35" ] || { echo "ERROR: floor $floor exceeds 2.35"; exit 1; }

        install -Dm755 /target/release/qbzd /out/qbzd-aarch64-linux
      '
    ;;
  *)
    echo "[qbzd-aarch64] ERROR: unknown QBZD_BUILD_MODE: $mode" >&2
    exit 1
    ;;
esac

echo "[qbzd-aarch64] done -> $OUT"
file "$OUT" || true
