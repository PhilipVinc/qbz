# syntax=docker/dockerfile:1
#
# qbzd-aarch64.Dockerfile — the aarch64 `qbzd` build environment, as a container.
#
# ── Why this exists ──────────────────────────────────────────────────────────
# scripts/build-aarch64-qbzd.sh had two modes: NATIVE (on aarch64 Linux) and
# CROSS (x86-64 Linux + Docker + `cross`). Neither runs on an Apple-silicon
# Mac. This image adds a third: on an M-series host, `linux/arm64` containers
# execute NATIVELY (no QEMU), so this is not a cross-compile at all — it is the
# NATIVE build, in a box. That matters, because qbzd's dep graph resolves
# `aws-lc-sys` (rustls provider, cmake + C) plus `alsa-sys` and `jack-sys`;
# a macOS -> linux-gnu cross would have to defeat all three, and a native
# container defeats none of them.
#
# ── Why ubuntu 22.04 specifically ────────────────────────────────────────────
# glibc 2.35 — the exact floor that release.yml gates on with objdump. Building here produces a binary comparable to the
# released one, and 2.35 loads fine on the newer target (moOde 10 / Debian 13
# trixie carries glibc 2.41 — forward-compatible, never backward).
#
# Dep list is kept in sync with build-aarch64-qbzd.sh's DEPS array and the
# workflow's `system deps` step. The GUI stack is deliberately absent: qbzd is
# the slint-free column and links none of it.
FROM ubuntu:22.04

ARG RUST_TOOLCHAIN=stable
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential pkg-config cmake clang libclang-dev \
      libasound2-dev libjack-jackd2-dev libdbus-1-dev libssl-dev \
      curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# rustup rather than apt's rustc: matches rust-toolchain.toml (floating
# stable) and the workflow's dtolnay/rust-toolchain@stable.
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --no-modify-path --profile minimal \
                 --default-toolchain "${RUST_TOOLCHAIN}" \
 && rustc -vV && cargo -V

# /target is a named volume (see the script): keeping the target dir OFF the
# virtiofs bind mount is what keeps this fast, and keeps container artifacts
# from colliding with the host's macOS-triple ones in target/.
ENV CARGO_TARGET_DIR=/target \
    CARGO_INCREMENTAL=0

WORKDIR /src
