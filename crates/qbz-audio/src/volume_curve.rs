//! How a volume percentage becomes an amplitude multiplier.
//!
//! Scaling samples by the fraction itself — 30 % of the slider means 0.30 ×
//! amplitude — is linear in AMPLITUDE, and amplitude is not what ears hear.
//! 0.30 is only −10.5 dB, so a linear fader spends its whole top half doing
//! almost nothing and crams every useful listening level into the bottom tenth
//! of its travel.
//!
//! Every serious player therefore bends the curve. MPD's software mixer maps
//! percent to amplitude as `(e^(v/25) − 1) / (e^4 − 1)`, and librespot ships
//! `--volume-ctrl log|cubic|linear|fixed` with linear pointedly not the default.
//! Matching MPD matters on a host like moOde, where the same box plays a local
//! library through MPD and Qobuz through this daemon: one number on one slider
//! should mean one loudness, whatever is playing. Measured on a real player, 30
//! on moOde's own knob and 5 here were the same volume — 1.3 dB apart — which is
//! exactly the gap between the two mappings.
//!
//! The curve applies ONLY to software volume. With `alsa_hardware_volume` the
//! card's mixer owns the mapping, and ALSA mixers are already dB-scaled.

use std::sync::atomic::{AtomicU8, Ordering};

/// `e^4`, the normalisation constant in MPD's mapping.
const E4: f32 = 54.598_15;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeCurve {
    /// MPD's exponential mapping. One slider position means the same loudness
    /// here as it does for everything else on a moOde box.
    Perceptual = 0,
    /// The fraction scales amplitude directly. Kept because it is what this
    /// daemon did before, and because a host that applies its own curve
    /// upstream of us wants no second one.
    Linear = 1,
}

impl VolumeCurve {
    pub fn from_key(key: &str) -> Option<Self> {
        match key.trim().to_ascii_lowercase().as_str() {
            "perceptual" | "mpd" | "exponential" => Some(Self::Perceptual),
            "linear" | "amplitude" => Some(Self::Linear),
            _ => None,
        }
    }

    pub fn as_key(self) -> &'static str {
        match self {
            Self::Perceptual => "perceptual",
            Self::Linear => "linear",
        }
    }

    /// The amplitude multiplier for `fraction` (0.0–1.0) under this curve.
    pub fn gain(self, fraction: f32) -> f32 {
        let fraction = fraction.clamp(0.0, 1.0);
        match self {
            Self::Linear => fraction,
            Self::Perceptual => {
                // Endpoints exactly, so mute is silent and full is untouched
                // (bit-perfect playback at 100 % must not be scaled at all).
                if fraction <= 0.0 {
                    return 0.0;
                }
                if fraction >= 1.0 {
                    return 1.0;
                }
                ((fraction * 100.0 / 25.0).exp() - 1.0) / (E4 - 1.0)
            }
        }
    }
}

impl Default for VolumeCurve {
    fn default() -> Self {
        Self::Perceptual
    }
}

/// Process-wide curve, set once from the audio settings at player start. A
/// global rather than a field because the mapping belongs to the host's
/// configuration, not to any one stream, and every engine variant applies it at
/// the same point.
static CURVE: AtomicU8 = AtomicU8::new(VolumeCurve::Perceptual as u8);

pub fn set_volume_curve(curve: VolumeCurve) {
    CURVE.store(curve as u8, Ordering::Relaxed);
}

pub fn volume_curve() -> VolumeCurve {
    match CURVE.load(Ordering::Relaxed) {
        1 => VolumeCurve::Linear,
        _ => VolumeCurve::Perceptual,
    }
}

/// The amplitude multiplier for `fraction` under the configured curve.
pub fn gain_for(fraction: f32) -> f32 {
    volume_curve().gain(fraction)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db(gain: f32) -> f32 {
        20.0 * gain.log10()
    }

    /// The mapping is MPD's, value for value — that is the whole point, so it is
    /// pinned rather than described. Reference figures computed from
    /// `(e^(v/25) − 1) / (e^4 − 1)`.
    #[test]
    fn perceptual_matches_mpds_software_mixer() {
        let curve = VolumeCurve::Perceptual;
        for (percent, expected) in [
            (10.0, 0.009_18),
            (20.0, 0.022_87),
            (30.0, 0.043_29),
            (50.0, 0.119_20),
            (70.0, 0.288_16),
        ] {
            let got = curve.gain(percent / 100.0);
            assert!(
                (got - expected).abs() < 1e-4,
                "{percent}% -> {got}, expected {expected}"
            );
        }
    }

    /// Why the two scales felt so different on the same box: moOde's 30 and this
    /// daemon's old linear 5 are the same loudness, within a decibel and a half.
    #[test]
    fn thirty_perceptual_lands_where_five_linear_did() {
        let perceptual = db(VolumeCurve::Perceptual.gain(0.30));
        let linear = db(VolumeCurve::Linear.gain(0.05));
        assert!(
            (perceptual - linear).abs() < 1.5,
            "perceptual 30% = {perceptual} dB, linear 5% = {linear} dB"
        );
    }

    /// Full scale must not be scaled at all — anything else quietly ends
    /// bit-perfect playback — and mute must be silent.
    #[test]
    fn endpoints_are_exact_on_both_curves() {
        for curve in [VolumeCurve::Perceptual, VolumeCurve::Linear] {
            assert_eq!(curve.gain(1.0), 1.0, "{curve:?} at full scale");
            assert_eq!(curve.gain(0.0), 0.0, "{curve:?} at mute");
            assert_eq!(curve.gain(1.5), 1.0, "{curve:?} clamps above full scale");
            assert_eq!(curve.gain(-0.5), 0.0, "{curve:?} clamps below mute");
        }
    }

    #[test]
    fn perceptual_is_monotonic() {
        let mut previous = 0.0;
        for step in 1..=100 {
            let gain = VolumeCurve::Perceptual.gain(step as f32 / 100.0);
            assert!(gain > previous, "step {step}: {gain} <= {previous}");
            previous = gain;
        }
    }

    #[test]
    fn keys_round_trip_and_reject_nonsense() {
        assert_eq!(
            VolumeCurve::from_key("perceptual"),
            Some(VolumeCurve::Perceptual)
        );
        assert_eq!(VolumeCurve::from_key("MPD"), Some(VolumeCurve::Perceptual));
        assert_eq!(VolumeCurve::from_key(" linear "), Some(VolumeCurve::Linear));
        assert_eq!(VolumeCurve::from_key("loud"), None);
        assert_eq!(VolumeCurve::Perceptual.as_key(), "perceptual");
        assert_eq!(VolumeCurve::Linear.as_key(), "linear");
    }
}
