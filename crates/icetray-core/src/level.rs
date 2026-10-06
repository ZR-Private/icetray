//! Lookup levels (RFD 020): the lookup cost a table declares it accepts and
//! the cost its snapshot's layout achieves, stated in what a lookup costs so
//! a level means the same on every table.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::approx;
use crate::local::ResidentPages;

/// Below this many resident pages in a thousand of a `disk` table's chunk
/// files, the table falls to balanced: 950, where about one lookup in
/// twenty over uniform keys reads its key page from the device, which puts
/// the 95th-percentile lookup there (RFD 020).
pub const FALL_PER_MILLE: u64 = 950;

/// Above this many resident pages in a thousand, a table that fell for the
/// page cache recovers its layout's level: 990, above `FALL_PER_MILLE` so
/// that a residency hovering at the fall does not flap the level.
pub const RECOVER_PER_MILLE: u64 = 990;

/// Thousandths in the whole: what `FALL_PER_MILLE` and `RECOVER_PER_MILLE`
/// count resident pages against.
pub const PER_MILLE: u64 = 1_000;

/// A lookup cost. The derived order runs from cheapest to dearest, so the
/// level several caps set together is the greatest of them. Display and
/// serde give the lowercase name that appears in the file, the doctor's
/// output, and logs.
// The serde names and Display are the same strings;
// `test_serialize_level_matches_display` holds them together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    /// A hit costs one GET, a miss none; a lookup probes one file, decodes
    /// one key page of ordinary size, and reads its key chunks from memory
    /// or the page cache.
    Optimal,
    /// A lookup probes one file, with a fixed extra cost: extra GETs per
    /// hit, GETs on a miss, larger key pages, or device reads of key
    /// chunks.
    Balanced,
    /// Any snapshot icetray serves correctly, including one where a lookup
    /// probes more than one file.
    Permissive,
}

impl Level {
    /// The level's lowercase name, as the file spells it.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Optimal => "optimal",
            Self::Balanced => "balanced",
            Self::Permissive => "permissive",
        }
    }

    /// Whether a table at this level meets `declared`: it costs no more.
    pub fn meets(self, declared: Self) -> bool {
        self <= declared
    }

    /// The level as the level gauges read it: 0 for optimal, 1 for
    /// balanced, 2 for permissive (RFD 020).
    pub(crate) const fn ordinal(self) -> u8 {
        match self {
            Self::Optimal => 0,
            Self::Balanced => 1,
            Self::Permissive => 2,
        }
    }

    /// `ordinal` as the value a level gauge is set to.
    pub(crate) fn gauge(self) -> f64 {
        approx::gauge(u64::from(self.ordinal()))
    }
}

/// Whether a `disk` table's chunk files are short of the page cache (RFD
/// 020): a condition of the serving process, not of the snapshot, so it
/// caps the level the table achieves and `strict` never acts on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageCacheCondition {
    /// The last sample judged nothing: a chunk file's residency could not
    /// be observed, or no page was counted. Caps nothing.
    Unjudged,
    /// The last sample's residency, and whether the chunk files are short
    /// of the page cache.
    Judged {
        /// What the last sample counted.
        residency: ResidentPages,
        /// Whether the table is below optimal for the page cache.
        short: bool,
    },
}

impl PageCacheCondition {
    /// The condition after `sample`: unjudged for `None`, a sample that
    /// observed nothing, and for a sample that counted no page, which a
    /// `disk` generation with no chunk to sample gives. A table falls short
    /// below `FALL_PER_MILLE` resident pages in a thousand and recovers
    /// above `RECOVER_PER_MILLE`; between the two it keeps what it was.
    /// Unjudged keeps nothing, so a table judged, then unjudged, then
    /// judged again starts from not short, as one judged first does.
    /// Compared in integers, so a fraction exactly at a threshold neither
    /// falls nor recovers.
    pub(crate) fn next(self, sample: Option<ResidentPages>) -> Self {
        let Some(residency) = sample.filter(|sample| sample.pages > 0) else {
            return Self::Unjudged;
        };
        let resident = u128::from(residency.resident) * u128::from(PER_MILLE);
        let pages = u128::from(residency.pages);
        let short = if self.short() {
            resident <= pages * u128::from(RECOVER_PER_MILLE)
        } else {
            resident < pages * u128::from(FALL_PER_MILLE)
        };
        Self::Judged { residency, short }
    }

    /// Whether the chunk files are short of the page cache.
    fn short(self) -> bool {
        matches!(self, Self::Judged { short: true, .. })
    }

    /// The level the condition caps a table at: balanced while short,
    /// since a lookup then reads its key page from the device; optimal,
    /// which caps nothing, otherwise.
    pub(crate) fn cap(self) -> Level {
        if self.short() {
            Level::Balanced
        } else {
            Level::Optimal
        }
    }

    /// The resident fraction the last sample counted, while judged.
    pub(crate) fn fraction(self) -> Option<f64> {
        match self {
            Self::Judged { residency, .. } => Some(residency.fraction()),
            Self::Unjudged => None,
        }
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every level, cheapest first.
    const ALL: [Level; 3] = [Level::Optimal, Level::Balanced, Level::Permissive];

    #[test]
    fn test_fmt_level_lowercase_names() {
        let cases = [
            (Level::Optimal, "optimal"),
            (Level::Balanced, "balanced"),
            (Level::Permissive, "permissive"),
        ];
        for (level, name) in cases {
            assert_eq!(level.to_string(), name);
        }
    }

    #[test]
    fn test_serialize_level_matches_display() {
        for level in ALL {
            let json = serde_json::to_string(&level).unwrap();
            assert_eq!(json, format!("\"{level}\""));
            let back: Level = serde_json::from_str(&json).unwrap();
            assert_eq!(back, level);
        }
    }

    #[test]
    fn test_ordinal_counts_from_optimal() {
        let ordinals: Vec<u8> = ALL.into_iter().map(Level::ordinal).collect();
        assert_eq!(ordinals, [0, 1, 2]);
    }

    /// A sample of `resident` pages in a thousand.
    fn per_mille(resident: u64) -> ResidentPages {
        ResidentPages {
            pages: PER_MILLE,
            resident,
        }
    }

    /// Whether the condition after each of `samples` in turn, from
    /// unjudged, is short.
    fn shorts(samples: &[u64]) -> Vec<bool> {
        let mut condition = PageCacheCondition::Unjudged;
        samples
            .iter()
            .map(|&resident| {
                condition = condition.next(Some(per_mille(resident)));
                condition.short()
            })
            .collect()
    }

    #[test]
    fn test_next_falls_only_below_the_fall_and_recovers_only_above_the_recovery() {
        let between = between_thresholds();
        assert_eq!(
            shorts(&[
                PER_MILLE,
                FALL_PER_MILLE,
                between,
                FALL_PER_MILLE - 1,
                between,
                RECOVER_PER_MILLE,
                RECOVER_PER_MILLE + 1,
                between,
            ]),
            [false, false, false, true, true, true, false, false]
        );
    }

    #[test]
    fn test_next_without_a_counted_page_is_unjudged() {
        let short = PageCacheCondition::Unjudged.next(Some(per_mille(0)));
        assert!(short.short());
        assert_eq!(short.next(None), PageCacheCondition::Unjudged);
        assert_eq!(
            short.next(Some(ResidentPages::default())),
            PageCacheCondition::Unjudged
        );
        let again = PageCacheCondition::Unjudged.next(Some(per_mille(between_thresholds())));
        assert!(!again.short(), "a table judged anew starts from not short");
    }

    /// A resident count between the fall and the recovery.
    fn between_thresholds() -> u64 {
        u64::midpoint(FALL_PER_MILLE, RECOVER_PER_MILLE)
    }

    #[test]
    fn test_cap_is_balanced_only_while_short() {
        let short = PageCacheCondition::Unjudged.next(Some(per_mille(0)));
        assert_eq!(short.cap(), Level::Balanced);
        let whole = PageCacheCondition::Unjudged.next(Some(per_mille(PER_MILLE)));
        assert_eq!(whole.cap(), Level::Optimal);
        assert_eq!(PageCacheCondition::Unjudged.cap(), Level::Optimal);
        assert_eq!(whole.fraction().map(f64::to_bits), Some(1.0_f64.to_bits()));
        assert_eq!(PageCacheCondition::Unjudged.fraction(), None);
    }

    #[test]
    fn test_meets_is_at_or_below_the_declared_cost() {
        assert!(Level::Optimal.meets(Level::Optimal));
        assert!(Level::Optimal.meets(Level::Permissive));
        assert!(Level::Balanced.meets(Level::Permissive));
        assert!(!Level::Balanced.meets(Level::Optimal));
        assert!(!Level::Permissive.meets(Level::Balanced));
    }
}
