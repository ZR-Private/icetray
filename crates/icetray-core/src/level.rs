//! Lookup levels (RFD 020): the lookup cost a table declares it accepts and
//! the cost its snapshot's layout achieves, stated in what a lookup costs so
//! a level means the same on every table.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::approx;

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
    /// hit, GETs on a miss, or larger key pages.
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

    #[test]
    fn test_meets_is_at_or_below_the_declared_cost() {
        assert!(Level::Optimal.meets(Level::Optimal));
        assert!(Level::Optimal.meets(Level::Permissive));
        assert!(Level::Balanced.meets(Level::Permissive));
        assert!(!Level::Balanced.meets(Level::Optimal));
        assert!(!Level::Permissive.meets(Level::Balanced));
    }
}
