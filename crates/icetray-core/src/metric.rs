//! The metric and label names of the deployment contract (RFD 012, blueprint
//! section 7). They are a public surface: a deployer's dashboards and alerts
//! name them, so a rename here is a breaking change. A metric name at a call
//! site is one of these constants and a label value is an enum's `Display`,
//! never a literal, so the set of series a process can emit is closed and
//! readable in one place.

use std::fmt;

/// gRPC calls received, by `call`.
pub const REQUESTS_TOTAL: &str = "icetray_requests_total";
/// Object-store GETs issued, by `kind`; a hedge is a GET and counts here too.
pub(crate) const OBJECT_GETS_TOTAL: &str = "icetray_object_gets_total";
/// Bytes requested across those GETs, by `kind`.
pub(crate) const OBJECT_BYTES_TOTAL: &str = "icetray_object_bytes_total";
/// Reads served from the local directory (RFD 017).
pub(crate) const LOCAL_READS_TOTAL: &str = "icetray_local_reads_total";
/// Bytes of those local reads.
pub(crate) const LOCAL_BYTES_TOTAL: &str = "icetray_local_bytes_total";
/// Second requests issued because a lookup GET was slow (RFD 013).
pub(crate) const HEDGES_TOTAL: &str = "icetray_hedges_total";
/// Value pages served from the page cache without a fetch.
pub const PAGE_CACHE_HITS_TOTAL: &str = "icetray_page_cache_hits_total";
/// Value-page fetches the page cache ran, after coalescing.
pub const PAGE_CACHE_FETCHES_TOTAL: &str = "icetray_page_cache_fetches_total";
/// Lookups answered, by `table` and `result`.
pub const LOOKUPS_TOTAL: &str = "icetray_lookups_total";
/// Refreshes that failed and left the previous generation serving, by
/// `table` and `reason`.
pub(crate) const REFRESH_ERRORS_TOTAL: &str = "icetray_refresh_errors_total";
/// Generations swapped in, by `table`.
pub(crate) const GENERATION_SWAPS_TOTAL: &str = "icetray_generation_swaps_total";

/// Lookup latency in seconds, by `table` and `result`.
pub const LOOKUP_SECONDS: &str = "icetray_lookup_seconds";
/// Object-store GET latency in seconds, by `kind`.
pub const OBJECT_GET_SECONDS: &str = "icetray_object_get_seconds";

/// The resident index's live bytes, by `table`: what a residency tier sheds,
/// which process memory does not show on every platform.
pub(crate) const INDEX_BYTES: &str = "icetray_index_bytes";
/// The process's resident set size.
pub const PROCESS_RSS_BYTES: &str = "icetray_process_rss_bytes";
/// Bytes the page cache holds.
pub(crate) const PAGE_CACHE_BYTES: &str = "icetray_page_cache_bytes";
/// Findings the doctor raised on the serving generation, by `table` and `code`.
pub(crate) const FINDINGS: &str = "icetray_findings";
/// Seconds from the served snapshot's timestamp to now, by `table`.
pub const GENERATION_AGE_SECONDS: &str = "icetray_generation_age_seconds";
/// The serving snapshot's sequence number, by `table`: one series per
/// table, which each swap overwrites.
pub const GENERATION_SEQUENCE_NUMBER: &str = "icetray_generation_sequence_number";
/// When the serving generation was swapped in, in seconds since the Unix
/// epoch, by `table`.
pub const GENERATION_SWAPPED_AT_SECONDS: &str = "icetray_generation_swapped_at_seconds";
/// 1 for the table's current state and 0 for the others, by `table` and `state`.
pub const TABLE_STATE: &str = "icetray_table_state";

/// The `call` label: the RPC, from the RPC enum's `Display`.
pub const LABEL_CALL: &str = "call";
/// The `kind` label: the read path, from `ReadKind`.
pub(crate) const LABEL_KIND: &str = "kind";
/// The `table` label: the table's configured name.
pub const LABEL_TABLE: &str = "table";
/// The `result` label: the lookup outcome, from `LookupResult`.
pub(crate) const LABEL_RESULT: &str = "result";
/// The `reason` label: why a refresh failed, from `ErrorKind`, which gives
/// the refusal's own name for a refusal.
pub(crate) const LABEL_REASON: &str = "reason";
/// The `code` label: the finding, from `FindingCode`.
pub(crate) const LABEL_CODE: &str = "code";
/// The `state` label: the table's lifecycle state, from `TableState`.
pub const LABEL_STATE: &str = "state";

/// Which path a GET serves; the `kind` label. Load reads (footers, key
/// chunks, metadata) are bulk work nobody waits on and are never hedged;
/// lookup reads (value pages, and key pages under the bloom tier) are what a
/// client waits on and are the only ones hedged (RFD 013). Display gives the
/// label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ReadKind {
    /// A read made while a generation is built.
    Load,
    /// A read made to answer a client's lookup.
    Lookup,
}

impl fmt::Display for ReadKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Load => "load",
            Self::Lookup => "lookup",
        };
        f.write_str(name)
    }
}

/// The three outcomes of a lookup as the `result` metric label sees them.
/// Display gives the `snake_case` label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LookupResult {
    /// The key was present and its value page was fetched.
    ColdHit,
    /// The key was present and its value page was already cached.
    WarmHit,
    /// The key was absent.
    Miss,
}

impl LookupResult {
    /// Every outcome, for the per-result series of a table.
    pub(crate) const ALL: [Self; 3] = [Self::ColdHit, Self::WarmHit, Self::Miss];
}

impl fmt::Display for LookupResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::ColdHit => "cold_hit",
            Self::WarmHit => "warm_hit",
            Self::Miss => "miss",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fmt_lookup_result_snake_case_names() {
        let cases = [
            (LookupResult::ColdHit, "cold_hit"),
            (LookupResult::WarmHit, "warm_hit"),
            (LookupResult::Miss, "miss"),
        ];
        for (result, name) in cases {
            assert_eq!(result.to_string(), name);
        }
    }

    #[test]
    fn test_fmt_read_kind_label_values() {
        let cases = [(ReadKind::Load, "load"), (ReadKind::Lookup, "lookup")];
        for (kind, name) in cases {
            assert_eq!(kind.to_string(), name);
        }
    }
}
