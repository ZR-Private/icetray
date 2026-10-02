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
pub const OBJECT_GETS_TOTAL: &str = "icetray_object_gets_total";
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
/// Configuration reloads that read a changed file, by `result`: applied or
/// refused (RFD 018).
pub const CONFIG_RELOADS_TOTAL: &str = "icetray_config_reloads_total";

/// Lookup latency in seconds, by `table` and `result`.
pub const LOOKUP_SECONDS: &str = "icetray_lookup_seconds";
/// Object-store GET latency in seconds, by `kind`.
pub const OBJECT_GET_SECONDS: &str = "icetray_object_get_seconds";

/// The resident index's live bytes, by `table`: what a residency tier sheds,
/// which process memory does not show on every platform.
pub(crate) const INDEX_BYTES: &str = "icetray_index_bytes";
/// The process's resident set size.
pub const PROCESS_RSS_BYTES: &str = "icetray_process_rss_bytes";
/// Bytes the program holds in live allocations, as jemalloc counts them.
pub const ALLOCATOR_ALLOCATED_BYTES: &str = "icetray_allocator_allocated_bytes";
/// Bytes in jemalloc's pages that hold live allocations: at least
/// `allocated`, the excess being space those pages hold free.
pub const ALLOCATOR_ACTIVE_BYTES: &str = "icetray_allocator_active_bytes";
/// Bytes of jemalloc's own bookkeeping.
pub const ALLOCATOR_METADATA_BYTES: &str = "icetray_allocator_metadata_bytes";
/// Bytes jemalloc has resident: active pages, its metadata, and freed pages
/// it has not yet returned to the operating system.
pub const ALLOCATOR_RESIDENT_BYTES: &str = "icetray_allocator_resident_bytes";
/// Bytes of address space jemalloc keeps mapped for reuse after returning
/// its pages: virtual, not resident.
pub const ALLOCATOR_RETAINED_BYTES: &str = "icetray_allocator_retained_bytes";
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
/// 1 while the configuration file differs from the configuration the
/// server applied, 0 once they match (RFD 018).
pub const CONFIG_PENDING: &str = "icetray_config_pending";

/// The `call` label: the RPC, from the RPC enum's `Display`.
pub const LABEL_CALL: &str = "call";
/// The `kind` label: the read path, from `ReadKind`.
pub const LABEL_KIND: &str = "kind";
/// The `table` label: the table's configured name.
pub const LABEL_TABLE: &str = "table";
/// The `result` label: a lookup's outcome, from `LookupResult`, or a
/// reload's.
pub const LABEL_RESULT: &str = "result";
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
pub enum ReadKind {
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
