//! The serving layout's numbers as named constants (RFD 002, RFD 008). This
//! module is the one home for every constant another module or crate needs;
//! none redeclares them.

/// Rows per key page and per value page in the served layout. A lookup
/// decodes one key page and fetches one value page, so the row count sets
/// the CPU a lookup spends decoding keys; 1,000 rows is the value the layout
/// was measured at (RFD 002).
pub const PAGE_ROWS: i64 = 1_000;

/// The byte bound on a value page. A row limit alone leaves pages of many
/// megabytes when values run to kilobytes, and the one value page a cold hit
/// fetches is the bytes that hit transfers; 64 kibibytes is the bound the
/// layout was measured at (RFD 002).
pub const VALUE_PAGE_BYTES: u64 = 64 * 1024;

/// The lower end of the data file size target, 32 mebibytes. Smaller files
/// mean more files for the same bytes: more footer GETs at load and more
/// index bytes per key (RFD 002).
pub const TARGET_FILE_BYTES_MIN: u64 = 32 * 1024 * 1024;

/// The upper end of the data file size target, 64 mebibytes. First-byte
/// latency of a range GET rises with object size, and this is the largest
/// size that measured in the fast class (RFD 002).
pub const TARGET_FILE_BYTES_MAX: u64 = 64 * 1024 * 1024;

/// The longest key that is served; a longer key is a refusal at load. The
/// resident index and its fence array hold whole keys, so the cap bounds
/// what one key can cost there (RFD 008).
pub const MAX_KEY_BYTES: usize = 4_096;

/// The key length above which the doctor advises. Resident bytes per key
/// grow with key length, and the price is memory, which no penalty unit
/// measures, so the finding is advice rather than a cost (RFD 008).
pub const KEY_BYTES_ADVICE: usize = 128;
