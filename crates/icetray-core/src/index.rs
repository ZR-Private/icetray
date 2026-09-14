//! The per-file resident index (RFD 004, RFD 006): key chunks, fences, the
//! exact key range of each row group, and a lookup that touches no byte it
//! does not already hold.

/// What makes a data file unservable beyond the contract's refusals. The set
/// is closed; a fault outside it is a library error, not a file fault.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FileFault {
    /// The table's schema names a column the file does not have.
    #[error("column {0} is not in the file")]
    MissingColumn(String),
    /// A column's Parquet physical type is not the one its Iceberg type maps to.
    #[error("physical type {got} where {expected} was expected")]
    UnexpectedPhysicalType {
        /// The physical type the Iceberg type maps to.
        expected: &'static str,
        /// The physical type the file carries.
        got: String,
    },
    /// The file has no page index on a column that needs one.
    #[error("no page index")]
    MissingPageIndex,
    /// A row position lies outside every page of its row group.
    #[error("row {row} is outside every page of row group {row_group}")]
    PageOutOfRange {
        /// The row position that no page covers.
        row: i64,
        /// The row group it was looked for in.
        row_group: usize,
    },
    /// A bloom filter that does not decode.
    #[error("bloom filter of row group {row_group} does not decode")]
    CorruptBloomFilter {
        /// The row group whose filter is unreadable.
        row_group: usize,
    },
}
