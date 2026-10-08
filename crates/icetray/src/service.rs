//! The gRPC service over the generated contract (RFD 010): `Get`,
//! `MultiGet`, and `TableInfo` over the tables `open` built, the wire key
//! read under the table's key type, and core's errors mapped to status codes
//! by variant. Every call is counted under `requests_total` by the code it
//! answered around the service, not in it (`counted::Counted`). `Get` and
//! `MultiGet` run under the server's deadline for the method (RFD 013); a
//! client's own deadline is tonic's, which drops the call at it. Together
//! they hold at most `server.max_in_flight` slots, and a call that finds
//! none is refused at once. A `MultiGet` asks for at most
//! `server.max_multi_get_values` values, its keys times the columns it
//! names, whatever fields each names (RFD 022, RFD 024).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::Guard;
use bytes::Bytes;
use icetray_core::config::ServerConfig;
use icetray_core::key::UUID_BYTES;
use icetray_core::{
    CacheError, ColumnRead, Error, Key, KeyType, Opened, Row, Table, TableConfig, TableLayout,
    Tables, ValueFormat, ValueType, approx,
};
use icetray_proto::v1;
use icetray_proto::v1::icetray_server::Icetray;
use icetray_proto::v1::{
    ColumnValue, GetRequest, GetResponse, Hit, MultiGetRequest, MultiGetResponse, NullValue,
    TableInfoRequest, TableInfoResponse, column_value,
};
use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};
use tonic::{Request, Response, Status};

use crate::error::{CallFault, WireKeyFault};

/// The in-flight slots `Get` and `MultiGet` hold together, at most
/// `server.max_in_flight`: what admits a call, and what
/// `requests_in_flight` reads, so the gauge counts the slots the bound
/// holds and cannot drift from it. Clone shares the slots.
#[derive(Clone, Debug)]
pub struct Slots {
    /// Nothing closes it.
    semaphore: Arc<Semaphore>,
    /// The slots `semaphore` was built with.
    limit: usize,
}

impl Slots {
    /// `max_in_flight` slots, held to tokio's `Semaphore::MAX_PERMITS`,
    /// which is far above any count of calls one process can hold open.
    pub fn new(max_in_flight: usize) -> Self {
        let limit = max_in_flight.min(Semaphore::MAX_PERMITS);
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            limit,
        }
    }

    /// A slot for one call, held until the call ends, its deadline
    /// included; with none free the call is refused at once, never queued.
    fn admit(&self) -> Result<SemaphorePermit<'_>, CallFault> {
        self.semaphore.try_acquire().map_err(|error| match error {
            // `Closed` is unreachable, since nothing closes the
            // semaphore; were it closed, no call could be admitted.
            TryAcquireError::NoPermits | TryAcquireError::Closed => {
                CallFault::Overloaded { limit: self.limit }
            }
        })
    }

    /// The slots there are: `server.max_in_flight` as the bound enforces it.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// The slots held now, one per call admitted and not yet ended.
    pub fn admitted(&self) -> usize {
        self.limit - self.semaphore.available_permits()
    }
}

/// The served tables and the bounds each call runs under. Clone: tonic
/// hands one clone to every connection, and they share the tables and the
/// in-flight slots.
#[derive(Clone)]
pub struct Service {
    /// The tables a call looks up by name, as the last reload left them
    /// (RFD 018).
    opened: Arc<Opened>,
    get_deadline: Duration,
    multi_get_deadline: Duration,
    /// `Get` and `MultiGet` together.
    slots: Slots,
    /// The most values one `MultiGet` may ask for.
    max_multi_get_values: usize,
}

impl Service {
    /// The service over the tables `opened` serves, by configured name,
    /// under `server`'s request bounds.
    pub fn new(opened: &Arc<Opened>, server: &ServerConfig) -> Self {
        Self {
            opened: Arc::clone(opened),
            get_deadline: server.get_deadline,
            multi_get_deadline: server.multi_get_deadline,
            slots: Slots::new(server.max_in_flight),
            max_multi_get_values: server.max_multi_get_values,
        }
    }

    /// The in-flight slots the service admits calls through.
    pub fn slots(&self) -> &Slots {
        &self.slots
    }

    /// The tables served as they stand, borrowed for one call
    /// (`Opened::serving_tables`): a call holds them to its end, so a
    /// reload that drops its table meanwhile does not cut it short. With
    /// the lookup's own generation guard, a call waiting on a read holds
    /// two of its thread's fast slots.
    fn serving(&self) -> Guard<Arc<Tables>> {
        self.opened.serving_tables()
    }
}

/// The table `tables` serves under `name`; a name not served is
/// `NOT_FOUND`.
fn table<'a>(tables: &'a Tables, name: &str) -> Result<&'a Arc<Table>, Status> {
    tables
        .get(name)
        .ok_or_else(|| Status::not_found(format!("table {name} is not served")))
}

/// The variant a wire `Key` carries (RFD 010), the closed set the `oneof`
/// declares, for the fault naming one the table's key type does not take.
/// Display gives the field's name in the contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireVariant {
    /// `Key.long`.
    Long,
    /// `Key.str`.
    Str,
    /// `Key.bin`.
    Bin,
}

impl WireVariant {
    /// The variant `key` carries.
    pub fn of(key: &v1::key::Key) -> Self {
        match key {
            v1::key::Key::Long(_) => Self::Long,
            v1::key::Key::Str(_) => Self::Str,
            v1::key::Key::Bin(_) => Self::Bin,
        }
    }
}

impl fmt::Display for WireVariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Long => "long",
            Self::Str => "str",
            Self::Bin => "bin",
        };
        f.write_str(name)
    }
}

/// A key from the wire under the table's key type (RFD 010): the variant
/// must match the type, and `bin` on a uuid table must be `UUID_BYTES` long.
/// A missing key or a `Key` with no variant is `Missing`; every other
/// pairing is `VariantMismatch`.
pub fn key_from_wire(key: Option<v1::Key>, key_type: KeyType) -> Result<Key, WireKeyFault> {
    let Some(variant) = key.and_then(|key| key.key) else {
        return Err(WireKeyFault::Missing);
    };
    match (variant, key_type) {
        (v1::key::Key::Long(value), KeyType::Long) => Ok(Key::Long(value)),
        (v1::key::Key::Str(text), KeyType::String) => Ok(Key::Str(text)),
        (v1::key::Key::Bin(bytes), KeyType::Binary) => Ok(Key::Bytes(bytes)),
        (v1::key::Key::Bin(bytes), KeyType::Uuid) => <[u8; UUID_BYTES]>::try_from(bytes.as_slice())
            .map(Key::Uuid)
            .map_err(|_| WireKeyFault::UuidLength { bytes: bytes.len() }),
        (variant @ v1::key::Key::Long(_), KeyType::String | KeyType::Binary | KeyType::Uuid)
        | (variant @ v1::key::Key::Str(_), KeyType::Long | KeyType::Binary | KeyType::Uuid)
        | (variant @ v1::key::Key::Bin(_), KeyType::Long | KeyType::String) => {
            Err(WireKeyFault::VariantMismatch {
                expected: key_type,
                got: WireVariant::of(&variant),
            })
        }
    }
}

/// A wire key's fault as a status: every fault is the caller's, so
/// `INVALID_ARGUMENT`, with the fault's `Display` as the message.
impl From<WireKeyFault> for Status {
    fn from(fault: WireKeyFault) -> Self {
        Self::invalid_argument(fault.to_string())
    }
}

/// A call's own bound as a status: each fault's code, with the fault's
/// `Display` as the message.
impl From<CallFault> for Status {
    fn from(fault: CallFault) -> Self {
        let message = fault.to_string();
        match fault {
            CallFault::DeadlineExceeded { .. } => Self::deadline_exceeded(message),
            CallFault::Overloaded { .. } => Self::resource_exhausted(message),
            CallFault::TooManyValues { .. } => Self::invalid_argument(message),
        }
    }
}

/// The values `request` asks for, its keys times the columns it names, a
/// presence-only call counting one per key (RFD 022), held to `limit`:
/// `CallFault::TooManyValues` over it, before any lookup or slot.
fn check_values(request: &MultiGetRequest, limit: usize) -> Result<(), CallFault> {
    let keys = request.keys.len();
    let columns = request.columns.len();
    if keys.saturating_mul(columns.max(1)) > limit {
        return Err(CallFault::TooManyValues {
            keys,
            columns,
            limit,
        });
    }
    Ok(())
}

/// Runs `work` under the server's deadline `after`; past it, `work` is
/// dropped and the call is `DEADLINE_EXCEEDED`. A client's `grpc-timeout`
/// is not read here: tonic's server layer drops the call at it, and the
/// client's library reports that expiry as its own.
async fn within<T>(
    after: Duration,
    work: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    tokio::time::timeout(after, work)
        .await
        .map_err(|_elapsed| CallFault::DeadlineExceeded { after })?
}

/// Core's error as a status (RFD 010): a key of the wrong type, and a
/// column the table does not serve, named twice (RFD 022), or read with a
/// field list its format does not take (RFD 024), is `INVALID_ARGUMENT`; a
/// value a field list trims that is not the protobuf its column declares is
/// `FAILED_PRECONDITION`, which a retry meets again (RFD 024); a page
/// fetch that ran past `server.get_deadline` is
/// `DEADLINE_EXCEEDED`, the code the call's own deadline answers: a `Get`'s
/// deadline and its fetch's pass moments apart, and the call answers one
/// code whichever passes first; a read the caller can
/// retry (no generation yet, an object-store, cache, or local-directory
/// failure, a catalog call that failed, a catalog or load that did not
/// answer in time, a load's key
/// verification or a batch's lookup task canceled as the runtime shut
/// down) is
/// `UNAVAILABLE`; every other failure is `INTERNAL`. The message is the
/// error's `Display`.
pub fn status_from(error: &Error) -> Status {
    let message = error.to_string();
    match error {
        Error::KeyType { .. } | Error::ColumnRequest { .. } => Status::invalid_argument(message),
        Error::MalformedValue { .. } => Status::failed_precondition(message),
        Error::Cache(CacheError::FetchDeadline { .. }) => Status::deadline_exceeded(message),
        Error::NotReady { .. }
        | Error::ObjectStore(_)
        | Error::Cache(
            CacheError::Fetch(_) | CacheError::Backend(_) | CacheError::DescriptorLimit { .. },
        )
        | Error::Local { .. }
        | Error::CatalogTimeout { .. }
        | Error::Catalog { .. }
        | Error::LoadDeadline { .. }
        | Error::RefreshDeadline { .. }
        | Error::VerifyCanceled { .. }
        | Error::LookupCanceled { .. } => Status::unavailable(message),
        Error::Iceberg(_)
        | Error::Parquet(_)
        | Error::ObjectStorePath(_)
        | Error::Json(_)
        | Error::Config(_)
        | Error::Location { .. }
        | Error::Refused { .. }
        | Error::Table { .. }
        | Error::File { .. } => Status::internal(message),
    }
}

/// A lookup's outcome on the wire: the presence and, for a present key, one
/// value per column named, in order, each its bytes or `null`; none for an
/// absent key. A present value's bytes are the lookup's own, moved into the
/// message, not copied.
pub fn row_wire(row: Row) -> (v1::Presence, Vec<ColumnValue>) {
    match row {
        Row::Present(values) => (
            v1::Presence::Present,
            values.into_iter().map(column_value_wire).collect(),
        ),
        Row::Absent => (v1::Presence::Absent, Vec::new()),
    }
}

/// One column's value on the wire: its bytes, or `null` for a null, which
/// is not a value of zero length.
fn column_value_wire(value: Option<Bytes>) -> ColumnValue {
    let kind = match value {
        Some(bytes) => column_value::Kind::Value(bytes),
        None => column_value::Kind::NullValue(NullValue {}),
    };
    ColumnValue { kind: Some(kind) }
}

/// The table's value columns on the wire, in the layout's order, which is
/// by name, each with the format `entry`, the table's entry as it stands,
/// declares it in.
fn value_columns_wire(layout: &TableLayout, entry: &TableConfig) -> Vec<v1::ValueColumn> {
    let formats = entry.formats();
    layout
        .value_columns
        .iter()
        .map(|column| {
            let value_type = match column.value_type {
                ValueType::Binary => v1::ValueType::Binary,
                ValueType::Fixed { .. } => v1::ValueType::Fixed,
                ValueType::String => v1::ValueType::String,
            };
            let width = column.value_type.width().map_or(0, |width| {
                u32::try_from(width).expect(
                    "a layout admits a fixed column only as wide as a Parquet type length, an i32",
                )
            });
            let format = match formats(&column.name) {
                ValueFormat::Opaque => v1::ValueFormat::Opaque,
                ValueFormat::Protobuf => v1::ValueFormat::Protobuf,
            };
            v1::ValueColumn {
                name: column.name.clone(),
                value_type: i32::from(value_type),
                width,
                format: i32::from(format),
            }
        })
        .collect()
}

/// The column reads a request carries, as the core takes them.
fn reads(columns: &[v1::ColumnRead]) -> Vec<ColumnRead<'_>> {
    columns
        .iter()
        .map(|read| ColumnRead {
            name: &read.name,
            fields: &read.fields,
        })
        .collect()
}

/// The table's key type on the wire.
fn key_type_wire(key_type: KeyType) -> v1::KeyType {
    match key_type {
        KeyType::Long => v1::KeyType::Long,
        KeyType::String => v1::KeyType::String,
        KeyType::Binary => v1::KeyType::Binary,
        KeyType::Uuid => v1::KeyType::Uuid,
    }
}

impl Service {
    /// `Get` inside its deadline.
    async fn lookup(&self, request: GetRequest) -> Result<Response<GetResponse>, Status> {
        let tables = self.serving();
        let table = table(&tables, &request.table)?;
        let key_type = table.key_type().map_err(|error| status_from(&error))?;
        let key = key_from_wire(request.key, key_type)?;
        let lookup = table
            .get(&key, &reads(&request.columns))
            .await
            .map_err(|error| status_from(&error))?;
        let (presence, values) = row_wire(lookup.row);
        Ok(Response::new(GetResponse {
            presence: i32::from(presence),
            values,
            snapshot_id: lookup.snapshot_id.0,
        }))
    }

    /// `MultiGet` inside its deadline.
    async fn lookup_batch(
        &self,
        request: MultiGetRequest,
    ) -> Result<Response<MultiGetResponse>, Status> {
        let tables = self.serving();
        let table = table(&tables, &request.table)?;
        let key_type = table.key_type().map_err(|error| status_from(&error))?;
        let keys = request
            .keys
            .into_iter()
            .map(|key| key_from_wire(Some(key), key_type))
            .collect::<Result<Vec<Key>, WireKeyFault>>()?;
        let lookup = table
            .multi_get(&keys, &reads(&request.columns))
            .await
            .map_err(|error| status_from(&error))?;
        let results = lookup
            .rows
            .into_iter()
            .map(|row| {
                let (presence, values) = row_wire(row);
                Hit {
                    presence: i32::from(presence),
                    values,
                }
            })
            .collect();
        Ok(Response::new(MultiGetResponse {
            results,
            snapshot_id: lookup.snapshot_id.0,
        }))
    }

    /// `Get`, admitted and inside its deadline.
    async fn answer_get(&self, request: GetRequest) -> Result<Response<GetResponse>, Status> {
        let _slot = self.slots.admit()?;
        within(self.get_deadline, self.lookup(request)).await
    }

    /// `MultiGet`, held to `max_multi_get_values` (`check_values`),
    /// admitted, and inside its deadline.
    async fn answer_multi_get(
        &self,
        request: MultiGetRequest,
    ) -> Result<Response<MultiGetResponse>, Status> {
        check_values(&request, self.max_multi_get_values)?;
        let _slot = self.slots.admit()?;
        within(self.multi_get_deadline, self.lookup_batch(request)).await
    }

    /// `TableInfo`: the serving generation's description.
    fn answer_table_info(
        &self,
        request: TableInfoRequest,
    ) -> Result<Response<TableInfoResponse>, Status> {
        let tables = self.serving();
        let table = table(&tables, &request.table)?;
        let generation = table.serving().map_err(|error| status_from(&error))?;
        Ok(Response::new(TableInfoResponse {
            table: request.table,
            snapshot_id: generation.snapshot_id.0,
            buckets: generation
                .layout
                .bucketing
                .map_or(0, |bucketing| bucketing.count),
            data_files: approx::count(generation.files.len()),
            records: generation.records,
            key_type: i32::from(key_type_wire(generation.layout.key_type)),
            index_bytes: approx::count(generation.index_bytes),
            value_columns: value_columns_wire(&generation.layout, &table.config()),
        }))
    }
}

#[tonic::async_trait]
impl Icetray for Service {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        self.answer_get(request.into_inner()).await
    }

    async fn multi_get(
        &self,
        request: Request<MultiGetRequest>,
    ) -> Result<Response<MultiGetResponse>, Status> {
        self.answer_multi_get(request.into_inner()).await
    }

    async fn table_info(
        &self,
        request: Request<TableInfoRequest>,
    ) -> Result<Response<TableInfoResponse>, Status> {
        self.answer_table_info(request.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use bytes::Bytes;
    use icetray_core::config::{ConfigError, ConfigViolation, StorageConfig};
    use icetray_core::{
        ColumnRequestFault, FileFault, LocalFault, Malformed, RefusalReason, SourceConfig, Storage,
        TableFault, ValueColumnConfig,
    };
    use tonic::Code;

    use super::*;

    /// The table the sample errors name.
    const TABLE: &str = "kv";

    /// A `bin` key one byte short of a uuid.
    const SHORT_BIN_LEN: usize = UUID_BYTES - 1;

    /// The value of the sample long key.
    const LONG_VALUE: i64 = 42;

    /// The value of the sample string key.
    const STR_VALUE: &str = "user:42";

    fn wire(variant: v1::key::Key) -> v1::Key {
        v1::Key { key: Some(variant) }
    }

    #[test]
    fn test_key_from_wire_missing_key_or_variant_is_missing_for_every_type() {
        for key_type in KeyType::ALL {
            assert_eq!(key_from_wire(None, key_type), Err(WireKeyFault::Missing));
            assert_eq!(
                key_from_wire(Some(v1::Key { key: None }), key_type),
                Err(WireKeyFault::Missing)
            );
        }
    }

    #[test]
    fn test_key_from_wire_matching_variant_per_type() {
        assert_eq!(
            key_from_wire(Some(wire(v1::key::Key::Long(LONG_VALUE))), KeyType::Long).unwrap(),
            Key::Long(LONG_VALUE)
        );
        assert_eq!(
            key_from_wire(
                Some(wire(v1::key::Key::Str(STR_VALUE.to_string()))),
                KeyType::String
            )
            .unwrap(),
            Key::Str(STR_VALUE.to_string())
        );
        let bytes = vec![0u8; SHORT_BIN_LEN];
        assert_eq!(
            key_from_wire(
                Some(wire(v1::key::Key::Bin(bytes.clone()))),
                KeyType::Binary
            )
            .unwrap(),
            Key::Bytes(bytes)
        );
        let uuid = [7u8; UUID_BYTES];
        assert_eq!(
            key_from_wire(Some(wire(v1::key::Key::Bin(uuid.to_vec()))), KeyType::Uuid).unwrap(),
            Key::Uuid(uuid)
        );
    }

    #[test]
    fn test_key_from_wire_mismatched_variant_names_both_sides() {
        let variants = [
            v1::key::Key::Long(LONG_VALUE),
            v1::key::Key::Str(STR_VALUE.to_string()),
            v1::key::Key::Bin(vec![0u8; UUID_BYTES]),
        ];
        for key_type in KeyType::ALL {
            for variant in &variants {
                let matches = matches!(
                    (variant, key_type),
                    (v1::key::Key::Long(_), KeyType::Long)
                        | (v1::key::Key::Str(_), KeyType::String)
                        | (v1::key::Key::Bin(_), KeyType::Binary | KeyType::Uuid)
                );
                let result = key_from_wire(Some(wire(variant.clone())), key_type);
                if matches {
                    assert!(result.is_ok(), "{variant:?} on {key_type}");
                } else {
                    assert_eq!(
                        result,
                        Err(WireKeyFault::VariantMismatch {
                            expected: key_type,
                            got: WireVariant::of(variant),
                        }),
                        "{variant:?} on {key_type}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_key_from_wire_uuid_needs_exactly_sixteen_bytes() {
        for len in [0, SHORT_BIN_LEN, UUID_BYTES + 1] {
            assert_eq!(
                key_from_wire(Some(wire(v1::key::Key::Bin(vec![0u8; len]))), KeyType::Uuid),
                Err(WireKeyFault::UuidLength { bytes: len }),
                "{len} bytes"
            );
        }
    }

    #[test]
    fn test_from_wire_key_fault_is_invalid_argument_per_variant() {
        let faults = [
            WireKeyFault::Missing,
            WireKeyFault::VariantMismatch {
                expected: KeyType::Long,
                got: WireVariant::Str,
            },
            WireKeyFault::UuidLength {
                bytes: SHORT_BIN_LEN,
            },
        ];
        for fault in faults {
            assert_eq!(Status::from(fault).code(), Code::InvalidArgument);
        }
    }

    /// A deadline for the sample fault.
    const SAMPLE_DEADLINE: Duration = Duration::from_millis(250);

    /// A limit for the sample faults.
    const SAMPLE_LIMIT: usize = 8;

    #[test]
    fn test_from_call_fault_code_per_variant() {
        let cases = [
            (
                CallFault::DeadlineExceeded {
                    after: SAMPLE_DEADLINE,
                },
                Code::DeadlineExceeded,
            ),
            (
                CallFault::Overloaded {
                    limit: SAMPLE_LIMIT,
                },
                Code::ResourceExhausted,
            ),
            (
                CallFault::TooManyValues {
                    keys: SAMPLE_LIMIT + 1,
                    columns: 1,
                    limit: SAMPLE_LIMIT,
                },
                Code::InvalidArgument,
            ),
        ];
        for (fault, code) in cases {
            assert_eq!(Status::from(fault).code(), code);
        }
    }

    /// A slot is held from `admit` to the drop of what it returns, and
    /// with every slot held the next call is refused.
    #[test]
    fn test_admit_holds_a_slot_until_dropped_and_refuses_past_the_limit() {
        let slots = Slots::new(SAMPLE_LIMIT);
        assert_eq!((slots.limit(), slots.admitted()), (SAMPLE_LIMIT, 0));
        let held: Vec<_> = (0..SAMPLE_LIMIT).map(|_| slots.admit().unwrap()).collect();
        assert_eq!(slots.admitted(), SAMPLE_LIMIT);
        assert!(matches!(
            slots.admit(),
            Err(CallFault::Overloaded {
                limit: SAMPLE_LIMIT
            })
        ));
        assert_eq!(slots.admitted(), SAMPLE_LIMIT, "a refusal holds no slot");
        drop(held);
        assert_eq!(slots.admitted(), 0);
    }

    #[test]
    fn test_new_holds_max_in_flight_to_the_semaphore_s_bound() {
        assert_eq!(Slots::new(usize::MAX).limit(), Semaphore::MAX_PERMITS);
    }

    #[test]
    fn test_of_names_every_wire_variant() {
        let cases = [
            (v1::key::Key::Long(LONG_VALUE), WireVariant::Long),
            (v1::key::Key::Str(STR_VALUE.to_string()), WireVariant::Str),
            (v1::key::Key::Bin(Vec::new()), WireVariant::Bin),
        ];
        for (key, variant) in cases {
            assert_eq!(WireVariant::of(&key), variant);
        }
    }

    /// One error of every variant the binary can build without the Iceberg,
    /// Parquet, or object-store crates, beside its code.
    fn status_cases() -> Vec<(Error, Code)> {
        vec![
            (
                Error::KeyType {
                    table: TABLE.to_string(),
                    expected: KeyType::Long,
                    got: KeyType::String,
                },
                Code::InvalidArgument,
            ),
            (
                Error::NotReady {
                    table: TABLE.to_string(),
                },
                Code::Unavailable,
            ),
            (
                Error::Cache(CacheError::Fetch(Arc::new(Error::NotReady {
                    table: TABLE.to_string(),
                }))),
                Code::Unavailable,
            ),
            (
                Error::Cache(CacheError::FetchDeadline {
                    path: "mem://f0.parquet".to_string(),
                    offset: 0,
                    after: SAMPLE_DEADLINE,
                }),
                Code::DeadlineExceeded,
            ),
            (
                Error::Local {
                    path: PathBuf::from("/var/lib/icetray"),
                    fault: LocalFault::Io(std::io::Error::other("sample")),
                },
                Code::Unavailable,
            ),
            (
                Error::Json(serde_json::from_str::<u8>("sample").unwrap_err()),
                Code::Internal,
            ),
            (
                Error::Config(ConfigError::Invalid(ConfigViolation::EmptyTableName {
                    index: 0,
                })),
                Code::Internal,
            ),
            (
                Error::Refused {
                    table: TABLE.to_string(),
                    reason: RefusalReason::KeyTypeUnsupported,
                },
                Code::Internal,
            ),
            (
                Error::Table {
                    table: TABLE.to_string(),
                    fault: TableFault::MetadataLocationUnknown,
                },
                Code::Internal,
            ),
            (
                Error::File {
                    path: "mem://f0.parquet".to_string(),
                    fault: FileFault::NoRows,
                },
                Code::Internal,
            ),
            (
                Error::CatalogTimeout {
                    table: TABLE.to_string(),
                    after: Duration::ZERO,
                },
                Code::Unavailable,
            ),
            (
                Error::LoadDeadline {
                    table: TABLE.to_string(),
                    after: Duration::ZERO,
                },
                Code::Unavailable,
            ),
            (
                Error::RefreshDeadline {
                    table: TABLE.to_string(),
                    after: Duration::ZERO,
                },
                Code::Unavailable,
            ),
        ]
        .into_iter()
        .chain(column_cases())
        .chain(canceled_cases())
        .collect()
    }

    /// Every fault of a column a lookup names, beside its code, and a
    /// malformed value a projection met.
    fn column_cases() -> Vec<(Error, Code)> {
        let field = 7;
        [
            ColumnRequestFault::NotServed,
            ColumnRequestFault::Repeated,
            ColumnRequestFault::NotProtobuf,
            ColumnRequestFault::FieldOutOfRange { field: 0 },
            ColumnRequestFault::RepeatedField { field },
        ]
        .into_iter()
        .map(|fault| {
            (
                Error::ColumnRequest {
                    table: TABLE.to_string(),
                    column: "value".to_string(),
                    fault,
                },
                Code::InvalidArgument,
            )
        })
        .chain([(
            Error::MalformedValue {
                table: TABLE.to_string(),
                column: "value".to_string(),
                key: Key::Long(LONG_VALUE),
                fault: Malformed::WireType {
                    at: 0,
                    wire_type: 7,
                },
            },
            Code::FailedPrecondition,
        )])
        .collect()
    }

    /// The variants a canceled task ends in, beside their code.
    fn canceled_cases() -> [(Error, Code); 2] {
        [
            (
                Error::VerifyCanceled {
                    path: "mem://f0.parquet".to_string(),
                    source: canceled_task(),
                },
                Code::Unavailable,
            ),
            (
                Error::LookupCanceled {
                    table: TABLE.to_string(),
                    source: canceled_task(),
                },
                Code::Unavailable,
            ),
        ]
    }

    /// The outcome of a task aborted before it ran: the `JoinError` a
    /// canceled task leaves.
    fn canceled_task() -> tokio::task::JoinError {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let task = tokio::spawn(std::future::pending::<()>());
                task.abort();
                task.await.unwrap_err()
            })
    }

    #[test]
    fn test_status_from_code_per_variant() {
        for (error, code) in status_cases() {
            assert_eq!(status_from(&error).code(), code, "{error}");
        }
    }

    #[tokio::test]
    async fn test_status_from_object_store_failure_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let missing = format!("file://{}/missing", dir.path().display());
        let error = Storage::new(StorageConfig::default())
            .read_all(&missing)
            .await
            .expect_err("a missing file");
        assert!(matches!(error, Error::ObjectStore(_)), "{error:?}");
        assert_eq!(status_from(&error).code(), Code::Unavailable);
    }

    #[test]
    fn test_row_wire_carries_each_value_in_order_null_apart_from_zero_length() {
        let bytes = Bytes::from_static(STR_VALUE.as_bytes());
        let (present, wire) = row_wire(Row::Present(vec![
            None,
            Some(bytes.clone()),
            Some(Bytes::new()),
        ]));
        assert_eq!(present, v1::Presence::Present);
        assert_eq!(
            wire,
            [
                ColumnValue {
                    kind: Some(column_value::Kind::NullValue(NullValue {})),
                },
                ColumnValue {
                    kind: Some(column_value::Kind::Value(bytes.clone())),
                },
                ColumnValue {
                    kind: Some(column_value::Kind::Value(Bytes::new())),
                },
            ]
        );
        let Some(column_value::Kind::Value(carried)) = &wire[1].kind else {
            panic!("the second value is bytes");
        };
        assert_eq!(
            carried.as_ptr(),
            bytes.as_ptr(),
            "the message carries the lookup's bytes, not a copy"
        );
        assert_eq!(
            row_wire(Row::Present(Vec::new())),
            (v1::Presence::Present, Vec::new()),
            "a presence-only answer carries no value"
        );
        assert_eq!(row_wire(Row::Absent), (v1::Presence::Absent, Vec::new()));
    }

    #[test]
    fn test_check_values_counts_keys_times_columns_and_a_presence_only_key_as_one() {
        let request = |keys: usize, columns: usize| MultiGetRequest {
            table: TABLE.to_string(),
            keys: vec![wire(v1::key::Key::Long(LONG_VALUE)); keys],
            columns: (0..columns)
                .map(|at| v1::ColumnRead {
                    name: format!("c{at}"),
                    fields: vec![1, 2, 3],
                })
                .collect(),
        };
        let columns_named = 2;
        let at_limit = SAMPLE_LIMIT / columns_named;
        assert!(check_values(&request(at_limit, columns_named), SAMPLE_LIMIT).is_ok());
        assert!(matches!(
            check_values(&request(at_limit + 1, columns_named), SAMPLE_LIMIT),
            Err(CallFault::TooManyValues {
                keys,
                columns: 2,
                limit: SAMPLE_LIMIT,
            }) if keys == at_limit + 1
        ));
        assert!(check_values(&request(SAMPLE_LIMIT, 0), SAMPLE_LIMIT).is_ok());
        assert!(matches!(
            check_values(&request(SAMPLE_LIMIT + 1, 0), SAMPLE_LIMIT),
            Err(CallFault::TooManyValues { columns: 0, .. })
        ));
    }

    #[test]
    fn test_value_columns_wire_names_each_type_a_fixed_width_and_each_format() {
        let fixed_width = 16;
        let column = |name: &str, value_type| icetray_core::ValueColumnLayout {
            name: name.to_string(),
            field_id: 0,
            value_type,
            default: None,
        };
        let layout = TableLayout {
            bucketing: None,
            key_column: "key".to_string(),
            key_field_id: 0,
            key_type: KeyType::Long,
            key_width: None,
            value_columns: vec![
                column("a", ValueType::Binary),
                column("b", ValueType::Fixed { width: fixed_width }),
                column("c", ValueType::String),
            ],
        };
        let entry = TableConfig {
            name: TABLE.to_string(),
            source: SourceConfig::Static {
                location: "mem://w/db/kv".to_string(),
            },
            r#ref: "main".to_string(),
            key_column: "key".to_string(),
            value_columns: vec![
                ValueColumnConfig::named("c"),
                ValueColumnConfig::of("a", ValueFormat::Protobuf),
                ValueColumnConfig::named("b"),
            ],
            strict: false,
            lookups: None,
            residency: icetray_core::Residency::Full,
            resident_budget_bytes: None,
        };
        let wire: Vec<(String, i32, u32, i32)> = value_columns_wire(&layout, &entry)
            .into_iter()
            .map(|column| (column.name, column.value_type, column.width, column.format))
            .collect();
        let opaque = i32::from(v1::ValueFormat::Opaque);
        assert_eq!(
            wire,
            [
                (
                    "a".to_string(),
                    i32::from(v1::ValueType::Binary),
                    0,
                    i32::from(v1::ValueFormat::Protobuf)
                ),
                (
                    "b".to_string(),
                    i32::from(v1::ValueType::Fixed),
                    u32::try_from(fixed_width).unwrap(),
                    opaque
                ),
                ("c".to_string(), i32::from(v1::ValueType::String), 0, opaque),
            ]
        );
    }

    #[test]
    fn test_key_type_wire_per_type() {
        let cases = [
            (KeyType::Long, v1::KeyType::Long),
            (KeyType::String, v1::KeyType::String),
            (KeyType::Binary, v1::KeyType::Binary),
            (KeyType::Uuid, v1::KeyType::Uuid),
        ];
        assert_eq!(cases.map(|(key_type, _)| key_type), KeyType::ALL);
        for (key_type, wire) in cases {
            assert_eq!(key_type_wire(key_type), wire);
        }
    }
}
