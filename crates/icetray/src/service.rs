//! The gRPC service over the generated contract (RFD 010): `Get`,
//! `MultiGet`, and `TableInfo` over the tables `open` built, the wire key
//! read under the table's key type, and core's errors mapped to status codes
//! by variant. Every call is counted under `requests_total`. `Get` and
//! `MultiGet` run under the server's deadline for the method (RFD 013); a
//! client's own deadline is tonic's, which drops the call at it. Together
//! they hold at most `server.max_in_flight` slots, and a call that finds
//! none is refused at once. A `MultiGet` carries at most
//! `server.max_multi_get_keys` keys.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::Guard;
use bytes::Bytes;
use icetray_core::config::ServerConfig;
use icetray_core::key::UUID_BYTES;
use icetray_core::{CacheError, Error, Key, KeyType, Opened, Table, Tables, Value, approx};
use icetray_proto::v1;
use icetray_proto::v1::icetray_server::Icetray;
use icetray_proto::v1::{
    GetRequest, GetResponse, Hit, MultiGetRequest, MultiGetResponse, TableInfoRequest,
    TableInfoResponse,
};
use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};
use tonic::{Request, Response, Status};

use crate::error::{CallFault, WireKeyFault};
use crate::telemetry::{self, Call};

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
    /// `Get` and `MultiGet` together; nothing closes it.
    in_flight: Arc<Semaphore>,
    /// The slots `in_flight` was built with, for the refusal.
    max_in_flight: usize,
    /// The most keys one `MultiGet` may carry.
    max_multi_get_keys: usize,
}

impl Service {
    /// The service over the tables `opened` serves, by configured name,
    /// under `server`'s request bounds. `max_in_flight` is held to tokio's
    /// `Semaphore::MAX_PERMITS`, which is far above any count of calls one
    /// process can hold open.
    pub fn new(opened: &Arc<Opened>, server: &ServerConfig) -> Self {
        let max_in_flight = server.max_in_flight.min(Semaphore::MAX_PERMITS);
        Self {
            opened: Arc::clone(opened),
            get_deadline: server.get_deadline,
            multi_get_deadline: server.multi_get_deadline,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
            max_in_flight,
            max_multi_get_keys: server.max_multi_get_keys,
        }
    }

    /// A slot for one call, held until the call ends, its deadline
    /// included; with none free the call is refused at once, never queued.
    fn admit(&self) -> Result<SemaphorePermit<'_>, CallFault> {
        self.in_flight.try_acquire().map_err(|error| match error {
            // `Closed` is unreachable, since nothing closes the
            // semaphore; were it closed, no call could be admitted.
            TryAcquireError::NoPermits | TryAcquireError::Closed => CallFault::Overloaded {
                limit: self.max_in_flight,
            },
        })
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
            CallFault::TooManyKeys { .. } => Self::invalid_argument(message),
        }
    }
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

/// Core's error as a status (RFD 010): a key of the wrong type is
/// `INVALID_ARGUMENT`; a page fetch that ran past `server.get_deadline` is
/// `DEADLINE_EXCEEDED`, the code the call's own deadline answers: a `Get`'s
/// deadline and its fetch's pass moments apart, and the call answers one
/// code whichever passes first; a read the caller can
/// retry (no generation yet, an object-store, cache, or local-directory
/// failure, a catalog or load that did not answer in time, a load's key
/// verification or a batch's lookup task canceled as the runtime shut
/// down) is
/// `UNAVAILABLE`; every other failure is `INTERNAL`. The message is the
/// error's `Display`.
pub fn status_from(error: &Error) -> Status {
    let message = error.to_string();
    match error {
        Error::KeyType { .. } => Status::invalid_argument(message),
        Error::Cache(CacheError::FetchDeadline { .. }) => Status::deadline_exceeded(message),
        Error::NotReady { .. }
        | Error::ObjectStore(_)
        | Error::Cache(CacheError::Fetch(_) | CacheError::Backend(_))
        | Error::Local { .. }
        | Error::CatalogTimeout { .. }
        | Error::LoadDeadline { .. }
        | Error::RefreshDeadline { .. }
        | Error::VerifyCanceled { .. }
        | Error::LookupCanceled { .. } => Status::unavailable(message),
        Error::Iceberg(_)
        | Error::Parquet(_)
        | Error::ObjectStorePath(_)
        | Error::Json(_)
        | Error::Config(_)
        | Error::Refused { .. }
        | Error::Inefficient { .. }
        | Error::Table { .. }
        | Error::File { .. } => Status::internal(message),
    }
}

/// A lookup's outcome on the wire: the presence and the value bytes, empty
/// for a null or absent key. A present value's bytes are the lookup's own,
/// moved into the message, not copied.
pub fn presence(value: Value) -> (v1::Presence, Bytes) {
    match value {
        Value::Present(bytes) => (v1::Presence::Present, bytes),
        Value::Null => (v1::Presence::Null, Bytes::new()),
        Value::Absent => (v1::Presence::Absent, Bytes::new()),
    }
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
        let lookup = table.get(&key).await.map_err(|error| status_from(&error))?;
        let (presence, value) = presence(lookup.value);
        Ok(Response::new(GetResponse {
            presence: i32::from(presence),
            value,
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
            .multi_get(&keys)
            .await
            .map_err(|error| status_from(&error))?;
        let results = lookup
            .values
            .into_iter()
            .map(|value| {
                let (presence, value) = presence(value);
                Hit {
                    presence: i32::from(presence),
                    value,
                }
            })
            .collect();
        Ok(Response::new(MultiGetResponse {
            results,
            snapshot_id: lookup.snapshot_id.0,
        }))
    }
}

#[tonic::async_trait]
impl Icetray for Service {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        telemetry::count(Call::Get);
        let _slot = self.admit()?;
        within(self.get_deadline, self.lookup(request.into_inner())).await
    }

    async fn multi_get(
        &self,
        request: Request<MultiGetRequest>,
    ) -> Result<Response<MultiGetResponse>, Status> {
        telemetry::count(Call::MultiGet);
        let keys = request.get_ref().keys.len();
        if keys > self.max_multi_get_keys {
            return Err(CallFault::TooManyKeys {
                keys,
                limit: self.max_multi_get_keys,
            }
            .into());
        }
        let _slot = self.admit()?;
        within(
            self.multi_get_deadline,
            self.lookup_batch(request.into_inner()),
        )
        .await
    }

    async fn table_info(
        &self,
        request: Request<TableInfoRequest>,
    ) -> Result<Response<TableInfoResponse>, Status> {
        telemetry::count(Call::TableInfo);
        let request = request.into_inner();
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
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use bytes::Bytes;
    use icetray_core::config::{ConfigError, ConfigViolation, StorageConfig};
    use icetray_core::{FileFault, FindingCode, LocalFault, RefusalReason, Storage, TableFault};
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
                CallFault::TooManyKeys {
                    keys: SAMPLE_LIMIT + 1,
                    limit: SAMPLE_LIMIT,
                },
                Code::InvalidArgument,
            ),
        ];
        for (fault, code) in cases {
            assert_eq!(Status::from(fault).code(), code);
        }
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
                    reason: RefusalReason::DeleteFiles,
                },
                Code::Internal,
            ),
            (
                Error::Inefficient {
                    table: TABLE.to_string(),
                    findings: vec![FindingCode::DictionaryEncodedValue],
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
        .chain(canceled_cases())
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
    fn test_presence_per_value() {
        let bytes = Bytes::from_static(STR_VALUE.as_bytes());
        let (present, wire) = presence(Value::Present(bytes.clone()));
        assert_eq!((present, &wire), (v1::Presence::Present, &bytes));
        assert_eq!(
            wire.as_ptr(),
            bytes.as_ptr(),
            "the message carries the lookup's bytes, not a copy"
        );
        assert_eq!(presence(Value::Null), (v1::Presence::Null, Bytes::new()));
        assert_eq!(
            presence(Value::Absent),
            (v1::Presence::Absent, Bytes::new())
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
