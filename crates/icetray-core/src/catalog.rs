//! Where a table's current metadata comes from (RFD 005): a static table
//! root followed through its version hint, a metadata file named directly,
//! or a catalog, the REST catalog included. A poll that finds nothing moved
//! costs one small read: the version hint, or one table load from the
//! catalog. Every byte of object storage goes through `Storage` (RFD 003);
//! a catalog client speaks to its catalog over HTTP and never to object
//! storage.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use iceberg::io::MemoryStorageFactory;
use iceberg::spec::{Snapshot, TableMetadata};
use iceberg::{Catalog, CatalogBuilder as _, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};

use crate::config::SourceConfig;
use crate::error::{Error, Result};
use crate::snapshot::SnapshotId;
use crate::storage::{PATH_SEPARATOR, SCHEME_SEPARATOR, Storage, split_location};

/// The file under a static table root that names the current metadata
/// version, as the Hadoop catalog lays a table out. Its content is a bare
/// version number (`3` means `metadata/v3.metadata.json`), a file name
/// under `metadata/`, or a full location.
pub(crate) const VERSION_HINT: &str = "metadata/version-hint.text";

/// The directory under a table root that holds its metadata files, where a
/// version hint's bare version or file name resolves.
pub(crate) const METADATA_DIR: &str = "metadata";

/// The suffix of a metadata file's name. A static location ending in it
/// names the file itself and never advances; any other names a table root.
const METADATA_SUFFIX: &str = ".metadata.json";

/// The prefix of a metadata file named by a bare version: `v3.metadata.json`.
const VERSION_FILE_PREFIX: &str = "v";

/// What separates the levels of a namespace in configuration: `db.schema`.
const NAMESPACE_SEPARATOR: char = '.';

/// The name the REST client is registered under. The protocol requires one;
/// nothing reads it back.
const REST_CATALOG_NAME: &str = "icetray";

/// What makes a table unusable before any data file is read: its metadata,
/// its version hint, or its catalog record. The set is closed; the `table`
/// of `Error::Table` says which table, or which source, it is about.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TableFault {
    /// The configured ref is neither a branch nor a tag of the metadata.
    /// iceberg-rust checks every ref against the snapshot list when
    /// metadata is parsed or built, so a ref that is present always names a
    /// snapshot the metadata holds; a missing ref is the one ref fault.
    #[error("ref {ref_name} is not in the metadata")]
    RefNotFound {
        /// The ref as configured.
        ref_name: String,
    },
    /// The snapshot records a schema id the metadata does not have.
    #[error("schema {schema_id} is not in the metadata")]
    SchemaMissing {
        /// The schema id the snapshot records.
        schema_id: i32,
    },
    /// A manifest records a partition spec id the metadata does not have.
    #[error("partition spec {spec_id} is not in the metadata")]
    SpecMissing {
        /// The spec id the manifest list records for the manifest.
        spec_id: i32,
    },
    /// A configured column is not in the snapshot's schema.
    #[error("column {column} is not in the snapshot's schema")]
    ColumnMissing {
        /// The column as configured.
        column: String,
    },
    /// A partition spec buckets the key into zero buckets; the transform
    /// divides by the count, so no key routes.
    #[error("partition spec {spec_id} buckets the key into zero buckets")]
    ZeroBuckets {
        /// The spec carrying the transform.
        spec_id: i32,
    },
    /// A data file's partition tuple has no non-negative integer where its
    /// spec puts the key's bucket (a null, another type, or a negative
    /// value), so the file cannot be routed to.
    #[error("data file {path} has no usable bucket value under partition spec {spec_id}")]
    BucketValueMissing {
        /// The data file's location.
        path: String,
        /// The spec the manifest listed the file under.
        spec_id: i32,
    },
    /// A data file's partition tuple puts it in a bucket at or past its
    /// spec's count. No key hashes to that bucket, so a lookup never
    /// consults the file and a key it holds would be answered absent.
    #[error(
        "data file {path} is in bucket {bucket} of partition spec {spec_id}, which has {count} buckets"
    )]
    BucketValueOutOfRange {
        /// The data file's location.
        path: String,
        /// The spec the manifest listed the file under.
        spec_id: i32,
        /// The bucket the partition tuple records.
        bucket: u32,
        /// The spec's bucket count.
        count: u32,
    },
    /// The version hint under `root` holds nothing but whitespace.
    #[error("version hint under {root} is empty")]
    VersionHintEmpty {
        /// The table root.
        root: String,
    },
    /// The version hint under `root` is not one token of text: it is not
    /// UTF-8, or it carries whitespace or control characters inside.
    #[error("version hint under {root} is malformed: {hint:?}")]
    VersionHintMalformed {
        /// The table root.
        root: String,
        /// The content, rendered lossily when it is not UTF-8.
        hint: String,
    },
    /// The catalog returned the table without a metadata location, so
    /// there is nothing to compare a poll against.
    #[error("the catalog holds no metadata location for the table")]
    MetadataLocationUnknown,
    /// The snapshot's timestamp is outside the range a `jiff::Timestamp`
    /// represents, so its age cannot be measured.
    #[error("snapshot {snapshot_id} has a timestamp outside the representable range")]
    SnapshotTimestampOutOfRange {
        /// The snapshot whose timestamp it is.
        snapshot_id: SnapshotId,
    },
}

/// The current metadata and the location it was read from; the location is
/// what a later poll compares against.
#[derive(Clone, Debug)]
pub(crate) struct Resolved {
    /// The parsed metadata.
    pub metadata: Arc<TableMetadata>,
    /// The metadata file's location.
    pub location: String,
}

impl Resolved {
    /// The snapshot the branch or tag `ref_name` points at; a ref the
    /// metadata does not have is `TableFault::RefNotFound` under `table`.
    pub(crate) fn snapshot_for_ref(&self, table: &str, ref_name: &str) -> Result<Arc<Snapshot>> {
        self.metadata
            .snapshot_for_ref(ref_name)
            .cloned()
            .ok_or_else(|| {
                Error::table_fault(
                    table,
                    TableFault::RefNotFound {
                        ref_name: ref_name.to_string(),
                    },
                )
            })
    }
}

/// A source of current table metadata.
#[derive(Clone, Debug)]
pub enum TableSource {
    /// A metadata file, or a table root whose `metadata/` carries a version
    /// hint, read through `Storage`.
    Static {
        /// The metadata file location or the table root.
        location: String,
    },
    /// Any catalog client, loaded by identifier.
    Catalog {
        /// The catalog.
        catalog: Arc<dyn Catalog>,
        /// The table within it.
        ident: TableIdent,
    },
}

impl TableSource {
    /// Builds the source a configuration names. A REST source becomes a
    /// `RestCatalog` over its URI and warehouse; this is the only place a
    /// catalog client is constructed. Constructing one sends nothing: the
    /// client first speaks to the catalog when a table is loaded.
    pub async fn from_config(cfg: &SourceConfig) -> Result<Self> {
        match cfg {
            SourceConfig::Static { location } => Ok(Self::Static {
                location: location.clone(),
            }),
            SourceConfig::Rest {
                uri,
                warehouse,
                namespace,
                table,
            } => {
                let mut props = HashMap::from([(REST_CATALOG_PROP_URI.to_string(), uri.clone())]);
                if let Some(warehouse) = warehouse {
                    props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                }
                // The client refuses to load a table without a storage
                // factory and builds file I/O from it lazily, on the first
                // read through the library. icetray reads every byte through
                // `Storage` (RFD 003) and never through the library, so the
                // factory is never asked to build; the in-memory one is the
                // placeholder that, should it ever be, reaches an empty store
                // and never the host.
                let catalog = RestCatalogBuilder::default()
                    .with_storage_factory(Arc::new(MemoryStorageFactory))
                    .load(REST_CATALOG_NAME, props)
                    .await?;
                let namespace = NamespaceIdent::from_strs(namespace.split(NAMESPACE_SEPARATOR))?;
                Ok(Self::Catalog {
                    catalog: Arc::new(catalog),
                    ident: TableIdent::new(namespace, table.clone()),
                })
            }
        }
    }

    /// The current metadata. A static root costs two reads, the version
    /// hint and the metadata file; a static metadata file one; a catalog
    /// source one table load from the catalog and no object-storage read.
    pub(crate) async fn resolve(&self, storage: &Storage) -> Result<Resolved> {
        Ok(self
            .resolve_if_changed(storage, None)
            .await?
            .expect("with no known location nothing is unchanged, so the metadata is read"))
    }

    /// The current metadata when its location is not `known`, else `None`;
    /// with no `known`, the metadata. When nothing moved this costs one
    /// small read: a static root reads its version hint and parses no
    /// metadata; a catalog source costs one table load, whose metadata the
    /// client parses either way, within `Storage::catalog_timeout`. A
    /// static source that names a metadata file directly reads nothing and
    /// never advances.
    pub(crate) async fn resolve_if_changed(
        &self,
        storage: &Storage,
        known: Option<&str>,
    ) -> Result<Option<Resolved>> {
        match self {
            Self::Static { location } => {
                let file = current_metadata_file(storage, location).await?;
                if known == Some(file.as_str()) {
                    return Ok(None);
                }
                read_metadata(storage, file).await.map(Some)
            }
            Self::Catalog { catalog, ident } => {
                let resolved = load_from_catalog(catalog, ident, storage.catalog_timeout()).await?;
                Ok((known != Some(resolved.location.as_str())).then_some(resolved))
            }
        }
    }
}

/// The metadata file a static location names now: the location itself when
/// it ends in `METADATA_SUFFIX`, otherwise the file the root's version hint
/// points at, which is one read.
async fn current_metadata_file(storage: &Storage, location: &str) -> Result<String> {
    if location.ends_with(METADATA_SUFFIX) {
        return Ok(location.to_string());
    }
    let root = location.trim_end_matches(PATH_SEPARATOR);
    let bytes = storage.read_all(&hint_location(root)).await?;
    Ok(parse_hint(root, &bytes)?.metadata_file(root))
}

/// Reads and parses the metadata file at `file`: one read.
async fn read_metadata(storage: &Storage, file: String) -> Result<Resolved> {
    let bytes = storage.read_all(&file).await?;
    let metadata: TableMetadata = serde_json::from_slice(&bytes)?;
    Ok(Resolved {
        metadata: Arc::new(metadata),
        location: file,
    })
}

/// The table as the catalog holds it now. A record without a metadata
/// location is `TableFault::MetadataLocationUnknown` under the identifier.
/// The call runs within `timeout`, and one that runs longer is dropped as
/// `Error::CatalogTimeout`. The bound is on the call rather than on an HTTP
/// client handed to the REST builder, so it covers every request the call
/// makes, the client's configuration and token fetches included, and holds
/// for any `Catalog`.
async fn load_from_catalog(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    timeout: Duration,
) -> Result<Resolved> {
    let table = tokio::time::timeout(timeout, catalog.load_table(ident))
        .await
        .map_err(|_elapsed| Error::CatalogTimeout {
            table: ident.to_string(),
            after: timeout,
        })??;
    let location = table
        .metadata_location()
        .ok_or_else(|| Error::table_fault(&ident.to_string(), TableFault::MetadataLocationUnknown))?
        .to_string();
    Ok(Resolved {
        metadata: table.metadata_ref(),
        location,
    })
}

/// `rest` under `root`, which carries no trailing separator.
pub(crate) fn under(root: &str, rest: &str) -> String {
    format!("{root}{PATH_SEPARATOR}{rest}")
}

/// The location of the version hint under `root`, which carries no trailing
/// separator: `metadata/version-hint.text`. A writer that publishes a
/// snapshot to a static root writes the current metadata location there;
/// this module owns the layout under a table root, so the testkit's hint
/// writer names the file through it.
pub fn hint_location(root: &str) -> String {
    under(root, VERSION_HINT)
}

/// The directory names a path can end in that name no directory of their
/// own: the one it is in and the one above.
const RELATIVE_DIRS: [&str; 2] = [".", ".."];

/// The name of the table a static `location` opens: the last component of
/// its table root, `kv` for `s3://warehouse/db/kv/` and for
/// `s3://warehouse/db/kv/metadata/v3.metadata.json`, whose root is the
/// directory holding its `metadata/`. `None` when the root has no such
/// component, as for a bucket alone or a path ending in `..`, or the
/// location is not one `Storage` resolves.
pub fn table_root_name(location: &str) -> Option<String> {
    let (_, path) = split_location(location).ok()?;
    let mut components = path
        .rsplit(PATH_SEPARATOR)
        .filter(|component| !component.is_empty());
    let mut name = components.next()?;
    if name.ends_with(METADATA_SUFFIX) {
        name = components.next()?;
        if name == METADATA_DIR {
            name = components.next()?;
        }
    }
    (!RELATIVE_DIRS.contains(&name)).then(|| name.to_string())
}

/// What ends a URI's scheme (RFC 3986).
const SCHEME_END: char = ':';

/// Whether `location` is a relative local path: not empty, not starting at
/// the root, and naming no URI scheme, which RFC 3986 spells as a letter,
/// then letters, digits, `+`, `-`, or `.`, up to a `:`. `Storage` refuses
/// such a location, since it names no store; the command line makes one
/// absolute against its working directory first, while a configuration
/// file's locations must be absolute or URIs already, so the file reads the
/// same from any directory.
pub fn is_relative_path(location: &str) -> bool {
    let scheme = location
        .split_once(SCHEME_END)
        .map(|(scheme, _)| scheme)
        .filter(|scheme| is_scheme(scheme));
    !location.is_empty() && !location.starts_with(PATH_SEPARATOR) && scheme.is_none()
}

/// Whether `text` is a URI scheme as RFC 3986 spells one.
fn is_scheme(text: &str) -> bool {
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && chars.all(|rest| rest.is_ascii_alphanumeric() || matches!(rest, '+' | '-' | '.'))
}

/// The metadata file a bare `version` names under `root`:
/// `metadata/v<version>.metadata.json`, the Hadoop catalog's layout. A
/// writer publishing a snapshot by hand names its metadata file through it.
pub fn version_file(root: &str, version: u64) -> String {
    under(
        root,
        &format!("{METADATA_DIR}{PATH_SEPARATOR}{VERSION_FILE_PREFIX}{version}{METADATA_SUFFIX}"),
    )
}

/// The three forms a version hint takes.
#[derive(Debug, PartialEq, Eq)]
enum VersionHint {
    /// A bare version number: `3` names `metadata/v3.metadata.json`.
    Version(u64),
    /// A file name under `metadata/`.
    FileName(String),
    /// A full location: a URI or an absolute path.
    Location(String),
}

impl VersionHint {
    /// The metadata file the hint names for the table at `root`.
    fn metadata_file(self, root: &str) -> String {
        match self {
            Self::Version(version) => version_file(root, version),
            Self::FileName(name) => under(root, &format!("{METADATA_DIR}{PATH_SEPARATOR}{name}")),
            Self::Location(location) => location,
        }
    }
}

/// The hint the version-hint file under `root` carries: its content
/// trimmed, which must be one token of UTF-8 text. Nothing but whitespace
/// is `TableFault::VersionHintEmpty`; bytes that are not UTF-8, or text
/// with whitespace or control characters inside, are
/// `TableFault::VersionHintMalformed`.
fn parse_hint(root: &str, bytes: &[u8]) -> Result<VersionHint> {
    let malformed = |hint: String| {
        Error::table_fault(
            root,
            TableFault::VersionHintMalformed {
                root: root.to_string(),
                hint,
            },
        )
    };
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Err(malformed(String::from_utf8_lossy(bytes).into_owned()));
    };
    let text = text.trim();
    if text.is_empty() {
        return Err(Error::table_fault(
            root,
            TableFault::VersionHintEmpty {
                root: root.to_string(),
            },
        ));
    }
    if text
        .chars()
        .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(malformed(text.to_string()));
    }
    if let Ok(version) = text.parse::<u64>() {
        return Ok(VersionHint::Version(version));
    }
    if text.contains(SCHEME_SEPARATOR) || text.starts_with(PATH_SEPARATOR) {
        return Ok(VersionHint::Location(text.to_string()));
    }
    Ok(VersionHint::FileName(text.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use iceberg::TableCreation;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        MAIN_BRANCH, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
        TableMetadata,
    };

    use super::*;
    use crate::config::StorageConfig;
    use crate::generation::testing::{
        FIRST_SEQUENCE, FIRST_SNAPSHOT, FIRST_VERSION, SECOND_SEQUENCE, SECOND_SNAPSHOT,
        SECOND_VERSION, TABLE, THIRD_VERSION, table_fault_of,
    };
    use crate::manifest::testing;
    use crate::storage::testing::{DEADLINE_GUARD, LocalHttp, MEM_PREFIX, MemStore, Reply};

    /// The table root under the prefix.
    const ROOT_KEY: &str = "db/kv";
    /// The tag the first snapshot carries.
    const TAG: &str = "serving";
    /// A ref no metadata has.
    const MISSING_REF: &str = "nightly";
    /// Bytes that are not UTF-8: a lone continuation byte, then a digit.
    const NOT_UTF8: &[u8] = &[0xff, b'1'];
    /// The catalog name and namespace of the memory-catalog test.
    const CATALOG_NAME: &str = "test";
    const NAMESPACE: &str = "db";
    /// The warehouse of the memory catalog, in its own in-memory storage.
    const WAREHOUSE: &str = "memory://warehouse";
    /// The catalog timeout of the test that waits on a catalog that never
    /// answers: short, so the test is quick.
    const TEST_CATALOG_TIMEOUT: Duration = Duration::from_millis(200);

    fn root() -> String {
        format!("{MEM_PREFIX}{ROOT_KEY}")
    }

    fn metadata_location(version: u64) -> String {
        version_file(&root(), version)
    }

    /// The in-memory store behind a storage, with the writes the tests make.
    struct Store {
        mem: MemStore,
    }

    impl Store {
        fn new() -> Self {
            Self {
                mem: MemStore::new(),
            }
        }

        async fn put(&self, location: &str, bytes: impl Into<Bytes>) {
            self.mem.put(location, bytes).await;
        }

        async fn put_metadata(&self, version: u64, md: &TableMetadata) {
            self.put(&metadata_location(version), serde_json::to_vec(md).unwrap())
                .await;
        }

        async fn put_hint(&self, content: impl Into<Bytes>) {
            self.put(&hint_location(&root()), content).await;
        }

        fn gets(&self) -> u64 {
            self.mem.storage.stats().gets()
        }
    }

    /// A snapshot to add to `md`; its manifest list is never read.
    fn snapshot(md: &TableMetadata, id: i64, parent: Option<i64>, sequence: i64) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence)
            .with_timestamp_ms(testing::snapshot_timestamp(md, sequence))
            .with_manifest_list(format!("{}/metadata/snap-{id}.avro", root()))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }

    /// Metadata with `FIRST_SNAPSHOT` on main and under `TAG`.
    fn first_metadata() -> TableMetadata {
        let base = testing::metadata(&root(), testing::long_schema(), testing::unpartitioned());
        let first = snapshot(&base, FIRST_SNAPSHOT, None, FIRST_SEQUENCE);
        base.into_builder(None)
            .set_branch_snapshot(first, MAIN_BRANCH)
            .unwrap()
            .set_ref(
                TAG,
                SnapshotReference::new(
                    FIRST_SNAPSHOT,
                    SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    /// `first_metadata` advanced: main at `SECOND_SNAPSHOT`, the tag still
    /// at the first.
    fn second_metadata() -> TableMetadata {
        let first = first_metadata();
        let second = snapshot(
            &first,
            SECOND_SNAPSHOT,
            Some(FIRST_SNAPSHOT),
            SECOND_SEQUENCE,
        );
        first
            .into_builder(None)
            .set_branch_snapshot(second, MAIN_BRANCH)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    fn static_root() -> TableSource {
        TableSource::Static { location: root() }
    }

    #[tokio::test]
    async fn test_resolve_follows_every_version_hint_form() {
        let store = Store::new();
        store.put_metadata(SECOND_VERSION, &second_metadata()).await;
        let expected = metadata_location(SECOND_VERSION);
        let hints = [
            SECOND_VERSION.to_string(),
            format!("{SECOND_VERSION}\n"),
            format!("v{SECOND_VERSION}.metadata.json"),
            expected.clone(),
        ];
        for hint in hints {
            store.put_hint(hint.clone()).await;
            let before = store.gets();
            let resolved = static_root().resolve(&store.mem.storage).await.unwrap();
            assert_eq!(resolved.location, expected, "hint {hint:?}");
            assert_eq!(
                resolved.metadata.current_snapshot_id(),
                Some(SECOND_SNAPSHOT)
            );
            assert_eq!(store.gets(), before + 2, "the hint and the metadata");
        }
        // A root with a trailing separator resolves the same files.
        let resolved = TableSource::Static {
            location: format!("{}/", root()),
        }
        .resolve(&store.mem.storage)
        .await
        .unwrap();
        assert_eq!(resolved.location, expected);
    }

    #[tokio::test]
    async fn test_resolve_empty_hint_is_a_table_fault() {
        let store = Store::new();
        for content in ["", "  \n\t"] {
            store.put_hint(content).await;
            assert_eq!(
                table_fault_of(static_root().resolve(&store.mem.storage).await, &root()),
                TableFault::VersionHintEmpty { root: root() }
            );
        }
        assert_eq!(store.gets(), 2, "the hint alone, twice");
    }

    #[tokio::test]
    async fn test_resolve_malformed_hint_is_a_table_fault() {
        let store = Store::new();
        let cases: [(&[u8], String); 3] = [
            (b"1 2", "1 2".to_string()),
            (
                b"v2.metadata.json\nv3.metadata.json",
                "v2.metadata.json\nv3.metadata.json".to_string(),
            ),
            (NOT_UTF8, String::from_utf8_lossy(NOT_UTF8).into_owned()),
        ];
        for (content, hint) in cases {
            store.put_hint(content).await;
            assert_eq!(
                table_fault_of(static_root().resolve(&store.mem.storage).await, &root()),
                TableFault::VersionHintMalformed { root: root(), hint }
            );
        }
    }

    #[tokio::test]
    async fn test_resolve_if_changed_costs_one_get_when_unchanged_and_advances_with_the_hint() {
        let store = Store::new();
        store.put_metadata(FIRST_VERSION, &first_metadata()).await;
        store.put_hint(FIRST_VERSION.to_string()).await;
        let known = static_root().resolve(&store.mem.storage).await.unwrap();
        assert_eq!(known.location, metadata_location(FIRST_VERSION));

        let before = store.gets();
        let unchanged = static_root()
            .resolve_if_changed(&store.mem.storage, Some(&known.location))
            .await
            .unwrap();
        assert!(unchanged.is_none());
        assert_eq!(store.gets(), before + 1, "the hint alone");

        store.put_metadata(SECOND_VERSION, &second_metadata()).await;
        store.put_hint(SECOND_VERSION.to_string()).await;
        let before = store.gets();
        let advanced = static_root()
            .resolve_if_changed(&store.mem.storage, Some(&known.location))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(advanced.location, metadata_location(SECOND_VERSION));
        assert_eq!(
            advanced.metadata.current_snapshot_id(),
            Some(SECOND_SNAPSHOT)
        );
        assert_eq!(store.gets(), before + 2, "the hint and the new metadata");
    }

    #[tokio::test]
    async fn test_resolve_if_changed_static_metadata_file_never_advances() {
        let store = Store::new();
        store.put_metadata(FIRST_VERSION, &first_metadata()).await;
        store.put_metadata(SECOND_VERSION, &second_metadata()).await;
        store.put_hint(SECOND_VERSION.to_string()).await;
        let pinned = TableSource::Static {
            location: metadata_location(FIRST_VERSION),
        };
        let resolved = pinned.resolve(&store.mem.storage).await.unwrap();
        assert_eq!(resolved.location, metadata_location(FIRST_VERSION));
        assert_eq!(
            resolved.metadata.current_snapshot_id(),
            Some(FIRST_SNAPSHOT)
        );
        assert_eq!(store.gets(), 1, "the metadata alone; no hint is read");

        let unchanged = pinned
            .resolve_if_changed(&store.mem.storage, Some(&resolved.location))
            .await
            .unwrap();
        assert!(unchanged.is_none());
        assert_eq!(store.gets(), 1, "nothing is read for a pinned file");
    }

    #[test]
    fn test_snapshot_for_ref_branch_tag_and_missing() {
        let resolved = Resolved {
            metadata: Arc::new(second_metadata()),
            location: metadata_location(SECOND_VERSION),
        };
        assert_eq!(
            resolved
                .snapshot_for_ref(TABLE, MAIN_BRANCH)
                .unwrap()
                .snapshot_id(),
            SECOND_SNAPSHOT
        );
        assert_eq!(
            resolved.snapshot_for_ref(TABLE, TAG).unwrap().snapshot_id(),
            FIRST_SNAPSHOT
        );
        assert_eq!(
            table_fault_of(resolved.snapshot_for_ref(TABLE, MISSING_REF), TABLE),
            TableFault::RefNotFound {
                ref_name: MISSING_REF.to_string(),
            }
        );
    }

    /// The second metadata as JSON with `FIRST_SNAPSHOT` removed from the
    /// snapshot list and log, and with the tag naming it kept or removed.
    fn json_without_first_snapshot(keep_tag: bool) -> Vec<u8> {
        let mut json: serde_json::Value = serde_json::to_value(second_metadata()).unwrap();
        let object = json.as_object_mut().unwrap();
        let names_first = |entry: &serde_json::Value| entry["snapshot-id"] == FIRST_SNAPSHOT;
        for list in ["snapshots", "snapshot-log"] {
            object[list]
                .as_array_mut()
                .unwrap()
                .retain(|entry| !names_first(entry));
        }
        if !keep_tag {
            object["refs"].as_object_mut().unwrap().remove(TAG);
        }
        serde_json::to_vec(&json).unwrap()
    }

    #[tokio::test]
    async fn test_resolve_refuses_metadata_whose_ref_names_a_missing_snapshot_at_parse() {
        let store = Store::new();
        store.put_hint(THIRD_VERSION.to_string()).await;
        store
            .put(
                &metadata_location(THIRD_VERSION),
                json_without_first_snapshot(false),
            )
            .await;
        let parsed = static_root().resolve(&store.mem.storage).await.unwrap();
        assert_eq!(parsed.metadata.snapshots().len(), 1);
        assert!(parsed.metadata.snapshot_for_ref(TAG).is_none());

        store
            .put(
                &metadata_location(THIRD_VERSION),
                json_without_first_snapshot(true),
            )
            .await;
        // iceberg-rust validates refs inside its deserializer, so the
        // refusal arrives as the JSON parse error.
        let dangling = static_root().resolve(&store.mem.storage).await;
        assert!(
            matches!(dangling, Err(Error::Json(_))),
            "a ref to a missing snapshot is refused by the parser, got {dangling:?}"
        );
    }

    #[tokio::test]
    async fn test_from_config_builds_a_static_source_and_a_rest_catalog() {
        let fixed = TableSource::from_config(&SourceConfig::Static { location: root() })
            .await
            .unwrap();
        let TableSource::Static { location } = fixed else {
            panic!("expected a static source, got {fixed:?}");
        };
        assert_eq!(location, root());

        let rest = TableSource::from_config(&SourceConfig::Rest {
            uri: "http://127.0.0.1:1/catalog".to_string(),
            warehouse: Some("mem://w".to_string()),
            namespace: "db.analytics".to_string(),
            table: TABLE.to_string(),
        })
        .await
        .unwrap();
        let TableSource::Catalog { ident, .. } = rest else {
            panic!("expected a catalog source, got {rest:?}");
        };
        assert_eq!(ident.namespace().as_ref(), &["db", "analytics"]);
        assert_eq!(ident.name(), TABLE);
    }

    #[tokio::test]
    async fn test_resolve_fails_at_the_catalog_timeout_when_the_catalog_never_answers() {
        let catalog = LocalHttp::start(Reply::Never);
        let source = TableSource::from_config(&SourceConfig::Rest {
            uri: catalog.url(),
            warehouse: None,
            namespace: NAMESPACE.to_string(),
            table: TABLE.to_string(),
        })
        .await
        .unwrap();
        let storage = Storage::new(StorageConfig {
            catalog_timeout: TEST_CATALOG_TIMEOUT,
            ..StorageConfig::default()
        });
        let resolved = tokio::time::timeout(DEADLINE_GUARD, source.resolve(&storage))
            .await
            .expect("the catalog call ends at its timeout, before the guard");
        let Err(Error::CatalogTimeout { table, after }) = resolved else {
            panic!("expected a catalog timeout, got {resolved:?}");
        };
        let ident = TableIdent::new(
            NamespaceIdent::new(NAMESPACE.to_string()),
            TABLE.to_string(),
        );
        assert_eq!(table, ident.to_string());
        assert_eq!(after, TEST_CATALOG_TIMEOUT);
        assert!(catalog.requests() > 0, "the call reached the catalog");
    }

    #[tokio::test]
    async fn test_resolve_through_a_catalog_reads_nothing_from_object_storage() {
        let catalog = MemoryCatalogBuilder::default()
            .load(
                CATALOG_NAME,
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), WAREHOUSE.to_string())]),
            )
            .await
            .unwrap();
        let namespace = NamespaceIdent::new(NAMESPACE.to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let created = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(TABLE.to_string())
                    .schema(testing::long_schema())
                    .build(),
            )
            .await
            .unwrap();
        let source = TableSource::Catalog {
            catalog: Arc::new(catalog),
            ident: created.identifier().clone(),
        };
        let store = Store::new();
        let resolved = source.resolve(&store.mem.storage).await.unwrap();
        assert_eq!(resolved.location, created.metadata_location().unwrap());
        assert_eq!(resolved.metadata.uuid(), created.metadata().uuid());
        let unchanged = source
            .resolve_if_changed(&store.mem.storage, Some(&resolved.location))
            .await
            .unwrap();
        assert!(unchanged.is_none());
        let moved = source
            .resolve_if_changed(&store.mem.storage, Some(MISSING_REF))
            .await
            .unwrap();
        assert!(moved.is_some());
        assert_eq!(
            store.gets(),
            0,
            "a catalog source never reads object storage"
        );
    }

    #[test]
    fn test_table_root_name_is_the_root_s_last_component() {
        let kv = Some("kv".to_string());
        for location in [
            "s3://warehouse/db/kv",
            "s3://warehouse/db/kv/",
            "s3://warehouse/db/kv/metadata/v3.metadata.json",
            "s3://warehouse/db/kv/metadata/00001-6a3e.metadata.json",
            "s3://warehouse/kv/v3.metadata.json",
            "file:///data/kv",
            "/data/kv//",
            "/data/kv/metadata/v1.metadata.json",
        ] {
            assert_eq!(table_root_name(location), kv, "{location}");
        }
    }

    #[test]
    fn test_is_relative_path_only_for_a_local_path_off_the_root() {
        for location in [
            "kv",
            "db/kv",
            "./db/kv",
            "../warehouse/db/kv/",
            "db/a:b",
            "1db:kv",
        ] {
            assert!(is_relative_path(location), "{location}");
        }
        for location in [
            "",
            "/data/kv",
            "s3://warehouse/db/kv",
            "s3a://warehouse/db/kv",
            "file:///data/kv",
            "file:/data/kv",
            "file:data/kv",
            "mem+x.y-z:kv",
        ] {
            assert!(!is_relative_path(location), "{location}");
        }
    }

    #[test]
    fn test_table_root_name_is_none_without_a_usable_component() {
        for location in [
            "s3://warehouse",
            "s3://warehouse/",
            "s3://warehouse/metadata/v3.metadata.json",
            "/",
            "/data/..",
            "/data/.",
            "relative/kv",
        ] {
            assert_eq!(table_root_name(location), None, "{location}");
        }
    }
}
