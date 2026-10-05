//! The AWS catalog sources of RFD 019: a table in Glue or in S3 Tables,
//! asked for its current metadata location with one call through the AWS
//! SDK. The client signs with the credentials every S3 store of the same
//! `Storage` signs with (`Storage::aws_credentials`), so a catalog call
//! never runs a credential chain of its own, and the metadata file the
//! location names is read through `Storage` like a static source's (RFD
//! 003).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_glue::config::IdentityCache;
use aws_sdk_glue::error::{DisplayErrorContext, SdkError};
use aws_sdk_glue::operation::get_table::GetTableError;
use aws_sdk_s3tables::operation::get_table_metadata_location::GetTableMetadataLocationError;
use tokio::sync::OnceCell;

use crate::catalog::TableFault;
use crate::error::{Error, Result};
use crate::storage::Storage;

/// The Glue table parameter that says what format the table is in.
const GLUE_TABLE_TYPE: &str = "table_type";

/// The `table_type` of an Iceberg table, compared without regard to case:
/// Iceberg's own Glue catalog writes it in capitals, and other writers do
/// not all agree.
const ICEBERG_TABLE_TYPE: &str = "ICEBERG";

/// The Glue table parameter that holds an Iceberg table's current metadata
/// location, as Iceberg's Glue catalog writes it.
const GLUE_METADATA_LOCATION: &str = "metadata_location";

/// What separates the fields of an ARN.
const ARN_SEPARATOR: char = ':';

/// The first field of every ARN.
const ARN_SCHEME: &str = "arn";

/// The service field of a table bucket ARN.
const S3TABLES_SERVICE: &str = "s3tables";

/// What a table bucket ARN's resource field starts with, before the
/// bucket's name.
const BUCKET_RESOURCE: &str = "bucket/";

/// The fields of an ARN: `arn`, partition, service, region, account, and
/// resource. The resource may itself hold the separator, so the ARN is
/// split into at most this many.
const ARN_FIELDS: usize = 6;

/// The digits of an AWS account id.
const ACCOUNT_ID_DIGITS: usize = 12;

/// The region a table bucket ARN names, `arn:<partition>:s3tables:<region>:
/// <account>:bucket/<name>`, or `None` when `arn` is not one: a partition,
/// a region of lowercase letters, digits, and hyphens, a twelve-digit
/// account, and a bucket name of lowercase letters, digits, and hyphens.
/// What validation holds a configured ARN to, and the region an `s3tables`
/// source falls back to.
pub(crate) fn table_bucket_region(arn: &str) -> Option<&str> {
    let fields: Vec<&str> = arn.splitn(ARN_FIELDS, ARN_SEPARATOR).collect();
    let [scheme, partition, service, region, account, resource] = fields[..] else {
        return None;
    };
    let name = resource.strip_prefix(BUCKET_RESOURCE)?;
    let lowercase_word = |text: &str| {
        !text.is_empty()
            && text
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    };
    let valid = scheme == ARN_SCHEME
        && lowercase_word(partition)
        && service == S3TABLES_SERVICE
        && lowercase_word(region)
        && account.len() == ACCOUNT_ID_DIGITS
        && account.bytes().all(|byte| byte.is_ascii_digit())
        && lowercase_word(name);
    valid.then_some(region)
}

/// Why an AWS catalog call failed: the SDK's error for the one operation
/// each source makes, whole, so a caller can match the service's error
/// code. Display carries the SDK's full chain of causes.
#[derive(Debug, thiserror::Error)]
pub enum AwsCatalogError {
    /// Glue's `GetTable` failed.
    #[error("glue GetTable failed: {}", DisplayErrorContext(.0.as_ref()))]
    Glue(Box<SdkError<GetTableError>>),
    /// S3 Tables' `GetTableMetadataLocation` failed.
    #[error("s3 tables GetTableMetadataLocation failed: {}", DisplayErrorContext(.0.as_ref()))]
    S3Tables(Box<SdkError<GetTableMetadataLocationError>>),
}

/// A table in an AWS catalog (RFD 019), with the client that asks for it.
/// Cloning shares the client. The client is built on the first call, with
/// the storage the source is resolved through, after that storage has
/// loaded the SDK's defaults; a source is resolved through one storage.
#[derive(Clone, Debug)]
pub struct AwsSource {
    inner: Arc<AwsTable>,
}

/// Where the table is, the overrides its source names, and the client.
#[derive(Debug)]
struct AwsTable {
    catalog: AwsCatalog,
    /// The region the source names, which wins over every other.
    region: Option<String>,
    /// The endpoint URL the source names, for a catalog other than AWS's.
    endpoint: Option<String>,
}

/// The table in one of the two catalogs, with that catalog's client.
#[derive(Debug)]
enum AwsCatalog {
    Glue {
        database: String,
        table: String,
        /// Another account's catalog; the caller's own when absent.
        catalog_id: Option<String>,
        client: OnceCell<aws_sdk_glue::Client>,
    },
    S3Tables {
        table_bucket_arn: String,
        namespace: String,
        table: String,
        client: OnceCell<aws_sdk_s3tables::Client>,
    },
}

/// What an AWS client is built with, shared by the two catalogs' builders.
struct ClientSettings {
    /// The SDK's defaults: retry and timeout settings, the HTTP client, and
    /// the region its own resolution found.
    sdk: SdkConfig,
    /// The region the source settles on before the SDK's, if any.
    region: Option<Region>,
    /// The storage's credentials (`Storage::aws_credentials`).
    credentials: Option<SharedCredentialsProvider>,
    endpoint: Option<String>,
}

impl AwsSource {
    /// A Glue table: `database` and `table`, in `catalog_id`'s catalog or
    /// the caller's own.
    pub(crate) fn glue(
        database: &str,
        table: &str,
        catalog_id: Option<&str>,
        region: Option<&str>,
        endpoint: Option<&str>,
    ) -> Self {
        Self::new(
            AwsCatalog::Glue {
                database: database.to_string(),
                table: table.to_string(),
                catalog_id: catalog_id.map(str::to_string),
                client: OnceCell::new(),
            },
            region,
            endpoint,
        )
    }

    /// An S3 Tables table: `table` in `namespace` of the table bucket
    /// `table_bucket_arn`.
    pub(crate) fn s3tables(
        table_bucket_arn: &str,
        namespace: &str,
        table: &str,
        region: Option<&str>,
        endpoint: Option<&str>,
    ) -> Self {
        Self::new(
            AwsCatalog::S3Tables {
                table_bucket_arn: table_bucket_arn.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                client: OnceCell::new(),
            },
            region,
            endpoint,
        )
    }

    fn new(catalog: AwsCatalog, region: Option<&str>, endpoint: Option<&str>) -> Self {
        Self {
            inner: Arc::new(AwsTable {
                catalog,
                region: region.map(str::to_string),
                endpoint: endpoint.map(str::to_string),
            }),
        }
    }

    /// The table as its catalog names it, for an error about it before any
    /// configured name applies: `db.kv`, `db.kv in catalog 111122223333`,
    /// or `ns.kv in arn:aws:s3tables:...`.
    pub(crate) fn name(&self) -> String {
        match &self.inner.catalog {
            AwsCatalog::Glue {
                database,
                table,
                catalog_id: None,
                ..
            } => format!("{database}.{table}"),
            AwsCatalog::Glue {
                database,
                table,
                catalog_id: Some(catalog_id),
                ..
            } => format!("{database}.{table} in catalog {catalog_id}"),
            AwsCatalog::S3Tables {
                table_bucket_arn,
                namespace,
                table,
                ..
            } => format!("{namespace}.{table} in {table_bucket_arn}"),
        }
    }

    /// The table's current metadata location: one catalog call, within
    /// `Storage::catalog_timeout`. A call that runs longer is dropped as
    /// `Error::CatalogTimeout`, one the catalog refuses is `Error::Catalog`,
    /// both naming the table as its catalog does. The client is built
    /// before the bound starts, so the SDK's first load, which may wait on
    /// the instance metadata service, is not counted against the call.
    pub(crate) async fn metadata_location(&self, storage: &Storage) -> Result<String> {
        let timeout = storage.catalog_timeout();
        match &self.inner.catalog {
            AwsCatalog::Glue {
                database,
                table,
                catalog_id,
                client,
            } => {
                let client = client
                    .get_or_init(|| async { self.glue_client(storage).await })
                    .await;
                let call = client
                    .get_table()
                    .database_name(database)
                    .name(table)
                    .set_catalog_id(catalog_id.clone())
                    .send();
                let output = self
                    .within(timeout, call)
                    .await?
                    .map_err(|error| self.failed(AwsCatalogError::Glue(Box::new(error))))?;
                glue_location(
                    &self.name(),
                    output.table().and_then(|table| table.parameters()),
                )
            }
            AwsCatalog::S3Tables {
                table_bucket_arn,
                namespace,
                table,
                client,
            } => {
                let client = client
                    .get_or_init(|| async { self.s3tables_client(storage).await })
                    .await;
                let call = client
                    .get_table_metadata_location()
                    .table_bucket_arn(table_bucket_arn)
                    .namespace(namespace)
                    .name(table)
                    .send();
                let output = self
                    .within(timeout, call)
                    .await?
                    .map_err(|error| self.failed(AwsCatalogError::S3Tables(Box::new(error))))?;
                output
                    .metadata_location()
                    .filter(|location| !location.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        Error::table_fault(&self.name(), TableFault::MetadataLocationUnknown)
                    })
            }
        }
    }

    /// `call`'s outcome when it ends within `timeout`, else
    /// `Error::CatalogTimeout` naming the table.
    async fn within<T>(&self, timeout: Duration, call: impl Future<Output = T>) -> Result<T> {
        tokio::time::timeout(timeout, call)
            .await
            .map_err(|_elapsed| Error::CatalogTimeout {
                table: self.name(),
                after: timeout,
            })
    }

    /// `Error::Catalog` for this table.
    fn failed(&self, source: AwsCatalogError) -> Error {
        Error::Catalog {
            table: self.name(),
            source,
        }
    }

    /// What either client is built with. The region is the source's own,
    /// else `[storage.s3]`'s, else, for S3 Tables, the one the table
    /// bucket's ARN names, which is where the bucket is; only when none of
    /// those names one does the SDK's own resolution decide.
    async fn settings(&self, storage: &Storage) -> ClientSettings {
        let arn_region = match &self.inner.catalog {
            AwsCatalog::Glue { .. } => None,
            AwsCatalog::S3Tables {
                table_bucket_arn, ..
            } => table_bucket_region(table_bucket_arn),
        };
        let region = self
            .inner
            .region
            .as_deref()
            .or_else(|| storage.s3_region())
            .or(arn_region)
            .map(|region| Region::new(region.to_string()));
        let sdk = storage
            .aws_sdk_config()
            .await
            .cloned()
            .unwrap_or_else(|| SdkConfig::builder().build());
        ClientSettings {
            sdk,
            region,
            credentials: storage.aws_credentials(),
            endpoint: self.inner.endpoint.clone(),
        }
    }

    /// A Glue client over `settings`. The SDK's own identity cache is off:
    /// the credentials come from the storage's one cache, and a second
    /// cache in front of it would hold a credential past the moment the
    /// storage's own stops handing it out.
    async fn glue_client(&self, storage: &Storage) -> aws_sdk_glue::Client {
        let settings = self.settings(storage).await;
        let mut builder = aws_sdk_glue::config::Builder::from(&settings.sdk)
            .behavior_version(BehaviorVersion::latest())
            .identity_cache(IdentityCache::no_cache());
        builder.set_credentials_provider(settings.credentials);
        if let Some(region) = settings.region {
            builder = builder.region(region);
        }
        if let Some(endpoint) = settings.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        aws_sdk_glue::Client::from_conf(builder.build())
    }

    /// An S3 Tables client over `settings`, built as `glue_client` builds
    /// Glue's.
    async fn s3tables_client(&self, storage: &Storage) -> aws_sdk_s3tables::Client {
        let settings = self.settings(storage).await;
        let mut builder = aws_sdk_s3tables::config::Builder::from(&settings.sdk)
            .behavior_version(BehaviorVersion::latest())
            .identity_cache(IdentityCache::no_cache());
        builder.set_credentials_provider(settings.credentials);
        if let Some(region) = settings.region {
            builder = builder.region(region);
        }
        if let Some(endpoint) = settings.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        aws_sdk_s3tables::Client::from_conf(builder.build())
    }
}

/// The metadata location a Glue table's `parameters` hold, for the table
/// its catalog names `name`. A table whose `table_type` is not `ICEBERG`
/// in any case, or is absent, is `TableFault::NotIceberg`; an Iceberg
/// table without a `metadata_location`, or with an empty one, is
/// `TableFault::MetadataLocationUnknown`.
fn glue_location(name: &str, parameters: Option<&HashMap<String, String>>) -> Result<String> {
    let parameter = |key: &str| parameters.and_then(|parameters| parameters.get(key));
    let table_type = parameter(GLUE_TABLE_TYPE);
    if !table_type.is_some_and(|table_type| table_type.eq_ignore_ascii_case(ICEBERG_TABLE_TYPE)) {
        return Err(Error::table_fault(
            name,
            TableFault::NotIceberg {
                table_type: table_type.cloned(),
            },
        ));
    }
    parameter(GLUE_METADATA_LOCATION)
        .filter(|location| !location.is_empty())
        .cloned()
        .ok_or_else(|| Error::table_fault(name, TableFault::MetadataLocationUnknown))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use aws_credential_types::Credentials;
    use aws_sdk_glue::operation::get_table::GetTableError;

    use super::*;
    use crate::catalog::{TableSource, version_file};
    use crate::config::{S3Config, StorageConfig};
    use crate::generation::testing::{FIRST_VERSION, SECOND_VERSION, TABLE, table_fault_of};
    use crate::manifest::testing;
    use crate::storage::testing::{
        CountingProvider, DEADLINE_GUARD, LocalHttp, MEM_PREFIX, MemStore, Reply, with_loaded_sdk,
    };

    /// The Glue database and the S3 Tables namespace the tests name.
    const DATABASE: &str = "db";
    /// The region the SDK's own resolution found, as the tests load it.
    const SDK_REGION: &str = "us-sdk-1";
    /// The region a source names for itself.
    const SOURCE_REGION: &str = "us-source-1";
    /// The region `[storage.s3]` names.
    const STORAGE_REGION: &str = "us-storage-1";
    /// The region the test table bucket's ARN names.
    const ARN_REGION: &str = "us-arn-1";
    /// The test table bucket's ARN.
    const TABLE_BUCKET_ARN: &str = "arn:aws:s3tables:us-arn-1:111122223333:bucket/warehouse";
    /// The access key id the chain hands out.
    const KEY_ID: &str = "AKIACHAIN";
    /// The secret the chain hands out.
    const SECRET: &str = "secret-chain";
    /// The provider name the chain's credential carries.
    const PROVIDER: &str = "test";
    /// The catalog timeout of the test that waits on a catalog that never
    /// answers: short, so the test is quick.
    const TEST_CATALOG_TIMEOUT: Duration = Duration::from_millis(200);
    /// The status line of a request the catalog refuses.
    const BAD_REQUEST: &str = "400 Bad Request";
    /// The status line of a request the catalog answers.
    const OK: &str = "200 OK";
    /// What precedes the access key id in a `SigV4` `Authorization` header.
    const CREDENTIAL_FIELD: &str = "Credential=";
    /// Glue's `GetTable` as a JSON 1.1 request's target header names it.
    const GLUE_GET_TABLE: &str = "AWSGlue.GetTable";
    /// The header `SigV4` signs a request in.
    const AUTHORIZATION: &str = "authorization";

    fn root() -> String {
        format!("{MEM_PREFIX}{DATABASE}/{TABLE}")
    }

    fn location(version: u64) -> String {
        version_file(&root(), version)
    }

    /// A storage whose SDK chain is a counting provider, with the counter,
    /// and the in-memory store the metadata is written to.
    fn chained_storage(config: StorageConfig) -> (MemStore, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = CountingProvider {
            answers: vec![Credentials::new(KEY_ID, SECRET, None, None, PROVIDER)],
            calls: Arc::clone(&calls),
        };
        let sdk = SdkConfig::builder()
            .region(Region::new(SDK_REGION))
            .credentials_provider(SharedCredentialsProvider::new(provider))
            .build();
        (with_loaded_sdk(config, sdk), calls)
    }

    /// Writes metadata for a table with no snapshot at `location(version)`.
    async fn put_metadata(store: &MemStore, version: u64) {
        let metadata = testing::metadata(&root(), testing::long_schema(), testing::unpartitioned());
        store
            .put(&location(version), serde_json::to_vec(&metadata).unwrap())
            .await;
    }

    fn glue_source(catalog: &LocalHttp) -> TableSource {
        TableSource::Aws(AwsSource::glue(
            DATABASE,
            TABLE,
            None,
            None,
            Some(&catalog.url()),
        ))
    }

    fn s3tables_source(catalog: &LocalHttp) -> TableSource {
        TableSource::Aws(AwsSource::s3tables(
            TABLE_BUCKET_ARN,
            DATABASE,
            TABLE,
            None,
            Some(&catalog.url()),
        ))
    }

    /// Glue's answer for a table with these parameters.
    fn glue_answer(parameters: &serde_json::Value) -> Reply {
        Reply::Json {
            status: OK,
            body: serde_json::json!({
                "Table": {"Name": TABLE, "DatabaseName": DATABASE, "Parameters": parameters}
            })
            .to_string(),
        }
    }

    /// Glue's answer for an Iceberg table at `location`.
    fn glue_iceberg(location: &str) -> Reply {
        glue_answer(&serde_json::json!({
            GLUE_TABLE_TYPE: ICEBERG_TABLE_TYPE,
            GLUE_METADATA_LOCATION: location,
        }))
    }

    /// S3 Tables' answer, with `location` when there is one.
    fn s3tables_answer(location: Option<&str>) -> Reply {
        let mut body = serde_json::json!({
            "versionToken": "token-1",
            "warehouseLocation": "s3://warehouse-bucket",
        });
        if let Some(location) = location {
            body["metadataLocation"] = serde_json::Value::from(location);
        }
        Reply::Json {
            status: OK,
            body: body.to_string(),
        }
    }

    /// The value of the header `name` in a request head.
    fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(key, _)| key.trim().eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim())
    }

    /// Whether some header of a request head has the value `value`.
    fn has_header_value(head: &str, value: &str) -> bool {
        head.lines()
            .filter_map(|line| line.split_once(':'))
            .any(|(_, found)| found.trim() == value)
    }

    /// The `SigV4` credential scope a request was signed under:
    /// `<key id>/<date>/<region>/<service>/aws4_request`.
    fn credential_scope(head: &str) -> String {
        let authorization = header(head, AUTHORIZATION).expect("the request is signed");
        let (_, scope) = authorization
            .split_once(CREDENTIAL_FIELD)
            .expect("a `SigV4` authorization names its credential");
        scope.split(',').next().unwrap().to_string()
    }

    fn scope_parts(head: &str) -> Vec<String> {
        credential_scope(head)
            .split('/')
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn test_table_bucket_region_is_the_arn_s_region_and_none_for_another_arn() {
        assert_eq!(table_bucket_region(TABLE_BUCKET_ARN), Some(ARN_REGION));
        assert_eq!(
            table_bucket_region("arn:aws-cn:s3tables:cn-north-1:111122223333:bucket/b-1"),
            Some("cn-north-1")
        );
        for arn in [
            "",
            "warehouse",
            "arn:aws:s3:::warehouse",
            "arn:aws:s3tables:us-east-1:111122223333:table/warehouse",
            "arn:aws:s3tables:us-east-1:111122223333:bucket/",
            "arn:aws:s3tables:us-east-1:111122223333:bucket/a/b",
            "arn:aws:s3tables:us-east-1:1111:bucket/warehouse",
            "arn:aws:s3tables::111122223333:bucket/warehouse",
            "arn::s3tables:us-east-1:111122223333:bucket/warehouse",
            "arn:aws:s3tables:US-EAST-1:111122223333:bucket/warehouse",
            "urn:aws:s3tables:us-east-1:111122223333:bucket/warehouse",
        ] {
            assert_eq!(table_bucket_region(arn), None, "{arn}");
        }
    }

    #[test]
    fn test_glue_location_takes_an_iceberg_table_s_location_in_any_case() {
        for table_type in ["ICEBERG", "iceberg", "Iceberg"] {
            let parameters = HashMap::from([
                (GLUE_TABLE_TYPE.to_string(), table_type.to_string()),
                (GLUE_METADATA_LOCATION.to_string(), location(FIRST_VERSION)),
            ]);
            assert_eq!(
                glue_location(TABLE, Some(&parameters)).unwrap(),
                location(FIRST_VERSION),
                "{table_type}"
            );
        }
    }

    #[test]
    fn test_glue_location_refuses_a_table_that_is_not_iceberg_or_has_no_location() {
        let hive = HashMap::from([
            (GLUE_TABLE_TYPE.to_string(), "HIVE".to_string()),
            (GLUE_METADATA_LOCATION.to_string(), location(FIRST_VERSION)),
        ]);
        assert_eq!(
            table_fault_of(glue_location(TABLE, Some(&hive)), TABLE),
            TableFault::NotIceberg {
                table_type: Some("HIVE".to_string()),
            }
        );
        let untyped =
            HashMap::from([(GLUE_METADATA_LOCATION.to_string(), location(FIRST_VERSION))]);
        for parameters in [Some(&untyped), None] {
            assert_eq!(
                table_fault_of(glue_location(TABLE, parameters), TABLE),
                TableFault::NotIceberg { table_type: None }
            );
        }
        for location in [None, Some(String::new())] {
            let mut parameters =
                HashMap::from([(GLUE_TABLE_TYPE.to_string(), ICEBERG_TABLE_TYPE.to_string())]);
            if let Some(location) = location {
                parameters.insert(GLUE_METADATA_LOCATION.to_string(), location);
            }
            assert_eq!(
                table_fault_of(glue_location(TABLE, Some(&parameters)), TABLE),
                TableFault::MetadataLocationUnknown
            );
        }
    }

    /// The region is the source's, else `[storage.s3]`'s, else the table
    /// bucket ARN's; only a Glue source with neither leaves it to the SDK.
    #[tokio::test]
    async fn test_settings_take_the_region_in_order() {
        let (plain, _) = chained_storage(StorageConfig::default());
        let (regional, _) = chained_storage(StorageConfig {
            s3: Some(S3Config {
                region: Some(STORAGE_REGION.to_string()),
                ..S3Config::default()
            }),
            ..StorageConfig::default()
        });
        let region = |source: &AwsSource, storage: &MemStore| {
            let source = source.clone();
            let storage = Arc::clone(&storage.storage);
            async move { source.settings(&storage).await.region }
        };
        let glue = AwsSource::glue(DATABASE, TABLE, None, None, None);
        let glue_named = AwsSource::glue(DATABASE, TABLE, None, Some(SOURCE_REGION), None);
        let s3tables = AwsSource::s3tables(TABLE_BUCKET_ARN, DATABASE, TABLE, None, None);
        let s3tables_named =
            AwsSource::s3tables(TABLE_BUCKET_ARN, DATABASE, TABLE, Some(SOURCE_REGION), None);
        assert_eq!(region(&glue, &plain).await, None);
        assert_eq!(
            region(&glue, &regional).await,
            Some(Region::new(STORAGE_REGION))
        );
        assert_eq!(
            region(&glue_named, &regional).await,
            Some(Region::new(SOURCE_REGION))
        );
        assert_eq!(
            region(&s3tables, &plain).await,
            Some(Region::new(ARN_REGION))
        );
        assert_eq!(
            region(&s3tables, &regional).await,
            Some(Region::new(STORAGE_REGION))
        );
        assert_eq!(
            region(&s3tables_named, &regional).await,
            Some(Region::new(SOURCE_REGION))
        );
        let client = glue.glue_client(&plain.storage).await;
        assert_eq!(client.config().region(), Some(&Region::new(SDK_REGION)));
    }

    /// One Glue call finds the location, the metadata file is read through
    /// storage, and a poll that finds the location unchanged costs that
    /// one call and no read; a moved location is read on the next poll.
    #[tokio::test]
    async fn test_resolve_glue_reads_the_location_s_metadata_and_polls_with_one_call() {
        let catalog = LocalHttp::start(glue_iceberg(&location(FIRST_VERSION)));
        let (store, _) = chained_storage(StorageConfig::default());
        put_metadata(&store, FIRST_VERSION).await;
        put_metadata(&store, SECOND_VERSION).await;
        let source = glue_source(&catalog);
        let stats = store.storage.stats();

        let resolved = source.resolve(&store.storage).await.unwrap();
        assert_eq!(resolved.location, location(FIRST_VERSION));
        assert_eq!(catalog.requests(), 1);
        assert_eq!(stats.gets(), 1, "the metadata file");
        let head = &catalog.heads()[0];
        assert!(has_header_value(head, GLUE_GET_TABLE), "{head}");

        let unchanged = source
            .resolve_if_changed(&store.storage, Some(&resolved.location))
            .await
            .unwrap();
        assert!(unchanged.is_none());
        assert_eq!(catalog.requests(), 2, "one call per poll");
        assert_eq!(stats.gets(), 1, "an unchanged poll reads nothing");

        catalog.set_reply(glue_iceberg(&location(SECOND_VERSION)));
        let moved = source
            .resolve_if_changed(&store.storage, Some(&resolved.location))
            .await
            .unwrap()
            .expect("the location moved");
        assert_eq!(moved.location, location(SECOND_VERSION));
        assert_eq!(catalog.requests(), 3);
        assert_eq!(stats.gets(), 2, "the new metadata file");
    }

    #[tokio::test]
    async fn test_resolve_s3tables_reads_the_location_s_metadata_and_polls_with_one_call() {
        let catalog = LocalHttp::start(s3tables_answer(Some(&location(FIRST_VERSION))));
        let (store, _) = chained_storage(StorageConfig::default());
        put_metadata(&store, FIRST_VERSION).await;
        let source = s3tables_source(&catalog);

        let resolved = source.resolve(&store.storage).await.unwrap();
        assert_eq!(resolved.location, location(FIRST_VERSION));
        let unchanged = source
            .resolve_if_changed(&store.storage, Some(&resolved.location))
            .await
            .unwrap();
        assert!(unchanged.is_none());
        assert_eq!(catalog.requests(), 2, "one call per poll");
        assert_eq!(store.storage.stats().gets(), 1, "the metadata file once");
        let parts = scope_parts(&catalog.heads()[0]);
        assert_eq!(parts[2], ARN_REGION, "signed for the bucket's region");
        assert_eq!(parts[3], S3TABLES_SERVICE);
    }

    #[tokio::test]
    async fn test_resolve_refuses_a_glue_table_that_is_not_iceberg() {
        let catalog = LocalHttp::start(glue_answer(&serde_json::json!({
            GLUE_TABLE_TYPE: "HIVE",
            GLUE_METADATA_LOCATION: location(FIRST_VERSION),
        })));
        let (store, _) = chained_storage(StorageConfig::default());
        let name = format!("{DATABASE}.{TABLE}");
        assert_eq!(
            table_fault_of(glue_source(&catalog).resolve(&store.storage).await, &name),
            TableFault::NotIceberg {
                table_type: Some("HIVE".to_string()),
            }
        );
        assert_eq!(store.storage.stats().gets(), 0, "nothing is read");
    }

    #[tokio::test]
    async fn test_resolve_refuses_a_table_without_a_metadata_location() {
        let (store, _) = chained_storage(StorageConfig::default());
        let glue = LocalHttp::start(glue_answer(
            &serde_json::json!({GLUE_TABLE_TYPE: ICEBERG_TABLE_TYPE}),
        ));
        assert_eq!(
            table_fault_of(
                glue_source(&glue).resolve(&store.storage).await,
                &format!("{DATABASE}.{TABLE}")
            ),
            TableFault::MetadataLocationUnknown
        );
        let s3tables = LocalHttp::start(s3tables_answer(None));
        assert_eq!(
            table_fault_of(
                s3tables_source(&s3tables).resolve(&store.storage).await,
                &format!("{DATABASE}.{TABLE} in {TABLE_BUCKET_ARN}")
            ),
            TableFault::MetadataLocationUnknown
        );
    }

    #[tokio::test]
    async fn test_resolve_names_the_table_when_the_catalog_refuses_the_call() {
        let catalog = LocalHttp::start(Reply::Json {
            status: BAD_REQUEST,
            body: serde_json::json!({
                "__type": "EntityNotFoundException",
                "Message": "table not found",
            })
            .to_string(),
        });
        let (store, _) = chained_storage(StorageConfig::default());
        let source = TableSource::Aws(AwsSource::glue(
            DATABASE,
            TABLE,
            Some("111122223333"),
            None,
            Some(&catalog.url()),
        ));
        let resolved = source.resolve(&store.storage).await;
        let Err(Error::Catalog {
            table,
            source: AwsCatalogError::Glue(error),
        }) = resolved
        else {
            panic!("expected a glue catalog error, got {resolved:?}");
        };
        assert_eq!(table, format!("{DATABASE}.{TABLE} in catalog 111122223333"));
        assert!(
            error
                .as_service_error()
                .is_some_and(GetTableError::is_entity_not_found_exception),
            "{error:?}"
        );
        assert_eq!(catalog.requests(), 1, "a refusal is not retried");
    }

    #[tokio::test]
    async fn test_resolve_fails_at_the_catalog_timeout_naming_the_table() {
        let catalog = LocalHttp::start(Reply::Never);
        let (store, _) = chained_storage(StorageConfig {
            catalog_timeout: TEST_CATALOG_TIMEOUT,
            ..StorageConfig::default()
        });
        let resolved = tokio::time::timeout(
            DEADLINE_GUARD,
            s3tables_source(&catalog).resolve(&store.storage),
        )
        .await
        .expect("the catalog call ends at its timeout, before the guard");
        let Err(Error::CatalogTimeout { table, after }) = resolved else {
            panic!("expected a catalog timeout, got {resolved:?}");
        };
        assert_eq!(table, format!("{DATABASE}.{TABLE} in {TABLE_BUCKET_ARN}"));
        assert_eq!(after, TEST_CATALOG_TIMEOUT);
        assert!(catalog.requests() > 0, "the call reached the catalog");
    }

    /// The catalog signs with the storage's one bridge: two catalog calls
    /// and a read from an S3 store run the chain once between them, and
    /// the catalog sees the chain's key in the region it was configured
    /// for.
    #[tokio::test]
    async fn test_catalog_calls_sign_through_the_bridge_s3_stores_share() {
        let catalog = LocalHttp::start(glue_iceberg(&location(FIRST_VERSION)));
        let s3 = LocalHttp::start(Reply::Status("503 Service Unavailable"));
        let (store, calls) = chained_storage(StorageConfig {
            s3: Some(S3Config {
                endpoint: Some(s3.url()),
                allow_http: true,
                path_style: true,
                ..S3Config::default()
            }),
            max_retries: 0,
            ..StorageConfig::default()
        });
        put_metadata(&store, FIRST_VERSION).await;
        let source = TableSource::Aws(AwsSource::glue(
            DATABASE,
            TABLE,
            None,
            Some(SOURCE_REGION),
            Some(&catalog.url()),
        ));
        let resolved = source.resolve(&store.storage).await.unwrap();
        source
            .resolve_if_changed(&store.storage, Some(&resolved.location))
            .await
            .unwrap();
        let s3_read = store.storage.read_all("s3://bucket-a/db/kv/f.bin").await;
        assert!(matches!(s3_read, Err(Error::ObjectStore(_))), "{s3_read:?}");
        assert_eq!(s3.requests(), 1, "the S3 read was sent");
        assert_eq!(catalog.requests(), 2);
        assert_eq!(calls.load(Ordering::Relaxed), 1, "one chain run for all");
        for head in catalog.heads() {
            let parts = scope_parts(&head);
            assert_eq!(parts[0], KEY_ID);
            assert_eq!(parts[2], SOURCE_REGION);
            assert_eq!(parts[3], "glue");
        }
    }

    /// With static keys in `[storage.s3]`, the catalog signs with them, as
    /// every S3 store does, and the chain is never run.
    #[tokio::test]
    async fn test_catalog_calls_sign_with_the_static_keys_when_set() {
        let catalog = LocalHttp::start(glue_iceberg(&location(FIRST_VERSION)));
        let static_key = "AKIASTATIC";
        let (store, calls) = chained_storage(StorageConfig {
            s3: Some(S3Config {
                access_key_id: Some(static_key.to_string()),
                secret_access_key: Some(SECRET.to_string()),
                ..S3Config::default()
            }),
            ..StorageConfig::default()
        });
        put_metadata(&store, FIRST_VERSION).await;
        glue_source(&catalog).resolve(&store.storage).await.unwrap();
        assert_eq!(scope_parts(&catalog.heads()[0])[0], static_key);
        assert_eq!(calls.load(Ordering::Relaxed), 0, "the chain is not run");
    }
}
