//! A stand-in for the two AWS catalog operations icetray calls (RFD 019):
//! Glue's `GetTable`, a JSON 1.1 POST that names the operation in its
//! `X-Amz-Target` header, and S3 Tables' `GetTableMetadataLocation`, a REST GET of
//! `/tables/{arn}/{namespace}/{name}/metadata-location`. It serves HTTP/1
//! on a loopback port from a thread of its own, so it answers a test on
//! any runtime and a binary the test spawns alike, and a source reaches it
//! through its `endpoint` setting. Each table's answer is set by the test;
//! a table it was never told of is the service's not-found error. It counts
//! the calls it answers and records the access key id each was signed
//! with, and checks no signature.

use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex, PoisonError};

use crate::error::Result;

/// Loopback with an OS-assigned port: the address the server binds.
const LOOPBACK_ANY_PORT: (Ipv4Addr, u16) = (Ipv4Addr::LOCALHOST, 0);

/// What ends a request's head.
const END_OF_HEAD: &[u8] = b"\r\n\r\n";

/// The header that gives a request body's length.
const CONTENT_LENGTH: &str = "content-length";

/// The header a JSON 1.1 request names its operation in.
const TARGET: &str = "X-Amz-Target";

/// Glue's `GetTable` as the `TARGET` header names it.
const GLUE_GET_TABLE: &str = "AWSGlue.GetTable";

/// The header `SigV4` signs a request in.
const AUTHORIZATION: &str = "authorization";

/// What precedes the access key id in a `SigV4` `Authorization` header.
const CREDENTIAL_FIELD: &str = "Credential=";

/// What separates the access key id from the rest of the credential scope.
const SCOPE_SEPARATOR: char = '/';

/// The first path segment of every S3 Tables table operation.
const TABLES_SEGMENT: &str = "tables";

/// The last path segment of `GetTableMetadataLocation`.
const METADATA_LOCATION_SEGMENT: &str = "metadata-location";

/// The Glue table parameters an Iceberg table carries.
const GLUE_TABLE_TYPE: &str = "table_type";
const GLUE_METADATA_LOCATION: &str = "metadata_location";
const ICEBERG: &str = "ICEBERG";

/// The status lines the fake answers with.
const OK: &str = "200 OK";
const BAD_REQUEST: &str = "400 Bad Request";
const NOT_FOUND: &str = "404 Not Found";

/// What the fake answers for one table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogEntry {
    /// An Iceberg table whose current metadata is at this location.
    Iceberg(String),
    /// A Glue table whose `table_type` parameter is this, with a metadata
    /// location; S3 Tables holds only Iceberg tables and answers it as
    /// `Iceberg`.
    TableType {
        /// The `table_type` parameter.
        table_type: String,
        /// The metadata location.
        location: String,
    },
    /// An Iceberg table the catalog holds no metadata location for.
    NoLocation,
}

/// The fake's state, shared with its thread.
#[derive(Debug, Default)]
struct State {
    /// Each table's answer, by namespace (or Glue database) and name.
    tables: HashMap<(String, String), CatalogEntry>,
    /// The access key id of every call answered, in order; empty for a
    /// call that was not signed.
    key_ids: Vec<String>,
}

/// A running fake. Its thread outlives it; the process ends it.
pub struct FakeAwsCatalog {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
}

impl FakeAwsCatalog {
    /// Binds a loopback port and starts answering.
    pub fn start() -> Result<Self> {
        let listener = TcpListener::bind(LOOPBACK_ANY_PORT)?;
        let addr = listener.local_addr()?;
        let state = Arc::new(Mutex::new(State::default()));
        let served = Arc::clone(&state);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let answered = stream.and_then(|mut stream| answer(&mut stream, &served));
                if let Err(error) = answered {
                    tracing::debug!(%error, "aws catalog fake connection ended");
                }
            }
        });
        Ok(Self { addr, state })
    }

    /// `http://` and the fake's address: a source's `endpoint`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Answers `entry` for `table` in `namespace` from now on.
    pub fn set(&self, namespace: &str, table: &str, entry: CatalogEntry) {
        self.lock()
            .tables
            .insert((namespace.to_string(), table.to_string()), entry);
    }

    /// The calls answered so far.
    pub fn calls(&self) -> usize {
        self.lock().key_ids.len()
    }

    /// The access key id each call answered so far was signed with.
    pub fn key_ids(&self) -> Vec<String> {
        self.lock().key_ids.clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One request read whole.
struct Request {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    /// The value of the header `name`, compared without regard to case.
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// The access key id the request was signed with, or empty.
    fn key_id(&self) -> String {
        self.header(AUTHORIZATION)
            .and_then(|authorization| authorization.split_once(CREDENTIAL_FIELD))
            .and_then(|(_, scope)| scope.split(SCOPE_SEPARATOR).next())
            .unwrap_or_default()
            .to_string()
    }
}

/// Reads one request from `stream`, answers it from `state`, and closes.
fn answer(stream: &mut TcpStream, state: &Mutex<State>) -> std::io::Result<()> {
    let request = read_request(stream)?;
    let (status, body) = {
        let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
        state.key_ids.push(request.key_id());
        route(&request, &state.tables)
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// The status line and body for `request`.
fn route(
    request: &Request,
    tables: &HashMap<(String, String), CatalogEntry>,
) -> (&'static str, String) {
    if request.method == "POST" && request.header(TARGET) == Some(GLUE_GET_TABLE) {
        // intentional: a body that is not JSON names no table, and is
        // answered as a table the catalog does not hold.
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap_or_default();
        let database = body["DatabaseName"].as_str().unwrap_or_default();
        let name = body["Name"].as_str().unwrap_or_default();
        return glue_answer(database, name, tables);
    }
    let segments: Vec<&str> = request.path.trim_start_matches('/').split('/').collect();
    match segments[..] {
        [
            TABLES_SEGMENT,
            _arn,
            namespace,
            name,
            METADATA_LOCATION_SEGMENT,
        ] if request.method == "GET" => s3tables_answer(namespace, name, tables),
        _ => (
            BAD_REQUEST,
            serde_json::json!({"__type": "UnknownOperationException"}).to_string(),
        ),
    }
}

/// Glue's answer for `GetTable` of `name` in `database`.
fn glue_answer(
    database: &str,
    name: &str,
    tables: &HashMap<(String, String), CatalogEntry>,
) -> (&'static str, String) {
    let parameters = match tables.get(&(database.to_string(), name.to_string())) {
        None => {
            return (
                BAD_REQUEST,
                serde_json::json!({
                    "__type": "EntityNotFoundException",
                    "Message": "Table not found",
                })
                .to_string(),
            );
        }
        Some(CatalogEntry::Iceberg(location)) => serde_json::json!({
            GLUE_TABLE_TYPE: ICEBERG,
            GLUE_METADATA_LOCATION: location,
        }),
        Some(CatalogEntry::TableType {
            table_type,
            location,
        }) => serde_json::json!({
            GLUE_TABLE_TYPE: table_type,
            GLUE_METADATA_LOCATION: location,
        }),
        Some(CatalogEntry::NoLocation) => serde_json::json!({GLUE_TABLE_TYPE: ICEBERG}),
    };
    let body = serde_json::json!({
        "Table": {"Name": name, "DatabaseName": database, "Parameters": parameters}
    });
    (OK, body.to_string())
}

/// S3 Tables' answer for `GetTableMetadataLocation` of `name` in
/// `namespace`.
fn s3tables_answer(
    namespace: &str,
    name: &str,
    tables: &HashMap<(String, String), CatalogEntry>,
) -> (&'static str, String) {
    let location = match tables.get(&(namespace.to_string(), name.to_string())) {
        None => {
            return (
                NOT_FOUND,
                serde_json::json!({"__type": "NotFoundException", "message": "Table not found"})
                    .to_string(),
            );
        }
        Some(CatalogEntry::Iceberg(location) | CatalogEntry::TableType { location, .. }) => {
            Some(location)
        }
        Some(CatalogEntry::NoLocation) => None,
    };
    let mut body = serde_json::json!({
        "versionToken": "fake-version",
        "warehouseLocation": "s3://fake-warehouse",
    });
    if let Some(location) = location {
        body["metadataLocation"] = serde_json::Value::from(location.as_str());
    }
    (OK, body.to_string())
}

/// Reads one request whole: the head through the blank line that ends it,
/// then as many body bytes as its `content-length` names.
fn read_request(stream: &mut TcpStream) -> std::io::Result<Request> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(END_OF_HEAD) {
        stream.read_exact(&mut byte)?;
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).into_owned();
    let mut lines = head.lines();
    let mut request_line = lines.next().unwrap_or_default().split_whitespace();
    let method = request_line.next().unwrap_or_default().to_string();
    let path = request_line.next().unwrap_or_default().to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
        .collect();
    let body_len = match headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(CONTENT_LENGTH))
    {
        Some((_, value)) => value
            .parse::<usize>()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?,
        None => 0,
    };
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body)?;
    Ok(Request {
        method,
        path,
        headers,
        body,
    })
}
