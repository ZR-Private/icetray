//! The one-shot subcommands: `doctor`, `info`, `get`, and `scan`, each
//! opening tables through `icetray_core::open` exactly as `serve` does and
//! printing JSON lines. They call no `retain`: the daemon owns the local
//! directory's housekeeping.

use std::fmt;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use futures::future::join_all;
use icetray_core::config::{Config, TableConfig};
use icetray_core::{
    BoundSide, ColumnRead, FindingLog, Opened, ScanBounds, Table, TableKind, TableLayout,
    check_scan_request, diagnose, examine, judge, open, time,
};

use crate::cli::{Columns, ScanArgs};
use crate::error::{CliError, EXIT_BELOW_LEVEL, EXIT_FAILURE, EXIT_OK};
use crate::output::{
    FileLine, FindingLine, LookupLine, RefusalLine, RowLine, ScanLine, SummaryLine, UnwalkedLine,
    emit, file_orders, scan_orders, warn_if_inexact_order,
};
use crate::{key_text, order_text};

/// A one-shot that reads one table: `get`, of a key-value table, or
/// `scan`, of an ordered one. Display gives the command's name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reader {
    /// `get`.
    Get,
    /// `scan`.
    Scan,
}

impl Reader {
    /// The kind of table the command reads.
    pub fn reads(self) -> TableKind {
        match self {
            Self::Get => TableKind::KeyValue,
            Self::Scan => TableKind::Ordered,
        }
    }

    /// The command that reads a table of `kind`.
    pub fn of(kind: TableKind) -> Self {
        match kind {
            TableKind::KeyValue => Self::Get,
            TableKind::Ordered => Self::Scan,
        }
    }
}

impl fmt::Display for Reader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Get => "get",
            Self::Scan => "scan",
        })
    }
}

/// Opens every table of `config`, or the one named by `only`, logging the
/// doctor's findings as `findings` says; a name no
/// table is configured under is `CliError::NoTableMatched`. `config` is
/// validated, so it names at least one table. Nothing is loaded.
pub async fn open_tables(
    config: &Config,
    only: Option<&str>,
    findings: FindingLog,
) -> Result<Opened, CliError> {
    let opened = open(config, only, findings).await?;
    match only {
        Some(name) if opened.tables().is_empty() => Err(CliError::NoTableMatched {
            name: name.to_string(),
            tables: config
                .tables
                .iter()
                .map(|table| table.name.clone())
                .collect(),
        }),
        Some(_) | None => Ok(opened),
    }
}

/// Loads every table concurrently; the outcomes in table order.
async fn load_all(opened: &Opened) -> Vec<(Arc<Table>, icetray_core::Result<()>)> {
    let tables = opened.tables();
    let outcomes = join_all(tables.iter().map(|table| table.load())).await;
    tables.iter().cloned().zip(outcomes).collect()
}

/// `doctor`: one `table` line per table with its findings and, with
/// `files`, its data files. A table that cannot be loaded is a `refusal`
/// line, and so is one the load admits but whose full bucket check
/// (`Generation::verify_buckets`, which decodes and hashes every key and
/// which no load or refresh runs) finds a key outside its file's recorded
/// bucket or cannot finish: that table would answer a present key absent,
/// so it is a refusal, not a cap or advice (RFD 007). The report is
/// `examine`'s, which walks each protobuf column's default and a value
/// page of it (RFD 024); a column whose page could not be read is an
/// `unwalked` line after the findings, and the table's report and
/// judgement stand without it. Every table is
/// loaded with `strict` off, so its findings are reported whatever its
/// setting, diagnosed against its configured `lookups`, and then judged by
/// `judge` under its configured `strict`: a table below its level that
/// `serve` would refuse gets a `refusal` line after its findings (RFD 016,
/// RFD 020). A `--location` table declares its residency's best level and
/// is never strict (`Target::config`). The findings are the run's output,
/// so the load logs them at debug alone. The exit code is `EXIT_FAILURE`
/// when any table was refused, else `EXIT_BELOW_LEVEL` when any is below
/// its declared level, else `EXIT_OK`. With `files`, a table whose `file`
/// lines carry a long order value past 2^53 also gets one warning on
/// stderr (`warn_if_inexact_order`); stdout is unchanged.
pub async fn doctor(
    config: &Config,
    only: Option<&str>,
    files: bool,
) -> Result<ExitCode, CliError> {
    let mut lenient = config.clone();
    for table in &mut lenient.tables {
        table.strict = false;
    }
    let opened = open_tables(&lenient, only, FindingLog::Debug).await?;
    let mut refused = false;
    let mut below_level = false;
    for (table, outcome) in load_all(&opened).await {
        match outcome {
            Ok(()) => {
                let generation = table.serving()?;
                if let Err(error) = generation.verify_buckets().await {
                    emit(&RefusalLine::new(table.name(), &error))?;
                    refused = true;
                    continue;
                }
                let configured = configured(config, table.name());
                let examined = examine(
                    &generation,
                    configured.lookups(),
                    &configured.formats(),
                    configured.scan_limits(
                        config.server.max_scan_bytes,
                        config.server.max_multi_scan_values,
                    ),
                )
                .await;
                let report = &examined.report;
                emit(&SummaryLine::new(&report.summary))?;
                for finding in &report.findings {
                    emit(&FindingLine::new(table.name(), finding))?;
                }
                for unwalked in &examined.unwalked {
                    emit(&UnwalkedLine::new(table.name(), unwalked))?;
                }
                if files {
                    for file in &report.files {
                        emit(&FileLine::new(table.name(), file))?;
                    }
                    warn_if_inexact_order(table.name(), file_orders(&report.files));
                }
                let summary = &report.summary;
                if let Err(error) = judge(
                    table.name(),
                    summary.level,
                    &summary.column_levels(),
                    &report.capping(),
                    summary.lookups,
                    configured.strict,
                ) {
                    emit(&RefusalLine::new(table.name(), &error))?;
                    refused = true;
                } else if !summary.level.meets(summary.lookups) {
                    below_level = true;
                }
            }
            Err(error) => {
                emit(&RefusalLine::new(table.name(), &error))?;
                refused = true;
            }
        }
    }
    let code = if refused {
        EXIT_FAILURE
    } else if below_level {
        EXIT_BELOW_LEVEL
    } else {
        EXIT_OK
    };
    Ok(ExitCode::from(code))
}

/// The entry `config` gives the table named `name`, which `open` opened
/// from it, so the name is configured.
fn configured<'a>(config: &'a Config, name: &str) -> &'a TableConfig {
    config
        .tables
        .iter()
        .find(|table| table.name == name)
        .expect("every opened table is configured")
}

/// `info`: one `table` line per table from the doctor's summary. A load
/// that fails ends the run before any line is printed.
pub async fn info(config: &Config, only: Option<&str>) -> Result<(), CliError> {
    let opened = open_tables(config, only, FindingLog::Events).await?;
    let mut loaded = Vec::with_capacity(opened.tables().len());
    for (table, outcome) in load_all(&opened).await {
        outcome?;
        loaded.push(table);
    }
    for table in loaded {
        let generation = table.serving()?;
        let entry = table.config();
        let report = diagnose(
            &generation,
            entry.lookups(),
            &entry.formats(),
            entry.scan_limits(
                config.server.max_scan_bytes,
                config.server.max_multi_scan_values,
            ),
        );
        emit(&SummaryLine::new(&report.summary))?;
    }
    Ok(())
}

/// The one table of `opened` that `command` reads: the one named by
/// `--table`, which `open_tables` already selected, or the only configured
/// one; several and no name is `CliError::TableNameRequired`. A table of
/// the kind the command does not read is `CliError::WrongCommand`, found
/// from its entry's `order_column` before anything is read. Its columns,
/// `columns`, are checked against its configured ones and their formats
/// (`TableConfig::check_column_request`), so a column it does not serve,
/// one named twice, or one read with fields its format does not take is
/// `CliError::Request`, also before anything is read.
fn one_table(
    opened: &Opened,
    command: Reader,
    columns: &[ColumnRead<'_>],
) -> Result<Arc<Table>, CliError> {
    let tables = opened.tables();
    let table = match &tables[..] {
        [table] => Arc::clone(table),
        tables => {
            return Err(CliError::TableNameRequired {
                command,
                tables: tables
                    .iter()
                    .map(|table| table.name().to_string())
                    .collect(),
            });
        }
    };
    let kind = table.kind();
    if kind != command.reads() {
        return Err(CliError::WrongCommand {
            table: table.name().to_string(),
            kind,
            command,
        });
    }
    table
        .config()
        .check_column_request(columns)
        .map_err(CliError::Request)?;
    Ok(table)
}

/// The reads `columns` name, whole or trimmed to the fields each names.
fn reads(columns: &Columns) -> Vec<ColumnRead<'_>> {
    columns
        .columns
        .iter()
        .map(|column| ColumnRead {
            name: &column.name,
            fields: &column.fields,
        })
        .collect()
}

/// The names `columns` reads, in the order given, which a line keys its
/// values by.
fn names(columns: &Columns) -> Vec<&str> {
    columns
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect()
}

/// `get`: one `lookup` line per key from the table `one_table` picks, each
/// reading the value columns `columns` names, whole or trimmed to the
/// fields each names (RFD 024), none for presence alone (RFD 022). Every
/// key is parsed under the key type of the snapshot the ref names, read
/// from the table's metadata before any data file
/// (`Table::resolve_layout`), so an ordered table, a column `one_table`
/// refuses, and a key that does not parse are usage errors before the
/// load, which on a large table takes minutes. Each line carries the GETs
/// the lookup issued and its elapsed time.
pub async fn get(
    config: &Config,
    only: Option<&str>,
    columns: &Columns,
    keys: &[String],
) -> Result<(), CliError> {
    let opened = open_tables(config, only, FindingLog::Events).await?;
    let reads = reads(columns);
    let names = names(columns);
    let table = one_table(&opened, Reader::Get, &reads)?;
    let key_type = table.resolve_layout().await?.key_type;
    let parsed = keys
        .iter()
        .map(|text| key_text::parse(text, key_type))
        .collect::<Result<Vec<_>, _>>()?;
    table.load().await?;
    let stats = opened.storage.stats();
    for key in &parsed {
        let before = stats.gets();
        let started = Instant::now();
        let lookup = table.get(key, &reads).await?;
        let elapsed = time::micros(started.elapsed());
        emit(&LookupLine::new(
            table.name(),
            key,
            &names,
            &lookup,
            stats.gets() - before,
            elapsed,
        ))?;
    }
    Ok(())
}

/// `scan`: one Scan of the request's key on the table `one_table` picks
/// (RFD 025), printed as one `row` line per row in the direction asked and
/// then one `scan` line with the rows returned, the next page's bound, the
/// GETs the Scan issued, and its elapsed time; a long order value past
/// 2^53 on any of them gets one warning on stderr
/// (`warn_if_inexact_order`). The key is parsed under the key type and the
/// bounds under the order type of the snapshot the ref names, read from the
/// table's metadata before any data file (`Table::resolve_layout`), and the
/// request is held to core's checks of a Scan request
/// (`check_scan_request`, `Table::check_scan`), the ones the gRPC service
/// runs before it admits a call; so a key-value table, a column
/// `one_table` refuses, a key or a bound that does not parse, a limit of 0
/// or above `max_scan_rows`, and an empty window are usage errors before
/// the load. The Scan itself runs them again through `Table::scan`, with
/// the checks against the loaded generation.
pub async fn scan(config: &Config, only: Option<&str>, request: &ScanArgs) -> Result<(), CliError> {
    let opened = open_tables(config, only, FindingLog::Events).await?;
    let reads = reads(&request.columns);
    let names = names(&request.columns);
    let table = one_table(&opened, Reader::Scan, &reads)?;
    let layout = table.resolve_layout().await?;
    let key = key_text::parse(&request.key, layout.key_type)?;
    let bounds = scan_bounds(request, &layout)?;
    check_scan_request(table.name(), &reads, &bounds).map_err(CliError::Request)?;
    table.check_scan(&bounds).map_err(CliError::Request)?;
    table.load().await?;
    let stats = opened.storage.stats();
    let before = stats.gets();
    let started = Instant::now();
    let lookup = table.scan(&key, &reads, &bounds).await?;
    let elapsed = time::micros(started.elapsed());
    for row in &lookup.scanned.rows {
        emit(&RowLine::new(table.name(), &key, &names, row))?;
    }
    emit(&ScanLine::new(
        table.name(),
        &key,
        &lookup,
        stats.gets() - before,
        elapsed,
    ))?;
    warn_if_inexact_order(table.name(), scan_orders(&lookup));
    Ok(())
}

/// The bounds `request` asks for, its `--start` and `--before` parsed under
/// the order type of `layout`, an ordered table's.
fn scan_bounds(request: &ScanArgs, layout: &TableLayout) -> Result<ScanBounds, CliError> {
    let order_type = layout
        .order
        .as_ref()
        .expect("the layout of a table whose entry names an order column has one")
        .order_type;
    let bound = |text: &Option<String>, side| {
        text.as_deref()
            .map(|text| order_text::parse(text, order_type, side))
            .transpose()
    };
    Ok(ScanBounds {
        direction: request.direction,
        limit: request.limit,
        start: bound(&request.start, BoundSide::Start)?,
        before: bound(&request.before, BoundSide::Before)?,
    })
}
