//! The one-shot subcommands: `doctor`, `info`, and `get`, each opening
//! tables through `icetray_core::open` exactly as `serve` does and printing
//! JSON lines. They call no `retain`: the daemon owns the local directory's
//! housekeeping.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use futures::future::join_all;
use icetray_core::config::{Config, TableConfig};
use icetray_core::{FindingLog, Opened, Table, diagnose, judge, open, time};

use crate::error::{CliError, EXIT_BELOW_LEVEL, EXIT_FAILURE, EXIT_OK};
use crate::key_text;
use crate::output::{FileLine, FindingLine, LookupLine, RefusalLine, SummaryLine, emit};

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
/// so it is a refusal, not a cap or advice (RFD 007). Every table is
/// loaded with `strict` off, so its findings are reported whatever its
/// setting, diagnosed against its configured `lookups`, and then judged by
/// `judge` under its configured `strict`: a table below its level that
/// `serve` would refuse gets a `refusal` line after its findings (RFD 016,
/// RFD 020). A `--location` table declares its residency's best level and
/// is never strict (`Target::config`). The findings are the run's output,
/// so the load logs them at debug alone. The exit code is `EXIT_FAILURE`
/// when any table was refused, else `EXIT_BELOW_LEVEL` when any is below
/// its declared level, else `EXIT_OK`.
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
                let report = diagnose(&generation, configured.lookups());
                emit(&SummaryLine::new(&report.summary))?;
                for finding in &report.findings {
                    emit(&FindingLine::new(table.name(), finding))?;
                }
                if files {
                    for file in &report.files {
                        emit(&FileLine::new(table.name(), file))?;
                    }
                }
                let summary = &report.summary;
                if let Err(error) = judge(
                    table.name(),
                    summary.level,
                    &report.capping_codes(),
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
        let report = diagnose(&generation, table.config().lookups());
        emit(&SummaryLine::new(&report.summary))?;
    }
    Ok(())
}

/// `get`: one `lookup` line per key from the table named by `only`, or the
/// only configured table; several tables and no name is
/// `CliError::TableNameRequired`. Every key is parsed under the key type
/// of the snapshot the ref names, read from the table's metadata before
/// any data file (`Table::resolve_key_type`), so a key that does not parse
/// is a usage error before the load, which on a large table takes minutes.
/// Each line carries the GETs the lookup issued and its elapsed time.
pub async fn get(config: &Config, only: Option<&str>, keys: &[String]) -> Result<(), CliError> {
    let opened = open_tables(config, only, FindingLog::Events).await?;
    let tables = opened.tables();
    let table = match &tables[..] {
        [table] => table,
        tables => {
            return Err(CliError::TableNameRequired {
                tables: tables
                    .iter()
                    .map(|table| table.name().to_string())
                    .collect(),
            });
        }
    };
    let key_type = table.resolve_key_type().await?;
    let parsed = keys
        .iter()
        .map(|text| key_text::parse(text, key_type))
        .collect::<Result<Vec<_>, _>>()?;
    table.load().await?;
    let stats = opened.storage.stats();
    for key in &parsed {
        let before = stats.gets();
        let started = Instant::now();
        let lookup = table.get(key).await?;
        let elapsed = time::micros(started.elapsed());
        emit(&LookupLine::new(
            table.name(),
            key,
            &lookup,
            stats.gets() - before,
            elapsed,
        ))?;
    }
    Ok(())
}
