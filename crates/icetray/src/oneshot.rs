//! The one-shot subcommands: `doctor`, `info`, and `get`, each opening
//! tables through `icetray_core::open` exactly as `serve` does and printing
//! JSON lines. They call no `retain`: the daemon owns the local directory's
//! housekeeping.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use futures::future::join_all;
use icetray_core::config::Config;
use icetray_core::{Opened, Serviceability, Table, diagnose, open, time};

use crate::error::{CliError, EXIT_FAILURE, EXIT_INEFFICIENT, EXIT_OK};
use crate::key_text;
use crate::output::{FileLine, FindingLine, GetLine, Kind, RefusedLine, SummaryLine, emit};

/// Opens every table of `config`, or the one named by `only`; no table is
/// `CliError::NoTableMatched`. Nothing is loaded.
pub async fn open_tables(config: &Config, only: Option<&str>) -> Result<Opened, CliError> {
    let opened = open(config, only).await?;
    if opened.tables().is_empty() {
        return Err(CliError::NoTableMatched {
            name: only.map(str::to_string),
        });
    }
    Ok(opened)
}

/// Loads every table concurrently; the outcomes in table order.
async fn load_all(opened: &Opened) -> Vec<(Arc<Table>, icetray_core::Result<()>)> {
    let tables = opened.tables();
    let outcomes = join_all(tables.iter().map(|table| table.load())).await;
    tables.iter().cloned().zip(outcomes).collect()
}

/// `doctor`: one `doctor` line per table with its findings and, with
/// `files`, its data files. A table that cannot be loaded is a `refused`
/// line, and so is one the load admits but whose full bucket check
/// (`Generation::verify_buckets`, which decodes and hashes every key and
/// which no load or refresh runs) finds a key outside its file's recorded
/// bucket or cannot finish: that table would answer a present key absent,
/// so it is a refusal, not a cost or advice (RFD 007). Every table is
/// opened with `strict` off, so a table `strict` would refuse for cost is
/// reported rather than refused (RFD 016). The exit code is `EXIT_FAILURE`
/// when any table was refused, else `EXIT_INEFFICIENT` when any serves at
/// a cost, else `EXIT_OK`.
pub async fn doctor(
    config: &Config,
    only: Option<&str>,
    files: bool,
) -> Result<ExitCode, CliError> {
    let mut lenient = config.clone();
    for table in &mut lenient.tables {
        table.strict = false;
    }
    let opened = open_tables(&lenient, only).await?;
    let mut refused = false;
    let mut inefficient = false;
    for (table, outcome) in load_all(&opened).await {
        match outcome {
            Ok(()) => {
                let generation = table.serving()?;
                if let Err(error) = generation.verify_buckets().await {
                    emit(&RefusedLine::new(table.name(), &error))?;
                    refused = true;
                    continue;
                }
                let report = diagnose(&generation);
                emit(&SummaryLine::new(Kind::Doctor, &report.summary))?;
                for finding in &report.findings {
                    emit(&FindingLine::new(table.name(), finding))?;
                }
                if files {
                    for file in &report.files {
                        emit(&FileLine::new(table.name(), file))?;
                    }
                }
                if report.summary.serviceability == Serviceability::Inefficient {
                    inefficient = true;
                }
            }
            Err(error) => {
                emit(&RefusedLine::new(table.name(), &error))?;
                refused = true;
            }
        }
    }
    let code = if refused {
        EXIT_FAILURE
    } else if inefficient {
        EXIT_INEFFICIENT
    } else {
        EXIT_OK
    };
    Ok(ExitCode::from(code))
}

/// `info`: one `table` line per table from the doctor's summary. A load
/// that fails ends the run before any line is printed.
pub async fn info(config: &Config, only: Option<&str>) -> Result<(), CliError> {
    let opened = open_tables(config, only).await?;
    let mut loaded = Vec::with_capacity(opened.tables().len());
    for (table, outcome) in load_all(&opened).await {
        outcome?;
        loaded.push(table);
    }
    for table in loaded {
        let generation = table.serving()?;
        emit(&SummaryLine::new(
            Kind::Table,
            &diagnose(&generation).summary,
        ))?;
    }
    Ok(())
}

/// `get`: one `get` line per key from the table named by `only`, or the
/// only configured table; several tables and no name is
/// `CliError::TableNameRequired`. Every key is parsed under the served
/// generation's key type before any lookup, so a key that does not parse
/// is a usage error before anything is read. Each line carries the GETs
/// the lookup issued and its elapsed time.
pub async fn get(config: &Config, only: Option<&str>, keys: &[String]) -> Result<(), CliError> {
    let opened = open_tables(config, only).await?;
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
    table.load().await?;
    let key_type = table.key_type()?;
    let parsed = keys
        .iter()
        .map(|text| key_text::parse(text, key_type))
        .collect::<Result<Vec<_>, _>>()?;
    let stats = opened.storage.stats();
    for key in &parsed {
        let before = stats.gets();
        let started = Instant::now();
        let lookup = table.get(key).await?;
        let elapsed = time::micros(started.elapsed());
        emit(&GetLine::new(
            table.name(),
            key,
            &lookup,
            stats.gets() - before,
            elapsed,
        ))?;
    }
    Ok(())
}
