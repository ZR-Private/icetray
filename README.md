# icetray

Batch-frozen data, served one cube at a time.

icetray is a read-only key-value serving layer over Apache Iceberg: point
lookups against sorted, bucket-partitioned Parquet on object storage, with
snapshot pinning for atomic dataset swaps. Build a dataset in Spark, commit
a snapshot, and serve get-by-key straight off the lake — no bulk load into
a database, no write path, no compaction.

The lineage: [ElephantDB](https://github.com/nathanmarz/elephantdb) proved
the batch-build-then-serve shape; Spotify's Random Access Parquet showed it
works directly against lake files. icetray is that idea on stock Iceberg
tables — the bucket transform routes a key to its partition by arithmetic,
the Parquet page index finds the row, and every byte below a snapshot is
immutable, so caches never invalidate.

Status: design recorded, implementation starting. Nothing to run yet.

The decisions are in [docs/rfd/](docs/rfd/), one per file, starting with
[how to RFD](docs/rfd/001-how-to-rfd.md). The measurements behind them are in
[docs/analysis/](docs/analysis/), and the build they describe is planned in
[docs/plans/](docs/plans/). Conventions are in
[CONTRIBUTING.md](CONTRIBUTING.md). Agents read [AGENTS.md](AGENTS.md).

## License

MIT or Apache-2.0, at your option.
