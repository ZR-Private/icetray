# Building and testing

`rust-toolchain.toml` pins the stable toolchain and the `rustfmt` and
`clippy` components. The build carries its own protobuf compiler.

```sh
cargo build --release
cargo test --workspace
dev/ci-checks.sh
```

`dev/ci-checks.sh` is the lint gate: `cargo fmt --all --check` and `cargo
clippy --workspace --all-targets --locked -- -D warnings`. CI runs that
script and `cargo test --workspace --locked`.

## Gated suites

Two suites need fixtures and show as ignored under a plain `cargo test`.

- `dev/test-int.sh` runs the REST catalog tests against a catalog at
  `ICETRAY_REST_URI` (default `http://127.0.0.1:8181`). `dev/runtime.sh up`
  then `dev/rest-fixture.sh up` starts `apache/iceberg-rest-fixture:1.10.1`
  in a container.
- `dev/test-golden.sh` runs the layout tests against the golden tables
  `dev/golden-tables.py` writes: one contract-shaped table per key type,
  one in table format version 3 (the rest are version 2, the writer's
  default), and the off-contract layouts whose cost and advice findings
  the doctor reports.

```sh
dev/golden-tables.sh
ICETRAY_GOLDEN_WAREHOUSE=file://$PWD/target/golden-warehouse dev/test-golden.sh
```

`dev/golden-tables.sh` runs the recipe with Spark 4.1.3 and Iceberg 1.11.0
(`iceberg-spark-runtime-4.1_2.13`) in a container. The recipe runs anywhere
Spark 4.1 has the Iceberg runtime on its classpath and writes to any object
store or local directory.

## Scale probe

An example program that writes 1,500 files under a temporary directory,
loads them, and times lookups and a refresh:

```sh
cargo run -p icetray-testkit --example scale --release
```
