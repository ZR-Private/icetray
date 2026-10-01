# Building and testing

`rust-toolchain.toml` pins the stable toolchain and the `rustfmt` and
`clippy` components; rustup installs it on first use. Building from source
needs no protobuf compiler on the host, since the build carries its own.

```sh
cargo build --release
cargo test --workspace
dev/ci-checks.sh
```

`dev/ci-checks.sh` is the lint gate: `cargo fmt --all --check` and `cargo
clippy --workspace --all-targets --locked -- -D warnings`. CI runs that
script and `cargo test --workspace --locked`, the same two commands a
contributor runs before a push.

Two suites are gated behind fixtures and refuse to run rather than report a
pass they did not earn; under a plain `cargo test` they show as ignored.
`dev/test-int.sh` runs the REST catalog tests against a catalog at
`ICETRAY_REST_URI` (default `http://127.0.0.1:8181`); `dev/runtime.sh up`
followed by `dev/rest-fixture.sh up` starts the reference catalog in a
container. `dev/test-golden.sh` runs the layout tests against the golden
tables `dev/golden-tables.py` writes, one contract-shaped table per key
type, one more in table format version 3 (the rest are version 2, the
writer's default), and the off-contract layouts whose cost and advice
findings the doctor reports:

```sh
dev/golden-tables.sh
ICETRAY_GOLDEN_WAREHOUSE=file://$PWD/target/golden-warehouse dev/test-golden.sh
```

The testkit also carries a scale probe, a program and not a test, that
writes 1,500 files under a temporary directory, loads them, times lookups
and a refresh, and prints what it measured:

```sh
cargo run -p icetray-testkit --example scale --release
```

`dev/golden-tables.sh` runs the recipe with Spark 4.1.3 and Iceberg 1.11.0
(`iceberg-spark-runtime-4.1_2.13`) in a container, so the host needs neither
Spark nor a Java runtime; the recipe itself runs anywhere Spark 4.1 has the
Iceberg runtime on its classpath and can write to any object store or local
directory. Spark 4.1 is the newest line Iceberg 1.11 ships a runtime for.
The REST suite runs against the `apache/iceberg-rest-fixture:1.10.1` image.
The recipe is also the worked example of the contract: the properties it
sets are the ones a table owner sets.
