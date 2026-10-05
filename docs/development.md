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

## The development environment

`.flox/env/manifest.toml` declares an optional [flox](https://flox.dev)
environment. `flox activate` puts on PATH rustup, which installs the
pinned toolchain; colima, the docker client, and docker-compose, which
`dev/runtime.sh` drives for the container-gated suites and the golden
tables; grpcurl, for calls to a running server; and ghz and samply, which
the performance sweep uses to load the gRPC server and profile it.
Nothing in CI, the hooks, or the `dev/` scripts needs it, and any of the
same tools installed another way work as well.

```sh
flox activate -- cargo test --workspace
flox activate -- grpcurl -plaintext -proto crates/icetray-proto/proto/icetray.proto \
  -d '{"table": "long_2m"}' 127.0.0.1:7433 icetray.v1.Icetray/TableInfo
```

The server offers no gRPC reflection, so grpcurl and ghz are given the
proto file (`-proto` for grpcurl, `--proto` for ghz).

## Platform notes

On Linux, the tests of `disk` residency need their temporary directory on
a filesystem that serves non-blocking reads of cached pages, such as ext4,
xfs, or btrfs. Where `/tmp` is tmpfs or overlayfs, as on some
distributions and in most containers, point `TMPDIR` at a directory on
disk.

## Gated suites

Two suites need fixtures and show as ignored under a plain `cargo test`.

- `dev/test-int.sh` runs the REST catalog tests against a catalog at
  `ICETRAY_REST_URI` (default `http://127.0.0.1:8181`). `dev/runtime.sh up`
  then `dev/rest-fixture.sh up` starts `apache/iceberg-rest-fixture:1.10.1`
  in a container.
- `dev/test-golden.sh` runs the layout tests against the golden tables
  `dev/golden-tables.py` writes: one contract-shaped table per key type,
  one in table format version 3 (the rest are version 2, the writer's
  default), and the off-contract layouts whose inefficient and advice findings
  the doctor reports.

```sh
dev/golden-tables.sh
ICETRAY_GOLDEN_WAREHOUSE=file://$PWD/target/golden-warehouse dev/test-golden.sh
```

`dev/golden-tables.sh` runs the recipe with Spark 4.1.3 and Iceberg 1.11.0
(`iceberg-spark-runtime-4.1_2.13`) in the `apache/spark:4.1.3-python3`
image, which carries Java 21, so the host needs a container runtime
(`dev/runtime.sh up`) and `python3`. `--only contract` or `--only
off-contract` writes one group, and `--only` with a table's name writes
that table alone: `--only long_2m` writes about 80 MB. The recipe also
runs on the host with PySpark 4.1 and Java 17 or later, and writes to any
object store or local directory:

```sh
pip install 'pyspark==4.1.*'
python3 dev/golden-tables.py --warehouse "file://$PWD/target/golden-warehouse"
```

The first run of either downloads the Iceberg runtime from Maven Central.
Both groups together are about 4.3 GB, most of it the two tables of 16 KiB
values. The tables are also the quickest way to try the binary:
`icetray get --location target/golden-warehouse/db/long_2m 42` answers
from one of them, and the README's
[Try it locally](../README.md#try-it-locally) walks through `get`,
`doctor`, and `serve`.

## Scale probe

An example program that writes 1,500 files under a temporary directory,
loads them, and times lookups and a refresh:

```sh
cargo run -p icetray-testkit --example scale --release
```

## Releases

A release is a tag. Set `version` under `[workspace.package]` in
`Cargo.toml`, commit, then tag that commit with the same version and push
the tag:

```sh
git tag v0.1.0
git push origin v0.1.0
```

The release workflow (`.github/workflows/release.yml`) refuses a tag that
differs from the crate version. It builds `icetray` with `--release
--locked` for `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`,
each on Ubuntu 24.04, so the binaries run on that release's glibc or
newer, and publishes a GitHub Release with one archive per target and its
SHA-256 sum. Each archive holds the binary, the README, and both licenses.
It also pushes a container image for both architectures to the GitHub
Container Registry, `ghcr.io/<owner>/icetray:<version>` without the `v`,
built from the same binaries on Ubuntu 24.04 by the `Dockerfile`. Run by
hand from the Actions tab, the workflow builds the same archives as run
artifacts and the same image, and publishes nothing.
