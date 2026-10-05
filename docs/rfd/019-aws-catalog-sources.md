# AWS catalog sources

## Problem

A table's source tells icetray where to find the table's current metadata
file. Two kinds exist: a static table root, followed through its version
hint, and an Iceberg REST catalog. That covers tables written through a
file-system catalog, but most production lakes keep their tables in a
catalog service. On AWS that service is Glue or S3 Tables, both
authenticated with AWS credentials, and icetray can reach neither. A
deployer whose tables live there can pin a metadata file, which never
follows new commits, or cannot serve the table at all.

The way in has to respect RFD 003 (One storage stack): every byte icetray
reads goes through one storage layer with one credential surface, the AWS
SDK's default chain. A catalog may tell icetray where the metadata is. It
may not read it.

## Decision

Tables in Glue and S3 Tables will be resolved to their current metadata
location by asking the catalog directly through the AWS SDK, with the same
credentials storage uses, and their metadata will be read through the one
storage layer like any other table's.

Two source kinds will join `static` and `rest`:

- `glue`: a database and a table. The source asks Glue for the table and
  takes the location from its `metadata_location` parameter, refusing a
  table that is not an Iceberg table. An optional catalog id reaches
  another account's catalog, and optional region and endpoint settings
  override the defaults.
- `s3tables`: a table bucket ARN, a namespace, and a table. The source asks
  S3 Tables for the table's metadata location.

Credentials are the ones storage already has: the SDK's default chain, or
the static keys in `storage.s3` when they are set. Catalog and storage
resolve them once and share the cache RFD 003 requires, so a catalog call
never spawns a credential helper of its own.

Both kinds will refresh the way every source does (RFD 005, Generations and
refresh): each poll makes one catalog call, compares the location with the
one the table holds, and stops when they match. A catalog call is bounded
by `storage.catalog_timeout`.

## Alternatives Considered

**The iceberg-rs Glue and S3 Tables catalog clients as they come.** They
exist at the version icetray uses, but loading a table through them reads
the metadata file through the library's own file I/O, over a second S3
client with its own credentials. That is the two-stack design RFD 003
rejected. Each catalog's table API is one call that returns the location,
so asking it directly is simpler and keeps one stack.

**Glue's and S3 Tables' Iceberg REST endpoints, through the REST source.**
These endpoints authenticate with AWS request signing, which the REST
client does not do. Adding it would put a credential bridge inside another
library's HTTP client, a second home for AWS credentials.

**Authentication for REST catalogs in the same change.** Catalogs such as
Polaris, Unity, Lakekeeper, and Nessie take OAuth2 client credentials or a
bearer token. The REST client already implements the standard Iceberg REST
properties for both, so supporting them would mean passing those properties
through, reading secret values from the environment or a file rather than
the configuration file, and renewing OAuth2 tokens, which the client keeps
until they expire. It waits because Glue and S3 Tables cover the AWS
deployments this RFD addresses, and because it adds the one configuration
section icetray could not validate. It is additive when someone needs it. Until then the REST source
serves catalogs that need no credentials, or that sit behind a proxy that
adds them.

**Hive Metastore and other catalog protocols.** Left out until someone
needs one.

## Consequences

icetray will gain the AWS SDK's Glue and S3 Tables clients as
dependencies. Each is used for one call, and both share the SDK
configuration icetray already resolves. A deployer will grant
`glue:GetTable` or `s3tables:GetTableMetadataLocation` beside the S3 read
permissions, and the operating docs will list them once the sources are
built.

A source is part of a table's entry, so moving a table between sources
applies on reload under RFD 018 (Configuration reload).

The REST source sends no credentials, and the documentation says so where
the source is described.

Credentials vended by a catalog stay out of scope. The open question RFD
003 records is unchanged.

RFD 011 (Configuration) lists a table's source among its settings; its
list of source kinds grows to four when this is built.
