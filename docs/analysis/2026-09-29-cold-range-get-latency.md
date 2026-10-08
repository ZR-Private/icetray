# Range-GET latency to an object not read recently

Measured 2026-09-28 and 2026-09-29 from a Kubernetes pod in us-west-2
against an S3 bucket in the same region. The earlier
[range-GET latency analysis](2026-09-10-s3-range-get-latency.md) read eleven
objects round-robin, 150 times each, so every read came about eleven
requests after the last read of the same object. A cold hit in icetray
reads a page of a file no lookup has touched lately, and in a table of
thousands of files that is the common case. Read that way, S3 answers three
to six times slower than the earlier numbers, and object size matters more.

## Method

- **Spread against revisited.** 20 KB range GETs from boto3, sequential:
  one read each from 300 different data files of 53 to 70 MB (a table's
  files, written and then read once by a load some hours before), against
  300 reads of one such file in a row.
- **By size, never read.** 50 objects of random bytes of each of 1, 16, 64,
  and 256 MiB, uploaded in one pass and left ten minutes. Then one
  60,000-byte range at a random offset from each object once, the sizes
  interleaved so drift touches them alike; then 200 reads of one object of
  each size in a row. boto3, sequential. Times run to the last byte of the
  body, since a lookup waits for its whole page.
- **icetray's own share.** A trace of `icetray get` over eight cold keys,
  with the HTTP client's pool events at trace level: each lookup takes an
  idle pooled connection, sends one GET, and returns the connection.
  Connection reuse over a cold pass: 225 new TCP connections for 5,182 GETs.

## Results

Milliseconds, p50 / p90 / p99.

| read | | |
|---|---|---|
| one read each, 300 data files | 92 / 125 / 190 | |
| 300 reads of one data file | 26 / 36 / 82 | |
| icetray cold hits over the same table, 8 in flight | 81 / 119 / 172 | the value-page GET and about 0.5 ms of icetray |

| never-read objects | 1 MiB | 16 MiB | 64 MiB | 256 MiB |
|---|---|---|---|---|
| one read each, 50 objects | 106 / 143 / 217 | 120 / 169 / 217 | 174 / 234 / 320 | 173 / 240 / 264 |
| 200 reads of one object | 29 / 41 / 94 | 26 / 32 / 93 | 27 / 53 / 122 | 32 / 106 / 168 |

## What this does and does not establish

It establishes, for this bucket, region, and two days:

- An object read moments before answers in about 26 to 32 ms at the median,
  whatever its size; an object read hours before, in about 90 ms; one never
  read since it was written, in 106 to 174 ms. Revisiting warms, contrary to
  the earlier analysis, whose revisits came about eleven requests apart and
  whose large objects stayed slow: reads of one object back to back are the
  warm case, and that pattern warmed the 256 MiB objects to 32 ms.
- Cold, size matters at a lower boundary than the earlier analysis found:
  64 MiB objects answer with the 256 MiB ones, about 55 ms slower at the
  median than 16 MiB ones. Where between 16 and 64 MiB the step falls is
  not measured.
- icetray adds about half a millisecond to a cold GET; the rest is S3.

It does not establish the mechanism, how long an object stays warm, or
whether an object's age or its last read sets how cold it is. The
never-read objects were minutes old; the data files, hours old and read
once. Fifty samples a size put the p99 on one or two observations.

## What the evidence implies

- Cold-hit budgets in-region are about 90 to 175 ms of S3 time at the
  median and 190 to 320 ms at p99, not the 25 to 30 ms the earlier analysis
  gave; that figure is the warm case, a file read within seconds.
- The file-size clause of RFD 002 (Serving layout) keeps its
  reason cold, and the cold boundary is lower: files of 64 MiB answer a
  cold read like files of 256 MiB. Whether to lower the target below 64
  MiB, against more files and more load round trips, is the open call.
- Warm and cold differ by three to six times, so a table's hit latency
  depends on how recently each file was read, not on its size alone: a
  per-table hedge window (RFD 013 (Tail latency)) sees a mixture, and its
  percentile moves with the table's read pattern.
- Every saving measured against a cold hit's cost grows three to six times:
  the decompressed page-cache level of RFD 006 (Lookup hot path) breaks even
  at fewer extra cold hits than its text states.
