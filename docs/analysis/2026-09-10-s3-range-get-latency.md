# Small S3 objects answer a range GET three to four times faster than large ones

Measured 2026-09-10. For a 60 KB range read, objects of 64 MiB and below
answered in 25 to 30 ms at the median from the same region, and objects of
256 MiB and above in 80 to 100 ms. From a laptop in another city the
ordering was the same and the ratio about two, because fixed network latency
dilutes the gap. Multipart objects were a few milliseconds slower than
single-part objects of the same size, small against the size effect, so the
upload method is not what separates the size classes. What file size a
serving table should target will be decided in RFD 002 (Serving layout
contract), which will cite this document as its evidence.

## Why it was measured

Cold hits (a lookup that finds its key and fetches one Parquet page from S3)
in the prototype differed by table, all read from the same laptop with one
GET per hit: about 60 ms at the median on a table with 20 MB data files and
about 120 ms on one with 80 MB files, both after a credential-caching fix;
about 230 ms on one with 549 MB files, measured before that fix, of which
roughly 80 ms was credential overhead
([prototype findings, finding 4](2026-09-10-prototype-findings.md#4-lookup-latency)).
The reader opened no connections during lookups, so the gap was S3's. A raw
HTTP check from the laptop, recorded only here: twelve 60 KB range reads per
file over one reused connection gave first bytes at 52 to 63 ms for a 20 MB
file, 50 to 125 ms (median about 80) for the 80 MB file, and 125 to 200 ms
for the 549 MB file, and reading an object in full beforehand did not change
its first-byte time. This measurement takes the reader and the connection
setup out of the picture and adds an in-region vantage point.

## Method

- **Objects.** Eight synthetic objects of random bytes, 16, 64, 256, and
  512 MiB, each uploaded once as a single part and once through the
  multipart API with 64 MiB parts. At 16 and 64 MiB the multipart upload
  therefore has one part; the 256 and 512 MiB objects have 4 and 8. All
  under one prefix in one bucket. Plus one real Parquet data file from each
  of three prototype tables: 1.1 MB (a small file from an append), 80 MB,
  and 549 MB (decimal, as S3 reports them; 524 MiB).
- **Requests.** 60,000-byte range GETs at random offsets (seeded), one
  reused HTTPS connection for the whole run, objects visited round-robin so
  that drift affects all of them equally. Time from sending the request to
  reading the first byte of the body.
- **Vantage points.** A notebook pod in the same AWS region as the bucket,
  150 rounds per object. A laptop over a residential connection in another
  city, 60 rounds per object.
- **Tooling.** The script below over the standard library's HTTP client and
  presigned URLs; no SDK on the request path. Raw per-request samples were
  not preserved; the percentiles are as printed.

```python
import http.client, random, sys, time
from urllib.parse import urlparse
# input: one line per object, tab-separated: name, size in bytes, presigned URL
objects = {}
for line in open(sys.argv[1]):
    name, size, url = line.rstrip("\n").split("\t")
    objects[name] = (url, int(size))
rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 60
rng = random.Random(1)
host = urlparse(next(iter(objects.values()))[0]).netloc
conn = http.client.HTTPSConnection(host, timeout=60)
ms = {name: [] for name in objects}
span = 60_000
for _ in range(rounds):
    for name, (url, size) in objects.items():
        off = rng.randrange(0, size - span)
        path = url[url.index(host) + len(host):]
        t0 = time.perf_counter()
        conn.request("GET", path, headers={"Range": f"bytes={off}-{off + span - 1}"})
        r = conn.getresponse(); r.read(1)
        ms[name].append((time.perf_counter() - t0) * 1000)
        r.read()
for name, v in ms.items():
    v.sort(); n = len(v)
    print(f"{name:42s} n={n} p50={v[n//2]:6.1f} p90={v[int(n*.9)]:6.1f} p99={v[int(n*.99)]:6.1f} max={v[-1]:6.1f} ms")
```

## Results

Milliseconds to first byte.

| object | parts | in-region p50 | p90 | p99 | laptop p50 | p90 | p99 |
|---|---|---|---|---|---|---|---|
| 16 MiB synthetic | 1 | 25.1 | 36.7 | 157.1 | 59.4 | 64.9 | 131.8 |
| 16 MiB synthetic | 1 (multipart API) | 26.1 | 35.9 | 55.7 | 57.5 | 67.1 | 199.3 |
| 64 MiB synthetic | 1 | 28.7 | 79.2 | 261.5 | 65.5 | 130.0 | 154.0 |
| 64 MiB synthetic | 1 (multipart API) | 28.7 | 72.0 | 130.7 | 71.0 | 146.4 | 176.5 |
| 256 MiB synthetic | 1 | 81.2 | 123.2 | 150.8 | 118.0 | 147.6 | 203.6 |
| 256 MiB synthetic | 4 | 88.9 | 129.6 | 168.7 | 119.7 | 151.4 | 208.5 |
| 512 MiB synthetic | 1 | 93.1 | 116.4 | 156.0 | 123.4 | 151.2 | 244.8 |
| 512 MiB synthetic | 8 | 100.9 | 133.1 | 193.2 | 134.7 | 166.3 | 196.2 |
| 1.1 MB data file | 1 | 25.8 | 36.7 | 59.2 | 57.1 | 67.1 | 77.6 |
| 80 MB data file | not recorded | 36.1 | 99.2 | 140.6 | 99.9 | 141.3 | 163.3 |
| 549 MB data file | not recorded | 100.6 | 134.7 | 184.1 | 131.2 | 171.6 | 401.9 |

With 60 rounds the laptop p99 is the largest single sample; with 150 rounds
the in-region p99 is the second-largest. Both columns are one or two
observations each.

Observations:

- In-region, objects of 64 MiB and below answer in 25 to 29 ms at the
  median; objects of 256 MiB and above take 81 to 101 ms, three to four
  times longer.
- From the laptop the ordering holds and the ratio is about two (58 to 71 ms
  against 118 to 135 ms). The laptop adds roughly 30 to 40 ms to every
  synthetic object and to the 1.1 MB and 549 MB data files, and about 64 ms
  to the 80 MB data file, whose tail is wide from both vantage points.
- At 256 and 512 MiB, the only sizes where the upload method actually
  differed, the multipart object was slower in 11 of 12 percentile cells, by
  about 8 ms at the in-region median. That gap is small next to the 55 to
  75 ms size effect, so the upload method is not what separates the size
  classes.
- The 64 MiB objects already show the slow behavior in their tail: an
  in-region p90 of 72 to 79 ms against a p50 of 29 ms. The 16 MiB objects
  do not (p90 36 to 37 ms against a p50 of 25 to 26 ms).
- The real data files fall where their sizes predict: the 1.1 MB file with
  the 16 MiB synthetics, the 549 MB file with the 512 MiB ones, the 80 MB
  file in between with a wide tail.
- Revisiting an object did not warm anything: each object was read 60 to
  150 times over the run without the large ones speeding up (and in the
  earlier laptop check above, a full read beforehand did not change the
  first-byte time either).

## What this does and does not establish

It establishes that, for this bucket, region, and day, first-byte latency for
a small range read depends on object size, with a boundary somewhere between
64 and 256 MiB, and that the upload method is not the cause. The 64 MiB p90
suggests the slow mode may begin near the low end of that band. It does not
establish the mechanism. S3 does not document one; internal placement or
indexing that differs by object size is a guess.

## What the evidence implies

- If serving tables target data files of 64 MiB or less, cold-hit budgets
  for in-region serving can assume about 25 to 30 ms of S3 first-byte time
  per GET at the median, with a p90 near 75 ms at the 64 MiB end of that
  range, plus the reader's own work (well under a millisecond, prototype
  findings finding 4) and transfer of one page.
- Any advisory threshold on data file size sits in the unmeasured 64 to
  256 MiB band. Where in it is a judgment the RFD makes.
- Smaller files have costs the decision has to weigh, none of them measured
  here:
  - **Load requests.** Load (reading each data file's footer and key column
    chunks to build the in-memory index the server keeps per file) is two
    GETs per file regardless of size
    ([prototype findings, finding 3](2026-09-10-prototype-findings.md#3-load-time)).
    Against Iceberg's default 512 MB target, files of 64 or 32 MiB mean 8 or
    16 times the file count and therefore the load requests. Estimate:
    2 GETs times files times 25 to 30 ms, divided by the load concurrency. The prototype loaded 1,500 small files in 83 ms
    from local disk (release build) with nothing quadratic in the count;
    the S3 wall time at that count was not measured.
  - **Index bytes per key** (the size of that in-memory index). Footers are
    a fixed cost per file; at 1,000
    rows per file they dominated (6.6 bytes per key against 1.2 to 1.8 at
    hundreds of thousands of rows per file,
    [prototype findings, finding 2](2026-09-10-prototype-findings.md#2-resident-index-size)).
    At tens of MiB per file with small values, rows per file stay in the
    hundreds of thousands and the overhead is small; not measured at that
    size.
  - **Compaction.** Iceberg's file rewrite procedures target their own file
    size unless told otherwise, so a serving table's compaction has to carry
    the same target or it undoes the layout. Known behavior, not measured.
  - **Manifests and request charges.** Manifest entries and per-GET charges
    both scale with file count and request count. Small, not measured.
  - **A floor.** Nothing here argues against files smaller than 16 MiB on
    latency grounds; the argument for a floor is the load and index cost
    above.

## Not measured, and what would settle it

- Where between 64 and 256 MiB the slow mode begins: bisect with objects at
  96, 128, 160, 192 MiB.
- Stability over hours and days, and from a pod in the serving cluster
  rather than a notebook: run the same script on a schedule from both.
- Other regions, and S3 Express One Zone, which has a different placement
  model and may not show the effect: repeat the run there.
