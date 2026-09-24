//! A generation (RFD 005, RFD 017): one snapshot of one table, every live
//! data file indexed, the files in a deterministic order, the overlap
//! groups among them checked for a key two files share, and the
//! resident-bytes budget checked at the footer stage before any key chunk
//! is fetched. Immutable once built; a `Table` builds the next one beside
//! it, reusing every file whose path is unchanged, and swaps atomically.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::stream::{self, StreamExt as _, TryStreamExt as _};
use iceberg::spec::Snapshot;

use crate::approx;
use crate::cache::{PageCache, PageOrigin};
use crate::catalog::{Resolved, TableFault};
use crate::config::{Residency, TableConfig};
use crate::error::{Error, RefusalReason, Result};
use crate::index::{self, FileIndex, Inspected, Keys, PageRef};
use crate::key::Key;
use crate::local::{DataFileIdentity, LocalDir, Reservation};
use crate::manifest::{self, Bucketing, DataFileEntry, TableLayout};
use crate::metric::LookupResult;
use crate::parquet_io::{FileHandle, Segments};
use crate::snapshot::SnapshotId;
use crate::storage::{HedgeWindow, Storage};
use crate::time;

/// Files worked on at once while a generation loads: footers loaded at the
/// footer stage, then chunks fetched and verified at the chunk stage, then
/// the key chunks of one overlap group gathered for its merge. Each file in
/// flight is one GET or local read waiting, and at the chunk stage one
/// file's key chunks in memory; sixteen keeps a load of hundreds of files a
/// few round trips deep without holding more than sixteen files' chunks
/// before they are kept or dropped.
const LOAD_CONCURRENCY: usize = 16;

/// One data file with its manifest entry and its resident index.
#[derive(Debug)]
pub struct LoadedFile {
    /// The file as its manifest recorded it.
    pub entry: DataFileEntry,
    /// The file's resident index.
    pub index: FileIndex,
}

/// One snapshot, fully indexed and ready to serve. `Send + Sync`: a
/// `Table` shares it across every lookup task.
#[derive(Debug)]
pub struct Generation {
    /// The table's configured name.
    pub table: String,
    /// The snapshot served.
    pub snapshot_id: SnapshotId,
    /// The metadata file the snapshot was read from; what the next poll
    /// compares against.
    pub metadata_location: String,
    /// The layout derived from the snapshot's schema.
    pub layout: TableLayout,
    /// Where every file's key chunks live.
    pub residency: Residency,
    /// Every live data file, sorted by (bucketing, bucket, exact first
    /// key, path).
    pub files: Vec<Arc<LoadedFile>>,
    /// Rows across every file, from the manifests.
    pub records: u64,
    /// Bytes resident across every file index.
    pub index_bytes: usize,
    /// When the load finished.
    pub loaded_at: Instant,
    /// The snapshot's own timestamp, when the writer committed it; what
    /// `generation_age_seconds` measures against now.
    pub snapshot_timestamp: jiff::Timestamp,
    /// Positions in `files`, in file order, per bucketing the files carry
    /// and per bucket under it; how a key finds its candidates.
    routes: Routes,
    /// Per bucket a member of an overlap group is in, the widest group
    /// touching it: how many files a lookup there may probe.
    overlapping: BTreeMap<u32, usize>,
    /// Overlap groups this load merged to prove key uniqueness.
    verified_groups: usize,
}

/// Positions in a generation's files by bucketing and bucket. `None`
/// bucketing files are all under `manifest`'s unbucketed bucket.
type Routes = BTreeMap<Option<Bucketing>, Buckets>;

/// Positions in a generation's files by bucket, in file order.
type Buckets = BTreeMap<u32, Vec<usize>>;

impl Generation {
    /// Loads `snapshot` of `table`. Derives the layout from the snapshot's
    /// schema, lists its live files, reuses from `previous` every file the
    /// snapshot still names by the same path at the same size (the same
    /// `Arc`, RFD 005; a data file is immutable, so a path whose size
    /// changed is another file and its index would be stale), opens a
    /// `FileHandle` for every other file with the manifest's size and
    /// `hedge`, inspects their footers `LOAD_CONCURRENCY` at a time,
    /// checks the budget `config` and `local` set before any chunk is
    /// fetched (Full: `config.resident_budget_bytes` against the reused
    /// files' resident bytes plus every new file's predicted full bytes;
    /// Disk: `local.index_budget()` against the bytes `index/` holds, what
    /// other tables' loads have reserved, and what every new file would
    /// write there, reserved until the new indexes are built; over either
    /// is `RefusalReason::OverBudget`), then builds the new indexes at the
    /// same concurrency and assembles the generation: files sorted, overlap
    /// groups found, and every group that gained a file merged to refuse a
    /// duplicate key. `Err` leaves nothing behind; the caller keeps serving
    /// `previous`. A snapshot timestamp jiff cannot represent is
    /// `TableFault::SnapshotTimestampOutOfRange`, raised before any read.
    pub async fn load(
        config: &TableConfig,
        resolved: &Resolved,
        snapshot: &Snapshot,
        storage: &Arc<Storage>,
        hedge: Arc<HedgeWindow>,
        local: Option<&LocalDir>,
        previous: Option<&Generation>,
    ) -> Result<Self> {
        let table = config.name.as_str();
        let snapshot_timestamp = snapshot_timestamp(table, snapshot)?;
        let layout = TableLayout::derive(
            table,
            &resolved.metadata,
            snapshot,
            &config.key_column,
            &config.value_column,
        )?;
        let entries =
            manifest::list_data_files(table, &resolved.metadata, snapshot, &layout, storage)
                .await?;
        let (reused, pending) = reuse_or_open(entries, previous, storage, &hedge)?;
        let inspected = inspect_all(table, &layout, pending).await?;
        let reservation = check_budget(table, config, local, &reused, &inspected)?;
        let built = build_all(config.residency, local, inspected).await;
        drop(reservation);
        let built = built?;
        let members = reused
            .into_iter()
            .map(|file| Member { file, reused: true })
            .chain(built.into_iter().map(|file| Member {
                file,
                reused: false,
            }))
            .collect();
        Self::assemble(
            table,
            snapshot,
            snapshot_timestamp,
            &resolved.location,
            layout,
            config.residency,
            members,
        )
        .await
    }

    /// Everything after the files are indexed: sorts them, builds the
    /// routes, finds the overlap groups, and merges every group that a
    /// non-reused file belongs to, refusing a key two files share. The
    /// groups are merged one at a time, each one's key chunks gathered
    /// before its merge and dropped after, so the load holds one group's
    /// chunks at once and not the table's (RFD 017: the Disk tier reads
    /// them back from the chunk files, the Bloom tier fetches them again).
    async fn assemble(
        table: &str,
        snapshot: &Snapshot,
        snapshot_timestamp: jiff::Timestamp,
        metadata_location: &str,
        layout: TableLayout,
        residency: Residency,
        mut members: Vec<Member>,
    ) -> Result<Self> {
        members.sort_by(|a, b| file_order(&a.file, &b.file));
        let files: Vec<Arc<LoadedFile>> = members
            .iter()
            .map(|member| Arc::clone(&member.file))
            .collect();
        let routes = routes(&files);
        let groups = overlap_groups(&files, &routes);
        let overlapping = widest_per_bucket(&files, &groups);
        let to_verify: Vec<Vec<usize>> = groups
            .into_iter()
            .filter(|group| !group.iter().all(|&at| members[at].reused))
            .collect();
        for group in &to_verify {
            let chunks = gather_chunks(&files, group).await?;
            index::off_worker(|| refuse_duplicates(table, &files, group, &chunks))?;
        }
        let records = files.iter().map(|file| file.entry.records).sum();
        let index_bytes = files.iter().map(|file| file.index.resident_bytes()).sum();
        tracing::debug!(
            table,
            snapshot_id = snapshot.snapshot_id(),
            files = files.len(),
            index_bytes,
            overlap_groups_verified = to_verify.len(),
            "assembled generation"
        );
        Ok(Self {
            table: table.to_string(),
            snapshot_id: SnapshotId(snapshot.snapshot_id()),
            metadata_location: metadata_location.to_string(),
            layout,
            residency,
            files,
            records,
            index_bytes,
            loaded_at: Instant::now(),
            snapshot_timestamp,
            routes,
            overlapping,
            verified_groups: to_verify.len(),
        })
    }

    /// The files that may hold `key`, in file order: under every bucketing
    /// the files carry, the files of the bucket `key` falls in under it
    /// (each bucketing routes the key to its own bucket), and every file
    /// without a bucketing, narrowed to those whose exact key range covers
    /// the key. A table whose files all carry one bucketing yields one
    /// bucket's files; after a spec evolution, one bucket's files per
    /// bucketing.
    pub fn candidates(&self, key: &Key) -> impl Iterator<Item = &Arc<LoadedFile>> {
        self.routes
            .iter()
            .flat_map(move |(bucketing, buckets)| {
                let bucket =
                    bucketing.map_or(manifest::UNBUCKETED, |bucketing| bucketing.bucket_of(key));
                buckets.get(&bucket).into_iter().flatten()
            })
            .map(|&at| &self.files[at])
            .filter(move |file| file.index.may_contain(key))
    }

    /// Per bucket that an overlap group touches, the widest such group:
    /// how many files a lookup in that bucket may probe (RFD 007). A group
    /// spanning two bucketings is recorded under every member's bucket.
    /// Keyed by the bucket number alone, as the doctor reports it: after a
    /// spec evolution, buckets of two bucketings that share a number share
    /// one entry, and an unbucketed file's `manifest::UNBUCKETED` is bucket
    /// zero's number.
    pub fn overlapping_buckets(&self) -> &BTreeMap<u32, usize> {
        &self.overlapping
    }

    /// Overlap groups this load merged to prove key uniqueness: the groups
    /// that gained a file since `previous`, or every group on a first load.
    pub fn overlap_groups_verified(&self) -> usize {
        self.verified_groups
    }

    /// One lookup: the candidates are probed in order and the first whose
    /// `find` answers gives the row, whose value page is read through
    /// `cache`. `Present` carries the bytes, `Null` is a present key with a
    /// null value, `Absent` a key no candidate holds. The GET cost is the
    /// index's: in the Full tier one GET for a cold hit, none for a warm hit
    /// or a miss.
    pub async fn get(&self, key: &Key, cache: &PageCache) -> Result<Value> {
        self.lookup(key, cache).await.map(|(value, _result)| value)
    }

    /// `get`, with the `result` label the lookup earns: `Miss` for an absent
    /// key; for a present one, `ColdHit` when its value page came from
    /// outside the cache (a fetch this lookup ran or joined) and `WarmHit`
    /// when the cache served it from memory or its disk level. Only the
    /// value page decides it; a key-page GET in the Bloom tier does not.
    pub(crate) async fn lookup(
        &self,
        key: &Key,
        cache: &PageCache,
    ) -> Result<(Value, LookupResult)> {
        for file in self.candidates(key) {
            if let Some(row) = file.index.find(key, Some(cache)).await? {
                let (value, origin) = file.index.read_value(row, Some(cache)).await?;
                let value = match value {
                    Some(bytes) => Value::Present(bytes),
                    None => Value::Null,
                };
                let result = match origin {
                    PageOrigin::Fetched => LookupResult::ColdHit,
                    PageOrigin::Cached => LookupResult::WarmHit,
                };
                return Ok((value, result));
            }
        }
        Ok((Value::Absent, LookupResult::Miss))
    }

    /// Which file and page each key would reach; the oracle for cost tests.
    /// The first candidate whose `page_of` names a value page, with that
    /// page: in the Full tier that is the file holding the key, wherever in
    /// an overlap group it sits, and the page's `value_page_offset` is the
    /// one GET a cold hit makes. When no candidate names a value page (the
    /// key is absent from every file whose fences cover it, or the tier is
    /// Disk or Bloom, where naming one would take a read), the first
    /// candidate that would decode a key page for it, the first probe.
    /// `None` for a key no candidate would decode a page for, which `get`
    /// answers `Absent` with no I/O.
    pub fn plan(&self, keys: &[Key]) -> Vec<Option<(Arc<LoadedFile>, PageRef)>> {
        keys.iter().map(|key| self.plan_one(key)).collect()
    }

    fn plan_one(&self, key: &Key) -> Option<(Arc<LoadedFile>, PageRef)> {
        let mut first_probe = None;
        for file in self.candidates(key) {
            let Some(page) = file.index.page_of(key) else {
                continue;
            };
            if page.value_page_offset.is_some() {
                return Some((Arc::clone(file), page));
            }
            first_probe.get_or_insert_with(|| (Arc::clone(file), page));
        }
        first_probe
    }

    /// The identity of every file's chunk file, for `LocalDir::retain`:
    /// the union over every table sharing the directory is the live set
    /// (RFD 017). Meaningful for Disk residency, where each file has a
    /// chunk file; the identities of a Full or Bloom generation name files
    /// that were never written and retain nothing.
    pub fn identities(&self) -> HashSet<DataFileIdentity> {
        self.files
            .iter()
            .map(|file| index::identity_of(file.index.handle()))
            .collect()
    }
}

/// A file of a generation being assembled, and whether it came from the
/// previous generation, which verified every overlap group it was in.
struct Member {
    file: Arc<LoadedFile>,
    reused: bool,
}

/// The order of a generation's files: bucketing, bucket, the exact first
/// key, then path, so two loads of one snapshot order the same and files
/// of one bucket are in key order for the overlap sweep.
fn file_order(a: &LoadedFile, b: &LoadedFile) -> Ordering {
    a.entry
        .bucketing
        .cmp(&b.entry.bucketing)
        .then(a.entry.bucket.cmp(&b.entry.bucket))
        .then_with(|| a.index.key_range().0.cmp(b.index.key_range().0))
        .then_with(|| a.entry.path.cmp(&b.entry.path))
}

/// The routes of `files`, which are in `file_order`.
fn routes(files: &[Arc<LoadedFile>]) -> Routes {
    let mut routes = Routes::new();
    for (at, file) in files.iter().enumerate() {
        routes
            .entry(file.entry.bucketing)
            .or_default()
            .entry(file.entry.bucket)
            .or_default()
            .push(at);
    }
    routes
}

/// The snapshot's timestamp as jiff holds it; one outside jiff's range is
/// `TableFault::SnapshotTimestampOutOfRange` under `table`.
fn snapshot_timestamp(table: &str, snapshot: &Snapshot) -> Result<jiff::Timestamp> {
    time::timestamp_from_millis(snapshot.timestamp_ms()).map_err(|_out_of_range| {
        Error::table_fault(
            table,
            TableFault::SnapshotTimestampOutOfRange {
                snapshot_id: SnapshotId(snapshot.snapshot_id()),
            },
        )
    })
}

/// A file the previous generation does not hold, open for inspection.
type Pending = (DataFileEntry, Arc<FileHandle>);

/// Splits the snapshot's files into those `previous` holds by the same path
/// at the same size, reused as the same `Arc` (RFD 005; a data file is
/// immutable, so a path whose size changed is another file and its index
/// would be stale), and the rest, each opened as a `FileHandle` with the
/// manifest's size and `hedge`. Opening sends nothing.
fn reuse_or_open(
    entries: Vec<DataFileEntry>,
    previous: Option<&Generation>,
    storage: &Arc<Storage>,
    hedge: &Arc<HedgeWindow>,
) -> Result<(Vec<Arc<LoadedFile>>, Vec<Pending>)> {
    let reusable: HashMap<&str, &Arc<LoadedFile>> = previous
        .map(|generation| {
            generation
                .files
                .iter()
                .map(|file| (file.entry.path.as_str(), file))
                .collect()
        })
        .unwrap_or_default();
    let mut reused = Vec::new();
    let mut pending = Vec::new();
    for entry in entries {
        let unchanged = reusable
            .get(entry.path.as_str())
            .filter(|file| file.entry.size == entry.size);
        if let Some(file) = unchanged {
            reused.push(Arc::clone(file));
        } else {
            let handle = FileHandle::open(
                Arc::clone(storage),
                &entry.path,
                entry.size,
                Arc::clone(hedge),
            )?;
            pending.push((entry, Arc::new(handle)));
        }
    }
    Ok((reused, pending))
}

/// Inspects every pending file's footer under `layout`, `LOAD_CONCURRENCY`
/// at a time; the first refusal or fault ends the load.
async fn inspect_all(
    table: &str,
    layout: &TableLayout,
    pending: Vec<Pending>,
) -> Result<Vec<(DataFileEntry, Inspected)>> {
    stream::iter(pending)
        .map(|(entry, handle)| async move {
            let inspected = FileIndex::inspect(
                table,
                handle,
                &layout.key_column,
                &layout.value_column,
                layout.key_type,
            )
            .await?;
            Ok::<_, Error>((entry, inspected))
        })
        .buffered(LOAD_CONCURRENCY)
        .try_collect()
        .await
}

/// Builds every inspected file's index in `residency`, `LOAD_CONCURRENCY`
/// at a time, after the budget check admitted them all.
async fn build_all(
    residency: Residency,
    local: Option<&LocalDir>,
    inspected: Vec<(DataFileEntry, Inspected)>,
) -> Result<Vec<Arc<LoadedFile>>> {
    stream::iter(inspected)
        .map(|(entry, inspected)| async move {
            let index = inspected.build(residency, local).await?;
            Ok::<_, Error>(Arc::new(LoadedFile { entry, index }))
        })
        .buffered(LOAD_CONCURRENCY)
        .try_collect()
        .await
}

/// The budget the residency sets, checked before any key chunk is fetched
/// (RFD 017). Full: `config.resident_budget_bytes`, against the resident
/// bytes of the reused files plus the predicted full bytes of every new
/// one; the budget is the table's alone, and the check holds nothing. Disk:
/// `local.index_budget()`, shared by every table, reserved through
/// `LocalDir::reserve` for the bytes every new file would write, which is
/// nothing for a file whose chunk file is already there, against the bytes
/// `index/` holds (the reused files' chunk files among them) and every
/// other load's outstanding reservation. The reservation returned must be
/// held until the load has finished writing. Bloom takes no budget. Over
/// is `RefusalReason::OverBudget` carrying both numbers. The sums saturate,
/// as the predictions they add do: a need that would wrap stays at
/// `u64::MAX` instead of wrapping to a small number that fits the budget.
fn check_budget<'a>(
    table: &str,
    config: &TableConfig,
    local: Option<&'a LocalDir>,
    reused: &[Arc<LoadedFile>],
    inspected: &[(DataFileEntry, Inspected)],
) -> Result<Option<Reservation<'a>>> {
    match config.residency {
        Residency::Full => {
            if let Some(budget) = config.resident_budget_bytes {
                let needed = reused
                    .iter()
                    .map(|file| approx::count(file.index.resident_bytes()))
                    .chain(
                        inspected
                            .iter()
                            .map(|(_, inspected)| inspected.predicted().full),
                    )
                    .fold(0, u64::saturating_add);
                if needed > budget {
                    return Err(Error::refused(
                        table,
                        RefusalReason::OverBudget { needed, budget },
                    ));
                }
            }
            Ok(None)
        }
        Residency::Disk => match local {
            Some(local) if local.index_budget().is_some() => {
                let new = inspected.iter().try_fold(0, |sum: u64, (_, inspected)| {
                    Ok::<_, Error>(sum.saturating_add(inspected.disk_bytes_needed(local)?))
                })?;
                local.reserve(table, new).map(Some)
            }
            Some(_) | None => Ok(None),
        },
        Residency::Bloom => Ok(None),
    }
}

/// Whether the exact key ranges of two files intersect.
fn intersects(a: &LoadedFile, b: &LoadedFile) -> bool {
    let (a_first, a_last) = a.index.key_range();
    let (b_first, b_last) = b.index.key_range();
    a_first <= b_last && b_first <= a_last
}

/// A disjoint-set forest over positions in a generation's files.
struct Components {
    parent: Vec<usize>,
}

impl Components {
    fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
        }
    }

    /// The representative of `at`'s set, halving the path on the way.
    fn root(&mut self, mut at: usize) -> usize {
        while self.parent[at] != at {
            let grandparent = self.parent[self.parent[at]];
            self.parent[at] = grandparent;
            at = grandparent;
        }
        at
    }

    fn link(&mut self, a: usize, b: usize) {
        let (a, b) = (self.root(a), self.root(b));
        if a != b {
            self.parent[a] = b;
        }
    }

    /// The sets of more than one member, each in ascending position order,
    /// ordered by their first member.
    fn groups(mut self) -> Vec<Vec<usize>> {
        let mut by_root: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for at in 0..self.parent.len() {
            let root = self.root(at);
            by_root.entry(root).or_default().push(at);
        }
        let mut groups: Vec<Vec<usize>> = by_root
            .into_values()
            .filter(|members| members.len() > 1)
            .collect();
        groups.sort_by_key(|members| members[0]);
        groups
    }
}

/// The overlap groups of `files` (RFD 005): the connected components of
/// "may hold a key in common", where two files may when their exact key
/// ranges intersect and one key can fall in both their buckets
/// (`buckets_may_share_a_key`). Within one bucket the files are in
/// first-key order, so a file joins the running group exactly when it
/// starts at or before the group's greatest last key; across two
/// bucketings, which a table carries only after a spec evolution, every
/// intersecting pair of files in buckets a key can share is linked, a
/// pairwise pass whose cost is accepted for that rare case.
fn overlap_groups(files: &[Arc<LoadedFile>], routes: &Routes) -> Vec<Vec<usize>> {
    let mut components = Components::new(files.len());
    for buckets in routes.values() {
        for members in buckets.values() {
            let mut open: Option<(usize, &Key)> = None;
            for &at in members {
                let (first, last) = files[at].index.key_range();
                match open {
                    Some((member, group_last)) if first <= group_last => {
                        components.link(member, at);
                        if last > group_last {
                            open = Some((member, last));
                        }
                    }
                    Some(_) | None => open = Some((at, last)),
                }
            }
        }
    }
    let per_bucketing: Vec<(Option<Bucketing>, &Buckets)> = routes
        .iter()
        .map(|(bucketing, buckets)| (*bucketing, buckets))
        .collect();
    for (at, ours) in per_bucketing.iter().enumerate() {
        for theirs in &per_bucketing[at + 1..] {
            link_across_bucketings(&mut components, files, *ours, *theirs);
        }
    }
    components.groups()
}

/// Links every pair of files, one under each bucketing, whose exact key
/// ranges intersect and whose buckets one key can fall in at once.
fn link_across_bucketings(
    components: &mut Components,
    files: &[Arc<LoadedFile>],
    (ours, our_buckets): (Option<Bucketing>, &Buckets),
    (theirs, their_buckets): (Option<Bucketing>, &Buckets),
) {
    for (&our_bucket, our_members) in our_buckets {
        for (&their_bucket, their_members) in their_buckets {
            if !buckets_may_share_a_key((ours, our_bucket), (theirs, their_bucket)) {
                continue;
            }
            for &a in our_members {
                for &b in their_members {
                    if intersects(&files[a], &files[b]) {
                        components.link(a, b);
                    }
                }
            }
        }
    }
}

/// Whether one key can be in bucket `a` under one bucketing and bucket `b`
/// under another. A key's bucket under `bucket[n]` is its hash modulo `n`
/// (`bucket::bucket`), so under counts `m` and `n` its two buckets are
/// congruent modulo `gcd(m, n)`; files whose buckets are not can never
/// share a key, and linking them would fuse every file of an evolved table
/// into one group and make every later append re-verify the whole table,
/// the cost RFD 005 rejected. A file without a bucketing holds keys of
/// every bucket.
fn buckets_may_share_a_key(a: (Option<Bucketing>, u32), b: (Option<Bucketing>, u32)) -> bool {
    match (a.0, b.0) {
        (Some(ours), Some(theirs)) => {
            let common = gcd(ours.count, theirs.count);
            a.1 % common == b.1 % common
        }
        (None, _) | (_, None) => true,
    }
}

/// The greatest common divisor of two bucket counts, both above zero.
fn gcd(mut a: u32, mut b: u32) -> u32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// Per bucket a group member is in, the widest group touching it.
fn widest_per_bucket(files: &[Arc<LoadedFile>], groups: &[Vec<usize>]) -> BTreeMap<u32, usize> {
    let mut widest = BTreeMap::new();
    for group in groups {
        for &at in group {
            let entry = widest.entry(files[at].entry.bucket).or_insert(0);
            *entry = (*entry).max(group.len());
        }
    }
    widest
}

/// The key chunks of every member of one `group`, by position: what its
/// merge decodes from, gathered `LOAD_CONCURRENCY` members at a time. Full
/// hands out its resident chunks; Disk reads them back as local reads;
/// Bloom fetches them again (RFD 017). The positions are streamed by value:
/// a closure taking a reference and returning an `async` block makes every
/// future up the call chain unprovably `Send`, and a load must be spawnable.
async fn gather_chunks(
    files: &[Arc<LoadedFile>],
    group: &[usize],
) -> Result<HashMap<usize, Arc<Segments>>> {
    stream::iter(group.iter().copied())
        .map(|at| async move {
            let chunks = files[at].index.key_chunks_for_verification().await?;
            Ok::<_, Error>((at, chunks))
        })
        .buffered(LOAD_CONCURRENCY)
        .try_collect()
        .await
}

/// A cursor over one file's keys in file order, one key page decoded at a
/// time, so a merge holds one page per file and never a row group.
struct KeyCursor<'a> {
    /// The file's position in the generation's files.
    at: usize,
    index: &'a FileIndex,
    chunks: &'a Arc<Segments>,
    row_group: usize,
    page: usize,
    keys: Keys,
    position: usize,
}

impl<'a> KeyCursor<'a> {
    /// A cursor on the first key of file `at`; `None` for a file with no
    /// key page, which `FileIndex::build` does not produce.
    fn open(at: usize, index: &'a FileIndex, chunks: &'a Arc<Segments>) -> Result<Option<Self>> {
        let mut cursor = Self {
            at,
            index,
            chunks,
            row_group: 0,
            page: 0,
            keys: Keys::Long(Vec::new()),
            position: 0,
        };
        Ok(cursor.fill()?.then_some(cursor))
    }

    /// The key under the cursor.
    fn head(&self) -> Key {
        self.keys.get(self.position)
    }

    /// Decodes the next key page that holds a key; `false` once the file
    /// is exhausted.
    fn fill(&mut self) -> Result<bool> {
        while self.row_group < self.index.metadata().num_row_groups() {
            if self.page >= self.index.page_count(self.row_group) {
                self.row_group += 1;
                self.page = 0;
                continue;
            }
            self.keys = self
                .index
                .decode_keys(self.row_group, Some(self.page), self.chunks)?;
            self.page += 1;
            self.position = 0;
            if !self.keys.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Moves to the next key; `false` once the file is exhausted.
    fn advance(&mut self) -> Result<bool> {
        self.position += 1;
        if self.position < self.keys.len() {
            return Ok(true);
        }
        self.fill()
    }
}

/// The heads of cursors `a` and `b` compared in served order.
fn head_order(cursors: &[KeyCursor<'_>], a: usize, b: usize) -> Ordering {
    cursors[a]
        .keys
        .cmp_at(cursors[a].position, &cursors[b].keys, cursors[b].position)
}

/// Merges the sorted key runs of `group` and refuses the first key two
/// files share. Every file is sorted and free of duplicates on its own
/// (`FileIndex::build` checked), so two equal heads at the front of the
/// merge are exactly a key in two files. The cursors are kept in a vector
/// ordered by head; each step takes the least, advances it, and reinserts
/// it by binary search. The refusal names the key and the later of the two
/// files in file order (RFD 007).
fn refuse_duplicates(
    table: &str,
    files: &[Arc<LoadedFile>],
    group: &[usize],
    chunks: &HashMap<usize, Arc<Segments>>,
) -> Result<()> {
    let mut cursors: Vec<KeyCursor<'_>> = group
        .iter()
        .filter_map(|&at| {
            let chunks = chunks
                .get(&at)
                .expect("gather_chunks fetched every group member's chunks");
            KeyCursor::open(at, &files[at].index, chunks).transpose()
        })
        .collect::<Result<_>>()?;
    let mut order: Vec<usize> = (0..cursors.len()).collect();
    order.sort_by(|&a, &b| head_order(&cursors, a, b));
    while order.len() > 1 {
        let (least, next) = (order[0], order[1]);
        if head_order(&cursors, least, next) == Ordering::Equal {
            let later = cursors[least].at.max(cursors[next].at);
            return Err(Error::refused(
                table,
                RefusalReason::DuplicateKeys {
                    path: files[later].entry.path.clone(),
                    key: cursors[least].head(),
                },
            ));
        }
        order.remove(0);
        if cursors[least].advance()? {
            let insert_at = order
                .partition_point(|&cursor| head_order(&cursors, cursor, least) == Ordering::Less);
            order.insert(insert_at, least);
        }
    }
    Ok(())
}

/// The three outcomes of a lookup; the wire carries the same three (RFD 010).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// The key is present with this value.
    Present(Bytes),
    /// The key is present and its value is null.
    Null,
    /// No file holds the key.
    Absent,
}

/// One lookup's answer and the snapshot it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lookup {
    /// The outcome.
    pub value: Value,
    /// The snapshot the answer was read from.
    pub snapshot_id: SnapshotId,
}

/// Several lookups' answers from one snapshot, in the order of the keys
/// asked. Fails as a whole if any key's read fails; partial results are a
/// later, separate call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiLookup {
    /// One outcome per key, in order.
    pub values: Vec<Value>,
    /// The snapshot every answer was read from.
    pub snapshot_id: SnapshotId,
}

/// Iceberg tables in a temporary directory whose data files arrow writes
/// under typed layout knobs, followed through `Storage`'s `file://` store
/// and loaded through `Generation::load`, with the key and value
/// derivations every fixture shares; for the tests of this module and of
/// `check`. Nothing here is compiled outside tests.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BinaryArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use bytes::Bytes;
    use iceberg::io::FileIO;
    use iceberg::spec::{
        DataContentType, DataFile, ManifestContentType, ManifestStatus, PrimitiveType, Schema,
        TableMetadata, TableMetadataBuilder, Type, UnboundPartitionSpec,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
    use parquet::schema::types::ColumnPath;

    use super::Generation;
    use crate::approx::count;
    use crate::cache::PageCache;
    use crate::catalog::{TableFault, TableSource, VERSION_HINT, hint_location};
    use crate::config::{
        HedgeConfig, LocalConfig, Residency, SourceConfig, StorageConfig, TableConfig,
    };
    use crate::contract::PAGE_ROWS;
    use crate::error::{Error, RefusalReason, Result};
    use crate::key::{Key, KeyType, UUID_BYTES};
    use crate::local::LocalDir;
    use crate::manifest::Bucketing;
    use crate::manifest::testing::{
        self as manifest_testing, DATA_DIR, KEY_COLUMN, KEY_FIELD_ID, Partition, VALUE_COLUMN,
    };
    use crate::storage::{HedgeWindow, IoStats, Storage};

    /// The table every fixture is named.
    pub(crate) const TABLE: &str = "kv";
    /// The synthetic prefix an in-memory store is registered under, in the
    /// tests that read through one.
    pub(crate) const MEM_PREFIX: &str = "mem://w/";
    /// The memory budget of a test page cache: every page a test touches
    /// fits, so no test sees an eviction it did not ask for.
    pub(crate) const CACHE_BYTES: usize = 64 * 1024 * 1024;
    /// The branch the fixture tables are followed on.
    pub(crate) const MAIN: &str = iceberg::spec::MAIN_BRANCH;
    /// The step between written long keys, so the key after any written key
    /// is absent and inside the file's range.
    pub(crate) const KEY_STEP: i64 = 3;
    /// A resident-bytes budget below any file's resident bytes, so a `full`
    /// load under it is refused at the footer stage.
    pub(crate) const TINY_BUDGET: u64 = 1;
    /// Bytes per value where a fixture does not say.
    pub(crate) const VALUE_BYTES: usize = 32;
    /// The mixers that make a value a function of its key or row and of the
    /// byte's position, distinct for neighbouring keys: two small primes and
    /// a prime modulus under 256, so every byte is in range.
    const VALUE_KEY_MIX: i64 = 31;
    const VALUE_BYTE_MIX: i64 = 7;
    const VALUE_MODULUS: i64 = 251;
    /// The false-positive rate a `Bloom::Sized` filter is written at, the
    /// writer recipe's (RFD 002).
    pub(crate) const BLOOM_FPP: f64 = 0.01;
    /// Buckets of the two-bucket fixture.
    pub(crate) const TWO: u32 = 2;
    /// Keys in each end-to-end data file.
    pub(crate) const E2E_KEYS: i64 = 24;
    /// The snapshot ids and sequence numbers of the end-to-end snapshots.
    pub(crate) const FIRST_SNAPSHOT: i64 = 1_001;
    pub(crate) const SECOND_SNAPSHOT: i64 = 1_002;
    pub(crate) const THIRD_SNAPSHOT: i64 = 1_003;
    pub(crate) const FIRST_SEQUENCE: i64 = 1;
    pub(crate) const SECOND_SEQUENCE: i64 = 2;
    pub(crate) const THIRD_SEQUENCE: i64 = 3;
    /// The metadata versions of the end-to-end snapshots.
    pub(crate) const FIRST_VERSION: u64 = 1;
    pub(crate) const SECOND_VERSION: u64 = 2;
    pub(crate) const THIRD_VERSION: u64 = 3;

    /// The contract's page row limit as the writer takes it.
    pub(crate) fn page_rows() -> usize {
        usize::try_from(PAGE_ROWS).unwrap()
    }

    /// `VALUE_BYTES` bytes that are a function of `key`.
    pub(crate) fn value_for(key: i64) -> Vec<u8> {
        value_of_len(key, VALUE_BYTES)
    }

    /// `len` bytes that are a function of `mix`, distinct for neighbouring
    /// mixes.
    pub(crate) fn value_of_len(mix: i64, len: usize) -> Vec<u8> {
        (0..len)
            .map(|at| {
                let at = i64::try_from(at).unwrap();
                u8::try_from((mix * VALUE_KEY_MIX + at * VALUE_BYTE_MIX).rem_euclid(VALUE_MODULUS))
                    .unwrap()
            })
            .collect()
    }

    /// Each key's value, none null.
    pub(crate) fn values_of(keys: &[i64]) -> Vec<Option<Vec<u8>>> {
        keys.iter().map(|key| Some(value_for(*key))).collect()
    }

    /// Keys `from`, `from + KEY_STEP`, ... `count` of them.
    pub(crate) fn keys_from(from: i64, count: i64) -> Vec<i64> {
        (0..count).map(|at| from + at * KEY_STEP).collect()
    }

    /// The keys among `keys` that `bucketing` routes to `bucket`.
    pub(crate) fn keys_in_bucket(keys: &[i64], bucketing: Bucketing, bucket: u32) -> Vec<i64> {
        keys.iter()
            .copied()
            .filter(|key| bucketing.bucket_of(&Key::Long(*key)) == bucket)
            .collect()
    }

    /// `bucket[count]` over the fixture key column.
    pub(crate) fn bucketing(count: u32) -> Bucketing {
        Bucketing {
            count,
            source_field_id: KEY_FIELD_ID,
        }
    }

    /// `keys` as long keys.
    pub(crate) fn long_keys(keys: &[i64]) -> Vec<Key> {
        keys.iter().map(|key| Key::Long(*key)).collect()
    }

    /// A memory-only page cache of `CACHE_BYTES`.
    pub(crate) async fn cache() -> PageCache {
        PageCache::new(CACHE_BYTES, None).await.unwrap()
    }

    /// The refusal `result` ended in, for `TABLE`.
    pub(crate) fn refusal_of<T: std::fmt::Debug>(result: Result<T>) -> RefusalReason {
        let Err(Error::Refused { table, reason }) = result else {
            panic!("expected a refusal, got {result:?}");
        };
        assert_eq!(table, TABLE);
        reason
    }

    /// The table fault `result` ended in, under `table`.
    pub(crate) fn table_fault_of<T: std::fmt::Debug>(result: Result<T>, table: &str) -> TableFault {
        let Err(Error::Table {
            table: named,
            fault,
        }) = result
        else {
            panic!("expected a table fault, got {result:?}");
        };
        assert_eq!(named, table);
        fault
    }

    /// Which fixture columns are dictionary-encoded.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Dictionary {
        /// Neither: the contract's shape.
        Off,
        /// The key column alone, whose dictionary page costs nothing extra.
        Key,
        /// The value column alone, whose dictionary page is one more GET per
        /// hit.
        Value,
        /// Every column.
        All,
    }

    /// `builder` with dictionary encoding on the columns `dictionary` names
    /// and off elsewhere.
    pub(crate) fn with_dictionary(
        builder: WriterPropertiesBuilder,
        dictionary: Dictionary,
    ) -> WriterPropertiesBuilder {
        let builder = builder.set_dictionary_enabled(dictionary == Dictionary::All);
        let column = match dictionary {
            Dictionary::Off | Dictionary::All => None,
            Dictionary::Key => Some(KEY_COLUMN),
            Dictionary::Value => Some(VALUE_COLUMN),
        };
        match column {
            Some(column) => builder.set_column_dictionary_enabled(ColumnPath::from(column), true),
            None => builder,
        }
    }

    /// Whether and how the key column gets a bloom filter. The writer sizes
    /// a filter for the file's keys and folds it down to the smallest size
    /// that meets the false-positive rate, so the rate is the one knob that
    /// sets its bytes per key.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub(crate) enum Bloom {
        /// None.
        Off,
        /// Sized to the file's keys at `BLOOM_FPP`: the contract's filter.
        Sized,
        /// Sized to the file's keys at this false-positive rate: far below
        /// `BLOOM_FPP` an oversized filter, far above it an undersized one.
        Fpp(f64),
    }

    impl Bloom {
        /// The false-positive rate the filter is written at; `None` for no
        /// filter.
        pub(crate) fn fpp(self) -> Option<f64> {
            match self {
                Self::Off => None,
                Self::Sized => Some(BLOOM_FPP),
                Self::Fpp(fpp) => Some(fpp),
            }
        }
    }

    /// `builder` with a filter on the key column written at `bloom`'s rate
    /// for `ndv` distinct keys, or unchanged for no filter. The filter
    /// setters enable a filter on the column they name, so the value column
    /// never gets one.
    pub(crate) fn with_key_bloom(
        builder: WriterPropertiesBuilder,
        bloom: Bloom,
        ndv: u64,
    ) -> WriterPropertiesBuilder {
        let Some(fpp) = bloom.fpp() else {
            return builder;
        };
        let key = ColumnPath::from(KEY_COLUMN);
        builder
            .set_column_bloom_filter_enabled(key.clone(), true)
            .set_column_bloom_filter_fpp(key.clone(), fpp)
            .set_column_bloom_filter_ndv(key, ndv)
    }

    /// The writer's layout knobs; `contract()` is RFD 002's shape.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub(crate) struct WriterLayout {
        /// Rows per page, key and value alike; the write batch is the same,
        /// so the row limit binds exactly.
        pub(crate) page_rows: usize,
        /// Which column is dictionary-encoded.
        pub(crate) dictionary: Dictionary,
        /// The column index's truncation length; `None` keeps whole values.
        pub(crate) column_index_truncate: Option<usize>,
        /// The key column's bloom filter.
        pub(crate) bloom: Bloom,
        /// Rows per row group; `None` leaves the writer's default, one row
        /// group for every fixture this size.
        pub(crate) row_group_rows: Option<usize>,
    }

    impl WriterLayout {
        /// RFD 002's shape: contract pages, no dictionary, a whole column
        /// index, a sized filter.
        pub(crate) fn contract() -> Self {
            Self {
                page_rows: page_rows(),
                dictionary: Dictionary::Off,
                column_index_truncate: None,
                bloom: Bloom::Sized,
                row_group_rows: None,
            }
        }

        /// The writer properties for a file of `rows` rows, the filter
        /// sized to all of them.
        fn properties(&self, rows: usize) -> WriterProperties {
            let builder = WriterProperties::builder()
                .set_data_page_row_count_limit(self.page_rows)
                .set_write_batch_size(self.page_rows)
                .set_column_index_truncate_length(self.column_index_truncate)
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .set_bloom_filter_enabled(false)
                .set_max_row_group_row_count(self.row_group_rows);
            let builder = with_dictionary(builder, self.dictionary);
            with_key_bloom(builder, self.bloom, count(rows)).build()
        }
    }

    /// How a fixture's key column is typed in arrow.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum KeyColumn {
        Int64,
        Utf8,
        Binary,
        FixedBinary(i32),
    }

    /// The arrow column a key type is written as: a uuid as a fixed binary
    /// of `UUID_BYTES`, as Parquet stores the type.
    pub(crate) fn column_of(key_type: KeyType) -> KeyColumn {
        match key_type {
            KeyType::Long => KeyColumn::Int64,
            KeyType::String => KeyColumn::Utf8,
            KeyType::Binary => KeyColumn::Binary,
            KeyType::Uuid => KeyColumn::FixedBinary(i32::try_from(UUID_BYTES).unwrap()),
        }
    }

    /// The arrow type of a key column.
    pub(crate) fn key_data_type(column: KeyColumn) -> DataType {
        match column {
            KeyColumn::Int64 => DataType::Int64,
            KeyColumn::Utf8 => DataType::Utf8,
            KeyColumn::Binary => DataType::Binary,
            KeyColumn::FixedBinary(width) => DataType::FixedSizeBinary(width),
        }
    }

    /// `keys` as the key column's array; a `None` is a null.
    pub(crate) fn key_array(column: KeyColumn, keys: &[Option<Key>]) -> ArrayRef {
        let raw = || {
            keys.iter()
                .map(|key| key.as_ref().map(|key| key.as_bytes().into_owned()))
        };
        match column {
            KeyColumn::Int64 => Arc::new(Int64Array::from(
                keys.iter()
                    .map(|key| {
                        key.as_ref().map(|key| {
                            let Key::Long(value) = key else {
                                panic!("a long fixture holds long keys");
                            };
                            *value
                        })
                    })
                    .collect::<Vec<_>>(),
            )),
            KeyColumn::Utf8 => Arc::new(
                keys.iter()
                    .map(|key| {
                        key.as_ref().map(|key| {
                            let Key::Str(text) = key else {
                                panic!("a string fixture holds string keys");
                            };
                            text.clone()
                        })
                    })
                    .collect::<StringArray>(),
            ),
            KeyColumn::Binary => Arc::new(raw().collect::<BinaryArray>()),
            KeyColumn::FixedBinary(width) => Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(raw(), width).unwrap(),
            ),
        }
    }

    /// A Parquet file of `keys` with `values` (a `None` is a null) laid out
    /// as `layout` says, one row group.
    pub(crate) fn parquet(
        key_type: KeyType,
        keys: &[Key],
        values: &[Option<Vec<u8>>],
        layout: &WriterLayout,
    ) -> Bytes {
        let column = column_of(key_type);
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(KEY_COLUMN, key_data_type(column), false),
            Field::new(VALUE_COLUMN, DataType::Binary, true),
        ]));
        let keys: Vec<Option<Key>> = keys.iter().cloned().map(Some).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                key_array(column, &keys),
                Arc::new(values.iter().cloned().collect::<BinaryArray>()) as ArrayRef,
            ],
        )
        .unwrap();
        let mut out = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut out, schema, Some(layout.properties(keys.len()))).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(out)
    }

    /// The Iceberg schema of a table keyed by `key_type` with binary values.
    pub(crate) fn schema_for(key_type: KeyType) -> Schema {
        let key = match key_type {
            KeyType::Long => PrimitiveType::Long,
            KeyType::String => PrimitiveType::String,
            KeyType::Binary => PrimitiveType::Binary,
            KeyType::Uuid => PrimitiveType::Uuid,
        };
        manifest_testing::schema(Type::Primitive(key), Type::Primitive(PrimitiveType::Binary))
    }

    /// What a fixture file's manifest entry records as its key bounds.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Bounds {
        /// The exact least and greatest key, as
        /// `write.metadata.metrics.column.<key>=full` records them.
        Exact,
        /// No bounds.
        Missing,
        /// String bounds cut to this many bytes, as Iceberg's default
        /// metrics cut them (prototype findings, finding 8): the lower
        /// bound is the cut prefix, the upper the cut prefix with its last
        /// byte incremented, so both still bound the keys.
        Truncated(usize),
    }

    /// `bounds` as the manifest records them for a file whose least and
    /// greatest keys are `min` and `max`.
    fn recorded_bounds(bounds: Bounds, min: &Key, max: &Key) -> Option<(Key, Key)> {
        match bounds {
            Bounds::Exact => Some((min.clone(), max.clone())),
            Bounds::Missing => None,
            Bounds::Truncated(len) => Some((
                truncated_bound(min, len, false),
                truncated_bound(max, len, true),
            )),
        }
    }

    /// A string key cut to `len` bytes, the last byte incremented for an
    /// upper bound; fixture keys are ASCII, so the cut is text.
    fn truncated_bound(key: &Key, len: usize, upper: bool) -> Key {
        let Key::Str(text) = key else {
            panic!("truncated bounds are a string-key fixture");
        };
        let mut cut = text.as_bytes()[..len].to_vec();
        if upper {
            *cut.last_mut().unwrap() += 1;
        }
        Key::Str(String::from_utf8(cut).unwrap())
    }

    /// One data file to write and record for the next snapshot.
    pub(crate) struct FileSpec<'a> {
        /// The file's name under the table's data directory.
        pub(crate) name: &'a str,
        /// Its keys, ascending, all of the table's key type.
        pub(crate) keys: &'a [Key],
        /// Its values, one per key; a `None` is a null.
        pub(crate) values: Vec<Option<Vec<u8>>>,
        /// Its partition tuple under the table's default spec.
        pub(crate) partition: Partition,
        /// What its manifest entry records as key bounds.
        pub(crate) bounds: Bounds,
        /// How its Parquet is laid out.
        pub(crate) layout: WriterLayout,
    }

    /// An Iceberg table in a temporary directory: metadata from
    /// `TableMetadataBuilder`, data files arrow writes, manifests and
    /// snapshots iceberg-rust writes, all followed through `Storage`'s
    /// `file://` store.
    pub(crate) struct Table {
        dir: tempfile::TempDir,
        /// The one storage every read goes through; its counter is the oracle.
        pub(crate) storage: Arc<Storage>,
        /// The table's hedge window, handed to every load.
        pub(crate) hedge: Arc<HedgeWindow>,
        key_type: KeyType,
        md: TableMetadata,
        /// The default spec, which `add_file` writes under.
        spec_id: i32,
        /// Every data file written so far with the spec it was written
        /// under: its entry for the next manifest.
        pub(crate) files: Vec<(DataFile, i32)>,
    }

    impl Table {
        /// A table keyed by `key_type` under `spec`, with no snapshot yet.
        pub(crate) fn new(key_type: KeyType, spec: UnboundPartitionSpec) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let root = manifest_testing::file_root(dir.path());
            let md = manifest_testing::metadata(&root, schema_for(key_type), spec);
            Self {
                dir,
                storage: Arc::new(Storage::new(StorageConfig::default())),
                hedge: Arc::new(HedgeWindow::new(&HedgeConfig::default())),
                key_type,
                spec_id: md.default_partition_spec_id(),
                md,
                files: Vec::new(),
            }
        }

        /// A long-keyed table bucketed `buckets` ways.
        pub(crate) fn bucketed(buckets: u32) -> Self {
            Self::new(KeyType::Long, manifest_testing::bucket_spec(buckets))
        }

        /// The table root as a `file://` location.
        pub(crate) fn root(&self) -> String {
            manifest_testing::file_root(self.dir.path())
        }

        /// The location data file `name` is written at.
        pub(crate) fn path_of(&self, name: &str) -> String {
            format!("{}/{DATA_DIR}/{name}.parquet", self.root())
        }

        /// A configuration following this table's main branch in `residency`
        /// with `budget` as its resident-bytes budget.
        pub(crate) fn config(&self, residency: Residency, budget: Option<u64>) -> TableConfig {
            TableConfig {
                name: TABLE.to_string(),
                source: SourceConfig::Static {
                    location: self.root(),
                },
                r#ref: MAIN.to_string(),
                key_column: KEY_COLUMN.to_string(),
                value_column: VALUE_COLUMN.to_string(),
                strict: true,
                residency,
                resident_budget_bytes: budget,
            }
        }

        /// Writes `spec`'s file and records it for the next snapshot.
        pub(crate) async fn add_file(&mut self, spec: FileSpec<'_>) {
            let path = self.path_of(spec.name);
            let bytes = parquet(self.key_type, spec.keys, &spec.values, &spec.layout);
            let size = count(bytes.len());
            manifest_testing::write_file(&FileIO::new_with_fs(), &path, bytes).await;
            let min = spec.keys.iter().min().unwrap();
            let max = spec.keys.iter().max().unwrap();
            let bounds = recorded_bounds(spec.bounds, min, max);
            self.files.push((
                manifest_testing::data_file(
                    &path,
                    self.spec_id,
                    manifest_testing::partition(spec.partition),
                    count(spec.keys.len()),
                    size,
                    bounds.as_ref().map(|(min, max)| (min, max)),
                    DataContentType::Data,
                ),
                self.spec_id,
            ));
        }

        /// `add_file` of a contract-layout file of long `keys` with their
        /// values and exact bounds, in `bucket`.
        pub(crate) async fn add_long_file(&mut self, name: &str, keys: &[i64], bucket: u32) {
            let typed = long_keys(keys);
            self.add_file(FileSpec {
                name,
                keys: &typed,
                values: values_of(keys),
                partition: Partition::Bucket(bucket),
                bounds: Bounds::Exact,
                layout: WriterLayout::contract(),
            })
            .await;
        }

        /// Drops the recorded file `name`, so the next snapshot no longer
        /// lists it or lists another file added under the same name.
        pub(crate) fn remove_file(&mut self, name: &str) {
            let path = self.path_of(name);
            self.files.retain(|(file, _)| file.file_path() != path);
        }

        /// Deletes the version hint, so the next resolve of the root fails
        /// on the store's own error, as a poll does while the root is
        /// unreachable; `restore_hint` or `commit` writes it again.
        pub(crate) fn remove_hint(&self) {
            std::fs::remove_file(self.dir.path().join(VERSION_HINT)).unwrap();
        }

        /// Writes the version hint pointing at metadata `version` again.
        pub(crate) async fn restore_hint(&self, version: u64) {
            manifest_testing::write_file(
                &FileIO::new_with_fs(),
                &hint_location(&self.root()),
                Bytes::from(version.to_string()),
            )
            .await;
        }

        /// Evolves the partition spec to `buckets` buckets; files added
        /// from here on are written under the new spec.
        pub(crate) fn evolve(&mut self, buckets: u32) {
            self.md = self
                .md
                .clone()
                .into_builder(None)
                .add_partition_spec(manifest_testing::bucket_spec(buckets))
                .unwrap()
                .set_default_partition_spec(TableMetadataBuilder::LAST_ADDED)
                .unwrap()
                .build()
                .unwrap()
                .metadata;
            self.spec_id = self.md.default_partition_spec_id();
        }

        /// Commits every file added so far as snapshot `id`, one manifest
        /// per spec the files were written under, and writes the metadata
        /// as `version`, pointing the hint at it.
        pub(crate) async fn commit(
            &mut self,
            id: i64,
            parent: Option<i64>,
            sequence: i64,
            version: u64,
        ) {
            let mut by_spec: BTreeMap<i32, Vec<(DataFile, ManifestStatus)>> = BTreeMap::new();
            for (file, spec_id) in &self.files {
                by_spec
                    .entry(*spec_id)
                    .or_default()
                    .push((file.clone(), ManifestStatus::Added));
            }
            let manifests = by_spec
                .into_iter()
                .map(|(spec_id, entries)| manifest_testing::ManifestSpec {
                    spec_id,
                    content: ManifestContentType::Data,
                    entries,
                })
                .collect();
            let snapshot = manifest_testing::write_snapshot(
                &self.md,
                id,
                parent,
                sequence,
                self.md.current_schema_id(),
                manifests,
            )
            .await;
            self.md = manifest_testing::with_snapshot(self.md.clone(), snapshot);
            manifest_testing::write_metadata(&self.md, version).await;
        }

        /// Resolves the root and loads the main branch's snapshot.
        pub(crate) async fn load(
            &self,
            config: &TableConfig,
            local: Option<&LocalDir>,
            previous: Option<&Generation>,
        ) -> Result<Generation> {
            let source = TableSource::Static {
                location: self.root(),
            };
            let resolved = source.resolve(&self.storage).await?;
            let snapshot = resolved.snapshot_for_ref(TABLE, MAIN)?;
            Generation::load(
                config,
                &resolved,
                &snapshot,
                &self.storage,
                Arc::clone(&self.hedge),
                local,
                previous,
            )
            .await
        }

        /// GETs the table's storage has issued so far.
        pub(crate) fn gets(&self) -> u64 {
            self.storage.stats().gets()
        }
    }

    /// Two contract files in two buckets, committed as `FIRST_SNAPSHOT`,
    /// and every key they hold.
    pub(crate) async fn two_bucket_table() -> (Table, Vec<i64>) {
        let mut table = Table::bucketed(TWO);
        let all = keys_from(0, E2E_KEYS);
        for bucket in 0..TWO {
            let keys = keys_in_bucket(&all, bucketing(TWO), bucket);
            table
                .add_long_file(&format!("f{bucket}"), &keys, bucket)
                .await;
        }
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        (table, all)
    }

    /// A local directory under `dir` charging its reads to `stats`, with
    /// `budget` on its `index/` and no page cache disk level.
    pub(crate) fn local_dir(
        dir: &std::path::Path,
        stats: Arc<IoStats>,
        budget: Option<u64>,
    ) -> LocalDir {
        LocalDir::open(
            &LocalConfig {
                dir: dir.to_path_buf(),
                index_budget_bytes: budget,
                page_cache_disk_bytes: 0,
            },
            stats,
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{Operation, Summary};
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};

    use super::testing::{
        self, Bloom, Bounds, E2E_KEYS, FIRST_SEQUENCE, FIRST_SNAPSHOT, FIRST_VERSION, FileSpec,
        KEY_STEP, MEM_PREFIX, SECOND_SEQUENCE, SECOND_SNAPSHOT, SECOND_VERSION, TABLE,
        THIRD_SEQUENCE, THIRD_SNAPSHOT, THIRD_VERSION, TINY_BUDGET, TWO, Table, WriterLayout,
        bucketing, cache, keys_from, keys_in_bucket, local_dir, long_keys, refusal_of,
        two_bucket_table, value_for, values_of,
    };
    use super::*;
    use crate::catalog::TableSource;
    use crate::config::{HedgeConfig, StorageConfig};
    use crate::key::KeyType;
    use crate::manifest::testing::{
        DATA_DIR, KEY_COLUMN, KEY_FIELD_ID, Partition, VALUE_COLUMN, VALUE_FIELD_ID,
    };

    /// The metadata location the assembled generations claim.
    const MEM_METADATA: &str = "mem://w/db/kv/metadata/v1.metadata.json";
    /// The snapshot id and sequence number of the assembled generations.
    const SNAPSHOT: i64 = 77;
    const SEQUENCE: i64 = 1;
    /// Bucket counts the routing tests use beside the fixture's `TWO`.
    const FOUR: u32 = 4;
    const EIGHT: u32 = 8;
    /// Keys per bucket file in the routing tests.
    const KEYS_PER_FILE: i64 = 16;
    /// A false-positive rate far above the contract's: the writer folds the
    /// filter to its one-block minimum, so with `BLOOM_KEYS` keys the
    /// filter passes about a third of the absent keys in range and rules
    /// out the rest, and both kinds of miss are reachable.
    const LOOSE_FPP: f64 = 0.5;
    /// Keys in the Bloom-tier fixture's one file: enough to fill a
    /// one-block filter to that rate, few enough that it rules keys out.
    const BLOOM_KEYS: i64 = 64;
    /// The GETs a Bloom-tier cold hit costs: the key page, then the value
    /// page (RFD 017).
    const BLOOM_HIT_GETS: u64 = 2;

    fn layout(bucketing: Option<Bucketing>) -> TableLayout {
        TableLayout {
            bucketing,
            key_column: KEY_COLUMN.to_string(),
            key_field_id: KEY_FIELD_ID,
            key_type: KeyType::Long,
            value_column: VALUE_COLUMN.to_string(),
            value_field_id: VALUE_FIELD_ID,
        }
    }

    /// The snapshot the assembled generations are said to serve; nothing
    /// of it is read but its id and sequence number.
    fn placeholder_snapshot() -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(SNAPSHOT)
            .with_sequence_number(SEQUENCE)
            .with_timestamp_ms(0)
            .with_manifest_list(format!("{MEM_PREFIX}db/kv/metadata/snap.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }

    /// An in-memory store behind a storage, and the files built over it.
    struct Mem {
        storage: Arc<Storage>,
        store: Arc<InMemory>,
        hedge: Arc<HedgeWindow>,
    }

    impl Mem {
        fn new() -> Self {
            let store = Arc::new(InMemory::new());
            let storage = Arc::new(Storage::new(StorageConfig::default()));
            let registered: Arc<dyn ObjectStore> = Arc::<InMemory>::clone(&store);
            storage.register(MEM_PREFIX, registered);
            Self {
                storage,
                store,
                hedge: Arc::new(HedgeWindow::new(&HedgeConfig::default())),
            }
        }

        fn gets(&self) -> u64 {
            self.storage.stats().gets()
        }

        fn location(name: &str) -> String {
            format!("{MEM_PREFIX}db/kv/{DATA_DIR}/{name}.parquet")
        }

        /// Writes `keys` with `values` as file `name` and indexes it in the
        /// Full tier, filed under `bucketing` and `bucket`.
        async fn file(
            &self,
            name: &str,
            keys: &[i64],
            values: &[Option<Vec<u8>>],
            bucketing: Option<Bucketing>,
            bucket: u32,
        ) -> Arc<LoadedFile> {
            let bytes = testing::parquet(
                KeyType::Long,
                &testing::long_keys(keys),
                values,
                &WriterLayout::contract(),
            );
            let size = approx::count(bytes.len());
            let key = Self::location(name)
                .strip_prefix(MEM_PREFIX)
                .unwrap()
                .to_string();
            self.store
                .put(&ObjectPath::from(key), PutPayload::from(bytes))
                .await
                .unwrap();
            let handle = Arc::new(
                FileHandle::open(
                    Arc::clone(&self.storage),
                    &Self::location(name),
                    size,
                    Arc::clone(&self.hedge),
                )
                .unwrap(),
            );
            let index = FileIndex::build(
                TABLE,
                handle,
                KEY_COLUMN,
                VALUE_COLUMN,
                KeyType::Long,
                Residency::Full,
                None,
            )
            .await
            .unwrap();
            let bounds = keys.iter().min().zip(keys.iter().max());
            Arc::new(LoadedFile {
                entry: DataFileEntry {
                    path: Self::location(name),
                    bucket,
                    bucketing,
                    records: approx::count(keys.len()),
                    size,
                    key_min: bounds.map(|(min, _)| Key::Long(*min)),
                    key_max: bounds.map(|(_, max)| Key::Long(*max)),
                },
                index,
            })
        }

        /// A file of `keys` with their values.
        async fn keyed(
            &self,
            name: &str,
            keys: &[i64],
            bucketing: Option<Bucketing>,
            bucket: u32,
        ) -> Arc<LoadedFile> {
            self.file(name, keys, &values_of(keys), bucketing, bucket)
                .await
        }
    }

    /// Assembles a generation from `files`, none reused.
    async fn assemble(files: Vec<Arc<LoadedFile>>, layout: TableLayout) -> Result<Generation> {
        assemble_with(
            files.into_iter().map(|file| (file, false)).collect(),
            layout,
        )
        .await
    }

    async fn assemble_with(
        members: Vec<(Arc<LoadedFile>, bool)>,
        layout: TableLayout,
    ) -> Result<Generation> {
        Generation::assemble(
            TABLE,
            &placeholder_snapshot(),
            jiff::Timestamp::UNIX_EPOCH,
            MEM_METADATA,
            layout,
            Residency::Full,
            members
                .into_iter()
                .map(|(file, reused)| Member { file, reused })
                .collect(),
        )
        .await
    }

    fn paths<'a>(files: impl Iterator<Item = &'a Arc<LoadedFile>>) -> Vec<String> {
        files.map(|file| file.entry.path.clone()).collect()
    }

    /// Every key of `keys` is `Present` with its value, through a fresh
    /// cache, at exactly one GET per distinct value page the plan names and
    /// none besides: the first hit on a page fetches it, every later hit on
    /// it is served warm.
    async fn check_present(generation: &Generation, storage: &Storage, keys: &[i64]) {
        let typed: Vec<Key> = keys.iter().map(|key| Key::Long(*key)).collect();
        let pages: HashSet<(String, u64)> = generation
            .plan(&typed)
            .into_iter()
            .map(|planned| {
                let (file, page) = planned.expect("a present key has a plan");
                let offset = page
                    .value_page_offset
                    .expect("the Full tier names the value page of a present key");
                (file.entry.path.clone(), offset)
            })
            .collect();
        let distinct = approx::count(pages.len());
        assert!(distinct >= 1);
        let cache = cache().await;
        let before = storage.stats().gets();
        for (key, raw) in typed.iter().zip(keys) {
            assert_eq!(
                generation.get(key, &cache).await.unwrap(),
                Value::Present(Bytes::from(value_for(*raw))),
                "key {key}"
            );
            assert_eq!(
                storage.stats().gets(),
                before + cache.fetches(),
                "key {key}: every fetch is one GET and nothing else is"
            );
        }
        assert_eq!(cache.fetches(), distinct, "one fetch per value page");
        assert_eq!(storage.stats().gets(), before + distinct);
    }

    #[tokio::test]
    async fn test_candidates_route_by_bucket_under_one_bucketing() {
        let mem = Mem::new();
        let all = keys_from(0, KEYS_PER_FILE * i64::from(FOUR));
        let mut files = Vec::new();
        for bucket in 0..FOUR {
            let keys = keys_in_bucket(&all, bucketing(FOUR), bucket);
            assert!(keys.len() > 1, "bucket {bucket} holds keys");
            files.push(
                mem.keyed(&format!("b{bucket}"), &keys, Some(bucketing(FOUR)), bucket)
                    .await,
            );
        }
        let generation = assemble(files, layout(Some(bucketing(FOUR))))
            .await
            .unwrap();
        assert_eq!(generation.files.len(), approx::length(FOUR));
        assert_eq!(generation.overlapping_buckets().len(), 0);
        for key in &all {
            let bucket = bucketing(FOUR).bucket_of(&Key::Long(*key));
            assert_eq!(
                paths(generation.candidates(&Key::Long(*key))),
                [Mem::location(&format!("b{bucket}"))],
                "key {key} routes to its bucket's file alone"
            );
        }
        check_present(&generation, &mem.storage, &all).await;
        let cache = cache().await;
        let before = mem.gets();
        for key in &all {
            let absent = Key::Long(key + 1);
            assert_eq!(
                generation.get(&absent, &cache).await.unwrap(),
                Value::Absent
            );
        }
        assert_eq!(mem.gets(), before, "a miss costs nothing");
    }

    #[tokio::test]
    async fn test_candidates_route_under_two_bucketings_and_unbucketed_files_always() {
        let mem = Mem::new();
        let all = keys_from(0, KEYS_PER_FILE * i64::from(EIGHT));
        let (under_four, under_eight) = all.split_at(all.len() / 2);
        let unbucketed_keys = keys_from(all.last().unwrap() + KEY_STEP, KEYS_PER_FILE);
        let mut files = Vec::new();
        for bucket in 0..FOUR {
            let keys = keys_in_bucket(under_four, bucketing(FOUR), bucket);
            files.push(
                mem.keyed(
                    &format!("four{bucket}"),
                    &keys,
                    Some(bucketing(FOUR)),
                    bucket,
                )
                .await,
            );
        }
        for bucket in 0..EIGHT {
            let keys = keys_in_bucket(under_eight, bucketing(EIGHT), bucket);
            files.push(
                mem.keyed(
                    &format!("eight{bucket}"),
                    &keys,
                    Some(bucketing(EIGHT)),
                    bucket,
                )
                .await,
            );
        }
        files.push(
            mem.keyed("flat", &unbucketed_keys, None, manifest::UNBUCKETED)
                .await,
        );
        let generation = assemble(files, layout(Some(bucketing(EIGHT))))
            .await
            .unwrap();

        // The unbucketed file comes first in file order and is a candidate
        // for every key in its range, whatever bucket the key hashes to.
        assert_eq!(generation.files[0].entry.path, Mem::location("flat"));
        for key in &unbucketed_keys {
            assert_eq!(
                paths(generation.candidates(&Key::Long(*key))),
                [Mem::location("flat")]
            );
        }
        // A key written under the four-bucket spec is probed in its bucket
        // under both bucketings when the eight-bucket file's range covers
        // it, and never in another bucket's file under either.
        for key in under_four {
            let key = Key::Long(*key);
            let four = bucketing(FOUR).bucket_of(&key);
            let eight = bucketing(EIGHT).bucket_of(&key);
            let candidates = paths(generation.candidates(&key));
            let eight_file = generation
                .files
                .iter()
                .find(|file| file.entry.path == Mem::location(&format!("eight{eight}")))
                .unwrap();
            let mut expected = vec![Mem::location(&format!("four{four}"))];
            if eight_file.index.may_contain(&key) {
                expected.push(Mem::location(&format!("eight{eight}")));
            }
            assert_eq!(candidates, expected, "key {key}");
        }
        check_present(&generation, &mem.storage, &all).await;
        check_present(&generation, &mem.storage, &unbucketed_keys).await;
    }

    #[tokio::test]
    async fn test_get_present_null_and_absent_at_the_promised_get_counts() {
        let mem = Mem::new();
        let keys = keys_from(0, KEYS_PER_FILE);
        let null_key = keys[2];
        let values: Vec<Option<Vec<u8>>> = keys
            .iter()
            .map(|key| (*key != null_key).then(|| value_for(*key)))
            .collect();
        let file = mem
            .file("f", &keys, &values, None, manifest::UNBUCKETED)
            .await;
        let generation = assemble(vec![file], layout(None)).await.unwrap();
        assert_eq!(generation.snapshot_id, SnapshotId(SNAPSHOT));
        assert_eq!(generation.records, approx::count(keys.len()));
        let cache = cache().await;
        let before = mem.gets();
        assert_eq!(
            generation.get(&Key::Long(keys[1]), &cache).await.unwrap(),
            Value::Present(Bytes::from(value_for(keys[1])))
        );
        assert_eq!(mem.gets(), before + 1, "a cold hit is one GET");
        assert_eq!(
            generation.get(&Key::Long(keys[0]), &cache).await.unwrap(),
            Value::Present(Bytes::from(value_for(keys[0])))
        );
        assert_eq!(mem.gets(), before + 1, "a warm hit is none");
        assert_eq!(
            generation.get(&Key::Long(null_key), &cache).await.unwrap(),
            Value::Null
        );
        assert_eq!(mem.gets(), before + 1, "a null on the cached page is none");
        for absent in [keys[1] + 1, keys[0] - 1, keys.last().unwrap() + 1] {
            assert_eq!(
                generation.get(&Key::Long(absent), &cache).await.unwrap(),
                Value::Absent
            );
        }
        assert_eq!(mem.gets(), before + 1, "a miss is none");
    }

    #[tokio::test]
    async fn test_lookup_labels_cold_hit_then_warm_hit_then_miss() {
        let (table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let generation = table.load(&config, None, None).await.unwrap();
        let cache = cache().await;
        let key = Key::Long(all[0]);
        let present = Value::Present(Bytes::from(value_for(all[0])));
        let before = table.gets();
        assert_eq!(
            generation.lookup(&key, &cache).await.unwrap(),
            (present.clone(), LookupResult::ColdHit)
        );
        assert_eq!(table.gets(), before + 1, "a cold hit is one GET");
        assert_eq!(
            generation.lookup(&key, &cache).await.unwrap(),
            (present, LookupResult::WarmHit)
        );
        assert_eq!(
            generation
                .lookup(&Key::Long(all[0] + 1), &cache)
                .await
                .unwrap(),
            (Value::Absent, LookupResult::Miss)
        );
        assert_eq!(table.gets(), before + 1, "a warm hit and a miss cost none");
    }

    /// Two lookups of `key` through `cache`: the first `ColdHit` at
    /// `hit_gets` GETs, the second `WarmHit` at none.
    async fn assert_cold_then_warm(
        table: &Table,
        generation: &Generation,
        cache: &PageCache,
        key: i64,
        hit_gets: u64,
    ) {
        let present = Value::Present(Bytes::from(value_for(key)));
        let before = table.gets();
        assert_eq!(
            generation.lookup(&Key::Long(key), cache).await.unwrap(),
            (present.clone(), LookupResult::ColdHit)
        );
        assert_eq!(table.gets(), before + hit_gets, "a cold hit");
        assert_eq!(
            generation.lookup(&Key::Long(key), cache).await.unwrap(),
            (present, LookupResult::WarmHit)
        );
        assert_eq!(table.gets(), before + hit_gets, "a warm hit costs none");
    }

    #[tokio::test]
    async fn test_lookup_labels_follow_the_value_page_in_disk_and_bloom_residency() {
        let (table, all) = two_bucket_table().await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), table.storage.stats(), None);
        let disk = table
            .load(&table.config(Residency::Disk, None), Some(&local), None)
            .await
            .unwrap();
        let disk_cache = cache().await;
        assert_cold_then_warm(&table, &disk, &disk_cache, all[0], 1).await;
        let before = table.gets();
        assert_eq!(
            disk.lookup(&Key::Long(all[0] + 1), &disk_cache)
                .await
                .unwrap(),
            (Value::Absent, LookupResult::Miss)
        );
        assert_eq!(
            table.gets(),
            before,
            "a Disk miss reads the chunk file, no GET"
        );

        let mut table = Table::bucketed(1);
        let all = keys_from(0, BLOOM_KEYS);
        let keys = long_keys(&all);
        table
            .add_file(FileSpec {
                name: "f",
                keys: &keys,
                values: values_of(&all),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout: WriterLayout {
                    bloom: Bloom::Fpp(LOOSE_FPP),
                    ..WriterLayout::contract()
                },
            })
            .await;
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let bloom = table
            .load(&table.config(Residency::Bloom, None), None, None)
            .await
            .unwrap();
        let bloom_cache = cache().await;
        assert_cold_then_warm(&table, &bloom, &bloom_cache, all[0], BLOOM_HIT_GETS).await;
        let absent: Vec<Key> = all[..all.len() - 1]
            .iter()
            .map(|key| Key::Long(key + 1))
            .collect();
        let plans = bloom.plan(&absent);
        let passing = absent
            .iter()
            .zip(&plans)
            .find(|(_, plan)| plan.is_some())
            .map(|(key, _)| key.clone())
            .expect("an undersized filter passes some absent key in range");
        let ruled_out = absent
            .iter()
            .zip(&plans)
            .find(|(_, plan)| plan.is_none())
            .map(|(key, _)| key.clone())
            .expect("a filter rules out some absent key in range");
        // A fresh cache, so the key page the hit above fetched is not held.
        let miss_cache = cache().await;
        let before = table.gets();
        assert_eq!(
            bloom.lookup(&passing, &miss_cache).await.unwrap(),
            (Value::Absent, LookupResult::Miss)
        );
        assert_eq!(
            table.gets(),
            before + 1,
            "a filter-positive miss costs the key page and is still a miss"
        );
        assert_eq!(
            bloom.lookup(&ruled_out, &miss_cache).await.unwrap(),
            (Value::Absent, LookupResult::Miss)
        );
        assert_eq!(
            table.gets(),
            before + 1,
            "a filter-negative miss costs none"
        );
    }

    #[tokio::test]
    async fn test_plan_names_the_file_and_page_get_uses_and_none_for_an_absent_key() {
        let mem = Mem::new();
        let keys = keys_from(0, KEYS_PER_FILE);
        let file = mem.keyed("f", &keys, None, manifest::UNBUCKETED).await;
        let generation = assemble(vec![Arc::clone(&file)], layout(None))
            .await
            .unwrap();
        let present = Key::Long(keys[3]);
        let inside = Key::Long(keys[3] + 1);
        let outside = Key::Long(keys.last().unwrap() + KEY_STEP);
        let plan = generation.plan(&[present.clone(), inside, outside]);
        assert_eq!(plan.len(), 3);
        let (planned, page) = plan[0].as_ref().unwrap();
        assert!(Arc::ptr_eq(planned, &file));
        assert_eq!((page.row_group, page.key_page), (0, 0));
        assert!(page.value_page_offset.is_some());
        let (_, inside_page) = plan[1].as_ref().unwrap();
        assert_eq!(inside_page.value_page_offset, None);
        assert!(plan[2].is_none());
        assert_cold_hit_reads_the_planned_page(&generation, &mem, planned, page, keys[3]).await;
    }

    /// The compressed size of the value page of `file`'s first row group
    /// at `offset`.
    fn value_page_size(file: &LoadedFile, offset: u64) -> u64 {
        file.index.metadata().offset_index().unwrap()[0][file.index.value_leaf()]
            .page_locations()
            .iter()
            .find(|location| u64::try_from(location.offset).unwrap() == offset)
            .map(|location| u64::try_from(location.compressed_page_size).unwrap())
            .unwrap()
    }

    /// A cold `get` of `key` costs one GET of exactly the page `page` of
    /// `planned` names.
    async fn assert_cold_hit_reads_the_planned_page(
        generation: &Generation,
        mem: &Mem,
        planned: &LoadedFile,
        page: &PageRef,
        key: i64,
    ) {
        let page_size = value_page_size(planned, page.value_page_offset.unwrap());
        let cache = cache().await;
        let (gets, bytes) = (mem.gets(), mem.storage.stats().bytes());
        assert_eq!(
            generation.get(&Key::Long(key), &cache).await.unwrap(),
            Value::Present(Bytes::from(value_for(key)))
        );
        assert_eq!(mem.gets(), gets + 1);
        assert_eq!(mem.storage.stats().bytes(), bytes + page_size);
    }

    #[tokio::test]
    async fn test_plan_names_the_later_overlapping_file_that_holds_the_key() {
        let mem = Mem::new();
        let first = keys_from(0, KEYS_PER_FILE);
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        let a = mem.keyed("a", &first, Some(bucketing(1)), 0).await;
        let b = mem.keyed("b", &interleaved, Some(bucketing(1)), 0).await;
        let generation = assemble(
            vec![Arc::clone(&a), Arc::clone(&b)],
            layout(Some(bucketing(1))),
        )
        .await
        .unwrap();
        // `a` is probed first and its fences cover the key, but `b` holds it.
        let held_by_b = interleaved[interleaved.len() / 2];
        assert_eq!(
            paths(generation.candidates(&Key::Long(held_by_b))),
            [Mem::location("a"), Mem::location("b")]
        );
        let plan = generation.plan(&[Key::Long(held_by_b), Key::Long(held_by_b + 1)]);
        let (planned, page) = plan[0].as_ref().unwrap();
        assert!(
            Arc::ptr_eq(planned, &b),
            "the plan names the file holding the key"
        );
        assert!(page.value_page_offset.is_some());
        assert_cold_hit_reads_the_planned_page(&generation, &mem, planned, page, held_by_b).await;
        // A key inside both fences and in neither file: the first probe,
        // with no value page.
        let (first_probe, page) = plan[1].as_ref().unwrap();
        assert!(Arc::ptr_eq(first_probe, &a));
        assert_eq!(page.value_page_offset, None);
    }

    #[tokio::test]
    async fn test_files_are_ordered_by_bucketing_bucket_first_key_and_path() {
        let mem = Mem::new();
        // Five disjoint key runs, so no two files can share a key and the
        // order alone is under test.
        let run = |at: i64| keys_from(at * KEY_STEP * KEYS_PER_FILE, KEYS_PER_FILE);
        let four_one_high = mem.keyed("a", &run(1), Some(bucketing(FOUR)), 1).await;
        let four_one_low = mem.keyed("b", &run(0), Some(bucketing(FOUR)), 1).await;
        let four_zero = mem.keyed("c", &run(2), Some(bucketing(FOUR)), 0).await;
        let unbucketed = mem.keyed("d", &run(3), None, manifest::UNBUCKETED).await;
        let eight_zero = mem.keyed("e", &run(4), Some(bucketing(EIGHT)), 0).await;
        let expected = paths(
            [
                &unbucketed,
                &four_zero,
                &four_one_low,
                &four_one_high,
                &eight_zero,
            ]
            .into_iter(),
        );
        let forward = vec![
            four_one_high,
            four_one_low,
            four_zero,
            unbucketed,
            eight_zero,
        ];
        let mut backward = forward.clone();
        backward.reverse();
        for order in [forward, backward] {
            let generation = assemble(order, layout(Some(bucketing(FOUR))))
                .await
                .unwrap();
            assert_eq!(paths(generation.files.iter()), expected);
        }
    }

    /// Files by name with the given key ranges, all under `bucketing` in
    /// their given buckets, for the grouping tests; the files are not
    /// probed, so their keys are their bounds alone.
    async fn ranged(
        mem: &Mem,
        files: &[(&str, Option<Bucketing>, u32, i64, i64)],
    ) -> Vec<Arc<LoadedFile>> {
        let mut loaded = Vec::new();
        for (name, bucketing, bucket, first, last) in files {
            loaded.push(mem.keyed(name, &[*first, *last], *bucketing, *bucket).await);
        }
        loaded
    }

    /// The overlap groups of `files`, in file order as `assemble` puts
    /// them, as lists of paths.
    fn groups_of(files: &[Arc<LoadedFile>]) -> Vec<Vec<String>> {
        let mut files = files.to_vec();
        files.sort_by(|a, b| file_order(a, b));
        let files = &files;
        overlap_groups(files, &routes(files))
            .into_iter()
            .map(|group| {
                group
                    .into_iter()
                    .map(|at| files[at].entry.path.clone())
                    .collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_overlap_groups_disjoint_intersecting_transitive_and_across_buckets() {
        let mem = Mem::new();
        let one = Some(bucketing(1));
        let disjoint = ranged(&mem, &[("a", one, 0, 0, 10), ("b", one, 0, 20, 30)]).await;
        assert!(groups_of(&disjoint).is_empty());

        let pair = ranged(&mem, &[("a", one, 0, 0, 10), ("b", one, 0, 5, 15)]).await;
        assert_eq!(groups_of(&pair), [[Mem::location("a"), Mem::location("b")]]);

        let chain = ranged(
            &mem,
            &[
                ("a", one, 0, 0, 10),
                ("b", one, 0, 8, 20),
                ("c", one, 0, 18, 30),
                ("d", one, 0, 40, 50),
            ],
        )
        .await;
        assert_eq!(
            groups_of(&chain),
            [[Mem::location("a"), Mem::location("b"), Mem::location("c")]],
            "a and c meet through b; d stands alone"
        );

        let four = Some(bucketing(FOUR));
        let other_buckets = ranged(&mem, &[("a", four, 0, 0, 10), ("b", four, 1, 5, 15)]).await;
        assert!(
            groups_of(&other_buckets).is_empty(),
            "one bucketing, different buckets, never a group"
        );

        // Across bucketings a key's buckets are congruent modulo the gcd of
        // the counts (here four): bucket 0 of four and bucket 4 of eight can
        // share a key, bucket 0 of four and bucket 5 of eight cannot.
        let eight = Some(bucketing(EIGHT));
        let congruent = ranged(&mem, &[("a", four, 0, 0, 10), ("b", eight, 4, 5, 15)]).await;
        assert_eq!(
            groups_of(&congruent),
            [[Mem::location("a"), Mem::location("b")]],
            "different bucketings, congruent buckets, intersecting ranges group"
        );
        let incongruent = ranged(&mem, &[("a", four, 0, 0, 10), ("b", eight, 5, 5, 15)]).await;
        assert!(
            groups_of(&incongruent).is_empty(),
            "buckets no key falls in at once never group"
        );

        let flat = ranged(
            &mem,
            &[
                ("a", four, 2, 0, 10),
                ("b", None, manifest::UNBUCKETED, 5, 15),
                ("c", four, 3, 40, 50),
            ],
        )
        .await;
        assert_eq!(
            groups_of(&flat),
            [[Mem::location("b"), Mem::location("a")]],
            "an unbucketed file groups with any intersecting file"
        );
    }

    #[tokio::test]
    async fn test_assemble_refuses_a_duplicate_across_overlapping_files_naming_the_later_file() {
        let mem = Mem::new();
        let first = keys_from(0, KEYS_PER_FILE);
        let duplicate = first[first.len() / 2];
        let overlapping = [duplicate, first.last().unwrap() + KEY_STEP];
        let a = mem.keyed("a", &first, Some(bucketing(1)), 0).await;
        let b = mem.keyed("b", &overlapping, Some(bucketing(1)), 0).await;
        assert_eq!(
            refusal_of(
                assemble(
                    vec![Arc::clone(&b), Arc::clone(&a)],
                    layout(Some(bucketing(1)))
                )
                .await
            ),
            RefusalReason::DuplicateKeys {
                path: Mem::location("b"),
                key: Key::Long(duplicate),
            }
        );
    }

    #[tokio::test]
    async fn test_assemble_serves_overlapping_files_without_duplicates_and_counts_them() {
        let mem = Mem::new();
        let first = keys_from(0, KEYS_PER_FILE);
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        let a = mem.keyed("a", &first, Some(bucketing(1)), 0).await;
        let b = mem.keyed("b", &interleaved, Some(bucketing(1)), 0).await;
        let generation = assemble(vec![a, b], layout(Some(bucketing(1))))
            .await
            .unwrap();
        assert_eq!(generation.overlapping_buckets(), &BTreeMap::from([(0, 2)]));
        assert_eq!(generation.overlap_groups_verified(), 1);
        check_present(&generation, &mem.storage, &first).await;
        check_present(&generation, &mem.storage, &interleaved).await;
        let cache = cache().await;
        assert_eq!(
            generation
                .get(&Key::Long(first[1] + 2), &cache)
                .await
                .unwrap(),
            Value::Absent
        );
    }

    #[tokio::test]
    async fn test_assemble_verifies_only_groups_that_gained_a_file() {
        let mem = Mem::new();
        let first = keys_from(0, KEYS_PER_FILE);
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        let beyond = keys_from(first.last().unwrap() + KEY_STEP, KEYS_PER_FILE);
        let a = mem.keyed("a", &first, Some(bucketing(1)), 0).await;
        let b = mem.keyed("b", &interleaved, Some(bucketing(1)), 0).await;
        let c = mem.keyed("c", &beyond, Some(bucketing(1)), 0).await;
        let one = layout(Some(bucketing(1)));

        let fresh = assemble_with(
            vec![(Arc::clone(&a), false), (Arc::clone(&b), false)],
            one.clone(),
        )
        .await
        .unwrap();
        assert_eq!(fresh.overlap_groups_verified(), 1);

        let disjoint_append = assemble_with(
            vec![
                (Arc::clone(&a), true),
                (Arc::clone(&b), true),
                (Arc::clone(&c), false),
            ],
            one.clone(),
        )
        .await
        .unwrap();
        assert_eq!(disjoint_append.overlap_groups_verified(), 0);
        assert_eq!(
            disjoint_append.overlapping_buckets(),
            &BTreeMap::from([(0, 2)])
        );

        let d = mem
            .keyed("d", &[first[0] + 2, beyond[0] + 2], Some(bucketing(1)), 0)
            .await;
        let joined = assemble_with(
            vec![
                (Arc::clone(&a), true),
                (Arc::clone(&b), true),
                (Arc::clone(&c), true),
                (d, false),
            ],
            one,
        )
        .await
        .unwrap();
        assert_eq!(joined.overlap_groups_verified(), 1, "d bridges both groups");
        assert_eq!(joined.overlapping_buckets(), &BTreeMap::from([(0, 4)]));
    }

    /// The GETs a load makes before any data file: the hint, the metadata,
    /// the manifest list, and one manifest.
    const METADATA_GETS: u64 = 4;
    /// One more GET per further manifest a snapshot lists, one per spec
    /// its files were written under.
    const MANIFEST_GET: u64 = 1;
    /// The GETs a new single-row-group file costs: its footer and its chunk.
    const FILE_GETS: u64 = 2;

    /// Every key of `keys` is `Present` with its value.
    async fn assert_present(generation: &Generation, keys: &[i64]) {
        let cache = cache().await;
        for key in keys {
            assert_eq!(
                generation.get(&Key::Long(*key), &cache).await.unwrap(),
                Value::Present(Bytes::from(value_for(*key))),
                "key {key}"
            );
        }
    }

    /// The successor of every key of `keys` is `Absent`.
    async fn assert_successors_absent(generation: &Generation, keys: &[i64]) {
        let cache = cache().await;
        for key in keys {
            assert_eq!(
                generation.get(&Key::Long(key + 1), &cache).await.unwrap(),
                Value::Absent,
                "key {}",
                key + 1
            );
        }
    }

    /// Appends to `table` one file in bucket 0 whose keys lie above every
    /// key of `all`, as `SECOND_SNAPSHOT`; returns the appended keys.
    async fn append_disjoint(table: &mut Table, all: &[i64]) -> Vec<i64> {
        let appended = keys_in_bucket(
            &keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS),
            bucketing(TWO),
            0,
        );
        table.add_long_file("f2", &appended, 0).await;
        table
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        appended
    }

    #[tokio::test]
    async fn test_load_end_to_end_then_reuses_unchanged_files_on_a_disjoint_append() {
        let (mut table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let first = table.load(&config, None, None).await.unwrap();
        assert_eq!(table.gets(), METADATA_GETS + 2 * FILE_GETS);
        assert_eq!(first.snapshot_id, SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(
            first.metadata_location,
            crate::catalog::version_file(&table.root(), FIRST_VERSION)
        );
        assert_eq!(first.layout.bucketing, Some(bucketing(TWO)));
        assert_eq!(first.residency, Residency::Full);
        let committed = TableSource::Static {
            location: table.root(),
        }
        .resolve(&table.storage)
        .await
        .unwrap()
        .snapshot_for_ref(TABLE, testing::MAIN)
        .unwrap();
        assert_eq!(
            first.snapshot_timestamp,
            time::timestamp_from_millis(committed.timestamp_ms()).unwrap()
        );
        assert_eq!(first.files.len(), 2);
        assert_eq!(first.records, approx::count(all.len()));
        assert_eq!(
            first.index_bytes,
            first
                .files
                .iter()
                .map(|file| file.index.resident_bytes())
                .sum::<usize>()
        );
        assert_eq!(first.overlap_groups_verified(), 0);
        for file in &first.files {
            assert_eq!(file.entry.bucketing, Some(bucketing(TWO)));
            assert_eq!(
                (file.entry.key_min.as_ref(), file.entry.key_max.as_ref()),
                (
                    Some(file.index.key_range().0),
                    Some(file.index.key_range().1)
                ),
                "the manifest bounds are the exact range"
            );
        }
        assert_present(&first, &all).await;
        assert_successors_absent(&first, &all).await;

        let appended = append_disjoint(&mut table, &all).await;
        let before = table.gets();
        let second = table.load(&config, None, Some(&first)).await.unwrap();
        assert_eq!(
            table.gets(),
            before + METADATA_GETS + FILE_GETS,
            "one new file's footer and chunk"
        );
        assert_eq!(second.snapshot_id, SnapshotId(SECOND_SNAPSHOT));
        assert_eq!(second.files.len(), 3);
        for old in &first.files {
            assert!(
                second.files.iter().any(|file| Arc::ptr_eq(file, old)),
                "{} is reused by pointer",
                old.entry.path
            );
        }
        assert_eq!(
            second.overlap_groups_verified(),
            0,
            "a disjoint append verifies nothing"
        );
        assert_eq!(second.overlapping_buckets().len(), 0);
        assert_present(&second, &appended).await;
        assert_present(&second, &all).await;
        assert_successors_absent(&second, &appended).await;
    }

    #[tokio::test]
    async fn test_load_reloads_a_path_whose_size_changed_instead_of_reusing_it() {
        let (mut table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let first = table.load(&config, None, None).await.unwrap();
        // Another file under f0's path: more keys of bucket 0, so its size
        // differs, and the manifest records the new size.
        let in_bucket_zero = keys_in_bucket(&all, bucketing(TWO), 0);
        let more = keys_in_bucket(
            &keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS),
            bucketing(TWO),
            0,
        );
        let replaced: Vec<i64> = in_bucket_zero.iter().chain(&more).copied().collect();
        table.remove_file("f0");
        table.add_long_file("f0", &replaced, 0).await;
        table
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        let before = table.gets();
        let second = table.load(&config, None, Some(&first)).await.unwrap();
        assert_eq!(
            table.gets(),
            before + METADATA_GETS + FILE_GETS,
            "the replaced file is loaded anew, the other reused"
        );
        let by_name = |generation: &Generation, name: &str| {
            let path = table.path_of(name);
            generation
                .files
                .iter()
                .find(|file| file.entry.path == path)
                .cloned()
                .unwrap()
        };
        assert!(Arc::ptr_eq(&by_name(&first, "f1"), &by_name(&second, "f1")));
        assert!(!Arc::ptr_eq(
            &by_name(&first, "f0"),
            &by_name(&second, "f0")
        ));
        assert_ne!(
            by_name(&first, "f0").entry.size,
            by_name(&second, "f0").entry.size
        );
        assert_present(&second, &replaced).await;
        assert_present(&second, &all).await;
    }

    #[tokio::test]
    async fn test_load_routes_files_under_an_evolved_bucket_count() {
        let (mut table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let first = table.load(&config, None, None).await.unwrap();
        table.evolve(FOUR);
        let more = keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS);
        for bucket in 0..FOUR {
            let keys = keys_in_bucket(&more, bucketing(FOUR), bucket);
            assert!(!keys.is_empty(), "bucket {bucket} holds keys");
            table
                .add_long_file(&format!("g{bucket}"), &keys, bucket)
                .await;
        }
        table
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        let before = table.gets();
        let second = table.load(&config, None, Some(&first)).await.unwrap();
        assert_eq!(
            table.gets(),
            before + METADATA_GETS + MANIFEST_GET + u64::from(FOUR) * FILE_GETS,
            "one manifest per spec, then the new files"
        );
        assert_eq!(
            second.layout.bucketing,
            Some(bucketing(FOUR)),
            "the default spec's"
        );
        let carried: HashSet<Option<Bucketing>> = second
            .files
            .iter()
            .map(|file| file.entry.bucketing)
            .collect();
        assert_eq!(
            carried,
            HashSet::from([Some(bucketing(TWO)), Some(bucketing(FOUR))])
        );
        for file in &second.files {
            let bucketing = file.entry.bucketing.unwrap();
            assert_eq!(
                file.entry.bucket,
                bucketing.bucket_of(file.index.key_range().0),
                "{} is in its bucket under its own spec",
                file.entry.path
            );
        }
        assert_eq!(
            second.overlap_groups_verified(),
            0,
            "the new keys lie above the old"
        );
        check_present(&second, &table.storage, &all).await;
        check_present(&second, &table.storage, &more).await;
        assert_successors_absent(&second, &more).await;
    }

    #[tokio::test]
    async fn test_load_verifies_the_group_a_new_file_joins() {
        let (mut table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let first = table.load(&config, None, None).await.unwrap();
        let appended = append_disjoint(&mut table, &all).await;
        let second = table.load(&config, None, Some(&first)).await.unwrap();

        // A file inside bucket 0's existing range, without duplicates,
        // joins an overlap group that this load verifies.
        let interleaved: Vec<i64> = keys_in_bucket(&all, bucketing(TWO), 0)
            .iter()
            .map(|key| key + 1)
            .filter(|key| bucketing(TWO).bucket_of(&Key::Long(*key)) == 0)
            .collect();
        assert!(interleaved.len() > 1);
        table.add_long_file("f3", &interleaved, 0).await;
        table
            .commit(
                THIRD_SNAPSHOT,
                Some(SECOND_SNAPSHOT),
                THIRD_SEQUENCE,
                THIRD_VERSION,
            )
            .await;
        let third = table.load(&config, None, Some(&second)).await.unwrap();
        assert_eq!(third.files.len(), 4);
        assert_eq!(third.overlap_groups_verified(), 1);
        assert_eq!(third.overlapping_buckets(), &BTreeMap::from([(0, 2)]));
        assert_present(&third, &interleaved).await;
        assert_present(&third, &all).await;
        assert_present(&third, &appended).await;
    }

    #[tokio::test]
    async fn test_load_refuses_a_duplicate_key_added_by_a_later_snapshot() {
        let (mut table, all) = two_bucket_table().await;
        let config = table.config(Residency::Full, None);
        let first = table.load(&config, None, None).await.unwrap();
        // The duplicate file starts above f0's first key, so it is the
        // later file in file order and the one the refusal names.
        let in_bucket_zero = keys_in_bucket(&all, bucketing(TWO), 0);
        table.add_long_file("dup", &in_bucket_zero[1..3], 0).await;
        table
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        assert_eq!(
            refusal_of(table.load(&config, None, Some(&first)).await),
            RefusalReason::DuplicateKeys {
                path: table.path_of("dup"),
                key: Key::Long(in_bucket_zero[1]),
            }
        );
        // The previous generation is untouched and keeps answering.
        assert_present(&first, &all).await;
        assert_successors_absent(&first, &all).await;
    }

    #[tokio::test]
    async fn test_load_full_budget_refuses_at_the_footer_stage() {
        let (table, _) = two_bucket_table().await;
        let config = table.config(Residency::Full, Some(TINY_BUDGET));
        let RefusalReason::OverBudget { needed, budget } =
            refusal_of(table.load(&config, None, None).await)
        else {
            panic!("expected an over-budget refusal");
        };
        assert_eq!(budget, TINY_BUDGET);
        assert_eq!(table.gets(), METADATA_GETS + 2, "two footers and no chunk");
        // The refusal carries exactly what the two footers predicted.
        let predicted: u64 = {
            let mut total = 0;
            for (file, _) in &table.files {
                let handle = Arc::new(
                    FileHandle::open(
                        Arc::clone(&table.storage),
                        file.file_path(),
                        file.file_size_in_bytes(),
                        Arc::clone(&table.hedge),
                    )
                    .unwrap(),
                );
                total += FileIndex::inspect(TABLE, handle, KEY_COLUMN, VALUE_COLUMN, KeyType::Long)
                    .await
                    .unwrap()
                    .predicted()
                    .full;
            }
            total
        };
        assert_eq!(needed, predicted);
        let generous = table.config(Residency::Full, Some(predicted));
        let loaded = table.load(&generous, None, None).await.unwrap();
        assert_eq!(approx::count(loaded.index_bytes), predicted);
    }

    #[tokio::test]
    async fn test_load_disk_budget_refuses_at_the_footer_stage_and_reuses_chunk_files() {
        let (table, all) = two_bucket_table().await;
        let scratch = tempfile::tempdir().unwrap();
        let config = table.config(Residency::Disk, None);
        let tight = local_dir(scratch.path(), table.storage.stats(), Some(TINY_BUDGET));
        let RefusalReason::OverBudget { needed, budget } =
            refusal_of(table.load(&config, Some(&tight), None).await)
        else {
            panic!("expected an over-budget refusal");
        };
        assert_eq!(budget, TINY_BUDGET);
        assert!(needed > budget);
        assert_eq!(table.gets(), METADATA_GETS + 2, "two footers and no chunk");
        assert_eq!(
            tight.index_bytes_in_use().unwrap(),
            0,
            "nothing was written"
        );

        let unbounded = local_dir(scratch.path(), table.storage.stats(), None);
        let loaded = table.load(&config, Some(&unbounded), None).await.unwrap();
        assert_eq!(loaded.residency, Residency::Disk);
        let on_disk = unbounded.index_bytes_in_use().unwrap();
        assert!(on_disk > 0);
        assert_eq!(loaded.identities().len(), 2);
        assert_eq!(
            unbounded.retain(&loaded.identities()).unwrap(),
            0,
            "every chunk file is live"
        );
        let cache = cache().await;
        for key in &all {
            assert_eq!(
                loaded.get(&Key::Long(*key), &cache).await.unwrap(),
                Value::Present(Bytes::from(value_for(*key)))
            );
        }

        // A restart over the same directory writes nothing, so a budget of
        // exactly what is on disk admits it and the load fetches footers only.
        let exact = local_dir(scratch.path(), table.storage.stats(), Some(on_disk));
        let before = table.gets();
        let restarted = table.load(&config, Some(&exact), None).await.unwrap();
        assert_eq!(table.gets(), before + METADATA_GETS + 2, "footers only");
        assert_eq!(restarted.files.len(), 2);
        assert_eq!(exact.index_bytes_in_use().unwrap(), on_disk);
        assert_eq!(
            unbounded.retain(&HashSet::new()).unwrap(),
            on_disk,
            "nothing live frees all"
        );
    }

    #[test]
    fn test_generation_and_lookups_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Generation>();
        assert_send_sync::<LoadedFile>();
        assert_send_sync::<Value>();
        assert_send_sync::<Lookup>();
        assert_send_sync::<MultiLookup>();
    }
}
