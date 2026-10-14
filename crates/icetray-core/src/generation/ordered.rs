//! An ordered table's uniqueness across files (RFD 025, Refresh): a key
//! and order value together live in at most one file. A load checks only
//! where a repeated live pair can occur: among the files of an overlap
//! group (`overlap_groups`) that holds a file that is new, or reused with
//! deletes that shrank (RFD 021). Such a checked file meets another when
//! one key can fall in both (RFD 005's rule across buckets and bucketings)
//! and their key ranges and order ranges both meet. One sweep of the group
//! in order-value order finds which files meet; then one k-way merge by
//! key and order value runs over every checked file that meets another,
//! read whole, and over the order pages of the group's other files that
//! can hold a checked file's rows, each read once. A group where no file
//! meets a checked one, as an append of later rows does, is merged with
//! nothing and reads nothing.

use std::cmp::Ordering;
use std::sync::Arc;

use futures::stream::{self, StreamExt as _, TryStreamExt as _};

use super::{DeletedRows, LoadedFile, Member, buckets_may_share_a_key, load_concurrency};
use crate::blocking;
use crate::deletes::FileDeletes;
use crate::error::{Error, RefusalReason, Result};
use crate::index::{Bounds, PairChunks, Pairs, RowPosition, RowSpan};
use crate::key::Key;
use crate::manifest::Bucketing;
use crate::order::OrderValue;

/// One file as the check across files sees it: the bucket its manifest
/// entry files it under, its exact first and last key, and its least and
/// greatest order value. A span's position in its slice stands for the
/// file's.
#[derive(Clone, Copy, Debug)]
pub(super) struct OrderedSpan<'a> {
    pub(super) bucketing: Option<Bucketing>,
    pub(super) bucket: u32,
    pub(super) keys: (&'a Key, &'a Key),
    pub(super) orders: (&'a OrderValue, &'a OrderValue),
}

impl<'a> OrderedSpan<'a> {
    /// The span of a file of an ordered table, from its key range and the
    /// order facts its verification recorded.
    pub(super) fn of(file: &'a LoadedFile) -> Self {
        let facts = file
            .index
            .order_facts()
            .expect("every file of an ordered table records its order facts");
        Self {
            bucketing: file.entry.bucketing,
            bucket: file.entry.bucket,
            keys: file.index.key_range(),
            orders: (&facts.min, &facts.max),
        }
    }

    /// The rows the file can hold, as another file's order pages are
    /// judged against them.
    fn bounds(self) -> Bounds<'a> {
        (self.keys, self.orders)
    }
}

/// Whether two closed ranges meet; ends that touch do.
fn meet<T: Ord + ?Sized>(a: (&T, &T), b: (&T, &T)) -> bool {
    a.0 <= b.1 && b.0 <= a.1
}

/// Whether the files `ours` and `theirs` span can share a live pair: one
/// key can fall in both their buckets (`buckets_may_share_a_key`) and their
/// key ranges and order ranges both meet, ends included.
fn is_counterpart(ours: OrderedSpan<'_>, theirs: OrderedSpan<'_>) -> bool {
    buckets_may_share_a_key(
        (ours.bucketing, ours.bucket),
        (theirs.bucketing, theirs.bucket),
    ) && meet(ours.keys, theirs.keys)
        && meet(ours.orders, theirs.orders)
}

/// What a load's check across files did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct RowCheck {
    /// The overlap groups whose check merged files.
    pub(super) merges: usize,
    /// The comparisons the check made: of order values while sweeping the
    /// groups, sorts included; of two files' spans; and of two cursors'
    /// pairs while merging.
    pub(super) compared: u64,
}

/// What the sweep of one overlap group found, by position in the group.
struct Meetings {
    /// Per member, for a checked one, whether another checked member is
    /// its counterpart (`is_counterpart`).
    joined: Vec<bool>,
    /// Per member, for one not checked, the checked members that are its
    /// counterparts, whose bounds its order pages are judged against.
    against: Vec<Vec<usize>>,
}

/// Sweeps `group`, positions in `spans` of the members of one overlap
/// group, in order-value order: by least order value, keeping the members
/// whose order range is still open, those whose greatest order value is
/// not below the next member's least. Each member is tested against the
/// open members only, every member it meets by order range, so the sweep
/// costs two sorts and a pass besides the pairs whose order ranges meet. A
/// pair of checked members both already joined is not tested again.
/// `checked` marks the checked files by position in `spans`; every
/// comparison is counted in `compared`.
fn meetings(
    spans: &[OrderedSpan<'_>],
    checked: &[bool],
    group: &[usize],
    compared: &mut u64,
) -> Meetings {
    let span = |member: usize| spans[group[member]];
    let mut by_least: Vec<usize> = (0..group.len()).collect();
    by_least.sort_by(|&a, &b| {
        *compared += 1;
        span(a).orders.0.cmp(span(b).orders.0)
    });
    let mut by_greatest: Vec<usize> = (0..group.len()).collect();
    by_greatest.sort_by(|&a, &b| {
        *compared += 1;
        span(a).orders.1.cmp(span(b).orders.1)
    });
    let mut found = Meetings {
        joined: vec![false; group.len()],
        against: vec![Vec::new(); group.len()],
    };
    let mut closed = vec![false; group.len()];
    let mut closing = 0;
    let mut open_checked: Vec<usize> = Vec::new();
    let mut open_other: Vec<usize> = Vec::new();
    for &member in &by_least {
        let ours = span(member);
        let before = closing;
        while let Some(&next) = by_greatest.get(closing) {
            *compared += 1;
            if span(next).orders.1 >= ours.orders.0 {
                break;
            }
            closed[next] = true;
            closing += 1;
        }
        if closing > before {
            open_checked.retain(|&open| !closed[open]);
            open_other.retain(|&open| !closed[open]);
        }
        if checked[group[member]] {
            for &open in &open_checked {
                if found.joined[member] && found.joined[open] {
                    continue;
                }
                *compared += 1;
                if is_counterpart(ours, span(open)) {
                    found.joined[member] = true;
                    found.joined[open] = true;
                }
            }
            for &open in &open_other {
                *compared += 1;
                if is_counterpart(ours, span(open)) {
                    found.against[open].push(member);
                }
            }
            open_checked.push(member);
        } else {
            for &open in &open_checked {
                *compared += 1;
                if is_counterpart(span(open), ours) {
                    found.against[member].push(open);
                }
            }
            open_other.push(member);
        }
    }
    found
}

/// What one file of a merge reads: all of its rows, for a checked file, or
/// the order pages that can hold a checked file's rows.
enum Read {
    /// Every row, by `FileIndex::all_pair_chunks`.
    Whole,
    /// The pages `FileIndex::order_pages_meeting` chose, by
    /// `FileIndex::order_page_chunks`.
    Pages(Vec<RowSpan>),
}

/// Checks every overlap group of `groups` (`overlap_groups`) that holds a
/// checked file of `files`, sorted as `members` is: one new, or reused with
/// deletes that shrank (RFD 021). Per group, the sweep (`meetings`) finds
/// which files meet a checked one; the order pages of each file not checked
/// that can hold a checked counterpart's rows are chosen without I/O
/// (`FileIndex::order_pages_meeting`), and a checked file whose only
/// counterparts hold no such page is left out. Every checked file left in
/// is read whole, and each chosen page once however many checked files it
/// meets; then one merge (`refuse_shared_pairs`) refuses a live pair two of
/// them share; `deletes` are the rows each file's deletes remove. A group
/// where nothing is left in reads nothing. The groups are checked one at a
/// time, each one's reads gathered `load_concurrency()` at a time before
/// its merge and dropped after, and each merge runs on the blocking pool
/// over owned data; a canceled one is `Error::VerifyCanceled` for the
/// group's first checked file read.
pub(super) async fn verify_rows(
    table: &str,
    members: &[Member],
    files: &[Arc<LoadedFile>],
    groups: &[Vec<usize>],
    deletes: &Arc<[Option<Arc<FileDeletes>>]>,
) -> Result<RowCheck> {
    let spans: Vec<OrderedSpan<'_>> = files.iter().map(|file| OrderedSpan::of(file)).collect();
    let checked: Vec<bool> = members
        .iter()
        .map(|member| !member.reused || member.shrank)
        .collect();
    let mut check = RowCheck::default();
    for group in groups {
        if !group.iter().any(|&at| checked[at]) {
            continue;
        }
        let mut found = meetings(&spans, &checked, group, &mut check.compared);
        let mut reads: Vec<(usize, Read)> = Vec::new();
        for (member, against) in found.against.iter().enumerate() {
            if against.is_empty() {
                continue;
            }
            let bounds: Vec<Bounds<'_>> = against
                .iter()
                .map(|&theirs| spans[group[theirs]].bounds())
                .collect();
            let (pages, met) = files[group[member]].index.order_pages_meeting(&bounds);
            for (&theirs, met) in against.iter().zip(met) {
                found.joined[theirs] |= met;
            }
            if !pages.is_empty() {
                reads.push((group[member], Read::Pages(pages)));
            }
        }
        reads.extend(
            (0..group.len())
                .filter(|&member| found.joined[member])
                .map(|member| (group[member], Read::Whole)),
        );
        if reads.is_empty() {
            continue;
        }
        check.compared += refuse_duplicate_rows(table, files, &checked, reads, deletes).await?;
        check.merges += 1;
    }
    Ok(check)
}

/// One file of a merge with the rows it reads, owned, so that the merge
/// can move to the blocking pool.
struct PairMember {
    /// The file's position in the generation's files.
    at: usize,
    /// Whether the load checks the file (`verify_rows`).
    checked: bool,
    file: Arc<LoadedFile>,
    chunks: PairChunks,
}

/// Reads what `reads` names of `files`, `load_concurrency()` at a time: a
/// whole file's rows by `FileIndex::all_pair_chunks`, chosen order pages by
/// `FileIndex::order_page_chunks`; then merges them on the blocking pool
/// (`refuse_shared_pairs`), the rows `deletes` removes not live. How many
/// comparisons the merge made.
async fn refuse_duplicate_rows(
    table: &str,
    files: &[Arc<LoadedFile>],
    checked: &[bool],
    reads: Vec<(usize, Read)>,
    deletes: &Arc<[Option<Arc<FileDeletes>>]>,
) -> Result<u64> {
    let first = reads
        .iter()
        .find(|(at, _)| checked[*at])
        .map(|(at, _)| files[*at].index.handle().location.clone())
        .expect("a merge reads a checked file whole");
    let members = stream::iter(reads)
        .map(|(at, read)| {
            let file = Arc::clone(&files[at]);
            let checked = checked[at];
            async move {
                let chunks = match read {
                    Read::Whole => file.index.all_pair_chunks().await?,
                    Read::Pages(pages) => file.index.order_page_chunks(pages).await?,
                };
                Ok::<_, Error>(PairMember {
                    at,
                    checked,
                    file,
                    chunks,
                })
            }
        })
        .buffered(load_concurrency())
        .try_collect::<Vec<_>>()
        .await?;
    let owned_table = table.to_string();
    let removed = Arc::clone(deletes);
    blocking::on_blocking_pool(move || {
        let gone = move |at: usize, position: RowPosition| {
            removed[at]
                .as_ref()
                .is_some_and(|rows| rows.contains(position))
        };
        refuse_shared_pairs(&owned_table, &members, &gone)
    })
    .await
    .map_err(|canceled| Error::verify_canceled(&first, canceled))?
}

/// A cursor over the live pairs one `PairMember` reads, in file order, one
/// span decoded at a time, passing over the rows `deleted` names.
struct PairCursor<'a> {
    member: &'a PairMember,
    deleted: &'a DeletedRows,
    /// The next span of the member's chunks to decode.
    span: usize,
    pairs: Pairs,
    /// The head's index in `pairs`.
    offset: usize,
}

impl<'a> PairCursor<'a> {
    /// A cursor on the first live pair of `member`; `None` when it reads
    /// none.
    fn open(member: &'a PairMember, deleted: &'a DeletedRows) -> Result<Option<Self>> {
        let mut cursor = Self {
            member,
            deleted,
            span: 0,
            pairs: Pairs::empty(),
            offset: 0,
        };
        let more = cursor.fill()?;
        Ok(cursor.past_deleted(more)?.then_some(cursor))
    }

    /// The pair under the cursor against the one under `other`.
    fn cmp_head(&self, other: &Self) -> Ordering {
        self.pairs.cmp_at(self.offset, &other.pairs, other.offset)
    }

    /// Moves to the next live pair; `false` once the member is exhausted.
    fn advance(&mut self) -> Result<bool> {
        let more = self.step()?;
        self.past_deleted(more)
    }

    /// Steps over deleted rows from the head while `more` holds; whether a
    /// live pair is under the cursor.
    fn past_deleted(&mut self, mut more: bool) -> Result<bool> {
        while more && (self.deleted)(self.member.at, self.pairs.position(self.offset)) {
            more = self.step()?;
        }
        Ok(more)
    }

    /// Moves to the next pair, deleted or not; `false` once the member is
    /// exhausted.
    fn step(&mut self) -> Result<bool> {
        self.offset += 1;
        if self.offset < self.pairs.len() {
            return Ok(true);
        }
        self.fill()
    }

    /// Decodes the next span that holds a pair; `false` once the member is
    /// exhausted.
    fn fill(&mut self) -> Result<bool> {
        while self.span < self.member.chunks.len() {
            self.pairs = self
                .member
                .file
                .index
                .decode_pairs(&self.member.chunks, self.span)?;
            self.span += 1;
            self.offset = 0;
            if !self.pairs.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Cursors in a binary min-heap by head, counting the comparisons of two
/// heads it makes.
struct Heads<'a> {
    cursors: Vec<PairCursor<'a>>,
    /// Indices into `cursors`, a heap by head.
    heap: Vec<usize>,
    compared: u64,
}

impl<'a> Heads<'a> {
    /// Every cursor of `cursors` in the heap.
    fn of(cursors: Vec<PairCursor<'a>>) -> Self {
        let mut heads = Self {
            heap: Vec::with_capacity(cursors.len()),
            cursors,
            compared: 0,
        };
        for cursor in 0..heads.cursors.len() {
            heads.push(cursor);
        }
        heads
    }

    /// The head of cursor `a` against that of cursor `b`.
    fn cmp(&mut self, a: usize, b: usize) -> Ordering {
        self.compared += 1;
        self.cursors[a].cmp_head(&self.cursors[b])
    }

    /// The cursor with the least head, left in the heap.
    fn peek(&self) -> Option<usize> {
        self.heap.first().copied()
    }

    /// Puts `cursor` in the heap by its head.
    fn push(&mut self, cursor: usize) {
        self.heap.push(cursor);
        let mut at = self.heap.len() - 1;
        while at > 0 {
            let parent = (at - 1) / 2;
            if self.cmp(self.heap[at], self.heap[parent]) != Ordering::Less {
                break;
            }
            self.heap.swap(at, parent);
            at = parent;
        }
    }

    /// Takes the cursor with the least head out of the heap.
    fn pop(&mut self) -> Option<usize> {
        if self.heap.is_empty() {
            return None;
        }
        let least = self.heap.swap_remove(0);
        let mut at = 0;
        loop {
            let left = 2 * at + 1;
            if left >= self.heap.len() {
                break;
            }
            let right = left + 1;
            let mut lesser = left;
            if right < self.heap.len()
                && self.cmp(self.heap[right], self.heap[left]) == Ordering::Less
            {
                lesser = right;
            }
            if self.cmp(self.heap[lesser], self.heap[at]) != Ordering::Less {
                break;
            }
            self.heap.swap(at, lesser);
            at = lesser;
        }
        Some(least)
    }
}

/// Merges the live pairs `members` read by key and order value, and
/// refuses the first pair two of them hold where one is checked, as
/// `RefusalReason::DuplicateRows` naming the latest copy in file order
/// (RFD 025, Uniqueness); a row `deleted` names is not live (RFD 021).
/// Every file is sorted by key and order value and free of repeated pairs
/// on its own (verification checked), and a file not checked reads a
/// subsequence of its rows, so equal heads are exactly a pair in several
/// files. Two files neither of which is checked were proven unique by the
/// generation that last checked them, so a tie between them alone passes.
/// The merge ends once every checked file is exhausted. How many
/// comparisons it made.
fn refuse_shared_pairs(table: &str, members: &[PairMember], deleted: &DeletedRows) -> Result<u64> {
    let mut cursors = Vec::with_capacity(members.len());
    for member in members {
        cursors.extend(PairCursor::open(member, deleted)?);
    }
    let mut checked = cursors
        .iter()
        .filter(|cursor| cursor.member.checked)
        .count();
    let mut heads = Heads::of(cursors);
    let mut tied = Vec::new();
    while checked > 0 {
        let least = heads
            .pop()
            .expect("a checked cursor not exhausted is in the heap");
        tied.push(least);
        while let Some(next) = heads.peek() {
            if heads.cmp(next, least) != Ordering::Equal {
                break;
            }
            tied.extend(heads.pop());
        }
        if tied.len() > 1
            && tied
                .iter()
                .any(|&cursor| heads.cursors[cursor].member.checked)
        {
            let later = tied
                .iter()
                .copied()
                .max_by_key(|&cursor| heads.cursors[cursor].member.at)
                .expect("a tie holds two cursors");
            let head = &heads.cursors[least];
            return Err(Error::refused(
                table,
                RefusalReason::DuplicateRows {
                    path: heads.cursors[later].member.file.entry.path.clone(),
                    key: head.pairs.key(head.offset),
                    order: Box::new(head.pairs.order(head.offset)),
                },
            ));
        }
        for cursor in tied.drain(..) {
            if heads.cursors[cursor].advance()? {
                heads.push(cursor);
            } else if heads.cursors[cursor].member.checked {
                checked -= 1;
            }
        }
    }
    Ok(heads.compared)
}

#[cfg(test)]
mod tests {
    use super::super::testing::{TABLE, bucketing, refusal_of};
    use super::super::{Span, overlap_groups, routes};
    use super::*;
    use crate::approx;
    use crate::config::Residency;
    use crate::index::testing as index_testing;
    use crate::key::KeyType;
    use crate::manifest::{self, DataFileEntry};
    use crate::order::OrderType;
    use crate::snapshot::{SequenceNumber, SnapshotId};
    use crate::storage::IoStats;

    /// Bucket counts of a table before and after a spec evolution: one key
    /// falls in buckets congruent modulo two under both.
    const TWO: u32 = 2;
    const FOUR: u32 = 4;

    /// Where a file is filed: its bucketing's count and its bucket, or
    /// unbucketed. Ordered as file order orders bucketings and buckets.
    type Place = Option<(u32, u32)>;

    /// One file of a fixture: its place, its key range, its order range.
    type Described = (Place, (Key, Key), (OrderValue, OrderValue));

    fn span(described: &Described) -> OrderedSpan<'_> {
        let (place, keys, orders) = described;
        OrderedSpan {
            bucketing: place.map(|(count, _)| bucketing(count)),
            bucket: place.map_or(0, |(_, bucket)| bucket),
            keys: (&keys.0, &keys.1),
            orders: (&orders.0, &orders.1),
        }
    }

    fn file(place: Place, keys: (i64, i64), orders: (i64, i64)) -> Described {
        (
            place,
            (Key::Long(keys.0), Key::Long(keys.1)),
            (OrderValue::Long(orders.0), OrderValue::Long(orders.1)),
        )
    }

    /// Every file's counterparts by a scan of every other file: the oracle
    /// the sweep within overlap groups is checked against.
    fn by_full_scan(spans: &[OrderedSpan<'_>]) -> Vec<Vec<usize>> {
        (0..spans.len())
            .map(|at| {
                (0..spans.len())
                    .filter(|&other| other != at && is_counterpart(spans[at], spans[other]))
                    .collect()
            })
            .collect()
    }

    /// The overlap groups of `spans`, which are in file order, as
    /// `assemble` finds them.
    fn groups_of(spans: &[OrderedSpan<'_>]) -> Vec<Vec<usize>> {
        let swept: Vec<Span<'_>> = spans
            .iter()
            .map(|span| Span {
                bucketing: span.bucketing,
                bucket: span.bucket,
                first: span.keys.0,
                last: span.keys.1,
                added_by: SnapshotId(0),
            })
            .collect();
        overlap_groups(&swept, &routes(&swept))
    }

    /// What the sweep of every group of `spans` finds with the files
    /// `checked` marks checked, by file position: per unchecked file, its
    /// checked counterparts, ascending; per checked file, whether a checked
    /// counterpart meets it.
    fn swept(spans: &[OrderedSpan<'_>], checked: &[bool]) -> (Vec<Vec<usize>>, Vec<bool>) {
        let mut against = vec![Vec::new(); spans.len()];
        let mut joined = vec![false; spans.len()];
        let mut compared = 0;
        for group in groups_of(spans) {
            let found = meetings(spans, checked, &group, &mut compared);
            for (member, &at) in group.iter().enumerate() {
                against[at] = found.against[member].iter().map(|&c| group[c]).collect();
                against[at].sort_unstable();
                joined[at] = found.joined[member];
            }
        }
        (against, joined)
    }

    /// The sweep, with each file in turn the one unchecked file, finds
    /// exactly the oracle's counterparts of it, and every checked file is
    /// joined exactly when a checked counterpart meets it.
    fn assert_sweep_is_the_full_scan(spans: &[OrderedSpan<'_>]) -> Vec<Vec<usize>> {
        let oracle = by_full_scan(spans);
        for at in 0..spans.len() {
            let checked: Vec<bool> = (0..spans.len()).map(|other| other != at).collect();
            let (against, joined) = swept(spans, &checked);
            assert_eq!(against[at], oracle[at], "the counterparts of {at}");
            for other in (0..spans.len()).filter(|&other| other != at) {
                assert!(against[other].is_empty(), "{other} is checked");
                let expected = oracle[other].iter().any(|&theirs| theirs != at);
                assert_eq!(joined[other], expected, "{other} beside unchecked {at}");
            }
        }
        oracle
    }

    #[test]
    fn test_meetings_meet_by_key_and_order_range_ends_included_in_buckets_that_share() {
        let files = [
            file(None, (0, 10), (0, 5)),
            file(Some((TWO, 0)), (0, 10), (0, 5)),
            file(Some((TWO, 0)), (0, 10), (6, 9)),
            file(Some((TWO, 0)), (10, 20), (5, 9)),
            file(Some((TWO, 0)), (11, 20), (0, 5)),
            file(Some((TWO, 1)), (0, 10), (0, 5)),
            file(Some((FOUR, 2)), (0, 10), (0, 5)),
            file(Some((FOUR, 3)), (0, 10), (0, 5)),
        ];
        let spans: Vec<OrderedSpan<'_>> = files.iter().map(span).collect();
        let found = assert_sweep_is_the_full_scan(&spans);
        assert_eq!(
            found[1],
            [0, 3, 6],
            "ends that touch meet; an order range or key range apart, or a bucket no key shares, does not"
        );
        assert_eq!(
            found[0],
            [1, 3, 5, 6, 7],
            "an unbucketed file shares every bucket"
        );
    }

    /// Files of a fixture spread over several groups.
    const SPREAD_FILES: u64 = 120;

    /// A multiplier and an increment of a linear congruential sequence,
    /// which spreads the fixture's places and ranges without a seed.
    const MIX: u64 = 6_364_136_223_846_793_005;
    const STEP: u64 = 1_442_695_040_888_963_407;

    /// The span of key and order values the spread fixture's ranges fall in,
    /// and the widest range it draws.
    const SPACE: u64 = 1_000;
    const WIDEST: u64 = 60;

    #[test]
    fn test_meetings_within_overlap_groups_are_those_of_a_full_scan() {
        let mut state = 0_u64;
        let mut draw = |below: u64| {
            state = state.wrapping_mul(MIX).wrapping_add(STEP);
            (state >> 33) % below
        };
        let range = |draw: &mut dyn FnMut(u64) -> u64| {
            let low = draw(SPACE);
            let high = low + draw(WIDEST);
            (i64::try_from(low).unwrap(), i64::try_from(high).unwrap())
        };
        let places: [Place; 7] = [
            None,
            Some((TWO, 0)),
            Some((TWO, 1)),
            Some((FOUR, 0)),
            Some((FOUR, 1)),
            Some((FOUR, 2)),
            Some((FOUR, 3)),
        ];
        let mut files: Vec<Described> = (0..SPREAD_FILES)
            .map(|_| {
                let place = places[usize::try_from(draw(approx::count(places.len()))).unwrap()];
                let keys = range(&mut draw);
                file(place, keys, range(&mut draw))
            })
            .collect();
        files.sort_by(|a, b| (a.0, &a.1.0).cmp(&(b.0, &b.1.0)));
        let spans: Vec<OrderedSpan<'_>> = files.iter().map(span).collect();
        let oracle = assert_sweep_is_the_full_scan(&spans);
        assert!(groups_of(&spans).len() > 1, "several overlap groups");
        let found: usize = oracle.iter().map(Vec::len).sum();
        let keys_meet_only = (0..spans.len())
            .flat_map(|at| (0..spans.len()).map(move |other| (at, other)))
            .filter(|&(at, other)| {
                at != other
                    && meet(spans[at].keys, spans[other].keys)
                    && !meet(spans[at].orders, spans[other].orders)
            })
            .count();
        assert!(found > 0, "some files are counterparts");
        assert!(keys_meet_only > 0, "some files meet by key alone");
    }

    /// One ordered file of a test, indexed over an in-memory store of its
    /// own, so the GETs and local reads its stats count are its own.
    struct Built {
        file: Arc<LoadedFile>,
        stats: Arc<IoStats>,
        /// The Disk tier's local directory, which outlives the index.
        _dir: Option<tempfile::TempDir>,
    }

    impl Built {
        /// GETs and local reads of the file so far.
        fn reads(&self) -> (u64, u64) {
            (self.stats.gets(), self.stats.local_reads())
        }
    }

    /// The rows of `rows`, (key, order value) pairs ascending, as file
    /// `name` of the first bucket of two, in pages of `page_rows` rows and
    /// one row group, indexed in `residency`.
    async fn built(
        name: &str,
        rows: &[(i64, i64)],
        page_rows: usize,
        residency: Residency,
    ) -> Built {
        let keys: Vec<i64> = rows.iter().map(|&(key, _)| key).collect();
        let orders: Vec<i64> = rows.iter().map(|&(_, order)| order).collect();
        let mut fixture =
            index_testing::Fixture::ordered(&keys, index_testing::long_orders(&orders));
        fixture.layout.page_rows = page_rows;
        fixture.layout.write_batch = page_rows;
        fixture.layout.row_group_rows = rows.len();
        let opened = index_testing::store(&fixture).await;
        let (index, dir) = match residency {
            Residency::Disk => {
                let dir = tempfile::tempdir().unwrap();
                let index = opened
                    .build_ordered_in(OrderType::Long, residency, dir.path())
                    .await
                    .unwrap();
                (index, Some(dir))
            }
            Residency::Full | Residency::Bloom => {
                let layout = index_testing::ordered_layout(KeyType::Long, OrderType::Long);
                (
                    opened.build_under(&layout, residency, None).await.unwrap(),
                    None,
                )
            }
        };
        let (first, last) = index.key_range();
        let (first, last) = (first.clone(), last.clone());
        Built {
            file: Arc::new(LoadedFile {
                entry: DataFileEntry {
                    path: name.to_string(),
                    bucket: 0,
                    bucketing: Some(bucketing(TWO)),
                    records: approx::count(rows.len()),
                    sequence: SequenceNumber(1),
                    added_by: SnapshotId(1),
                    size: opened.handle.size(),
                    key_min: Some(first),
                    key_max: Some(last),
                    partition: manifest::Partition::unpartitioned(),
                },
                index,
            }),
            stats: opened.stats(),
            _dir: dir,
        }
    }

    /// The check across files of a generation of `built`, which are in
    /// file order, each reused unshrunk where `reused` says so and new
    /// otherwise.
    async fn check_rows(built: &[Built], reused: &[bool]) -> Result<RowCheck> {
        let files: Vec<Arc<LoadedFile>> = built.iter().map(|one| Arc::clone(&one.file)).collect();
        let members: Vec<Member> = files
            .iter()
            .zip(reused)
            .map(|(file, &reused)| Member {
                file: Arc::clone(file),
                reused,
                deletes: None,
                shrank: false,
            })
            .collect();
        let spans: Vec<Span<'_>> = files.iter().map(|file| Span::of(file)).collect();
        let groups = overlap_groups(&spans, &routes(&spans));
        let deletes: Arc<[Option<Arc<FileDeletes>>]> = files.iter().map(|_| None).collect();
        verify_rows(TABLE, &members, &files, &groups, &deletes).await
    }

    /// Files a first load of an appended bucket holds: one per append, each
    /// over the bucket's keys at order values later than the last's.
    const APPENDS: usize = 2_000;

    /// Keys every append writes, one row each.
    const APPENDED_KEYS: i64 = 8;

    /// The most comparisons the check may make per row it holds: the sweep
    /// sorts the files twice and passes each once, so on an appended bucket
    /// it compares far fewer times than there are rows, where a search of
    /// the group per checked file compares about `APPENDS` squared halved.
    const COMPARED_PER_ROW: u64 = 2;

    /// Rows per page where a test does not care how many pages a file has.
    const WHOLE_PAGE_ROWS: usize = 1_000;

    /// A first load of a bucket of `APPENDS` appends, whose order ranges
    /// are disjoint, merges nothing (RFD 025, Refresh): no file is read
    /// again after its build, which read its order chunk once, and the
    /// check's comparisons are linear in its rows.
    #[tokio::test]
    async fn test_first_load_of_an_appended_bucket_reads_each_file_once() {
        let mut files = Vec::with_capacity(APPENDS);
        for append in 0..APPENDS {
            let first = i64::try_from(append).unwrap() * APPENDED_KEYS;
            let rows: Vec<(i64, i64)> = (0..APPENDED_KEYS).map(|key| (key, first + key)).collect();
            files.push(
                built(
                    &format!("a-{append:05}"),
                    &rows,
                    WHOLE_PAGE_ROWS,
                    Residency::Full,
                )
                .await,
            );
        }
        let built_reads: Vec<(u64, u64)> = files.iter().map(Built::reads).collect();
        assert!(
            built_reads.iter().all(|&(gets, _)| gets > 0),
            "each build reads its file"
        );
        let check = check_rows(&files, &[false; APPENDS]).await.unwrap();
        for (one, before) in files.iter().zip(&built_reads) {
            assert_eq!(
                one.reads(),
                *before,
                "{} is not read again",
                one.file.entry.path
            );
        }
        assert_eq!(check.merges, 0);
        let rows = approx::count(APPENDS) * approx::count(usize::try_from(APPENDED_KEYS).unwrap());
        assert!(
            check.compared > 0 && check.compared <= COMPARED_PER_ROW * rows,
            "{} comparisons over {rows} rows",
            check.compared
        );
    }

    /// Files of a first load whose order ranges all overlap.
    const OVERLAPPING: usize = 50;

    /// Keys of each overlapping file, and rows per key.
    const OVERLAPPING_KEYS: i64 = 4;
    const OVERLAPPING_ROWS_PER_KEY: i64 = 8;

    /// Rows per page of the overlapping files: each file's order column has
    /// many pages, every one of which meets every other file's ranges.
    const FEW_PAGE_ROWS: usize = 4;

    /// The reads a checked file's whole rows cost in one row group: under
    /// `full` the order chunk by GET, the key chunk being resident; under
    /// `bloom` both chunks by GET; under `disk` both from the chunk file.
    const FULL_CHECK_GETS: u64 = 1;
    const BLOOM_CHECK_GETS: u64 = 2;
    const DISK_CHECK_LOCAL_READS: u64 = 2;

    /// A first load of `OVERLAPPING` files whose order ranges all overlap
    /// merges them once, reading each file's order pages exactly once
    /// however many files they meet: every file is checked and read whole,
    /// in one read per column the tier does not hold.
    #[tokio::test]
    async fn test_first_load_reads_each_page_once_in_an_overlapping_group() {
        for residency in [Residency::Full, Residency::Disk, Residency::Bloom] {
            let mut files = Vec::new();
            let spread = i64::try_from(OVERLAPPING).unwrap();
            for at in 0..OVERLAPPING {
                let offset = i64::try_from(at).unwrap();
                let rows: Vec<(i64, i64)> = (0..OVERLAPPING_KEYS)
                    .flat_map(|key| {
                        (0..OVERLAPPING_ROWS_PER_KEY).map(move |row| (key, row * spread + offset))
                    })
                    .collect();
                files.push(built(&format!("o-{at:03}"), &rows, FEW_PAGE_ROWS, residency).await);
            }
            let before: Vec<(u64, u64)> = files.iter().map(Built::reads).collect();
            let check = check_rows(&files, &[false; OVERLAPPING]).await.unwrap();
            assert_eq!(check.merges, 1, "{residency:?}: one group, one merge");
            for (one, (gets, local)) in files.iter().zip(&before) {
                let (now_gets, now_local) = one.reads();
                let read = (now_gets - gets, now_local - local);
                let expected = match residency {
                    Residency::Full => (FULL_CHECK_GETS, 0),
                    Residency::Bloom => (BLOOM_CHECK_GETS, 0),
                    Residency::Disk => (0, DISK_CHECK_LOCAL_READS),
                };
                assert_eq!(read, expected, "{residency:?}: {}", one.file.entry.path);
            }
        }
    }

    /// The pair two unchanged files of the tie tests share, and the order
    /// values a new file holds around it.
    const SHARED: (i64, i64) = (0, 10);
    const BEFORE_SHARED: i64 = 5;
    const AFTER_SHARED: i64 = 15;

    /// Two reused files whose deletes did not shrink were proven unique by
    /// the previous generation, so a tie between them alone in a merge is no
    /// refusal (RFD 025, Refresh): both are in the merge, since the new
    /// file's ranges meet their pages, and the load passes. When the new
    /// file holds the pair too, the tie holds a checked file and is
    /// refused, naming the latest copy in file order.
    #[tokio::test]
    async fn test_a_tie_between_two_unchanged_files_is_not_refused() {
        let (key, order) = SHARED;
        let unchanged = [(key, order)];
        let around = [(key, BEFORE_SHARED), (key, AFTER_SHARED)];
        let files = [
            built("t-0", &unchanged, WHOLE_PAGE_ROWS, Residency::Full).await,
            built("t-1", &unchanged, WHOLE_PAGE_ROWS, Residency::Full).await,
            built("t-2", &around, WHOLE_PAGE_ROWS, Residency::Full).await,
        ];
        let before: Vec<(u64, u64)> = files.iter().map(Built::reads).collect();
        let check = check_rows(&files, &[true, true, false]).await.unwrap();
        assert_eq!(check.merges, 1);
        for (one, (gets, _)) in files.iter().zip(&before) {
            assert!(
                one.reads().0 > *gets,
                "{} is in the merge",
                one.file.entry.path
            );
        }

        let repeating = [(key, BEFORE_SHARED), (key, order), (key, AFTER_SHARED)];
        let files = [
            built("t-0", &unchanged, WHOLE_PAGE_ROWS, Residency::Full).await,
            built("t-1", &repeating, WHOLE_PAGE_ROWS, Residency::Full).await,
            built("t-2", &unchanged, WHOLE_PAGE_ROWS, Residency::Full).await,
        ];
        let RefusalReason::DuplicateRows {
            path,
            key: refused_key,
            order: refused_order,
        } = refusal_of(check_rows(&files, &[true, false, true]).await)
        else {
            panic!("expected duplicate_rows");
        };
        assert_eq!(path, "t-2", "the latest of the three copies");
        assert_eq!(refused_key, Key::Long(key));
        assert_eq!(*refused_order, OrderValue::Long(order));
    }
}
