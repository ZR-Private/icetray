//! An ordered table's uniqueness across files (RFD 025, Refresh): a key
//! and order value together live in at most one file. A load checks only
//! where a repeated live pair can occur: each file that is new, or reused
//! with deletes that shrank (RFD 021), against every file that can share a
//! key with it (RFD 005's rule across buckets and bucketings) and whose key
//! range and order range both meet its own. The checked file reads all of
//! its rows; each file it is checked against reads only the order pages
//! that can hold them. A file whose order range meets no such file's, as an
//! append of later rows does, is merged with nothing and reads nothing.

use std::cmp::Ordering;
use std::sync::Arc;

use futures::stream::{self, StreamExt as _, TryStreamExt as _};

use super::{DeletedRows, LoadedFile, Member, buckets_may_share_a_key, load_concurrency};
use crate::blocking;
use crate::deletes::FileDeletes;
use crate::error::{Error, RefusalReason, Result};
use crate::index::{PairChunks, Pairs, RowPosition, RowSpan};
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
}

/// Whether two closed ranges meet; ends that touch do.
fn meet<T: Ord + ?Sized>(a: (&T, &T), b: (&T, &T)) -> bool {
    a.0 <= b.1 && b.0 <= a.1
}

/// The files a refresh checks: new ones, and reused ones whose deletes
/// shrank, whose deleted copy of a pair may live again; by position in
/// `members`, ascending.
pub(super) fn checked(members: &[Member]) -> Vec<usize> {
    members
        .iter()
        .enumerate()
        .filter(|(_, member)| !member.reused || member.shrank)
        .map(|(at, _)| at)
        .collect()
}

/// Whether the file `theirs` spans is checked against the file `ours`
/// spans: one key can fall in both their buckets (`buckets_may_share_a_key`)
/// and their key ranges and order ranges both meet, ends included.
fn is_counterpart(ours: OrderedSpan<'_>, theirs: OrderedSpan<'_>) -> bool {
    buckets_may_share_a_key(
        (ours.bucketing, ours.bucket),
        (theirs.bucketing, theirs.bucket),
    ) && meet(ours.keys, theirs.keys)
        && meet(ours.orders, theirs.orders)
}

/// For the file at `at`, the files it is checked against
/// (`is_counterpart`), ascending, found among `group`: the members of its
/// overlap group (`overlap_groups`), ascending by least order value. The
/// groups link every two files whose key ranges meet and whose buckets one
/// key can fall in, so every counterpart is in the file's group; the scan
/// stops at the first member whose least order value is above the file's
/// greatest, which no later member's order range can meet.
pub(super) fn counterparts(spans: &[OrderedSpan<'_>], at: usize, group: &[usize]) -> Vec<usize> {
    let ours = spans[at];
    let mut found: Vec<usize> = group
        .iter()
        .copied()
        .take_while(|&other| spans[other].orders.0 <= ours.orders.1)
        .filter(|&other| other != at && is_counterpart(ours, spans[other]))
        .collect();
    found.sort_unstable();
    found
}

/// `groups`, overlap groups of the files `spans` describe, each sorted
/// ascending by least order value, and per file the group it is in;
/// `None` for a file in no group, which has no counterpart.
fn by_least_order(
    spans: &[OrderedSpan<'_>],
    groups: &[Vec<usize>],
) -> (Vec<Vec<usize>>, Vec<Option<usize>>) {
    let mut group_of = vec![None; spans.len()];
    let sorted = groups
        .iter()
        .enumerate()
        .map(|(index, group)| {
            for &at in group {
                group_of[at] = Some(index);
            }
            let mut sorted = group.clone();
            sorted.sort_by(|&a, &b| spans[a].orders.0.cmp(spans[b].orders.0));
            sorted
        })
        .collect();
    (sorted, group_of)
}

/// Checks every file of `files`, sorted as `members` is, that `checked`
/// names against its counterparts within its overlap group of `groups`
/// (`overlap_groups`), one checked file at a time, and refuses the first
/// live pair two files share; `deletes` are the rows each file's deletes
/// remove. Two checked files that are each other's counterparts are merged
/// once, from the earlier: a check reads every row of its file, so it finds
/// every pair the two share. How many checked files were merged with a
/// counterpart.
pub(super) async fn verify_rows(
    table: &str,
    members: &[Member],
    files: &[Arc<LoadedFile>],
    groups: &[Vec<usize>],
    deletes: &Arc<[Option<Arc<FileDeletes>>]>,
) -> Result<usize> {
    let spans: Vec<OrderedSpan<'_>> = files.iter().map(|file| OrderedSpan::of(file)).collect();
    let (groups, group_of) = by_least_order(&spans, groups);
    let checked = checked(members);
    let mut is_checked = vec![false; files.len()];
    for &at in &checked {
        is_checked[at] = true;
    }
    let mut merged = 0;
    for &at in &checked {
        let Some(group) = group_of[at] else {
            continue;
        };
        let against: Vec<usize> = counterparts(&spans, at, &groups[group])
            .into_iter()
            .filter(|&other| !is_checked[other] || other > at)
            .collect();
        if refuse_duplicate_rows(table, files, at, &against, deletes).await? {
            merged += 1;
        }
    }
    Ok(merged)
}

/// One file of a check with the rows it reads, owned, so that the merge
/// can move to the blocking pool.
struct PairMember {
    /// The file's position in the generation's files.
    at: usize,
    file: Arc<LoadedFile>,
    chunks: PairChunks,
}

/// The merge by key and order value of the file at `checked` in `files`
/// against the files at `counterparts`, refusing a live pair it shares with
/// one as `RefusalReason::DuplicateRows`, naming the later of the two in
/// file order (RFD 025, Uniqueness); a row `deletes` removes is not live
/// (RFD 021). Each counterpart reads only its order pages that can hold the
/// checked file's rows (`FileIndex::order_pages_within`), and the checked
/// file reads all of its rows (`FileIndex::all_pair_chunks`), the reads
/// `load_concurrency()` at a time; where no counterpart page can, nothing
/// is read and `Ok(false)` says no merge ran. The merge runs on the
/// blocking pool; a canceled one is `Error::VerifyCanceled` for the checked
/// file.
pub(super) async fn refuse_duplicate_rows(
    table: &str,
    files: &[Arc<LoadedFile>],
    checked: usize,
    counterparts: &[usize],
    deletes: &Arc<[Option<Arc<FileDeletes>>]>,
) -> Result<bool> {
    let file = Arc::clone(&files[checked]);
    let OrderedSpan { keys, orders, .. } = OrderedSpan::of(&file);
    let pages: Vec<(usize, Vec<RowSpan>)> = counterparts
        .iter()
        .map(|&at| (at, files[at].index.order_pages_within(keys, orders)))
        .filter(|(_, spans)| !spans.is_empty())
        .collect();
    if pages.is_empty() {
        return Ok(false);
    }
    let own = async {
        let chunks = file.index.all_pair_chunks().await?;
        Ok::<_, Error>(PairMember {
            at: checked,
            file: Arc::clone(&file),
            chunks,
        })
    };
    let others = stream::iter(pages)
        .map(|(at, spans)| {
            let file = Arc::clone(&files[at]);
            async move {
                let chunks = file.index.order_page_chunks(spans).await?;
                Ok::<_, Error>(PairMember { at, file, chunks })
            }
        })
        .buffered(load_concurrency())
        .try_collect::<Vec<_>>();
    let (own, others) = futures::try_join!(own, others)?;
    let path = file.index.handle().location.clone();
    let owned_table = table.to_string();
    let removed = Arc::clone(deletes);
    blocking::on_blocking_pool(move || {
        let gone = move |at: usize, position: RowPosition| {
            removed[at]
                .as_ref()
                .is_some_and(|rows| rows.contains(position))
        };
        refuse_shared_pairs(&owned_table, &own, &others, &gone)
    })
    .await
    .map_err(|canceled| Error::verify_canceled(&path, canceled))??;
    Ok(true)
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

/// Merges the live pairs of `own`, the checked file's rows, with those
/// `others` read, and refuses the first pair `own` shares with one of them.
/// Every file is sorted by key and order value and free of repeated pairs
/// on its own (verification checked), and each of `others` reads a
/// subsequence of its rows, so equal heads at the front of the merge are
/// exactly a pair in two files; equal heads among `others` alone are a pair
/// the check of one of them answers for. The cursors are kept in a vector
/// ordered by head; each step takes the least, advances it, and reinserts
/// it by binary search. The merge ends once `own` is exhausted.
fn refuse_shared_pairs(
    table: &str,
    own: &PairMember,
    others: &[PairMember],
    deleted: &DeletedRows,
) -> Result<()> {
    let Some(checked) = PairCursor::open(own, deleted)? else {
        return Ok(());
    };
    let mut cursors = vec![checked];
    for member in others {
        cursors.extend(PairCursor::open(member, deleted)?);
    }
    let mut order: Vec<usize> = (0..cursors.len()).collect();
    order.sort_by(|&a, &b| cursors[a].cmp_head(&cursors[b]));
    while order.len() > 1 {
        let least = order[0];
        let tied = order
            .iter()
            .take_while(|&&cursor| cursors[cursor].cmp_head(&cursors[least]) == Ordering::Equal)
            .count();
        if tied > 1 && order[..tied].contains(&0) {
            let later = order[..tied]
                .iter()
                .copied()
                .max_by_key(|&cursor| cursors[cursor].member.at)
                .expect("a tie holds two cursors");
            let head = &cursors[least];
            return Err(Error::refused(
                table,
                RefusalReason::DuplicateRows {
                    path: cursors[later].member.file.entry.path.clone(),
                    key: head.pairs.key(head.offset),
                    order: Box::new(head.pairs.order(head.offset)),
                },
            ));
        }
        order.remove(0);
        if cursors[least].advance()? {
            let insert_at = order.partition_point(|&cursor| {
                cursors[cursor].cmp_head(&cursors[least]) == Ordering::Less
            });
            order.insert(insert_at, least);
        } else if least == 0 {
            return Ok(());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::testing::bucketing;
    use super::super::{Span, overlap_groups, routes};
    use super::*;
    use crate::approx;

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
    /// the search within overlap groups is checked against.
    fn by_full_scan(spans: &[OrderedSpan<'_>]) -> Vec<Vec<usize>> {
        (0..spans.len())
            .map(|at| {
                (0..spans.len())
                    .filter(|&other| other != at && is_counterpart(spans[at], spans[other]))
                    .collect()
            })
            .collect()
    }

    /// Every file's counterparts as `verify_rows` finds them: within its
    /// overlap group, the groups found over `spans`, which are in file
    /// order, as `assemble` finds them; and how many groups there are.
    fn by_group(spans: &[OrderedSpan<'_>]) -> (Vec<Vec<usize>>, usize) {
        let swept: Vec<Span<'_>> = spans
            .iter()
            .map(|span| Span {
                bucketing: span.bucketing,
                bucket: span.bucket,
                first: span.keys.0,
                last: span.keys.1,
            })
            .collect();
        let groups = overlap_groups(&swept, &routes(&swept));
        let (groups, group_of) = by_least_order(spans, &groups);
        let found = (0..spans.len())
            .map(|at| {
                group_of[at].map_or_else(Vec::new, |group| counterparts(spans, at, &groups[group]))
            })
            .collect();
        (found, groups.len())
    }

    #[test]
    fn test_counterparts_meet_by_key_and_order_range_ends_included_in_buckets_that_share() {
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
        let (found, _) = by_group(&spans);
        assert_eq!(found, by_full_scan(&spans));
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
    fn test_counterparts_within_overlap_groups_are_those_of_a_full_scan() {
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
        let oracle = by_full_scan(&spans);
        let (grouped, groups) = by_group(&spans);
        assert_eq!(grouped, oracle);
        assert!(groups > 1, "{groups} overlap groups");
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
}
