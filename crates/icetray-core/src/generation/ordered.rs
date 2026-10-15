//! An ordered table's uniqueness across files (RFD 025, Refresh): a key
//! and order value together live in at most one file. A load checks only
//! where a repeated live pair can occur: among the files of an overlap
//! group (`overlap_groups`) that holds a file that is new, or reused with
//! deletes that shrank (RFD 021). Such a checked file meets another when
//! one key can fall in both (RFD 005's rule across buckets and bucketings)
//! and their key ranges and order ranges both meet (`meetings`). Of two
//! files that meet, the order pages that can hold a pair both hold are
//! chosen by the order fences, a page's pairs lying from its fence up to
//! the next page's (`chosen_pages`), and one k-way merge by key and order
//! value per group runs over the chosen pages, each read once, a row group
//! of each file at a time as the merge advances. A group where no file
//! meets a checked one, as an append of later rows does, is merged with
//! nothing and reads nothing.

use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap, VecDeque};
use std::sync::Arc;

use futures::stream::{self, StreamExt as _, TryStreamExt as _};

use super::{LoadedFile, Member, buckets_may_share_a_key, load_concurrency};
use crate::blocking;
use crate::deletes::FileDeletes;
use crate::error::{Error, RefusalReason, Result};
use crate::index::{OrderFences, Pair, PairChunks, PairPart, Pairs, RowPosition};
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

    /// The rows both `self` and `other` can hold: the key range and the
    /// order range they share, as fences compare them.
    fn shared(self, other: Self) -> Shared<'a> {
        Shared {
            keys: (
                PairPart::of_key(self.keys.0.max(other.keys.0)),
                PairPart::of_key(self.keys.1.min(other.keys.1)),
            ),
            orders: (
                PairPart::of_order(self.orders.0.max(other.orders.0)),
                PairPart::of_order(self.orders.1.min(other.orders.1)),
            ),
        }
    }
}

/// A closed key range and a closed order range two files that meet share:
/// where a pair both hold lies.
#[derive(Clone, Copy)]
struct Shared<'a> {
    keys: (PairPart<'a>, PairPart<'a>),
    orders: (PairPart<'a>, PairPart<'a>),
}

/// What a load's check across files did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct RowCheck {
    /// The overlap groups whose check merged files.
    pub(super) merges: usize,
    /// The comparisons the check made: of keys and order values while
    /// sweeping the groups, sorts and searches included; of two files'
    /// buckets; of order pages' pairs while choosing pages; and of two
    /// cursors' pairs while merging.
    pub(super) compared: u64,
    /// The most bytes of chosen pages one merge held at once.
    pub(super) held: u64,
}

/// The members of a sweep whose order ranges are open, by their closed
/// ranges of order ranks (`order_ranks`): a segment tree over the ranks
/// whose every node lists the open members whose range it covers in the
/// range's canonical decomposition, and the open members by the rank their
/// range starts at. A member whose range holds a rank is listed at exactly
/// one node on that rank's path to the root, so `meeting` finds each open
/// member whose range meets a range once, at the cost of the path, the
/// members it reports, and closed members it passes over, each of which
/// it drops from the node it found them at.
struct OpenRanges {
    /// The tree's leaves, a power of two at or above the ranks.
    leaves: usize,
    nodes: Vec<Vec<usize>>,
    starts: BTreeSet<(usize, usize)>,
    open: Vec<bool>,
    start_of: Vec<usize>,
}

impl OpenRanges {
    /// No member open, over `ranks` ranks and `members` members.
    fn new(ranks: usize, members: usize) -> Self {
        let leaves = ranks.next_power_of_two();
        Self {
            leaves,
            nodes: vec![Vec::new(); 2 * leaves],
            starts: BTreeSet::new(),
            open: vec![false; members],
            start_of: vec![0; members],
        }
    }

    /// Opens `member`, whose order range is the ranks `range` spans.
    fn open(&mut self, member: usize, range: (usize, usize)) {
        self.open[member] = true;
        self.start_of[member] = range.0;
        self.starts.insert((range.0, member));
        let (mut low, mut high) = (range.0 + self.leaves, range.1 + self.leaves + 1);
        while low < high {
            if low % 2 == 1 {
                self.nodes[low].push(member);
                low += 1;
            }
            if high % 2 == 1 {
                high -= 1;
                self.nodes[high].push(member);
            }
            low /= 2;
            high /= 2;
        }
    }

    /// Closes `member`, if open.
    fn close(&mut self, member: usize) {
        if std::mem::take(&mut self.open[member]) {
            self.starts.remove(&(self.start_of[member], member));
        }
    }

    /// Adds to `met` every open member whose range meets the ranks `range`
    /// spans: those whose range holds its start, and those whose range
    /// starts past its start and at or before its end.
    fn meeting(&mut self, range: (usize, usize), met: &mut Vec<usize>) {
        let mut node = range.0 + self.leaves;
        while node > 0 {
            let open = &self.open;
            self.nodes[node].retain(|&member| open[member]);
            met.extend_from_slice(&self.nodes[node]);
            node /= 2;
        }
        if range.0 < range.1 {
            met.extend(
                self.starts
                    .range((range.0 + 1, 0)..=(range.1, usize::MAX))
                    .map(|&(_, member)| member),
            );
        }
    }
}

/// Per member of `orders`, its closed order range as ranks among the
/// distinct order values the ranges end at, and how many there are: one
/// sort of the ends, which takes a pass where they already ascend, as an
/// appended bucket's do, and one pass over them. Every comparison is
/// counted in `compared`.
fn order_ranks(
    orders: &[(&OrderValue, &OrderValue)],
    compared: &mut u64,
) -> (Vec<(usize, usize)>, usize) {
    let mut ends: Vec<(usize, bool, &OrderValue)> = orders
        .iter()
        .enumerate()
        .flat_map(|(member, &(low, high))| [(member, false, low), (member, true, high)])
        .collect();
    ends.sort_by(|a, b| {
        *compared += 1;
        a.2.cmp(b.2)
    });
    let mut ranks = vec![(0, 0); orders.len()];
    let mut rank = 0;
    let mut previous: Option<&OrderValue> = None;
    for &(member, high, value) in &ends {
        if let Some(previous) = previous {
            *compared += 1;
            if previous != value {
                rank += 1;
            }
        }
        previous = Some(value);
        if high {
            ranks[member].1 = rank;
        } else {
            ranks[member].0 = rank;
        }
    }
    (ranks, rank + 1)
}

/// Per member of `group`, positions in `spans` of the members of one
/// overlap group, the members it meets where one of the two is checked, by
/// position in the group: one key can fall in both their buckets
/// (`buckets_may_share_a_key`) and their key ranges and order ranges both
/// meet, ends included. `checked` marks the checked files by position in
/// `spans`. One sweep of the group by first key keeps the members whose key
/// range is still open, those whose last key is not below the next
/// member's first, by order range (`OpenRanges`); each member finds the
/// open members its order range meets, the open checked members alone when
/// it is not checked, and keeps those a key can share a bucket with. The
/// sweep costs a few sorts, a logarithmic step per member, and a step per
/// pair of members whose key and order ranges both meet, whichever
/// dimension the group's files overlap in. Every comparison is counted in
/// `compared`.
fn meetings(
    spans: &[OrderedSpan<'_>],
    checked: &[bool],
    group: &[usize],
    compared: &mut u64,
) -> Vec<Vec<usize>> {
    let span = |member: usize| spans[group[member]];
    let orders: Vec<_> = (0..group.len()).map(|member| span(member).orders).collect();
    let (ranks, distinct) = order_ranks(&orders, compared);
    let mut by_first: Vec<usize> = (0..group.len()).collect();
    by_first.sort_by(|&a, &b| {
        *compared += 1;
        span(a).keys.0.cmp(span(b).keys.0)
    });
    let mut by_last: Vec<usize> = (0..group.len()).collect();
    by_last.sort_by(|&a, &b| {
        *compared += 1;
        span(a).keys.1.cmp(span(b).keys.1)
    });
    let mut every = OpenRanges::new(distinct, group.len());
    let mut of_checked = OpenRanges::new(distinct, group.len());
    let mut found = vec![Vec::new(); group.len()];
    let mut met = Vec::new();
    let mut closing = 0;
    for &member in &by_first {
        let ours = span(member);
        while let Some(&next) = by_last.get(closing) {
            *compared += 1;
            if span(next).keys.1 >= ours.keys.0 {
                break;
            }
            every.close(next);
            of_checked.close(next);
            closing += 1;
        }
        let is_checked = checked[group[member]];
        let open = if is_checked {
            &mut every
        } else {
            &mut of_checked
        };
        open.meeting(ranks[member], &mut met);
        for theirs in met.drain(..) {
            *compared += 1;
            let other = span(theirs);
            if buckets_may_share_a_key(
                (ours.bucketing, ours.bucket),
                (other.bucketing, other.bucket),
            ) {
                found[member].push(theirs);
                found[theirs].push(member);
            }
        }
        every.open(member, ranks[member]);
        if is_checked {
            of_checked.open(member, ranks[member]);
        }
    }
    found
}

/// The top of an order page's pairs: below the next page's fence, or, on
/// a file's last page, at or below its last key with its greatest order
/// value.
#[derive(Clone, Copy, Debug)]
enum Top<'a> {
    Below(Pair<'a>),
    AtMost(Pair<'a>),
}

impl<'a> Top<'a> {
    /// The pair the top is set by.
    fn pair(self) -> Pair<'a> {
        match self {
            Self::Below(pair) | Self::AtMost(pair) => pair,
        }
    }

    /// Whether `pair` lies under the top.
    fn covers(self, pair: Pair<'_>) -> bool {
        match self {
            Self::Below(top) => pair < top,
            Self::AtMost(top) => pair <= top,
        }
    }

    /// The lower of `self` and `other`, the one a pair below both lies
    /// under.
    fn lower(self, other: Self) -> Self {
        match self.pair().cmp(&other.pair()) {
            Ordering::Less => self,
            Ordering::Greater => other,
            Ordering::Equal => match (self, other) {
                (Self::AtMost(_), Self::Below(_)) => other,
                (Self::Below(_), Self::AtMost(_) | Self::Below(_))
                | (Self::AtMost(_), Self::AtMost(_)) => self,
            },
        }
    }
}

/// Whether a pair from `low` up to `top` can lie in `shared`: within one
/// key, the key in the shared keys and the order values from `low`'s up to
/// the top's meeting the shared orders; across keys, `low` at or below the
/// shared range's greatest pair and its least under the top. Each is what
/// a pair in both the range and `shared` requires, so a range for which it
/// fails holds no such pair.
fn may_hold(low: Pair<'_>, top: Top<'_>, shared: Shared<'_>) -> bool {
    let (keys, orders) = (shared.keys, shared.orders);
    let key = top.pair().key;
    if low.key == key {
        keys.0 <= key
            && key <= keys.1
            && low.order <= orders.1
            && top.covers(Pair {
                key,
                order: orders.0,
            })
    } else {
        low <= Pair {
            key: keys.1,
            order: orders.1,
        } && top.covers(Pair {
            key: keys.0,
            order: orders.0,
        })
    }
}

/// One order page of a member of a group as `chosen_pages` sweeps it: its
/// pairs from `low` up to `top`, and whether it is chosen so far.
struct PageRange<'a> {
    row_group: usize,
    page: usize,
    low: Pair<'a>,
    top: Top<'a>,
    chosen: bool,
}

/// A walk over one file's order pages in pair order: the row group and
/// page it stands on, its fences, and the top of its last page.
struct PageWalk<'a> {
    fences: &'a OrderFences,
    last: Pair<'a>,
    row_group: usize,
    page: usize,
}

impl<'a> PageWalk<'a> {
    /// The walk from the first order page of the file `fences` and `last`,
    /// its last key with its greatest order value, describe; `None` for a
    /// file without one.
    fn first(fences: &'a OrderFences, last: Pair<'a>) -> Option<Self> {
        Self {
            fences,
            last,
            row_group: 0,
            page: 0,
        }
        .on_a_page()
    }

    /// The walk from the first page at or after where it stands.
    fn on_a_page(mut self) -> Option<Self> {
        while self.row_group < self.fences.row_groups()
            && self.page >= self.fences.pages(self.row_group)
        {
            self.row_group += 1;
            self.page = 0;
        }
        (self.row_group < self.fences.row_groups()).then_some(self)
    }

    /// The walk from the page after the one it stands on.
    fn next(mut self) -> Option<Self> {
        self.page += 1;
        self.on_a_page()
    }

    /// The page it stands on.
    fn range(&self) -> PageRange<'a> {
        let (low, next) = self.fences.page_pairs(self.row_group, self.page);
        PageRange {
            row_group: self.row_group,
            page: self.page,
            low,
            top: next.map_or(Top::AtMost(self.last), Top::Below),
            chosen: false,
        }
    }
}

/// A member's next page in `chosen_pages`' sweep, in a max-heap that pops
/// the least fence first, the lesser member first among equal ones, every
/// comparison counted in `compared`.
struct Next<'a, 'c> {
    low: Pair<'a>,
    member: usize,
    compared: &'c Cell<u64>,
}

impl PartialEq for Next<'_, '_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Next<'_, '_> {}

impl PartialOrd for Next<'_, '_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Next<'_, '_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compared.set(self.compared.get() + 1);
        other
            .low
            .cmp(&self.low)
            .then_with(|| other.member.cmp(&self.member))
    }
}

/// One row group of a file and the order pages of it a merge reads,
/// ascending.
type GroupPages = (usize, Vec<usize>);

/// `page` after `groups`, the pages chosen of one file so far in file
/// order, when it is chosen.
fn keep(groups: &mut Vec<GroupPages>, page: &PageRange<'_>) {
    if !page.chosen {
        return;
    }
    match groups.last_mut() {
        Some((row_group, pages)) if *row_group == page.row_group => pages.push(page.page),
        Some(_) | None => groups.push((page.row_group, vec![page.page])),
    }
}

/// Per member of `group` that `meetings` found `counterparts` for, the
/// order pages that can hold a pair one of its counterparts holds, by the
/// fences: of two pages, one of each file, whose pairs' ranges meet, both
/// are chosen when a pair both files' ranges share can lie in the range
/// they meet in (`may_hold`). One k-way merge of those members' pages by
/// fence, each member's pages already in pair order (`PageWalk`), keeps
/// each member's latest page, the one whose range holds the merge's pair
/// when any does, since a file's pages tile its pairs; each page is tested
/// against the latest page of each counterpart, and a counterpart's later
/// pages test it in turn. The cost is a heap step per page and a step per
/// page per counterpart, and what it holds besides the pages chosen is a
/// page per member. Each member chosen with its file's position in `files`
/// and its chosen pages per row group, in file order.
fn chosen_pages(
    spans: &[OrderedSpan<'_>],
    files: &[Arc<LoadedFile>],
    group: &[usize],
    counterparts: &[Vec<usize>],
    compared: &mut u64,
) -> Vec<(usize, Vec<GroupPages>)> {
    let counted = Cell::new(0);
    let mut walks: Vec<Option<PageWalk<'_>>> = Vec::with_capacity(group.len());
    let mut heap = BinaryHeap::new();
    for (member, theirs) in counterparts.iter().enumerate() {
        let walk = if theirs.is_empty() {
            None
        } else {
            let fences = files[group[member]]
                .index
                .order_fences()
                .expect("every file of an ordered table records its order fences");
            let span = spans[group[member]];
            PageWalk::first(fences, Pair::of(span.keys.1, span.orders.1))
        };
        if let Some(walk) = &walk {
            heap.push(Next {
                low: walk.range().low,
                member,
                compared: &counted,
            });
        }
        walks.push(walk);
    }
    let mut latest: Vec<Option<PageRange<'_>>> = (0..group.len()).map(|_| None).collect();
    let mut reads: Vec<Vec<GroupPages>> = vec![Vec::new(); group.len()];
    while let Some(Next { member, .. }) = heap.pop() {
        let walk = walks[member]
            .take()
            .expect("a member in the heap stands on a page");
        let mut ours = walk.range();
        if let Some(before) = latest[member].take() {
            keep(&mut reads[member], &before);
        }
        for &theirs in &counterparts[member] {
            let Some(page) = latest[theirs].as_mut() else {
                continue;
            };
            *compared += 1;
            if !page.top.covers(ours.low) {
                continue;
            }
            let shared = spans[group[member]].shared(spans[group[theirs]]);
            if may_hold(ours.low, ours.top.lower(page.top), shared) {
                ours.chosen = true;
                page.chosen = true;
            }
        }
        latest[member] = Some(ours);
        walks[member] = walk.next();
        if let Some(walk) = &walks[member] {
            heap.push(Next {
                low: walk.range().low,
                member,
                compared: &counted,
            });
        }
    }
    drop(heap);
    *compared += counted.get();
    for (member, page) in latest.into_iter().enumerate() {
        if let Some(page) = page {
            keep(&mut reads[member], &page);
        }
    }
    reads
        .into_iter()
        .enumerate()
        .filter(|(_, groups)| !groups.is_empty())
        .map(|(member, groups)| (group[member], groups))
        .collect()
}

/// Checks every overlap group of `groups` (`overlap_groups`) that holds a
/// checked file of `files`, sorted as `members` is: one new, or reused with
/// deletes that shrank (RFD 021). Per group, the sweep (`meetings`) finds
/// which files meet, one of each two checked; the order pages that can
/// hold a pair two of them share are chosen without I/O (`chosen_pages`);
/// then one merge (`refuse_duplicate_rows`) reads each chosen page once
/// and refuses a live pair two files share where one is checked; `deletes`
/// are the rows each file's deletes remove. A group where no page is
/// chosen reads nothing. The groups are checked one at a time.
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
        let counterparts = meetings(&spans, &checked, group, &mut check.compared);
        let reads = chosen_pages(&spans, files, group, &counterparts, &mut check.compared);
        if reads.is_empty() {
            continue;
        }
        let merged = refuse_duplicate_rows(table, files, &checked, reads, deletes).await?;
        check.compared += merged.compared;
        check.held = check.held.max(merged.held);
        check.merges += 1;
    }
    Ok(check)
}

/// What one merge did: its comparisons of two cursors' pairs, and the most
/// bytes of chosen pages it held at once.
struct Merged {
    compared: u64,
    held: u64,
}

/// Merges the chosen pages `reads` names of `files`
/// (`MergeState::resume`), the rows `deletes` removes not live, reading
/// each file's pages a row group at a time as its cursor reaches them: the
/// row groups every cursor waits on are read `load_concurrency()` at a time
/// (`FileIndex::pair_chunks`), each after the cursor dropped the row group
/// before, and the merge then resumes on the blocking pool over owned data
/// until a cursor waits again or the merge ends. A canceled step is
/// `Error::VerifyCanceled` for the merge's first checked file.
async fn refuse_duplicate_rows(
    table: &str,
    files: &[Arc<LoadedFile>],
    checked: &[bool],
    reads: Vec<(usize, Vec<GroupPages>)>,
    deletes: &Arc<[Option<Arc<FileDeletes>>]>,
) -> Result<Merged> {
    let first = reads
        .iter()
        .find(|(at, _)| checked[*at])
        .map(|(at, _)| files[*at].index.handle().location.clone())
        .expect("a merge reads pages of a checked file");
    let cursors = reads
        .into_iter()
        .map(|(at, groups)| PairCursor::new(at, checked[at], Arc::clone(&files[at]), groups))
        .collect();
    let mut merge = MergeState::new(table, cursors, Arc::clone(deletes));
    loop {
        let fetched = stream::iter(merge.wanted())
            .map(|(cursor, file, (row_group, pages))| async move {
                let chunks = file.index.pair_chunks(row_group, &pages).await?;
                Ok::<_, Error>((cursor, chunks))
            })
            .buffered(load_concurrency())
            .try_collect::<Vec<_>>()
            .await?;
        let (resumed, progress) = blocking::on_blocking_pool(move || {
            let progress = merge.resume(fetched);
            (merge, progress)
        })
        .await
        .map_err(|canceled| Error::verify_canceled(&first, canceled))?;
        merge = resumed;
        match progress? {
            Progress::Waiting => {}
            Progress::Done => return Ok(merge.merged()),
        }
    }
}

/// Where a cursor stands after a step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Head {
    /// On a live pair.
    Live,
    /// Its row group's pages are spent and another row group is to read.
    Waiting,
    /// Exhausted.
    Done,
}

/// Whether the row at `position` of the file at `at` is one `deletes`
/// removes.
fn deleted(deletes: &[Option<Arc<FileDeletes>>], at: usize, position: RowPosition) -> bool {
    deletes[at]
        .as_ref()
        .is_some_and(|rows| rows.contains(position))
}

/// A cursor over the live pairs of one file's chosen pages, in file order,
/// one row group's pages held and one run of them decoded at a time,
/// passing over deleted rows.
struct PairCursor {
    /// The file's position in the generation's files.
    at: usize,
    /// Whether the load checks the file (`verify_rows`).
    checked: bool,
    file: Arc<LoadedFile>,
    /// The row groups left to read, with their chosen pages, in file order.
    rest: VecDeque<GroupPages>,
    /// The row group being read, until its pages are spent.
    chunks: Option<PairChunks>,
    /// The next run of `chunks` to decode.
    span: usize,
    pairs: Pairs,
    /// The head's index in `pairs`.
    offset: usize,
}

impl PairCursor {
    /// A cursor before the first of `groups` of the file `file` at `at`.
    fn new(at: usize, checked: bool, file: Arc<LoadedFile>, groups: Vec<GroupPages>) -> Self {
        Self {
            at,
            checked,
            file,
            rest: groups.into(),
            chunks: None,
            span: 0,
            pairs: Pairs::empty(),
            offset: 0,
        }
    }

    /// The pair under the cursor against the one under `other`.
    fn cmp_head(&self, other: &Self) -> Ordering {
        self.pairs.cmp_at(self.offset, &other.pairs, other.offset)
    }

    /// Moves onto the first live pair of `chunks`, the next row group's
    /// pages.
    fn resume(&mut self, chunks: PairChunks, deletes: &[Option<Arc<FileDeletes>>]) -> Result<Head> {
        self.chunks = Some(chunks);
        self.span = 0;
        let head = self.fill()?;
        self.past_deleted(head, deletes)
    }

    /// Moves to the next live pair.
    fn advance(&mut self, deletes: &[Option<Arc<FileDeletes>>]) -> Result<Head> {
        let head = self.step()?;
        self.past_deleted(head, deletes)
    }

    /// Steps over deleted rows from the head while it is on a pair.
    fn past_deleted(
        &mut self,
        mut head: Head,
        deletes: &[Option<Arc<FileDeletes>>],
    ) -> Result<Head> {
        while head == Head::Live && deleted(deletes, self.at, self.pairs.position(self.offset)) {
            head = self.step()?;
        }
        Ok(head)
    }

    /// Moves to the next pair, deleted or not.
    fn step(&mut self) -> Result<Head> {
        self.offset += 1;
        if self.offset < self.pairs.len() {
            return Ok(Head::Live);
        }
        self.fill()
    }

    /// Decodes the next run of the row group held that holds a pair.
    fn fill(&mut self) -> Result<Head> {
        if let Some(chunks) = &self.chunks {
            while self.span < chunks.len() {
                self.pairs = self.file.index.decode_pairs(chunks, self.span)?;
                self.span += 1;
                self.offset = 0;
                if !self.pairs.is_empty() {
                    return Ok(Head::Live);
                }
            }
        }
        Ok(if self.rest.is_empty() {
            Head::Done
        } else {
            Head::Waiting
        })
    }
}

/// Cursors in a binary min-heap by head, counting the comparisons of two
/// heads it makes.
struct Heads {
    cursors: Vec<PairCursor>,
    /// Indices into `cursors`, a heap by head.
    heap: Vec<usize>,
    compared: u64,
}

impl Heads {
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

/// Whether a merge can go on, or waits on row groups to be read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Progress {
    Waiting,
    Done,
}

/// One merge between its reads, owned, so that each step can move to the
/// blocking pool: the cursors, those of them waiting on their next row
/// group, the checked cursors not yet exhausted, and the bytes of chosen
/// pages held.
struct MergeState {
    table: String,
    heads: Heads,
    waiting: Vec<usize>,
    checked: usize,
    deletes: Arc<[Option<Arc<FileDeletes>>]>,
    held: u64,
    most_held: u64,
}

impl MergeState {
    /// A merge of `cursors`, every one waiting on its first row group.
    fn new(
        table: &str,
        cursors: Vec<PairCursor>,
        deletes: Arc<[Option<Arc<FileDeletes>>]>,
    ) -> Self {
        let checked = cursors.iter().filter(|cursor| cursor.checked).count();
        Self {
            table: table.to_string(),
            waiting: (0..cursors.len()).collect(),
            heads: Heads {
                heap: Vec::with_capacity(cursors.len()),
                cursors,
                compared: 0,
            },
            checked,
            deletes,
            held: 0,
            most_held: 0,
        }
    }

    /// The row groups the waiting cursors read next, each with its cursor
    /// and its file; none waits once they are handed out.
    fn wanted(&mut self) -> Vec<(usize, Arc<LoadedFile>, GroupPages)> {
        self.waiting
            .drain(..)
            .map(|cursor| {
                let cursor_of = &mut self.heads.cursors[cursor];
                let next = cursor_of
                    .rest
                    .pop_front()
                    .expect("a waiting cursor has a row group to read");
                (cursor, Arc::clone(&cursor_of.file), next)
            })
            .collect()
    }

    /// Hands each cursor of `fetched` its next row group's pages, then
    /// merges until a cursor waits or the merge ends.
    fn resume(&mut self, fetched: Vec<(usize, PairChunks)>) -> Result<Progress> {
        self.held += fetched
            .iter()
            .map(|(_, chunks)| chunks.read_bytes())
            .sum::<u64>();
        self.most_held = self.most_held.max(self.held);
        for (cursor, chunks) in fetched {
            let head = self.heads.cursors[cursor].resume(chunks, &self.deletes)?;
            self.place(cursor, head);
        }
        if !self.waiting.is_empty() {
            return Ok(Progress::Waiting);
        }
        self.merge()
    }

    /// Puts `cursor` back by where it stands: in the heap on a live pair,
    /// otherwise its row group's pages dropped, among the waiting when
    /// another row group is to read.
    fn place(&mut self, cursor: usize, head: Head) {
        if head != Head::Live
            && let Some(chunks) = self.heads.cursors[cursor].chunks.take()
        {
            self.held -= chunks.read_bytes();
        }
        match head {
            Head::Live => self.heads.push(cursor),
            Head::Waiting => self.waiting.push(cursor),
            Head::Done => {
                if self.heads.cursors[cursor].checked {
                    self.checked -= 1;
                }
            }
        }
    }

    /// Merges the live pairs of the cursors by key and order value, and
    /// refuses the first pair two of them hold where one is checked, as
    /// `RefusalReason::DuplicateRows` naming the latest copy in file order
    /// (RFD 025, Uniqueness); a deleted row is not live (RFD 021). Every
    /// file is sorted by key and order value and free of repeated pairs on
    /// its own (verification checked), and each cursor reads a subsequence
    /// of its file's rows, so equal heads are exactly a pair in several
    /// files. Two files neither of which is checked were proven unique by
    /// the generation that last checked them, so a tie between them alone
    /// passes. The merge stops once a cursor waits on its next row group,
    /// before any pair past it is taken, and ends once every checked
    /// cursor is exhausted.
    fn merge(&mut self) -> Result<Progress> {
        let mut tied = Vec::new();
        while self.checked > 0 {
            let heads = &mut self.heads;
            let least = heads
                .pop()
                .expect("a checked cursor not exhausted and not waiting is in the heap");
            tied.push(least);
            while let Some(next) = heads.peek() {
                if heads.cmp(next, least) != Ordering::Equal {
                    break;
                }
                tied.extend(heads.pop());
            }
            if tied.len() > 1 && tied.iter().any(|&cursor| heads.cursors[cursor].checked) {
                let later = tied
                    .iter()
                    .copied()
                    .max_by_key(|&cursor| heads.cursors[cursor].at)
                    .expect("a tie holds two cursors");
                let head = &heads.cursors[least];
                return Err(Error::refused(
                    &self.table,
                    RefusalReason::DuplicateRows {
                        path: heads.cursors[later].file.entry.path.clone(),
                        key: head.pairs.key(head.offset),
                        order: Box::new(head.pairs.order(head.offset)),
                    },
                ));
            }
            for cursor in tied.drain(..) {
                let head = self.heads.cursors[cursor].advance(&self.deletes)?;
                self.place(cursor, head);
            }
            if !self.waiting.is_empty() {
                return Ok(Progress::Waiting);
            }
        }
        Ok(Progress::Done)
    }

    /// What the merge did.
    fn merged(&self) -> Merged {
        Merged {
            compared: self.heads.compared,
            held: self.most_held,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

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
    /// `checked` marks checked, by file position: per file, its
    /// counterparts where one of the two is checked, ascending.
    fn swept(spans: &[OrderedSpan<'_>], checked: &[bool]) -> Vec<Vec<usize>> {
        let mut found = vec![Vec::new(); spans.len()];
        let mut compared = 0;
        for group in groups_of(spans) {
            let met = meetings(spans, checked, &group, &mut compared);
            for (member, &at) in group.iter().enumerate() {
                found[at] = met[member].iter().map(|&theirs| group[theirs]).collect();
                found[at].sort_unstable();
            }
        }
        found
    }

    /// The sweep finds exactly the oracle's counterparts of every file,
    /// less those where neither file is checked: with each file in turn
    /// the one unchecked file, with every file checked, and with every
    /// other file checked.
    fn assert_sweep_is_the_full_scan(spans: &[OrderedSpan<'_>]) -> Vec<Vec<usize>> {
        let oracle = by_full_scan(spans);
        let mut patterns: Vec<Vec<bool>> = (0..spans.len())
            .map(|at| (0..spans.len()).map(|other| other != at).collect())
            .collect();
        patterns.push(vec![true; spans.len()]);
        patterns.push((0..spans.len()).map(|at| at % 2 == 0).collect());
        for checked in &patterns {
            let found = swept(spans, checked);
            for at in 0..spans.len() {
                let expected: Vec<usize> = oracle[at]
                    .iter()
                    .copied()
                    .filter(|&theirs| checked[at] || checked[theirs])
                    .collect();
                assert_eq!(found[at], expected, "the counterparts of {at}");
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
        built_in_groups(name, rows, page_rows, rows.len(), residency).await
    }

    /// `built`, in row groups of `group_rows` rows.
    async fn built_in_groups(
        name: &str,
        rows: &[(i64, i64)],
        page_rows: usize,
        group_rows: usize,
        residency: Residency,
    ) -> Built {
        let keys: Vec<i64> = rows.iter().map(|&(key, _)| key).collect();
        let orders: Vec<i64> = rows.iter().map(|&(_, order)| order).collect();
        let mut fixture =
            index_testing::Fixture::ordered(&keys, index_testing::long_orders(&orders));
        fixture.layout.page_rows = page_rows;
        fixture.layout.write_batch = page_rows;
        fixture.layout.row_group_rows = group_rows;
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
        let none = vec![Vec::new(); built.len()];
        check_rows_deleting(built, reused, &none).await.1
    }

    /// `check_rows` with the rows at the positions of `gone` of each file
    /// deleted, and the overlap groups the check went through.
    async fn check_rows_deleting(
        built: &[Built],
        reused: &[bool],
        gone: &[Vec<u64>],
    ) -> (Vec<Vec<usize>>, Result<RowCheck>) {
        let files: Vec<Arc<LoadedFile>> = built.iter().map(|one| Arc::clone(&one.file)).collect();
        let deletes: Arc<[Option<Arc<FileDeletes>>]> = gone
            .iter()
            .map(|rows| {
                (!rows.is_empty()).then(|| Arc::new(FileDeletes::of_rows(rows.iter().copied())))
            })
            .collect();
        let members: Vec<Member> = files
            .iter()
            .zip(reused)
            .zip(deletes.iter())
            .map(|((file, &reused), deletes)| Member {
                file: Arc::clone(file),
                reused,
                deletes: deletes.clone(),
                shrank: false,
            })
            .collect();
        let spans: Vec<Span<'_>> = files.iter().map(|file| Span::of(file)).collect();
        let groups = overlap_groups(&spans, &routes(&spans));
        let check = verify_rows(TABLE, &members, &files, &groups, &deletes).await;
        (groups, check)
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

    /// The reads one row group's chosen pages cost when they are all of
    /// its pages, one run: under `full` the order pages by one GET, the key
    /// chunk being resident; under `bloom` the order and key pages, a GET
    /// each; under `disk` both from the chunk file.
    const FULL_CHECK_GETS: u64 = 1;
    const BLOOM_CHECK_GETS: u64 = 2;
    const DISK_CHECK_LOCAL_READS: u64 = 2;

    /// A first load of `OVERLAPPING` files whose order ranges all overlap
    /// merges them once, reading each file's order pages exactly once
    /// however many files they meet: every page of every file meets
    /// another file's, so each file's pages are chosen as one run, read in
    /// one read per column the tier does not hold.
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

    /// The bytes a merge reads of order page `page` of `row_group` of
    /// `file` in `residency`: the page, and under `bloom` and `disk` the key
    /// page holding the same rows, the fixture writing both columns in pages
    /// of the same rows.
    fn page_bytes(file: &LoadedFile, row_group: usize, page: usize, residency: Residency) -> u64 {
        let metadata = file.index.metadata();
        let offsets = &metadata.offset_index().unwrap()[row_group];
        let order = &offsets[file.index.order_leaf().unwrap()].page_locations()[page];
        let key = &offsets[file.index.key_leaf()].page_locations()[page];
        assert_eq!(key.first_row_index, order.first_row_index, "aligned pages");
        let size = |location: &parquet::file::page_index::offset_index::PageLocation| {
            u64::try_from(location.compressed_page_size).unwrap()
        };
        match residency {
            Residency::Full => size(order),
            Residency::Bloom | Residency::Disk => size(order) + size(key),
        }
    }

    /// The order pages of `row_group` of `file`.
    fn order_pages(file: &LoadedFile, row_group: usize) -> usize {
        file.index.metadata().offset_index().unwrap()[row_group][file.index.order_leaf().unwrap()]
            .page_locations()
            .len()
    }

    /// Bytes `built` has read so far, by GET and from its chunk file.
    fn bytes_read(built: &Built) -> u64 {
        built.stats.bytes() + built.stats.local_bytes()
    }

    /// Rows of the run that fills most of each edge-key file, over many
    /// pages, and rows of the key the two files share on each side of the
    /// edge, fewer than a page holds.
    const EDGE_RUN: i64 = 400;
    const EDGE_ROWS: i64 = 4;

    /// The keys of the edge-key files: the first file's run, the key the
    /// two share, and the second file's run.
    const BEFORE_EDGE: i64 = 0;
    const EDGE_KEY: i64 = 1;
    const AFTER_EDGE: i64 = 2;

    /// Two new files that meet at one edge key, as a compaction rolling
    /// files by size writes them, read only the order pages that key's
    /// rows lie on for their check (RFD 025, Refresh): the first file's
    /// last page and the second's first, by GET under `full`, with the key
    /// pages they span under `bloom`, and from the chunk file under `disk`.
    #[tokio::test]
    async fn test_verify_rows_of_files_meeting_at_an_edge_key_reads_only_the_edge_pages() {
        for residency in [Residency::Full, Residency::Disk, Residency::Bloom] {
            let head: Vec<(i64, i64)> = (0..EDGE_RUN)
                .map(|order| (BEFORE_EDGE, order))
                .chain((0..EDGE_ROWS).map(|order| (EDGE_KEY, order)))
                .collect();
            let tail: Vec<(i64, i64)> = (EDGE_ROWS..2 * EDGE_ROWS)
                .map(|order| (EDGE_KEY, order))
                .chain((0..EDGE_RUN).map(|order| (AFTER_EDGE, order)))
                .collect();
            let files = [
                built("e-0", &head, FEW_PAGE_ROWS, residency).await,
                built("e-1", &tail, FEW_PAGE_ROWS, residency).await,
            ];
            let before: Vec<u64> = files.iter().map(bytes_read).collect();
            let check = check_rows(&files, &[false, false]).await.unwrap();
            assert_eq!(check.merges, 1, "{residency:?}");
            let last = order_pages(&files[0].file, 0) - 1;
            let whole = |file: &LoadedFile| -> u64 {
                (0..order_pages(file, 0))
                    .map(|page| page_bytes(file, 0, page, residency))
                    .sum()
            };
            for ((one, before), page) in files.iter().zip(&before).zip([last, 0]) {
                let read = bytes_read(one) - before;
                assert_eq!(
                    read,
                    page_bytes(&one.file, 0, page, residency),
                    "{residency:?}: {}",
                    one.file.entry.path
                );
                assert!(read < whole(&one.file), "{residency:?}: not every page");
            }
        }
    }

    /// Row groups of each file of the held-bytes test, one key each.
    const GROUPS_PER_FILE: usize = 4;

    /// A first load of `OVERLAPPING` files of `GROUPS_PER_FILE` row groups
    /// each, every page of which meets every other file's, holds at most
    /// one row group's chosen pages per file at once (RFD 025, Refresh): at
    /// least every file's first row group, which the merge starts from, and
    /// at most each file's largest.
    #[tokio::test]
    async fn test_verify_rows_holds_one_row_group_of_chosen_pages_per_file() {
        let group_rows = usize::try_from(OVERLAPPING_ROWS_PER_KEY).unwrap();
        for residency in [Residency::Full, Residency::Disk, Residency::Bloom] {
            let mut files = Vec::new();
            let spread = i64::try_from(OVERLAPPING).unwrap();
            for at in 0..OVERLAPPING {
                let offset = i64::try_from(at).unwrap();
                let rows: Vec<(i64, i64)> = (0..i64::try_from(GROUPS_PER_FILE).unwrap())
                    .flat_map(|key| {
                        (0..OVERLAPPING_ROWS_PER_KEY).map(move |row| (key, row * spread + offset))
                    })
                    .collect();
                let name = format!("h-{at:03}");
                files.push(
                    built_in_groups(&name, &rows, FEW_PAGE_ROWS, group_rows, residency).await,
                );
            }
            let group_bytes = |file: &LoadedFile, row_group: usize| -> u64 {
                (0..order_pages(file, row_group))
                    .map(|page| page_bytes(file, row_group, page, residency))
                    .sum()
            };
            let mut first = 0;
            let mut largest = 0;
            let mut whole = 0;
            for one in &files {
                assert_eq!(one.file.index.metadata().num_row_groups(), GROUPS_PER_FILE);
                let per_group: Vec<u64> = (0..GROUPS_PER_FILE)
                    .map(|row_group| group_bytes(&one.file, row_group))
                    .collect();
                first += per_group[0];
                largest += per_group.iter().max().unwrap();
                whole += per_group.iter().sum::<u64>();
            }
            let before: u64 = files.iter().map(bytes_read).sum();
            let check = check_rows(&files, &[false; OVERLAPPING]).await.unwrap();
            let read = files.iter().map(bytes_read).sum::<u64>() - before;
            assert_eq!(check.merges, 1, "{residency:?}");
            assert_eq!(
                read, whole,
                "{residency:?}: every page meets another file's"
            );
            assert!(
                first <= check.held && check.held <= largest,
                "{residency:?}: held {} outside {first}..={largest}",
                check.held
            );
            assert!(check.held < whole, "{residency:?}");
        }
    }

    /// Files of a compaction's key chain, each sharing its edge keys with
    /// its neighbours, all over one order range, as files a compaction
    /// rolled by size from a bucket's whole history are.
    const CHAIN: usize = 4_000;

    /// Keys between a chained file's first key and its last, and the order
    /// range every chained file spans.
    const CHAIN_LINK_KEYS: i64 = 10;
    const CHAIN_ORDERS: (i64, i64) = (0, 1_000_000);

    /// The most comparisons the sweep of a key chain may make per file: a
    /// few sorts and searches of a group of `CHAIN` files, where testing
    /// each file against every file open by order range makes about
    /// `CHAIN` squared halved.
    const COMPARED_PER_CHAINED_FILE: u64 = 64;

    /// The sweep of an overlap group whose files chain by key and share one
    /// order range finds each file's neighbours, and only them, in a number
    /// of comparisons linear in the files up to a log factor.
    #[test]
    fn test_meetings_of_a_key_chain_with_one_order_range_is_linear() {
        let files: Vec<Described> = (0..i64::try_from(CHAIN).unwrap())
            .map(|at| {
                let first = at * CHAIN_LINK_KEYS;
                file(
                    Some((TWO, 0)),
                    (first, first + CHAIN_LINK_KEYS),
                    CHAIN_ORDERS,
                )
            })
            .collect();
        let spans: Vec<OrderedSpan<'_>> = files.iter().map(span).collect();
        let checked = vec![true; spans.len()];
        let group: Vec<usize> = (0..spans.len()).collect();
        let mut compared = 0;
        let found = meetings(&spans, &checked, &group, &mut compared);
        for (at, theirs) in found.iter().enumerate() {
            let mut theirs = theirs.clone();
            theirs.sort_unstable();
            let neighbours: Vec<usize> =
                [at.checked_sub(1), Some(at + 1).filter(|&next| next < CHAIN)]
                    .into_iter()
                    .flatten()
                    .collect();
            assert_eq!(theirs, neighbours, "the counterparts of {at}");
        }
        assert!(
            compared > 0 && compared <= COMPARED_PER_CHAINED_FILE * approx::count(CHAIN),
            "{compared} comparisons over {CHAIN} files"
        );
    }

    /// Cases of the drawn check, files per case at least and at most, and
    /// the pair universe the files draw from: few keys and order values,
    /// so files repeat pairs.
    const DRAWN_CASES: u64 = 150;
    const FEWEST_DRAWN_FILES: u64 = 2;
    const MOST_DRAWN_FILES: u64 = 6;
    const DRAWN_KEYS: u64 = 6;
    const DRAWN_ORDERS: u64 = 8;

    /// One in how many drawn pairs a file holds, one in how many drawn rows
    /// is deleted, and one in how many drawn files is checked.
    const PAIR_ONE_IN: u64 = 3;
    const DELETED_ONE_IN: u64 = 8;
    const CHECKED_ONE_IN: u64 = 2;

    /// The most rows a drawn file's pages and row groups hold.
    const MOST_DRAWN_PAGE_ROWS: u64 = 4;
    const MOST_DRAWN_GROUP_ROWS: u64 = 12;

    /// One drawn file: its rows, ascending, whether the load checks it, and
    /// the positions of its deleted rows.
    struct DrawnFile {
        rows: Vec<(i64, i64)>,
        checked: bool,
        deleted: Vec<u64>,
    }

    /// Files drawn from the pair universe by `draw`, in file order.
    fn draw_files(draw: &mut impl FnMut(u64) -> u64) -> Vec<DrawnFile> {
        let count = FEWEST_DRAWN_FILES + draw(MOST_DRAWN_FILES - 1);
        let mut drawn: Vec<DrawnFile> = (0..count)
            .map(|_| {
                let first = draw(DRAWN_KEYS);
                let last = first + draw(DRAWN_KEYS - first);
                let long = |value: u64| i64::try_from(value).unwrap();
                let mut rows: Vec<(i64, i64)> = (first..=last)
                    .flat_map(|key| (0..DRAWN_ORDERS).map(move |order| (long(key), long(order))))
                    .filter(|_| draw(PAIR_ONE_IN) == 0)
                    .collect();
                if rows.is_empty() {
                    rows.push((long(first), 0));
                }
                let deleted = (0..approx::count(rows.len()))
                    .filter(|_| draw(DELETED_ONE_IN) == 0)
                    .collect();
                DrawnFile {
                    rows,
                    checked: draw(CHECKED_ONE_IN) == 0,
                    deleted,
                }
            })
            .collect();
        drawn.sort_by_key(|file| file.rows[0].0);
        drawn
    }

    /// What a scan of every live pair of `drawn` finds: the pair the check
    /// refuses in the first of `groups` holding one, the least live pair
    /// two files hold where one is checked, with the position of the latest
    /// file holding it; and how many pairs only unchecked files share live,
    /// and how many have other copies all deleted.
    struct Scanned {
        refused: Option<((i64, i64), usize)>,
        unchecked_ties: usize,
        deleted_ties: usize,
    }

    fn scan_live_pairs(drawn: &[DrawnFile], groups: &[Vec<usize>]) -> Scanned {
        let mut holders: BTreeMap<(i64, i64), Vec<usize>> = BTreeMap::new();
        let mut copies: BTreeMap<(i64, i64), usize> = BTreeMap::new();
        for (at, file) in drawn.iter().enumerate() {
            for (position, &pair) in file.rows.iter().enumerate() {
                *copies.entry(pair).or_insert(0) += 1;
                if !file.deleted.contains(&approx::count(position)) {
                    holders.entry(pair).or_default().push(at);
                }
            }
        }
        let refusable =
            |live: &Vec<usize>| live.len() > 1 && live.iter().any(|&at| drawn[at].checked);
        let refused = groups.iter().find_map(|group| {
            holders
                .iter()
                .filter(|(_, live)| refusable(live))
                .find(|(_, live)| group.contains(&live[0]))
                .map(|(pair, live)| (*pair, *live.iter().max().unwrap()))
        });
        Scanned {
            refused,
            unchecked_ties: holders
                .values()
                .filter(|live| live.len() > 1 && !refusable(live))
                .count(),
            deleted_ties: holders
                .iter()
                .filter(|(pair, live)| live.len() == 1 && copies[*pair] > 1)
                .count(),
        }
    }

    /// The check across drawn files, of several row groups and pages each,
    /// some checked and some with deleted rows, in every tier, refuses
    /// exactly what a scan of every file's live pairs refuses (RFD 025,
    /// Refresh): in the first overlap group that holds one, the least live
    /// pair two files hold where one is checked, naming the latest file
    /// holding it in file order; a pair only unchecked files share, or one
    /// whose other copies are deleted, is no refusal.
    #[tokio::test]
    async fn test_verify_rows_refuses_what_a_scan_of_every_live_pair_refuses() {
        let mut state = 0_u64;
        let mut draw = |below: u64| {
            state = state.wrapping_mul(MIX).wrapping_add(STEP);
            (state >> 33) % below
        };
        let (mut refusals, mut serves, mut unchecked_ties, mut deleted_ties) = (0, 0, 0, 0);
        let residencies = [Residency::Full, Residency::Disk, Residency::Bloom];
        for case in 0..DRAWN_CASES {
            let residency = residencies[usize::try_from(case).unwrap() % residencies.len()];
            let drawn = draw_files(&mut draw);
            let mut files = Vec::new();
            for (at, file) in drawn.iter().enumerate() {
                let page_rows = 1 + draw(MOST_DRAWN_PAGE_ROWS);
                let group_rows = page_rows + draw(MOST_DRAWN_GROUP_ROWS);
                files.push(
                    built_in_groups(
                        &format!("d-{at:02}"),
                        &file.rows,
                        usize::try_from(page_rows).unwrap(),
                        usize::try_from(group_rows).unwrap(),
                        residency,
                    )
                    .await,
                );
            }
            let reused: Vec<bool> = drawn.iter().map(|file| !file.checked).collect();
            let gone: Vec<Vec<u64>> = drawn.iter().map(|file| file.deleted.clone()).collect();
            let (groups, check) = check_rows_deleting(&files, &reused, &gone).await;
            let scanned = scan_live_pairs(&drawn, &groups);
            unchecked_ties += scanned.unchecked_ties;
            deleted_ties += scanned.deleted_ties;
            let Some(((key, order), latest)) = scanned.refused else {
                assert!(check.is_ok(), "case {case}, {residency:?}: {check:?}");
                serves += 1;
                continue;
            };
            let RefusalReason::DuplicateRows {
                path,
                key: refused_key,
                order: refused_order,
            } = refusal_of(check)
            else {
                panic!("case {case}: expected duplicate_rows");
            };
            assert_eq!(
                (path, refused_key, *refused_order),
                (
                    format!("d-{latest:02}"),
                    Key::Long(key),
                    OrderValue::Long(order)
                ),
                "case {case}, {residency:?}"
            );
            refusals += 1;
        }
        assert!(
            refusals > 0 && serves > 0 && unchecked_ties > 0 && deleted_ties > 0,
            "refused {refusals}, served {serves}, unchecked ties {unchecked_ties}, deleted ties {deleted_ties}"
        );
    }

    /// A long pair of a test.
    fn long_pair(key: i64, order: i64) -> Pair<'static> {
        Pair {
            key: PairPart::Long(key),
            order: PairPart::Long(order),
        }
    }

    /// The shared range of the `may_hold` test: keys 2 to 5, order values
    /// 10 to 20.
    const SHARED_KEYS: (i64, i64) = (2, 5);
    const SHARED_ORDERS: (i64, i64) = (10, 20);

    /// A range of pairs holds a pair of the shared range exactly when one
    /// lies in both, ends included: within one key, by the key and the
    /// order values; across keys, by the shared range's least and greatest
    /// pairs, a top at or below either side of them as it covers them.
    #[test]
    fn test_may_hold_keeps_a_range_touching_the_shared_corners() {
        let shared = Shared {
            keys: (PairPart::Long(SHARED_KEYS.0), PairPart::Long(SHARED_KEYS.1)),
            orders: (
                PairPart::Long(SHARED_ORDERS.0),
                PairPart::Long(SHARED_ORDERS.1),
            ),
        };
        let (low_order, high_order) = (SHARED_ORDERS.0, SHARED_ORDERS.1);
        let (low_key, high_key) = (SHARED_KEYS.0, SHARED_KEYS.1);
        let inside = low_key + 1;
        let cases = [
            // Within one key.
            (
                long_pair(inside, high_order),
                Top::Below(long_pair(inside, high_order + 1)),
                true,
            ),
            (
                long_pair(inside, high_order + 1),
                Top::AtMost(long_pair(inside, high_order + 1)),
                false,
            ),
            (
                long_pair(inside, 0),
                Top::AtMost(long_pair(inside, low_order)),
                true,
            ),
            (
                long_pair(inside, 0),
                Top::Below(long_pair(inside, low_order)),
                false,
            ),
            (
                long_pair(low_key - 1, 0),
                Top::AtMost(long_pair(low_key - 1, low_order)),
                false,
            ),
            (
                long_pair(high_key + 1, low_order),
                Top::AtMost(long_pair(high_key + 1, high_order)),
                false,
            ),
            // Across keys.
            (
                long_pair(high_key, high_order),
                Top::Below(long_pair(high_key + 1, 0)),
                true,
            ),
            (
                long_pair(high_key, high_order + 1),
                Top::Below(long_pair(high_key + 1, 0)),
                false,
            ),
            (
                long_pair(low_key - 1, 0),
                Top::AtMost(long_pair(low_key, low_order)),
                true,
            ),
            (
                long_pair(low_key - 1, 0),
                Top::Below(long_pair(low_key, low_order)),
                false,
            ),
        ];
        for (low, top, held) in cases {
            assert_eq!(may_hold(low, top, shared), held, "{low:?} up to {top:?}");
        }
    }
}
