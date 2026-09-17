//! The `left-right` keydir (M11.5).
//!
//! Three things have to hold before the engine can sit on this:
//!
//! 1. the two copies converge — `absorb_first` and `absorb_second` are the
//!    same function applied twice, so any divergence is permanent;
//! 2. the *writer* sees its own writes before they are published, because the
//!    apply loop is a read-modify-write loop (`Cas`, the session table, the
//!    address book) and an entry must see the entry before it;
//! 3. readers see nothing until `publish`, which is the whole reason
//!    `published_index` has to exist.

use std::collections::HashMap;

use proptest::prelude::*;

use crate::index::left_right::{KeyDir, KvOp, LeftRightIndex};
use crate::index::{KeyDirIndex, LockedIndex, ValueLoc};

fn loc(offset: u64) -> ValueLoc {
    ValueLoc { segment_id: 0, offset, len: 8 }
}

/// A snapshot of what readers can see right now, ordered so two copies can be
/// compared for equality.
fn visible(index: &LeftRightIndex) -> Vec<(Vec<u8>, ValueLoc)> {
    let mut entries = index.read_factory().handle().entries();
    entries.sort();
    entries
}

#[test]
fn writer_sees_its_own_writes_before_publish() {
    let mut index = LeftRightIndex::default();

    index.insert(b"k".to_vec(), loc(1));

    // Not published: readers cannot see it yet, but the writer must.
    assert_eq!(index.get(b"k"), Some(loc(1)), "the apply loop must see the entry it just applied");
    assert!(visible(&index).is_empty(), "an unpublished write must not be visible to a reader");
}

#[test]
fn readers_see_a_write_only_after_publish() {
    let mut index = LeftRightIndex::default();
    index.insert(b"k".to_vec(), loc(1));

    assert!(index.publish());

    assert_eq!(visible(&index), vec![(b"k".to_vec(), loc(1))]);
}

#[test]
fn remove_returns_the_writers_view_of_the_old_location() {
    let mut index = LeftRightIndex::default();
    index.insert(b"k".to_vec(), loc(1));

    // Still unpublished, so a `remove` that consulted the read copy would
    // answer `None` here and the engine would lose a tombstone's old length.
    assert_eq!(index.remove(b"k"), Some(loc(1)));
    assert_eq!(index.get(b"k"), None);
}

#[test]
fn relocate_is_conditional_on_the_writers_view() {
    let mut index = LeftRightIndex::default();
    index.insert(b"k".to_vec(), loc(1));
    assert!(index.publish());

    // Compaction planned a move from loc(1); an overwrite landed first.
    index.insert(b"k".to_vec(), loc(2));
    assert!(!index.relocate(b"k", loc(1), loc(3)), "a stale relocation must not clobber a newer");
    assert_eq!(index.get(b"k"), Some(loc(2)));

    // And the same decision must be the one both copies reach.
    assert!(index.publish());
    assert_eq!(visible(&index), vec![(b"k".to_vec(), loc(2))]);
}

#[test]
fn a_relocation_that_applies_reaches_both_copies() {
    let mut index = LeftRightIndex::default();
    index.insert(b"k".to_vec(), loc(1));
    assert!(index.publish());

    assert!(index.relocate(b"k", loc(1), loc(9)));
    assert!(index.publish());

    assert_eq!(index.get(b"k"), Some(loc(9)));
    assert_eq!(visible(&index), vec![(b"k".to_vec(), loc(9))]);
}

/// One generated operation. `Relocate`'s `old` is generated independently of
/// what is in the index, so the sequence exercises both the applied and the
/// skipped branch.
#[derive(Debug, Clone)]
enum Step {
    Insert(u8, u64),
    Remove(u8),
    Relocate(u8, u64, u64),
    Publish,
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    let step = prop_oneof![
        (0u8..6, 0u64..6).prop_map(|(k, o)| Step::Insert(k, o)),
        (0u8..6).prop_map(Step::Remove),
        (0u8..6, 0u64..6, 0u64..6).prop_map(|(k, a, b)| Step::Relocate(k, a, b)),
        Just(Step::Publish),
    ];
    proptest::collection::vec(step, 0..40)
}

fn apply(index: &mut dyn KeyDirIndex, step: &Step) {
    match *step {
        Step::Insert(k, o) => index.insert(vec![k], loc(o)),
        Step::Remove(k) => {
            index.remove(&[k]);
        }
        Step::Relocate(k, a, b) => {
            index.relocate(&[k], loc(a), loc(b));
        }
        Step::Publish => {}
    }
}

proptest! {
    /// `absorb_first` and `absorb_second` are the same operation applied to
    /// the two copies. If they ever disagree the copies diverge for good, and
    /// every read after that is wrong in a way no single-threaded test sees.
    ///
    /// Publishing twice is how both copies are inspected through one
    /// `ReadHandle`: the second publish has nothing to absorb, so it only
    /// swaps, and the snapshot after it is the *other* copy.
    #[test]
    fn both_copies_are_identical_after_any_op_sequence(steps in steps()) {
        let mut index = LeftRightIndex::default();
        for step in &steps {
            apply(&mut index, step);
            if matches!(step, Step::Publish) {
                index.publish();
            }
        }
        index.publish();

        let first = visible(&index);
        index.publish();
        let second = visible(&index);

        prop_assert_eq!(&first, &second, "the two copies diverged");

        // And what they agree on is what the writer has.
        let mut writer = index.iter();
        writer.sort();
        prop_assert_eq!(writer, first);
    }

    /// The left-right index and the `RwLock<HashMap>` comparison arm must be
    /// the same map. Same sequence, same answers, same contents.
    #[test]
    fn left_right_matches_the_locked_index(steps in steps()) {
        let mut lr = LeftRightIndex::default();
        let mut locked = LockedIndex::default();

        for step in &steps {
            match *step {
                Step::Insert(k, o) => {
                    lr.insert(vec![k], loc(o));
                    locked.insert(vec![k], loc(o));
                }
                Step::Remove(k) => {
                    prop_assert_eq!(lr.remove(&[k]), locked.remove(&[k]));
                }
                Step::Relocate(k, a, b) => {
                    prop_assert_eq!(
                        lr.relocate(&[k], loc(a), loc(b)),
                        locked.relocate(&[k], loc(a), loc(b))
                    );
                }
                Step::Publish => {
                    lr.publish();
                }
            }
            prop_assert_eq!(lr.get(&[0]), locked.get(&[0]));
        }

        let (mut a, mut b) = (lr.iter(), locked.iter());
        a.sort();
        b.sort();
        prop_assert_eq!(a, b);
    }
}

/// `sync_with` is only called once — on the second publish, to bring the copy
/// that missed the pre-first-publish writes up to date. It is a wholesale
/// clone, and getting it wrong loses everything written before the first
/// publish, which is exactly the state a reopened engine starts in.
#[test]
fn sync_with_replaces_the_whole_copy() {
    let mut first = KeyDir::default();
    first.entries_mut().insert(b"a".to_vec(), loc(1));
    let mut second = KeyDir::default();
    second.entries_mut().insert(b"b".to_vec(), loc(2));

    left_right::Absorb::<KvOp>::sync_with(&mut second, &first);

    let expected: HashMap<Vec<u8>, ValueLoc> = [(b"a".to_vec(), loc(1))].into_iter().collect();
    assert_eq!(second.entries_mut(), &expected);
}

/// Whatever the engine does, it must do identically under either keydir. The
/// `RwLock<HashMap>` arm is not decoration — it is the comparison arm M11.5's
/// benchmark needs and the escape hatch if left-right ever misbehaves, so it
/// has to stay a real, working configuration rather than a type that compiles.
#[test]
fn the_engine_behaves_the_same_under_either_keydir() {
    for kind in [crate::IndexKind::Locked, crate::IndexKind::LeftRight] {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::EngineConfig { index: kind, ..Default::default() };
        let mut engine = crate::Engine::open_with_config(dir.path(), config).unwrap();

        engine.put(b"a", b"1").unwrap();
        engine.put(b"b", b"2").unwrap();
        engine.delete(b"a").unwrap();
        engine.put(b"b", b"3").unwrap();
        assert!(engine.publish(), "{kind:?}");

        assert_eq!(engine.get(b"a").unwrap(), None, "{kind:?}");
        assert_eq!(engine.get(b"b").unwrap(), Some(b"3".to_vec()), "{kind:?}");

        let view = engine.read_view_factory().view();
        assert_eq!(view.get(b"a").unwrap(), None, "{kind:?}");
        assert_eq!(view.get(b"b").unwrap(), Some(b"3".to_vec()), "{kind:?}");

        drop(engine);
        let reopened = crate::Engine::open_with_config(dir.path(), config).unwrap();
        assert_eq!(reopened.get(b"b").unwrap(), Some(b"3".to_vec()), "{kind:?} after reopen");
        // A reopened engine's contents must be visible to a reader without
        // anything having been applied first.
        assert_eq!(
            reopened.read_view_factory().view().get(b"b").unwrap(),
            Some(b"3".to_vec()),
            "{kind:?}: a reopened engine must be readable before its first write"
        );
    }
}

/// A `ReadView` resolved against the *published* keydir must still be able to
/// read a value compaction has since moved and unlinked.
///
/// This is the consequence §1.15 does not mention. Compaction relocates keydir
/// entries and then retires the segments they pointed at — but a reader is on
/// whichever copy was last published, which may still point into them. Drop
/// those descriptors when compaction finishes and that reader resolves a
/// location nothing has open. So they are held until a publish makes the
/// relocated keydir visible.
#[test]
fn a_reader_on_the_old_keydir_can_still_read_a_compacted_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = crate::Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
    engine.put(b"k", b"v0").unwrap();
    for i in 0..20u32 {
        engine.put(format!("filler{i}").as_bytes(), b"xxxxxxxxxxxxxxxx").unwrap();
    }
    assert!(engine.publish());

    // A view minted now sees the pre-compaction keydir. It stays on that copy
    // until it next enters, which is after the relocation below.
    let view = engine.read_view_factory().view();
    engine.compact().unwrap();

    // Not published yet: the reader is still resolving against the old
    // locations, whose segments compaction has just unlinked.
    assert!(engine.has_unpublished(), "compaction's relocations are writes like any other");
    assert_eq!(view.get(b"k").unwrap(), Some(b"v0".to_vec()), "the retired segment is still open");

    assert!(engine.publish());
    assert_eq!(view.get(b"k").unwrap(), Some(b"v0".to_vec()), "and the same after the swap");
    assert_eq!(engine.get(b"k").unwrap(), Some(b"v0".to_vec()));
}

/// Compaction is the second writer §1.15 warns about, and this is the shape of
/// the answer: its moves are `KvOp::Relocate`s through the one `WriteHandle`,
/// conditional on the old location still being current. An overwrite that
/// lands after the plan was taken wins.
#[test]
fn compaction_relocations_lose_to_a_later_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = crate::Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
    engine.put(b"k", b"old").unwrap();
    for i in 0..20u32 {
        engine.put(format!("filler{i}").as_bytes(), b"xxxxxxxxxxxxxxxx").unwrap();
    }

    let plan = engine.plan_compaction().unwrap().expect("closed segments to merge");
    // The exact race `plan_compaction`/`apply_compaction` were split apart
    // for: the write lands inside the snapshot-to-relocate window.
    engine.put(b"k", b"new").unwrap();
    engine.apply_compaction(plan).unwrap();
    assert!(engine.publish());

    assert_eq!(engine.get(b"k").unwrap(), Some(b"new".to_vec()));
    assert_eq!(
        engine.read_view_factory().view().get(b"k").unwrap(),
        Some(b"new".to_vec()),
        "the relocation must not have clobbered the newer write on the read copy either"
    );
}

/// A reader parked inside the published copy defers the swap rather than
/// blocking the writer in it.
///
/// This is the mechanism the whole read-visibility argument rests on: the
/// engine will not stall a writer on a reader, so a write can be *applied* and
/// not yet *visible*, for an unbounded time. Anything that waits for a write to
/// be readable has to wait on the published index.
#[test]
fn a_parked_reader_defers_the_swap_instead_of_blocking_the_writer() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = crate::Engine::open(dir.path()).unwrap();
    let factory = engine.read_view_factory();

    // The first publish after the hold is what records its epoch; left-right
    // only waits for readers that entered before the last swap.
    let hold = factory.hold_read_copy();
    engine.put(b"warm", b"v").unwrap();
    assert!(engine.publish());

    engine.put(b"k", b"v").unwrap();
    assert!(!engine.publish(), "a parked reader must defer the swap");
    assert!(engine.has_unpublished());
    assert_eq!(engine.get(b"k").unwrap(), Some(b"v".to_vec()), "the writer still sees its write");
    assert_eq!(factory.view().get(b"k").unwrap(), None, "and no reader can");

    drop(hold);
    assert!(engine.publish(), "the reader left, so the swap can proceed");
    assert_eq!(factory.view().get(b"k").unwrap(), Some(b"v".to_vec()));
}
