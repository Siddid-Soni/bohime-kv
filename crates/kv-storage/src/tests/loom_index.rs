//! `loom` over the keydir (M11.5's third ✅).
//!
//! **Run separately, and it has to be:**
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p kv-storage --lib loom_index
//! ```
//!
//! `left-right`'s own atomics are `#[cfg(loom)]`-switched (its `src/sync.rs`),
//! so without that flag the crate is built against `std::sync::atomic` and
//! loom sees no synchronisation inside it at all — the model would then permute
//! only the points *this* file yields at, which is close to permuting nothing.
//! With the flag it is loom's atomics all the way down, and the model explores
//! the epoch protocol, the pointer swap and the oplog replay.
//!
//! The rest of `src/tests/` is `#[cfg(not(loom))]` in `tests/mod.rs`, because
//! loom's primitives panic outside a model and every other test here builds a
//! real `Engine`.
//!
//! **Two threads, three ops.** The model is deliberately tiny — the risk on
//! this gate is a state-space explosion, not a weak test. `loom::model` is
//! exhaustive over preemption points, so a writer doing three ops against one
//! reader doing one read is already tens of thousands of interleavings.

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering};

use crate::index::ValueLoc;
use crate::index::left_right::{KeyDir, KvOp};

fn loc(offset: u64) -> ValueLoc {
    ValueLoc { segment_id: 0, offset, len: 8 }
}

/// Apply plus read plus relocate, interleaved.
///
/// The writer does the three-op batch a compaction produces — write a key,
/// move it, publish — and the reader resolves that key once, at some point
/// loom chooses. The assertion is the one that matters for a keydir: whatever
/// the reader sees is a location the writer actually wrote, never a torn one
/// and never one from a copy midway through absorbing a batch.
#[test]
fn a_read_never_observes_a_half_absorbed_batch() {
    loom::model(|| {
        let (mut writer, reader) = left_right::new::<KeyDir, KvOp>();
        writer.append(KvOp::Insert { key: b"k".to_vec(), loc: loc(1) });
        writer.publish();

        let reader = Arc::new(reader);
        let observer = {
            let reader = Arc::clone(&reader);
            loom::thread::spawn(move || {
                reader.enter().map(|keydir| keydir.locate(b"k").map(|(loc, _)| loc))
            })
        };

        // Op 2 and op 3: the relocation compaction would emit, and the swap
        // that makes it visible.
        writer.append(KvOp::Relocate { key: b"k".to_vec(), old: loc(1), new: loc(2) });
        writer.publish();

        let seen = observer.join().expect("the reader thread does not panic");
        // `None` for the handle itself is impossible here — the writer
        // outlives the reader — so the only two legal observations are the
        // pre- and post-relocation locations.
        let seen = seen.expect("the write handle is alive").expect("k is present in both copies");
        assert!(
            seen == loc(1) || seen == loc(2),
            "a reader observed {seen:?}, which is neither copy's state"
        );

        // And the writer's own view is the post-relocation one, whatever the
        // reader happened to catch.
        let after = reader.enter().expect("alive").locate(b"k").map(|(loc, _)| loc);
        assert_eq!(after, Some(loc(2)));
    });
}

/// The published-index protocol, which is the part of M11.5 that is *ours*
/// rather than left-right's: the writer stores the index with `Release` after
/// the publish that made it visible, and a reader that loads it with `Acquire`
/// and sees N must be able to see everything through N.
///
/// Getting the order backwards — store the index, then publish — is the bug
/// `Group::publish` is written to avoid, and this is the model that says so.
#[test]
fn a_reader_that_sees_the_published_index_sees_the_write() {
    loom::model(|| {
        let (mut writer, reader) = left_right::new::<KeyDir, KvOp>();
        writer.publish();

        let published = Arc::new(AtomicU64::new(0));
        let reader = Arc::new(reader);

        let checker = {
            let reader = Arc::clone(&reader);
            let published = Arc::clone(&published);
            loom::thread::spawn(move || {
                if published.load(Ordering::Acquire) >= 1 {
                    let seen = reader
                        .enter()
                        .expect("the write handle is alive")
                        .locate(b"k")
                        .map(|(loc, _)| loc);
                    assert_eq!(
                        seen,
                        Some(loc(1)),
                        "the published index advertised a write a reader cannot see"
                    );
                }
            })
        };

        writer.append(KvOp::Insert { key: b"k".to_vec(), loc: loc(1) });
        writer.publish();
        // After the publish, never before: the index is a promise about what
        // readers can already see. Inverting these two lines makes this model
        // fail, which is the evidence that it has teeth.
        published.store(1, Ordering::Release);

        checker.join().expect("the checking thread does not panic");
    });
}
