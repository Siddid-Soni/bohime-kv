//! What a leader tells a request that waited too long (2026-09-18).
//!
//! `Group::request_timeout` exists for the partitioned leader: it still
//! believes it leads, so a write it accepts never commits and a read it
//! starts never confirms, and without a deadline the client waits forever.
//! Its own doc comment notes the difficulty — "the client cannot tell that
//! from slowness" — and until now neither could the driver, which answered
//! `NotLeader` to both.
//!
//! For the overloaded case that answer is a lie with a feedback loop on it.
//! The hint points back at this same node, so the client returns and
//! **re-proposes**, and the single signal that a leader was behind doubled
//! its offered load. Measured on tmpfs at 64 shards, with no disk in the
//! picture: 5170 -> 3019 -> 1665 -> 784 write op/s as clients went
//! 64 -> 128 -> 256 -> 512, with `NotLeader` replies per operation rising
//! from 0.43 to 1.11 over the same range.
//!
//! The separator is the commit index. A partitioned leader's cannot move.

use std::time::Duration;

use crate::driver::{Expiry, expiry_decision};

/// The overload case: leading, committing, and behind. The entry is already
/// in the log and will commit; giving up on it here only asks for another.
#[test]
fn a_leader_that_is_still_committing_lets_the_request_wait() {
    let decision = expiry_decision(true, Duration::from_millis(10), Duration::from_secs(1));
    assert!(
        decision == Expiry::Wait,
        "a committing leader answered {decision:?}, which sends the client back to propose a duplicate",
    );
}

/// The case the deadline was written for, and it must not have moved: a
/// leader whose commit index has not advanced for a whole `request_timeout`
/// cannot reach a quorum, and the client has to go looking elsewhere.
#[test]
fn a_leader_that_has_committed_nothing_still_redirects() {
    let decision = expiry_decision(true, Duration::from_secs(5), Duration::from_secs(1));
    assert!(
        decision == Expiry::Redirect,
        "a partitioned leader answered {decision:?}: the client would wait on a node that can \
         never commit its request",
    );
}

/// And a node that is not a leader at all redirects regardless of how
/// recently the log moved — a follower's commit index advances continuously.
#[test]
fn a_follower_redirects_however_recently_it_committed() {
    let decision = expiry_decision(false, Duration::from_millis(10), Duration::from_secs(1));
    assert!(decision == Expiry::Redirect, "a follower answered {decision:?} to an expired request",);
}
