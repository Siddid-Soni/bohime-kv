//! Virtual time and the single source of randomness (M4, task 1).
//!
//! The clock is virtual not for speed but for determinism: "what happens at
//! tick N" becomes a total order the seed fixes, so a 100k-seed sweep and a
//! replay of one seed take the identical code path.

use rand::SeedableRng;
use rand::rngs::StdRng;

/// Monotonic tick counter. Ticks advance only when the driver says so — there
/// is no relationship to wall time anywhere in the simulator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Clock {
    now: u64,
}

impl Clock {
    pub fn now(&self) -> u64 {
        self.now
    }

    pub fn advance(&mut self) -> u64 {
        self.now += 1;
        self.now
    }
}

/// The simulator's one RNG.
///
/// Every random decision draws from a `SimRng`, and every `SimRng` in a run
/// is derived from the run's one seed — so the run is a pure function of it.
///
/// Independent concerns get independent streams (the network has one, the
/// nemesis another) rather than sharing a single generator. Sharing would make
/// every nemesis decision depend on exactly how many draws the network had
/// made first, so adding one `chance()` call anywhere would reshuffle every
/// later decision and a recorded failing seed would stop reproducing the bug
/// it was recorded for. Separate streams keep each concern's sequence stable
/// under unrelated changes.
pub struct SimRng {
    rng: StdRng,
    seed: u64,
}

impl SimRng {
    pub fn new(seed: u64) -> Self {
        Self { rng: StdRng::seed_from_u64(seed), seed }
    }

    /// An independent stream derived from this one's seed. Deterministic, but
    /// its sequence does not shift when the parent's draw count changes.
    pub fn derive(seed: u64, stream: u64) -> Self {
        Self::new(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(stream))
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Uniform in `[0, n)`. Returns 0 for `n == 0` rather than panicking, so a
    /// zero-length choice at the edge of a run is not a crash.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        use rand::Rng;
        self.rng.gen_range(0..n)
    }

    /// True with probability `percent/100`, drawn as an integer.
    ///
    /// Integer comparison rather than `f64 < rate`: float accumulation order is
    /// not associative, and a rate that is exact in decimal is not exact in
    /// binary. Percentages are all the fault model needs.
    pub fn chance(&mut self, percent: u32) -> bool {
        if percent == 0 {
            return false;
        }
        self.below(100) < u64::from(percent.min(100))
    }
}
