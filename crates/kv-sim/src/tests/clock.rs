use crate::clock::{Clock, SimRng};

#[test]
fn clock_starts_at_zero_and_only_moves_when_advanced() {
    let mut clock = Clock::default();
    assert_eq!(clock.now(), 0);
    assert_eq!(clock.advance(), 1);
    assert_eq!(clock.now(), 1, "reading the clock must not advance it");
    clock.advance();
    assert_eq!(clock.now(), 2);
}

#[test]
fn the_same_seed_produces_the_same_draws() {
    let draws = |seed| {
        let mut rng = SimRng::new(seed);
        (0..200).map(|_| rng.below(1000)).collect::<Vec<_>>()
    };
    assert_eq!(draws(7), draws(7));
}

#[test]
fn different_seeds_produce_different_draws() {
    let draws = |seed| {
        let mut rng = SimRng::new(seed);
        (0..200).map(|_| rng.below(1000)).collect::<Vec<_>>()
    };
    assert_ne!(draws(7), draws(8), "seeds must actually diverge, or a sweep tests one run");
}

#[test]
fn chance_respects_its_bounds_exactly() {
    let mut rng = SimRng::new(1);
    assert!((0..1000).all(|_| !rng.chance(0)), "0% must never fire");
    assert!((0..1000).all(|_| rng.chance(100)), "100% must always fire");
}

#[test]
fn chance_is_roughly_the_requested_rate() {
    let mut rng = SimRng::new(42);
    let hits = (0..10_000).filter(|_| rng.chance(20)).count();
    assert!((1700..2300).contains(&hits), "20% of 10k should land near 2000, got {hits}");
}

#[test]
fn below_zero_is_zero_not_a_panic() {
    let mut rng = SimRng::new(3);
    assert_eq!(rng.below(0), 0);
}
