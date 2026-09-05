//! Fsync-policy and throughput benchmarks (M1.7). Placeholder until then.

use criterion::{Criterion, criterion_group, criterion_main};

fn placeholder(c: &mut Criterion) {
    c.bench_function("placeholder", |b| b.iter(|| 2 + 2));
}

criterion_group!(benches, placeholder);
criterion_main!(benches);
