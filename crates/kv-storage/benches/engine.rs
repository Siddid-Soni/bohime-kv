//! Fsync-policy throughput benchmarks (M1.7).

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kv_storage::{Engine, EngineConfig, FsyncPolicy};

fn bench_fsync_policies(c: &mut Criterion) {
    let mut group = c.benchmark_group("fsync_policy");
    group.throughput(Throughput::Elements(1_000));

    let policies = [
        ("never", FsyncPolicy::Never),
        ("every_write", FsyncPolicy::EveryWrite),
        (
            "group_commit_100",
            FsyncPolicy::GroupCommit {
                max_records: 100,
                max_delay: std::time::Duration::from_millis(10),
            },
        ),
    ];

    for (name, policy) in policies {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let config = EngineConfig { fsync_policy: policy, ..Default::default() };
                    let engine = Engine::open_with_config(dir.path(), config).unwrap();
                    (dir, engine)
                },
                |(dir, mut engine)| {
                    for i in 0..1_000u32 {
                        engine.put(format!("key-{i}").as_bytes(), b"value").unwrap();
                    }
                    drop(engine);
                    drop(dir);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_fsync_policies);
criterion_main!(benches);
