use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use graft_query::executor::StorageAccess;
use graft_query::Value;
use graft_runtime::{Shard, ShardConfig};

// ===========================================================================
// 1. WAL WRITE THROUGHPUT — durable shard, measure write + WAL append cost
// ===========================================================================

fn bench_wal_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_write");

    for &n in &[100, 1_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("nodes", n), &n, |b, &n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let shard = Shard::open(
                        0,
                        &ShardConfig {
                            data_dir: dir.path().to_owned(),
                            pool_capacity: 1024,
                        },
                    )
                    .unwrap();
                    (dir, shard)
                },
                |(_dir, mut shard)| {
                    for i in 0..n {
                        shard.begin_tx();
                        shard.create_node(Some("N"), &[("v".into(), Value::Int(i as i64))]);
                        shard.commit_tx();
                    }
                    black_box(&shard);
                },
            );
        });
    }
    group.finish();
}

// ===========================================================================
// 2. FLUSH LATENCY — write then flush all dirty pages to disk
// ===========================================================================

fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush");

    for &n in &[100, 1_000] {
        group.bench_with_input(BenchmarkId::new("after_writes", n), &n, |b, &n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let mut shard = Shard::open(
                        0,
                        &ShardConfig {
                            data_dir: dir.path().to_owned(),
                            pool_capacity: 1024,
                        },
                    )
                    .unwrap();
                    for i in 0..n {
                        shard.begin_tx();
                        shard.create_node(Some("N"), &[("v".into(), Value::Int(i as i64))]);
                        shard.commit_tx();
                    }
                    (dir, shard)
                },
                |(_dir, shard)| {
                    shard.flush().unwrap();
                    black_box(&shard);
                },
            );
        });
    }
    group.finish();
}

// ===========================================================================
// 3. RECOVERY TIME — open a shard with existing data + WAL
// ===========================================================================

fn bench_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery");

    for &n in &[100, 1_000, 5_000] {
        // WAL replay: write data, don't flush pages (drop shard — WAL committed on each tx)
        group.bench_with_input(BenchmarkId::new("wal_replay", n), &n, |b, &n| {
            let dir = tempfile::tempdir().unwrap();
            {
                let mut shard = Shard::open(
                    0,
                    &ShardConfig {
                        data_dir: dir.path().to_owned(),
                        pool_capacity: 1024,
                    },
                )
                .unwrap();
                for i in 0..n {
                    shard.begin_tx();
                    shard.create_node(Some("N"), &[("v".into(), Value::Int(i as i64))]);
                    shard.commit_tx();
                }
                // Drop without flush — WAL has all committed records, data files don't
            }

            b.iter(|| {
                let shard = Shard::open(
                    0,
                    &ShardConfig {
                        data_dir: dir.path().to_owned(),
                        pool_capacity: 1024,
                    },
                )
                .unwrap();
                black_box(&shard);
            });
        });

        // Clean open: fully flushed, no WAL to replay
        group.bench_with_input(BenchmarkId::new("clean_open", n), &n, |b, &n| {
            let dir = tempfile::tempdir().unwrap();
            {
                let mut shard = Shard::open(
                    0,
                    &ShardConfig {
                        data_dir: dir.path().to_owned(),
                        pool_capacity: 1024,
                    },
                )
                .unwrap();
                for i in 0..n {
                    shard.begin_tx();
                    shard.create_node(Some("N"), &[("v".into(), Value::Int(i as i64))]);
                    shard.commit_tx();
                }
                shard.flush().unwrap();
            }

            b.iter(|| {
                let shard = Shard::open(
                    0,
                    &ShardConfig {
                        data_dir: dir.path().to_owned(),
                        pool_capacity: 1024,
                    },
                )
                .unwrap();
                black_box(&shard);
            });
        });
    }
    group.finish();
}

// ===========================================================================
// 4. DURABLE QUERY — end-to-end with persistence active
// ===========================================================================

fn bench_durable_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("durable_query");

    let dir = tempfile::tempdir().unwrap();
    let mut shard = Shard::open(
        0,
        &ShardConfig {
            data_dir: dir.path().to_owned(),
            pool_capacity: 1024,
        },
    )
    .unwrap();

    // Populate
    for i in 0..500 {
        shard.begin_tx();
        shard.create_node(
            Some("Person"),
            &[
                ("name".into(), Value::String(format!("person_{i}"))),
                ("age".into(), Value::Int(20 + (i % 50))),
            ],
        );
        shard.commit_tx();
    }
    shard.flush().unwrap();

    group.bench_function("scan_500", |b| {
        b.iter(|| {
            shard.begin_tx();
            let nodes = shard.scan_nodes(Some("Person"));
            shard.commit_tx();
            black_box(nodes);
        });
    });

    group.bench_function("point_lookup_500", |b| {
        let nodes = {
            shard.begin_tx();
            let n = shard.scan_nodes(Some("Person"));
            shard.commit_tx();
            n
        };
        let ids: Vec<_> = nodes.iter().map(|n| n.id).collect();
        let mut i = 0usize;
        b.iter(|| {
            shard.begin_tx();
            black_box(shard.get_node(ids[i % ids.len()]));
            shard.commit_tx();
            i += 1;
        });
    });

    group.bench_function("create_durable", |b| {
        let mut i = 0i64;
        b.iter(|| {
            shard.begin_tx();
            shard.create_node(Some("Bench"), &[("v".into(), Value::Int(i))]);
            shard.commit_tx();
            i += 1;
        });
    });

    group.finish();
}

// ===========================================================================
// 5. EPHEMERAL VS DURABLE — compare same operation with and without I/O
// ===========================================================================

fn bench_ephemeral_vs_durable(c: &mut Criterion) {
    let mut group = c.benchmark_group("ephemeral_vs_durable");
    let n = 1_000;
    group.throughput(Throughput::Elements(n as u64));

    group.bench_function("ephemeral_1000", |b| {
        b.iter_with_setup(
            || Shard::new(0),
            |mut shard| {
                for i in 0..n {
                    shard.begin_tx();
                    shard.create_node(Some("N"), &[("v".into(), Value::Int(i as i64))]);
                    shard.commit_tx();
                }
                black_box(&shard);
            },
        );
    });

    group.bench_function("durable_1000", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let shard = Shard::open(
                    0,
                    &ShardConfig {
                        data_dir: dir.path().to_owned(),
                        pool_capacity: 1024,
                    },
                )
                .unwrap();
                (dir, shard)
            },
            |(_dir, mut shard)| {
                for i in 0..n {
                    shard.begin_tx();
                    shard.create_node(Some("N"), &[("v".into(), Value::Int(i as i64))]);
                    shard.commit_tx();
                }
                black_box(&shard);
            },
        );
    });

    group.finish();
}

// ===========================================================================
// Group registration
// ===========================================================================

criterion_group!(
    benches,
    bench_wal_write,
    bench_flush,
    bench_recovery,
    bench_durable_query,
    bench_ephemeral_vs_durable,
);
criterion_main!(benches);
