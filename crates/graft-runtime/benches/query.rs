use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use graft_query::executor::StorageAccess;
use graft_runtime::{Database, Shard};

// ===========================================================================
// Helpers
// ===========================================================================

/// Build a single-shard database with `n` Person nodes and a KNOWS chain.
fn social_chain(n: usize) -> Database {
    let mut db = Database::new();
    for i in 0..n {
        db.query(&format!(
            "CREATE (:Person {{name: 'person_{i}', age: {age}}})",
            age = 20 + (i % 50)
        ))
        .unwrap();
    }
    for i in 0..n - 1 {
        db.query(&format!(
            "MATCH (a:Person {{name: 'person_{i}'}}), (b:Person {{name: 'person_{next}'}}) \
             CREATE (a)-[:KNOWS]->(b)",
            next = i + 1
        ))
        .unwrap();
    }
    db
}

/// Build a shard with `n` nodes via the direct StorageAccess API (no parsing).
fn shard_with_nodes(n: usize) -> Shard {
    use graft_query::Value;
    let mut shard = Shard::new(0);
    for i in 0..n {
        shard.create_node(
            Some("Person"),
            &[
                ("name".into(), Value::String(format!("person_{i}"))),
                ("age".into(), Value::Int(20 + (i as i64 % 50))),
            ],
        );
    }
    shard
}

/// Build a shard with `n` nodes connected in a chain via direct API.
fn shard_with_chain(n: usize) -> Shard {
    use graft_query::Value;
    let mut shard = Shard::new(0);
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let id = shard.create_node(
            Some("Person"),
            &[
                ("name".into(), Value::String(format!("person_{i}"))),
                ("age".into(), Value::Int(20 + (i as i64 % 50))),
            ],
        );
        ids.push(id);
    }
    for i in 0..n - 1 {
        shard.create_edge(ids[i], ids[i + 1], Some("KNOWS"), &[]);
    }
    shard
}

/// Build a shard where node 0 has `degree` outbound edges to distinct targets.
fn shard_with_fan_out(degree: usize) -> Shard {
    use graft_query::Value;
    let mut shard = Shard::new(0);
    let hub = shard.create_node(Some("Hub"), &[("name".into(), Value::String("hub".into()))]);
    for i in 0..degree {
        let target = shard.create_node(
            Some("Leaf"),
            &[("name".into(), Value::String(format!("leaf_{i}")))],
        );
        shard.create_edge(hub, target, Some("LINK"), &[]);
    }
    shard
}

// ===========================================================================
// 1. INSERT THROUGHPUT — raw StorageAccess, no parsing overhead
// ===========================================================================

fn bench_insert_node(c: &mut Criterion) {
    use graft_query::Value;
    let mut group = c.benchmark_group("insert_node");

    for &n in &[100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_with_setup(
                || Shard::new(0),
                |mut shard| {
                    for i in 0..n {
                        shard.create_node(
                            Some("N"),
                            &[("v".into(), Value::Int(i as i64))],
                        );
                    }
                    black_box(&shard);
                },
            );
        });
    }
    group.finish();
}

fn bench_insert_edge(c: &mut Criterion) {
    use graft_query::Value;
    let mut group = c.benchmark_group("insert_edge");

    for &n in &[100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_with_setup(
                || {
                    let mut shard = Shard::new(0);
                    let ids: Vec<_> = (0..n + 1)
                        .map(|i| shard.create_node(Some("N"), &[("v".into(), Value::Int(i as i64))]))
                        .collect();
                    (shard, ids)
                },
                |(mut shard, ids)| {
                    for i in 0..n {
                        shard.create_edge(ids[i], ids[i + 1], Some("E"), &[]);
                    }
                    black_box(&shard);
                },
            );
        });
    }
    group.finish();
}

// ===========================================================================
// 2. POINT LOOKUP — get_node + node_property by ID
// ===========================================================================

fn bench_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_lookup");

    for &n in &[100, 1_000, 10_000] {
        let shard = shard_with_nodes(n);

        // Collect all node IDs for lookup
        let nodes = shard.scan_nodes(Some("Person"));
        let ids: Vec<_> = nodes.iter().map(|n| n.id).collect();

        group.bench_with_input(BenchmarkId::new("get_node", n), &ids, |b, ids| {
            let mut i = 0usize;
            b.iter(|| {
                black_box(shard.get_node(ids[i % ids.len()]));
                i += 1;
            });
        });

        group.bench_with_input(BenchmarkId::new("node_property", n), &ids, |b, ids| {
            let mut i = 0usize;
            b.iter(|| {
                black_box(shard.node_property(ids[i % ids.len()], "name"));
                i += 1;
            });
        });
    }
    group.finish();
}

// ===========================================================================
// 3. SCAN THROUGHPUT — full label scan
// ===========================================================================

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");

    for &n in &[100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        let shard = shard_with_nodes(n);

        group.bench_with_input(BenchmarkId::new("label_scan", n), &shard, |b, shard| {
            b.iter(|| {
                black_box(shard.scan_nodes(Some("Person")));
            });
        });

        group.bench_with_input(BenchmarkId::new("full_scan", n), &shard, |b, shard| {
            b.iter(|| {
                black_box(shard.scan_nodes(None));
            });
        });
    }
    group.finish();
}

// ===========================================================================
// 4. TRAVERSAL THROUGHPUT — single-hop, multi-hop, variable-length
// ===========================================================================

fn bench_single_hop_traversal(c: &mut Criterion) {
    let mut group = c.benchmark_group("traversal_single_hop");

    for &degree in &[1, 10, 100, 1_000] {
        group.throughput(Throughput::Elements(degree as u64));
        let shard = shard_with_fan_out(degree);
        let hub = shard.scan_nodes(Some("Hub"))[0].id;

        group.bench_with_input(
            BenchmarkId::new("outbound", degree),
            &(shard, hub),
            |b, (shard, hub)| {
                b.iter(|| {
                    black_box(shard.outbound_edges(*hub, None));
                });
            },
        );
    }
    group.finish();
}

fn bench_multi_hop_traversal(c: &mut Criterion) {
    let mut group = c.benchmark_group("traversal_multi_hop");

    let shard = shard_with_chain(1_000);
    let first_node = shard.scan_nodes(Some("Person")).into_iter()
        .find(|n| {
            let rec_name = shard.node_property(n.id, "name");
            rec_name == graft_query::Value::String("person_0".into())
        })
        .unwrap().id;

    // Measure k-hop traversal by walking the chain
    for &hops in &[1, 2, 5, 10] {
        group.bench_with_input(
            BenchmarkId::new("chain_walk", hops),
            &hops,
            |b, &hops| {
                b.iter(|| {
                    let mut current = first_node;
                    for _ in 0..hops {
                        let edges = shard.outbound_edges(current, Some("KNOWS"));
                        if let Some(e) = edges.first() {
                            current = e.target;
                        } else {
                            break;
                        }
                    }
                    black_box(current);
                });
            },
        );
    }
    group.finish();
}

// ===========================================================================
// 5. END-TO-END GQL QUERIES (parse + plan + execute)
// ===========================================================================

fn bench_e2e_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_gql");

    let mut db = social_chain(100);

    group.bench_function("scan_return", |b| {
        b.iter(|| {
            black_box(db.query("MATCH (p:Person) RETURN p.name").unwrap());
        });
    });

    group.bench_function("filter_where", |b| {
        b.iter(|| {
            black_box(
                db.query("MATCH (p:Person) WHERE p.age > 40 RETURN p.name")
                    .unwrap(),
            );
        });
    });

    group.bench_function("single_hop", |b| {
        b.iter(|| {
            black_box(
                db.query(
                    "MATCH (a:Person {name: 'person_0'})-[:KNOWS]->(b:Person) RETURN b.name",
                )
                .unwrap(),
            );
        });
    });

    group.bench_function("two_hop", |b| {
        b.iter(|| {
            black_box(
                db.query(
                    "MATCH (a:Person {name: 'person_0'})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
                     RETURN c.name",
                )
                .unwrap(),
            );
        });
    });

    group.bench_function("var_path_1_to_5", |b| {
        b.iter(|| {
            black_box(
                db.query(
                    "MATCH (a:Person {name: 'person_0'})-[:KNOWS*1..5]->(b:Person) RETURN b.name",
                )
                .unwrap(),
            );
        });
    });

    group.bench_function("count_star", |b| {
        b.iter(|| {
            black_box(db.query("MATCH (p:Person) RETURN COUNT(*)").unwrap());
        });
    });

    group.bench_function("avg", |b| {
        b.iter(|| {
            black_box(db.query("MATCH (p:Person) RETURN AVG(p.age)").unwrap());
        });
    });

    group.bench_function("order_by", |b| {
        b.iter(|| {
            black_box(
                db.query("MATCH (p:Person) RETURN p.name ORDER BY p.age")
                    .unwrap(),
            );
        });
    });

    group.bench_function("create_node", |b| {
        let mut i = 0u64;
        b.iter(|| {
            db.query(&format!("CREATE (:Bench {{v: {i}}})")).unwrap();
            i += 1;
        });
    });

    group.bench_function("point_lookup", |b| {
        b.iter(|| {
            black_box(
                db.query("MATCH (p:Person {name: 'person_50'}) RETURN p.age")
                    .unwrap(),
            );
        });
    });

    group.finish();
}

// ===========================================================================
// 6. MULTI-SHARD — measure cluster overhead vs single-shard
// ===========================================================================

fn bench_multi_shard(c: &mut Criterion) {
    use graft_runtime::ShardCluster;

    let mut group = c.benchmark_group("multi_shard");

    for &shards in &[1, 2, 4] {
        let mut cluster = ShardCluster::new(shards);
        for i in 0..200 {
            cluster
                .query(&format!(
                    "CREATE (:Person {{name: 'person_{i}', age: {age}}})",
                    age = 20 + (i % 50)
                ))
                .unwrap();
        }

        group.bench_with_input(
            BenchmarkId::new("scan_200", shards),
            &shards,
            |b, _| {
                b.iter(|| {
                    black_box(
                        cluster
                            .query("MATCH (p:Person) RETURN p.name")
                            .unwrap(),
                    );
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("count_200", shards),
            &shards,
            |b, _| {
                b.iter(|| {
                    black_box(
                        cluster.query("MATCH (p:Person) RETURN COUNT(*)").unwrap(),
                    );
                });
            },
        );
    }
    group.finish();
}

// ===========================================================================
// 7. MEMORY OVERHEAD — measure bytes per node/edge
//    (not a Criterion benchmark, but printed as a one-shot)
// ===========================================================================

fn bench_memory_overhead(c: &mut Criterion) {
    use graft_query::Value;

    c.bench_function("memory_baseline_10k_nodes", |b| {
        b.iter_with_setup(
            || Shard::new(0),
            |mut shard| {
                for i in 0..10_000 {
                    shard.create_node(
                        Some("N"),
                        &[("v".into(), Value::Int(i))],
                    );
                }
                black_box(&shard);
            },
        );
    });
}

// ===========================================================================
// Group registration
// ===========================================================================

criterion_group!(
    benches,
    bench_insert_node,
    bench_insert_edge,
    bench_point_lookup,
    bench_scan,
    bench_single_hop_traversal,
    bench_multi_hop_traversal,
    bench_e2e_queries,
    bench_multi_shard,
    bench_memory_overhead,
);
criterion_main!(benches);
