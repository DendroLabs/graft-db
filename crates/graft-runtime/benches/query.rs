use criterion::{black_box, criterion_group, criterion_main, Criterion};
use graft_runtime::Database;

fn setup_social_network(n_people: usize) -> Database {
    let mut db = Database::new();
    for i in 0..n_people {
        db.query(&format!(
            "CREATE (:Person {{name: 'person_{i}', age: {age}}})",
            age = 20 + (i % 50)
        ))
        .unwrap();
    }
    // Create a chain of KNOWS edges: 0->1->2->...->n-1
    for i in 0..n_people - 1 {
        db.query(&format!(
            "MATCH (a:Person {{name: 'person_{i}'}}), (b:Person {{name: 'person_{next}'}}) \
             CREATE (a)-[:KNOWS]->(b)",
            next = i + 1
        ))
        .unwrap();
    }
    db
}

fn bench_scan(c: &mut Criterion) {
    let mut db = setup_social_network(100);

    c.bench_function("scan_100_nodes", |b| {
        b.iter(|| {
            black_box(
                db.query("MATCH (p:Person) RETURN p.name").unwrap(),
            );
        });
    });
}

fn bench_filter(c: &mut Criterion) {
    let mut db = setup_social_network(100);

    c.bench_function("filter_100_nodes", |b| {
        b.iter(|| {
            black_box(
                db.query("MATCH (p:Person) WHERE p.age > 40 RETURN p.name")
                    .unwrap(),
            );
        });
    });
}

fn bench_single_hop(c: &mut Criterion) {
    let mut db = setup_social_network(100);

    c.bench_function("single_hop_traversal", |b| {
        b.iter(|| {
            black_box(
                db.query(
                    "MATCH (a:Person {name: 'person_0'})-[:KNOWS]->(b:Person) RETURN b.name",
                )
                .unwrap(),
            );
        });
    });
}

fn bench_two_hop(c: &mut Criterion) {
    let mut db = setup_social_network(100);

    c.bench_function("two_hop_traversal", |b| {
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
}

fn bench_var_length_path(c: &mut Criterion) {
    let mut db = setup_social_network(100);

    c.bench_function("var_length_1_to_5", |b| {
        b.iter(|| {
            black_box(
                db.query(
                    "MATCH (a:Person {name: 'person_0'})-[:KNOWS*1..5]->(b:Person) RETURN b.name",
                )
                .unwrap(),
            );
        });
    });
}

fn bench_aggregation(c: &mut Criterion) {
    let mut db = setup_social_network(100);

    c.bench_function("count_100_nodes", |b| {
        b.iter(|| {
            black_box(
                db.query("MATCH (p:Person) RETURN COUNT(p)").unwrap(),
            );
        });
    });

    c.bench_function("avg_100_nodes", |b| {
        b.iter(|| {
            black_box(
                db.query("MATCH (p:Person) RETURN AVG(p.age)").unwrap(),
            );
        });
    });
}

fn bench_order_by(c: &mut Criterion) {
    let mut db = setup_social_network(100);

    c.bench_function("order_by_100_nodes", |b| {
        b.iter(|| {
            black_box(
                db.query("MATCH (p:Person) RETURN p.name ORDER BY p.age")
                    .unwrap(),
            );
        });
    });
}

fn bench_create_node(c: &mut Criterion) {
    c.bench_function("create_node", |b| {
        let mut db = Database::new();
        let mut i = 0u64;
        b.iter(|| {
            db.query(&format!("CREATE (:N {{v: {i}}})")).unwrap();
            i += 1;
        });
    });
}

fn bench_parse_plan_execute(c: &mut Criterion) {
    let mut db = Database::new();
    db.query("CREATE (:Person {name: 'Alice', age: 30})")
        .unwrap();

    c.bench_function("parse_plan_execute_simple", |b| {
        b.iter(|| {
            black_box(
                db.query("MATCH (p:Person {name: 'Alice'}) RETURN p.age")
                    .unwrap(),
            );
        });
    });
}

criterion_group!(
    benches,
    bench_scan,
    bench_filter,
    bench_single_hop,
    bench_two_hop,
    bench_var_length_path,
    bench_aggregation,
    bench_order_by,
    bench_create_node,
    bench_parse_plan_execute,
);
criterion_main!(benches);
