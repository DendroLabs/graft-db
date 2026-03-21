//! Wire protocol benchmark — measures end-to-end query latency over TCP.
//! Run graft-server first, then: cargo run --release --example bench_wire

use std::time::{Duration, Instant};

use graft_client::Client;

fn main() {
    let addr = "127.0.0.1:7687";
    let mut client = Client::connect(addr).expect("failed to connect to graft-server");

    println!("Connected to {addr}");
    println!("==============================================");
    println!("graft wire protocol benchmark (localhost TCP)");
    println!("==============================================\n");

    // -- Setup: create test data -----------------------------------------------
    print!("Setting up test data... ");
    let setup_start = Instant::now();

    // 1000 Person nodes
    for i in 0..1000 {
        client
            .query(&format!(
                "CREATE (:Person {{name: 'person_{i}', age: {age}}})",
                age = 20 + (i % 50)
            ))
            .unwrap();
    }

    // Chain of KNOWS edges: 0->1->2->...->999
    for i in 0..999 {
        client
            .query(&format!(
                "MATCH (a:Person {{name: 'person_{i}'}}), (b:Person {{name: 'person_{next}'}}) \
                 CREATE (a)-[:KNOWS]->(b)",
                next = i + 1
            ))
            .unwrap();
    }
    println!("done ({:.2}s)\n", setup_start.elapsed().as_secs_f64());

    // -- Benchmarks ------------------------------------------------------------

    // Helper: run a query N times, report stats
    let bench = |name: &str, query: &str, iterations: usize, client: &mut Client| {
        // Warmup
        for _ in 0..5 {
            client.query(query).unwrap();
        }

        let mut times = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let start = Instant::now();
            client.query(query).unwrap();
            times.push(start.elapsed());
        }

        times.sort();
        let total: Duration = times.iter().sum();
        let avg = total / iterations as u32;
        let p50 = times[iterations / 2];
        let p95 = times[(iterations as f64 * 0.95) as usize];
        let p99 = times[(iterations as f64 * 0.99) as usize];
        let min = times[0];
        let max = times[iterations - 1];
        let qps = iterations as f64 / total.as_secs_f64();

        println!("  {name}");
        println!("    iterations: {iterations}");
        println!(
            "    avg: {:>10.1} µs    p50: {:>10.1} µs",
            avg.as_nanos() as f64 / 1000.0,
            p50.as_nanos() as f64 / 1000.0
        );
        println!(
            "    p95: {:>10.1} µs    p99: {:>10.1} µs",
            p95.as_nanos() as f64 / 1000.0,
            p99.as_nanos() as f64 / 1000.0
        );
        println!(
            "    min: {:>10.1} µs    max: {:>10.1} µs",
            min.as_nanos() as f64 / 1000.0,
            max.as_nanos() as f64 / 1000.0
        );
        println!("    throughput: {qps:.0} queries/sec");
        println!();
    };

    println!("--- Point Lookup ---");
    bench(
        "GET node by property match",
        "MATCH (p:Person {name: 'person_500'}) RETURN p.age",
        1000,
        &mut client,
    );

    println!("--- Scan ---");
    bench(
        "SCAN 1000 nodes (return count)",
        "MATCH (p:Person) RETURN COUNT(*)",
        1000,
        &mut client,
    );
    bench(
        "SCAN 1000 nodes (return all names)",
        "MATCH (p:Person) RETURN p.name",
        100,
        &mut client,
    );

    println!("--- Filter ---");
    bench(
        "WHERE filter (age > 40)",
        "MATCH (p:Person) WHERE p.age > 40 RETURN p.name",
        500,
        &mut client,
    );

    println!("--- Traversal ---");
    bench(
        "Single-hop traversal",
        "MATCH (a:Person {name: 'person_0'})-[:KNOWS]->(b:Person) RETURN b.name",
        1000,
        &mut client,
    );
    bench(
        "Two-hop traversal",
        "MATCH (a:Person {name: 'person_0'})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN c.name",
        1000,
        &mut client,
    );
    bench(
        "Variable-length path *1..5",
        "MATCH (a:Person {name: 'person_0'})-[:KNOWS*1..5]->(b:Person) RETURN b.name",
        500,
        &mut client,
    );

    println!("--- Aggregation ---");
    bench(
        "AVG over 1000 nodes",
        "MATCH (p:Person) RETURN AVG(p.age)",
        500,
        &mut client,
    );

    println!("--- Mutation ---");
    bench(
        "CREATE single node",
        "CREATE (:Bench {v: 1})",
        1000,
        &mut client,
    );
    bench(
        "SET property",
        "MATCH (p:Person {name: 'person_500'}) SET p.age = 99",
        1000,
        &mut client,
    );

    println!("--- ORDER BY ---");
    bench(
        "ORDER BY 1000 nodes",
        "MATCH (p:Person) RETURN p.name ORDER BY p.age",
        100,
        &mut client,
    );

    println!("Done.");
}
