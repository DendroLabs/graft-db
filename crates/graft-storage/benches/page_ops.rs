use criterion::{black_box, criterion_group, criterion_main, Criterion};
use graft_storage::{Page, PageType, NodeRecord, EdgeRecord};
use graft_core::{NodeId, EdgeId, LabelId};

fn bench_page_alloc_free(c: &mut Criterion) {
    c.bench_function("page_alloc_slot", |b| {
        b.iter(|| {
            let mut page = Page::new(0, PageType::Node);
            for _ in 0..127 {
                black_box(page.alloc_slot().unwrap());
            }
        });
    });

    c.bench_function("page_alloc_free_cycle", |b| {
        b.iter(|| {
            let mut page = Page::new(0, PageType::Node);
            let slots: Vec<_> = (0..127).map(|_| page.alloc_slot().unwrap()).collect();
            for slot in slots {
                page.free_slot(slot);
            }
        });
    });
}

fn bench_record_write_read(c: &mut Criterion) {
    c.bench_function("node_record_write_read", |b| {
        let mut page = Page::new(0, PageType::Node);
        let slot = page.alloc_slot().unwrap();
        let record = NodeRecord::new(NodeId::new(0, 1), LabelId::new(1), 1);

        b.iter(|| {
            page.write_record(slot, &record.to_bytes()).unwrap();
            let bytes = page.read_record(slot).unwrap();
            black_box(NodeRecord::read_from(bytes));
        });
    });

    c.bench_function("edge_record_write_read", |b| {
        let mut page = Page::new(0, PageType::Edge);
        let slot = page.alloc_slot().unwrap();
        let record = EdgeRecord::new(
            EdgeId::new(0, 1),
            NodeId::new(0, 1),
            NodeId::new(0, 2),
            LabelId::new(1),
            1,
        );

        b.iter(|| {
            page.write_record(slot, &record.to_bytes()).unwrap();
            let bytes = page.read_record(slot).unwrap();
            black_box(EdgeRecord::read_from(bytes));
        });
    });
}

fn bench_checksum(c: &mut Criterion) {
    c.bench_function("page_checksum", |b| {
        let mut page = Page::new(0, PageType::Node);
        for _ in 0..64 {
            page.alloc_slot();
        }

        b.iter(|| {
            page.update_checksum();
            black_box(page.verify_checksum());
        });
    });
}

criterion_group!(benches, bench_page_alloc_free, bench_record_write_read, bench_checksum);
criterion_main!(benches);
