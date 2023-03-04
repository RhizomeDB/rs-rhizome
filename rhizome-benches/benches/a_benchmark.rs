use criterion::{criterion_group, criterion_main, Criterion};

pub fn add_benchmark(_c: &mut Criterion) {
    // c.bench_function("add", |b| {
    //     b.iter(|| {
    //         rhizome::parse(
    //             r#"
    //         edge(from: 0, to: 1).
    //         edge(from: 1, to: 2).
    //         edge(from: 2, to: 3).
    //         edge(from: 3, to: 4).

    //         path(from: X, to: Y) :- edge(from: X, to: Y).
    //         path(from: X, to: Z) :- edge(from: X, to: Y), path(from: Y, to: Z).
    //         "#,
    //         )
    //         .unwrap();
    //     })
    // });
}
criterion_group!(benches, add_benchmark);
criterion_main!(benches);
