use criterion::{criterion_group, criterion_main, Criterion};

pub fn add_benchmark(c: &mut Criterion) {
    c.bench_function("add", |b| b.iter(|| 1 + 2));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
