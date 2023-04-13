use criterion::{criterion_group, criterion_main, Criterion};

pub fn add_benchmark(_c: &mut Criterion) {}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
