use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::Criterion;
use kvs::KvStore;
use kvs::KvsEngine;
use kvs::SledKvsEngine;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use tempfile::TempDir;

fn write_benchmark(c: &mut Criterion) {
    c.bench_function("kvs_write", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let store = KvStore::open(dir.into_path()).unwrap();
                store
            },
            |mut store| {
                let mut rng = SmallRng::from_seed([0; 32]);
                for _ in 0..100 {
                    let i = rng.gen_range(0..100000);
                    let key = format!("key{}", i);
                    let value = format!("value{}", i);
                    store.set(key, value).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
    c.bench_function("sled_write", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let engine = SledKvsEngine::new(sled::open(dir).unwrap());
                engine
            },
            |mut engine| {
                let mut rng = SmallRng::from_seed([0; 32]);
                for _ in 0..100 {
                    let i = rng.gen_range(0..100000);
                    let key = format!("key{}", i);
                    let value = format!("value{}", i);
                    engine.set(key, value).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn read_benchmark(c: &mut Criterion) {
    c.bench_function("kvs_read", |b| {
        let dir = TempDir::new().unwrap();
        let mut store = KvStore::open(dir.into_path()).unwrap();
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            store.set(key, value).unwrap();
        }
        let mut rng = SmallRng::from_seed([0; 32]);
        b.iter(|| {
            let key = format!("key{}", rng.gen_range(0..100));
            store.get(key).unwrap();
        });
    });
    c.bench_function("sled_read", |b| {
        let dir = TempDir::new().unwrap();
        let mut engine = SledKvsEngine::new(sled::open(dir).unwrap());
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            engine.set(key, value).unwrap();
        }
        let mut rng = SmallRng::from_seed([0; 32]);
        b.iter(|| {
            let key = format!("key{}", rng.gen_range(0..100));
            engine.get(key).unwrap();
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(46));
    targets = write_benchmark, read_benchmark
}
criterion_main!(benches);
