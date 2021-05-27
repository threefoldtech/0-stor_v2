use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use zstor_v2::encryption::{AesGcm, Encryptor, SymmetricKey};

const ENCRYPTIONKEY: SymmetricKey = SymmetricKey::new([
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
]);

fn bench_aes_gcm_encryptor(c: &mut Criterion) {
    let enc = AesGcm::new(ENCRYPTIONKEY);

    let data = vec![0; 1 << 30];

    let mut group = c.benchmark_group("AES-GCM");
    // group.sample_size(10);
    group.throughput(Throughput::Bytes(1 << 10));
    group.bench_function("1KiB", |b| {
        b.iter(|| black_box(enc.encrypt(&data[..1 << 10])))
    });
    group.throughput(Throughput::Bytes(10 << 10));
    group.bench_function("10KiB", |b| {
        b.iter(|| black_box(enc.encrypt(&data[..10 << 10])))
    });
    group.throughput(Throughput::Bytes(100 << 10));
    group.bench_function("100KiB", |b| {
        b.iter(|| black_box(enc.encrypt(&data[..100 << 10])))
    });
    group.throughput(Throughput::Bytes(1 << 20));
    group.bench_function("1MiB", |b| {
        b.iter(|| black_box(enc.encrypt(&data[..1 << 20])))
    });
    group.throughput(Throughput::Bytes(10 << 20));
    group.bench_function("10MiB", |b| {
        b.iter(|| black_box(enc.encrypt(&data[..10 << 20])))
    });
    group.throughput(Throughput::Bytes(100 << 20));
    group.bench_function("100MiB", |b| {
        b.iter(|| black_box(enc.encrypt(&data[..100 << 20])))
    });
    // group.throughput(Throughput::Bytes(1 << 30));
    // group.bench_function("aes-gcm 1GiB", |b| b.iter(|| black_box(enc.encrypt(&data))));
    group.finish();
}

criterion_group!(benches, bench_aes_gcm_encryptor);
criterion_main!(benches);
