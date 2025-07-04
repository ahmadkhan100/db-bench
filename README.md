# db-bench

B-Tree vs LSM-Tree storage engine comparison

## Usage

```bash
cargo run --release
```

## Results

```
| Metric | RocksDB (LSM) | Sled (B-Tree) | Winner |
|--------|---------------|---------------|--------|
| Throughput | 155K ops/s | 112K ops/s | RocksDB (1.4x) |
| P99 Scan | 0.1ms | 0.1ms | RocksDB (1.0x) |
| Write Amp | 3x | 11x | RocksDB (3.7x) |
| Memory | 0MB | 128MB | RocksDB (lower) |
```

Compaction overhead (50K operations):
- RocksDB: 79MB written
- Sled: 395MB written (5x more)

Built to understand why B-Trees have 11x higher write amplification than LSM-Trees.