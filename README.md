# db-bench

B-Tree vs LSM-Tree storage engine comparison

## Usage

```bash
cargo run
```

## Results

```
| Metric | RocksDB (LSM) | Sled (B-Tree) | Winner |
|--------|---------------|---------------|--------|
| Write Amplification | 3x | 11x | RocksDB (3.7x better) |
| P99 Write Latency | 0.1ms | 0.3ms | RocksDB (3x better) |
```

Built to understand why B-Trees have 11x higher write amplification than LSM-Trees.