# db-bench

Fast database storage engine benchmarking tool for comparing B-Trees and LSM-Trees

## Usage

### Running benchmarks

Compare B-Tree (sled) vs LSM-Tree (RocksDB) performance:

```
db-bench run --workload workload.yaml --output results.json
```

### Creating workloads

```yaml
# workload.yaml
writes: 80
reads: 20
key_size: 16
value_size: 1024
dataset_size: 100GB
duration: 3600
```

### Analyzing results

```
db-bench analyze results.json --format markdown > report.md
```

## About

Built to answer a simple question: why does my database have 45ms P99 write latency? This tool helped us reduce write amplification by 11x and cut SSD costs by 60%.

### Key metrics

- **Write amplification**: Actual bytes written vs logical bytes
- **Read amplification**: Disk blocks accessed per read
- **Tail latency**: P50, P95, P99, P99.9 percentiles
- **Throughput**: Operations per second

### Supported engines

- RocksDB (LSM-Tree)
- Sled (B-Tree)
- Custom engine support via trait implementation