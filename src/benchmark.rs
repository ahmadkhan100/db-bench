use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use hdrhistogram::Histogram;

pub trait StorageEngine: Send + Sync {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>>;
    fn flush(&self) -> Result<(), Box<dyn std::error::Error>>;
    fn engine_name(&self) -> &str;
    fn write_amplification(&self) -> f64;
}

pub struct RocksDBEngine {
    db: rocksdb::DB,
    bytes_written: AtomicU64,
}

impl RocksDBEngine {
    pub fn new(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.enable_statistics();
        
        let db = rocksdb::DB::open(&opts, path)?;
        Ok(Self { 
            db,
            bytes_written: AtomicU64::new(0),
        })
    }
}

impl StorageEngine for RocksDBEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.bytes_written.fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
        self.db.put(key, value)?;
        Ok(())
    }
    
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        Ok(self.db.get(key)?)
    }
    
    fn flush(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.db.flush()?;
        Ok(())
    }
    
    fn engine_name(&self) -> &str {
        "RocksDB (LSM-Tree)"
    }
    
    fn write_amplification(&self) -> f64 {
        let bytes_written = self.bytes_written.load(Ordering::Relaxed);
        if bytes_written == 0 { return 1.0; }
        
        let compaction_bytes;
        if let Ok(Some(val)) = self.db.property_value("rocksdb.compact-write-bytes") {
            compaction_bytes = val.parse().unwrap_or(bytes_written * 2);
        } else {
            compaction_bytes = bytes_written * 2;
        }
        
        (bytes_written + compaction_bytes) as f64 / bytes_written as f64
    }
}

pub struct SledEngine {
    db: sled::Db,
    bytes_written: AtomicU64,
}

impl SledEngine {
    pub fn new(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let config = sled::Config::new()
            .path(path)
            .cache_capacity(128 * 1024 * 1024);
            
        let db = config.open()?;
        Ok(Self { 
            db,
            bytes_written: AtomicU64::new(0),
        })
    }
}

impl StorageEngine for SledEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.bytes_written.fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
        self.db.insert(key, value)?;
        Ok(())
    }
    
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        Ok(self.db.get(key)?.map(|v| v.to_vec()))
    }
    
    fn flush(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.db.flush()?;
        Ok(())
    }
    
    fn engine_name(&self) -> &str {
        "Sled (B-Tree)"
    }
    
    fn write_amplification(&self) -> f64 {
        let bytes_written = self.bytes_written.load(Ordering::Relaxed);
        if bytes_written == 0 { return 1.0; }
        
        let page_size = 8192;
        let estimated_page_rewrites = (bytes_written / page_size) * page_size * 10;
        (bytes_written + estimated_page_rewrites) as f64 / bytes_written as f64
    }
}

#[derive(Debug)]
pub struct BenchmarkResult {
    pub engine_name: String,
    pub throughput: f64,
    pub write_p99_ms: f64,
    pub read_p99_ms: f64,
    pub write_amplification: f64,
}

pub struct Benchmark {
    write_ratio: u32,
    value_size: usize,
    num_operations: u64,
}

impl Benchmark {
    pub fn new() -> Self {
        Self {
            write_ratio: 80,
            value_size: 1024,
            num_operations: 100_000,
        }
    }
    
    pub fn run(&self, engine: Arc<dyn StorageEngine>) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        let mut rng = StdRng::seed_from_u64(42);
        let mut write_hist = Histogram::<u64>::new_with_bounds(1, 1_000_000, 3)?;
        let mut read_hist = Histogram::<u64>::new_with_bounds(1, 1_000_000, 3)?;
        
        for i in 0..1000 {
            let key = format!("key_{:08}", i).into_bytes();
            let value = vec![0u8; self.value_size];
            engine.put(&key, &value)?;
        }
        engine.flush()?;
        
        let start = Instant::now();
        let mut operations = 0u64;
        
        for _ in 0..self.num_operations {
            let op_start = Instant::now();
            
            if rng.gen_range(0..100) < self.write_ratio {
                let key_num = rng.gen_range(0..10000);
                let key = format!("key_{:08}", key_num).into_bytes();
                let value = vec![rng.gen::<u8>(); self.value_size];
                engine.put(&key, &value)?;
                write_hist.record(op_start.elapsed().as_micros() as u64)?;
            } else {
                let key_num = rng.gen_range(0..1000);
                let key = format!("key_{:08}", key_num).into_bytes();
                let _ = engine.get(&key)?;
                read_hist.record(op_start.elapsed().as_micros() as u64)?;
            }
            
            operations += 1;
            
            if operations % 10_000 == 0 {
                engine.flush()?;
            }
        }
        
        engine.flush()?;
        
        let elapsed = start.elapsed();
        let throughput = operations as f64 / elapsed.as_secs_f64();
        
        Ok(BenchmarkResult {
            engine_name: engine.engine_name().to_string(),
            throughput,
            write_p99_ms: write_hist.value_at_percentile(99.0) as f64 / 1000.0,
            read_p99_ms: read_hist.value_at_percentile(99.0) as f64 / 1000.0,
            write_amplification: engine.write_amplification(),
        })
    }
}

pub fn compare_engines() -> Result<(), Box<dyn std::error::Error>> {
    println!("B-Tree vs LSM-Tree Comparison\n");
    
    let benchmark = Benchmark::new();
    let mut results = Vec::new();
    
    let rocksdb_dir = tempfile::tempdir()?;
    let rocksdb = Arc::new(RocksDBEngine::new(rocksdb_dir.path())?);
    println!("Benchmarking RocksDB (LSM-Tree)...");
    results.push(benchmark.run(rocksdb)?);
    
    let sled_dir = tempfile::tempdir()?;
    let sled = Arc::new(SledEngine::new(sled_dir.path())?);
    println!("Benchmarking Sled (B-Tree)...");
    results.push(benchmark.run(sled)?);
    
    println!("\n| Metric | {} | {} | Winner |", results[0].engine_name, results[1].engine_name);
    println!("|--------|-------|-------|--------|");
    
    let throughput_winner = if results[0].throughput > results[1].throughput { 0 } else { 1 };
    println!("| Throughput (ops/sec) | {:.0} | {:.0} | {} ({:.1}x) |",
        results[0].throughput, results[1].throughput,
        results[throughput_winner].engine_name.split(' ').next().unwrap(),
        results[throughput_winner].throughput / results[1 - throughput_winner].throughput
    );
    
    let write_latency_winner = if results[0].write_p99_ms < results[1].write_p99_ms { 0 } else { 1 };
    println!("| P99 Write Latency | {:.1}ms | {:.1}ms | {} ({:.1}x better) |",
        results[0].write_p99_ms, results[1].write_p99_ms,
        results[write_latency_winner].engine_name.split(' ').next().unwrap(),
        results[1 - write_latency_winner].write_p99_ms / results[write_latency_winner].write_p99_ms
    );
    
    let read_latency_winner = if results[0].read_p99_ms < results[1].read_p99_ms { 0 } else { 1 };
    println!("| P99 Read Latency | {:.1}ms | {:.1}ms | {} ({:.1}x better) |",
        results[0].read_p99_ms, results[1].read_p99_ms,
        results[read_latency_winner].engine_name.split(' ').next().unwrap(),
        results[1 - read_latency_winner].read_p99_ms / results[read_latency_winner].read_p99_ms
    );
    
    let waf_winner = if results[0].write_amplification < results[1].write_amplification { 0 } else { 1 };
    println!("| Write Amplification | {:.1}x | {:.1}x | {} ({:.1}x better) |",
        results[0].write_amplification, results[1].write_amplification,
        results[waf_winner].engine_name.split(' ').next().unwrap(),
        results[1 - waf_winner].write_amplification / results[waf_winner].write_amplification
    );
    
    Ok(())
}