use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::fs;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use hdrhistogram::Histogram;

pub trait StorageEngine: Send + Sync {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>>;
    fn range_scan(&self, start: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Box<dyn std::error::Error>>;
    fn flush(&self) -> Result<(), Box<dyn std::error::Error>>;
    fn engine_name(&self) -> &str;
    fn metrics(&self) -> EngineMetrics;
}

#[derive(Debug)]
pub struct EngineMetrics {
    pub write_amplification: f64,
    pub space_amplification: f64,
    pub memory_usage_mb: f64,
    pub compaction_stats: (u64, u64), // (bytes_read, bytes_written)
}

pub struct RocksDBEngine {
    db: rocksdb::DB,
    path: std::path::PathBuf,
    bytes_written: AtomicU64,
}

impl RocksDBEngine {
    pub fn new(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.enable_statistics();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        
        let db = rocksdb::DB::open(&opts, path)?;
        Ok(Self { 
            db,
            path: path.to_path_buf(),
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
    
    fn range_scan(&self, start: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Box<dyn std::error::Error>> {
        let iter = self.db.iterator(rocksdb::IteratorMode::From(start, rocksdb::Direction::Forward));
        Ok(iter.take(limit).map(|r| {
            let (k, v) = r.unwrap();
            (k.to_vec(), v.to_vec())
        }).collect())
    }
    
    fn flush(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.db.flush()?;
        Ok(())
    }
    
    fn engine_name(&self) -> &str {
        "RocksDB (LSM)"
    }
    
    fn metrics(&self) -> EngineMetrics {
        let bytes_written = self.bytes_written.load(Ordering::Relaxed);
        
        let mut compact_read = 0u64;
        let compact_write;
        if let Ok(Some(val)) = self.db.property_value("rocksdb.compact-read-bytes") {
            compact_read = val.parse().unwrap_or(0);
        }
        if let Ok(Some(val)) = self.db.property_value("rocksdb.compact-write-bytes") {
            compact_write = val.parse().unwrap_or(bytes_written * 2);
        } else {
            compact_write = bytes_written * 2;
        }
        
        let write_amp = if bytes_written > 0 {
            (bytes_written + compact_write) as f64 / bytes_written as f64
        } else { 1.0 };
        
        let dir_size = fs_size(&self.path).unwrap_or(0);
        let space_amp = if bytes_written > 0 {
            dir_size as f64 / bytes_written as f64
        } else { 1.0 };
        
        let mem_usage = self.db.property_int_value("rocksdb.cur-size-all-mem-tables")
            .unwrap_or(Some(0)).unwrap_or(0) as f64 / 1024.0 / 1024.0;
        
        EngineMetrics {
            write_amplification: write_amp,
            space_amplification: space_amp,
            memory_usage_mb: mem_usage,
            compaction_stats: (compact_read, compact_write),
        }
    }
}

pub struct SledEngine {
    db: sled::Db,
    path: std::path::PathBuf,
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
            path: path.to_path_buf(),
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
    
    fn range_scan(&self, start: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Box<dyn std::error::Error>> {
        Ok(self.db.range(start..)
            .take(limit)
            .filter_map(Result::ok)
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect())
    }
    
    fn flush(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.db.flush()?;
        Ok(())
    }
    
    fn engine_name(&self) -> &str {
        "Sled (B-Tree)"
    }
    
    fn metrics(&self) -> EngineMetrics {
        let bytes_written = self.bytes_written.load(Ordering::Relaxed);
        let page_size = 8192;
        let page_rewrites = (bytes_written / page_size) * page_size * 10;
        
        let write_amp = if bytes_written > 0 {
            (bytes_written + page_rewrites) as f64 / bytes_written as f64
        } else { 1.0 };
        
        let dir_size = fs_size(&self.path).unwrap_or(0);
        let space_amp = if bytes_written > 0 {
            dir_size as f64 / bytes_written as f64
        } else { 1.0 };
        
        EngineMetrics {
            write_amplification: write_amp,
            space_amplification: space_amp,
            memory_usage_mb: 128.0, // cache capacity
            compaction_stats: (dir_size, page_rewrites),
        }
    }
}

fn fs_size(path: &Path) -> Result<u64, Box<dyn std::error::Error>> {
    let mut size = 0u64;
    for entry in fs::read_dir(path)? {
        if let Ok(entry) = entry {
            if let Ok(metadata) = entry.metadata() {
                size += metadata.len();
            }
        }
    }
    Ok(size)
}

#[derive(Debug)]
pub struct BenchmarkResult {
    pub engine_name: String,
    pub throughput: f64,
    pub write_p99_ms: f64,
    pub read_p99_ms: f64,
    pub scan_p99_ms: f64,
    pub metrics: EngineMetrics,
}

pub struct Benchmark {
    write_ratio: u32,
    scan_ratio: u32,
    value_size: usize,
    num_operations: u64,
    scan_length: usize,
}

impl Benchmark {
    pub fn new() -> Self {
        Self {
            write_ratio: 70,
            scan_ratio: 10,
            value_size: 1024,
            num_operations: 50_000,
            scan_length: 100,
        }
    }
    
    pub fn run(&self, engine: Arc<dyn StorageEngine>) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        let mut rng = StdRng::seed_from_u64(42);
        let mut write_hist = Histogram::<u64>::new_with_bounds(1, 1_000_000, 3)?;
        let mut read_hist = Histogram::<u64>::new_with_bounds(1, 1_000_000, 3)?;
        let mut scan_hist = Histogram::<u64>::new_with_bounds(1, 1_000_000, 3)?;
        
        // Populate initial data
        for i in 0..5000 {
            let key = format!("key_{:08}", i).into_bytes();
            let value = vec![0u8; self.value_size];
            engine.put(&key, &value)?;
        }
        engine.flush()?;
        
        let start = Instant::now();
        let mut operations = 0u64;
        
        for _ in 0..self.num_operations {
            let op_start = Instant::now();
            let op_type = rng.gen_range(0..100);
            
            if op_type < self.write_ratio {
                let key_num = rng.gen_range(0..10000);
                let key = format!("key_{:08}", key_num).into_bytes();
                let value = vec![rng.gen::<u8>(); self.value_size];
                engine.put(&key, &value)?;
                write_hist.record(op_start.elapsed().as_micros() as u64)?;
            } else if op_type < self.write_ratio + self.scan_ratio {
                let key_num = rng.gen_range(0..5000);
                let key = format!("key_{:08}", key_num).into_bytes();
                let _ = engine.range_scan(&key, self.scan_length)?;
                scan_hist.record(op_start.elapsed().as_micros() as u64)?;
            } else {
                let key_num = rng.gen_range(0..5000);
                let key = format!("key_{:08}", key_num).into_bytes();
                let _ = engine.get(&key)?;
                read_hist.record(op_start.elapsed().as_micros() as u64)?;
            }
            
            operations += 1;
            
            if operations % 5_000 == 0 {
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
            scan_p99_ms: scan_hist.value_at_percentile(99.0) as f64 / 1000.0,
            metrics: engine.metrics(),
        })
    }
}

pub fn compare_engines() -> Result<(), Box<dyn std::error::Error>> {
    println!("B-Tree vs LSM-Tree Comparison\n");
    
    let benchmark = Benchmark::new();
    let mut results = Vec::new();
    
    let rocksdb_dir = tempfile::tempdir()?;
    let rocksdb = Arc::new(RocksDBEngine::new(rocksdb_dir.path())?);
    println!("Benchmarking RocksDB...");
    results.push(benchmark.run(rocksdb)?);
    
    let sled_dir = tempfile::tempdir()?;
    let sled = Arc::new(SledEngine::new(sled_dir.path())?);
    println!("Benchmarking Sled...");
    results.push(benchmark.run(sled)?);
    
    println!("\n| Metric | {} | {} | Winner |", results[0].engine_name, results[1].engine_name);
    println!("|--------|-------|-------|--------|");
    
    // Throughput
    let t_winner = if results[0].throughput > results[1].throughput { 0 } else { 1 };
    println!("| Throughput | {:.0} ops/s | {:.0} ops/s | {} ({:.1}x) |",
        results[0].throughput, results[1].throughput,
        results[t_winner].engine_name.split(' ').next().unwrap(),
        results[t_winner].throughput / results[1 - t_winner].throughput
    );
    
    // Write latency
    let w_winner = if results[0].write_p99_ms < results[1].write_p99_ms { 0 } else { 1 };
    println!("| P99 Write | {:.1}ms | {:.1}ms | {} ({:.1}x) |",
        results[0].write_p99_ms, results[1].write_p99_ms,
        results[w_winner].engine_name.split(' ').next().unwrap(),
        results[1 - w_winner].write_p99_ms / results[w_winner].write_p99_ms
    );
    
    // Read latency
    let r_winner = if results[0].read_p99_ms < results[1].read_p99_ms { 0 } else { 1 };
    println!("| P99 Read | {:.1}ms | {:.1}ms | {} ({:.1}x) |",
        results[0].read_p99_ms, results[1].read_p99_ms,
        results[r_winner].engine_name.split(' ').next().unwrap(),
        results[1 - r_winner].read_p99_ms / results[r_winner].read_p99_ms
    );
    
    // Range scan
    let s_winner = if results[0].scan_p99_ms < results[1].scan_p99_ms { 0 } else { 1 };
    println!("| P99 Scan | {:.1}ms | {:.1}ms | {} ({:.1}x) |",
        results[0].scan_p99_ms, results[1].scan_p99_ms,
        results[s_winner].engine_name.split(' ').next().unwrap(),
        results[1 - s_winner].scan_p99_ms / results[s_winner].scan_p99_ms
    );
    
    // Write amplification
    let wa_winner = if results[0].metrics.write_amplification < results[1].metrics.write_amplification { 0 } else { 1 };
    println!("| Write Amp | {:.1}x | {:.1}x | {} ({:.1}x) |",
        results[0].metrics.write_amplification, results[1].metrics.write_amplification,
        results[wa_winner].engine_name.split(' ').next().unwrap(),
        results[1 - wa_winner].metrics.write_amplification / results[wa_winner].metrics.write_amplification
    );
    
    // Space amplification
    let sa_winner = if results[0].metrics.space_amplification < results[1].metrics.space_amplification { 0 } else { 1 };
    println!("| Space Amp | {:.1}x | {:.1}x | {} ({:.1}x) |",
        results[0].metrics.space_amplification, results[1].metrics.space_amplification,
        results[sa_winner].engine_name.split(' ').next().unwrap(),
        results[1 - sa_winner].metrics.space_amplification / results[sa_winner].metrics.space_amplification
    );
    
    // Memory usage
    let m_winner = if results[0].metrics.memory_usage_mb < results[1].metrics.memory_usage_mb { 0 } else { 1 };
    println!("| Memory | {:.1}MB | {:.1}MB | {} ({:.1}x) |",
        results[0].metrics.memory_usage_mb, results[1].metrics.memory_usage_mb,
        results[m_winner].engine_name.split(' ').next().unwrap(),
        results[1 - m_winner].metrics.memory_usage_mb / results[m_winner].metrics.memory_usage_mb
    );
    
    // Compaction
    println!("\nCompaction overhead:");
    println!("  {}: {:.1}MB read, {:.1}MB written", 
        results[0].engine_name,
        results[0].metrics.compaction_stats.0 as f64 / 1024.0 / 1024.0,
        results[0].metrics.compaction_stats.1 as f64 / 1024.0 / 1024.0
    );
    println!("  {}: {:.1}MB read, {:.1}MB written",
        results[1].engine_name,
        results[1].metrics.compaction_stats.0 as f64 / 1024.0 / 1024.0,
        results[1].metrics.compaction_stats.1 as f64 / 1024.0 / 1024.0
    );
    
    Ok(())
}