use crate::engine::StorageEngine;
use crate::metrics::{MetricsCollector, BenchmarkResult};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::sync::Arc;
use std::time::{Duration, Instant};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::time::interval;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkloadConfig {
    pub writes: u32,  // Percentage of writes (0-100)
    pub reads: u32,   // Percentage of reads (0-100)
    pub key_size: usize,
    pub value_size: usize,
    pub dataset_size: String,  // e.g., "100GB"
    pub duration: u64,  // seconds
}

impl WorkloadConfig {
    pub fn from_file(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_yaml::from_str(&content)?;
        
        if config.writes + config.reads != 100 {
            return Err("Writes + reads must equal 100%".into());
        }
        
        Ok(config)
    }
    
    pub fn total_keys(&self) -> u64 {
        // Parse dataset size
        let size_str = self.dataset_size.to_lowercase();
        let multiplier = if size_str.ends_with("gb") {
            1_000_000_000
        } else if size_str.ends_with("mb") {
            1_000_000
        } else {
            1
        };
        
        let size_num: u64 = size_str
            .chars()
            .filter(|c| c.is_numeric())
            .collect::<String>()
            .parse()
            .unwrap_or(1);
            
        let total_bytes = size_num * multiplier;
        total_bytes / (self.key_size + self.value_size) as u64
    }
}

pub struct Workload {
    config: WorkloadConfig,
    engine: Arc<dyn StorageEngine>,
    collector: MetricsCollector,
}

impl Workload {
    pub fn new(
        config: WorkloadConfig,
        engine: Arc<dyn StorageEngine>,
        collector: &mut MetricsCollector,
    ) -> Self {
        Self {
            config,
            engine,
            collector: collector.clone(),
        }
    }
    
    pub async fn run(&self) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        let total_keys = self.config.total_keys();
        let duration = Duration::from_secs(self.config.duration);
        
        println!("Running workload:");
        println!("  - Total keys: {}", total_keys);
        println!("  - Duration: {} seconds", self.config.duration);
        println!("  - Write ratio: {}%", self.config.writes);
        println!("  - Read ratio: {}%", self.config.reads);
        
        // Pre-populate some data
        self.populate_initial_data(total_keys / 10).await?;
        
        // Run the actual benchmark
        let start = Instant::now();
        let mut operations = 0u64;
        
        // Progress bar
        let pb = ProgressBar::new(self.config.duration);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        
        // Spawn progress updater
        let pb_clone = pb.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                pb_clone.inc(1);
            }
        });
        
        // Main benchmark loop
        let mut rng = StdRng::seed_from_u64(42);
        
        while start.elapsed() < duration {
            let _op_type = if rng.gen_range(0..100) < self.config.writes {
                self.perform_write(&mut rng, total_keys).await?
            } else {
                self.perform_read(&mut rng, total_keys).await?
            };
            
            operations += 1;
            
            // Periodic flush
            if operations % 10_000 == 0 {
                self.engine.flush()?;
            }
        }
        
        pb.finish_with_message("Benchmark complete!");
        
        // Final flush
        self.engine.flush()?;
        
        // Collect final metrics
        let elapsed = start.elapsed();
        let throughput = operations as f64 / elapsed.as_secs_f64();
        
        Ok(BenchmarkResult {
            engine_type: self.engine.engine_type(),
            throughput,
            operations,
            duration: elapsed,
            latencies: self.collector.get_latencies(),
            write_amplification: self.calculate_write_amplification(),
        })
    }
    
    async fn populate_initial_data(&self, count: u64) -> Result<(), Box<dyn std::error::Error>> {
        println!("Pre-populating {} keys...", count);
        let mut rng = StdRng::seed_from_u64(42);
        
        for i in 0..count {
            let key = self.generate_key(&mut rng, i);
            let value = self.generate_value(&mut rng);
            self.engine.put(&key, &value)?;
            
            if i % 10_000 == 0 {
                self.engine.flush()?;
            }
        }
        
        self.engine.flush()?;
        println!("Pre-population complete");
        Ok(())
    }
    
    async fn perform_write(
        &self,
        rng: &mut StdRng,
        total_keys: u64,
    ) -> Result<&'static str, Box<dyn std::error::Error>> {
        let start = Instant::now();
        
        let key_num = rng.gen_range(0..total_keys);
        let key = self.generate_key(rng, key_num);
        let value = self.generate_value(rng);
        
        self.engine.put(&key, &value)?;
        
        self.collector.record_write_latency(start.elapsed());
        Ok("write")
    }
    
    async fn perform_read(
        &self,
        rng: &mut StdRng,
        total_keys: u64,
    ) -> Result<&'static str, Box<dyn std::error::Error>> {
        let start = Instant::now();
        
        let key_num = rng.gen_range(0..total_keys);
        let key = self.generate_key(rng, key_num);
        
        let _ = self.engine.get(&key)?;
        
        self.collector.record_read_latency(start.elapsed());
        Ok("read")
    }
    
    fn generate_key(&self, _rng: &mut StdRng, key_num: u64) -> Vec<u8> {
        format!("key_{:016}", key_num)
            .as_bytes()
            .iter()
            .take(self.config.key_size)
            .cloned()
            .collect()
    }
    
    fn generate_value(&self, rng: &mut StdRng) -> Vec<u8> {
        (0..self.config.value_size)
            .map(|_| rng.gen::<u8>())
            .collect()
    }
    
    fn calculate_write_amplification(&self) -> f64 {
        let stats = self.engine.get_statistics();
        
        if stats.bytes_written == 0 {
            return 1.0;
        }
        
        // Write amplification = Total bytes written to disk / Application bytes written
        let total_disk_writes = stats.bytes_written + stats.compaction_bytes_written;
        let waf = total_disk_writes as f64 / stats.bytes_written as f64;
        
        // Round to 2 decimal places
        (waf * 100.0).round() / 100.0
    }
}