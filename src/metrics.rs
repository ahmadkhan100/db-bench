use hdrhistogram::Histogram;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone)]
pub struct MetricsCollector {
    write_latencies: Arc<Mutex<Histogram<u64>>>,
    read_latencies: Arc<Mutex<Histogram<u64>>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            write_latencies: Arc::new(Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).unwrap()
            )),
            read_latencies: Arc::new(Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).unwrap()
            )),
        }
    }
    
    pub fn record_write_latency(&self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        self.write_latencies.lock().unwrap().record(micros).unwrap();
    }
    
    pub fn record_read_latency(&self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        self.read_latencies.lock().unwrap().record(micros).unwrap();
    }
    
    pub fn get_latencies(&self) -> LatencyStats {
        let write_hist = self.write_latencies.lock().unwrap();
        let read_hist = self.read_latencies.lock().unwrap();
        
        LatencyStats {
            write_p50: write_hist.value_at_percentile(50.0) as f64 / 1000.0,
            write_p95: write_hist.value_at_percentile(95.0) as f64 / 1000.0,
            write_p99: write_hist.value_at_percentile(99.0) as f64 / 1000.0,
            write_p999: write_hist.value_at_percentile(99.9) as f64 / 1000.0,
            read_p50: read_hist.value_at_percentile(50.0) as f64 / 1000.0,
            read_p95: read_hist.value_at_percentile(95.0) as f64 / 1000.0,
            read_p99: read_hist.value_at_percentile(99.0) as f64 / 1000.0,
            read_p999: read_hist.value_at_percentile(99.9) as f64 / 1000.0,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LatencyStats {
    pub write_p50: f64,   // milliseconds
    pub write_p95: f64,
    pub write_p99: f64,
    pub write_p999: f64,
    pub read_p50: f64,
    pub read_p95: f64,
    pub read_p99: f64,
    pub read_p999: f64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct BenchmarkResult {
    pub engine_type: crate::engine::EngineType,
    pub throughput: f64,  // ops/sec
    pub operations: u64,
    pub duration: Duration,
    pub latencies: LatencyStats,
    pub write_amplification: f64,
}