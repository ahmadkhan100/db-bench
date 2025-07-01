use crate::metrics::BenchmarkResult;
use std::path::PathBuf;

pub fn analyze_results(
    input_path: PathBuf,
    format: String,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load results
    let content = std::fs::read_to_string(input_path)?;
    let results: Vec<BenchmarkResult> = serde_json::from_str(&content)?;
    
    match format.as_str() {
        "markdown" => print_markdown_report(&results),
        "json" => println!("{}", serde_json::to_string_pretty(&results)?),
        "csv" => print_csv_report(&results),
        _ => return Err("Unsupported format".into()),
    }
    
    Ok(())
}

fn print_markdown_report(results: &[BenchmarkResult]) {
    println!("# Database Benchmark Results\n");
    
    println!("## Summary\n");
    println!("| Metric | RocksDB (LSM-Tree) | Sled (B-Tree) | Winner |");
    println!("|--------|-------------------|---------------|---------|");
    
    let rocksdb = results.iter().find(|r| matches!(r.engine_type, crate::engine::EngineType::RocksDB));
    let sled = results.iter().find(|r| matches!(r.engine_type, crate::engine::EngineType::Sled));
    
    if let (Some(r), Some(s)) = (rocksdb, sled) {
        // Throughput
        println!(
            "| Throughput (ops/sec) | {:.0} | {:.0} | {} |",
            r.throughput,
            s.throughput,
            if r.throughput > s.throughput { "RocksDB" } else { "Sled" }
        );
        
        // Write latency
        println!(
            "| Write P99 Latency | {:.1}ms | {:.1}ms | {} |",
            r.latencies.write_p99,
            s.latencies.write_p99,
            if r.latencies.write_p99 < s.latencies.write_p99 { "RocksDB" } else { "Sled" }
        );
        
        // Read latency
        println!(
            "| Read P99 Latency | {:.1}ms | {:.1}ms | {} |",
            r.latencies.read_p99,
            s.latencies.read_p99,
            if r.latencies.read_p99 < s.latencies.read_p99 { "RocksDB" } else { "Sled" }
        );
        
        // Write amplification
        println!(
            "| Write Amplification | {:.1}x | {:.1}x | {} |",
            r.write_amplification,
            s.write_amplification,
            if r.write_amplification < s.write_amplification { "RocksDB" } else { "Sled" }
        );
    }
    
    println!("\n## Detailed Results\n");
    
    for result in results {
        println!("### {:?}\n", result.engine_type);
        println!("**Performance:**");
        println!("- Throughput: {:.0} ops/sec", result.throughput);
        println!("- Total operations: {}", result.operations);
        println!("- Duration: {:.1}s", result.duration.as_secs_f64());
        println!();
        println!("**Write Latencies:**");
        println!("- P50: {:.2}ms", result.latencies.write_p50);
        println!("- P95: {:.2}ms", result.latencies.write_p95);
        println!("- P99: {:.2}ms", result.latencies.write_p99);
        println!("- P99.9: {:.2}ms", result.latencies.write_p999);
        println!();
        println!("**Read Latencies:**");
        println!("- P50: {:.2}ms", result.latencies.read_p50);
        println!("- P95: {:.2}ms", result.latencies.read_p95);
        println!("- P99: {:.2}ms", result.latencies.read_p99);
        println!("- P99.9: {:.2}ms", result.latencies.read_p999);
        println!();
        println!("**Write Amplification:** {:.1}x", result.write_amplification);
        println!();
    }
}

fn print_csv_report(results: &[BenchmarkResult]) {
    println!("engine,throughput,write_p50,write_p95,write_p99,write_p999,read_p50,read_p95,read_p99,read_p999,write_amplification");
    
    for result in results {
        println!(
            "{:?},{:.0},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.1}",
            result.engine_type,
            result.throughput,
            result.latencies.write_p50,
            result.latencies.write_p95,
            result.latencies.write_p99,
            result.latencies.write_p999,
            result.latencies.read_p50,
            result.latencies.read_p95,
            result.latencies.read_p99,
            result.latencies.read_p999,
            result.write_amplification,
        );
    }
}