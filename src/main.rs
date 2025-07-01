use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod engine;
mod workload;
mod metrics;
mod analyzer;

use engine::EngineType;
use workload::{Workload, WorkloadConfig};
use metrics::MetricsCollector;

#[derive(Parser)]
#[command(name = "db-bench")]
#[command(about = "Fast database storage engine benchmarking", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run benchmarks
    Run {
        /// Workload configuration file
        #[arg(short, long)]
        workload: PathBuf,
        
        /// Output results file
        #[arg(short, long)]
        output: PathBuf,
        
        /// Engine to benchmark (rocksdb, sled, both)
        #[arg(short, long, default_value = "both")]
        engine: String,
    },
    
    /// Analyze benchmark results
    Analyze {
        /// Results file to analyze
        input: PathBuf,
        
        /// Output format (json, markdown, csv)
        #[arg(short, long, default_value = "markdown")]
        format: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Run { workload, output, engine } => {
            run_benchmark(workload, output, engine).await?;
        }
        Commands::Analyze { input, format } => {
            analyzer::analyze_results(input, format)?;
        }
    }
    
    Ok(())
}

async fn run_benchmark(
    workload_path: PathBuf,
    output_path: PathBuf,
    engine_type: String,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load workload config
    let config = WorkloadConfig::from_file(&workload_path)?;
    println!("Loaded workload: {:?}", config);
    
    // Determine which engines to run
    let engines = match engine_type.as_str() {
        "rocksdb" => vec![EngineType::RocksDB],
        "sled" => vec![EngineType::Sled],
        "both" => vec![EngineType::RocksDB, EngineType::Sled],
        _ => return Err("Invalid engine type".into()),
    };
    
    let mut results = Vec::new();
    
    for engine_type in engines {
        println!("\nBenchmarking {:?}...", engine_type);
        
        // Create temp directory for this engine
        let temp_dir = tempfile::tempdir()?;
        let engine = engine::create_engine(engine_type, temp_dir.path())?;
        
        // Create metrics collector
        let mut collector = MetricsCollector::new();
        
        // Run workload
        let workload = Workload::new(config.clone(), engine, &mut collector);
        let result = workload.run().await?;
        
        results.push(result);
        
        // Cleanup
        drop(temp_dir);
    }
    
    // Save results
    let results_json = serde_json::to_string_pretty(&results)?;
    std::fs::write(output_path, results_json)?;
    
    println!("\nBenchmark complete! Results saved.");
    
    Ok(())
}