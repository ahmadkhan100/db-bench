#!/bin/bash

echo "Building db-bench..."
cargo build --release

echo -e "\nRunning benchmark..."
./target/release/db-bench run --workload workload.yaml --output results.json

echo -e "\nGenerating report..."
./target/release/db-bench analyze results.json --format markdown > report.md

echo -e "\nBenchmark complete! See report.md for results."