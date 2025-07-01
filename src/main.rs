mod benchmark;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    benchmark::compare_engines()
}