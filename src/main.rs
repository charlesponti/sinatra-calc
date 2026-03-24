use geo::run;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {}", err);
        std::process::exit(1);
    }
}
