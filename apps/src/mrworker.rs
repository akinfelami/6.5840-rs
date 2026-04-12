use std::env;

use mr::worker::Worker;

#[tokio::main]
async fn main() {
    if std::env::args().len() < 2 {
        eprintln!("Usage: mrworker sockname");
        std::process::exit(1);
    }

    // TODO: using wc as placeholder -- should be plugin-based
    let w = Worker::new(env::args().nth(1).unwrap());

    w.worker(mrapps::wc::map, mrapps::wc::reduce);

    println!("trying rpc example...");
    mr::worker::call_example().await;
}
