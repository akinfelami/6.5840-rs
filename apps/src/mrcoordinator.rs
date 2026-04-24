use std::env;

#[tokio::main]
async fn main() {
    if std::env::args().len() < 3 {
        eprintln!("Usage: mrcoordinator sockname inputfiles...");
        std::process::exit(1);
    }

    let m = mr::coordinator::Coordinator::make_coordinator(
        env::args().nth(1).unwrap_or_else(|| "sockname".into()),
        env::args().skip(2).collect(),
        10,
    )
    .await
    .expect("could not start server");

    // The coordinatoe should notice if a worker hasn't completed its task in a reasonable
    // amount of time (10 seconds for this lab), and give the same task to a different worker.

    while m.done().await == false {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
}
