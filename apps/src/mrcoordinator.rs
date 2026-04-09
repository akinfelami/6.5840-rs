use std::env;

fn main() {
    if std::env::args().len() < 3 {
        eprintln!("Usage: mrcoordinator sockname inputfiles...");
        std::process::exit(1);
    }

    let m = mr::coordinator::Coordinator::new(
        env::args().nth(1).unwrap_or_else(|| "sockname".into()),
        env::args().skip(2).collect(),
        10,
    );

    while m.done() == false {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    std::thread::sleep(std::time::Duration::from_secs(1));
}
