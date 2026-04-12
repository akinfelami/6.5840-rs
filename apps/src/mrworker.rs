use std::env;

use mr::worker::Worker;

#[tokio::main]
async fn main() {
    if std::env::args().len() < 3 {
        eprintln!("Usage: mrworker app sockname");
        std::process::exit(1);
    }

    // Each worker will process, in a loop, ask the coordinator for a task,
    // read the task's input from one or more files, execute the task
    // write the task's output to one or more files, and again ask the coordinator
    // for a new task.

    // TODO: using wc as placeholder -- should be plugin-based
    let w = Worker::new(env::args().nth(1).unwrap());

    // w.worker(mrapps::wc::map, mrapps::wc::reduce);
    match env::args().nth(1).unwrap().as_str() {
        "wc" => w.worker(mrapps::wc::map, mrapps::wc::reduce),
        _ => {
            eprintln!("Unknown app: {}", env::args().nth(1).unwrap());
            std::process::exit(1);
        }
    }

    println!("trying rpc example...");
    mr::worker::call_example().await;
}
