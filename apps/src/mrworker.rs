use std::env;

fn main() {
    if std::env::args().len() < 2 {
        eprintln!("Usage: mrworker sockname");
        std::process::exit(1);
    }

    // TODO: using wc as placeholde -- should be plugin-based
    mr::worker::worker(
        env::args().nth(1).unwrap().as_str(),
        mrapps::wc::map,
        mrapps::wc::reduce,
    );
}
