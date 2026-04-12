use mrapps::wc;
use std::io::Write;

fn main() {
    let args = std::env::args().collect::<Vec<String>>();
    if std::env::args().len() < 2 {
        eprintln!("Usage: mrsequential app inputfiles...");
        std::process::exit(1);
    }

    let mode = args[1].as_str();

    let (mapf, redf) = match mode {
        "wc" => (wc::map, wc::reduce),
        _ => {
            eprintln!("Unknown app");
            std::process::exit(1);
        }
    };

    // read each input file,
    // pass it to Map,
    // accumalate the intermediate Map output
    let mut interm = Vec::new();
    for filename in args.iter().skip(2) {
        // Assuming the file is small enough to fit in memory, read it all at once
        let contents = std::fs::read_to_string(&filename).expect("cannot read file");
        let res = mapf(&filename, &contents);
        interm.extend(res);
    }

    // a big difference from real MapReduce is that all the
    // intermediate data is in one place
    // rather than partitioned into NxM buckets.

    interm.sort_by(|a, b| a.key.cmp(&b.key));

    let oname = "mr-out-0";
    let mut ofile = std::fs::File::create(oname).expect("cannot create output file");

    // call Reduce on each distinc kty in intermediate[]
    // and print the result to mr-out-0
    let mut i = 0;
    while i < interm.len() {
        let mut j = i + 1;
        while j < interm.len() && interm[j].key == interm[i].key {
            j += 1;
        }
        let mut values = Vec::new();
        for k in i..j {
            values.push(interm[k].value.clone());
        }
        let output = redf(&interm[i].key, &values);
        writeln!(ofile, "{} {}", interm[i].key, output).expect("cannot write to output file");
        i = j;
    }
}
