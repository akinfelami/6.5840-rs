use std::{fs, path::Path};

use base64::{Engine, engine::general_purpose::URL_SAFE};
use rand::RngExt;
use tokio::{fs::DirBuilder, process::Command, sync::mpsc, time};

fn start_worker(app: &str, i: i32, tx: mpsc::Sender<i32>, sock: &str) {
    let mut worker = Command::new("cargo")
        .args(["run", "--bin", "mrworker", app, sock])
        .spawn()
        .expect("failed to start worker");

    tokio::spawn(async move {
        worker.wait().await.unwrap();
        tx.send(i).await.unwrap();
    });
}

async fn run_mr_chan(files: &[String], app: &str, n: i32, tx: mpsc::Sender<i32>, sock: &str) {
    let mut coordinator = Command::new("cargo")
        .args(["run", "--bin", "mrcoordinator", sock])
        .args(files)
        .spawn()
        .expect("failed to start coordinator");

    // give coordinator time to create the sockets
    time::sleep(time::Duration::from_secs(1)).await;

    for i in 0..n {
        start_worker(app, i, tx.clone(), sock);
    }
    coordinator.wait().await.unwrap();

    tx.send(n).await.unwrap();

    fs::remove_file(sock).unwrap();
}

fn rand_string(n: usize) -> String {
    let mut b = vec![0u8; 2 * n];
    rand::rng().fill(&mut b);
    let s = URL_SAFE.encode(&b);
    s[..n].to_string()
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator
fn coordinator_sock() -> String {
    let N = 20;
    let mut s = "/tmp/5840-mr-".to_string();
    s += &rand_string(N);
    s
}

async fn find_files(dir: &str, s: &str) -> Vec<String> {
    let cmd = Command::new("find")
        .args([dir, "-type", "f", "-name", s])
        .output()
        .await
        .expect("failed to run command find");

    let out = String::from_utf8_lossy(&cmd.stdout);
    let out = out.trim();
    if out.is_empty() {
        vec![]
    } else {
        out.split('\n').map(|s| s.to_string()).collect()
    }
}

async fn find_files_pre(dir: &str, s: &str, pre: &str) -> Vec<String> {
    let files = find_files(dir, s).await;
    files
        .into_iter()
        .map(|f| Path::new("..").join(&f).to_string_lossy().to_string())
        .collect()
}

async fn mk_out() {
    let tmp = format!("mr-tmp-{}", rand_string(8));
    DirBuilder::new().mode(0o755).create(tmp).await.unwrap();
}

async fn cleanup() {
    let files = find_files("tmp", "mr-*").await;
    for f in files {
        fs::remove_file(f).unwrap();
    }
}

// TODO: check this for stdout and stderr reading
fn run_cmp(f1: &str, f2: &str, msg: &str) {
    let cmp_cmd = Command::new("cmp")
        .args([f1, f2])
        .spawn()
        .expect("failed to start cmp");
}

fn count_pattern_file(f: &str, p: &str) -> usize {
    let dat = fs::read_to_string(f).unwrap();
    dat.matches(p).count()
}

fn count_pattern(files: &[String], p: &str) -> usize {
    let mut n = 0;
    for f in files {
        n += count_pattern_file(f, p);
    }
    n
}

async fn merge_output(tmp: &str, out: &str) {
    let files = find_files(tmp, "mr-out-[0-9]*").await;
    if files.is_empty() {
        panic!("reduce created no mr-out-X output files!");
    }

    let output_path = Path::new(tmp).join(out);
    let output_file =
        std::fs::File::create(&output_path).unwrap_or_else(|_| panic!("create {:?} failed", out));

    let mut cmd = Command::new("sort")
        .args(&files)
        .stdout(output_file)
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("sort failed to start");

    let status = cmd.wait().await.expect("failed to wait on sort");
    if !status.success() {
        panic!("sort failed with status {:?}", status.code());
    }
}
