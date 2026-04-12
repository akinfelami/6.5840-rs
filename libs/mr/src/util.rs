use std::{fs, path::Path};

use base64::{Engine, engine::general_purpose::URL_SAFE};
use rand::RngExt;
use tokio::{fs::DirBuilder, process::Command, sync::mpsc, time};

pub fn start_worker(app: &str, i: i32, tx: Option<mpsc::Sender<i32>>, sock: &str) {
    let mut worker = Command::new("cargo")
        .args(["run", "--bin", "mrworker", app, sock])
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("failed to start worker");

    tokio::spawn(async move {
        worker.wait().await.unwrap();
        if let Some(tx) = tx {
            tx.send(i).await.unwrap();
        }
    });
}

pub async fn run_mr_chan(
    files: &[String],
    app: &str,
    n: i32,
    tx: Option<mpsc::Sender<i32>>,
    sock: &str,
) {
    let mut coordinator = Command::new("cargo")
        .args(["run", "--bin", "mrcoordinator", sock])
        .args(files)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("failed to start coordinator");

    // give coordinator time to create the sockets
    time::sleep(time::Duration::from_secs(1)).await;

    for i in 0..n {
        start_worker(app, i, tx.clone(), sock);
    }
    coordinator.wait().await.unwrap();

    if let Some(tx) = tx {
        tx.send(n).await.unwrap();
    }

    fs::remove_file(sock).unwrap();
}

async fn run_mr(files: &[String], app: &str, n: i32) {
    let sock = coordinator_sock();
    run_mr_chan(files, app, n, None, &sock).await
}

pub fn rand_string(n: usize) -> String {
    let mut b = vec![0u8; 2 * n];
    rand::rng().fill(&mut b);
    let s = URL_SAFE.encode(&b);
    s[..n].to_string()
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator
pub fn coordinator_sock() -> String {
    let n = 20;
    let mut s = "/tmp/5840-mr-".to_string();
    s += &rand_string(n);
    s
}

pub async fn mk_correct_output(files: &[String], app: &str, out: &str) {
    let mut cmd = Command::new("cargo")
        .args(["run", "--bin", "mrsequential", app])
        .args(files)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .current_dir("tmp")
        .spawn()
        .expect("failed to start mrsequential");

    let stat = cmd.wait().await.expect("mrsequential failed to run");
    if !stat.success() {
        panic!("mrsequential failed with status: {}", stat);
    }

    let out_path = Path::new("tmp").join(out);
    let output_file =
        std::fs::File::create(&out_path).unwrap_or_else(|_| panic!("create {:?} failed", out));

    let out_path = Path::new("tmp").join("mr-out-0");
    let mut cmd = Command::new("sort")
        .arg(&out_path)
        .stdout(output_file)
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("sort failed to start");

    let stat = cmd.wait().await.expect("sort failed to run");
    if !stat.success() {
        panic!("sort failed with status: {}", stat);
    }

    std::fs::remove_file(out_path).expect("unable to remove out file");
}

pub async fn merge_output(out: &str) {
    let files = find_files("tmp", "mr-out-[0-9]").await;
    if files.len() < 1 {
        panic!("reduce created no mr-out-X output files!");
    }
    let output_path = Path::new("tmp").join(out);
    let output_file =
        std::fs::File::create(&output_path).unwrap_or_else(|_| panic!("create {:?} failed", out));

    let mut cmd = Command::new("sort")
        .args(&files)
        .stdout(output_file)
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("sort failed to start");

    let stat = cmd.wait().await.expect("sort failed to run");
    if !stat.success() {
        panic!("sort failed with status: {}", stat);
    }
}

pub async fn find_files(dir: &str, s: &str) -> Vec<String> {
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

pub async fn find_files_pre(dir: &str, s: &str, pre: &str) -> Vec<String> {
    let files = find_files(dir, s).await;
    files
        .into_iter()
        .map(|f| Path::new("..").join(&f).to_string_lossy().to_string())
        .collect()
}

pub async fn mk_out() {
    let tmp = format!("mr-tmp-{}", rand_string(8));
    DirBuilder::new().mode(0o755).create(tmp).await.unwrap();
}

pub async fn cleanup() {
    let files = find_files("tmp", "mr-*").await;
    for f in files {
        fs::remove_file(f).unwrap();
    }
}

pub async fn run_cmp(f1: &str, f2: &str, msg: &str) {
    let mut cmp_cmd = Command::new("cmp")
        .args([f1, f2])
        .current_dir("tmp")
        .spawn()
        .expect("failed to start cmp");

    let stat = cmp_cmd.wait().await.expect("cmp failed to run");
    if !stat.success() {
        panic!("cmp failed with status: {}. {}", stat, msg);
    }
}

pub fn count_pattern_file(f: &str, p: &str) -> usize {
    let dat = fs::read_to_string(f).unwrap();
    dat.matches(p).count()
}

pub fn count_pattern(files: &[String], p: &str) -> usize {
    let mut n = 0;
    for f in files {
        n += count_pattern_file(f, p);
    }
    n
}
