use ::std::io::Write;
use std::{
    fs::File,
    hash::{Hash, Hasher},
    vec,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::rpc::{ExampleArgs, ExampleReply, TaskAssignment};

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub struct Worker {
    coord_sock_name: String,
}

fn ihash(key: &str) -> u32 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as u32) & 0x7fffffff
}

impl Worker {
    pub fn new(socketname: &str) -> Self {
        Worker {
            coord_sock_name: socketname.to_string(),
        }
    }

    pub async fn worker<M, R>(&self, mapf: M, reducef: R)
    where
        M: Fn(&str, &str) -> Vec<KeyValue>,
        R: Fn(&str, &[String]) -> String,
    {
        // Your worker implementation here.
        // periodally ask coordinnator for work.
        loop {
            let work = self.ask_coordinator_for_work().await;
            match work {
                TaskAssignment::Exit => {
                    println!("Worker: received exit signal, exiting");
                    break;
                }
                TaskAssignment::MapTask {
                    task_id,
                    filename,
                    n_reduce,
                } => {
                    println!(
                        "Worker: running map task {} on file {} with n_reduce {}",
                        task_id, filename, n_reduce
                    );
                    let mut interm = Vec::new();
                    let contents =
                        std::fs::read_to_string(&filename).expect("failed to read input file");
                    interm.extend(mapf(&filename, &contents));

                    let buckets: Vec<File> = (0..n_reduce)
                        .map(|i| std::fs::File::create(format!("mr-{}-{}", task_id, i)).unwrap())
                        .collect();

                    // TODO: note from lab -- use temp files and atomically rename incase worker crashes
                    for kv in interm {
                        let bk = (ihash(&kv.key) % n_reduce as u32) as usize;
                        serde_json::to_writer(&buckets[bk], &kv)
                            .expect("failed to write intermediate file");
                    }
                    self.report_task_to_coordinator(&task_id).await;
                }
                TaskAssignment::ReduceTask {
                    task_id,
                    filenames,
                    n_reduce,
                } => {
                    println!(
                        "Worker: running reduce task {} on files {:?} with n_reduce {}",
                        task_id, filenames, n_reduce
                    );

                    for filename in filenames {
                        let f =
                            std::fs::File::open(format!("mr-{}-{}", filename, task_id)).unwrap();
                        let stream =
                            serde_json::Deserializer::from_reader(f).into_iter::<KeyValue>();
                        let mut kvs = vec![];
                        for item in stream {
                            let kv = item.unwrap();
                            kvs.push(kv);
                        }
                        kvs.sort_by(|a, b| a.key.cmp(&b.key));
                        let mut ofile = std::fs::File::create(format!("mr-out-{}", task_id))
                            .expect("cannot create output file");

                        // call reduce
                        let mut i = 0;
                        while i < kvs.len() {
                            let mut j = i + 1;
                            while j < kvs.len() && kvs[j].key == kvs[i].key {
                                j += 1;
                            }
                            let mut values = Vec::new();
                            for k in i..j {
                                values.push(kvs[k].value.clone());
                            }
                            let output = reducef(&kvs[i].key, &values);
                            writeln!(ofile, "{} {}", kvs[i].key, output)
                                .expect("cannot write to output file");
                            i = j;
                        }
                    }
                    self.report_task_to_coordinator(&task_id).await;
                }

                TaskAssignment::Wait => {
                    println!("Worker: no work available, waiting");
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            };
        }
    }

    async fn ask_coordinator_for_work(&self) -> TaskAssignment {
        match self
            .call::<(), TaskAssignment>("Coordinator.AskWork", &())
            .await
        {
            Some(resp) => resp,
            None => {
                eprintln!("Worker: failed to ask coordinator for work");
                TaskAssignment::Exit
            }
        }
    }

    async fn report_task_to_coordinator(&self, task_id: &str) -> bool {
        match self
            .call::<String, bool>("Coordinator.ReportTask", &task_id.to_string())
            .await
        {
            Some(resp) => resp,
            None => {
                eprintln!("Worker: failed to report task to coordinator");
                false
            }
        }
    }

    pub async fn call<A: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        rpcname: &str,
        args: &A,
    ) -> Option<R> {
        let mut stream = match tokio::net::UnixStream::connect(&self.coord_sock_name).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Worker: failed to connect to coordinator: {}", e);
                return None;
            }
        };
        let body = serde_json::to_string(args)
            .context("serialize args")
            .unwrap();

        println!("Worker: sending request {}", body);

        let req = format!(
            "POST /rpc/{} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            rpcname,
            body.len(),
            body
        );
        if let Err(e) = stream.write_all(req.as_bytes()).await {
            eprintln!("Worker: failed to send request: {}", e);
            return None;
        }

        let mut resp = String::new();
        if let Err(e) = stream.read_to_string(&mut resp).await {
            eprintln!("Worker: failed to read response: {}", e);
            return None;
        }
        let resp_body = resp.split("\r\n\r\n").nth(1).unwrap_or("");
        println!("resp_body: {}", resp_body);
        match serde_json::from_str::<R>(resp_body) {
            Ok(r) => Some(r),
            Err(e) => {
                eprintln!("Worker: failed to parse response: {}", e);
                None
            }
        }
    }
}

pub async fn call_example(sockname: &str) {
    let args = ExampleArgs { x: 98 };
    let worker = Worker::new(sockname);

    match worker
        .call::<ExampleArgs, ExampleReply>("Coordinator.Example", &args)
        .await
    {
        Some(reply) => println!("Got reply: {:?}", reply),
        None => eprintln!("RPC call failed"),
    }
}
