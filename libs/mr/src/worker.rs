use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::rpc::{ExampleArgs, ExampleReply};

#[derive(Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub struct Worker {
    coord_sock_name: String,
}

impl Worker {
    pub fn new(socketname: String) -> Self {
        Worker {
            coord_sock_name: socketname,
        }
    }

    pub fn worker<M, R>(&self, map: M, reduce: R)
    where
        M: Fn(&str, &str) -> Vec<KeyValue>,
        R: Fn(&str, &[String]) -> String,
    {
        // Your worker implementation here.
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

pub async fn call_example() {
    let args = ExampleArgs { x: 98 };
    let worker = Worker::new("/tmp/mr-coordinator.sock".to_string());

    match worker
        .call::<ExampleArgs, ExampleReply>("Coordinator.Example", &args)
        .await
    {
        Some(reply) => println!("Got reply: {:?}", reply),
        None => eprintln!("RPC call failed"),
    }
}
