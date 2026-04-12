use std::sync::Arc;

use crate::rpc::{ExampleArgs, ExampleReply};
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};

pub struct Coordinator {
    sockname: String,
    files: Vec<String>,
    n_reduce: usize,
}

impl Coordinator {
    fn new(sockname: String, files: Vec<String>, n_reduce: usize) -> Self {
        Coordinator {
            sockname,
            files,
            n_reduce,
        }
    }

    pub async fn make_coordinator(
        sockname: String,
        files: Vec<String>,
        n_reduce: usize,
    ) -> anyhow::Result<Arc<Self>> {
        let c = Arc::new(Self::new(sockname, files, n_reduce));
        // TODO: spawn this as tokio thread
        let c_clone = Arc::clone(&c);
        tokio::spawn(async move {
            if let Err(e) = c_clone.start_server().await {
                eprintln!("Coordinator server error: {}", e);
            }
        });
        Ok(c)
    }

    pub fn example(&self, args: &ExampleArgs, reply: &mut ExampleReply) -> anyhow::Result<()> {
        reply.y = args.x + 1;
        Ok(())
    }

    /// mrcoordinator.rs calls done() periodically to find out
    /// if the entire job has finished.
    pub fn done(&self) -> bool {
        // Your code here.
        // TODO: implement this
        false
    }

    /// start a thread that listens for RPCs from worker.rs
    async fn start_server(self: &Arc<Self>) -> anyhow::Result<()> {
        let _ = std::fs::remove_file(&self.sockname);
        let listener = tokio::net::UnixListener::bind(&self.sockname)?;

        let app = Router::new()
            .route("/rpc/{method}", post(Coordinator::handle_rpc))
            .with_state(self.clone());

        println!("bound to {} listening for requests", &self.sockname);

        axum::serve(listener, app).await?;
        Ok(())
    }

    async fn handle_rpc(
        State(coordinator): State<Arc<Coordinator>>,
        Path(method): Path<String>,
        Json(payload): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        println!("Received RPC: {} with payload: {}", method, payload);
        let result = match method.as_str() {
            "Coordinator.Example" => {
                let req = serde_json::from_value::<ExampleArgs>(payload).unwrap();
                let mut reply = ExampleReply { y: 0 };
                coordinator.example(&req, &mut reply).unwrap();
                serde_json::to_value(&reply).unwrap()
            }
            _ => {
                eprintln!("Unknown RPC method: {}", method);
                serde_json::Value::Null
            }
        };
        Json(result)
    }
}
