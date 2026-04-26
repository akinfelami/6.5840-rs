use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::rpc::{ExampleArgs, ExampleReply, TaskAssignment, TaskResponse};
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};
use tokio::sync::Mutex;

#[derive(Debug, PartialEq, Eq)]
enum TaskState {
    Idle,
    InProgress,
    Completed,
}

struct TaskMetadata {
    state: TaskState,
    assigned_at: Option<std::time::Instant>,
}

pub struct Coordinator {
    sockname: String,
    n_reduce: usize,
    filenames: Vec<String>,
    state: tokio::sync::Mutex<CoordinatorState>,
}

struct CoordinatorState {
    phase: Phase,
    // The key is the filename
    map_tasks: HashMap<String, TaskMetadata>,
    // [0-n_reduce)
    reduce_tasks: HashMap<usize, TaskMetadata>,
}

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    Map,
    Reduce,
    Done,
}

const TASK_TIMEOUT: Duration = Duration::from_secs(10);

impl Coordinator {
    fn new(sockname: String, files: Vec<String>, n_reduce: usize) -> Self {
        println!(
            "Coordinator initializing with\nsockname :{}\nfiles: {}\nand {} reduce tasks",
            sockname,
            files.join("\n"),
            n_reduce
        );
        let map_tasks: HashMap<String, TaskMetadata> = files
            .clone()
            .into_iter()
            .map(|f| {
                (
                    f,
                    TaskMetadata {
                        state: TaskState::Idle,
                        assigned_at: None,
                    },
                )
            })
            .collect();
        let reduce_tasks: HashMap<usize, TaskMetadata> = (0..n_reduce)
            .map(|i| {
                (
                    i,
                    TaskMetadata {
                        state: TaskState::Idle,
                        assigned_at: None,
                    },
                )
            })
            .collect();

        let coord_state = Mutex::new(CoordinatorState {
            phase: Phase::Map,
            map_tasks: map_tasks,
            reduce_tasks: reduce_tasks,
        });
        Coordinator {
            sockname,
            n_reduce,
            filenames: files,
            state: coord_state,
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

    // TODO: need to handle case where worker fails in the middle of a task, and the task needs to be re-assigned after some timeout
    pub async fn provide_work(&self) -> TaskAssignment {
        let mut slock = self.state.lock().await;

        match slock.phase {
            Phase::Done => {
                println!("Job is done, telling worker to exit");
                return TaskAssignment::Exit;
            }
            Phase::Map => {
                let all_done = slock
                    .map_tasks
                    .values()
                    .all(|meta| meta.state == TaskState::Completed);
                if all_done {
                    println!("All map tasks completed, moving to reduce phase");
                    slock.phase = Phase::Reduce;
                } else {
                    for (f, meta) in slock.map_tasks.iter_mut() {
                        if meta.state == TaskState::Idle {
                            println!("Assigning map task for file {}", f);
                            meta.state = TaskState::InProgress;
                            meta.assigned_at = Some(std::time::Instant::now());
                            return TaskAssignment::MapTask {
                                task_id: f.clone(),
                                filename: f.clone(),
                                n_reduce: self.n_reduce,
                            };
                        }
                    }
                }
            }
            Phase::Reduce => {
                let all_done = slock
                    .reduce_tasks
                    .values()
                    .all(|meta| meta.state == TaskState::Completed);
                if all_done {
                    println!("All reduce tasks completed, map reduce job done");
                    slock.phase = Phase::Done;
                } else {
                    for (i, meta) in slock.reduce_tasks.iter_mut() {
                        if meta.state == TaskState::Idle {
                            println!("Assigning reduce task {}", i);
                            meta.state = TaskState::InProgress;
                            meta.assigned_at = Some(std::time::Instant::now());
                            return TaskAssignment::ReduceTask {
                                task_id: i.to_string(),
                                filenames: self.filenames.clone(), // reducer needs to know all possible files
                                n_reduce: self.n_reduce,
                            };
                        }
                    }
                }
            }
        }
        TaskAssignment::Wait
    }

    pub async fn accept_task_report(&self, task_reponse: TaskResponse) -> bool {
        let mut slock = self.state.lock().await;
        match task_reponse.task_type {
            crate::rpc::TaskType::Map => {
                if let Some(meta) = slock.map_tasks.get_mut(&task_reponse.task_id) {
                    if task_reponse.success {
                        meta.state = TaskState::Completed;
                    } else {
                        println!(
                            "Worker reported failure for map task {}, marking it as idle for reassignment",
                            task_reponse.task_id
                        );
                        meta.state = TaskState::Idle;
                    }
                    meta.assigned_at = None;
                    true
                } else {
                    false
                }
            }
            crate::rpc::TaskType::Reduce => {
                if let Some(meta) = slock
                    .reduce_tasks
                    .get_mut(&task_reponse.task_id.parse::<usize>().unwrap())
                {
                    if task_reponse.success {
                        meta.state = TaskState::Completed;
                    } else {
                        println!(
                            "Worker reported failure for reduce task {}, marking it as idle for reassignment",
                            task_reponse.task_id
                        );
                        meta.state = TaskState::Idle;
                    }
                    meta.assigned_at = None;
                    true
                } else {
                    false
                }
            }
            crate::rpc::TaskType::Unknown => false,
        }
    }

    /// mrcoordinator.rs calls done() periodically to find out
    /// if the entire job has finished.
    pub async fn done(&self) -> bool {
        let slock = self.state.lock().await;
        slock.phase == Phase::Done
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
            "Coordinator.AskWork" => {
                let assignment = coordinator.provide_work().await;
                serde_json::to_value(&assignment).unwrap()
            }
            _ => {
                eprintln!("Unknown RPC method: {}", method);
                serde_json::Value::Null
            }
        };
        Json(result)
    }
}
