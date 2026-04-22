use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug, Deserialize)]
pub struct ExampleArgs {
    pub x: i32,
}
#[derive(Deserialize, Debug, Serialize)]
pub struct ExampleReply {
    pub y: i32,
}

#[derive(Serialize, Debug, Deserialize)]
pub enum TaskAssignment {
    MapTask {
        task_id: String,
        filename: String,
        n_reduce: usize,
    },
    ReduceTask {
        task_id: String,
        filenames: Vec<String>,
        n_reduce: usize,
    },
    Wait,
    Exit,
}

// my assumption: if worker is responding with Some(task_id), the task
// completed, otherwise should be re-tried
#[derive(Serialize, Debug, Deserialize)]
pub struct TaskResponse {
    pub task_id: String,
    pub task_type: TaskType,
    pub success: bool,
}

#[derive(Serialize, Debug, Deserialize)]
pub enum TaskType {
    Map,
    Reduce,
    Unknown,
}
