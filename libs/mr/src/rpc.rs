use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug, Deserialize)]
pub struct ExampleArgs {
    pub x: i32,
}
#[derive(Deserialize, Debug, Serialize)]
pub struct ExampleReply {
    pub y: i32,
}
