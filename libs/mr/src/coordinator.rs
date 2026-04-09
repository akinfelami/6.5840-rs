use crate::rpc::{ExampleArgs, ExampleReply};

pub struct Coordinator {
    sockname: String,
    files: Vec<String>,
    n_reduce: usize,
}

impl Coordinator {
    pub fn new(sockname: String, files: Vec<String>, n_reduce: usize) -> Self {
        Coordinator {
            sockname,
            files,
            n_reduce,
        }
    }

    pub fn example(args: &ExampleArgs, reply: &mut ExampleReply) -> anyhow::Result<()> {
        reply.y = args.x + 1;
        Ok(())
    }

    /// mrcoordinator.rs calls done() periodically to find out
    /// if the entire job has finished.
    pub fn done(&self) -> bool {
        self.files.is_empty()
    }

    /// start a thread that listens for RPCs from worker.rs
    pub fn start_server(&self) {}
}
