use super::ThreadPool;
use crate::error::Result;
use std::thread;

pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        Ok(Self {})
    }

    fn spawn<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(task);
    }
}
