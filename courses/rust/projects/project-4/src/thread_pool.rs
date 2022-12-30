use crate::error::Result;

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;
}

pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(Self {})
    }

    fn spawn<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        task()
    }
}

pub struct SharedQueueThreadPool {}

impl ThreadPool for SharedQueueThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(Self {})
    }

    fn spawn<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        task()
    }
}

pub struct RayonThreadPool {}

impl ThreadPool for RayonThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(Self {})
    }

    fn spawn<F>(&self, task: F) -> ()
    where
        F: FnOnce() + Send + 'static,
    {
        task()
    }
}
