use crate::error::Result;

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;
}

mod naive;
pub use naive::NaiveThreadPool;

mod shared_queue;
pub use shared_queue::SharedQueueThreadPool;

mod rayon;
pub use rayon::RayonThreadPool;
