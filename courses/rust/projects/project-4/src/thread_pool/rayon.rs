use super::ThreadPool;
use crate::error::KvsError;
use crate::error::Result;
use rayon::ThreadPoolBuilder;

pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .map_err(|e| KvsError::StringError(e.to_string()))?;
        Ok(Self(pool))
    }

    fn spawn<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.spawn(task);
    }
}
