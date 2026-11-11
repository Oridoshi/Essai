use std::panic::{self, AssertUnwindSafe};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let workers = (0..size)
            .map(|id| {
                let receiver = Arc::clone(&receiver);
                let thread = thread::Builder::new()
                    .name(format!("pool-worker-{}", id))
                    .spawn(move || loop {
                        let job = receiver.lock().unwrap().recv();

                        match job {
                            Ok(job) => {
                                if let Err(e) = panic::catch_unwind(AssertUnwindSafe(job)) {
                                    eprintln!("Worker {} task panicked: {:?}", id, e);
                                }
                            }
                            Err(_) => break,
                        }
                    })
                    .expect("Failed to spawn worker");

                Worker {
                    id,
                    thread: Some(thread),
                }
            })
            .collect();

        ThreadPool {
            workers,
            sender: Some(sender),
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .as_ref()
            .expect("ThreadPool has been shut down")
            .send(Box::new(f))
            .expect("Failed to send job to worker");
    }

    /// Get the number of workers in the pool
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // Close the channel
        drop(self.sender.take());

        // Join all worker threads
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_basic_execution() {
        let pool = ThreadPool::new(4);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..100 {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            });
        }

        drop(pool); // Wait for completion
        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }

    #[test]
    fn test_panic_recovery() {
        let pool = ThreadPool::new(2);
        let counter = Arc::new(AtomicUsize::new(0));

        // This task will panic
        pool.execute(|| {
            panic!("Intentional panic");
        });

        // These should still run
        for _ in 0..10 {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            });
        }

        drop(pool);
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }
}