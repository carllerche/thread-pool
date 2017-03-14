//! Execute tasks on one of possibly several pooled threads.
//!
//! A thread pool contains a set of previously spawned threads enabling running
//! tasks in parallel without having to spawn up a new thread for each task. The
//! thread pool supports a variety of different configuration options useful for
//! tweaking its exact behavior.
//!
//! Thread pools address two different porblems: they usually provide improved
//! performance when executing large numbers of asynchronous tasks, due to
//! reduced per-task invocation overhead, and they provide a means of bounding
//! and managing the resources, including threads, consumed when executing a
//! collection of tasks.
//!
//! To be useful across a wide range of contexts, `ThreadPool` provides a number
//! of adjustable parameters and extensibility hooks. However, programmers are
//! urged to use the more convenient builder methods,
//! [`fixed_size`](struct.ThreadPool.html#method.fixed_size), and
//! [`single_thread`](struct.ThreadPool.html#method.single_thread) (single
//! background thread), that preconfigure settings for the most common usage
//! scenarios. Otherwise, use the following guide when manually configuring and
//! tuning a `ThreadPool`.

#![deny(warnings, missing_docs, missing_debug_implementations)]

extern crate num_cpus;
extern crate two_lock_queue;

mod task;
mod state;
mod thread_pool;

pub use task::{Task, TaskBox};
pub use thread_pool::{Builder, Sender, ThreadPool};

pub use two_lock_queue::{
    SendError,
    TrySendError,
    SendTimeoutError,
};
