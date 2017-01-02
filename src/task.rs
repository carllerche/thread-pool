/// A task that runs on a thread pool
///
/// A `ThreadPool` instance is pinned to run only a single type of task,
/// represented by implementations of `Task`. However, it is sometimes useful to
/// be able to schedule any arbitrary work to run on a thread pool. This can be
/// done by using `Box<TaskBox>` as the task type.
pub trait Task: Send + 'static {
    /// Run the task
    fn run(self);
}

/// A version of `Task` intended to use as a trait object
pub trait TaskBox: Send + 'static {
    /// Run the task
    fn run_box(self: Box<Self>);
}

impl<F> Task for F
    where F: FnOnce() + Send + 'static,
{
    fn run(self) {
        (self)()
    }
}

impl<T: Sized + Task> TaskBox for T {
    fn run_box(self: Box<Self>) {
        (*self).run()
    }
}

impl Task for Box<TaskBox> {
    fn run(self) {
        self.run_box()
    }
}
