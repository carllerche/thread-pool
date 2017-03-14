use {Task, TaskBox};
use state::{AtomicState, Lifecycle, CAPACITY};
use two_lock_queue::{self as mpmc, SendError, SendTimeoutError, TrySendError, RecvTimeoutError};
use num_cpus;

use std::{fmt, thread, usize};
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

/// Execute tasks on one of possibly several pooled threads.
///
/// For more details, see the [library level documentation](./index.html).
pub struct ThreadPool<T> {
    inner: Arc<Inner<T>>,
}

/// Thread pool configuration.
///
/// Provide detailed control over the properties and behavior of the thread
/// pool.
#[derive(Debug)]
pub struct Builder {
    // Thread pool specific configuration values
    thread_pool: Config,

    // Max number of tasks that can be pending in the work queue
    work_queue_capacity: usize,
}

/// Thread pool specific configuration values
struct Config {
    core_pool_size: usize,
    max_pool_size: usize,
    keep_alive: Option<Duration>,
    allow_core_thread_timeout: bool,
    // Used to configure a worker thread
    name_prefix: Option<String>,
    stack_size: Option<usize>,
    after_start: Option<Arc<Fn() + Send + Sync>>,
    before_stop: Option<Arc<Fn() + Send + Sync>>,
}

/// A handle that allows dispatching work to a thread pool.
pub struct Sender<T> {
    tx: mpmc::Sender<T>,
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    // The main pool control state is an atomic integer packing two conceptual
    // fields
    //   worker_count: indicating the effective number of threads
    //   lifecycle:    indicating whether running, shutting down etc
    //
    // In order to pack them into one i32, we limit `worker_count` to (2^29)-1
    // (about 500 million) threads rather than (2^31)-1 (2 billion) otherwise
    // representable.
    //
    // The `worker_count` is the number of workers that have been permitted to
    // start and not permitted to stop. The value may be transiently different
    // from the actual number of live threads, for example when a thread
    // spawning fails to create a thread when asked, and when exiting threads
    // are still performing bookkeeping before terminating. The user-visible
    // pool size is reported as the current size of the workers set.
    //
    // The `lifecycle` provides the main lifecyle control, taking on values:
    //
    //   Running:    Accept new tasks and process queued tasks
    //   Shutdown:   Don't accept new tasks, but process queued tasks. This
    //               state is tracked by the work queue
    //   Stop:       Don't accept new tasks, don't process queued tasks, and
    //               interrupt in-progress tasks
    //   Tidying:    All tasks have terminated, worker_count is zero, the thread
    //               transitioning to state Tidying will run the terminated() hook
    //               method
    //   Terminated: terminated() has completed
    //
    // The numerical order among these values matters, to allow ordered
    // comparisons. The lifecycle monotonically increases over time, but need
    // not hit each state. The transitions are:
    //
    //   Running -> Shutdown
    //      On invocation of shutdown(), perhaps implicitly in finalize()
    //
    //   (Running or Shutdown) -> Stop
    //      On invocation of shutdown_now()
    //
    //   Shutdown -> Tidying
    //      When both queue and pool are empty
    //
    //   Stop -> Tidying
    //      When pool is empty
    //
    //   Tidying -> Terminated
    //      When the terminated() hook method has completed
    //
    // Threads waiting in await_termination() will return when the state reaches
    // Terminated.
    //
    // Detecting the transition from Shutdown to Tidying is less
    // straightforward than you'd like because the queue may become empty after
    // non-empty and vice versa during Shutdown state, but we can only
    // terminate if, after seeing that it is empty, we see that workerCount is
    // 0 (which sometimes entails a recheck -- see below).
    state: AtomicState,

    // Used to keep the work channel open even if there are no running threads.
    // This handle is cloned when spawning new workers
    rx: mpmc::Receiver<T>,

    // Acquired when waiting for the pool to shutdown
    termination_mutex: Mutex<()>,

    // Signaled when pool shutdown
    termination_signal: Condvar,

    // Used to name threads
    next_thread_id: AtomicUsize,

    // Configuration
    config: Config,
}

impl<T> Clone for ThreadPool<T> {
    fn clone(&self) -> Self {
        ThreadPool { inner: self.inner.clone() }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            tx: self.tx.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        const SOME: &'static &'static str = &"Some(_)";
        const NONE: &'static &'static str = &"None";

        fmt.debug_struct("ThreadPool")
           .field("core_pool_size", &self.core_pool_size)
           .field("core_pool_size", &self.core_pool_size)
           .field("max_pool_size", &self.max_pool_size)
           .field("keep_alive", &self.keep_alive)
           .field("allow_core_thread_timeout", &self.allow_core_thread_timeout)
           .field("name_prefix", &self.name_prefix)
           .field("stack_size", &self.stack_size)
           .field("after_start", if self.after_start.is_some() { SOME } else { NONE })
           .field("before_stop", if self.before_stop.is_some() { SOME } else { NONE })
           .finish()
    }
}

impl<T> fmt::Debug for ThreadPool<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ThreadPool").finish()
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Sender").finish()
    }
}

/// Tracks state associated with a worker thread
struct Worker<T> {
    // Work queue receive handle
    rx: mpmc::Receiver<T>,
    // Shared thread pool state
    inner: Arc<Inner<T>>,
}

// ===== impl Builder =====

impl Builder {
    /// Returns a builder with default values
    pub fn new() -> Builder {
        let num_cpus = num_cpus::get();

        Builder {
            thread_pool: Config {
                core_pool_size: num_cpus,
                max_pool_size: num_cpus,
                keep_alive: None,
                allow_core_thread_timeout: false,
                name_prefix: None,
                stack_size: None,
                after_start: None,
                before_stop: None,
            },
            work_queue_capacity: 64 * 1_024,
        }
    }

    /// Set the thread pool's core size.
    ///
    /// The number of threads to keep in the pool, even if they are idle.
    pub fn core_pool_size(mut self, val: usize) -> Self {
        self.thread_pool.core_pool_size = val;
        self
    }

    /// Set the thread pool's maximum size
    ///
    /// The maximum number of threads to allow in the pool.
    pub fn max_pool_size(mut self, val: usize) -> Self {
        self.thread_pool.max_pool_size = val;
        self
    }

    /// Set the thread keep alive duration
    ///
    /// When the number of threads is greater than core target or core threads
    /// are allowed to timeout, this is the maximum time that idle threads will
    /// wait for new tasks before terminating.
    pub fn keep_alive(mut self, val: Duration) -> Self {
        self.thread_pool.keep_alive = Some(val);
        self
    }

    /// Allow core threads to timeout
    pub fn allow_core_thread_timeout(mut self) -> Self {
        self.thread_pool.allow_core_thread_timeout = true;
        self
    }

    /// Maximum number of jobs that can be pending in the work queue
    pub fn work_queue_capacity(mut self, val: usize) -> Self {
        self.work_queue_capacity = val;
        self
    }

    /// Set name prefix of threads spawned by the pool
    ///
    /// Thread name prefix is used for generating thread names. For example, if
    /// prefix is `my-pool-`, then threads in the pool will get names like
    /// `my-pool-1` etc.
    pub fn name_prefix<S: Into<String>>(mut self, val: S) -> Self {
        self.thread_pool.name_prefix = Some(val.into());
        self
    }

    /// Set the stack size of threads spawned by the pool
    pub fn stack_size(mut self, val: usize) -> Self {
        self.thread_pool.stack_size = Some(val);
        self
    }

    /// Execute function `f` right after each thread is started but before
    /// running any tasks on it
    ///
    /// This is initially intended for bookkeeping and monitoring uses
    pub fn after_start<F>(mut self, f: F) -> Self
        where F: Fn() + Send + Sync + 'static
    {
        self.thread_pool.after_start = Some(Arc::new(f));
        self
    }

    /// Execute function `f` before each worker thread stops
    ///
    /// This is initially intended for bookkeeping and monitoring uses
    pub fn before_stop<F>(mut self, f: F) -> Self
        where F: Fn() + Send + Sync + 'static
    {
        self.thread_pool.before_stop = Some(Arc::new(f));
        self
    }

    /// Build and return the configured thread pool
    pub fn build<T: Task>(self) -> (Sender<T>, ThreadPool<T>) {
        assert!(self.thread_pool.core_pool_size >= 1, "at least one thread required");
        assert!(self.thread_pool.core_pool_size <= self.thread_pool.max_pool_size,
                "`core_pool_size` cannot be greater than `max_pool_size`");
        assert!(self.thread_pool.max_pool_size >= self.thread_pool.core_pool_size,
                "`max_pool_size` must be greater or equal to `core_pool_size`");


        // Create the work queue
        let (tx, rx) = mpmc::channel(self.work_queue_capacity);

        let inner = Arc::new(Inner {
            // Thread pool starts in the running state
            state: AtomicState::new(Lifecycle::Running),
            rx: rx,
            termination_mutex: Mutex::new(()),
            termination_signal: Condvar::new(),
            next_thread_id: AtomicUsize::new(1),
            config: self.thread_pool,
        });

        let sender = Sender {
            tx: tx,
            inner: inner.clone(),
        };

        let pool = ThreadPool {
            inner: inner,
        };

        (sender, pool)
    }
}

impl<T: Task> ThreadPool<T> {
    /// Create a thread pool that reuses a fixed number of threads operating off
    /// a shared unbounded queue.
    ///
    /// At any point, at most `size` threads will be active processing tasks. If
    /// additional tasks are submitted when all threads are active, they will
    /// wait in the queue until a thread is available. If any thread terminates
    /// due to a failure during execution prior to the thread pool shutting
    /// down, a new one will take its place if needed to execute subsequent
    /// tasks. The threads in the pool will exist until the thread pool is
    /// explicitly shutdown.
    pub fn fixed_size(size: usize) -> (Sender<T>, ThreadPool<T>) {
        Builder::new()
            .core_pool_size(size)
            .max_pool_size(size)
            .work_queue_capacity(usize::MAX)
            .build()
    }

    /// Create a thread pool with a single worker thread operating off an
    /// unbounded queue.
    ///
    /// Note, however, that if this single thread termintaes due to a failure
    /// during execution prior to the thread pool shutting down, a new one will
    /// take its place if needed to execute subsequent tasks. Tasks are
    /// guaranteed to execute sequentially, and no more than one task will be
    /// active at any given time.
    pub fn single_thread() -> (Sender<T>, ThreadPool<T>) {
        Builder::new()
            .core_pool_size(1)
            .max_pool_size(1)
            .work_queue_capacity(usize::MAX)
            .build()
    }

    /// Start a core thread, causing it to idly wait for work.
    ///
    /// This overrides the default policy of starting core threads only when new
    /// tasks are executed. This function will return `false` if all core
    /// threads have already been started.
    pub fn prestart_core_thread(&self) -> bool {
        let wc = self.inner.state.load().worker_count();

        if wc < self.inner.config.core_pool_size {
            self.inner.add_worker(None, &self.inner).is_ok()
        } else {
            false
        }
    }

    /// Start all core threads, causing them to idly wait for work.
    ///
    /// This overrides the default policy of starting core threads only when new
    /// tasks are executed.
    pub fn prestart_core_threads(&self) {
        while self.prestart_core_thread() {}
    }

    /// Initiate an orderly shutdown.
    ///
    /// Any previously submitted tasks are executed, but no new tasks will be
    /// accepted. Invocation has no additional effect if the thread pool has
    /// already been shut down.
    ///
    /// This function will not wait for previously submitted tasks to complete
    /// execution. Use `await_termination` to do that.
    pub fn shutdown(&self) {
        self.inner.rx.close();
    }

    /// Shutdown the thread pool as fast as possible.
    ///
    /// Worker threads will no longer receive tasks off of the work queue. This
    /// function will drain any remaining tasks before returning.
    ///
    /// There are no guarantees beyond best-effort attempts to actually shutdown
    /// in a timely fashion. Threads will finish processing tasks that are
    /// currently running.
    pub fn shutdown_now(&self) {
        self.inner.rx.close();

        // Try transitioning the state
        if self.inner.state.try_transition_to_stop() {
            loop {
                match self.inner.rx.recv() {
                    Err(_) => return,
                    Ok(_) => {}
                }
            }
        }
    }

    /// Returns `true` if the thread pool is in the process of terminating but
    /// has not yet terminated.
    pub fn is_terminating(&self) -> bool {
        !self.inner.rx.is_open() && !self.is_terminated()
    }

    /// Returns `true` if the thread pool is currently terminated.
    pub fn is_terminated(&self) -> bool {
        self.inner.state.load().is_terminated()
    }

    /// Blocks the current thread until the thread pool has terminated
    pub fn await_termination(&self) {
        let mut lock = self.inner.termination_mutex.lock().unwrap();

        while !self.inner.state.load().is_terminated() {
            lock = self.inner.termination_signal.wait(lock).unwrap();
        }
    }

    /// Returns the current number of running threads
    pub fn size(&self) -> usize {
        self.inner.state.load().worker_count()
    }

    /// Returns the current number of pending tasks
    pub fn queued(&self) -> usize {
        self.inner.rx.len()
    }
}

impl<T: Task> Sender<T> {
    /// Send a task to the thread pool, blocking if necessary
    ///
    /// The function may result in spawning additional threads depending on the
    /// current state and configuration of the thread pool.
    pub fn send(&self, task: T) -> Result<(), SendError<T>> {
        match self.try_send(task) {
            Ok(_) => Ok(()),
            Err(TrySendError::Disconnected(task)) => Err(SendError(task)),
            Err(TrySendError::Full(task)) => {
                // At capacity with all threads spawned, so just block
                self.tx.send(task)
            }
        }
    }

    /// Send a task to the thread pool, blocking if necessary for up to `duration`
    pub fn send_timeout(&self, task: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        match self.try_send(task) {
            Ok(_) => Ok(()),
            Err(TrySendError::Disconnected(task)) => {
                Err(SendTimeoutError::Disconnected(task))
            }
            Err(TrySendError::Full(task)) => {
                // At capacity with all threads spawned, so just block
                self.tx.send_timeout(task, timeout)
            }
        }
    }

    /// Send a task to the thread pool, returning immediately if at capacity.
    pub fn try_send(&self, task: T) -> Result<(), TrySendError<T>> {
        // Proceed in N steps

        match self.tx.try_send(task) {
            Ok(_) => {
                // Ensure that all the core threads are running
                let state = self.inner.state.load();

                if state.worker_count() < self.inner.config.core_pool_size {
                    let _ = self.inner.add_worker(None, &self.inner);
                }

                Ok(())
            }
            Err(TrySendError::Disconnected(task)) => {
                return Err(TrySendError::Disconnected(task));
            }
            Err(TrySendError::Full(task)) => {
                // Try to grow the pool size
                match self.inner.add_worker(Some(task), &self.inner) {
                    Ok(_) => return Ok(()),
                    Err(task) => return Err(TrySendError::Full(task.unwrap())),
                }
            }
        }
    }
}

impl Sender<Box<TaskBox>> {
    /// Send a fn to run on the thread pool, blocking if necessary
    ///
    /// The function may result in spawning additional threads depending on the
    /// current state and configuration of the thread pool.
    pub fn send_fn<F>(&self, task: F) -> Result<(), SendError<Box<TaskBox>>>
        where F: FnOnce() + Send + 'static
    {
        let task: Box<TaskBox> = Box::new(task);
        self.send(task)
    }

    /// Send a fn to run on the thread pool, blocking if necessary for up to
    /// `duration`
    pub fn send_fn_timeout<F>(&self, task: F, timeout: Duration)
        -> Result<(), SendTimeoutError<Box<TaskBox>>>
        where F: FnOnce() + Send + 'static
    {
        let task: Box<TaskBox> = Box::new(task);
        self.send_timeout(task, timeout)
    }

    /// Send a fn to run on the thread pool, returning immediately if at
    /// capacity.
    pub fn try_send_fn<F>(&self, task: F)
        -> Result<(), TrySendError<Box<TaskBox>>>
        where F: FnOnce() + Send + 'static
    {
        let task: Box<TaskBox> = Box::new(task);
        self.try_send(task)
    }
}

// ===== impl Inner =====

impl<T: Task> Inner<T> {
    fn add_worker(&self, task: Option<T>, arc: &Arc<Inner<T>>)
            -> Result<(), Option<T>> {

        let core = task.is_none();
        let mut state = self.state.load();

        'retry: loop {
            let lifecycle = state.lifecycle();

            if lifecycle >= Lifecycle::Stop {
                // If the lifecycle is greater than Lifecycle::Stop then never
                // create a add a new worker
                return Err(task);
            }

            loop {
                let wc = state.worker_count();

                // The number of threads that are expected to be running
                let target = if core {
                    self.config.core_pool_size
                } else {
                    self.config.max_pool_size
                };

                if wc >= CAPACITY || wc >= target {
                    return Err(task);
                }

                state = match self.state.compare_and_inc_worker_count(state) {
                    Ok(_) => break 'retry,
                    Err(state) => state,
                };

                if state.lifecycle() != lifecycle {
                    continue 'retry;
                }

                // CAS failed due to worker_count change; retry inner loop
            }
        }

        // == Spawn the thread ==

        let worker = Worker {
            rx: self.rx.clone(),
            inner: arc.clone(),
        };

        worker.spawn(task);

        Ok(())
    }

    fn finalize_thread_pool(&self) {
        // Transition to Terminated
        if self.state.try_transition_to_tidying() {
            self.state.transition_to_terminated();

            // Notify all pending threads
            self.termination_signal.notify_all();
        }
    }
}

// ===== impl Worker ====

impl<T: Task> Worker<T> {
    fn spawn(self, initial_task: Option<T>) {
        let mut b = thread::Builder::new();

        {
            let c = &self.inner.config;

            if let Some(stack_size) = c.stack_size {
                b = b.stack_size(stack_size);
            }

            if let Some(ref name_prefix) = c.name_prefix {
                let i = self.inner.next_thread_id.fetch_add(1, Relaxed);
                b = b.name(format!("{}{}", name_prefix, i));
            }
        }

        b.spawn(move || self.run(initial_task)).unwrap();
    }

    fn run(mut self, mut initial_task: Option<T>) {
        use std::panic::{self, AssertUnwindSafe};

        // Run the before hook
        self.inner.config.after_start.as_ref().map(|f| f());

        while let Some(task) = self.next_task(initial_task.take()) {
            // AssertUnwindSafe is used because `Task` is `Send + 'static`, which
            // is essentially unwind safe
            let _ = panic::catch_unwind(AssertUnwindSafe(move || task.run()));
        }
    }

    // Gets the next task, blocking if necessary. Returns None if the worker
    // should shutdown
    fn next_task(&mut self, mut task: Option<T>) -> Option<T> {
        // Load the state
        let state = self.inner.state.load();

        // Did the last `recv_task` call timeout?
        let mut timed_out = false;
        let allow_core_thread_timeout = self.inner.config.allow_core_thread_timeout;
        let core_pool_size = self.inner.config.core_pool_size;

        loop {
            if state.lifecycle() >= Lifecycle::Stop {
                // Run the after hook
                self.inner.config.before_stop.as_ref().map(|f| f());

                // No more tasks should be removed from the queue, exit the
                // worker
                self.decrement_worker_count();

                // Nothing else to do
                return None;
            }

            if task.is_some() {
                break;
            }

            let wc = state.worker_count();

            // Determine if there is a timeout for receiving the next task
            let timeout = if wc > core_pool_size || allow_core_thread_timeout {
                self.inner.config.keep_alive
            } else {
                None
            };

            if wc > self.inner.config.max_pool_size || (timeout.is_some() && timed_out) {
                // Only shutdown all threads if the work queue is empty
                if wc > 1 || self.rx.len() == 0 {
                    if self.inner.state.compare_and_dec_worker_count(state) {
                        // Run the after hook
                        self.inner.config.before_stop.as_ref().map(|f| f());

                        // This can never be a termination state since the
                        // lifecycle is not Stop or Terminate (checked above) and
                        // the queue has not been accessed, so it is unknown
                        // whether or not there is a pending task.
                        //
                        // This means that there is no need to call
                        // `finalize_worker`
                        return None;
                    }

                    // CAS failed, restart loop
                    continue;
                }
            }

            match self.recv_task(timeout) {
                Ok(t) => {
                    // Grab the task, but the loop will restart in order to
                    // check the state again. If the state transitioned to Stop
                    // while the worker was blocked on the queue, the task
                    // should be discarded and the worker shutdown.
                    task = Some(t);
                }
                Err(RecvTimeoutError::Disconnected) => {
                    // Run the after hook
                    self.inner.config.before_stop.as_ref().map(|f| f());

                    // No more tasks should be removed from the queue, exit the
                    // worker
                    self.decrement_worker_count();

                    // Nothing else to do
                    return None;
                }
                Err(RecvTimeoutError::Timeout) => {
                    timed_out = true;
                }
            }
        }

        task
    }

    fn recv_task(&self, timeout: Option<Duration>) -> Result<T, RecvTimeoutError> {
        match timeout {
            Some(timeout) => self.rx.recv_timeout(timeout),
            None => self.rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
        }
    }

    fn decrement_worker_count(&self) {
        let state = self.inner.state.fetch_dec_worker_count();

        if state.worker_count() == 1 && !self.rx.is_open() {
            self.inner.finalize_thread_pool();
        }
    }
}
