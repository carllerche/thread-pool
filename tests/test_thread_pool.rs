extern crate thread_pool;

use thread_pool::*;

use std::thread;
use std::time::Duration;
use std::sync::mpsc;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

struct Boop;

impl Task for Boop {
    fn run(self) {
        println!("boop");
    }
}

#[test]
fn type_bounds() {
    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    is_send::<ThreadPool<Boop>>();
    is_sync::<ThreadPool<Boop>>();
}

#[test]
fn one_thread_basic() {
    let (sender, _) = ThreadPool::fixed_size(1);
    let (tx, rx) = mpsc::sync_channel(0);

    sender.send(move || {
        tx.send("hi").unwrap();
    }).unwrap();

    assert_eq!("hi", rx.recv().unwrap());
}

#[test]
fn clone() {
    let (sender, _) = ThreadPool::fixed_size(1);
    let (tx, rx) = mpsc::sync_channel(0);

    sender.clone().send(move || {
        tx.send("hi").unwrap();
    }).unwrap();

    assert_eq!("hi", rx.recv().unwrap());
}

#[test]
fn debug() {
    format!("{:?}", ThreadPool::<Box<TaskBox>>::fixed_size(1));
}

#[test]
fn two_thread_basic() {
    let (sender, _) = ThreadPool::fixed_size(2);
    let (tx, rx) = mpsc::sync_channel(0);

    for _ in 0..2 {
        let tx = tx.clone();
        sender.send(move || {
            tx.send("hi").unwrap();
            thread::sleep(Duration::from_millis(500));

            tx.send("bye").unwrap();
            thread::sleep(Duration::from_millis(500));
        }).unwrap();
    }

    for &msg in ["hi", "hi", "bye", "bye"].iter() {
        assert_eq!(msg, rx.recv().unwrap());
    }
}

#[test]
fn num_cpus_plus_1() {
    extern crate num_cpus;
    ThreadPool::<Box<TaskBox>>::fixed_size(num_cpus::get() + 1);
}

#[test]
fn two_threads_task_queue_up() {
    let (sender, _) = ThreadPool::fixed_size(2);
    let (tx, rx) = mpsc::sync_channel(0);

    for _ in 0..4{
        let tx = tx.clone();

        sender.send(move || {
            tx.send("hi").unwrap();
            thread::sleep(Duration::from_millis(500));

            tx.send("bye").unwrap();
            thread::sleep(Duration::from_millis(500));
        }).unwrap();
    }

    for &msg in ["hi", "hi", "bye", "bye", "hi", "hi", "bye", "bye"].iter() {
        assert_eq!(msg, rx.recv().unwrap());
    }
}

#[test]
fn thread_pool_shutdown_by_dropping_sender() {
    let (sender, pool) = ThreadPool::fixed_size(1);
    let cnt = Arc::new(AtomicUsize::new(0));

    for _ in 0..20 {
        let cnt = cnt.clone();
        sender.send(move || {
            cnt.fetch_add(1, SeqCst);
        }).unwrap();
    }

    // Drop the pool, cleanly shutdown
    drop(sender);

    assert!(pool.is_terminating() || pool.is_terminated());

    // Wait for termination
    pool.await_termination();

    assert_eq!(20, cnt.load(SeqCst));
    assert!(pool.is_terminated());
}

#[test]
fn thread_pool_shutdown_by_calling_fn() {
    let (sender, pool) = ThreadPool::fixed_size(1);
    let cnt = Arc::new(AtomicUsize::new(0));

    for _ in 0..20 {
        let cnt = cnt.clone();
        sender.send(move || {
            cnt.fetch_add(1, SeqCst);
        }).unwrap();
    }

    // Drop the pool, cleanly shutdown
    pool.shutdown();

    assert!(pool.is_terminating() || pool.is_terminated());

    // Wait for termination
    pool.await_termination();

    assert_eq!(20, cnt.load(SeqCst));
    assert!(pool.is_terminated());
}

#[test]
fn thread_pool_shutdown_now() {
    let (sender, pool) = ThreadPool::fixed_size(1);
    let cnt = Arc::new(AtomicUsize::new(0));

    for _ in 0..20 {
        let cnt = cnt.clone();
        sender.send(move || {
            cnt.fetch_add(1, SeqCst);
            thread::sleep(Duration::from_millis(300));
        }).unwrap();
    }

    thread::sleep(Duration::from_millis(50));

    // Drop the pool, cleanly shutdown
    pool.shutdown_now();

    assert!(pool.is_terminating() || pool.is_terminated());

    // Wait for termination
    pool.await_termination();

    assert_eq!(1, cnt.load(SeqCst));
    assert!(pool.is_terminated());
}

#[test]
fn grow_pool() {
    let (sender, pool) = Builder::new()
        .core_pool_size(1)
        .max_pool_size(3)
        .work_queue_capacity(1)
        .build();

    // Used as a latch
    let (tx, rx) = mpsc::channel();

    // Core threads aren't pre-started
    assert_eq!(0, pool.size());

    // Spawn first thread
    sender.try_send_fn(move || {
        tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(500));
    }).unwrap();

    assert_eq!(1, pool.size());

    // Wait for the task to have started
    rx.recv().unwrap();

    for _ in 0..3 {
        sender.try_send_fn(|| {
            thread::sleep(Duration::from_millis(500));
        }).unwrap();
    }

    assert_eq!(3, pool.size());

    let res = sender.try_send_fn(|| {
        println!("Hello");
    });

    assert!(res.is_err());
}

#[test]
fn shrink_stretch_pool() {
    let (sender, pool) = Builder::new()
        .core_pool_size(1)
        .max_pool_size(2)
        .keep_alive(Duration::from_millis(50))
        .work_queue_capacity(1)
        .build();

    // Spawn tasks until the thread pool is full
    loop {
        let res = sender.try_send_fn(|| {
            thread::sleep(Duration::from_millis(50));
        });

        if res.is_err() {
            break;
        }
    }

    assert_eq!(2, pool.size());

    // Wait for the thread to shutdown
    thread::sleep(Duration::from_millis(200));

    assert_eq!(1, pool.size());
}

#[test]
fn shrink_core_pool() {
    let (sender, pool) = Builder::new()
        .core_pool_size(1)
        .max_pool_size(1)
        .keep_alive(Duration::from_millis(50))
        .allow_core_thread_timeout()
        .work_queue_capacity(1)
        .build();

    // Latch
    let (tx, rx) = mpsc::channel();

    // Spawn tasks until the thread pool is full
    loop {
        let tx = tx.clone();
        let res = sender.try_send_fn(move || {
            tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(50));
        });

        if res.is_err() {
            break;
        }
    }

    rx.recv().unwrap();

    assert_eq!(1, pool.size());

    // Wait for the thread to shutdown
    thread::sleep(Duration::from_millis(400));

    assert_eq!(0, pool.size());
}

#[test]
fn panic_in_task() {
    let (sender, pool) = ThreadPool::single_thread();
    let (tx, rx) = mpsc::channel();

    {
        let tx = tx.clone();
        sender.send_fn(move || {
            tx.send(1).unwrap();
            panic!();
        }).unwrap();
    }

    assert_eq!(1, rx.recv().unwrap());
    assert_eq!(1, pool.size());

    sender.send_fn(move || {
        tx.send(2).unwrap();
    }).unwrap();

    assert_eq!(2, rx.recv().unwrap());
}

#[test]
fn lifecycle_test() {
    use std::sync::atomic::ATOMIC_USIZE_INIT;

    static NUM_STARTS: AtomicUsize = ATOMIC_USIZE_INIT;
    static NUM_STOPS: AtomicUsize = ATOMIC_USIZE_INIT;

    fn after_start() {
        NUM_STARTS.fetch_add(1, SeqCst);
    }

    fn before_stop() {
        NUM_STOPS.fetch_add(1, SeqCst);
    }

    let (sender, pool) = Builder::new()
        .core_pool_size(4)
        .after_start(after_start)
        .before_stop(before_stop)
        .build();

    pool.prestart_core_threads();

    // Wait a bit
    thread::sleep(Duration::from_millis(500));

    sender.send_fn(|| {
        println!("boop");
    }).unwrap();

    pool.shutdown();
    pool.await_termination();

    assert_eq!(NUM_STARTS.load(SeqCst), 4);
    assert_eq!(NUM_STOPS.load(SeqCst), 4);
}
