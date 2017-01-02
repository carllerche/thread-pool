use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

pub struct AtomicState {
    atomic: AtomicUsize,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct State {
    state: usize,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Lifecycle {
    Running    = 0,
    Stop       = 1,
    Tidying    = 2,
    Terminated = 3,
}

const LIFECYCLE_BITS: usize = 3;
const LIFECYCLE_MASK: usize = 7;
pub const CAPACITY: usize = (1 << (32 - 3)) - 1;

// ===== impl AtomicState =====

impl AtomicState {
    pub fn new(lifecycle: Lifecycle) -> AtomicState {
        let i = State::of(lifecycle).as_usize();

        AtomicState {
            atomic: AtomicUsize::new(i as usize),
        }
    }

    pub fn load(&self) -> State {
        let num = self.atomic.load(SeqCst);
        State::load(num)
    }

    fn compare_and_swap(&self, expect: State, val: State) -> State {
        let actual = self.atomic.compare_and_swap(expect.as_usize(), val.as_usize(), SeqCst);
        State::load(actual)
    }

    pub fn compare_and_inc_worker_count(&self, expect: State) -> Result<State, State> {
        let expect_usize = expect.as_usize();
        let next_usize = expect_usize + (1 << LIFECYCLE_BITS);
        let actual_usize = self.atomic.compare_and_swap(expect_usize, next_usize, SeqCst);

        if actual_usize == expect_usize {
            Ok(expect)
        } else {
            Err(State::load(actual_usize))
        }
    }

    pub fn compare_and_dec_worker_count(&self, expect: State) -> bool {
        if expect.worker_count() == 0 {
            panic!("something went wrong");
        }

        let num = expect.as_usize();
        self.atomic.compare_and_swap(num, num - (1 << LIFECYCLE_BITS), SeqCst) == num
    }

    pub fn fetch_dec_worker_count(&self) -> State {
        let prev = self.atomic.fetch_sub(1 << LIFECYCLE_BITS, SeqCst);
        State::load(prev)
    }

    pub fn try_transition_to_stop(&self) -> bool {
        let mut state = self.load();

        loop {
            if state.lifecycle() >= Lifecycle::Stop {
                return false;
            }

            let next = state.with_lifecycle(Lifecycle::Stop);
            let actual = self.compare_and_swap(state, next);

            if state == actual {
                return true;
            }

            state = actual;
        }
    }

    /// Attempt to transition the state to `Tidying`
    ///
    /// If the state is currently before `Tidying`, then the transition will
    /// succeed. A successful transition grants the caller a lock to cleanup the
    /// pool state. Only the caller should transfer to `Terminated`.
    pub fn try_transition_to_tidying(&self) -> bool {
        let mut state = self.load();

        loop {
            if state.lifecycle() >= Lifecycle::Tidying {
                return false;
            }

            let next = state.with_lifecycle(Lifecycle::Tidying);
            let actual = self.compare_and_swap(state, next);

            if state == actual {
                return true;
            }

            state = actual;
        }
    }

    pub fn transition_to_terminated(&self) {
        let mut state = self.load();

        loop {
            if state.lifecycle() != Lifecycle::Tidying {
                panic!();
            }

            let next = state.with_lifecycle(Lifecycle::Terminated);
            let actual = self.compare_and_swap(state, next);

            if state == actual {
                return;
            }

            state = actual;
        }
    }
}

// ===== impl State =====

impl State {
    fn load(num: usize) -> State {
        State { state: num }
    }

    fn of(lifecycle: Lifecycle) -> State {
        State { state: lifecycle as usize }
    }

    pub fn lifecycle(&self) -> Lifecycle {
        Lifecycle::from_usize(self.state & LIFECYCLE_MASK)
    }

    fn with_lifecycle(&self, lifecycle: Lifecycle) -> State {
        let state = self.state & !LIFECYCLE_MASK | lifecycle as usize;
        State { state: state }
    }

    pub fn worker_count(&self) -> usize {
        self.state >> LIFECYCLE_BITS
    }

    pub fn is_terminated(&self) -> bool {
        self.lifecycle() == Lifecycle::Terminated
    }

    fn as_usize(&self) -> usize {
        self.state
    }
}

// ===== impl Lifecycle =====

impl Lifecycle {
    fn from_usize(val: usize) -> Lifecycle {
        use self::Lifecycle::*;

        match val {
            0 => Running,
            1 => Stop,
            2 => Tidying,
            3 => Terminated,
            _ => panic!("unexpected state value"),
        }
    }
}
