//! The stacks the task hook (see `task_hook` in the crate root) hands to component-model tasks,
//! and the wit-bindgen task state each of them keeps beside its stack.
//!
//! Kept free of the wasm-only pieces (the main-program stack's address is a parameter) so the
//! bookkeeping runs under native tests.

use core::sync::atomic::AtomicBool;
use core::sync::atomic::AtomicPtr;
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering;

/// Bytes of a secondary task stack.
const SECONDARY_STACK_SIZE: usize = 512 * 1024;

/// Most secondary stacks live at once, so most tasks suspended in the middle of a callback at
/// once beyond the one on the main-program stack.
const SECONDARY_STACKS: usize = 16;

/// A heap-allocated stack and the wit-bindgen state of the task running on it.
struct SecondaryStack {
    /// The high end of the stack, which is also the stack pointer a task starts on it with;
    /// `0` for a free entry.
    top: AtomicUsize,
    state: AtomicPtr<u8>,
}

/// Every stack a task can be on: the statically allocated main-program stack, and up to
/// [`SECONDARY_STACKS`] heap-allocated ones.
///
/// Two tasks can hold a stack at once. A callback-ABI task releases its stack when its callback
/// returns, but a task can also be suspended in the middle of its callback: a blocking built-in
/// parks its thread, frames and all, and the engine runs other tasks in the meantime. A
/// synchronous export is admitted then too (the component model lets a synchronous function
/// ignore the instance's entry gate, and `wcmp` does), so it needs a stack of its own: the
/// first task keeps the main-program stack, and tasks that start while it is held get a
/// secondary one.
///
/// Each stack carries the task's wit-bindgen state (`start_task` stores its boxed `TaskState`
/// and every callback reads it back), found from the stack pointer of the code asking.
pub(crate) struct TaskStacks {
    main_in_use: AtomicBool,
    main_state: AtomicPtr<u8>,
    secondary: [SecondaryStack; SECONDARY_STACKS],
}

impl TaskStacks {
    pub(crate) const fn new() -> Self {
        Self {
            main_in_use: AtomicBool::new(false),
            main_state: AtomicPtr::new(core::ptr::null_mut()),
            secondary: [const {
                SecondaryStack {
                    top: AtomicUsize::new(0),
                    state: AtomicPtr::new(core::ptr::null_mut()),
                }
            }; SECONDARY_STACKS],
        }
    }

    /// A stack for a task that starts or resumes, as the stack pointer it starts with: the main
    /// stack (whose top is `main_top`) when it is free, else a new secondary one.
    pub(crate) fn alloc(&self, main_top: usize) -> usize {
        if self
            .main_in_use
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.main_state
                .store(core::ptr::null_mut(), Ordering::Relaxed);
            return main_top;
        }
        let layout =
            std::alloc::Layout::from_size_align(SECONDARY_STACK_SIZE, 16).expect("valid layout");
        let base = unsafe { std::alloc::alloc(layout) };
        assert!(!base.is_null(), "out of memory allocating a task stack");
        let top = base as usize + SECONDARY_STACK_SIZE;
        let stack = self
            .secondary
            .iter()
            .find(|stack| {
                stack
                    .top
                    .compare_exchange(0, top, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            })
            .expect("more tasks suspended mid-callback than there are secondary task stacks");
        stack.state.store(core::ptr::null_mut(), Ordering::Relaxed);
        top
    }

    /// Give back the stack a task finished or blocked on. `top` is the task's stack pointer at
    /// that moment, which its frames have unwound to the value [`alloc`](Self::alloc) handed
    /// it. Answers the task's wit-bindgen state.
    pub(crate) fn free(&self, main_top: usize, top: usize) -> *mut u8 {
        if top == main_top {
            let state = self
                .main_state
                .swap(core::ptr::null_mut(), Ordering::Relaxed);
            self.main_in_use.store(false, Ordering::Release);
            return state;
        }
        let stack = self
            .secondary
            .iter()
            .find(|stack| stack.top.load(Ordering::Relaxed) == top)
            .expect("unexpected task stack handed to __wasilibc_task_hook Finish");
        let state = stack.state.swap(core::ptr::null_mut(), Ordering::Relaxed);
        stack.top.store(0, Ordering::Release);
        unsafe {
            std::alloc::dealloc(
                (top - SECONDARY_STACK_SIZE) as *mut u8,
                std::alloc::Layout::from_size_align_unchecked(SECONDARY_STACK_SIZE, 16),
            );
        }
        state
    }

    /// The state cell of the stack `sp` points into: a secondary stack's, else the main
    /// stack's.
    pub(crate) fn state(&self, main_top: usize, sp: usize) -> &AtomicPtr<u8> {
        for stack in &self.secondary {
            let top = stack.top.load(Ordering::Relaxed);
            if top != 0 && sp > top - SECONDARY_STACK_SIZE && sp <= top {
                return &stack.state;
            }
        }
        assert!(
            sp <= main_top,
            "a wit-bindgen task state accessor ran off every task stack"
        );
        &self.main_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAIN_TOP: usize = 1 << 20;

    #[test]
    fn overlapping_tasks_get_stacks_that_do_not_alias() {
        let stacks = TaskStacks::new();
        let first = stacks.alloc(MAIN_TOP);
        assert_eq!(first, MAIN_TOP, "the first task runs on the main stack");
        let second = stacks.alloc(MAIN_TOP);
        let third = stacks.alloc(MAIN_TOP);
        assert_ne!(second, MAIN_TOP);
        assert!(second.abs_diff(third) >= SECONDARY_STACK_SIZE);
        stacks.free(MAIN_TOP, second);
        stacks.free(MAIN_TOP, third);
        stacks.free(MAIN_TOP, first);
        assert_eq!(
            stacks.alloc(MAIN_TOP),
            MAIN_TOP,
            "the main stack is reusable once released"
        );
    }

    #[test]
    fn each_task_keeps_its_own_state_and_hands_it_back() {
        let stacks = TaskStacks::new();
        let (mut one, mut two) = (1_u8, 2_u8);
        let first = stacks.alloc(MAIN_TOP);
        let second = stacks.alloc(MAIN_TOP);
        // The accessors find the stack from a stack pointer somewhere below its top.
        stacks
            .state(MAIN_TOP, first - 4096)
            .store(&mut one, Ordering::Relaxed);
        stacks
            .state(MAIN_TOP, second - 4096)
            .store(&mut two, Ordering::Relaxed);
        assert_eq!(
            stacks.state(MAIN_TOP, first - 64).load(Ordering::Relaxed),
            &mut one as *mut u8
        );
        assert_eq!(stacks.free(MAIN_TOP, second), &mut two as *mut u8);
        assert_eq!(stacks.free(MAIN_TOP, first), &mut one as *mut u8);
        let reused = stacks.alloc(MAIN_TOP);
        assert!(
            stacks
                .state(MAIN_TOP, reused - 8)
                .load(Ordering::Relaxed)
                .is_null(),
            "a task starts with no state"
        );
    }

    #[test]
    #[should_panic(expected = "unexpected task stack")]
    fn a_stack_the_hook_never_handed_out_is_refused() {
        let stacks = TaskStacks::new();
        stacks.alloc(MAIN_TOP);
        stacks.free(MAIN_TOP, 12345);
    }

    #[test]
    #[should_panic(expected = "ran off every task stack")]
    fn state_is_not_found_above_the_main_stack() {
        TaskStacks::new().state(MAIN_TOP, MAIN_TOP + 1);
    }
}
