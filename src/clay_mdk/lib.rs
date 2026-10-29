//! The Micro Development Kit (ADR 015 §5): the single authoring API every micro uses.
//! `manifest` and `ComponentRef` are neutral vocabulary every backend compiles against;
//! `bindings` carries the WIT-generated surface; `mirror` is the typed document-mirror
//! authoring surface over it (typed decoding, availability/freshness evidence, named config
//! bindings); `native` (feature `in_process`) is the in-process backend (authoring context, collections, provider
//! assembly), renderer-agnostic; `view` is its Dioxus half (the slot component and collection
//! hook a host-linked view needs); `wasm` assembles Wasmtime-backed providers for a composition
//! root, and `wcmp` (feature `wcmp`) is the browser-engine twin of the same provider shape
//! (ADR 018); `guest` (feature `dioxus`) is the guest backend — the same `activate(&Context)` shape
//! over the WIT imports, plus the Dioxus VirtualDom adapter and the `export_micro!` entry
//! point. The MDK adds ergonomics, never authority: capabilities are still the host-issued
//! resources.
#![cfg_attr(
    all(target_os = "wasi", target_env = "p3"),
    feature(asm_experimental_arch)
)]
pub use clay_collection::Batch;
pub use clay_collection::Query;
pub use clay_collection::RowChange;
pub use clay_collection::View;
pub use clay_collection::{self as collection};

/// wasi-libc v34's `__wasilibc_task_hook` calls `__pthread_tsd_run_dtors` on every
/// ReallocStart — i.e. on each `cabi_realloc` bracketing a string-returning import
/// lowering, inside the enclosing export. Rust's `#[thread_local]` EagerStorage
/// destructors therefore run mid-`activate`, destroy `RUNTIMES`/`RUNTIME`, and the
/// next access panics with `AccessError`. libc exports the symbol weak; a strong
/// definition here overrides it so TSD dtors never fire on realloc (matching
/// post-#884 wasi-libc, which shares one TLS block for the program lifetime and
/// never runs dtors from the realloc hook).
#[cfg(all(target_os = "wasi", target_env = "p3"))]
#[unsafe(no_mangle)]
pub extern "C" fn __pthread_tsd_run_dtors() {}

/// Component-model task lifecycle hook codes synthesized by `wit-component`
/// when componentizing a core module. See `wit-component`'s `TaskHook` enum.
#[cfg(all(target_os = "wasi", target_env = "p3"))]
mod task_hook {
    use core::sync::atomic::AtomicBool;
    use core::sync::atomic::Ordering;

    pub const SYNC_START: i32 = 0;
    pub const SYNC_FINISH: i32 = 1;
    pub const ASYNC_START: i32 = 2;
    pub const ASYNC_RESUME: i32 = 3;
    pub const ASYNC_BLOCK: i32 = 4;
    pub const ASYNC_FINISH: i32 = 5;
    pub const INITIALIZE_START: i32 = 6;
    pub const INITIALIZE_FINISH: i32 = 7;
    pub const RESOURCE_DTOR_START: i32 = 8;
    pub const RESOURCE_DTOR_FINISH: i32 = 9;
    pub const POST_RETURN_START: i32 = 10;
    pub const POST_RETURN_FINISH: i32 = 11;
    pub const REALLOC_START: i32 = 12;
    pub const REALLOC_FINISH: i32 = 13;

    /// Whether the statically allocated main-program stack (`__init_stack_pointer`)
    /// is currently handed out to a running task. Sequential cooperative exports
    /// free it on every FINISH before the next START re-acquires it.
    static INIT_STACK_IN_USE: AtomicBool = AtomicBool::new(false);

    /// Set exactly once on the first START hook: the main-thread TLS block is
    /// installed for the program lifetime and never swapped or freed (post-#884
    /// shared-TLS semantics; v34's per-task TLS swap is what made
    /// `#[thread_local]` `RUNTIMES`/`RUNTIME` vanish between `activate` and
    /// `mount`).
    static TLS_INSTALLED: AtomicBool = AtomicBool::new(false);

    #[inline]
    fn init_stack_pointer() -> i32 {
        // `__init_stack_pointer` is an immutable global set by the linker to the
        // top of the `--stack-first` 1 MiB stack (1048576 in the counter build).
        let sp: i32;
        unsafe {
            core::arch::asm!(
                ".globaltype __init_stack_pointer, i32, immutable",
                "global.get __init_stack_pointer",
                "local.set {0}",
                out(local) sp,
                options(nomem, nostack)
            );
        }
        sp
    }

    #[inline]
    fn init_tls_base() -> i32 {
        // `__init_tls_base` is a mutable global holding the offset of the static
        // TLS block laid out by the linker (1315400 in the counter build).
        let base: i32;
        unsafe {
            core::arch::asm!(
                ".globaltype __init_tls_base, i32",
                "global.get __init_tls_base",
                "local.set {0}",
                out(local) base,
                options(nomem, nostack)
            );
        }
        base
    }

    /// Install the shared main-thread TLS block exactly once. `__wasm_init_tls`
    /// both points the TLS base at `base` and initializes its contents from the
    /// `.tdata` template, so every later `#[thread_local]` access sees stable
    /// storage for the rest of the program.
    fn install_tls_once() {
        if TLS_INSTALLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let base = init_tls_base();
            // Linked into every wasip3 guest (present at counter call site
            // `__wasm_init_tls` inside SyncStart's old per-task path).
            unsafe extern "C" {
                fn __wasm_init_tls(base: *mut u8);
            }
            unsafe { __wasm_init_tls(base as *mut u8) };
        }
    }

    /// Acquire the main-program stack for a starting task. Nested/concurrent
    /// Starts are an invariant violation for this sequential cooperative guest
    /// and must not silently share the stack.
    fn task_stack_alloc() -> i32 {
        if INIT_STACK_IN_USE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            init_stack_pointer()
        } else {
            // Sequential export brackets (hook0/finish pairs) are strictly
            // nested-free: if the init stack is still held, two tasks are live
            // at once, which this guest never intends. Crash rather than
            // corrupt the shared stack.
            unreachable!("nested or concurrent __wasilibc_task_hook Start")
        }
    }

    /// Inverse of [`task_stack_alloc`]: release the main-program stack so the
    /// next sequential export can re-acquire it. Matches post-#884
    /// `task_stack_free` for the init-stack case (no secondary malloc stack is
    /// ever handed out).
    fn task_stack_free(prev_context_0: i32) {
        if prev_context_0 == init_stack_pointer() {
            INIT_STACK_IN_USE.store(false, Ordering::Release);
        } else if prev_context_0 != 0 {
            // Only the init stack is ever returned by `task_stack_alloc` above,
            // so anything else reaching here is an unexpected stack identity.
            unreachable!("unexpected task stack handed to __wasilibc_task_hook Finish");
        }
    }

    /// `__wasm_task_hook` (see wasi-libc `__wasm_task_hook.S`) saves the caller's
    /// SP, switches onto a 1024-byte hook stack, invokes this, then installs the
    /// returned value as the SP for the caller's continuation. Return values
    /// therefore mean "the SP the caller should resume with":
    ///
    /// * START → a fresh task stack (or `hook_stack` for realloc, which must not
    ///   allocate);
    /// * FINISH → `NULL` (task done; the component model drives the next Start,
    ///   and wasmtime restores SP around its own lifting trampolines);
    /// * `ASYNC_BLOCK` → the preserved task context; `ASYNC_RESUME` → fresh stack.
    ///
    /// TLS is installed once on the first Start and never touched again — shared
    /// for the whole program (wasi-libc #884). TSD destructors are never run
    /// from this hook (see the `__pthread_tsd_run_dtors` no-op above).
    pub extern "C" fn run(hook: i32, prev_context_0: i32, hook_stack: i32) -> i32 {
        match hook {
            SYNC_START | ASYNC_START | INITIALIZE_START | RESOURCE_DTOR_START
            | POST_RETURN_START => {
                install_tls_once();
                task_stack_alloc()
            }
            SYNC_FINISH | ASYNC_FINISH | INITIALIZE_FINISH | RESOURCE_DTOR_FINISH
            | POST_RETURN_FINISH => {
                task_stack_free(prev_context_0);
                0
            }
            ASYNC_BLOCK => {
                // Free the stack; preserve whatever task context the caller was
                // carrying in `prev_context_0` (post-#884 uses context slot 0
                // for this across a block). Our guest is fully synchronous, so
                // this path is only here to avoid trapping if wit-component ever
                // emits it.
                task_stack_free(prev_context_0);
                prev_context_0
            }
            ASYNC_RESUME => {
                install_tls_once();
                task_stack_alloc()
            }
            REALLOC_START => {
                // Realloc is bounded and non-reentrant: run it on the hook stack
                // we are already switched onto instead of allocating a second
                // task stack. Do not touch TLS (already installed).
                hook_stack
            }
            REALLOC_FINISH => {
                // wasmtime's lifting trampoline owns the surrounding SP; hand
                // back NULL exactly as post-#884 wasi-libc does.
                0
            }
            _ => unreachable!("unknown __wasilibc_task_hook code {hook}"),
        }
    }
}

/// Strong override of wasi-libc v34's `__wasilibc_task_hook`.
///
/// v34 (the wasi-sdk shipped in our rustc sysroot, nightly 2026-09-17) still
/// implements *per-task* TLS: every Start `posix_memalign`s a fresh TLS block
/// from the `.tdata` template, swaps `__tls_base` to it, and every Finish runs
/// TSD destructors and restores the previous TLS base. That means
/// `#[thread_local]` values written during `activate` (Rust std's `RUNTIMES`
/// inside dioxus-core, plus `clay_mdk`'s `RUNTIME`) live in the Start-time
/// block and are gone by the time `mount` opens its own Start — the guest
/// panics with "host drove a guest micro before activating it" and the host
/// maps that trap to `PresentationError::Revoked`.
///
/// Upstream fixed this in wasi-libc#884 ("Share TLS amongst all component
/// model tasks", 2026-10-20) by installing the main-thread TLS once for the
/// program lifetime and never running dtors from the hook. Our rustc nightly
/// (2026-09-17) predates that commit, so this strong definition reimplements
/// the post-#884 contract:
///
/// * TLS: installed once on the first Start via `__wasm_init_tls`, never
///   swapped or freed;
/// * stack: the single statically allocated `__init_stack_pointer` is acquired
///   on Start and released on Finish (exports are strictly sequential);
/// * realloc: runs on the existing hook stack; returns NULL on Finish;
/// * TSD dtors: never run (paired with the `__pthread_tsd_run_dtors` no-op).
///
/// `__wasm_task_hook` (assembly, also linked from libc.a) calls this with the
/// hook code, the caller's saved SP, and the address of its temporary 1024-byte
/// hook stack; whatever this returns becomes the SP after the hook returns.
#[cfg(all(target_os = "wasi", target_env = "p3"))]
#[unsafe(no_mangle)]
pub extern "C" fn __wasilibc_task_hook(hook: i32, prev_context_0: i32, hook_stack: i32) -> i32 {
    task_hook::run(hook, prev_context_0, hook_stack)
}

mod manifest;
pub use manifest::ConfigGrant;
pub use manifest::Contribution;
pub use manifest::FacetAccess;
pub use manifest::MicroId;
pub use manifest::MicroManifest;
pub use manifest::Provision;
pub use manifest::ProvisionRef;

mod component;
pub use component::ComponentRef;

#[cfg(feature = "wit")]
pub mod bindings {
    wit_bindgen::generate!({
        path: "../clay_guest_session/wit",
        world: "micro-client",
        pub_export_macro: true,
    });
}

#[cfg(feature = "wit")]
pub use bindings::exports::townframe::dayframe::document_events;
#[cfg(feature = "wit")]
pub use bindings::exports::townframe::dayframe::micro_lifecycle;
#[cfg(feature = "wit")]
pub use bindings::exports::townframe::dayframe::presentation_events;
#[cfg(feature = "wit")]
pub use bindings::townframe::dayframe::documents;
#[cfg(feature = "wit")]
pub use bindings::townframe::dayframe::presentation;

#[cfg(feature = "wit")]
pub mod mirror;
#[cfg(feature = "wit")]
pub use mirror::ConfigDocument;
#[cfg(feature = "wit")]
pub use mirror::FacetState;
#[cfg(feature = "wit")]
pub use mirror::MirrorSnapshot;
#[cfg(feature = "wit")]
pub use mirror::MirrorWatch;

#[cfg(feature = "in_process")]
pub mod native;

#[cfg(feature = "view")]
pub mod view;

#[cfg(feature = "wasmtime")]
pub mod wasm;

#[cfg(feature = "wcmp")]
pub mod wcmp;

#[cfg(feature = "dioxus")]
pub mod guest;
#[cfg(feature = "dioxus")]
pub use guest::GuestDom;

#[cfg(feature = "dioxus")]
pub mod wire;

#[cfg(all(test, feature = "dioxus", not(target_arch = "wasm32")))]
mod tests;
