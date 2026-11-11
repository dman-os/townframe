//! The Micro Development Kit (ADR 015 §5): the single authoring API every micro uses.
//! `manifest` and `ComponentRef` are neutral vocabulary every backend compiles against;
//! `bindings` carries the WIT-generated surface; `mirror` is the typed document-mirror
//! authoring surface over it (typed decoding, availability/freshness evidence, named config
//! bindings). With feature `in_process`, [`Context`] is the one authoring surface a micro's
//! `activate` receives (config, collections, requirements, component and call publication,
//! owned tasks), backed by the in-process host sessions; `hooks` (feature `view`) binds its
//! handles to Dioxus reactivity; `host` is the composition-root vocabulary (factories,
//! resources, bindings) that micros never touch, including the Wasmtime (`wasm`) and browser
//! (`wcmp`, ADR 018) guest-engine factories. `guest` (feature `dioxus`) is the guest backend,
//! still on its own `activate(&guest::Context)` shape over the WIT imports until it joins
//! [`Context`], plus the Dioxus VirtualDom adapter and the `export_micro!` entry point that
//! adapts it to `micro-lifecycle.initialize` (ADR 030). The MDK adds ergonomics, never
//! authority: capabilities are still the host-issued resources.
#![cfg_attr(
    all(target_os = "wasi", target_env = "p3"),
    feature(asm_experimental_arch)
)]
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
    use core::sync::atomic::AtomicPtr;
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
    /// is currently handed out to a running task. A task releases it on FINISH or when it
    /// blocks, before any other task starts or resumes.
    static INIT_STACK_IN_USE: AtomicBool = AtomicBool::new(false);

    /// Set exactly once on the first START hook: the main-thread TLS block is
    /// installed for the program lifetime and never swapped or freed (post-#884
    /// shared-TLS semantics; v34's per-task TLS swap is what made
    /// `#[thread_local]` `RUNTIMES`/`RUNTIME` vanish between `activate` and
    /// `mount`).
    static TLS_INSTALLED: AtomicBool = AtomicBool::new(false);

    /// The running async export task's wit-bindgen task state (`start_task` stores its boxed
    /// `TaskState` here and every callback reads it back). wit-bindgen without its `std`
    /// feature reaches it through [`wit_bindgen_0_62_0_get_task_context`] and
    /// [`wit_bindgen_0_62_0_set_task_context`], which we define over this pointer instead of
    /// the C runtime's `_Thread_local`.
    ///
    /// This is task state, not thread state: TLS is shared by every task (see [`run`]), so the
    /// hook must park this pointer in the task's component-model context slot 0 whenever the
    /// task blocks and put it back when the task resumes. Between a `BLOCK` and the matching
    /// `RESUME` this is null, so another task (async or sync) can run in between.
    pub(super) static CURRENT: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());

    /// Whether wit-bindgen has ever stored task state through our `set_task_context`. Every
    /// async export calls `start_task`, which does, so a task that blocks or finishes without
    /// this being set means wit-bindgen is not using our symbols (its `std` feature came back
    /// on through some dependency's default features, or the version in the symbol names no
    /// longer matches the resolved wit-bindgen). [`run`] turns that into an immediate panic
    /// instead of a silent return to one-suspended-export-per-instance.
    pub(super) static TASK_CONTEXT_WIRED: AtomicBool = AtomicBool::new(false);

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
            // A task releases the stack before any other can run (FINISH or BLOCK),
            // so a held stack means two tasks are running at once, which the
            // component model never does on one instance. Crash rather than
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
    /// returned value as the value of context slot 0 for the caller's continuation.
    /// `prev_context_0` is what slot 0 held when the event fired. Return values
    /// therefore mean "what slot 0 (the SP) should be from here on":
    ///
    /// * START → a fresh task stack (or `hook_stack` for realloc, which must not
    ///   allocate);
    /// * FINISH → `NULL` (task done; the component model drives the next Start,
    ///   and wasmtime restores SP around its own lifting trampolines);
    /// * `ASYNC_BLOCK` → the task's parked wit-bindgen state (see [`CURRENT`]), which the
    ///   component model keeps in slot 0 for this task alone until it resumes;
    /// * `ASYNC_RESUME` → a fresh stack, with [`CURRENT`] restored from `prev_context_0`.
    ///
    /// Two different things are deliberately scoped differently:
    ///
    /// * **TLS is shared by the whole program.** It is installed once on the first Start and
    ///   never touched again (wasi-libc #884). Dioxus keeps its `RUNTIMES` stack and our
    ///   `RUNTIME` in `#[thread_local]`s, and they must survive from `activate` (one export
    ///   task) into `mount` and `dispatch` (other tasks); wasi-libc v34's per-task TLS block
    ///   would make them vanish between exports. TSD destructors are never run from this hook
    ///   (see the `__pthread_tsd_run_dtors` no-op above).
    /// * **wit-bindgen's task state is per task.** `start_task` asserts the slot it stores the
    ///   task in is empty, and a task that is parked on an await must find its own state again
    ///   on resume, while any number of other async exports may start, run and park in
    ///   between. That is what lets calls into one instance overlap. Stock wasi-libc gets this
    ///   by swapping the TLS base per task; since we share TLS, we swap just this one pointer
    ///   the same way, through slot 0 (the slot the stack pointer uses while a task runs).
    ///
    /// Sync exports (`mount`, `dispatch`, ...) never touch [`CURRENT`]: they run to completion
    /// on the shared TLS while any async tasks are parked with a null `CURRENT`.
    pub extern "C" fn run(hook: i32, prev_context_0: i32, hook_stack: i32) -> i32 {
        match hook {
            ASYNC_START => {
                assert!(
                    CURRENT.load(Ordering::Relaxed).is_null(),
                    "async export started while another task's state is current"
                );
                install_tls_once();
                task_stack_alloc()
            }
            SYNC_START | INITIALIZE_START | RESOURCE_DTOR_START | POST_RETURN_START => {
                install_tls_once();
                task_stack_alloc()
            }
            ASYNC_FINISH => {
                assert_task_context_wired();
                CURRENT.store(core::ptr::null_mut(), Ordering::Relaxed);
                task_stack_free(prev_context_0);
                0
            }
            SYNC_FINISH | INITIALIZE_FINISH | RESOURCE_DTOR_FINISH | POST_RETURN_FINISH => {
                task_stack_free(prev_context_0);
                0
            }
            ASYNC_BLOCK => {
                // The stack pointer in slot 0 is no longer needed once the task blocks, and
                // the next task must find `CURRENT` empty. What the task keeps in slot 0
                // while parked is its wit-bindgen state, handed back on RESUME.
                assert_task_context_wired();
                task_stack_free(prev_context_0);
                CURRENT.swap(core::ptr::null_mut(), Ordering::Relaxed) as usize as i32
            }
            ASYNC_RESUME => {
                install_tls_once();
                CURRENT.store(prev_context_0 as usize as *mut u8, Ordering::Relaxed);
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

    fn assert_task_context_wired() {
        assert!(
            TASK_CONTEXT_WIRED.load(Ordering::Relaxed),
            "wit-bindgen task state does not go through clay_mdk's task-context symbols: \
             its `std` feature is on, or its version no longer matches the symbol names"
        );
    }
}

/// wit-bindgen 0.62.0's per-task state accessors for `wasm32-wasip3` without its `std`
/// feature (its default `std` feature would use a libc `thread_local!` instead, which we do
/// not swap per task). The symbol names embed the wit-bindgen version, so a version bump
/// silently stops using them; `task_hook::run` panics on the first async export that blocks or
/// finishes without having gone through [`wit_bindgen_0_62_0_set_task_context`], and the
/// workspace pins wit-bindgen with `=0.62.0` so that a bump is a conscious edit next to these
/// names. They override the C runtime's `_Thread_local` versions in `libwit_bindgen_cabi_wasip3.a`
/// (our object precedes the archive on the link line).
#[cfg(all(target_os = "wasi", target_env = "p3"))]
#[unsafe(no_mangle)]
pub extern "C" fn wit_bindgen_0_62_0_get_task_context() -> *mut u8 {
    task_hook::CURRENT.load(core::sync::atomic::Ordering::Relaxed)
}

/// See [`wit_bindgen_0_62_0_get_task_context`].
#[cfg(all(target_os = "wasi", target_env = "p3"))]
#[unsafe(no_mangle)]
pub extern "C" fn wit_bindgen_0_62_0_set_task_context(ptr: *mut u8) {
    task_hook::TASK_CONTEXT_WIRED.store(true, core::sync::atomic::Ordering::Relaxed);
    task_hook::CURRENT.store(ptr, core::sync::atomic::Ordering::Relaxed);
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
///   on Start/Resume and released on Finish/Block. Only one task ever *runs* at a
///   time and a parked task holds no stack, so async exports can overlap;
/// * wit-bindgen task state: one pointer, swapped through context slot 0 on
///   Block/Resume (see `task_hook::run`);
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

/// Export the `micro-client` world for a micro authored as `async fn activate(Context)`, and —
/// when the MDK is built with its in-process backend — a native `ACTIVATE` entry over the same
/// function, so one crate links natively (`host::factory(.., ACTIVATE)`) or builds as a
/// `cdylib` guest. Invoke once at the crate root. `$manifest` is the `fn() -> MicroManifest`
/// the host admits this micro against: a guest names its call contracts and checks its binding
/// names from it, so there is no second source of truth.
#[macro_export]
macro_rules! export_micro {
    ($activate:path, $manifest:path) => {
        $crate::__export_native_entry!($activate);
        $crate::__export_guest_world!($activate, $manifest);
    };
}

/// The native half of [`export_micro!`].
#[doc(hidden)]
#[macro_export]
#[cfg(feature = "in_process")]
macro_rules! __export_native_entry {
    ($activate:path) => {
        /// The micro's `activate` as the supervisor factory takes it.
        pub const ACTIVATE: fn(
            $crate::Context,
        ) -> ::std::pin::Pin<
            ::std::boxed::Box<
                dyn ::std::future::Future<Output = ::std::result::Result<(), $crate::Error>>,
            >,
        > = |context| ::std::boxed::Box::pin($activate(context));
    };
}

#[doc(hidden)]
#[macro_export]
#[cfg(not(feature = "in_process"))]
macro_rules! __export_native_entry {
    ($activate:path) => {};
}

/// The guest half of [`export_micro!`]. Only a wasi target exports the component world: a native
/// build of the same crate links through `ACTIVATE` instead (and a native `cdylib` has no
/// component exports to link).
#[doc(hidden)]
#[macro_export]
#[cfg(feature = "guest")]
macro_rules! __export_guest_world {
    ($activate:path, $manifest:path) => {
        #[cfg(target_os = "wasi")]
        struct ClayMdkExportedMicro;

        #[cfg(target_os = "wasi")]
        impl $crate::micro_lifecycle::Guest for ClayMdkExportedMicro {
            type InstanceRoot = $crate::guest::GuestRoot;

            async fn initialize(
                context: $crate::micro_lifecycle::InitialContext,
            ) -> ::std::result::Result<
                $crate::micro_lifecycle::Initialized,
                $crate::micro_lifecycle::InitializationError,
            > {
                $crate::guest::Runtime::initialize($activate, $manifest, context).await
            }

            async fn call(
                publication: ::std::string::String,
                request: ::std::vec::Vec<u8>,
            ) -> ::std::result::Result<::std::vec::Vec<u8>, ::std::string::String> {
                $crate::guest::Runtime::call(publication, request).await
            }
        }

        #[cfg(target_os = "wasi")]
        impl $crate::document_events::Guest for ClayMdkExportedMicro {
            fn changed(snapshot: $crate::documents::DocumentSnapshot) {
                $crate::guest::Runtime::changed(snapshot);
            }

            fn lagged(watch_id: u64) {
                $crate::guest::Runtime::lagged(watch_id);
            }

            fn completed(receipt: $crate::documents::OperationReceipt) {
                $crate::guest::Runtime::completed(receipt);
            }
        }

        #[cfg(target_os = "wasi")]
        impl $crate::presentation_events::Guest for ClayMdkExportedMicro {
            fn mount(
                publication: ::std::string::String,
                placement: &$crate::presentation::MountPoint,
            ) -> ::std::result::Result<
                $crate::presentation::Surface,
                $crate::presentation::PresentationError,
            > {
                $crate::guest::Runtime::mount(&publication, placement)
            }

            fn unmount(surface: &$crate::presentation::Surface) {
                $crate::guest::Runtime::unmount(surface);
            }

            fn dispatch(
                surface: &$crate::presentation::Surface,
                event: $crate::presentation_events::Event,
            ) -> $crate::presentation_events::EventResponse {
                $crate::guest::Runtime::dispatch(surface, event)
            }
        }

        #[cfg(target_os = "wasi")]
        $crate::bindings::export!(ClayMdkExportedMicro with_types_in $crate::bindings);
    };
}

#[doc(hidden)]
#[macro_export]
#[cfg(not(feature = "guest"))]
macro_rules! __export_guest_world {
    ($activate:path, $manifest:path) => {};
}

mod manifest;
pub use manifest::Access;
pub use manifest::AttachmentDeclaration;
pub use manifest::BindingResolution;
pub use manifest::CollectionCapability;
pub use manifest::CollectionContract;
pub use manifest::ConfigurationRequest;
pub use manifest::ContributionDeclaration;
pub use manifest::Entrypoints;
pub use manifest::FacetAccess;
pub use manifest::InterfaceRef;
pub use manifest::LOCAL_NAMESPACE;
pub use manifest::MANIFEST_NSID;
pub use manifest::MicroId;
pub use manifest::MicroManifest;
pub use manifest::Necessity;
pub use manifest::Nsid;
pub use manifest::PublicationDeclaration;
pub use manifest::PublicationKind;
pub use manifest::Request;
pub use manifest::SHELL_ROOTS;
pub use manifest::Sidecar;

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

/// The `instance-root` of a guest whose incarnation needs no orderly close work: its state
/// is released when the host drops the store.
#[cfg(feature = "wit")]
pub struct StatelessRoot;

#[cfg(feature = "wit")]
impl micro_lifecycle::GuestInstanceRoot for StatelessRoot {
    async fn close(&self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(feature = "wit")]
pub use bindings::exports::townframe::dayframe::document_events;
#[cfg(feature = "wit")]
pub use bindings::exports::townframe::dayframe::micro_lifecycle;
#[cfg(feature = "wit")]
pub use bindings::exports::townframe::dayframe::presentation_events;
#[cfg(feature = "wit")]
pub use bindings::townframe::dayframe::capability_bank;
#[cfg(feature = "wit")]
pub use bindings::townframe::dayframe::collection_registration;
#[cfg(feature = "wit")]
pub use bindings::townframe::dayframe::collections;
#[cfg(feature = "wit")]
pub use bindings::townframe::dayframe::documents;
#[cfg(feature = "wit")]
pub use bindings::townframe::dayframe::presentation;

#[cfg(feature = "wit")]
pub mod mirror;
#[cfg(feature = "wit")]
pub mod registration;
#[cfg(feature = "wit")]
pub use mirror::ConfigDocument;
#[cfg(feature = "wit")]
pub use mirror::MirrorSnapshot;
#[cfg(feature = "wit")]
pub use mirror::MirrorWatch;

#[cfg(feature = "package")]
pub mod package;

/// The `context` micro is one source file linked both as the `context` example crate and into
/// this crate's tests (the native conformance run and the Wasmtime slot test's manifest), so it
/// names the MDK as `clay_mdk` in both.
#[cfg(all(test, feature = "view"))]
extern crate self as clay_mdk;
#[cfg(all(test, feature = "view"))]
#[path = "context_micro.rs"]
mod context_micro;

#[cfg(feature = "in_process")]
mod native;

#[cfg(feature = "contract")]
mod attachment;
#[cfg(any(feature = "in_process", feature = "guest"))]
mod context;
#[cfg(any(feature = "in_process", feature = "guest"))]
mod rows;
#[cfg(feature = "contract")]
pub use attachment::Attachment;
#[cfg(feature = "contract")]
pub use attachment::CAPABILITY_ATTACHMENT_TAG;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::CallBinding;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::Capability;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::Config;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::Context;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::Error;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::Facet;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::FacetSnapshot;
#[cfg(feature = "in_process")]
pub use context::Instance;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::Operation;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::OperationError;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::OperationReceipt;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::OperationStatus;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::PresentationComponent;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::RefStatus;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::RowAttachment;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use context::Stamp;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use rows::Collection;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use rows::Registration;
#[cfg(any(feature = "in_process", feature = "guest"))]
pub use rows::Row;

/// A configuration facet's availability (ADR 015 §4): distinct `Loading` (not hydrated yet) and
/// `Absent` (known absent after hydration) so an uncached facet is never mistaken for a missing
/// one. Shared by every backend's facet reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FacetState<T = String> {
    Loading,
    Absent,
    Present(T),
}

#[cfg(feature = "in_process")]
pub mod host;

#[cfg(any(feature = "view", feature = "guest"))]
pub mod hooks;

#[cfg(any(feature = "wasmtime", feature = "wcmp"))]
mod engine;

#[cfg(feature = "wasmtime")]
mod wasm;

#[cfg(feature = "wcmp")]
mod wcmp;

#[cfg(feature = "guest")]
pub mod guest;
#[cfg(feature = "guest")]
pub use guest::GuestDom;

#[cfg(feature = "guest")]
pub mod wire;

#[cfg(all(test, feature = "guest", not(target_arch = "wasm32")))]
mod tests;
