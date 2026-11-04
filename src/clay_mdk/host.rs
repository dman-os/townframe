//! Composition-root vocabulary: what a shell or composition needs to link micros, and nothing a
//! micro's `activate` should touch. Micros author against [`crate::Context`]; the supervisor
//! side builds their factories here, grants them [`Resources`], resolves their collection
//! requests into [`CollectionBindings`], and reads the declared rows and shell roots they
//! register.
pub use crate::native::BoundCollection;
pub use crate::native::CollectionBindings;
pub use crate::native::CollectionRegistry;
pub use crate::native::DeclaredRows;
pub use crate::native::DocumentService;
pub use crate::native::Resources;
pub use crate::native::RootRow;
pub use crate::native::declared_rows;
pub use crate::native::factory;
pub use crate::native::manifest_scope;
pub use crate::native::runtime_scope;
#[cfg(feature = "wasmtime")]
pub use crate::wasm::WasmPackage;
#[cfg(feature = "wasmtime")]
pub use crate::wasm::component_package;
#[cfg(feature = "wasmtime")]
pub use crate::wasm::factory as wasm_factory;
#[cfg(feature = "wcmp")]
pub use crate::wcmp::WcmpPackage;
#[cfg(feature = "wcmp")]
pub use crate::wcmp::factory as wcmp_factory;
pub use clay_collection::ReplicaId;
pub use clay_docs::FacetRoute;
pub use clay_guest_session::capabilities::CapabilityPublication;
pub use clay_guest_session::capabilities::PresentationComponent;
