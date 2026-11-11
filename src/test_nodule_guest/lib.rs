//! Tiny `wasi:http/handler@0.3.0` guest for exercising the nodule Wasm provider. The handlers
//! (routes, headers and behavior) live in `test_nodule_handlers`, which the native test harness
//! serves too; this crate only exports them as the component.

big_ndk::export_nodule!(test_nodule_handlers::Fixture);
