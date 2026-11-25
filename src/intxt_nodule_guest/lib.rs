//! intxt's `wasi:http/handler@0.3.0` component. The handlers live in `intxt_nodule`, which the
//! native test harness serves too; this crate only exports them as the component.

big_ndk::export_nodule!(intxt_nodule::Intxt);
