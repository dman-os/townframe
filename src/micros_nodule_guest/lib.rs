//! The core cartridge's micros nodule (ADR 052) as the component the node embeds. The handlers
//! live in `micros_nodule`, which the native test harness serves too; this crate only exports
//! them as a `wasi:http/handler@0.3.0` component.

big_ndk::export_nodule!(micros_nodule::Micros);
