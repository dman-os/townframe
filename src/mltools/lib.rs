/*
Requirements:

mltools_local: local execution of ML tools.
mltools_cloud: client for cloud token providers.
mltools_server: mltools_local but for servers.
mltools_gateway: durable-streams based API for mltools_server or mltools_cloud.
mltools: routes to mltools_local, mltools_client or mltools_cloud depending on config.

ML tools support:
- OCR
    - Local
    - Cloud
    - Server
- Embedding
    - Local
    - Cloud
    - Server
- LLM
    - Cloud
    - Server
- STT
    - Cloud
    - Server

*/
// mod wit {
//     wit_bindgen::generate!({
//         world: "guest",
//         additional_derives: [serde::Serialize, serde::Deserialize],
//         with: {
//             // "wasi:keyvalue/store@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::store,
//             // "wasi:keyvalue/atomics@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::atomics,
//             // "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,
//             // "wasmcloud:postgres/types@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::types,
//             // "wasmcloud:postgres/query@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::query,
//             // "wasi:io/poll@0.2.6": api_utils_rs::wit::wasi::io::poll,
//             // "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
//             "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,
//             // "wasi:config/runtime@0.2.0-draft": api_utils_rs::wit::wasi::config::runtime,
//
//             // "townframe:api-utils/utils": api_utils_rs::wit::utils,
//
//             "townframe:mltools/types": generate,
//             "townframe:mltools/llm-chat": generate,
//         }
//     });
// }

/// local execution of ML tools.
mod local {}
/// client for cloud token providers.
mod cloud {}
/// durable-streams based API for mltools_server or mltools_cloud.
mod gateway {}
/// mltools_local but for servers.
mod server {}
/// routes to mltools_local, mltools_client or mltools_cloud depending on config.
mod client {}
