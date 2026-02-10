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

mod interlude {
    pub use utils_rs::prelude::*;
}

use interlude::*;

pub struct Config {
    pub ocr: OcrConfig,
}

pub struct OcrConfig {
    pub backends: Vec<OcrBackendConfig>,
}

pub enum OcrBackendConfig {
    LocalOnnx {
        text_recognition_onnx_path: PathBuf,
        text_detection_onnx_path: PathBuf,
        character_dict_txt_path: PathBuf,

        document_orientation_onnx_path: Option<PathBuf>,
        text_line_orientation_onnx_path: Option<PathBuf>,
        document_rectification_onnx_path: Option<PathBuf>,

        supported_languages_bcp47: Vec<String>,
    },
}

pub struct Ctx {
    pub config: Config,
}

impl Ctx {
    pub async fn new(config: Config) -> Arc<Self> {
        Self { config }.into()
    }
}

pub fn embed_text(_ctx: &Ctx) {}

pub struct OcrResult {
    pub text: String,
    pub regions: Vec<TextRegion>,
}
pub struct TextRegion {
    /// x,y pairs
    pub bounding_box: Vec<(f32, f32)>,
    pub text: Option<Arc<str>>,
    pub confidence: Option<f32>,
}

pub async fn ocr_image(ctx: &Ctx, images: &[PathBuf]) -> Res<Vec<OcrResult>> {
    if images.is_empty() {
        eyre::bail!("no images provided");
    }

    let backend_config = ctx
        .config
        .ocr
        .backends
        .iter()
        .map(|backend| {
            let OcrBackendConfig::LocalOnnx {
                text_recognition_onnx_path,
                text_detection_onnx_path,
                character_dict_txt_path,
                document_orientation_onnx_path,
                text_line_orientation_onnx_path,
                document_rectification_onnx_path,
                ..
            } = backend;

            (
                text_recognition_onnx_path,
                text_detection_onnx_path,
                character_dict_txt_path,
                document_orientation_onnx_path,
                text_line_orientation_onnx_path,
                document_rectification_onnx_path,
            )
        })
        .next()
        .ok_or_eyre("no ocr backend configured")?;

    use oar_ocr::oarocr::OAROCRBuilder;
    use oar_ocr::utils::load_image;

    let (
        text_recognition_onnx_path,
        text_detection_onnx_path,
        character_dict_txt_path,
        document_orientation_onnx_path,
        text_line_orientation_onnx_path,
        document_rectification_onnx_path,
    ) = backend_config;

    let mut builder = OAROCRBuilder::new(
        text_detection_onnx_path,
        text_recognition_onnx_path,
        character_dict_txt_path,
    );

    if let Some(path) = document_orientation_onnx_path {
        builder = builder.with_document_image_orientation_classification(path);
    }
    if let Some(path) = text_line_orientation_onnx_path {
        builder = builder.with_text_line_orientation_classification(path);
    }
    if let Some(path) = document_rectification_onnx_path {
        builder = builder.with_document_image_rectification(path);
    }

    let ocr = builder.build()?;

    let mut loaded_images = Vec::with_capacity(images.len());
    for image_path in images {
        loaded_images.push(load_image(image_path)?);
    }

    let raw_results = ocr.predict(loaded_images)?;
    let mut results = Vec::with_capacity(raw_results.len());

    for raw_result in raw_results {
        let mut regions = Vec::with_capacity(raw_result.text_regions.len());
        for raw_region in raw_result.text_regions {
            regions.push(TextRegion {
                bounding_box: raw_region
                    .bounding_box
                    .points
                    .into_iter()
                    .map(|point| (point.x, point.y))
                    .collect(),
                text: raw_region.text,
                confidence: raw_region.confidence,
            });
        }

        let text = regions
            .iter()
            .filter_map(|region| region.text.as_deref())
            .collect::<Vec<_>>()
            .join("\n");

        results.push(OcrResult { text, regions });
    }

    Ok(results)
}

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
