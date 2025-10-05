#![allow(unused)]

mod interlude {
    pub use api_utils_rs::{api, prelude::*};

    pub use crate::{Context, SharedContext};
    pub use async_trait::async_trait;
}

use crate::interlude::*;
use api_utils_rs::api;
use futures::TryFutureExt;

pub struct Context {
    config: Config,
    db: api::StdDb,
    // kanidm: kanidm_client::KanidmClient,
    // rt: tokio::runtime::Runtime,
}

pub type SharedContext = Arc<Context>;

#[derive(educe::Educe, Clone)]
#[educe(Deref, DerefMut)]
pub struct ServiceContext(pub SharedContext);

#[derive(educe::Educe, Clone)]
#[educe(Deref, DerefMut)]
pub struct SharedServiceContext(pub ServiceContext);

#[derive(Debug)]
pub struct Config {}

// mod bindings;
mod doc;

fn init() -> Res<()> {
    // CX.set(Arc::new(Context {
    //     config: Config {},
    //     db: StdDb::PgWasi {},
    //     // rt: tokio::runtime::Builder::new_current_thread()
    //     //     .enable_all()
    //     //     .build()
    //     //     .wrap_err(ERROR_TOKIO)?,
    // }))
    // .map_err(|_| ferr!("double component intialization"))?;
    Ok(())
}

fn cx() -> SharedContext {
    crate::CX
        .get_or_init(|| {
            Arc::new(Context {
                config: Config {},
                db: StdDb::PgWasi {},
                // rt: tokio::runtime::Builder::new_current_thread()
                //     .enable_all()
                //     .build()
                //     .wrap_err(ERROR_TOKIO)?,
            })
        })
        // .expect("component was not initialized")
        .clone()
}

pub const CX: std::sync::OnceLock<SharedContext> = std::sync::OnceLock::new();

mod wit {
    wit_bindgen::generate!({
        path: "../daybook_api/wit",
        world: "api",
        // generate_all,
        // async: true,
        additional_derives: [serde::Serialize, serde::Deserialize],
        with: {
            "wasi:keyvalue/store@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::store,
            "wasi:keyvalue/atomics@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::atomics,
            "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,
            "wasmcloud:postgres/types@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::types,
            "wasmcloud:postgres/query@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::query,
            "wasi:io/poll@0.2.6": api_utils_rs::wit::wasi::io::poll,
            "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
            "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,
            "wasi:config/runtime@0.2.0-draft": api_utils_rs::wit::wasi::config::runtime,

            "townframe:api-utils/utils": api_utils_rs::wit::utils,

            "townframe:daybook-api/doc/doc-kind": daybook_types::doc::DocKind,
            "townframe:daybook-api/doc/doc-tag": daybook_types::doc::DocTag,
            "townframe:daybook-api/doc/doc-image": daybook_types::doc::DocImage,
            "townframe:daybook-api/doc/doc": daybook_types::doc::Doc,
            "townframe:daybook-api/doc-create/input": daybook_types::doc::doc_create::Input,
            "townframe:daybook-api/doc-create/error-id-occupied": daybook_types::doc::doc_create::ErrorIdOccupied,
            "townframe:daybook-api/doc/doc-tag-kind": daybook_types::doc::DocTagKind,
            "townframe:daybook-api/doc-create/error": daybook_types::doc::doc_create::Error,
            "townframe:daybook-api/doc/doc-blob": daybook_types::doc::DocBlob,
            "townframe:daybook-api/doc/doc-content": daybook_types::doc::DocContent,
        }
    });
}

wit::export!(Component with_types_in wit);

struct Component;

impl wit::exports::townframe::daybook_api::ctx::Guest for Component {
    fn init() -> Result<(), String> {
        crate::init().map_err(|err| format!("{err:?}"))?;
        Ok(())
    }
}
impl wit::exports::townframe::daybook_api::doc_create::Guest for Component {
    type Service = doc::create::DocCreate;
}
