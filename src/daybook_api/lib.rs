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
mod gen;

fn init() -> Res<()> {
    // CX.set(Arc::new(Context {
    //     config: Config {},
    //     db: StdDb::PgWasi {},
    //     // rt: tokio::runtime::Builder::new_current_thread()
    //     //     .enable_all()
    //     //     .build()
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
        with: {
            "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,

            "wasi:io/poll@0.2.6": generate,
            "wasi:io/error@0.2.6": generate,
            "wasi:io/streams@0.2.6": generate,
            "wasi:http/types@0.2.6": generate,

            "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
            "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,

            "townframe:api-utils/utils": api_utils_rs::wit::utils,

            "townframe:daybook-api/ctx": generate,
            "townframe:daybook-types/doc": daybook_types::gen::wit::doc,
            "townframe:daybook-types/doc/doc": daybook_types::wit::Doc,
            "townframe:daybook-types/doc/doc-content-kind": generate,
            "townframe:daybook-types/doc/doc-prop": daybook_types::gen::wit::doc::DocProp,
            "townframe:daybook-types/doc/doc-blob": daybook_types::gen::wit::doc::DocBlob,
            "townframe:daybook-types/doc/doc-content": daybook_types::gen::wit::doc::DocContent,
            "townframe:daybook-types/doc/image-meta": daybook_types::gen::wit::doc::ImageMeta,

            "townframe:daybook-api/doc-create/input": crate::gen::doc::doc_create::Input,
            "townframe:daybook-api/doc-create/error-id-occupied": crate::gen::doc::doc_create::ErrorIdOccupied,
            "townframe:daybook-api/doc-create": generate,
            "townframe:daybook-api/doc-create/error": crate::gen::doc::doc_create::Error,
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
