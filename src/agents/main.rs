mod interlude {
    pub use restate_sdk::prelude::*;
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

mod auxiliary;
mod cart_object;
mod checkout_service;

use crate::cart_object::CartObject;
use crate::checkout_service::CheckoutService;

fn main() -> Res<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(app_main())
}

async fn app_main() -> Res<()> {
    utils_rs::setup_tracing()?;

    let addr = std::net::SocketAddr::from((
        std::net::Ipv4Addr::UNSPECIFIED,
        utils_rs::get_env_var("PORT")
            .and_then(|str| {
                str.parse()
                    .map_err(|err| ferr!("error parsing port env var ({str}): {err}"))
            })
            .unwrap_or(9090),
    ));

    HttpServer::new(
        Endpoint::builder()
            .bind(cart_object::CartObjectImpl.serve())
            .bind(checkout_service::CheckoutServiceImpl.serve())
            .build(),
    )
    .listen_and_serve(addr)
    .await;

    Ok(())
}
