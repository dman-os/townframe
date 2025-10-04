use crate::interlude::*;

mod create;

pub fn router() -> axum::Router {
    axum::Router::new()
        //
        .route(
            create::ROUTE,
            method_router(create::METHOD, create::service),
        )
}
