use crate::interlude::*;

use crate::wit::serde::townframe::daybook_api::doc_create::*;

pub const ROUTE: &str = "/doc";
pub const METHOD: Method = Method::POST;

pub async fn service(Json(inp): Json<Input>) -> Response {
    let service = Service::new();
    match service.serve(&inp) {
        Ok(val) => Json(val).into_response(),
        Err(err) => match &err {
            Error::IdOccupied(..) => (StatusCode::BAD_REQUEST, Json(err)).into_response(),
            Error::InvalidInput(..) => (StatusCode::BAD_REQUEST, Json(err)).into_response(),
            Error::Internal(..) => (StatusCode::INTERNAL_SERVER_ERROR, Json(err)).into_response(),
        },
    }
}
