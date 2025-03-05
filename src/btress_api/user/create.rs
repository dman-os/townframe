use crate::interlude::*;

#[derive(Debug, Clone)]
pub struct CreateUser;

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema, garde::Validate)]
pub struct Request {
    #[garde(ascii, length(min = 3, max = 25), pattern(super::USERNAME_REGEX))]
    pub username: String,
}

pub type Response = SchemaRef<super::User>;

#[derive(
    Debug, Serialize, thiserror::Error, displaydoc::Display, macros::HttpError, utoipa::ToSchema,
)]
pub enum Error {
    /// username occupied {username:?}
    #[http(code(StatusCode::BAD_REQUEST), desc("Username occupied"))]
    UsernameOccupied { username: String },
    /// invalid input: {issues:?}
    #[http(code(StatusCode::BAD_REQUEST), desc("Invalid input"))]
    InvalidInput {
        #[from]
        issues: ValidationErrors,
    },
    /// internal server error: {msg}
    #[http(code(StatusCode::INTERNAL_SERVER_ERROR), desc("Internal server error"))]
    Internal { msg: String },
}

#[async_trait]
impl Endpoint for CreateUser {
    type Request = Request;
    type Response = Response;
    type Error = Error;
    type Cx = Context;

    async fn handle(
        &self,
        _cx: &Self::Cx,
        request: Self::Request,
    ) -> Result<Self::Response, Self::Error> {
        Ok(super::User {
            username: request.username,
        }
        .into())
    }
}

impl HttpEndpoint for CreateUser {
    const METHOD: Method = Method::Post;
    const PATH: &'static str = "/users";

    type SharedCx = SharedContext;
    type HttpRequest = (Json<Request>,);

    fn request((Json(req),): Self::HttpRequest) -> Result<Self::Request, Self::Error> {
        Ok(req)
    }

    fn response(resp: Self::Response) -> HttpResponse {
        Json(resp).into_response()
    }
}

impl DocumentedEndpoint for CreateUser {
    const TAG: &'static Tag = &super::TAG;
}

#[cfg(test)]
mod test {
    use crate::interlude::*;
}
