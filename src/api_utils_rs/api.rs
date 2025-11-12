use crate::interlude::*;

// pub use sqlx;
pub use http::StatusCode;

use std::borrow::Cow;
use utils_rs::type_name_raw;
use utoipa::openapi;

/* #[derive(Clone, educe::Educe)]
#[educe(Deref, Debug)]
pub struct RedisPool(
    #[educe(Debug(ignore))] pub bb8_redis::bb8::Pool<bb8_redis::RedisConnectionManager>,
); */

#[derive(Debug)]
#[non_exhaustive]
pub enum StdDb {
    PgWasi {},
}

#[async_trait]
pub trait Endpoint: Send + Sync + 'static {
    type Request: Send + Sync + 'static;
    type Response;
    type Error;
    type Cx: Send + Sync + 'static;

    async fn handle(
        &self,
        cx: &Self::Cx,
        request: Self::Request,
    ) -> Result<Self::Response, Self::Error>;
}

#[async_trait]
pub trait Authorize {
    type Info: Send + Sync + 'static;
    type Request: Send + Sync + 'static;
    type Error;

    async fn authorize(&self, request: Self::Request) -> Result<Self::Info, Self::Error>;
}

#[async_trait]
pub trait AuthenticatedEndpoint: Send + Sync + 'static {
    type Request: Send + Sync + 'static;
    type Response;
    type Error: From<<Self::Cx as Authorize>::Error>;
    type Cx: Send + Sync + 'static + Authorize;

    fn authorize_request(&self, request: &Self::Request) -> <Self::Cx as Authorize>::Request;

    async fn handle(
        &self,
        cx: &Self::Cx,
        auth_info: <Self::Cx as Authorize>::Info,
        request: Self::Request,
    ) -> Result<Self::Response, Self::Error>;
}

// pub enum AuthenticatedEndpointError<E> {
//     AuthenticationError(E),
//     EndpointError(E)
// }

#[async_trait]
impl<T> Endpoint for T
where
    T: AuthenticatedEndpoint,
    T::Cx: Authorize,
{
    type Request = T::Request;
    type Response = T::Response;
    type Error = T::Error;
    type Cx = T::Cx;

    async fn handle(
        &self,
        cx: &Self::Cx,
        request: Self::Request,
    ) -> Result<Self::Response, Self::Error> {
        let auth_info = {
            let auth_args = self.authorize_request(&request);
            cx.authorize(auth_args).await?
        };
        self.handle(cx, auth_info, request).await
    }
}

pub struct Tag {
    pub name: &'static str,
    pub desc: &'static str,
}

impl From<Tag> for openapi::Tag {
    fn from(tag: Tag) -> Self {
        openapi::tag::TagBuilder::new()
            .name(tag.name)
            .description(Some(tag.desc))
            .build()
    }
}

pub const DEFAULT_TAG: Tag = Tag {
    name: "api",
    desc: "This is the catch all tag.",
};

pub fn axum_path_parameter_list(path: &str) -> Vec<String> {
    path.split('/')
        .filter(|s| !s.is_empty())
        .filter(|s| &s[0..1] == ":")
        .map(|s| s[1..].to_string())
        .collect()
}

#[test]
fn test_axum_path_paramter_list() {
    for (expected, path) in [
        (vec!["id".to_string()], "/users/:id"),
        (
            vec!["id".to_string(), "resID".to_string()],
            "/users/:id/resource/:resID",
        ),
    ] {
        assert_eq!(
            expected,
            &axum_path_parameter_list(path)[..],
            "failed on {path}"
        );
    }
}

pub trait ToRefOrSchema {
    fn schema_name() -> Cow<'static, str>;
    fn ref_or_schema() -> openapi::RefOr<openapi::schema::Schema>;
}

impl<T> ToRefOrSchema for T
where
    T: utoipa::ToSchema,
{
    fn ref_or_schema() -> openapi::RefOr<openapi::schema::Schema> {
        T::schema()
    }

    fn schema_name() -> Cow<'static, str> {
        T::name()
        // type_name_raw::<T>()
    }
}

pub struct NoContent;

impl From<()> for NoContent {
    fn from(_: ()) -> Self {
        Self
    }
}

impl ToRefOrSchema for NoContent {
    fn schema_name() -> Cow<'static, str> {
        type_name_raw::<NoContent>().into()
    }

    fn ref_or_schema() -> openapi::RefOr<openapi::schema::Schema> {
        panic!("this baby is special cased")
    }
}

/// Used to reference another schema
#[derive(educe::Educe, serde::Serialize, serde::Deserialize)]
#[serde(crate = "serde")]
#[educe(Deref, DerefMut)]
pub struct SchemaRef<T>(pub T);

impl<T> From<T> for SchemaRef<T> {
    fn from(inner: T) -> Self {
        Self(inner)
    }
}

impl<T> ToRefOrSchema for SchemaRef<T>
where
    T: utoipa::ToSchema + serde::Serialize,
{
    fn ref_or_schema() -> openapi::RefOr<openapi::schema::Schema> {
        openapi::schema::Ref::from_schema_name(type_name_raw::<T>()).into()
        // utoipa::openapi::ObjectBuilder::new()
        //     .property(
        //         "$ref",
        //         utoipa::openapi::schema::Ref::from_schema_name(T::type_name_raw()),
        //     )
        //     .into()
    }

    fn schema_name() -> Cow<'static, str> {
        T::schema_name()
    }
}

/// (description, example)
pub type ErrorResponse<Err> = (&'static str, Err);

pub trait ErrorResp {
    fn error_responses() -> Vec<(StatusCode, String)>;
}
