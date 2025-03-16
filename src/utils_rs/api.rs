use std::{borrow::Cow, future::Future};

use axum::{
    extract::*,
    response::{self, IntoResponse},
};
use utoipa::openapi;

pub use axum::http::StatusCode;
// pub use sqlx;

use super::{default, type_name_raw};

pub type BearerToken =
    axum_extra::headers::Authorization<axum_extra::headers::authorization::Bearer>;
pub type DiscardBody = axum::body::Body;

/* #[derive(Clone, educe::Educe)]
#[educe(Deref, Debug)]
pub struct RedisPool(
    #[educe(Debug(ignore))] pub bb8_redis::bb8::Pool<bb8_redis::RedisConnectionManager>,
); */

#[derive(Debug)]
#[non_exhaustive]
pub enum StdDb {
    Pg { db_pool: sqlx::postgres::PgPool },
}

#[async_trait::async_trait]
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

#[async_trait::async_trait]
pub trait Authorize {
    type Info: Send + Sync + 'static;
    type Request: Send + Sync + 'static;
    type Error;

    async fn authorize(&self, request: Self::Request) -> Result<Self::Info, Self::Error>;
}

#[async_trait::async_trait]
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

#[async_trait::async_trait]
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

pub type Method = openapi::HttpMethod;
pub type HttpResponse = axum::response::Response;

pub trait HttpEndpoint: Endpoint + Clone
where
    Self::Error: serde::Serialize,
    for<'a> &'a Self::Error: Into<StatusCode>,
{
    type SharedCx: std::ops::Deref<Target = Self::Cx> + Send + Sync + Clone;
    type HttpRequest: axum::extract::FromRequest<Self::SharedCx> + Send + Sync + 'static;
    const METHOD: Method;
    const PATH: &'static str;
    const SUCCESS_CODE: StatusCode = StatusCode::OK;
    // type HttpResponse: axum::response::IntoResponse;

    /// TODO: consider making this a `From` trait bound on `Self::HttpRequest`
    fn request(params: Self::HttpRequest) -> Result<Self::Request, Self::Error>;
    /// TODO: consider constraining `Self::Response` to `Serialize` and providing
    /// a default impl using `Json`
    fn response(resp: Self::Response) -> HttpResponse;

    /// This actally need not be a method but I guess it allows for easy behavior
    /// modification. We ought to probably move these to the `Handler` impl
    /// when they stabilize specialization
    fn http(
        &self,
        req: Request,
        cx: Self::SharedCx,
    ) -> std::pin::Pin<Box<dyn Future<Output = axum::response::Response> + Send>> {
        let this = self.clone();
        Box::pin(async move {
            let req = match Self::HttpRequest::from_request(req, &cx).await {
                Ok(val) => val,
                Err(err) => return err.into_response(),
            };
            let req = match Self::request(req) {
                Ok(val) => val,
                Err(err) => {
                    return (Into::<StatusCode>::into(&err), response::Json(err)).into_response()
                }
            };
            // we have to clone it or the borrow checker biches that &T is
            match this.handle(&cx, req).await {
                // Ok(ok) => Into::<Self::HttpResponse>::into(ok).into_response(),
                Ok(ok) => {
                    let mut resp = Self::response(ok);
                    *resp.status_mut() = Self::SUCCESS_CODE;
                    resp
                }
                Err(err) => (Into::<StatusCode>::into(&err), response::Json(err)).into_response(),
            }
        })
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

pub enum ParameterDoc {
    Param(Box<openapi::path::Parameter>),
    Body(Box<openapi::request_body::RequestBody>),
}

impl From<openapi::path::Parameter> for ParameterDoc {
    fn from(param: openapi::path::Parameter) -> Self {
        Self::Param(Box::new(param))
    }
}

impl From<openapi::request_body::RequestBody> for ParameterDoc {
    fn from(body: openapi::request_body::RequestBody) -> Self {
        Self::Body(Box::new(body))
    }
}

pub trait DocumentedParameter {
    // FIXME:: yikes
    const HAS_BEARER: bool = false;
    fn to_openapi(op_id: &str, path: &str) -> Vec<ParameterDoc>;
}

// impl<T> DocumentedParameter for axum::extract::Path<T> {
//     fn to_openapi(_op_id: &str, path: &str) -> Vec<ParameterDoc> {
//         axum_path_parameter_list(path)
//             .into_iter()
//             .map(|name| {
//                 openapi::path::ParameterBuilder::new()
//                     .name(name)
//                     .parameter_in(openapi::path::ParameterIn::Path)
//                     .required(openapi::Required::True)
//                     .build()
//                     .into()
//             })
//             .collect()
//     }
// }

impl DocumentedParameter for axum::extract::Path<String> {
    fn to_openapi(_op_id: &str, path: &str) -> Vec<ParameterDoc> {
        axum_path_parameter_list(path)
            .into_iter()
            .map(|name| {
                openapi::path::ParameterBuilder::new()
                    .name(name)
                    .parameter_in(openapi::path::ParameterIn::Path)
                    .required(openapi::Required::True)
                    .schema(Some(
                        openapi::schema::ObjectBuilder::new()
                            .schema_type(openapi::schema::Type::String),
                    ))
                    .build()
                    .into()
            })
            .collect()
    }
}

impl DocumentedParameter for axum::extract::Path<uuid::Uuid> {
    fn to_openapi(_op_id: &str, path: &str) -> Vec<ParameterDoc> {
        axum_path_parameter_list(path)
            .into_iter()
            .map(|name| {
                openapi::path::ParameterBuilder::new()
                    .name(name)
                    .parameter_in(openapi::path::ParameterIn::Path)
                    .required(openapi::Required::True)
                    .schema(Some(
                        openapi::schema::ObjectBuilder::new()
                            .schema_type(openapi::schema::Type::String)
                            .format(Some(openapi::SchemaFormat::KnownFormat(
                                openapi::KnownFormat::Uuid,
                            ))),
                    ))
                    .build()
                    .into()
            })
            .collect()
    }
}

impl<T> DocumentedParameter for axum::extract::Json<T>
where
    T: ToRefOrSchema,
{
    fn to_openapi(_op_id: &str, _path: &str) -> Vec<ParameterDoc> {
        vec![utoipa::openapi::request_body::RequestBodyBuilder::new()
            .content(
                "application/json",
                utoipa::openapi::ContentBuilder::new()
                    .schema(Some(match &T::schema_name()[..] {
                        "Request" => T::ref_or_schema(),
                        name => utoipa::openapi::Ref::from_schema_name(name).into(),
                    }))
                    .build(),
            )
            // .name("body")
            // .parameter_in(utoipa::openapi::path::ParameterIn::Path)
            // .required(utoipa::openapi::Required::True)
            .build()
            .into()]
    }
}

impl<T> DocumentedParameter for axum::extract::Query<T>
where
    T: utoipa::IntoParams,
{
    fn to_openapi(_op_id: &str, _path: &str) -> Vec<ParameterDoc> {
        T::into_params(|| Some(openapi::path::ParameterIn::Query))
            .into_iter()
            .map(|par| par.into())
            .collect()
        /* match T::schema().1 {
            openapi::Schema::Object(obj) => {}
            openapi::Schema::Array(_) => panic!(
                "{} is an Array schema: not allowed as Query paramter",
                std::any::type_name::<T>()
            ),
            openapi::Schema::OneOf(_) => panic!(
                "{} is an OneOf schema: not allowed as Query paramter",
                std::any::type_name::<T>()
            ),
            _ => todo!(),
        }
        vec![openapi::path::ParameterBuilder::new().schema({
            .schema(match T::ref_or_schema() {
                openapi::RefOr::T(schema) => {
                    if T::schema_name() == "Request" {
                        schema.into()
                    } else {
                        utoipa::openapi::Ref::from_schema_name(T::schema_name().to_string())
                            .into()
                    }
                }
                ref_or => ref_or,
            })
        })
            // .name("body")
            // .parameter_in(utoipa::openapi::path::ParameterIn::Path)
            // .required(utoipa::openapi::Required::True)
            .build()
            .into()] */
    }
}

impl<T> DocumentedParameter for Option<T>
where
    T: DocumentedParameter,
{
    const HAS_BEARER: bool = T::HAS_BEARER;
    fn to_openapi(op_id: &str, path: &str) -> Vec<ParameterDoc> {
        let mut vec = T::to_openapi(op_id, path);
        for param in &mut vec {
            match param {
                ParameterDoc::Param(param) => {
                    param.required = openapi::Required::False;
                }
                ParameterDoc::Body(body) => {
                    body.required = Some(openapi::Required::False);
                }
            }
        }
        vec
    }
}
impl DocumentedParameter for () {
    fn to_openapi(_op_id: &str, _path: &str) -> Vec<ParameterDoc> {
        vec![]
    }
}

impl DocumentedParameter for DiscardBody {
    fn to_openapi(_op_id: &str, _path: &str) -> Vec<ParameterDoc> {
        vec![]
    }
}

impl<T> DocumentedParameter for (T,)
where
    T: DocumentedParameter,
{
    const HAS_BEARER: bool = T::HAS_BEARER;
    fn to_openapi(op_id: &str, path: &str) -> Vec<ParameterDoc> {
        T::to_openapi(op_id, path)
    }
}

impl<T1, T2> DocumentedParameter for (T1, T2)
where
    T1: DocumentedParameter,
    T2: DocumentedParameter,
{
    const HAS_BEARER: bool = T1::HAS_BEARER | T2::HAS_BEARER;
    fn to_openapi(op_id: &str, path: &str) -> Vec<ParameterDoc> {
        let mut vec = T1::to_openapi(op_id, path);
        vec.append(&mut T2::to_openapi(op_id, path));
        vec
    }
}

impl<T1, T2, T3> DocumentedParameter for (T1, T2, T3)
where
    T1: DocumentedParameter,
    T2: DocumentedParameter,
    T3: DocumentedParameter,
{
    const HAS_BEARER: bool = T1::HAS_BEARER | T2::HAS_BEARER | T3::HAS_BEARER;
    fn to_openapi(op_id: &str, path: &str) -> Vec<ParameterDoc> {
        let mut vec = T1::to_openapi(op_id, path);
        vec.append(&mut T2::to_openapi(op_id, path));
        vec.append(&mut T3::to_openapi(op_id, path));
        vec
    }
}

/// (description, example)
pub type ErrorResponse<Err> = (&'static str, Err);

pub trait ErrorResp {
    fn error_responses() -> Vec<(StatusCode, String)>;
}

pub trait DocumentedEndpoint: HttpEndpoint + Sized
where
    Self::Response: ToRefOrSchema,
    Self::Error: ToRefOrSchema + serde::Serialize + ErrorResp,
    for<'a> &'a Self::Error: Into<StatusCode>,
    Self::HttpRequest: DocumentedParameter,
{
    const TAG: &'static Tag = &DEFAULT_TAG;
    const SUMMARY: &'static str = "";
    const DESCRIPTION: &'static str = "";
    const SUCCESS_DESCRIPTION: &'static str = "";
    const DEPRECATED: bool = false;

    /// By default, this calls [`utils::type_name_raw`] on `Self`.
    fn id() -> &'static str {
        type_name_raw::<Self>()
    }

    /// Provide examples to be used for the success response
    /// generated by [`success_responses`]. These examples will
    /// be used for a single Response object keyed under [`HttpEndpoint::SUCCESS_CODE`]
    fn success_examples() -> Vec<serde_json::Value> {
        vec![]
    }

    /// Read at `success_examples` for the default behavior.
    fn success_responses() -> Vec<(String, openapi::Response)> {
        let response = if Self::Response::schema_name() != type_name_raw::<NoContent>() {
            openapi::ResponseBuilder::new().content("application/json", {
                let mut schema = match Self::Response::ref_or_schema() {
                    // if it's a `Ref`, use the `schema_name`
                    openapi::RefOr::Ref(_) => openapi::ContentBuilder::new().schema(Some(
                        openapi::Ref::from_schema_name(Self::Response::schema_name()),
                    )),
                    // else, assume generic name
                    openapi::RefOr::T(schema) => {
                        openapi::ContentBuilder::new()
                            // .schema(utoipa::openapi::Ref::from_schema_name(
                            //     format!("{id}Response"),
                            // ))
                            .schema(Some(schema))
                    }
                };
                for example in Self::success_examples() {
                    schema = schema.example(Some(serde_json::to_value(example).unwrap()))
                }
                schema.build()
            })
        } else {
            openapi::ResponseBuilder::new()
        };

        let response = if !Self::SUCCESS_DESCRIPTION.is_empty() {
            response.description(Self::SUCCESS_DESCRIPTION)
        } else {
            response
        };
        let response = response.build();
        vec![(Self::SUCCESS_CODE.as_u16().to_string(), response)]
    }

    /// Besides what's stated in the doc of [`errors`], the default impl assumes that
    /// the `Error` type schema is registered as a component under `EndpointIdError`
    /// endpoint id coming from [`DocumentedEndpoint::id`]
    fn error_responses() -> Vec<(String, openapi::Response)> {
        let id = Self::id();
        struct ResponseSummary {
            descs: Vec<String>,
            examples: Vec<(String, serde_json::Value)>,
        }
        Self::Error::error_responses()
            .into_iter()
            .fold(std::collections::HashMap::new(), |mut out, (code, desc)| {
                let summary = out.entry(code.as_u16()).or_insert_with(|| ResponseSummary {
                    descs: default(),
                    examples: vec![],
                });
                summary.descs.push(desc.clone());
                // summary
                //     .examples
                //     .push((desc.to_owned(), serde_json::to_value(example).unwrap()));
                out
            })
            .into_iter()
            .map(|(code, summary)| {
                (
                    code.to_string(),
                    openapi::ResponseBuilder::new()
                        .description(summary.descs.into_iter().fold(String::new(), |out, desc| {
                            if out.is_empty() {
                                desc
                            } else {
                                format!("{out} | {desc}")
                            }
                        }))
                        .content("application/json", {
                            openapi::ContentBuilder::new()
                                .schema(Some(utoipa::openapi::Ref::from_schema_name(format!(
                                    "{id}Error"
                                ))))
                                .examples_from_iter(summary.examples.into_iter().map(
                                    |(desc, value)| {
                                        (
                                            desc.clone(),
                                            openapi::example::ExampleBuilder::new()
                                                .summary(desc)
                                                .value(Some(value))
                                                .build(),
                                        )
                                    },
                                ))
                                .build()
                        })
                        .build(),
                )
            })
            .collect()
    }

    /// Makes use of [`success_responses`] and [`default_responses`].
    fn responses() -> openapi::Responses {
        let builder = openapi::ResponsesBuilder::new();
        let builder = builder.responses_from_iter(Self::success_responses());
        let builder = builder.responses_from_iter(Self::error_responses());
        builder.build()
    }

    fn paramters() -> (
        Option<openapi::request_body::RequestBody>,
        Vec<openapi::path::Parameter>,
    ) {
        let id = Self::id();
        let (params, bodies) = Self::HttpRequest::to_openapi(id, Self::PATH)
            .into_iter()
            .fold((vec![], vec![]), |(mut params, mut bodies), doc| {
                match doc {
                    ParameterDoc::Param(param) => {
                        params.push(*param);
                    }
                    ParameterDoc::Body(body) => {
                        bodies.push(*body);
                    }
                }
                (params, bodies)
            });
        assert!(bodies.len() < 2, "{id} has more than one Body ParameterDoc");
        (bodies.into_iter().next(), params)
    }

    fn operation() -> openapi::path::Operation {
        let id = Self::id();
        let (body, params) = Self::paramters();
        openapi::path::OperationBuilder::new()
            .operation_id(Some(id))
            .deprecated(Some(if Self::DEPRECATED {
                openapi::Deprecated::True
            } else {
                openapi::Deprecated::False
            }))
            .summary(if !Self::SUMMARY.is_empty() {
                Some(Self::SUMMARY)
            } else {
                None
            })
            .description(if !Self::DESCRIPTION.is_empty() {
                Some(Self::DESCRIPTION)
            } else {
                None
            })
            .tag(Self::TAG.name)
            .securities(if Self::HttpRequest::HAS_BEARER {
                Some([openapi::security::SecurityRequirement::new::<
                    &str,
                    [&str; 1usize],
                    &str,
                >("bearer", [""])])
            } else {
                None
            })
            .request_body(body)
            .parameters(Some(params.into_iter()))
            .responses(Self::responses())
            .build()
    }
    fn path_item() -> openapi::PathItem {
        openapi::PathItem::new(Self::METHOD, Self::operation())
    }

    /// Registers the [`Error`] type schema under `EndpointIdError` name using the
    /// id provided at [`DocumentedEndpoint::id`]
    fn default_components(builder: openapi::ComponentsBuilder) -> openapi::ComponentsBuilder {
        let id = Self::id();
        // let (_, bodies) = Self::Parameters::to_openapi(id, Self::PATH)
        //     .into_iter()
        //     .fold((vec![], vec![]), |(mut params, mut bodies), doc| {
        //         match doc {
        //             ParameterDoc::Param(param) => {
        //                 params.push(param);
        //             }
        //             ParameterDoc::Body(body) => {
        //                 bodies.push(body);
        //             }
        //         }
        //         (params, bodies)
        //     });
        [
            // (
            //     format!("{id}Response"),
            //     <Self::Response as ToRefOrSchema>::ref_or_schema(),
            // ),
            (
                format!("{id}Error"),
                <Self::Error as ToRefOrSchema>::ref_or_schema(),
            ),
        ]
        .into_iter()
        .fold(
            builder,
            |builder, (name, ref_or)| builder.schema(name, ref_or),
            //         match ref_or {
            //     // assume the component has been added elsewhere
            //     utoipa::openapi::RefOr::Ref(_) => builder,
            //     utoipa::openapi::RefOr::T(schema) => builder.schema(name, schema),
            // }
        )
    }

    fn components(builder: openapi::ComponentsBuilder) -> openapi::ComponentsBuilder {
        Self::default_components(builder)
    }
}

// pub struct DocParameterBuilder {
//     inner: utoipa::openapi::path::ParameterBuilder,
// }
// pub enum ParamExample<T> {
//     Query(T),
//     Path(T),
//     Header(T),
//     Cookie(T),
// }
// impl DocParameterBuilder {
//     pub fn new<T>(name: &'static str, example: ) -> Self {
//         Self {
//             inner: utoipa::openapi::path::ParameterBuilder::new().name(name)
//         }
//     }
//     pub fn build(self: Self) -> Parameter {
//         todo!()
//     }
// }

/// This is used to get around Rust orphaning rules. This allow us
/// to implement any foreign traits lik `axum::handler::Handler` for any `T`
/// that implements `Endpoint`
#[derive(educe::Educe)]
#[educe(Deref, DerefMut)]
pub struct EndpointWrapper<T> {
    inner: T,
}

impl<T> EndpointWrapper<T>
where
    T: HttpEndpoint + Clone + Send + Sized + 'static,
    T::Error: serde::Serialize,
    for<'a> &'a T::Error: Into<StatusCode>,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    // pub fn add_to_router<S>(self, router: axum::Router<S>) -> axum::Router<S>
    // where
    //     T::SharedCx: FromRef<S>,
    //     S: Clone + Send + Sync,
    // {
    //     use utoipa::openapi::PathItemType;
    //     let method = match T::METHOD {
    //         PathItemType::Get => axum::routing::get(self),
    //         PathItemType::Post => axum::routing::post(self),
    //         PathItemType::Put => axum::routing::put(self),
    //         PathItemType::Delete => axum::routing::delete(self),
    //         PathItemType::Options => axum::routing::options(self),
    //         PathItemType::Head => axum::routing::head(self),
    //         PathItemType::Patch => axum::routing::patch(self),
    //         PathItemType::Trace => axum::routing::trace(self),
    //         PathItemType::Connect => todo!(),
    //     };
    //     router.route(T::PATH, method)
    // }
}

impl<T> Clone for EndpointWrapper<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> axum::handler::Handler<T::Request, T::SharedCx> for EndpointWrapper<T>
where
    T: HttpEndpoint + Clone,
    T::Error: serde::Serialize,
    for<'a> &'a T::Error: Into<StatusCode>,
{
    type Future = std::pin::Pin<Box<dyn Future<Output = axum::response::Response> + Send>>;

    fn call(self, req: Request, cx: T::SharedCx) -> Self::Future {
        self.http(req, cx)
    }
}

impl<T> From<EndpointWrapper<T>> for axum::Router<T::SharedCx>
where
    T: HttpEndpoint + Clone,
    T::Error: serde::Serialize,
    for<'a> &'a T::Error: Into<StatusCode>,
{
    fn from(wrapper: EndpointWrapper<T>) -> Self {
        use utoipa::openapi::HttpMethod;
        let method = match T::METHOD {
            HttpMethod::Get => axum::routing::get(wrapper),
            HttpMethod::Post => axum::routing::post(wrapper),
            HttpMethod::Put => axum::routing::put(wrapper),
            HttpMethod::Delete => axum::routing::delete(wrapper),
            HttpMethod::Options => axum::routing::options(wrapper),
            HttpMethod::Head => axum::routing::head(wrapper),
            HttpMethod::Patch => axum::routing::patch(wrapper),
            HttpMethod::Trace => axum::routing::trace(wrapper),
            // HttpMethod::Connect => todo!(),
        };
        axum::Router::new().route(T::PATH, method)
        // wrapper.add_to_router(axum::Router::new())
    }
}

impl<T> utoipa::Path for EndpointWrapper<T>
where
    T: DocumentedEndpoint,
    T::Request: axum::extract::FromRequest<T::SharedCx, axum::body::Body>,
    T::Response: utoipa::ToSchema,
    T::Error: utoipa::ToSchema + serde::Serialize + ErrorResp,
    for<'a> &'a T::Error: Into<StatusCode>,
    T::HttpRequest: DocumentedParameter,
{
    fn methods() -> Vec<openapi::path::HttpMethod> {
        vec![<T as HttpEndpoint>::METHOD]
    }

    fn path() -> String {
        <T as HttpEndpoint>::PATH.into()
    }

    fn operation() -> openapi::path::Operation {
        <T as DocumentedEndpoint>::operation()
    }
}

pub struct AuthedUid(pub std::sync::Arc<str>);

impl<S> axum::extract::FromRequestParts<S> for AuthedUid
where
    S: Send + Sync,
{
    type Rejection = axum::response::Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        use axum_extra::{headers::Authorization, TypedHeader};
        let header =
            <TypedHeader<BearerToken> as axum::extract::FromRequestParts<S>>::from_request_parts(
                parts, state,
            )
            .await
            .map_err(|err| err.into_response())?;
        let TypedHeader(Authorization(token)) = header;
        Ok(Self(std::sync::Arc::from(token.token())))
    }

    /* async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
    } */
}

impl DocumentedParameter for AuthedUid {
    const HAS_BEARER: bool = true;
    fn to_openapi(_op_id: &str, _path: &str) -> Vec<ParameterDoc> {
        vec![]
    }
}

impl DocumentedParameter for axum_extra::TypedHeader<BearerToken> {
    const HAS_BEARER: bool = true;
    fn to_openapi(_op_id: &str, _path: &str) -> Vec<ParameterDoc> {
        vec![]
    }
}
