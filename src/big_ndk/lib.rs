//! The nodule development kit: what a nodule author depends on.
//!
//! A nodule is a Wasm component serving XRPC methods as `wasi:http/handler` requests. The author
//! declares its methods with [`method_core::methods!`], implements [`Handler`] for each on one
//! type and exports them with [`export_nodule!`]:
//!
//! ```ignore
//! method_core::methods! {
//!     error = ReviewError, nsid_prefix = "org.example.intxt.";
//!     CreateReview => ReviewCreated, procedure, "createReview";
//! }
//!
//! #[derive(Default)]
//! struct Intxt;
//!
//! impl big_ndk::Handler<CreateReview> for Intxt {
//!     async fn call(&self, cx: &big_ndk::Cx, input: CreateReview) -> Result<ReviewCreated, ReviewError> {
//!         let receipt = cx.call(CreateDoc { /* .. */ }).await?;
//!         Ok(ReviewCreated { review: receipt.doc_id })
//!     }
//! }
//!
//! big_ndk::export_nodule!(Intxt => [CreateReview]);
//! ```
//!
//! The wire contract is the one every XRPC hop speaks (`service_core_rs::xrpc`): the host
//! delivers `authorization: Bearer <caller capability id>`, `idempotency-key`, the remaining
//! budget as a relative `x-timeout-ms`, `x-request-id` and `traceparent`/`tracestate`, plus
//! `x-nodule-capability` (the nodule's own dispatch capability as a bare id). [`Cx`] is those
//! headers, parsed once. Outgoing calls go to `http://dispatch.local` over `wasi:http/client`
//! ([`WasiHttp`]) with the nodule's own capability.
//!
//! The kit serves unary procedures only: methods with a query-string input and subscriptions
//! are refused at compile time by [`export_nodule!`].

mod cx;
mod serve;
mod wasi;

pub use cx::Cx;
pub use cx::HttpHandle;
pub use method_core;
pub use serve::Nodule;
pub use serve::Reply;
pub use serve::invoke;
pub use serve::serve;
pub use wasi::WasiHttp;
pub use wasi::serve_wasi;

/// The `wasi:http` bindings every nodule component is built on: its world imports the client and
/// exports the handler. Public so [`export_nodule!`] can name them from the author's crate.
#[doc(hidden)]
pub mod wit {
    wit_bindgen::generate!({
        path: "wit",
        world: "nodule",
        generate_all,
        async: ["export:wasi:http/handler@0.3.0#handle"],
        pub_export_macro: true,
        default_bindings_module: "big_ndk::wit",
    });
}

use method_core::Unary;
use std::future::Future;

/// Serves one method `M` for a nodule: the server side of [`method_core::Method`]. Unlike
/// `service_utils_rs::Handler` it has no `Send` or `Sync` bounds, because a wasm guest's futures
/// are not `Send`. The future carries no `Send` bound for the same reason
/// `service_client_core_rs::UnaryTransport`'s does.
pub trait Handler<M: Unary> {
    fn call(&self, cx: &Cx, input: M) -> impl Future<Output = Result<M::Output, M::Error>>;
}

/// Exports a nodule as the component's `wasi:http/handler@0.3.0`.
///
/// `export_nodule!(Type => [MethodA, MethodB])` routes `POST /xrpc/<nsid>` to the
/// [`Handler`] impl of `Type` for the method whose `XRPC.nsid` is `<nsid>` exactly, and
/// requires `Type: Default`: a fresh value serves each request, because a component is
/// stateless between requests and durable state goes through services. `export_nodule!(Type)` exports
/// a `Type` that implements [`Nodule`] by hand, for a nodule whose routing is not an exact NSID
/// match.
#[macro_export]
macro_rules! export_nodule {
    ($nodule:ty => [$($method:ty),* $(,)?]) => {
        impl $crate::Nodule for $nodule {
            fn route(
                &self,
                nsid: &str,
                cx: &$crate::Cx,
                body: &[u8],
            ) -> impl ::core::future::Future<Output = ::core::option::Option<$crate::Reply>> {
                async move {
                    $(
                        if nsid == <$method as $crate::method_core::Method>::XRPC.nsid {
                            return ::core::option::Option::Some(
                                $crate::invoke::<Self, $method>(self, cx, body).await,
                            );
                        }
                    )*
                    ::core::option::Option::None
                }
            }
        }
        $crate::export_nodule!($nodule);
    };
    ($nodule:ty) => {
        const _: () = {
            struct NoduleExport;
            impl $crate::wit::exports::wasi::http::handler::Guest for NoduleExport {
                async fn handle(
                    request: $crate::wit::wasi::http::types::Request,
                ) -> ::core::result::Result<
                    $crate::wit::wasi::http::types::Response,
                    $crate::wit::wasi::http::types::ErrorCode,
                > {
                    $crate::serve_wasi::<$nodule>(request).await
                }
            }
            $crate::wit::export!(NoduleExport with_types_in $crate::wit);
        };
    };
}

#[cfg(test)]
mod tests;
