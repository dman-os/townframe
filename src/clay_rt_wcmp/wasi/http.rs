//! `wasi:http/types` and `wasi:http/client`, under which every outgoing request is refused.
//!
//! The wcmp engine has no `dispatch.local` egress (`documents.rs` asserts a composition gives it
//! none), so it is in the position of a micro the Wasmtime runtime gave no egress: that
//! runtime's `egress` module answers every request with `http-request-denied`, and so does
//! `client.send` here. A guest that links `clay_mdk`'s node calls (`Context::call`, over
//! `big_ndk::WasiHttp`) imports these interfaces, so without them it fails at instantiation;
//! with them it loads, and each of its calls fails with the denial.
//!
//! The guest's path to `send` is the surface this module serves: a `fields` and a
//! `request-options` are made, headers are appended, the options' first-byte timeout is set, a
//! `request` is made from them, and its method, scheme, authority and path are set. Nothing a
//! request is built from is ever read, because nothing is sent, so the resources hold no state:
//! each handle's rep is `0`. Every other function of the two interfaces answers with
//! [`unsupported`](super::unsupported) when called, as for the rest of this WASI host.
//!
//! Function types are not restated here. The component declares them in the instances it
//! imports, so each function is defined with the type the component gives it, with the
//! component's own resource types replaced by the identities this module registers for them.

use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use std::collections::HashMap;

use wcmp::Accessor;
use wcmp::Component;
use wcmp::ComponentType;
use wcmp::Error;
use wcmp::ExternType;
use wcmp::FixedLengthListType;
use wcmp::FunctionParameter;
use wcmp::FunctionType;
use wcmp::FutureAny;
use wcmp::FutureProducer;
use wcmp::FutureReader;
use wcmp::FutureType;
use wcmp::HostCall;
use wcmp::Lift;
use wcmp::Linker;
use wcmp::ListType;
use wcmp::Lower;
use wcmp::MapType;
use wcmp::OptionType;
use wcmp::RecordField;
use wcmp::RecordType;
use wcmp::Resource;
use wcmp::ResourceType;
use wcmp::ResultType;
use wcmp::StoreContext;
use wcmp::StreamType;
use wcmp::TupleType;
use wcmp::Val;
use wcmp::ValueType;
use wcmp::VariantCase;
use wcmp::VariantType;

use super::identifier;
use super::unsupported;

const HTTP_TYPES: &str = "wasi:http/types@0.3.0";
const HTTP_CLIENT: &str = "wasi:http/client@0.3.0";

/// Marker types naming the host resource identities of `wasi:http/types`.
struct Fields;
struct Request;
struct Response;
struct RequestOptions;

/// The identity this module registers for the resource `name` of `wasi:http/types`.
fn identity(name: &str) -> ResourceType {
    match name {
        // `headers` and `trailers` are `type` aliases of `fields` in the WIT, but a component
        // built against it (rustc's wasm32-wasip3 std) lists each alias among its type items.
        "fields" | "headers" | "trailers" => ResourceType::host::<Fields>(),
        "request" => ResourceType::host::<Request>(),
        "response" => ResourceType::host::<Response>(),
        "request-options" => ResourceType::host::<RequestOptions>(),
        other => panic!("`{other}` is not a resource of `wasi:http/types`"),
    }
}

/// Define the `wasi:http` interfaces `component` imports on `linker`. An interface the component
/// does not import is left undefined.
pub(super) fn define<T: 'static>(
    linker: &mut Linker<T>,
    component: &Component,
) -> Result<(), Error> {
    let http_instances = || {
        component.imports.iter().filter_map(|import| {
            let name = import.name.to_string();
            match &import.ty {
                ExternType::Instance(instance)
                    if matches!(name.as_str(), HTTP_TYPES | HTTP_CLIENT) =>
                {
                    Some((name, instance))
                }
                _ => None,
            }
        })
    };
    // What each resource type the component declares is, here. `client` names `request` and
    // `response` from `types`, so they are the same declared type in both instances.
    let mut identities = HashMap::new();
    for (_, instance) in http_instances() {
        for item in &instance.items {
            if let ExternType::Resource(declared) | ExternType::ResourceEquals(declared) = &item.ty
            {
                identities.insert(*declared, identity(&item.name));
            }
        }
    }
    for (interface, instance) in http_instances() {
        let mut defined = linker.instance(&identifier(&interface));
        for item in &instance.items {
            match &item.ty {
                ExternType::Resource(_) | ExternType::ResourceEquals(_) => {
                    defined.resource(item.name.clone(), identity(&item.name), |_store, _rep| {
                        Ok(())
                    })?;
                }
                ExternType::Function(ty) if interface == HTTP_TYPES => {
                    let function = item.name.clone();
                    defined.func_new(
                        item.name.clone(),
                        host_function_type(&identities, ty),
                        move |mut call, args, results| {
                            call_types(&mut call, &function, args, results)
                        },
                    )?;
                }
                _ => {}
            }
        }
        if interface == HTTP_CLIENT {
            // Async-typed, so it is a concurrent registration even though the denial is ready
            // at once: the link check holds a registration's form to the import's `async`.
            defined.func_wrap_concurrent(
                "send",
                |_: &Accessor<T>, (_request,): (Resource<Request>,)| async {
                    Ok((Err::<Resource<Response>, _>(ErrorCode::HttpRequestDenied),))
                },
            )?;
        }
    }
    Ok(())
}

/// Run the function `function` of `wasi:http/types` the guest's request path calls, or answer
/// that this host does not provide it.
fn call_types<T: 'static>(
    call: &mut HostCall<'_, T>,
    function: &str,
    args: &[Val],
    results: &mut [Val],
) -> Result<(), Error> {
    // An `own` argument is the host's to release; nothing here keeps what a handle names.
    for arg in args {
        let handle = match arg {
            Val::Resource(handle) => Some(handle),
            Val::Option(Some(inner)) => match inner.as_ref() {
                Val::Resource(handle) => Some(handle),
                _ => None,
            },
            _ => None,
        };
        if let Some(handle) = handle.filter(|handle| handle.owned()) {
            handle.resource_drop(call.store())?;
        }
    }
    match function {
        "[constructor]fields" => {
            results[0] =
                Val::Resource(Resource::<Fields>::new_own(0).try_into_resource_any(call.store())?);
        }
        "[constructor]request-options" => {
            results[0] = Val::Resource(
                Resource::<RequestOptions>::new_own(0).try_into_resource_any(call.store())?,
            );
        }
        "[method]fields.append"
        | "[method]request-options.set-first-byte-timeout"
        | "[method]request.set-method"
        | "[method]request.set-scheme"
        | "[method]request.set-authority"
        | "[method]request.set-path-with-query" => results[0] = Val::Result(Ok(None)),
        "[static]request.new" => {
            let [_headers, contents, trailers, _options] = args else {
                return Err(Error::Internal {
                    message: format!("`request.new` was given {args:?}"),
                });
            };
            // The request is refused whatever its body, so the host stops reading the body
            // and trailers the guest handed it as it is made: the guest's pending writes then
            // end instead of waiting on a reader that does not exist.
            if let Val::Option(Some(stream)) = contents
                && let Val::Stream(stream) = stream.as_ref()
            {
                stream.clone().close(call.store())?;
            }
            if let Val::Future(trailers) = trailers {
                trailers.clone().close(call.store())?;
            }
            let request = Resource::<Request>::new_own(0).try_into_resource_any(call.store())?;
            let transmitted = FutureReader::new(call.store(), Untransmitted)?;
            let transmitted = FutureAny::try_from_future_reader(call.store(), transmitted)?;
            results[0] = Val::Tuple(Box::new([Val::Resource(request), Val::Future(transmitted)]));
        }
        other => return Err(unsupported(HTTP_TYPES, other)),
    }
    Ok(())
}

/// A request's completion future: it never resolves, since the request is never transmitted.
/// The guest dropping it is the `finish` poll.
struct Untransmitted;

impl<T: 'static> FutureProducer<T> for Untransmitted {
    type Item = Result<(), ErrorCode>;

    fn poll_produce(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _store: &mut StoreContext<'_, T>,
        finish: bool,
    ) -> Poll<Result<Option<Self::Item>, Error>> {
        if finish {
            Poll::Ready(Ok(None))
        } else {
            Poll::Pending
        }
    }
}

/// `function` with each resource type the component declares replaced by the identity this
/// module registers for it.
fn host_function_type(
    identities: &HashMap<ResourceType, ResourceType>,
    function: &FunctionType,
) -> FunctionType {
    FunctionType {
        parameters: function
            .parameters
            .iter()
            .map(|parameter| FunctionParameter {
                name: parameter.name.clone(),
                ty: host_value_type(identities, &parameter.ty),
            })
            .collect(),
        result: function
            .result
            .as_ref()
            .map(|result| host_value_type(identities, result)),
        async_: function.async_,
    }
}

fn host_value_type(identities: &HashMap<ResourceType, ResourceType>, ty: &ValueType) -> ValueType {
    let inner = |ty: &ValueType| host_value_type(identities, ty);
    let resource = |declared: &ResourceType| identities.get(declared).copied().unwrap_or(*declared);
    match ty {
        ValueType::Own(declared) => ValueType::Own(resource(declared)),
        ValueType::Borrow(declared) => ValueType::Borrow(resource(declared)),
        ValueType::Record(record) => ValueType::Record(RecordType::new(
            record
                .fields()
                .iter()
                .map(|field| RecordField::new(field.name(), inner(field.ty()))),
        )),
        ValueType::Variant(variant) => ValueType::Variant(VariantType::new(
            variant
                .cases()
                .iter()
                .map(|case| VariantCase::new(case.name(), case.payload().map(inner))),
        )),
        ValueType::List(list) => ValueType::List(ListType::new(inner(list.element()))),
        ValueType::Map(map) => ValueType::Map(MapType::new(inner(map.key()), inner(map.value()))),
        ValueType::FixedLengthList(list) => ValueType::FixedLengthList(FixedLengthListType::new(
            inner(list.element()),
            list.length(),
        )),
        ValueType::Option(option) => ValueType::Option(OptionType::new(inner(option.payload()))),
        ValueType::Result(result) => ValueType::Result(ResultType::new(
            result.ok().map(inner),
            result.err().map(inner),
        )),
        ValueType::Tuple(tuple) => {
            ValueType::Tuple(TupleType::new(tuple.elements().iter().map(inner)))
        }
        ValueType::Stream(stream) => {
            ValueType::Stream(StreamType::new(stream.payload().map(inner)))
        }
        ValueType::Future(future) => {
            ValueType::Future(FutureType::new(future.payload().map(inner)))
        }
        other => other.clone(),
    }
}

/// `wasi:http/types.error-code`, case by case and in order.
#[derive(ComponentType, Lift, Lower, Debug)]
#[component(variant)]
enum ErrorCode {
    #[component(name = "DNS-timeout")]
    DnsTimeout,
    #[component(name = "DNS-error")]
    DnsError(DnsErrorPayload),
    #[component(name = "destination-not-found")]
    DestinationNotFound,
    #[component(name = "destination-unavailable")]
    DestinationUnavailable,
    #[component(name = "destination-IP-prohibited")]
    DestinationIpProhibited,
    #[component(name = "destination-IP-unroutable")]
    DestinationIpUnroutable,
    #[component(name = "connection-refused")]
    ConnectionRefused,
    #[component(name = "connection-terminated")]
    ConnectionTerminated,
    #[component(name = "connection-timeout")]
    ConnectionTimeout,
    #[component(name = "connection-read-timeout")]
    ConnectionReadTimeout,
    #[component(name = "connection-write-timeout")]
    ConnectionWriteTimeout,
    #[component(name = "connection-limit-reached")]
    ConnectionLimitReached,
    #[component(name = "TLS-protocol-error")]
    TlsProtocolError,
    #[component(name = "TLS-certificate-error")]
    TlsCertificateError,
    #[component(name = "TLS-alert-received")]
    TlsAlertReceived(TlsAlertReceivedPayload),
    #[component(name = "HTTP-request-denied")]
    HttpRequestDenied,
    #[component(name = "HTTP-request-length-required")]
    HttpRequestLengthRequired,
    #[component(name = "HTTP-request-body-size")]
    HttpRequestBodySize(Option<u64>),
    #[component(name = "HTTP-request-method-invalid")]
    HttpRequestMethodInvalid,
    #[component(name = "HTTP-request-URI-invalid")]
    HttpRequestUriInvalid,
    #[component(name = "HTTP-request-URI-too-long")]
    HttpRequestUriTooLong,
    #[component(name = "HTTP-request-header-section-size")]
    HttpRequestHeaderSectionSize(Option<u32>),
    #[component(name = "HTTP-request-header-size")]
    HttpRequestHeaderSize(Option<FieldSizePayload>),
    #[component(name = "HTTP-request-trailer-section-size")]
    HttpRequestTrailerSectionSize(Option<u32>),
    #[component(name = "HTTP-request-trailer-size")]
    HttpRequestTrailerSize(FieldSizePayload),
    #[component(name = "HTTP-response-incomplete")]
    HttpResponseIncomplete,
    #[component(name = "HTTP-response-header-section-size")]
    HttpResponseHeaderSectionSize(Option<u32>),
    #[component(name = "HTTP-response-header-size")]
    HttpResponseHeaderSize(FieldSizePayload),
    #[component(name = "HTTP-response-body-size")]
    HttpResponseBodySize(Option<u64>),
    #[component(name = "HTTP-response-trailer-section-size")]
    HttpResponseTrailerSectionSize(Option<u32>),
    #[component(name = "HTTP-response-trailer-size")]
    HttpResponseTrailerSize(FieldSizePayload),
    #[component(name = "HTTP-response-transfer-coding")]
    HttpResponseTransferCoding(Option<String>),
    #[component(name = "HTTP-response-content-coding")]
    HttpResponseContentCoding(Option<String>),
    #[component(name = "HTTP-response-timeout")]
    HttpResponseTimeout,
    #[component(name = "HTTP-upgrade-failed")]
    HttpUpgradeFailed,
    #[component(name = "HTTP-protocol-error")]
    HttpProtocolError,
    #[component(name = "loop-detected")]
    LoopDetected,
    #[component(name = "configuration-error")]
    ConfigurationError,
    #[component(name = "internal-error")]
    InternalError(Option<String>),
}

#[derive(ComponentType, Lift, Lower, Debug)]
#[component(record)]
struct DnsErrorPayload {
    rcode: Option<String>,
    #[component(name = "info-code")]
    info_code: Option<u16>,
}

#[derive(ComponentType, Lift, Lower, Debug)]
#[component(record)]
struct TlsAlertReceivedPayload {
    #[component(name = "alert-id")]
    alert_id: Option<u8>,
    #[component(name = "alert-message")]
    alert_message: Option<String>,
}

#[derive(ComponentType, Lift, Lower, Debug)]
#[component(record)]
struct FieldSizePayload {
    #[component(name = "field-name")]
    field_name: Option<String>,
    #[component(name = "field-size")]
    field_size: Option<u32>,
}
