//! The HTTP client that performs the requests [`big_nodule::egress::authorize_net`] allowed, and
//! the second layer of the `#net` gate: every address a request actually dials is checked.
//!
//! `authorize_net` sees only the name in the URL, and a name a guest controls can resolve
//! anywhere: to `127.0.0.1`, to the cloud metadata address, or to a printer on the LAN. So the
//! client's connector resolves the name itself and hands the HTTP client only the addresses the
//! [`EgressAddressPolicy`] permits. Those are the very addresses it then dials, so there is no
//! second resolution for a rebinding DNS server to answer differently. wash's own
//! `DefaultOutgoingHandler` has no hook between resolving and dialing for `wasi:http` (the policy
//! only guards its sockets and plugins), which is why this client exists instead of it.
//!
//! The policy is stricter than wash's default: private ranges (RFC 1918, unique-local, CGNAT)
//! are refused as well as loopback, link-local, the metadata address, unspecified, multicast and
//! documentation ranges, in every spelling (IPv4-mapped IPv6 included). On an end-user device the
//! LAN is as sensitive as localhost.
//!
//! The machine's own loopback is the one thing outside that policy a guest can be given, and
//! only by name: [`NetTarget::HOST_LOOPBACK`] resolves to `127.0.0.1` when the node's switch is
//! on, and a guest's own `127.0.0.1` or `localhost` never does.

use big_cartridges_core::config::NetTarget;
use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::combinators::UnsyncBoxBody;
use hyper_rustls::HttpsConnector;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::connect::dns::Name;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioTimer;
use std::collections::BTreeMap;
use std::error::Error as _;
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use wash_runtime::host::egress_policy::EgressAddressPolicy;
use wash_runtime::host::http::RequestIoFuture;
use wash_runtime::host::http::SendResult;
use wasmtime_wasi_http::Error as WasiHttpError;
use wasmtime_wasi_http::RequestOptions;
use wasmtime_wasi_http::WasiBody;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// What wasi:http leaves unbounded: the budget wash's own client gives a request that sets none.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(600);

/// How long an idle keep-alive connection is kept, as in wash's pools.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

/// The destination a request named resolved only to addresses the policy refuses, or is one.
#[derive(Debug, thiserror::Error)]
#[error("the network policy refuses `{host}`, which is or resolves only to {addresses:?}")]
pub(crate) struct Refused {
    host: String,
    addresses: Vec<IpAddr>,
}

/// Resolves a name for the HTTP client's connector and keeps only the addresses the policy
/// permits. A name with none left is [`Refused`].
#[derive(Clone)]
struct CheckedResolver {
    policy: EgressAddressPolicy,
    /// Whether [`NetTarget::HOST_LOOPBACK`] resolves to the machine's loopback.
    host_loopback: bool,
}

impl tower::Service<Name> for CheckedResolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let policy = self.policy;
        let host_loopback = self.host_loopback;
        Box::pin(async move {
            // The sentinel never reaches DNS. It is the machine's loopback when the node says so,
            // the one range the policy would refuse, and refused like any other when it does not.
            // `authorize_net` has already required the cartridge's grant for the port.
            if name.as_str() == NetTarget::HOST_LOOPBACK {
                return if host_loopback {
                    Ok(vec![SocketAddr::from(([127, 0, 0, 1], 0))].into_iter())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        Refused {
                            host: name.as_str().to_owned(),
                            addresses: Vec::new(),
                        },
                    ))
                };
            }
            // The connector replaces the port of every address it is handed.
            let resolved: Vec<SocketAddr> =
                tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            if resolved.is_empty() {
                return Err(io::ErrorKind::AddrNotAvailable.into());
            }
            let permitted: Vec<SocketAddr> = resolved
                .iter()
                .copied()
                .filter(|address| policy.permits(address.ip()))
                .collect();
            if permitted.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    Refused {
                        host: name.as_str().to_owned(),
                        addresses: resolved.iter().map(SocketAddr::ip).collect(),
                    },
                ));
            }
            Ok(permitted.into_iter())
        })
    }
}

type PooledClient =
    Client<HttpsConnector<HttpConnector<CheckedResolver>>, UnsyncBoxBody<Bytes, BoxError>>;

/// The HTTP clients of the running workloads, one pool each: connection-scoped server state
/// (cookies tied to a connection, rate-limit attribution) never crosses from one nodule to
/// another.
pub(crate) struct NetClients {
    policy: EgressAddressPolicy,
    host_loopback: bool,
    clients: Mutex<BTreeMap<String, PooledClient>>,
}

impl NetClients {
    /// `host_loopback` is the node's switch for [`NetTarget::HOST_LOOPBACK`].
    pub(crate) fn new(host_loopback: bool) -> Self {
        Self {
            host_loopback,
            // Everything that is not the public internet: wash's default refuses the machine
            // and its link, this adds the private ranges.
            policy: EgressAddressPolicy {
                deny_special: true,
                allow_private: false,
            },
            clients: Mutex::default(),
        }
    }

    fn client(&self, workload_id: &str) -> PooledClient {
        self.clients
            .lock()
            .expect("net clients lock")
            .entry(workload_id.to_owned())
            .or_insert_with(|| {
                wash_runtime::init_crypto();
                let mut http = HttpConnector::new_with_resolver(CheckedResolver {
                    policy: self.policy,
                    host_loopback: self.host_loopback,
                });
                // The HTTPS connector wraps this one and sees https URIs.
                http.enforce_http(false);
                http.set_nodelay(true);
                let connector = HttpsConnectorBuilder::new()
                    .with_tls_config(
                        wash_runtime::host::http_client::default_client_tls_config()
                            .as_ref()
                            .clone(),
                    )
                    .https_or_http()
                    .enable_http1()
                    .wrap_connector(http);
                Client::builder(TokioExecutor::new())
                    .pool_timer(TokioTimer::new())
                    .pool_idle_timeout(POOL_IDLE_TIMEOUT)
                    .build(connector)
            })
            .clone()
    }

    /// Drops the workload's connections, as it is stopped.
    pub(crate) fn forget(&self, workload_id: &str) {
        self.clients
            .lock()
            .expect("net clients lock")
            .remove(workload_id);
    }

    /// Sends `request`, which `authorize_net` allowed. `Err` is a destination the address
    /// policy refused, which the caller answers as it answers `authorize_net`'s refusals; any
    /// other failure is the guest's transport error in the `Ok` result, as wasi:http defines it.
    pub(crate) async fn send(
        &self,
        workload_id: &str,
        request: hyper::Request<WasiBody>,
        options: Option<RequestOptions>,
    ) -> Result<SendResult, Refused> {
        // An IP literal is not resolved, so the connector never asks the resolver about it.
        if let Some(host) = request.uri().host() {
            let literal = host.trim_start_matches('[').trim_end_matches(']');
            if let Ok(address) = literal.parse::<IpAddr>()
                && !self.policy.permits(address)
            {
                return Err(Refused {
                    host: host.to_owned(),
                    addresses: vec![address],
                });
            }
        }
        let client = self.client(workload_id);
        let budget = |pick: fn(&RequestOptions) -> Option<Duration>| {
            options.as_ref().and_then(pick).unwrap_or(DEFAULT_TIMEOUT)
        };
        // One deadline for the connect and the response head together, where wash keeps two:
        // either is only ever a bound on a request that has stopped making progress.
        let head_budget = budget(|options| options.connect_timeout)
            .saturating_add(budget(|options| options.first_byte_timeout));
        let (parts, body) = request.into_parts();
        let body = body.map_err(BoxError::from).boxed_unsync();
        let sent = tokio::time::timeout(
            head_budget,
            client.request(hyper::Request::from_parts(parts, body)),
        )
        .await;
        let response = match sent {
            Err(_elapsed) => return Ok(Err(WasiHttpError::ConnectionReadTimeout)),
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                return match refusal(&error) {
                    Some(refused) => Err(refused),
                    None => Ok(Err(guest_error(&error))),
                };
            }
        };
        let response = response.map(|body| body.map_err(WasiHttpError::Hyper).boxed_unsync());
        // The upload's outcome is the request's own: a failure of the body is the send's error.
        let io: RequestIoFuture = Box::new(async { Ok(()) });
        Ok(Ok((response, io)))
    }
}

/// The [`Refused`] a failed send carries, whichever layer of the client wrapped it.
fn refusal(error: &(dyn std::error::Error + 'static)) -> Option<Refused> {
    let mut current = Some(error);
    while let Some(error) = current {
        let inner = error.downcast_ref::<Refused>().or_else(|| {
            error
                .downcast_ref::<io::Error>()
                .and_then(io::Error::get_ref)
                .and_then(|inner| inner.downcast_ref::<Refused>())
        });
        if let Some(refused) = inner {
            return Some(Refused {
                host: refused.host.clone(),
                addresses: refused.addresses.clone(),
            });
        }
        current = error.source();
    }
    None
}

/// The wasi:http error a transport failure is reported as.
fn guest_error(error: &hyper_util::client::legacy::Error) -> WasiHttpError {
    if !error.is_connect() {
        return WasiHttpError::HttpProtocolError;
    }
    let mut current = error.source();
    while let Some(source) = current {
        let text = source.to_string();
        if text.starts_with("dns error") || text.contains("failed to lookup address") {
            return WasiHttpError::DnsError {
                rcode: Some("address not available".to_owned()),
                info_code: Some(0),
            };
        }
        if source.is::<std::io::Error>() && text.to_ascii_lowercase().contains("certificate") {
            return WasiHttpError::TlsCertificateError;
        }
        current = source.source();
    }
    WasiHttpError::ConnectionRefused
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;

    fn request(url: &str) -> hyper::Request<WasiBody> {
        hyper::Request::get(url)
            .body(
                Full::new(Bytes::new())
                    .map_err(|never| match never {})
                    .boxed_unsync(),
            )
            .unwrap()
    }

    /// A destination is refused before anything is dialed, whether the URL names an address or a
    /// name that resolves to one (here `localhost`, with a listener on it that must never see a
    /// connection): the machine, its link and metadata, and every private range.
    #[tokio::test]
    async fn a_destination_in_a_refused_range_is_never_dialed() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let clients = NetClients::new(false);
        for url in [
            format!("http://localhost:{port}/"),
            format!("http://127.0.0.1:{port}/"),
            format!("http://[::1]:{port}/"),
            format!("http://[::ffff:127.0.0.1]:{port}/"),
            "http://169.254.169.254/latest/meta-data".to_owned(),
            "http://[fd00:ec2::254]/".to_owned(),
            "http://0.0.0.0:80/".to_owned(),
            "http://224.0.0.1/".to_owned(),
            "http://192.0.2.1/".to_owned(),
            // The private ranges, which wash's default policy allows.
            "http://10.0.0.5/".to_owned(),
            "http://172.16.0.1/".to_owned(),
            "http://192.168.1.1:8080/".to_owned(),
            "http://100.64.0.1/".to_owned(),
            "https://[fd12:3456::1]/".to_owned(),
        ] {
            match clients.send("workload", request(&url), None).await {
                Err(_) => {}
                Ok(_) => panic!("{url} was not refused"),
            }
        }
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "a refused destination was dialed"
        );
    }

    /// Public addresses pass the policy: only what the policy names is refused.
    #[test]
    fn a_public_address_is_permitted() {
        let policy = NetClients::new(false).policy;
        for address in ["93.184.216.34", "8.8.8.8", "2606:2800:220:1::1"] {
            assert!(policy.permits(address.parse().unwrap()), "{address}");
        }
    }
}
