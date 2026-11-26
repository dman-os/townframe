//! What the server functions read from: the node the component's environment names, or the
//! built-in fixture when it names none. This is the only place that knows; the page does not.

use crate::app::PageError;
use crate::node::ConfigError;
use crate::source::FixtureSource;
use crate::source::IntxtSource;
use crate::source::LoadError;
use crate::view::IntxtView;
use crate::view::Origin;
use big_cabinet_core::DocId;
use big_cabinet_core::dmeta::MultibaseKey;

#[cfg(target_os = "wasi")]
use crate::node::NodeSource;
#[cfg(target_os = "wasi")]
use crate::wasi_http::WasiHttp;

/// The configured source: all three variables unset serves the fixture; only some set is a
/// misconfiguration, reported on every page.
enum Backend {
    Fixture,
    #[cfg(target_os = "wasi")]
    Node(NodeSource<WasiHttp>),
}

impl IntxtSource for Backend {
    async fn load(&self, drawer: &DocId, via: &[MultibaseKey]) -> Result<IntxtView, LoadError> {
        match self {
            Self::Fixture => FixtureSource.load(drawer, via).await,
            #[cfg(target_os = "wasi")]
            Self::Node(node) => node.load(drawer, via).await,
        }
    }

    fn origin(&self) -> Origin {
        match self {
            Self::Fixture => FixtureSource.origin(),
            #[cfg(target_os = "wasi")]
            Self::Node(node) => node.origin(),
        }
    }
}

#[cfg(target_os = "wasi")]
fn environment(name: &str) -> Option<String> {
    crate::wasi_http::environment(name)
}

// Off the component there is no node transport, so only the fixture can be served.
#[cfg(not(target_os = "wasi"))]
fn environment(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

fn misconfigured(message: String) -> PageError {
    PageError {
        status: 500,
        message,
    }
}

fn backend() -> Result<Backend, PageError> {
    let config = crate::node::NodeConfig::from_env(environment)
        .map_err(|error: ConfigError| misconfigured(error.to_string()))?;
    let Some(config) = config else {
        return Ok(Backend::Fixture);
    };
    #[cfg(target_os = "wasi")]
    {
        let base = crate::node::BaseUrl::parse(&config.node)
            .map_err(|error| misconfigured(format!("{}: {error}", crate::node::NODE_VAR)))?;
        Ok(Backend::Node(NodeSource::new(WasiHttp::new(base), config)))
    }
    #[cfg(not(target_os = "wasi"))]
    {
        let _ = config;
        Err(misconfigured(
            "a node can only be read from the wasi component".to_owned(),
        ))
    }
}

fn bad_request(message: String) -> PageError {
    PageError {
        status: 400,
        message: format!("bad request: {message}"),
    }
}

/// `via=a|b`, the value of the hint. Empty means no hint.
fn parse_via(via: &str) -> Result<Vec<MultibaseKey>, PageError> {
    via.split('|')
        .filter(|key| !key.is_empty())
        .map(|key| {
            key.parse()
                .map_err(|error| bad_request(format!("via {key:?}: {error}")))
        })
        .collect()
}

/// The intxt in `drawer`, read through `via`. Loading finishes before anything is rendered, so a
/// failure is an error page with its own status, never a half page.
pub async fn load(drawer: &str, via: &str) -> Result<IntxtView, PageError> {
    let drawer: DocId = drawer
        .parse()
        .map_err(|error| bad_request(format!("drawer id: {error}")))?;
    let via = parse_via(via)?;
    backend()?
        .load(&drawer, &via)
        .await
        .map_err(|error| PageError {
            status: error.status(),
            message: error.to_string(),
        })
}

pub async fn origin() -> Result<Origin, PageError> {
    Ok(backend()?.origin())
}
