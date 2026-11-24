use super::WcmpPackage;
use super::WcmpRuntime;
use clay_guest_session::capabilities::Contract;
use clay_guest_session::capabilities::DeclaredPublication;
use clay_host::ComponentRef;
use clay_host::Host;
use clay_host::MicroId;
use clay_rt_wcmp::InitialContext;
use clay_surface::Mutation;
use clay_surface::PresentationError;
use clay_surface::SurfaceSink;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

/// Collects the text the guest renders, and wakes a waiter on each batch.
struct TextSink(Mutex<Vec<String>>, tokio::sync::Notify);

impl SurfaceSink<ComponentRef> for TextSink {
    fn is_live(&self) -> bool {
        true
    }

    fn apply(&self, edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
        self.0
            .lock()
            .unwrap()
            .extend(edits.iter().filter_map(|edit| match edit {
                Mutation::CreateText(text) => Some(text.clone()),
                _ => None,
            }));
        self.1.notify_one();
        Ok(0)
    }

    fn withdraw(&self) {}
}

/// The browser engine hosts no `dispatch.local` egress, so every request a guest makes is
/// refused the way the Wasmtime runtime refuses a micro it gave no egress, and the guest loads:
/// the `dispatch` fixture imports `wasi:http` (its node calls and its raw request to another
/// origin go through `client.send`), which the engine once failed to link. Each attempt, the
/// activation's calls and the view's call alike, must come back as `HttpRequestDenied`.
#[tokio::test]
async fn every_outgoing_http_request_is_refused_as_denied() {
    let package = WcmpPackage::from_env("dispatch")
        .await
        .expect("the prepared dispatch component links");
    let host = Host::new();
    let owner = host.start("dispatch-fixture");
    let context = InitialContext {
        publications: BTreeMap::from([(
            "view".to_owned(),
            DeclaredPublication {
                contract: Contract::Presentation,
            },
        )]),
        micro: Some(MicroId::new("dispatch-fixture")),
        ..InitialContext::default()
    };
    let (runtime, driver) =
        WcmpRuntime::initialize(&host, owner.clone(), package.as_shared(), context)
            .await
            .unwrap_or_else(|error| panic!("the fixture activates: {error}"));
    let sink = Arc::new(TextSink(Mutex::new(Vec::new()), tokio::sync::Notify::new()));
    let session = runtime
        .mount("view", Arc::clone(&sink) as _)
        .unwrap_or_else(|error| {
            panic!(
                "the fixture mounts: {error:?}, fault {:?}",
                host.fault(&owner)
            )
        });
    // The `resource` line renders once the view's call answers, which takes the driver.
    let mut driver = Box::pin(driver);
    let rendered = async {
        loop {
            let notified = sink.1.notified();
            let texts = sink.0.lock().unwrap().clone();
            if texts.iter().any(|text| text.starts_with("resource=")) {
                return texts;
            }
            notified.await;
        }
    };
    let texts = tokio::select! {
        texts = rendered => texts,
        () = driver.as_mut() => panic!(
            "the incarnation ended before the view's call answered, fault {:?}",
            host.fault(&owner)
        ),
    };
    let attempts: BTreeMap<&str, &str> = texts
        .iter()
        .flat_map(|text| text.lines())
        .map(|line| line.split_once('=').expect("name=outcome"))
        .collect();
    for name in ["granted", "ungranted", "forged", "refused", "resource"] {
        let outcome = attempts[name];
        assert!(
            outcome.starts_with("err(") && outcome.contains("HttpRequestDenied"),
            "{name} is refused as a denied request: {outcome}"
        );
    }
    drop(session);
    drop(driver);
    host.close();
}
