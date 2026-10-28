use super::*;
use clay_surface::NodeId;
use clay_surface::SurfaceTree;
use std::sync::Mutex;

struct RecordingSink(Mutex<Option<SurfaceTree<ComponentRef>>>);

impl RecordingSink {
    fn tree<R>(&self, read: impl FnOnce(&SurfaceTree<ComponentRef>) -> R) -> R {
        read(self.0.lock().unwrap().as_ref().expect("surface is live"))
    }
}

impl SurfaceSink<ComponentRef> for RecordingSink {
    fn is_live(&self) -> bool {
        self.0.lock().unwrap().is_some()
    }

    fn apply(&self, edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
        self.0
            .lock()
            .unwrap()
            .as_mut()
            .ok_or(PresentationError::Revoked)?
            .apply(edits)
    }

    fn withdraw(&self) {
        self.0.lock().unwrap().take();
    }
}

/// The counter component `x/check-dayframe.ts --prepare-only` builds, named by
/// `DAYFRAME_COUNTER_COMPONENT` as for `clay_rt_wasmtime`'s tests.
fn counter_bytes() -> Vec<u8> {
    let path = std::env::var("DAYFRAME_COUNTER_COMPONENT").expect(
        "DAYFRAME_COUNTER_COMPONENT must name the prepared counter component; run deno run --allow-all x/check-dayframe.ts --prepare-only",
    );
    std::fs::read(&path).unwrap_or_else(|error| panic!("reading {path}: {error}"))
}

#[tokio::test]
async fn counter_activates_and_mount_dispatch_unmount_complete_on_first_poll() {
    let package = WcmpPackage::load(&counter_bytes()).await.unwrap();
    let host = Host::new();
    let owner = host.start("counter");
    let runtime = WcmpRuntime::activate(&host, owner.clone(), &package)
        .await
        .unwrap();

    let sink = Arc::new(RecordingSink(Mutex::new(Some(SurfaceTree::new()))));
    let session = runtime.mount(Arc::clone(&sink) as _, &[]).unwrap();
    assert_eq!(sink.tree(SurfaceTree::text_content), "0");

    let dispatch = session.handler();
    for expected in ["1", "2"] {
        let (revision, element) = sink.tree(|tree| {
            (
                tree.revision(),
                tree.listener_id("button", "click")
                    .expect("counter button listens for clicks"),
            )
        });
        let response = dispatch(Event {
            revision,
            element: NodeId::try_from(element.raw()).unwrap(),
            name: "click".into(),
            bubbles: true,
            data_json: serde_json::to_string(&dioxus_html::SerializedMouseData::default()).unwrap(),
        });
        assert_eq!(response, EventResponse::default());
        assert_eq!(sink.tree(SurfaceTree::text_content), expected);
    }

    drop(session);
    assert!(!sink.is_live(), "unmount withdraws the surface");
    assert_eq!(
        host.check(&owner),
        Ok(()),
        "a clean unmount keeps the instance"
    );
    host.close();
}
