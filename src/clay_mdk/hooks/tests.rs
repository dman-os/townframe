use super::*;
use crate::Collection;
use clay_collection::WriteBasis;
use clay_guest_session::capabilities::CapabilityLimits;
use clay_guest_session::capabilities::CapabilitySession;
use clay_guest_session::collections::CollectionLimits;
use clay_guest_session::collections::CollectionSession;
use clay_guest_session::collections::bytes_registrations;
use clay_host::Host;
use clay_host::MicroId;
use std::future::Future;
use std::num::NonZeroUsize;
use std::task::Context as TaskContext;
use std::task::Waker;

fn cbor(text: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(text, &mut bytes).unwrap();
    bytes
}

fn app(watch: Collection<String, String>) -> Element {
    let rows = use_rows(&watch);
    let text = rows
        .iter()
        .map(|row| format!("{}={}", row.key, row.value))
        .collect::<Vec<_>>()
        .join(",");
    rsx! { "[{text}]" }
}

/// Let the feed task and the Dioxus runtime make the progress they can, then render.
async fn settle(dom: &mut VirtualDom, tree: &mut clay_surface::SurfaceTree) {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    {
        let mut work = std::pin::pin!(dom.wait_for_work());
        let _pending = work
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()));
    }
    dom.render_immediate(tree);
}

#[tokio::test(flavor = "current_thread")]
async fn rows_hook_rerenders_on_each_change_and_hides_tombstones() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let host = Host::new();
            let instance = host.start("viewer");
            let registrations = bytes_registrations(NonZeroUsize::new(64).unwrap(), [0, 0]);
            let sessions =
                CollectionSession::new(host.clone(), instance.clone(), CollectionLimits::default());
            let capabilities =
                CapabilitySession::new(host.clone(), instance.clone(), CapabilityLimits::default());
            let writer = sessions
                .grant_writer(clay_host::Grant {
                    collection: registrations.clone(),
                    micro: MicroId::new("writer"),
                })
                .unwrap();
            let reader = sessions.grant(registrations.projection()).unwrap();
            let (watch, feed) = Collection::<String, String>::start(reader, capabilities);
            tokio::task::spawn_local(feed);

            let mut dom = VirtualDom::new_with_props(app, watch);
            let mut tree = clay_surface::SurfaceTree::new();
            dom.rebuild(&mut tree);
            settle(&mut dom, &mut tree).await;
            assert_eq!(tree.text_content(), "[]");

            writer
                .put(cbor("a"), WriteBasis::Concurrent, cbor("one"))
                .unwrap();
            settle(&mut dom, &mut tree).await;
            assert_eq!(tree.text_content(), "[a=one]");

            let stamp = writer
                .put(cbor("b"), WriteBasis::Concurrent, cbor("two"))
                .unwrap();
            writer
                .delete(cbor("b"), WriteBasis::Observed(stamp))
                .unwrap();
            settle(&mut dom, &mut tree).await;
            assert_eq!(tree.text_content(), "[a=one]");

            drop(dom);
            host.close();
        })
        .await;
}
