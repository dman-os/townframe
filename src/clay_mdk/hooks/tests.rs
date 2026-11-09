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
use std::num::NonZeroUsize;

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

/// Wait until the mirror holds `rows` rows, then drive the hook's own follower task until the
/// rendered text is `expected`. Both waits are woken by the change they wait for: the mirror's
/// `changed`, and the dirty scope the follower marks (`wait_for_work`); nothing polls.
async fn rendered(
    dom: &mut VirtualDom,
    tree: &mut clay_surface::SurfaceTree,
    watch: &Collection<String, String>,
    rows: usize,
    expected: &str,
) {
    let mut changes = watch.clone();
    while changes.rows().unwrap().len() != rows {
        changes.changed().await.unwrap();
    }
    loop {
        dom.render_immediate(tree);
        if tree.text_content() == expected {
            return;
        }
        dom.wait_for_work().await;
    }
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
                    writer: clay_collection::WriterId::new(
                        &clay_collection::ReplicaId::from_bytes(
                            [1; clay_collection::ReplicaId::LEN],
                        ),
                        "writer",
                    ),
                })
                .unwrap();
            let reader = sessions.grant(registrations.projection()).unwrap();
            let (watch, feed) = Collection::<String, String>::start(reader, capabilities);
            tokio::task::spawn_local(feed);

            let mut dom = VirtualDom::new_with_props(app, watch.clone());
            let mut tree = clay_surface::SurfaceTree::new();
            dom.rebuild(&mut tree);
            rendered(&mut dom, &mut tree, &watch, 0, "[]").await;

            writer
                .put(cbor("a"), WriteBasis::Concurrent, cbor("one"))
                .unwrap();
            rendered(&mut dom, &mut tree, &watch, 1, "[a=one]").await;

            // The tombstone is observed (and so hidden) before the render is checked: the
            // second row shows, then goes.
            let stamp = writer
                .put(cbor("b"), WriteBasis::Concurrent, cbor("two"))
                .unwrap();
            rendered(&mut dom, &mut tree, &watch, 2, "[a=one,b=two]").await;
            writer
                .delete(cbor("b"), WriteBasis::Observed(stamp))
                .unwrap();
            rendered(&mut dom, &mut tree, &watch, 1, "[a=one]").await;

            drop(dom);
            host.close();
        })
        .await;
}
