//! Authoring-surface fixture: a micro written only against [`clay_mdk::Context`]. The `context`
//! example exports it as a guest component with `export_micro!`, and the `clay_mdk` native
//! conformance run (`native::conformance_tests`) links the same source in-process, so every engine
//! runs one micro. It reads the collection bound to `feed`, registers one row of its own that
//! records what it read (so a host can tell a guest collection read from a guest registration
//! write), and serves a `rows` call that answers every row it can see once its own is among
//! them. A second call, `attach`, writes a row after `activate` returned that attaches the
//! declared presentation `detail` by name. Engine conformance runs it as `lifecycle_conformance::context_collections_and_calls`.
//!
//! It also follows another micro's presentation the way a slot does. The presentation `slot`
//! renders a [`PresentationSlot`] over the attachment of the `feed` row keyed `view`; the call
//! `follow` waits for that row, then redeems its attachment and reports each step (`mounted`
//! when a redemption succeeded and the provider incarnation it holds then ended as `revoked`,
//! `unavailable` for each failed redemption, after which it waits for the reference's status to
//! change). Conformance runs it as `lifecycle_conformance::context_follows_a_provider`, the
//! `clay_mdk` wasm tests mount `slot`.
//!
//! Its calls `hold` and `release` retain a row's call and drop that binding
//! (`lifecycle_conformance::context_retains_a_call`). Its call `probe` reports what the guest sees
//! through a row's attachment: the reference's status, then what a call and a `retain_call`
//! through it answer (`lifecycle_conformance::context_sees_a_closed_provider_revoked`).
//!
//! Two of these micros can depend on each other (`lifecycle_conformance::context_peers_*`): the
//! call `echo` answers its request, and the `feed` row keyed `init`, if one exists when `activate`
//! runs, is a call the micro makes before it finishes initializing, so an `init` row attaching a
//! peer's `echo` makes this micro's activation wait on the peer's.
//!
//! The lifecycle scenarios (`lifecycle_conformance::provider_consumer_graph` and the rest) pick
//! this micro's role from what it is granted, never from arguments:
//!
//! * the optional requirements `presentation` and `service` make it a consumer: it redeems the
//!   presentation and calls the service's `echo` while activating, and keeps the redeemed proxy
//!   in a [`Held`] owned by a task of its incarnation, and a second one in the `detail` view's
//!   closure (a view-held value); their `Drop`s run when the incarnation
//!   closes (ADR 030 §6). There is no close hook: cleanup is RAII on what the incarnation owns;
//! * the optional requirement `peer` is what its `relay` call forwards to (`relay:` plus the
//!   peer's answer);
//! * a `feed` row keyed `reject` makes `activate` fail after everything was published, which
//!   the host must discard;
//! * a `feed` row keyed `name` prefixes the keys this incarnation writes (`<name>/guest`), so
//!   several incarnations of the micro can share one collection and be told apart.
use clay_mdk::Access;
use clay_mdk::BindingResolution;
use clay_mdk::CallBinding;
use clay_mdk::Capability;
use clay_mdk::Collection;
use clay_mdk::Context;
use clay_mdk::Entrypoints;
use clay_mdk::Error;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::Nsid;
use clay_mdk::PresentationComponent;
use clay_mdk::PublicationDeclaration;
use clay_mdk::PublicationKind;
use clay_mdk::Registration;
use clay_mdk::Request;
use clay_mdk::RowAttachment;
use clay_mdk::hooks::PresentationSlot;
use clay_mdk::hooks::use_rows;
use dioxus::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;

/// The `feed` row whose attachment `slot` shows and `follow` waits for.
const VIEW: &str = "view";

pub fn manifest() -> MicroManifest {
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        publications: vec![
            PublicationDeclaration {
                name: "rows".into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal("org.example.clay.test.context.rows")),
            },
            PublicationDeclaration {
                name: "attach".into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal("org.example.clay.test.context.attach")),
            },
            PublicationDeclaration {
                name: "follow".into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal("org.example.clay.test.context.follow")),
            },
            PublicationDeclaration {
                name: "hold".into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal("org.example.clay.test.context.hold")),
            },
            PublicationDeclaration {
                name: "release".into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal("org.example.clay.test.context.release")),
            },
            PublicationDeclaration {
                name: "echo".into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal("org.example.clay.test.context.echo")),
            },
            PublicationDeclaration {
                name: "relay".into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal("org.example.clay.test.context.relay")),
            },
            PublicationDeclaration {
                name: "probe".into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal("org.example.clay.test.context.probe")),
            },
            PublicationDeclaration {
                name: "detail".into(),
                kind: PublicationKind::Presentation,
                contract: None,
            },
            PublicationDeclaration {
                name: "slot".into(),
                kind: PublicationKind::Presentation,
                contract: None,
            },
        ],
        requires: ["presentation", "service", "peer"]
            .map(|binding| Request {
                binding: binding.into(),
                contract: Nsid::literal(&format!("org.example.clay.test.context.{binding}")),
                access: None,
                necessity: Necessity::Optional,
                resolution: BindingResolution::Configured,
                min_revision: None,
            })
            .into(),
        configuration_requests: vec![],
        collection_contracts: vec![],
        collection_requests: vec![Request {
            binding: "feed".into(),
            contract: Nsid::literal("org.example.clay.test.context.feed"),
            access: Some(Access::Write),
            necessity: Necessity::Required,
            resolution: BindingResolution::Configured,
            min_revision: None,
        }],
        contributions: vec![],
        extra: Default::default(),
    }
}

/// The first attachment of the row `key`, once it exists.
async fn attachment_of(
    collection: &mut Collection<String, String>,
    key: &str,
) -> Result<Capability, Error> {
    loop {
        if let Some(row) = collection.rows()?.iter().find(|row| row.key == key) {
            return Ok(row.attachments[0].clone());
        }
        collection.changed().await?;
    }
}

/// A redeemed presentation the incarnation keeps for as long as it lives. Dropping it, which
/// the incarnation's close does while its dependencies are still retained (ADR 030 §6), finds
/// the provider's presentation still mountable: the sync `component()` is the observable the
/// host-side scenarios rely on, because a drop that ran after the provider was revoked fails
/// it, and a drop cannot return an error, so the failure is a crash.
struct Held(PresentationComponent);

impl Drop for Held {
    fn drop(&mut self) {
        self.0
            .component()
            .expect("the provider was withdrawn before its consumer closed");
    }
}

/// The call `follow`: redeems the attachment of the `feed` row it is named, reporting each step
/// as a `progress/<n>` row (see the module docs).
fn publish_follow(context: &Context, feed: &Registration<String, String>) -> Result<(), Error> {
    context.publish_call("follow", {
        let feed = feed.clone();
        move |key: String| {
            let feed = feed.clone();
            let mut collection = feed.collection().clone();
            async move {
                let reference = attachment_of(&mut collection, &key).await?;
                let mut steps = Vec::new();
                // Each step is also a row, so a host waiting on the collection learns the call
                // got there.
                let step = async |steps: &mut Vec<String>, name: &str| -> Result<(), Error> {
                    feed.put(
                        &format!("progress/{}", steps.len()),
                        &name.to_owned(),
                        Vec::new(),
                    )
                    .await?;
                    steps.push(name.to_owned());
                    Ok(())
                };
                loop {
                    // Read before redeeming: a change after the failure then still wakes the
                    // wait, where reading afterwards could miss it.
                    let before = reference.status();
                    match reference.redeem_presentation().await {
                        Ok(proxy) => {
                            proxy.component()?;
                            step(&mut steps, "mounted").await?;
                            proxy.revoked().await;
                            step(&mut steps, "revoked").await?;
                            return Ok(steps);
                        }
                        Err(_) => {
                            step(&mut steps, "unavailable").await?;
                            reference.status_changed(before).await;
                        }
                    }
                }
            }
        }
    })?;
    Ok(())
}

/// The calls `hold`, `probe` and `release` over the attachments of `collection`'s rows.
fn publish_retention(
    context: &Context,
    collection: &Collection<String, String>,
) -> Result<(), Error> {
    // `hold` retains the call the row `key` attaches and calls it twice (an echo), keeping the
    // binding until `release`, so a host can see the provider live past the call that took it
    // and then idle once the binding is dropped.
    let held = Rc::new(RefCell::new(None::<CallBinding>));
    context.publish_call("hold", {
        let collection = collection.clone();
        let held = Rc::clone(&held);
        move |key: String| {
            let mut collection = collection.clone();
            let held = Rc::clone(&held);
            async move {
                let binding = attachment_of(&mut collection, &key)
                    .await?
                    .retain_call()
                    .await?;
                let mut replies = Vec::new();
                for text in ["one", "two"] {
                    replies.push(binding.call::<String, String>(&text.to_owned()).await?);
                }
                *held.borrow_mut() = Some(binding);
                Ok(replies)
            }
        }
    })?;
    // `probe` reports the status of the row `key`'s attachment, then what a call and a
    // `retain_call` through it answer, each as the result's text: a reference the guest holds
    // must say `Revoked` and fail as `Revoked` once its provider incarnation closed, and neither
    // may start anything.
    context.publish_call("probe", {
        let collection = collection.clone();
        move |key: String| {
            let mut collection = collection.clone();
            async move {
                let reference = attachment_of(&mut collection, &key).await?;
                let status = format!("{:?}", reference.status());
                let call = reference.call::<String, String>(&"probe".to_owned()).await;
                let retained = reference.retain_call().await.map(drop);
                Ok(vec![
                    status,
                    format!("{:?}", call.as_ref().map(drop)),
                    format!("{retained:?}"),
                ])
            }
        }
    })?;
    context.publish_call("release", move |(): ()| {
        held.borrow_mut().take();
        async { Ok(()) }
    })?;
    Ok(())
}

pub async fn activate(context: Context) -> Result<(), Error> {
    let feed = context
        .registration::<String, String>("feed")
        .expect("the feed collection is bound");
    feed.collection().bootstrapped().await;
    let rows = feed.collection().rows()?;
    let seen = rows
        .iter()
        .map(|row| format!("{}={}", row.key, row.value))
        .collect::<Vec<_>>()
        .join(",");
    let guest = rows
        .iter()
        .find(|row| row.key == "name")
        .map_or_else(|| "guest".to_owned(), |row| format!("{}/guest", row.value));
    feed.put(&guest, &seen, Vec::new()).await?;
    let reject = rows.iter().any(|row| row.key == "reject");
    // A consumer redeems its provider's presentation before it finishes initializing, and
    // calls the service if it has one; a failure refuses the activation.
    let mut shown = None;
    if let Some(presentation) = context.require("presentation") {
        let proxy = presentation.redeem_presentation().await?;
        // A second proxy is captured by the `detail` view below: views are dropped by the close
        // too, so it is dropped with the provider still live as well.
        shown = Some(Held(presentation.redeem_presentation().await?));
        proxy.component()?;
        if let Some(service) = context.require("service") {
            let answer: String = service.call(&"ping".to_owned()).await?;
            if answer != "ping" {
                return Err(Error::Rejected(format!("service answered {answer:?}")));
            }
        }
        // Owned by a task of the incarnation: closing drops the task and with it the proxy.
        context.spawn(async move {
            let _held = Held(proxy);
            std::future::pending::<()>().await;
        });
    }
    let collection = feed.collection().clone();
    // An `init` row is a call this activation makes and waits for: through a lazy reference it
    // demands the peer's activation, which is how two micros come to wait on each other.
    let init = collection
        .rows()?
        .iter()
        .find(|row| row.key == "init")
        .map(|row| row.attachments[0].clone());
    if let Some(peer) = init {
        peer.call::<String, String>(&"init".to_owned()).await?;
    }
    context.publish_call("echo", |request: String| async move { Ok(request) })?;
    // `relay` forwards its request to the `peer` requirement and answers `relay:` plus the
    // peer's answer; a handler in flight holds its own share of the reference.
    let peer = context.require("peer").cloned();
    context.publish_call("relay", move |request: String| {
        let peer = peer.clone();
        async move {
            let peer = peer
                .ok_or_else(|| Error::Rejected("the relay has no peer requirement".to_owned()))?;
            let answer: String = peer.call(&request).await?;
            Ok(format!("relay:{answer}"))
        }
    })?;
    context.publish_component("detail", move || {
        let _ = &shown;
        dioxus_core::VNode::empty()
    })?;
    context.publish_component("slot", {
        let collection = collection.clone();
        move || {
            let rows = use_rows(&collection);
            rsx! {
                for row in rows.iter().filter(|row| row.key == VIEW) {
                    PresentationSlot { key: "{row.key}", reference: row.attachments[0].clone() }
                }
            }
        }
    })?;
    publish_follow(&context, &feed)?;
    publish_retention(&context, &collection)?;
    // Runs long after `initialize` returned, when no handle to `detail` exists any more.
    context.publish_call("attach", move |publication: String| {
        let feed = feed.clone();
        async move {
            feed.put(
                &"late".to_owned(),
                &"late".to_owned(),
                vec![RowAttachment::own(publication)],
            )
            .await?;
            Ok(())
        }
    })?;
    context.publish_call("rows", move |(): ()| {
        let mut collection = collection.clone();
        let guest = guest.clone();
        async move {
            loop {
                let rows = collection.rows()?;
                // Our own row arrives through the same feed once the host commits it.
                if rows.iter().any(|row| row.key == guest) {
                    return Ok(rows
                        .iter()
                        .map(|row| (row.key.clone(), row.value.clone()))
                        .collect::<Vec<_>>());
                }
                collection.changed().await?;
            }
        }
    })?;
    // After everything was published: the host discards what a refused activation staged.
    if reject {
        return Err(Error::Rejected(
            "failing provider rejects its initialization".to_owned(),
        ));
    }
    Ok(())
}
