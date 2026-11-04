#![cfg(target_arch = "wasm32")]
//! Authoring-surface fixture: a guest written only against [`clay_mdk::Context`] and exported by
//! `export_micro!`. It reads the collection bound to `feed`, registers one row of its own that
//! records what it read (so a host can tell a guest collection read from a guest registration
//! write), and serves a `rows` call that answers every row it can see once its own is among
//! them. A second call, `attach`, writes a row after `activate` returned that attaches the
//! declared presentation `detail` by name. Engine conformance runs it as `lifecycle_conformance::context_collections_and_calls`.
use clay_mdk::Access;
use clay_mdk::BindingResolution;
use clay_mdk::Context;
use clay_mdk::Entrypoints;
use clay_mdk::Error;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::Nsid;
use clay_mdk::PublicationDeclaration;
use clay_mdk::PublicationKind;
use clay_mdk::Request;
use clay_mdk::RowAttachment;

fn manifest() -> MicroManifest {
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
                name: "detail".into(),
                kind: PublicationKind::Presentation,
                contract: None,
            },
        ],
        requires: vec![],
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

async fn activate(context: Context) -> Result<(), Error> {
    let feed = context
        .registration::<String, String>("feed")
        .expect("the feed collection is bound");
    feed.collection().bootstrapped().await;
    let seen = feed
        .collection()
        .rows()?
        .iter()
        .map(|row| format!("{}={}", row.key, row.value))
        .collect::<Vec<_>>()
        .join(",");
    feed.put(&"guest".to_owned(), &seen, Vec::new()).await?;
    let collection = feed.collection().clone();
    context.publish_component("detail", dioxus_core::VNode::empty)?;
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
        async move {
            loop {
                let rows = collection.rows()?;
                // Our own row arrives through the same feed once the host commits it.
                if rows.iter().any(|row| row.key == "guest") {
                    return Ok(rows
                        .iter()
                        .map(|row| (row.key.clone(), row.value.clone()))
                        .collect::<Vec<_>>());
                }
                collection.changed().await?;
            }
        }
    })?;
    Ok(())
}

clay_mdk::export_micro!(activate, manifest);
