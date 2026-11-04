//! The blob workers retain a nodule's handler artifact through the `Blob` datum the nodule
//! service keeps in the config document, and release it when the nodule is uninstalled.

use super::super::*;
use super::all_datums;
use super::blob_cap;
use big_blobs::BlobService;
use big_blobs_core::Blob;
use big_blobs_core::BlobId;
use big_blobs_core::CipherBlob;
use big_blobs_core::Jwk;
use big_blobs_core::PutBlob;
use big_cabinet_core::datum::DatumMember;
use big_nodule_core::Handler;
use big_nodule_core::Nodule;
use big_nodule_core::config::artifact_key;
use big_nodule_core::protocol::AdmitNodule;
use big_nodule_core::protocol::UninstallNodule;
use big_plugs_core::Artifact;
use big_plugs_core::Contribution;
use big_plugs_core::Digest;
use big_plugs_core::NoduleContribution;
use big_plugs_core::Plug;
use big_plugs_core::example_plug;
use std::time::Duration;

const PLUG: &str = example_plug!("artifacts");

async fn retained(sql: &Database, document: &DocId) -> BTreeSet<String> {
    sql.query(
        "SELECT digest FROM big_blobs_retention WHERE doc_id = ?1",
        (document.to_string(),),
    )
    .await
    .unwrap()
    .all::<(String,)>()
    .unwrap()
    .into_iter()
    .map(|(digest,)| digest)
    .collect()
}

#[tokio::test]
async fn the_blob_workers_retain_a_nodules_handler_artifact_through_the_config_document() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(
        &secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Memory,
    ))
    .await
    .unwrap();
    let config_doc = DocId::from(node.drawers.local().config_id.clone());
    assert!(retained(&sql, &config_doc).await.is_empty());

    let bytes = b"handler component bytes";
    let info = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            PutBlob {
                capability: blob_cap(&node, BlobService::put_object().into(), Vec::new()).await,
                bytes: bytes.to_vec(),
            },
        )
        .await
        .unwrap();
    let nodule = Nodule {
        handler: Some(Handler {
            artifact: "handler".into(),
            extra: Default::default(),
        }),
        implements: vec!["org.example.other.ping".into()],
        ..Default::default()
    };
    let mut plug = Plug::new(PLUG.parse().unwrap(), "1.0.0".parse().unwrap());
    plug.artifacts.push(Artifact {
        key: "handler".into(),
        digest: Digest(info.blob_id.to_bytes32()),
        size: Some(bytes.len() as u64),
        media_type: Some("application/wasm".into()),
        extra: Default::default(),
    });
    plug.contributions
        .push(Contribution::Nodule(NoduleContribution {
            schema: Nodule::ID.into(),
            description: serde_json::from_value(serde_json::to_value(nodule).unwrap()).unwrap(),
            extra: Default::default(),
        }));
    let operator = Authorization::identity(node.authority.root);
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            AdmitNodule {
                plug,
                drawers: None,
            },
        )
        .await
        .unwrap();

    // The workers pick the config document up, encrypt the referenced bytes into it and
    // retain both representations for it.
    let retained_digests = loop {
        let digests = retained(&sql, &config_doc).await;
        if !digests.is_empty() {
            break digests;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let slot = artifact_key(&PLUG.parse().unwrap(), "handler");
    let datums = all_datums(&node, &config_doc).await.datums;
    let DatumRead::Accepted { value, .. } = &datums[&slot] else {
        panic!("the artifact reference is accepted: {:?}", datums[&slot]);
    };
    assert_eq!(
        serde_json::from_value::<Blob>(value.clone())
            .unwrap()
            .digest,
        info.blob_id.to_string()
    );
    let representation = |tag: &str| {
        let key = DatumKey::new(tag, slot.slot_bytes().to_vec()).unwrap();
        datums[&key].clone()
    };
    assert!(matches!(
        representation(Jwk::ID),
        DatumRead::Accepted { .. }
    ));
    let DatumRead::Accepted { value, .. } = representation(CipherBlob::ID) else {
        panic!("the cipherBlob is accepted");
    };
    let cipher: CipherBlob = serde_json::from_value(value).unwrap();
    // The retention table spells ids in the multihash form, the datums in the bare one.
    assert_eq!(
        retained_digests
            .iter()
            .map(|digest| digest.parse::<BlobId>().unwrap())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([cipher.representation.digest.parse().unwrap(), info.blob_id])
    );

    // Uninstalling removes the reference, and with it the document's retention.
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            UninstallNodule {
                plug_id: PLUG.parse().unwrap(),
            },
        )
        .await
        .unwrap();
    while !retained(&sql, &config_doc).await.is_empty() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown(node, stop).await;
}
