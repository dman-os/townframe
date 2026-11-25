//! The app's installs on a booted node: the review micro's install file configures it and its
//! grants come with its install, and installing intxt at boot makes `summarizeIntxts` reachable
//! for the review micro.

use super::install_intxt;
use super::interim_installs;
use bytes::Bytes;
use clay_node::test_support::create_drawer;
use clay_node::test_support::create_note;
use clay_node::test_support::no_schemas;
use clay_node::test_support::note_schemas;
use clay_node::test_support::read_datums;
use clay_node::NodeComposition;
use clay_node::NodeConfig;
use http_body_util::BodyExt;
use http_body_util::Full;
use serde_json::json;
use serde_json::Value;
use std::sync::Arc;

const REVIEW: &str = "did:example:clay-local/dayframe.review";

/// The review micro's install from the operator's `review.json`: its drawers are read from the
/// file, its config datum is the file's members plus the minted `docCap`, the capability reads the
/// listed drawers and no other, and the datum is where the micro's manifest asks for it. With no
/// file the micro installs unconfigured: no drawers, no datum, no capability.
#[tokio::test]
async fn a_review_install_file_configures_the_micro_and_no_file_leaves_it_unconfigured() {
    let dir = tempfile::tempdir().unwrap();
    let mut composition = NodeComposition::boot(NodeConfig {
        root: dir.path().to_owned(),
        bootstrap: vec![],
        schemas: note_schemas(),
        installs: vec![],
        serve: false,
    })
    .await
    .unwrap();
    let github = create_drawer(composition.node(), "github").await;
    let intxt = create_drawer(composition.node(), "intxt").await;
    let other = create_drawer(composition.node(), "other").await;
    let github_doc = create_note(composition.node(), &github, "a pull").await;
    let other_doc = create_note(composition.node(), &other, "hidden").await;

    let file = json!({
        "githubDrawer": github.to_string(),
        "intxtDrawers": [intxt.to_string()],
        // A file that carries `docCap` must not decide it: only the composition mints one.
        "docCap": "forged",
        "blobCap": "forged",
    });
    let file = serde_json::to_vec(&file).unwrap();
    let installs = interim_installs([REVIEW.to_owned()], |name| {
        assert_eq!(name, "review.json");
        Ok(Some(file.clone()))
    })
    .unwrap();
    assert_eq!(installs.len(), 1);
    assert!(
        installs[0]
            .methods
            .contains("org.example.intxt.summarizeIntxts"),
        "the review micro's grants come with its install: {:?}",
        installs[0].methods
    );
    assert!(
        installs[0].methods.contains("org.example.daybook.blobs.read"),
        "the review micro reads a commit's patch blob: {:?}",
        installs[0].methods
    );
    for install in installs {
        composition.install_micro(install).await.unwrap();
    }
    let capability = composition
        .doc_capability(REVIEW)
        .expect("a configured review micro is minted a document capability");
    let (tag, key, config) = composition.config_datum(REVIEW).unwrap();
    // The datum goes where the micro's manifest reads its configuration.
    let manifest = micro_review_manifest::manifest();
    assert!(
        manifest.configuration_requests.iter().any(|request| {
            request.facets.iter().any(|facet| {
                facet.tag.as_str() == tag && facet.key_id.as_deref().is_none_or(|id| id == key)
            })
        }),
        "the review manifest requests {tag}/{key}"
    );
    let mut decoded = config.clone();
    let blob_cap = decoded
        .as_object_mut()
        .unwrap()
        .remove("blobCap")
        .expect("the composition mints the review micro a blobCap");
    assert_ne!(
        blob_cap,
        json!("forged"),
        "the file does not decide blobCap"
    );
    // The micro's config requires `blobCap`: a datum without it does not decode, which left the
    // micro Unconfigured before the composition minted one.
    assert!(
        serde_json::from_value::<micro_review_manifest::ReviewConfig>(decoded.clone()).is_err()
    );
    assert_eq!(
        decoded,
        json!({
            "githubDrawer": github.to_string(),
            "intxtDrawers": [intxt.to_string()],
            "docCap": serde_json::to_value(capability).unwrap(),
        })
    );
    let decoded: micro_review_manifest::ReviewConfig =
        serde_json::from_value(config.clone()).expect("the installed config decodes");
    assert_eq!(serde_json::to_value(&decoded.blob_cap).unwrap(), blob_cap);
    assert_ne!(decoded.blob_cap, decoded.doc_cap);
    let egress = composition.egress(REVIEW).unwrap();
    let (status, read) = read_datums(&egress, capability, &github_doc).await;
    assert_eq!(status, 200, "{read}");
    let (status, denied) = read_datums(&egress, capability, &other_doc).await;
    assert_eq!((status, denied["error"].as_str()), (400, Some("Denied")));

    // No file: the micro is installed with its grants and nothing configured.
    let installs = interim_installs([REVIEW.to_owned()], |_| Ok(None)).unwrap();
    for install in installs {
        composition.install_micro(install).await.unwrap();
    }
    assert!(composition.doc_capability(REVIEW).is_none());
    assert!(composition.config_datum(REVIEW).is_none());

    // A file naming something that is not a drawer is refused, not half read.
    let refused = interim_installs([REVIEW.to_owned()], |_| {
        Ok(Some(
            br#"{"githubDrawer": "nope", "intxtDrawers": []}"#.to_vec(),
        ))
    })
    .unwrap_err();
    assert!(
        format!("{refused:#}").contains("githubDrawer"),
        "the refusal names the member: {refused:#}"
    );
    composition.stop().await.unwrap();
}

/// Where intxt's handler component is built: `INTXT_GUEST_WASM`, else the `wasm_utils_rs`
/// workspace's release output (the same lookup `day_cli`'s e2e uses).
fn intxt_guest() -> Vec<u8> {
    let path = std::env::var_os("INTXT_GUEST_WASM")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../wasm_utils_rs/target/wasm32-wasip3/release/intxt_nodule_guest.wasm")
        });
    std::fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "{}: {error}; build it with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_nodule_guest --target wasm32-wasip3 --release` or set INTXT_GUEST_WASM",
            path.display()
        )
    })
}

/// What the phone does at boot: install intxt from its bundled handler over the composition's own
/// operator transport. The review micro's actor then reaches `summarizeIntxts` through its
/// egress (it could not before: the node did not serve the method), and the install is
/// idempotent, so the next boot's call writes nothing.
#[tokio::test]
async fn installing_intxt_at_boot_makes_summarize_intxts_reachable_for_the_review_micro() {
    let wasm = intxt_guest();
    let dir = tempfile::tempdir().unwrap();
    let mut composition = NodeComposition::boot(NodeConfig {
        root: dir.path().to_owned(),
        bootstrap: vec![],
        schemas: no_schemas(),
        installs: vec![],
        serve: false,
    })
    .await
    .unwrap();
    let drawer = create_drawer(composition.node(), "intxt").await;
    let installs = interim_installs([REVIEW.to_owned()], |_| Ok(None)).unwrap();
    for install in installs {
        composition.install_micro(install).await.unwrap();
    }
    let egress = composition.egress(REVIEW).unwrap();
    let summarize = |drawers: Vec<String>| {
        let egress = Arc::clone(&egress);
        async move {
            let body = serde_json::to_vec(&json!({ "drawers": drawers })).unwrap();
            let request =
                http::Request::post("http://dispatch.local/xrpc/org.example.intxt.summarizeIntxts")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(
                        Full::new(Bytes::from(body))
                            .map_err(|never| match never {})
                            .boxed_unsync(),
                    )
                    .unwrap();
            let response = egress.send(request).await;
            let status = response.status().as_u16();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            (status, serde_json::from_slice::<Value>(&body).unwrap())
        }
    };
    let (status, before) = summarize(vec![]).await;
    assert_ne!(status, 200, "intxt is not installed yet: {before}");

    let installed = install_intxt(&composition, &wasm).await.unwrap();
    assert!(!installed.already_installed);
    let (status, empty) = summarize(vec![]).await;
    assert_eq!((status, &empty), (200, &json!({ "results": [] })));
    let (status, one) = summarize(vec![drawer.to_string()]).await;
    assert_eq!(status, 200, "{one}");
    assert_eq!(
        one["results"][0]["drawer"],
        json!(drawer.to_string()),
        "a drawer that holds no intxt is answered, not failed: {one}"
    );

    assert!(
        install_intxt(&composition, &wasm)
            .await
            .unwrap()
            .already_installed,
        "a second boot's install is a no-op"
    );
    composition.stop().await.unwrap();
}
