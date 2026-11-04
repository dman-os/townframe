//! The schema registry (ADR 028 §4–§5, plan item P7): an enabled nodule's datum records are
//! registered live in the node's schema repo and retired with it, data requests resolve against
//! the plug holding the datum (with `minRevision` checked), and copies of a Lexicon that cannot
//! be reconciled are refused. Against the fake executor, with no sleeps.

use super::Node;
use super::init::Probe;
use super::init::init_plug;
use super::init::node_hosting;
use super::init::with_requests;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::Dmeta;
use big_dispatch_core::Readiness;
use big_nodule_core::providers::LockedProvider;
use big_nodule_core::providers::RequestStatus;
use big_nodule_core::providers::Unresolved;
use big_plugs_core::Contribution;
use big_plugs_core::Plug;
use big_plugs_core::example_plug;
use big_plugs_core::request::Request;
use serde_json::Value;
use serde_json::json;

const ALPHA: &str = example_plug!("alpha");
const BETA: &str = example_plug!("beta");
const GAMMA: &str = example_plug!("gamma");
const NOTE: &str = "org.example.beta.note";

/// A record Lexicon for `nsid` with one string property `field`, at `revision` when given.
fn record(nsid: &str, revision: Option<u32>, field: &str) -> Value {
    let mut lexicon = json!({
        "lexicon": 1,
        "id": nsid,
        "defs": { "main": {
            "type": "record",
            "key": "tid",
            "record": { "type": "object", "properties": { field: { "type": "string" } } },
        } },
    });
    if let Some(revision) = revision {
        lexicon["revision"] = revision.into();
    }
    lexicon
}

/// `plug` carrying one more Lexicon file.
fn with_lexicon(mut plug: Plug, lexicon: Value) -> Plug {
    let Some(Contribution::Nodule(nodule)) = plug.contributions.first_mut() else {
        panic!("init plugs carry a nodule");
    };
    nodule
        .description
        .entry("lexicons".into())
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .unwrap()
        .push(lexicon);
    plug
}

/// Beta, holding the `note` record at `revision`.
fn beta(version: &str, revision: u32) -> Plug {
    with_lexicon(
        init_plug("beta", version, &[], &[], &[]),
        record(NOTE, Some(revision), "text"),
    )
}

fn read(contract: &str, min_revision: Option<u32>) -> Request {
    let mut request = Request::new(contract, contract);
    request.access = Some("read".into());
    request.min_revision = min_revision;
    request
}

/// An installed, disabled alpha making `requests`.
async fn node_with_alpha(requests: Vec<Request>) -> Node {
    let node = node_hosting(&Probe::new()).await;
    node.admit(
        node.operator(),
        with_requests(init_plug("alpha", "1.0.0", &[], &[], &[]), requests),
    )
    .await
    .unwrap();
    node
}

fn locked(
    contract: &str,
    provider: &str,
    version: Option<&str>,
    revision: Option<u32>,
) -> LockedProvider {
    LockedProvider {
        contract: contract.to_owned(),
        provider: provider.to_owned(),
        version: version.map(|version| version.parse().unwrap()),
        lexicon_revision: revision,
    }
}

#[tokio::test]
async fn an_enabled_nodules_datums_are_registered_live_and_retired_with_it() {
    let node = node_with_alpha(Vec::new()).await;
    node.admit(node.operator(), beta("1.0.0", 1)).await.unwrap();
    // Installed but disabled holds nothing: nobody can write the datum yet.
    assert!(node.schemas.resolve_for_read(NOTE).is_none());

    node.set_enabled(BETA, true).await;
    let handle = node.schemas.require_for_write(NOTE).unwrap();
    assert_eq!(handle.lexicon()["revision"], 1);
    assert!(!node.schemas.is_boot_schema(NOTE));

    // An upgrade re-registers the new revision under the same owner.
    node.admit(node.operator(), beta("1.1.0", 2)).await.unwrap();
    assert_eq!(
        node.schemas.resolve_for_read(NOTE).unwrap().lexicon()["revision"],
        2
    );

    node.set_enabled(BETA, false).await;
    assert!(node.schemas.resolve_for_read(NOTE).is_none());
    assert!(
        node.schemas.resolve_for_read(Dmeta::ID).is_some(),
        "the boot schemas are untouched"
    );
    node.shutdown().await;
}

#[tokio::test]
async fn a_nodule_holding_a_datum_the_node_already_knows_is_refused() {
    // `dmeta` is a boot schema: a plug claiming its NSID would shadow the node's own.
    let node = node_hosting(&Probe::new()).await;
    let mut plug = with_lexicon(
        init_plug("beta", "1.0.0", &[], &[], &[]),
        record(Dmeta::ID, None, "text"),
    );
    plug.authority = Some("org.example".into());
    node.install(plug).await;
    let record = node.record(BETA).await;
    assert_eq!(record.readiness, Readiness::Refused);
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("schema registration refused")),
        "{:?}",
        record.error
    );
    node.shutdown().await;
}

#[tokio::test]
async fn a_data_request_resolves_to_the_holder_and_min_revision_is_checked() {
    let node = node_with_alpha(vec![read(NOTE, Some(2))]).await;
    node.install(beta("1.0.0", 1)).await;
    let record = node.set_enabled_record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::ProviderMissing);
    assert_eq!(
        record.requests[NOTE],
        RequestStatus {
            contract: NOTE.to_owned(),
            locked: None,
            unresolved: Some(Unresolved::RevisionTooOld {
                provider: BETA.to_owned(),
                required: 2,
                offered: 1,
            }),
        }
    );

    // The holder at a sufficient revision resolves it, and the lockfile records that revision.
    node.admit(node.operator(), beta("1.1.0", 2)).await.unwrap();
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(
        record.requests[NOTE],
        RequestStatus {
            contract: NOTE.to_owned(),
            locked: Some(locked(NOTE, BETA, Some("1.1.0"), Some(2))),
            unresolved: None,
        }
    );

    // Without a holder it waits, however it is spelled elsewhere.
    node.set_enabled(BETA, false).await;
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::ProviderMissing);
    assert_eq!(
        record.requests[NOTE].unresolved,
        Some(Unresolved::NoProvider)
    );
    node.shutdown().await;
}

#[tokio::test]
async fn a_boot_schema_satisfies_a_data_request_as_a_native_provider() {
    let node = node_with_alpha(vec![read(Dmeta::ID, None)]).await;
    let record = node.set_enabled_record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(
        record.requests[Dmeta::ID].locked,
        Some(locked(Dmeta::ID, "native", None, None))
    );
    node.shutdown().await;
}

#[tokio::test]
async fn a_vendored_copy_that_conflicts_with_the_holders_is_refused_and_surfaced() {
    let node = node_hosting(&Probe::new()).await;
    node.install(beta("1.0.0", 1)).await;

    // Gamma vendors `note` at the holder's revision with a different body.
    let gamma = with_requests(
        with_lexicon(
            init_plug("gamma", "1.0.0", &[], &[], &[]),
            record(NOTE, Some(1), "title"),
        ),
        vec![read(NOTE, None)],
    );
    node.install(gamma).await;
    let record = node.record(GAMMA).await;
    assert_eq!(record.readiness, Readiness::Refused);
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains(NOTE) && error.contains("conflicts")),
        "{:?}",
        record.error
    );
    // The holder is not refused for a vendor's copy, and its schema stands.
    assert_eq!(node.record(BETA).await.readiness, Readiness::Ready);
    assert_eq!(
        node.schemas.resolve_for_read(NOTE).unwrap().lexicon()["defs"]["main"]["record"]["properties"]
            ["text"]["type"],
        "string"
    );
    node.shutdown().await;
}
