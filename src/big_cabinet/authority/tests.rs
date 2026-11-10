//! Service-level tests: every assertion goes through `Client<AuthorityProtocol>`,
//! so they exercise the same hosted path a real consumer uses.

use super::Authority;
use super::AuthorityBinding;
use super::AuthorityError;
use super::boot;
use big_cabinet_core::DatumKey;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::AttenuateCapability;
use big_cabinet_core::authority::AuthorityMessage;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::InspectCapability;
use big_cabinet_core::authority::MintCapability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::OPERATOR_OBJECT;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::ResolveActor;
use big_cabinet_core::authority::ResolveAuthority;
use big_cabinet_core::authority::ResolvedAuthority;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::authority::SignDatumClaim;
use big_cabinet_core::dmeta::ActorMetadata;
use big_cabinet_core::dmeta::Attribution;
use big_cabinet_core::dmeta::Cid;
use big_cabinet_core::dmeta::ClaimOperation;
use big_cabinet_core::dmeta::DatumClaim;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use irpc::Channels;
use irpc::WithChannels;
use irpc::channel::none::NoReceiver;
use method_core::Method;
use secrets_rs::SecretStore;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_client_native_rs::XrpcClient;
use service_utils_rs::Admission;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::IrpcAdapter;
use service_utils_rs::Reply;
use service_utils_rs::RequestContext;
use service_utils_rs::ServerBinding;
use service_utils_rs::StopToken;
use service_utils_rs::XrpcRouter;
use service_utils_rs::xrpc;
use sqlite_rs::RuntimeConfig;
use sqlite_rs::SqliteRuntime;
use sqlite_rs::test_support;
use std::sync::Arc;

const OBJECT: Rights = Rights::all();

async fn keyring() -> SecretStore {
    let keyring = keyring_core::mock::Store::new().expect("mock keyring")
        as Arc<keyring_core::CredentialStore>;
    SecretStore::boot_with_store(keyring)
        .await
        .expect("mock keyring")
}

struct TestAuthority {
    authority: Authority,
    stop: StopToken,
}

async fn boot_authority() -> TestAuthority {
    let sql = test_support::db(&[]).expect("authority database");
    let (authority, stop) = boot(&keyring().await, "test-node", sql.clone())
        .await
        .expect("authority boot");
    TestAuthority { authority, stop }
}

async fn call<P>(
    client: &Client<AuthorityProtocol>,
    params: P,
) -> Result<P::Output, AuthorityServiceError>
where
    P: Method<Output: method_core::Wire, Error = AuthorityServiceError>,
    AuthorityProtocol: From<Envelope<P>>,
    AuthorityMessage: From<WithChannels<Envelope<P>, AuthorityProtocol>>,
    Envelope<P>: Channels<AuthorityProtocol, Tx = Reply<P>, Rx = NoReceiver>,
{
    client
        .call(RequestContext::root(), params)
        .await
        .map_err(|error| match error {
            ClientError::Method(error) => error,
            ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
        })
}

impl TestAuthority {
    async fn call<P>(&self, params: P) -> Result<P::Output, AuthorityServiceError>
    where
        P: Method<Output: method_core::Wire, Error = AuthorityServiceError>,
        AuthorityProtocol: From<Envelope<P>>,
        AuthorityMessage: From<WithChannels<Envelope<P>, AuthorityProtocol>>,
        Envelope<P>: Channels<AuthorityProtocol, Tx = Reply<P>, Rx = NoReceiver>,
    {
        call(&self.authority.client, params).await
    }

    fn root(&self) -> CapId {
        self.authority.root
    }

    async fn open(
        &self,
        parent: CapId,
        path: &str,
        rights: Rights,
    ) -> Result<OpenedActor, AuthorityServiceError> {
        self.call(OpenActor {
            parent,
            path: path.to_owned(),
            rights,
        })
        .await
    }

    /// An actor under the operator holding every right.
    async fn subsystem(&self, path: &str) -> OpenedActor {
        self.open(self.root(), path, Rights::all())
            .await
            .expect("subsystem actor")
    }

    async fn namespace(&self, namespace_id: Uuid7) -> Namespace {
        self.authority
            .service
            .open_namespace(namespace_id)
            .await
            .unwrap()
    }

    async fn mint(
        &self,
        issuer: CapId,
        object: &str,
        rights: Rights,
    ) -> Result<Capability, AuthorityServiceError> {
        self.call(MintCapability {
            issuer,
            parent: None,
            dependencies: Vec::new(),
            object: object.to_owned(),
            rights,
            subject: None,
        })
        .await
    }

    async fn attenuate(
        &self,
        capability: CapId,
        rights: Rights,
    ) -> Result<Capability, AuthorityServiceError> {
        self.call(AttenuateCapability { capability, rights }).await
    }

    async fn retract(&self, via: CapId, capability: CapId) -> Result<(), AuthorityServiceError> {
        self.call(RetractCapability { via, capability }).await
    }

    /// The actor an identity capability is, as the old capability-gated `ResolveActor` said it:
    /// the capability must carry `USE` and be an identity capability.
    async fn actor_of(&self, capability: CapId) -> Result<Actor, AuthorityServiceError> {
        self.call(ResolveAuthority { capability })
            .await?
            .actor
            .ok_or(AuthorityServiceError::UnrecognizedActor)
    }

    async fn sign(&self, via: CapId, claim: DatumClaim) -> Result<(), AuthorityServiceError> {
        let signed = self
            .call(SignDatumClaim {
                via,
                claim: claim.clone(),
            })
            .await?;
        // Core verifies it with no knowledge of this service.
        signed.verify().expect("core verification");
        assert_eq!(signed.claim, claim);
        Ok(())
    }
}

fn datum_claim(actor: Uuid7, seed: u8) -> DatumClaim {
    let mut digest = vec![0x01, 0x55, 0x1e, 0x20];
    digest.extend([seed; 32]);
    DatumClaim {
        origin_document: DocId::new(b"mutation-document"),
        datum_key: DatumKey::from_text("org.example.note", "main").expect("datum key"),
        chosen_uuid: Uuid7::new(),
        actor_uuid: actor,
        operation: ClaimOperation::Set,
        value_cid: Some(Cid::try_from(digest.as_slice()).expect("cid")),
        predecessors: Vec::new(),
    }
}

macro_rules! assert_refused {
    ($result:expr, $error:pat) => {
        match $result {
            Err(error) => assert!(matches!(error, $error), "{error:?}"),
            Ok(value) => panic!("expected a refusal, got {value:?}"),
        }
    };
}

#[tokio::test]
async fn boot_hosts_a_service_whose_root_is_the_operators_actor_capability() {
    let test = boot_authority().await;
    let root = test
        .call(InspectCapability {
            capability: test.root(),
        })
        .await
        .expect("root capability");
    assert_eq!(
        root,
        Capability {
            capability_id: test.root(),
            object: OPERATOR_OBJECT.to_owned(),
            rights: Rights::all(),
            issuer_id: root.issuer_id,
            subject: Some(test.authority.service.operator_uuid()),
        }
    );
    let operator = test.actor_of(test.root()).await.expect("operator");
    // The operator is the root of the path hierarchy: its URL is the node root.
    assert_eq!(
        operator.url,
        format!("db+actor://{}/", test.authority.node_issuer)
    );
    assert_eq!(operator.parent_actor_id, None);
    assert_eq!(
        test.call(ResolveAuthority {
            capability: test.root(),
        })
        .await
        .unwrap(),
        ResolvedAuthority {
            capability: root,
            actor: Some(operator),
        }
    );
    test.retract(test.root(), test.root()).await.unwrap();
    assert_refused!(
        test.actor_of(test.root()).await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn an_opened_actor_carries_a_verifiable_attribution_certificate() {
    let test = boot_authority().await;
    let actor = test.subsystem("docs").await;
    assert_eq!(
        actor.actor.url,
        format!("db+actor://{}/docs", test.authority.node_issuer)
    );
    assert_eq!(
        actor.actor.parent_actor_id,
        Some(test.authority.service.operator_uuid())
    );
    assert!(actor.actor.public_key.to_string().starts_with('z'));

    // The certificate is exactly what dmeta stores, and it verifies against the
    // issuer a verifier is told to expect — not against its own `iss`.
    let metadata = ActorMetadata {
        extra: Default::default(),
        created_at: Vec::new(),
        pubkey: actor.actor.public_key.clone(),
        actor_url: actor.actor.url.clone(),
        delegation: Some(actor.actor.certificate.clone()),
    };
    Attribution::verify(
        &actor.actor.certificate,
        &test.authority.node_issuer,
        actor.actor.actor_id,
        &metadata,
    )
    .expect("attribution certificate");

    let resolved = test.actor_of(actor.capability).await.expect("resolve");
    assert_eq!(resolved, actor.actor);
    test.stop.stop().await;
}

#[tokio::test]
async fn actor_paths_extend_their_parent_by_exactly_one_segment() {
    let test = boot_authority().await;
    let runtime = test.subsystem("rt").await;
    let cartridges = test
        .open(runtime.capability, "rt/cartridges", Rights::all())
        .await
        .expect("nested actor");
    assert!(
        cartridges.actor.url.ends_with("/rt/cartridges"),
        "{}",
        cartridges.actor.url
    );
    assert_eq!(
        cartridges.actor.parent_actor_id,
        Some(runtime.actor.actor_id)
    );

    for (parent, path) in [
        (runtime.capability, "other"),
        (runtime.capability, "rt2"),
        (runtime.capability, "rt"),
        (runtime.capability, "rt/"),
        (runtime.capability, "rt/cartridges/ocr"),
        (runtime.capability, "rt/.."),
        (runtime.capability, "rt/a b"),
        (test.root(), "a/b"),
        (test.root(), ""),
    ] {
        assert_refused!(
            test.open(parent, path, Rights::all()).await,
            AuthorityServiceError::InvalidActorPath
        );
    }
    // Two actors may both have a `worker` child: the hierarchy is per parent.
    let other = test.subsystem("svc").await;
    test.open(other.capability, "svc/worker", Rights::all())
        .await
        .expect("independent nested actor");
    test.open(runtime.capability, "rt/worker", Rights::all())
        .await
        .expect("independent nested actor");
    test.stop.stop().await;
}

#[tokio::test]
async fn opening_an_actor_again_returns_the_same_identity() {
    let test = boot_authority().await;
    let first = test.open(test.root(), "docs", Rights::all()).await.unwrap();
    let again = test.open(test.root(), "docs", Rights::all()).await.unwrap();
    assert_eq!(again, first, "the same rights reuse the live capability");

    // Other rights are another handle on the same actor.
    let narrower = test
        .open(test.root(), "docs", Rights::USE | Rights::MINT_CHILD)
        .await
        .unwrap();
    assert_eq!(narrower.actor.actor_id, first.actor.actor_id);
    assert_eq!(narrower.actor.public_key, first.actor.public_key);
    assert_ne!(narrower.capability, first.capability);

    // A child's rights stay inside its parent's.
    assert_refused!(
        test.open(narrower.capability, "docs/index", Rights::DELEGATE)
            .await,
        AuthorityServiceError::Escalation
    );
    let index = test
        .open(narrower.capability, "docs/index", Rights::USE)
        .await
        .unwrap();
    assert_refused!(
        test.open(index.capability, "docs/index/x", Rights::USE)
            .await,
        AuthorityServiceError::Unauthorized
    );

    // Retracting a capability and opening again rotates the handle, not the actor.
    test.retract(test.root(), first.capability).await.unwrap();
    assert_refused!(
        test.actor_of(first.capability).await,
        AuthorityServiceError::InvalidCapability
    );
    let rotated = test.open(test.root(), "docs", Rights::all()).await.unwrap();
    assert_eq!(rotated.actor.actor_id, first.actor.actor_id);
    assert_eq!(rotated.actor.certificate, first.actor.certificate);
    assert_ne!(rotated.capability, first.capability);
    test.stop.stop().await;
}

#[tokio::test]
async fn only_an_actors_own_capability_acts_as_it() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let namespace = test.namespace(docs.actor.actor_id).await;
    assert_refused!(
        test.mint(docs.capability, "edit doc X", OBJECT).await,
        AuthorityServiceError::Unauthorized
    );
    let edit = test
        .mint(namespace.capability, "edit doc X", OBJECT)
        .await
        .unwrap();
    assert_eq!(edit.issuer_id, docs.actor.actor_id);

    assert_refused!(
        test.sign(edit.capability_id, datum_claim(docs.actor.actor_id, 1))
            .await,
        AuthorityServiceError::UnrecognizedActor
    );
    assert_refused!(
        test.actor_of(edit.capability_id).await,
        AuthorityServiceError::UnrecognizedActor
    );
    assert_refused!(
        test.open(edit.capability_id, "docs/sneaky", OBJECT).await,
        AuthorityServiceError::UnrecognizedActor
    );
    assert_refused!(
        test.mint(edit.capability_id, "edit doc Y", Rights::USE)
            .await,
        AuthorityServiceError::Unauthorized
    );

    // An actor capability without `use` does not identify or sign either.
    let opener = test
        .open(test.root(), "docs", Rights::MINT_CHILD)
        .await
        .unwrap();
    assert_refused!(
        test.sign(opener.capability, datum_claim(docs.actor.actor_id, 2))
            .await,
        AuthorityServiceError::Unauthorized
    );
    assert_refused!(
        test.actor_of(opener.capability).await,
        AuthorityServiceError::Unauthorized
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn capabilities_only_attenuate_and_labels_cannot_forge_issuance() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let namespace = test.namespace(docs.actor.actor_id).await;
    let edit = test
        .mint(
            namespace.capability,
            "edit doc X",
            Rights::USE | Rights::NARROW,
        )
        .await
        .unwrap();
    let copy = test
        .attenuate(edit.capability_id, Rights::USE)
        .await
        .unwrap();
    assert_eq!(
        copy,
        Capability {
            capability_id: copy.capability_id,
            object: edit.object.clone(),
            rights: Rights::USE,
            issuer_id: docs.actor.actor_id,
            subject: None,
        }
    );

    assert_refused!(
        test.attenuate(edit.capability_id, Rights::USE | Rights::DELEGATE)
            .await,
        AuthorityServiceError::Escalation
    );
    assert_refused!(
        test.attenuate(copy.capability_id, Rights::USE).await,
        AuthorityServiceError::Unauthorized
    );
    let registered = test
        .call(InspectCapability {
            capability: docs.capability,
        })
        .await
        .unwrap();
    let derived = test
        .attenuate(docs.capability, Rights::all())
        .await
        .unwrap();
    let named = test
        .mint(namespace.capability, &registered.object, Rights::all())
        .await
        .unwrap();
    assert_eq!(derived.object, registered.object);
    assert_eq!(derived.issuer_id, registered.issuer_id);
    assert_eq!(named.object, registered.object);
    assert_ne!(named.issuer_id, registered.issuer_id);
    // A handle derived from an identity capability is bound to the same actor, so it signs as
    // that actor; a handle that merely carries the same label is bound to nobody and cannot.
    assert_eq!(derived.subject, Some(docs.actor.actor_id));
    test.sign(derived.capability_id, datum_claim(docs.actor.actor_id, 4))
        .await
        .expect("a derived handle signs as its subject");
    assert_eq!(named.subject, None);
    assert_refused!(
        test.sign(named.capability_id, datum_claim(docs.actor.actor_id, 4))
            .await,
        AuthorityServiceError::UnrecognizedActor
    );
    for counterfeit in [derived, named] {
        assert_refused!(
            test.actor_of(counterfeit.capability_id).await,
            AuthorityServiceError::UnrecognizedActor
        );
        assert_refused!(
            test.open(counterfeit.capability_id, "docs/fake", Rights::all())
                .await,
            AuthorityServiceError::UnrecognizedActor
        );
        assert_refused!(
            test.mint(counterfeit.capability_id, "edit doc Y", Rights::USE)
                .await,
            AuthorityServiceError::Unauthorized
        );
    }
    let root_copy = test.attenuate(test.root(), Rights::all()).await.unwrap();
    assert_refused!(
        test.open(root_copy.capability_id, "fake", Rights::all())
            .await,
        AuthorityServiceError::UnrecognizedActor
    );
    let reader = test
        .attenuate(namespace.capability, Rights::USE | Rights::NARROW)
        .await
        .unwrap();
    assert_refused!(
        test.mint(reader.capability_id, "read doc X", Rights::DELEGATE)
            .await,
        AuthorityServiceError::Escalation
    );
    let readable = test
        .mint(reader.capability_id, "read doc X", Rights::USE)
        .await
        .unwrap();
    assert_eq!(readable.issuer_id, namespace.namespace_id);
    let limited = test
        .attenuate(namespace.capability, Rights::USE)
        .await
        .unwrap();
    assert_refused!(
        test.mint(limited.capability_id, "read doc X", Rights::USE)
            .await,
        AuthorityServiceError::Unauthorized
    );
    let namespace_metadata = test
        .call(InspectCapability {
            capability: namespace.capability,
        })
        .await
        .unwrap();
    let forged = test
        .mint(
            namespace.capability,
            &namespace_metadata.object,
            Rights::all(),
        )
        .await
        .unwrap();
    assert_eq!(forged.issuer_id, namespace_metadata.issuer_id);
    assert_eq!(forged.object, namespace_metadata.object);
    assert_eq!(forged.rights, namespace_metadata.rights);
    assert_refused!(
        test.mint(forged.capability_id, "forged namespace", Rights::USE)
            .await,
        AuthorityServiceError::Unauthorized
    );
    let forged_copy = test
        .attenuate(forged.capability_id, Rights::all())
        .await
        .unwrap();
    assert_refused!(
        test.mint(forged_copy.capability_id, "forged namespace", Rights::USE)
            .await,
        AuthorityServiceError::Unauthorized
    );
    test.retract(namespace.capability, reader.capability_id)
        .await
        .unwrap();
    assert_refused!(
        test.call(InspectCapability {
            capability: readable.capability_id
        })
        .await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn inspection_exposes_uuid_handles_but_not_parent_authority() {
    let test = boot_authority().await;
    let docs = test.namespace(Uuid7::new()).await;
    let edit = test
        .mint(docs.capability, "edit doc X", Rights::USE)
        .await
        .unwrap();
    let wire = serde_json::to_value(&edit).unwrap();
    assert_eq!(wire.as_object().unwrap().len(), 5);
    assert!(wire["subject"].is_null());
    assert!(wire.get("parent").is_none());
    assert!(wire.get("kind").is_none());
    let id = uuid::Uuid::parse_str(wire["capabilityId"].as_str().unwrap()).unwrap();
    assert_eq!(id.get_version_num(), 4);
    assert_eq!(id.to_string(), wire["capabilityId"].as_str().unwrap());
    assert_eq!(
        serde_json::from_value::<Capability>(wire.clone()).unwrap(),
        edit
    );
    assert!(
        !serde_json::to_string(&wire).unwrap().contains(
            serde_json::to_value(docs.capability)
                .unwrap()
                .as_str()
                .unwrap()
        )
    );
    assert_eq!(format!("{:?}", edit.capability_id), "CapId([redacted])");
    let issuer_as_cap = serde_json::from_value::<CapId>(wire["issuerId"].clone()).unwrap();
    assert_refused!(
        test.call(InspectCapability {
            capability: issuer_as_cap
        })
        .await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn derivations_stop_before_creating_unusable_handles() {
    let test = boot_authority().await;
    let docs = test.namespace(Uuid7::new()).await;
    let mut held = test
        .mint(docs.capability, "edit doc X", OBJECT)
        .await
        .unwrap();
    for _ in 2..super::cap::MAX_CHAIN_DEPTH {
        held = test.attenuate(held.capability_id, OBJECT).await.unwrap();
    }
    assert_refused!(
        test.attenuate(held.capability_id, OBJECT).await,
        AuthorityServiceError::Escalation
    );
    assert_eq!(
        test.call(InspectCapability {
            capability: held.capability_id
        })
        .await
        .unwrap(),
        held
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn retraction_is_structural_and_takes_derivations_with_it() {
    let test = boot_authority().await;
    let docs = test.namespace(Uuid7::new()).await;
    let other = test.namespace(Uuid7::new()).await;
    let edit = test
        .mint(docs.capability, "edit doc X", OBJECT)
        .await
        .unwrap();
    let copy = test
        .attenuate(edit.capability_id, Rights::USE)
        .await
        .unwrap();
    let held = test
        .attenuate(edit.capability_id, Rights::USE)
        .await
        .unwrap();

    assert_refused!(
        test.retract(other.capability, edit.capability_id).await,
        AuthorityServiceError::Unauthorized
    );
    assert_refused!(
        test.retract(copy.capability_id, edit.capability_id).await,
        AuthorityServiceError::Unauthorized
    );
    // A holder may give up its own capability.
    test.retract(held.capability_id, held.capability_id)
        .await
        .unwrap();

    test.retract(docs.capability, edit.capability_id)
        .await
        .unwrap();
    for gone in [edit.capability_id, copy.capability_id, held.capability_id] {
        assert_refused!(
            test.call(InspectCapability { capability: gone }).await,
            AuthorityServiceError::InvalidCapability
        );
    }
    let survivor = test
        .mint(docs.capability, "edit doc Y", OBJECT)
        .await
        .unwrap();
    test.retract(docs.capability, docs.capability)
        .await
        .unwrap();
    assert_refused!(
        test.mint(docs.capability, "new object", Rights::USE).await,
        AuthorityServiceError::InvalidCapability
    );
    test.mint(other.capability, "unrelated", Rights::USE)
        .await
        .unwrap();
    assert_refused!(
        test.call(InspectCapability {
            capability: survivor.capability_id
        })
        .await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn reopening_a_retracted_namespace_rotates_only_its_handle() {
    let test = boot_authority().await;
    let namespace = test.namespace(Uuid7::new()).await;
    let other = test.namespace(Uuid7::new()).await;
    let minter = test
        .attenuate(namespace.capability, Rights::all())
        .await
        .unwrap();
    let object = test
        .mint(minter.capability_id, "old object", Rights::all())
        .await
        .unwrap();
    let leaf = test
        .attenuate(object.capability_id, Rights::USE)
        .await
        .unwrap();
    let unrelated = test
        .mint(other.capability, "unrelated", Rights::USE)
        .await
        .unwrap();
    let before = test.authority.service.live_capabilities().await;
    assert_eq!(test.namespace(namespace.namespace_id).await, namespace);
    assert_eq!(test.authority.service.live_capabilities().await, before);
    assert_eq!(
        test.call(InspectCapability {
            capability: leaf.capability_id
        })
        .await
        .unwrap(),
        leaf
    );
    test.retract(namespace.capability, namespace.capability)
        .await
        .unwrap();
    let reopened = test.namespace(namespace.namespace_id).await;
    assert_eq!(reopened.namespace_id, namespace.namespace_id);
    assert_ne!(reopened.capability, namespace.capability);
    assert_eq!(test.namespace(reopened.namespace_id).await, reopened);
    // The namespace handle, the minter, the object, and the leaf are gone; the reopened
    // handle is the only new row, and the unrelated namespace kept its one capability.
    assert_eq!(test.authority.service.live_capabilities().await, before - 3);
    for capability in [
        namespace.capability,
        minter.capability_id,
        object.capability_id,
        leaf.capability_id,
    ] {
        assert_refused!(
            test.call(InspectCapability { capability }).await,
            AuthorityServiceError::InvalidCapability
        );
    }
    let fresh = test
        .mint(reopened.capability, "fresh object", Rights::USE)
        .await
        .unwrap();
    assert_eq!(fresh.issuer_id, namespace.namespace_id);
    assert_eq!(test.namespace(other.namespace_id).await, other);
    assert_eq!(
        test.call(InspectCapability {
            capability: unrelated.capability_id
        })
        .await
        .unwrap(),
        unrelated
    );
    assert_eq!(
        test.call(InspectCapability {
            capability: fresh.capability_id
        })
        .await
        .unwrap(),
        fresh
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn resolution_only_recognizes_exact_registered_identity_issuances() {
    let test = boot_authority().await;
    let actor = test
        .open(test.root(), "subject", Rights::USE)
        .await
        .unwrap();
    let capability = test
        .call(InspectCapability {
            capability: actor.capability,
        })
        .await
        .unwrap();
    assert_eq!(
        test.call(ResolveAuthority {
            capability: actor.capability
        })
        .await
        .unwrap(),
        ResolvedAuthority {
            capability,
            actor: Some(actor.actor)
        }
    );
    let namespace = test.namespace(Uuid7::new()).await;
    let named = test
        .mint(namespace.capability, OPERATOR_OBJECT, Rights::all())
        .await
        .unwrap();
    let derived = test
        .attenuate(test.root(), Rights::USE | Rights::NARROW)
        .await
        .unwrap();
    for capability in [named, derived] {
        assert_eq!(
            test.call(ResolveAuthority {
                capability: capability.capability_id
            })
            .await
            .unwrap(),
            ResolvedAuthority {
                capability: capability.clone(),
                actor: None
            }
        );
        test.retract(capability.capability_id, capability.capability_id)
            .await
            .unwrap();
        assert_refused!(
            test.call(ResolveAuthority {
                capability: capability.capability_id
            })
            .await,
            AuthorityServiceError::InvalidCapability
        );
    }
    let no_use = test
        .open(test.root(), "subject", Rights::MINT_CHILD)
        .await
        .unwrap();
    assert_refused!(
        test.call(ResolveAuthority {
            capability: no_use.capability
        })
        .await,
        AuthorityServiceError::Unauthorized
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn signing_binds_the_claim_to_the_presenting_actor() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    test.sign(docs.capability, datum_claim(docs.actor.actor_id, 7))
        .await
        .expect("signature");
    let signed = test
        .call(SignDatumClaim {
            via: docs.capability,
            claim: datum_claim(docs.actor.actor_id, 8),
        })
        .await
        .unwrap();
    assert_eq!(signed.actor_public_key, docs.actor.public_key);

    // A claim naming another actor is refused, including the actor's parent.
    for actor in [Uuid7::new(), test.authority.service.operator_uuid()] {
        assert_refused!(
            test.sign(docs.capability, datum_claim(actor, 7)).await,
            AuthorityServiceError::Unauthorized
        );
    }
    test.stop.stop().await;
}

#[tokio::test]
async fn unknown_capability_ids_are_refused() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let forged = CapId::default();
    assert_refused!(
        test.call(InspectCapability { capability: forged }).await,
        AuthorityServiceError::InvalidCapability
    );
    assert_refused!(
        test.actor_of(forged).await,
        AuthorityServiceError::InvalidCapability
    );
    assert_refused!(
        test.sign(forged, datum_claim(docs.actor.actor_id, 1)).await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn reboot_loses_handles_but_preserves_actors_and_keys() {
    let secrets = keyring().await;
    let directory = tempfile::tempdir().expect("authority state directory");
    let path = directory.path().join("authority.sqlite");
    let runtime = SqliteRuntime::new(RuntimeConfig::default());
    let sql = runtime.database(&path, &[], "_test_migrations");
    let (authority, stop) = boot(&secrets, "test-node", sql.clone()).await.unwrap();
    let first = TestAuthority { authority, stop };
    let docs = first.subsystem("docs").await;
    let plugin = first.subsystem("plugin").await;
    let namespace = first.namespace(docs.actor.actor_id).await;
    let edit = first
        .mint(namespace.capability, "edit doc X", OBJECT)
        .await
        .unwrap();
    let derived_identity = first
        .attenuate(docs.capability, Rights::all())
        .await
        .unwrap();
    let claim = datum_claim(docs.actor.actor_id, 9);
    let signature = first
        .call(SignDatumClaim {
            via: docs.capability,
            claim: claim.clone(),
        })
        .await
        .unwrap();
    let operator = first.authority.service.operator_uuid();
    let issuer = first.authority.node_issuer.clone();
    let root = first.root();
    first.stop.stop().await;
    drop(first.authority);

    drop(sql);
    drop(runtime);
    let runtime = SqliteRuntime::new(RuntimeConfig::default());
    let sql = runtime.database(&path, &[], "_test_migrations");
    let (authority, stop) = boot(&secrets, "test-node", sql.clone()).await.unwrap();
    let reopened = TestAuthority { authority, stop };
    assert_ne!(reopened.root(), root);
    assert_eq!(reopened.authority.service.operator_uuid(), operator);
    assert_eq!(reopened.authority.node_issuer, issuer);
    for capability in [
        root,
        docs.capability,
        plugin.capability,
        namespace.capability,
        edit.capability_id,
        derived_identity.capability_id,
    ] {
        assert_refused!(
            reopened.call(InspectCapability { capability }).await,
            AuthorityServiceError::InvalidCapability
        );
        assert_refused!(
            reopened.actor_of(capability).await,
            AuthorityServiceError::InvalidCapability
        );
    }
    let reopened_docs = reopened.subsystem("docs").await;
    assert_ne!(reopened_docs.capability, docs.capability);
    assert_eq!(
        reopened_docs,
        OpenedActor {
            capability: reopened_docs.capability,
            ..docs
        }
    );
    let reopened_namespace = reopened.namespace(namespace.namespace_id).await;
    assert_ne!(reopened_namespace.capability, namespace.capability);
    reopened
        .mint(reopened_namespace.capability, "edit doc X", OBJECT)
        .await
        .unwrap();
    let reopened_signature = reopened
        .call(SignDatumClaim {
            via: reopened_docs.capability,
            claim,
        })
        .await
        .unwrap();
    assert_eq!(reopened_signature, signature);
    reopened_signature.verify().unwrap();
    let forbidden: i64 = sql
        .query(
            "SELECT count(*) FROM sqlite_schema
              WHERE name IN ('authority_caps', 'authority_actor_issuances', 'authority_grants')",
            (),
        )
        .await
        .unwrap()
        .one()
        .unwrap();
    assert_eq!(forbidden, 0);
    let durable_root: i64 = sql
        .query(
            "SELECT count(*) FROM pragma_table_info('authority_node') WHERE name = 'root_cap'",
            (),
        )
        .await
        .unwrap()
        .one()
        .unwrap();
    assert_eq!(durable_root, 0);
    reopened.stop.stop().await;

    // The sealed keys open only under the wrapping key that sealed them, so a
    // keyring that lost it is refused rather than given fresh identities.
    let refused = boot(&keyring().await, "test-node", sql).await;
    assert!(
        matches!(refused, Err(AuthorityError::StateCorrupt(_))),
        "{:?}",
        refused.err()
    );
}

#[tokio::test]
async fn cross_namespace_derivations_depend_on_both_source_and_issuer() {
    let test = boot_authority().await;
    for retract_issuer in [false, true] {
        let source_namespace = test.namespace(Uuid7::new()).await;
        let target_namespace = test.namespace(Uuid7::new()).await;
        let source = test
            .mint(source_namespace.capability, "source", Rights::all())
            .await
            .unwrap();
        let issuer = test
            .attenuate(target_namespace.capability, Rights::all())
            .await
            .unwrap();
        let derived = test
            .call(MintCapability {
                issuer: issuer.capability_id,
                parent: Some(source.capability_id),
                dependencies: Vec::new(),
                object: "target".into(),
                rights: Rights::USE | Rights::NARROW | Rights::DELEGATE,
                subject: None,
            })
            .await
            .unwrap();
        assert_eq!(derived.issuer_id, target_namespace.namespace_id);
        let leaf = test
            .attenuate(derived.capability_id, Rights::USE)
            .await
            .unwrap();
        let unrelated = test
            .mint(target_namespace.capability, "unrelated", Rights::USE)
            .await
            .unwrap();
        assert_refused!(
            test.retract(unrelated.capability_id, derived.capability_id)
                .await,
            AuthorityServiceError::Unauthorized
        );
        let ancestor = if retract_issuer {
            target_namespace.capability
        } else {
            source_namespace.capability
        };
        test.retract(ancestor, leaf.capability_id).await.unwrap();
        test.call(InspectCapability {
            capability: derived.capability_id,
        })
        .await
        .unwrap();
        let (via, capability) = if retract_issuer {
            (target_namespace.capability, issuer.capability_id)
        } else {
            (source_namespace.capability, source.capability_id)
        };
        test.retract(via, capability).await.unwrap();
        for capability in [derived.capability_id, leaf.capability_id] {
            assert_refused!(
                test.call(InspectCapability { capability }).await,
                AuthorityServiceError::InvalidCapability
            );
        }
        assert_eq!(
            test.call(InspectCapability {
                capability: unrelated.capability_id
            })
            .await
            .unwrap(),
            unrelated
        );
    }
    test.stop.stop().await;
}

#[tokio::test]
async fn liveness_gates_preserve_rights_and_cross_namespace_retraction() {
    let test = boot_authority().await;
    for retracted in 0..4 {
        let namespace = test.namespace(Uuid7::new()).await;
        let source_namespace = test.namespace(Uuid7::new()).await;
        let gate_namespace = test.namespace(Uuid7::new()).await;
        let source = test
            .mint(source_namespace.capability, "source", Rights::all())
            .await
            .unwrap();
        let signer_parent = test.subsystem(&format!("signer-{retracted}")).await;
        let signer = test
            .open(
                signer_parent.capability,
                &format!("signer-{retracted}/bound"),
                Rights::USE,
            )
            .await
            .unwrap();
        let gate = test
            .mint(gate_namespace.capability, "grant gate", Rights::USE)
            .await
            .unwrap();
        let derived = test
            .call(MintCapability {
                issuer: namespace.capability,
                parent: Some(source.capability_id),
                dependencies: vec![signer.capability, gate.capability_id],
                object: "scoped object".into(),
                rights: Rights::all(),
                subject: None,
            })
            .await
            .unwrap();
        assert_eq!(
            derived,
            Capability {
                capability_id: derived.capability_id,
                object: "scoped object".into(),
                rights: Rights::all(),
                issuer_id: namespace.namespace_id,
                subject: None,
            }
        );
        assert_refused!(
            test.actor_of(derived.capability_id).await,
            AuthorityServiceError::UnrecognizedActor
        );
        let leaf = test
            .attenuate(derived.capability_id, Rights::USE)
            .await
            .unwrap();
        let ancestor = [
            signer_parent.capability,
            signer.capability,
            gate_namespace.capability,
            gate.capability_id,
        ][retracted];
        let withdrawn_leaf = test
            .attenuate(derived.capability_id, Rights::USE)
            .await
            .unwrap();
        test.retract(ancestor, withdrawn_leaf.capability_id)
            .await
            .unwrap();
        assert_refused!(
            test.call(InspectCapability {
                capability: withdrawn_leaf.capability_id,
            })
            .await,
            AuthorityServiceError::InvalidCapability
        );
        assert_eq!(
            test.call(InspectCapability {
                capability: derived.capability_id,
            })
            .await
            .unwrap(),
            derived
        );
        test.retract(ancestor, ancestor).await.unwrap();
        for capability in [derived.capability_id, leaf.capability_id] {
            assert_refused!(
                test.call(InspectCapability { capability }).await,
                AuthorityServiceError::InvalidCapability
            );
        }
        assert_eq!(
            test.call(InspectCapability {
                capability: source.capability_id,
            })
            .await
            .unwrap(),
            source
        );
    }
    test.stop.stop().await;
}

#[tokio::test]
async fn invalid_gates_publish_nothing_and_cannot_replace_narrowing_authority() {
    let test = boot_authority().await;
    let namespace = test.namespace(Uuid7::new()).await;
    let signer = test.open(test.root(), "signer", Rights::USE).await.unwrap();
    let revoked = test.namespace(Uuid7::new()).await;
    let gate = test
        .mint(revoked.capability, "gate", Rights::USE)
        .await
        .unwrap();
    test.retract(revoked.capability, revoked.capability)
        .await
        .unwrap();
    let before = test.authority.service.live_capabilities().await;
    let changes = test.authority.service.changes.subscribe();
    for dependency in [CapId::new_random(), revoked.capability, gate.capability_id] {
        assert_refused!(
            test.call(MintCapability {
                issuer: namespace.capability,
                parent: None,
                dependencies: vec![signer.capability, dependency],
                object: "rejected".into(),
                rights: Rights::all(),
                subject: None,
            })
            .await,
            AuthorityServiceError::InvalidCapability
        );
        assert_eq!(test.authority.service.live_capabilities().await, before);
    }
    for (issuer, parent) in [
        (namespace.capability, Some(signer.capability)),
        (signer.capability, None),
    ] {
        assert_refused!(
            test.call(MintCapability {
                issuer,
                parent,
                dependencies: vec![namespace.capability],
                object: "rejected".into(),
                rights: Rights::USE | Rights::NARROW,
                subject: None,
            })
            .await,
            AuthorityServiceError::Unauthorized
        );
        assert_eq!(test.authority.service.live_capabilities().await, before);
    }
    assert!(!changes.has_changed().unwrap());
    test.stop.stop().await;
}

#[tokio::test]
async fn mint_publication_is_serialized_with_dependency_retraction() {
    use service_utils_rs::Handler as _;

    let test = boot_authority().await;
    for retract_first in [false, true] {
        let namespace = test.namespace(Uuid7::new()).await;
        let gate_namespace = test.namespace(Uuid7::new()).await;
        let gate = test
            .mint(gate_namespace.capability, "gate", Rights::USE)
            .await
            .unwrap();
        let service = test.authority.service.as_ref();
        let before = service.live_capabilities().await;
        // The ledger stalls on the hold, so both calls queue behind it in poll order.
        let release = service.hold().await;
        let mint = service.call(Envelope::new(
            RequestContext::root(),
            MintCapability {
                issuer: namespace.capability,
                parent: None,
                dependencies: vec![gate.capability_id],
                object: "dependent".into(),
                rights: Rights::all(),
                subject: None,
            },
        ));
        let retract = service.call(Envelope::new(
            RequestContext::root(),
            RetractCapability {
                via: gate.capability_id,
                capability: gate.capability_id,
            },
        ));
        tokio::pin!(mint, retract);
        if retract_first {
            assert!(futures::poll!(&mut retract).is_pending());
            assert!(futures::poll!(&mut mint).is_pending());
        } else {
            assert!(futures::poll!(&mut mint).is_pending());
            assert!(futures::poll!(&mut retract).is_pending());
        }
        release.send(()).unwrap();
        let (minted, retracted) = tokio::join!(mint, retract);
        retracted.unwrap();
        // Retraction deletes the gate and whatever depends on it, so either order leaves the
        // table one row smaller than before the race.
        if retract_first {
            assert!(matches!(
                minted,
                Err(AuthorityServiceError::InvalidCapability)
            ));
            assert_eq!(service.live_capabilities().await, before - 1);
        } else {
            let minted = minted.unwrap().output;
            assert_eq!(service.live_capabilities().await, before - 1);
            assert_refused!(
                test.call(InspectCapability {
                    capability: minted.capability_id
                })
                .await,
                AuthorityServiceError::InvalidCapability
            );
        }
        assert_refused!(
            test.call(MintCapability {
                issuer: namespace.capability,
                parent: None,
                dependencies: vec![gate.capability_id],
                object: "after retraction".into(),
                rights: Rights::all(),
                subject: None,
            })
            .await,
            AuthorityServiceError::InvalidCapability
        );
    }
    test.stop.stop().await;
}

#[tokio::test]
async fn both_dependencies_bound_minting_and_failures_publish_no_capability() {
    let test = boot_authority().await;
    let namespace = test.namespace(Uuid7::new()).await;
    let source_namespace = test.namespace(Uuid7::new()).await;
    let source = test
        .mint(
            source_namespace.capability,
            "source",
            Rights::USE | Rights::NARROW,
        )
        .await
        .unwrap();
    let no_narrow = test
        .attenuate(source.capability_id, Rights::USE)
        .await
        .unwrap();
    let issuer = test
        .attenuate(namespace.capability, Rights::USE | Rights::NARROW)
        .await
        .unwrap();
    let retracted = test
        .mint(namespace.capability, "retracted", Rights::all())
        .await
        .unwrap();
    test.retract(retracted.capability_id, retracted.capability_id)
        .await
        .unwrap();
    let before = test.authority.service.live_capabilities().await;
    for (issuer, parent, rights, error) in [
        (
            namespace.capability,
            source.capability_id,
            Rights::DELEGATE,
            AuthorityServiceError::Escalation,
        ),
        (
            issuer.capability_id,
            source_namespace.capability,
            Rights::DELEGATE,
            AuthorityServiceError::Escalation,
        ),
        (
            namespace.capability,
            no_narrow.capability_id,
            Rights::USE,
            AuthorityServiceError::Unauthorized,
        ),
        (
            namespace.capability,
            retracted.capability_id,
            Rights::USE,
            AuthorityServiceError::InvalidCapability,
        ),
        (
            namespace.capability,
            CapId::new_random(),
            Rights::USE,
            AuthorityServiceError::InvalidCapability,
        ),
        (
            test.root(),
            source.capability_id,
            Rights::USE,
            AuthorityServiceError::Unauthorized,
        ),
    ] {
        assert_eq!(
            test.call(MintCapability {
                issuer,
                parent: Some(parent),
                dependencies: Vec::new(),
                object: "derived".into(),
                rights,
                subject: None,
            })
            .await
            .unwrap_err(),
            error
        );
        assert_eq!(test.authority.service.live_capabilities().await, before);
    }
    test.stop.stop().await;
}

#[tokio::test]
async fn diamond_dependencies_are_deduplicated_and_depth_is_bounded_before_insertion() {
    let test = boot_authority().await;
    let namespace = test.namespace(Uuid7::new()).await;
    let mut issuer = namespace.capability;
    let mut held = test.mint(issuer, "diamond", Rights::all()).await.unwrap();
    for _ in 2..super::cap::MAX_CHAIN_DEPTH {
        issuer = test
            .attenuate(issuer, Rights::all())
            .await
            .unwrap()
            .capability_id;
        held = test
            .call(MintCapability {
                issuer,
                parent: Some(held.capability_id),
                dependencies: vec![issuer, held.capability_id, held.capability_id],
                object: "diamond".into(),
                rights: Rights::all(),
                subject: None,
            })
            .await
            .unwrap();
    }
    let before = test.authority.service.live_capabilities().await;
    for (parent, dependencies) in [
        (Some(held.capability_id), Vec::new()),
        (None, vec![held.capability_id]),
    ] {
        assert_refused!(
            test.call(MintCapability {
                issuer,
                parent,
                dependencies,
                object: "too deep".into(),
                rights: Rights::all(),
                subject: None,
            })
            .await,
            AuthorityServiceError::Escalation
        );
        assert_eq!(test.authority.service.live_capabilities().await, before);
    }
    assert_eq!(
        test.call(InspectCapability {
            capability: held.capability_id
        })
        .await
        .unwrap(),
        held
    );
    test.retract(namespace.capability, issuer).await.unwrap();
    assert_refused!(
        test.call(InspectCapability {
            capability: held.capability_id
        })
        .await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn gate_graph_union_is_bounded_and_duplicate_roots_are_stored_once() {
    let test = boot_authority().await;
    let namespace = test.namespace(Uuid7::new()).await;
    let mut gates = Vec::new();
    for _ in 0..super::cap::MAX_CHAIN_NODES - 4 {
        gates.push(
            test.mint(namespace.capability, "gate", Rights::empty())
                .await
                .unwrap()
                .capability_id,
        );
    }
    let mut branches = Vec::new();
    for dependencies in gates.chunks(gates.len() / 2) {
        branches.push(
            test.call(MintCapability {
                issuer: namespace.capability,
                parent: None,
                dependencies: dependencies.to_vec(),
                object: "branch".into(),
                rights: Rights::USE,
                subject: None,
            })
            .await
            .unwrap()
            .capability_id,
        );
    }
    let request = MintCapability {
        issuer: namespace.capability,
        parent: None,
        dependencies: branches
            .iter()
            .copied()
            .cycle()
            .take(super::cap::MAX_CHAIN_NODES * 2)
            .chain([namespace.capability])
            .collect(),
        object: "at the node limit".into(),
        rights: Rights::USE | Rights::NARROW,
        subject: None,
    };
    let accepted = test.call(request.clone()).await.unwrap();
    let accepted_id = accepted.capability_id;
    let (ancestors, dependencies) = test
        .authority
        .service
        .peek(move |state| {
            let chain = super::cap::walk(state, &accepted_id).unwrap();
            (chain.ancestors.len(), chain.cap.dependencies.clone())
        })
        .await;
    assert_eq!(ancestors + 1, super::cap::MAX_CHAIN_NODES);
    assert_eq!(
        dependencies,
        [namespace.capability, branches[0], branches[1]]
    );
    let extra = test
        .mint(namespace.capability, "extra gate", Rights::empty())
        .await
        .unwrap();
    let before = test.authority.service.live_capabilities().await;
    let mut too_wide = request;
    too_wide.dependencies.push(extra.capability_id);
    assert_refused!(
        test.call(too_wide.clone()).await,
        AuthorityServiceError::Escalation
    );
    assert_eq!(test.authority.service.live_capabilities().await, before);
    too_wide.dependencies = gates.clone();
    too_wide.dependencies.extend(branches);
    too_wide.dependencies.push(extra.capability_id);
    assert_refused!(test.call(too_wide).await, AuthorityServiceError::Escalation);
    assert_eq!(test.authority.service.live_capabilities().await, before);
    assert_refused!(
        test.attenuate(accepted.capability_id, Rights::USE).await,
        AuthorityServiceError::Escalation
    );
    assert_eq!(test.authority.service.live_capabilities().await, before);
    assert_eq!(
        test.call(InspectCapability {
            capability: accepted.capability_id
        })
        .await
        .unwrap(),
        accepted
    );
    test.retract(gates[0], gates[0]).await.unwrap();
    assert_refused!(
        test.call(InspectCapability {
            capability: accepted.capability_id
        })
        .await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn simultaneous_service_instances_share_identities_not_handles() {
    let secrets = keyring().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("authority.sqlite");
    let first_runtime = SqliteRuntime::new(RuntimeConfig::default());
    let second_runtime = SqliteRuntime::new(RuntimeConfig::default());
    let sql = first_runtime.database(&path, &[], "_test_migrations");
    let second_sql = second_runtime.database(&path, &[], "_test_migrations");
    let (first, second) = tokio::join!(
        boot(&secrets, "test-node", sql.clone()),
        boot(&secrets, "test-node", second_sql)
    );
    let (authority, stop) = first.unwrap();
    let first = TestAuthority { authority, stop };
    let subject = first.subsystem("subject").await;
    let namespace = first.namespace(subject.actor.actor_id).await;
    let held = first
        .mint(namespace.capability, "shared", Rights::all())
        .await
        .unwrap();
    let (authority, stop) = second.unwrap();
    let second = TestAuthority { authority, stop };
    let second_subject = second.subsystem("subject").await;
    assert_eq!(second_subject.actor.actor_id, subject.actor.actor_id);
    assert_eq!(second_subject.actor.public_key, subject.actor.public_key);
    assert_ne!(second_subject.capability, subject.capability);
    assert_ne!(first.root(), second.root());
    let second_namespace = second.namespace(namespace.namespace_id).await;
    assert_ne!(namespace.capability, second_namespace.capability);
    let second_held = second
        .mint(second_namespace.capability, "shared", Rights::all())
        .await
        .unwrap();
    assert_refused!(
        second
            .call(InspectCapability {
                capability: held.capability_id
            })
            .await,
        AuthorityServiceError::InvalidCapability
    );
    assert_refused!(
        first
            .call(InspectCapability {
                capability: second_held.capability_id
            })
            .await,
        AuthorityServiceError::InvalidCapability
    );
    let (third, third_stop) = boot(&secrets, "test-node", sql).await.unwrap();
    assert_eq!(
        first
            .call(InspectCapability {
                capability: held.capability_id
            })
            .await
            .unwrap(),
        held
    );
    assert_eq!(
        second
            .call(InspectCapability {
                capability: second_held.capability_id
            })
            .await
            .unwrap(),
        second_held
    );
    second
        .retract(second_namespace.capability, second_namespace.capability)
        .await
        .unwrap();
    assert_eq!(
        first
            .call(InspectCapability {
                capability: held.capability_id
            })
            .await
            .unwrap(),
        held
    );
    first
        .sign(subject.capability, datum_claim(subject.actor.actor_id, 3))
        .await
        .unwrap();
    third_stop.stop().await;
    drop(third);
    second.stop.stop().await;
    first.stop.stop().await;
}

#[tokio::test]
async fn obsolete_schemas_are_refused_without_destroying_durable_data() {
    for statement in [
        Some("ALTER TABLE authority_node ADD COLUMN root_cap BLOB"),
        Some("ALTER TABLE authority_grants ADD COLUMN granted_via BLOB"),
        Some("CREATE TABLE authority_caps (cap_id BLOB PRIMARY KEY)"),
        Some("CREATE TABLE authority_actor_issuances (cap_id BLOB PRIMARY KEY)"),
        None,
    ] {
        let secrets = keyring().await;
        let sql = test_support::db(&[]).unwrap();
        let (authority, stop) = boot(&secrets, "test-node", sql.clone()).await.unwrap();
        let first = TestAuthority { authority, stop };
        let subject = first.subsystem("subject").await;
        sql.execute_batch(
            "CREATE TABLE authority_grants (
                 issuer TEXT NOT NULL
               , subject TEXT NOT NULL REFERENCES authority_actors (uuid)
               , object TEXT NOT NULL
               , rights INTEGER NOT NULL
               , PRIMARY KEY (issuer, subject, object)
             )",
        )
        .await
        .unwrap();
        sql.execute(
            "INSERT INTO authority_grants
                  ( issuer
                  , subject
                  , object
                  , rights
                  )
             VALUES ( :issuer
                    , :subject
                    , 'kept'
                    , :rights
                    )",
            (
                Uuid7::new().to_string(),
                subject.actor.actor_id.to_string(),
                Rights::USE.bits_i64(),
            ),
        )
        .await
        .unwrap();
        if let Some(statement) = statement {
            sql.execute_batch(statement).await.unwrap();
        }
        let snapshot = async || {
            let mut tables = Vec::new();
            for query in [
                "SELECT json_group_array(json_array(
                     uuid
                   , path
                   , parent_uuid
                   , hex(public_key)
                   , hex(sealed_key)
                   , hex(key_nonce)
                   , hex(certificate)
                 ))
                   FROM authority_actors",
                "SELECT json_group_array(json_array(
                     node_id
                   , hex(public_key)
                   , hex(sealed_key)
                   , hex(key_nonce)
                   , operator_uuid
                   , actors_issuer
                 ))
                   FROM authority_node",
                "SELECT json_group_array(json_array(
                     issuer
                   , subject
                   , object
                   , rights
                 ))
                   FROM authority_grants",
                "SELECT json_group_array(json_array(
                     type
                   , name
                   , tbl_name
                   , sql
                 ))
                   FROM sqlite_schema",
            ] {
                tables.push(sql.query(query, ()).await.unwrap().one::<String>().unwrap());
            }
            tables
        };
        let before = snapshot().await;
        let result = boot(&secrets, "test-node", sql.clone()).await;
        let required = if statement.is_some() {
            "legacy durable capability schema requires explicit migration"
        } else {
            "explicit domain-grant migration"
        };
        assert!(
            matches!(result, Err(AuthorityError::StateCorrupt(ref message)) if message.contains(required))
        );
        assert_eq!(snapshot().await, before);
        first
            .sign(subject.capability, datum_claim(subject.actor.actor_id, 4))
            .await
            .unwrap();
        assert_eq!(first.subsystem("subject").await, subject);
        first.stop.stop().await;
    }
}

#[tokio::test]
async fn unversioned_identities_gain_migration_history_without_replacing_keys() {
    let secrets = keyring().await;
    let sql = test_support::db(&[]).unwrap();
    sql.execute_batch(include_str!("sqlite/migrations/001_authority.sql"))
        .await
        .unwrap();
    let wrapping = zeroize::Zeroizing::new(
        secrets
            .get_or_create_secret(
                super::NODE_SERVICE,
                &super::wrapping_key_name("test-node"),
                32,
            )
            .await
            .unwrap(),
    );
    let wrapping = zeroize::Zeroizing::new(<[u8; 32]>::try_from(wrapping.as_slice()).unwrap());
    let transaction = sql.begin_write().await.unwrap();
    let node = super::AuthorityService::first_boot(&transaction, &wrapping)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let operator = super::store::actor(&sql, node.operator)
        .await
        .unwrap()
        .unwrap();
    let claim = datum_claim(node.operator, 11);
    let signed = super::signer::sign_claim(&wrapping, &operator, claim.clone()).unwrap();
    let before = sql.query("SELECT * FROM authority_node", ()).await.unwrap();
    let (authority, stop) = boot(&secrets, "test-node", sql.clone()).await.unwrap();
    let test = TestAuthority { authority, stop };
    assert_eq!(test.authority.service.operator_uuid(), node.operator);
    assert_eq!(
        test.authority.service.actors_namespace.namespace_id,
        node.actors_issuer
    );
    assert_eq!(
        test.call(SignDatumClaim {
            via: test.root(),
            claim,
        })
        .await
        .unwrap(),
        signed
    );
    assert_eq!(
        sql.query("SELECT * FROM authority_node", ()).await.unwrap(),
        before
    );
    let versions: Vec<i64> = sql
        .query(
            "SELECT version FROM _big_cabinet_authority_migrations ORDER BY version",
            (),
        )
        .await
        .unwrap()
        .all()
        .unwrap();
    assert_eq!(versions, vec![1]);
    super::store::migrate(&sql).await.unwrap();
    assert_eq!(
        sql.query("SELECT * FROM authority_node", ()).await.unwrap(),
        before
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn obsolete_schemas_are_refused_before_recording_migrations() {
    for schema in [
        "CREATE TABLE authority_node (root_cap BLOB)",
        "CREATE TABLE authority_grants (granted_via BLOB)",
        "CREATE TABLE authority_caps (cap_id BLOB PRIMARY KEY)",
        "CREATE TABLE authority_actor_issuances (cap_id BLOB PRIMARY KEY)",
        "CREATE TABLE authority_grants (object TEXT); INSERT INTO authority_grants VALUES ('kept')",
    ] {
        let secrets = keyring().await;
        let sql = test_support::db(&[]).unwrap();
        sql.execute_batch(schema).await.unwrap();
        let before = sql.query("SELECT * FROM sqlite_schema", ()).await.unwrap();
        let result = boot(&secrets, "test-node", sql.clone()).await;
        assert!(matches!(result, Err(AuthorityError::StateCorrupt(_))));
        assert_eq!(
            sql.query("SELECT * FROM sqlite_schema", ()).await.unwrap(),
            before
        );
        assert!(
            secrets
                .get_secret(super::NODE_SERVICE, &super::wrapping_key_name("test-node"))
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn an_empty_central_grant_table_does_not_block_boot_or_get_dropped() {
    let secrets = keyring().await;
    let sql = test_support::db(&[]).unwrap();
    let (first, first_stop) = boot(&secrets, "test-node", sql.clone()).await.unwrap();
    let schema = "CREATE TABLE authority_grants (
         issuer TEXT NOT NULL
       , subject TEXT NOT NULL REFERENCES authority_actors (uuid)
       , object TEXT NOT NULL
       , rights INTEGER NOT NULL
       , PRIMARY KEY (issuer, subject, object)
     )";
    sql.execute_batch(schema).await.unwrap();
    let (second, second_stop) = boot(&secrets, "test-node", sql.clone()).await.unwrap();
    assert_ne!(first.root, second.root);
    assert_eq!(
        first.service.operator_uuid(),
        second.service.operator_uuid()
    );
    assert_eq!(first.node_issuer, second.node_issuer);
    let persisted: String = sql
        .query(
            "SELECT sql FROM sqlite_schema WHERE name = 'authority_grants'",
            (),
        )
        .await
        .unwrap()
        .one()
        .unwrap();
    assert_eq!(persisted, schema);
    second_stop.stop().await;
    first_stop.stop().await;
}

/// The protocol is one hosted handler: direct, IRPC, and XRPC callers get the
/// same answers and the same refusals.
#[tokio::test]
async fn the_protocol_answers_alike_over_every_transport() {
    const BODY_LIMIT: usize = 64 * 1024;
    let test = boot_authority().await;
    let (irpc_client, irpc_stop) = IrpcAdapter::local::<_, AuthorityProtocol>(
        AuthorityBinding::new(Arc::clone(&test.authority.host)),
        Admission::new(4, 1),
        8,
    );
    let mut router = XrpcRouter::new(BODY_LIMIT);
    AuthorityBinding::mount(
        &mut router,
        AuthorityBinding::new(Arc::clone(&test.authority.host)),
    )
    .expect("mount authority");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let (http_task, http_shutdown) = xrpc::serve(listener, router.into_router())
        .await
        .expect("start HTTP server");
    let base = url::Url::parse(&format!("http://{address}/")).expect("base URL");
    let transports = [
        ("direct", test.authority.client.clone()),
        ("irpc", Client::new(Transport::irpc(irpc_client))),
        (
            "xrpc",
            Client::new(Transport::xrpc(
                XrpcClient::new(base, BODY_LIMIT).expect("HTTP client"),
            )),
        ),
    ];

    for (name, client) in &transports {
        let actor: OpenedActor = call(
            client,
            OpenActor {
                parent: test.root(),
                path: format!("over-{name}"),
                rights: Rights::all(),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        let namespace = test.namespace(actor.actor.actor_id).await;
        let edit = call(
            client,
            MintCapability {
                issuer: namespace.capability,
                parent: None,
                dependencies: vec![actor.capability],
                object: "edit doc X".into(),
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        let inspected = call(
            client,
            InspectCapability {
                capability: edit.capability_id,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(inspected, edit, "{name}");
        let derived = call(
            client,
            AttenuateCapability {
                capability: actor.capability,
                rights: Rights::all(),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        // A handle derived from an identity capability is bound to the same actor without being
        // an identity capability itself.
        let resolved = call(
            client,
            ResolveAuthority {
                capability: derived.capability_id,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(resolved.actor, None, "{name}");
        assert_eq!(
            resolved.capability.subject,
            Some(actor.actor.actor_id),
            "{name}"
        );
        let signed = call(
            client,
            SignDatumClaim {
                via: actor.capability,
                claim: datum_claim(actor.actor.actor_id, 3),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        signed.verify().expect("core verification");
        let refusal = call(
            client,
            SignDatumClaim {
                via: edit.capability_id,
                claim: datum_claim(actor.actor.actor_id, 3),
            },
        )
        .await;
        assert!(
            matches!(refusal, Err(AuthorityServiceError::UnrecognizedActor)),
            "{name}: {refusal:?}"
        );
        call(
            client,
            RetractCapability {
                via: actor.capability,
                capability: actor.capability,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_refused!(
            call(
                client,
                InspectCapability {
                    capability: edit.capability_id
                }
            )
            .await,
            AuthorityServiceError::InvalidCapability
        );
    }

    http_shutdown
        .send(())
        .expect("HTTP server is still serving");
    http_task
        .await
        .expect("HTTP server task join")
        .expect("HTTP server exit");
    irpc_stop.stop().await;
    test.stop.stop().await;
}

#[tokio::test]
async fn cached_actor_rows_never_outlive_the_capability_that_names_them() {
    let test = boot_authority().await;
    let cached = || test.authority.service.actor_rows.lock().unwrap().len();
    assert_eq!(cached(), 0);

    // The first resolution reads the row; later ones, including through other capabilities of
    // the same actor, are served from memory without growing the cache.
    let docs = test.subsystem("docs").await;
    let resolved = test.actor_of(docs.capability).await.unwrap();
    assert_eq!(resolved, docs.actor);
    assert_eq!(cached(), 1);
    for _ in 0..3 {
        assert_eq!(test.actor_of(docs.capability).await.unwrap(), docs.actor);
        test.call(ResolveAuthority {
            capability: docs.capability,
        })
        .await
        .unwrap();
    }
    assert_eq!(cached(), 1);

    // A write after the cache is warm is visible to the next read: a new actor resolves to its
    // own row, and opening the same path again names the same identity.
    let photos = test.subsystem("photos").await;
    let photos_resolved = test.actor_of(photos.capability).await.unwrap();
    assert_eq!(photos_resolved, photos.actor);
    assert_ne!(photos.actor.actor_id, docs.actor.actor_id);
    assert_eq!(cached(), 2);

    // Retiring the actor's capability refuses it even though its row is still cached: liveness
    // is the ledger's, checked before any row lookup.
    test.retract(docs.capability, docs.capability)
        .await
        .unwrap();
    assert_eq!(cached(), 2);
    for refused in [
        test.actor_of(docs.capability).await.map(|_| ()),
        test.call(ResolveAuthority {
            capability: docs.capability,
        })
        .await
        .map(|_| ()),
        test.sign(docs.capability, datum_claim(docs.actor.actor_id, 3))
            .await
            .map(|_| ()),
    ] {
        assert_refused!(refused, AuthorityServiceError::InvalidCapability);
    }

    // Reopening the retired path names the same actor under a fresh capability, served by the
    // same cached row.
    let reopened = test.subsystem("docs").await;
    assert_eq!(reopened.actor.actor_id, docs.actor.actor_id);
    assert_ne!(reopened.capability, docs.capability);
    assert_eq!(
        test.actor_of(reopened.capability).await.unwrap(),
        reopened.actor
    );
    assert_eq!(cached(), 2);
    test.stop.stop().await;
}

/// Nodes sharing one keyring each get a wrapping key of their own, keyed by node id, and
/// deleting one node's key leaves the other's alone: the node that lost it cannot boot again
/// (its actor keys are sealed under a key that is gone), the other boots as before.
#[tokio::test]
async fn nodes_sharing_a_keyring_keep_separate_wrapping_keys() {
    let secrets = keyring().await;
    let (first_sql, second_sql) = (
        test_support::db(&[]).unwrap(),
        test_support::db(&[]).unwrap(),
    );
    let (_first, first_stop) = boot(&secrets, "node-a", first_sql.clone()).await.unwrap();
    let (_second, second_stop) = boot(&secrets, "node-b", second_sql.clone()).await.unwrap();
    first_stop.stop().await;
    second_stop.stop().await;

    assert!(
        super::delete_wrapping_key(&secrets, "node-a")
            .await
            .unwrap()
    );
    assert!(
        !super::delete_wrapping_key(&secrets, "node-a")
            .await
            .unwrap()
    );
    let refused = boot(&secrets, "node-a", first_sql.clone()).await;
    assert!(matches!(refused, Err(AuthorityError::StateCorrupt(_))));
    let (_again, again_stop) = boot(&secrets, "node-b", second_sql.clone()).await.unwrap();
    again_stop.stop().await;
}

impl TestAuthority {
    /// Mint a handle in `namespace` from `parent`, naming `subject` as the actor it is bound to.
    async fn mint_from(
        &self,
        namespace: &Namespace,
        parent: CapId,
        rights: Rights,
        subject: Option<Uuid7>,
    ) -> Result<Capability, AuthorityServiceError> {
        self.call(MintCapability {
            issuer: namespace.capability,
            parent: Some(parent),
            dependencies: Vec::new(),
            object: "bound handle".to_owned(),
            rights,
            subject,
        })
        .await
    }
}

#[tokio::test]
async fn a_minted_handle_inherits_its_parents_subject() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let namespace = test.namespace(docs.actor.actor_id).await;
    assert_eq!(
        test.call(InspectCapability {
            capability: docs.capability
        })
        .await
        .unwrap()
        .subject,
        Some(docs.actor.actor_id)
    );
    let handle = test
        .mint_from(&namespace, docs.capability, Rights::all(), None)
        .await
        .unwrap();
    assert_eq!(handle.subject, Some(docs.actor.actor_id));
    let grandchild = test
        .mint_from(&namespace, handle.capability_id, Rights::USE, None)
        .await
        .unwrap();
    assert_eq!(grandchild.subject, Some(docs.actor.actor_id));
    let attenuated = test
        .attenuate(handle.capability_id, Rights::USE)
        .await
        .unwrap();
    assert_eq!(attenuated.subject, Some(docs.actor.actor_id));
    // With no parent the handle inherits from the issuer, a namespace root, which has none.
    assert_eq!(
        test.mint(namespace.capability, "rootless", Rights::USE)
            .await
            .unwrap()
            .subject,
        None
    );
    // Naming the subject it would inherit anyway is not an introduction.
    let limited = test
        .attenuate(docs.capability, Rights::USE | Rights::NARROW)
        .await
        .unwrap();
    assert_eq!(
        test.mint_from(
            &namespace,
            limited.capability_id,
            Rights::USE,
            Some(docs.actor.actor_id)
        )
        .await
        .unwrap()
        .subject,
        Some(docs.actor.actor_id)
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn an_introduction_needs_delegate_and_a_live_descendant_of_the_introducer() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let child = test
        .open(docs.capability, "docs/child", Rights::all())
        .await
        .unwrap();
    let grandchild = test
        .open(child.capability, "docs/child/grandchild", Rights::all())
        .await
        .unwrap();
    let namespace = test.namespace(docs.actor.actor_id).await;

    // A child and a grandchild are both the introducer's own descendants.
    for descendant in [&child, &grandchild] {
        let introduced = test
            .mint_from(
                &namespace,
                docs.capability,
                Rights::USE,
                Some(descendant.actor.actor_id),
            )
            .await
            .unwrap();
        assert_eq!(introduced.subject, Some(descendant.actor.actor_id));
    }

    // Without DELEGATE the source cannot bind a handle to anyone else.
    let no_delegate = test
        .attenuate(
            docs.capability,
            Rights::USE | Rights::NARROW | Rights::MINT_CHILD,
        )
        .await
        .unwrap();
    assert_refused!(
        test.mint_from(
            &namespace,
            no_delegate.capability_id,
            Rights::USE,
            Some(child.actor.actor_id)
        )
        .await,
        AuthorityServiceError::Unauthorized
    );

    // An introduction is never wider than its source.
    assert_refused!(
        test.mint_from(
            &namespace,
            no_delegate.capability_id,
            Rights::all(),
            Some(child.actor.actor_id)
        )
        .await,
        AuthorityServiceError::Escalation
    );

    // An actor nobody has opened cannot be introduced to, even by a source that may delegate.
    assert_refused!(
        test.mint_from(&namespace, docs.capability, Rights::USE, Some(Uuid7::new()))
            .await,
        AuthorityServiceError::ActorNotFound
    );
    test.stop.stop().await;
}

/// The rule of ADR 037 question 2: a handle is trusted for its own subject's descendants and
/// nothing else, so DELEGATE never lets a holder name a sibling, an ancestor, an unrelated
/// actor or the operator, and a handle bound to no actor cannot introduce at all.
#[tokio::test]
async fn an_introduction_to_anyone_but_a_descendant_is_an_escalation_even_with_delegate() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let sibling = test.subsystem("sibling").await;
    let child = test
        .open(docs.capability, "docs/child", Rights::all())
        .await
        .unwrap();
    let grandchild = test
        .open(child.capability, "docs/child/grandchild", Rights::all())
        .await
        .unwrap();
    let docs_namespace = test.namespace(docs.actor.actor_id).await;
    let operator = test.authority.service.operator_uuid();

    // From `docs`: its sibling and the operator above it.
    for named in [sibling.actor.actor_id, operator] {
        assert_refused!(
            test.mint_from(&docs_namespace, docs.capability, Rights::USE, Some(named))
                .await,
            AuthorityServiceError::Escalation
        );
    }
    // From `child`: its parent, its uncle, and the operator; its own child is fine.
    let child_namespace = test.namespace(child.actor.actor_id).await;
    for named in [docs.actor.actor_id, sibling.actor.actor_id, operator] {
        assert_refused!(
            test.mint_from(&child_namespace, child.capability, Rights::USE, Some(named))
                .await,
            AuthorityServiceError::Escalation
        );
    }
    assert_eq!(
        test.mint_from(
            &child_namespace,
            child.capability,
            Rights::USE,
            Some(grandchild.actor.actor_id)
        )
        .await
        .unwrap()
        .subject,
        Some(grandchild.actor.actor_id)
    );
    // A descendant does not reach back up through its own handle.
    assert_refused!(
        test.mint_from(
            &child_namespace,
            grandchild.capability,
            Rights::USE,
            Some(child.actor.actor_id)
        )
        .await,
        AuthorityServiceError::Escalation
    );
    // The operator introduces any other actor, because all of them descend from it.
    let operator_namespace = test.namespace(operator).await;
    assert_eq!(
        test.mint_from(
            &operator_namespace,
            test.root(),
            Rights::USE,
            Some(grandchild.actor.actor_id)
        )
        .await
        .unwrap()
        .subject,
        Some(grandchild.actor.actor_id)
    );
    // A namespace root is bound to no actor, so it has no descendants to introduce.
    assert_refused!(
        test.call(MintCapability {
            issuer: docs_namespace.capability,
            parent: None,
            dependencies: Vec::new(),
            object: "rootless".to_owned(),
            rights: Rights::USE,
            subject: Some(child.actor.actor_id),
        })
        .await,
        AuthorityServiceError::Escalation
    );
    // Refusals take the missing right first: without DELEGATE it is Unauthorized, never a hint
    // about who the named actor is related to.
    let no_delegate = test
        .attenuate(docs.capability, Rights::USE | Rights::NARROW)
        .await
        .unwrap();
    assert_refused!(
        test.mint_from(
            &docs_namespace,
            no_delegate.capability_id,
            Rights::USE,
            Some(sibling.actor.actor_id)
        )
        .await,
        AuthorityServiceError::Unauthorized
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn retracting_the_last_identity_capability_invalidates_handles_introduced_to_the_actor() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let other = test
        .open(docs.capability, "docs/other", Rights::all())
        .await
        .unwrap();
    // A second identity capability for the same actor: a different grant, so a different row.
    let second = test
        .open(
            docs.capability,
            "docs/other",
            Rights::USE | Rights::MINT_CHILD,
        )
        .await
        .unwrap();
    assert_eq!(second.actor.actor_id, other.actor.actor_id);
    assert_ne!(second.capability, other.capability);
    let namespace = test.namespace(docs.actor.actor_id).await;
    let introduced = test
        .mint_from(
            &namespace,
            docs.capability,
            Rights::USE,
            Some(other.actor.actor_id),
        )
        .await
        .unwrap();

    test.retract(other.capability, other.capability)
        .await
        .unwrap();
    test.call(InspectCapability {
        capability: introduced.capability_id,
    })
    .await
    .expect("one identity capability is still live");

    test.retract(second.capability, second.capability)
        .await
        .unwrap();
    assert_refused!(
        test.call(InspectCapability {
            capability: introduced.capability_id,
        })
        .await,
        AuthorityServiceError::InvalidCapability
    );
    // Introducing to the actor is refused until it has a live identity capability again.
    assert_refused!(
        test.mint_from(
            &namespace,
            docs.capability,
            Rights::USE,
            Some(other.actor.actor_id)
        )
        .await,
        AuthorityServiceError::ActorNotFound
    );
    let reopened = test
        .open(docs.capability, "docs/other", Rights::all())
        .await
        .unwrap();
    assert_eq!(reopened.actor.actor_id, other.actor.actor_id);
    test.mint_from(
        &namespace,
        docs.capability,
        Rights::USE,
        Some(other.actor.actor_id),
    )
    .await
    .expect("the reopened actor can be introduced to");
    // The retracted handle stays dead: liveness is a new row, not a revived one.
    assert_refused!(
        test.call(InspectCapability {
            capability: introduced.capability_id,
        })
        .await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn signing_by_handle_signs_as_its_subject_and_refuses_any_other_actor() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let other = test
        .open(docs.capability, "docs/other", Rights::all())
        .await
        .unwrap();
    let namespace = test.namespace(docs.actor.actor_id).await;
    let introduced = test
        .mint_from(
            &namespace,
            docs.capability,
            Rights::USE,
            Some(other.actor.actor_id),
        )
        .await
        .unwrap();
    let signed = test
        .call(SignDatumClaim {
            via: introduced.capability_id,
            claim: datum_claim(other.actor.actor_id, 1),
        })
        .await
        .unwrap();
    signed.verify().unwrap();
    assert_eq!(signed.actor_public_key, other.actor.public_key);
    // The handle's subject is `other`, so a claim for its introducer is refused.
    assert_refused!(
        test.sign(
            introduced.capability_id,
            datum_claim(docs.actor.actor_id, 1)
        )
        .await,
        AuthorityServiceError::Unauthorized
    );
    // A handle without USE does not sign, even for its own subject.
    let narrow_only = test
        .mint_from(
            &namespace,
            docs.capability,
            Rights::NARROW,
            Some(other.actor.actor_id),
        )
        .await
        .unwrap();
    assert_refused!(
        test.sign(
            narrow_only.capability_id,
            datum_claim(other.actor.actor_id, 1)
        )
        .await,
        AuthorityServiceError::Unauthorized
    );
    // A handle bound to nobody has no one to sign as.
    let unbound = test
        .mint(namespace.capability, "unbound", Rights::USE)
        .await
        .unwrap();
    assert_refused!(
        test.sign(unbound.capability_id, datum_claim(docs.actor.actor_id, 1))
            .await,
        AuthorityServiceError::UnrecognizedActor
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn resolving_an_actor_by_id_needs_no_capability() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    assert_eq!(
        test.call(ResolveActor {
            actor_id: docs.actor.actor_id
        })
        .await
        .unwrap(),
        docs.actor
    );
    assert_refused!(
        test.call(ResolveActor {
            actor_id: Uuid7::new()
        })
        .await,
        AuthorityServiceError::ActorNotFound
    );
    test.stop.stop().await;
}
