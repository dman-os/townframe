//! Service-level tests: every assertion goes through `Client<AuthorityProtocol>`,
//! so they exercise the same hosted path a real consumer uses.

use super::{
    Actor, AttenuateCapability, Authority, AuthorityError, AuthorityProtocol,
    AuthorityServiceError, CapId, Capability, InspectCapability, MintCapability, Namespace,
    OPERATOR_OBJECT, OpenActor, ResolveActor, ResolveAuthority, ResolvedAuthority,
    RetractCapability, Rights, SignFacetClaim, boot,
};
use super::{AuthorityBinding, AuthorityMessage};
use big_cabinet_core::FacetKey;
use big_cabinet_core::dmeta::{
    ActorMetadata, Attribution, Cid, ClaimOperation, DocId, FacetClaim, Uuid7,
};
use irpc::{Channels, WithChannels, channel::none::NoReceiver};
use secrets_rs::SecretStore;
use service_utils_rs::{
    Admission, Client, ClientError, Envelope, IrpcAdapter, Operation, Reply, RequestContext,
    ServerBinding, StopToken, XrpcClient, XrpcRouter, xrpc,
};
use sqlx_utils_rs::SqlCtx;
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
    let sql = SqlCtx::memory().await.expect("authority database");
    let (authority, stop) = boot(&keyring().await, sql).await.expect("authority boot");
    TestAuthority { authority, stop }
}

async fn call<P>(
    client: &Client<AuthorityProtocol>,
    params: P,
) -> Result<P::Output, AuthorityServiceError>
where
    P: Operation<Output: service_utils_rs::Wire, Error = AuthorityServiceError>,
    AuthorityProtocol: From<Envelope<P>>,
    AuthorityMessage: From<WithChannels<Envelope<P>, AuthorityProtocol>>,
    Envelope<P>: Channels<AuthorityProtocol, Tx = Reply<P>, Rx = NoReceiver>,
{
    client
        .call(RequestContext::root(), params)
        .await
        .map_err(|error| match error {
            ClientError::Operation(error) => error,
            ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
        })
}

impl TestAuthority {
    async fn call<P>(&self, params: P) -> Result<P::Output, AuthorityServiceError>
    where
        P: Operation<Output: service_utils_rs::Wire, Error = AuthorityServiceError>,
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
    ) -> Result<Actor, AuthorityServiceError> {
        self.call(OpenActor {
            parent,
            path: path.to_owned(),
            rights,
        })
        .await
    }

    /// An actor under the operator holding every right.
    async fn subsystem(&self, path: &str) -> Actor {
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

    async fn sign(&self, actor: CapId, claim: FacetClaim) -> Result<(), AuthorityServiceError> {
        let signed = self
            .call(SignFacetClaim {
                actor,
                claim: claim.clone(),
            })
            .await?;
        // Core verifies it with no knowledge of this service.
        signed.verify().expect("core verification");
        assert_eq!(signed.claim, claim);
        Ok(())
    }
}

fn facet_claim(actor: Uuid7, seed: u8) -> FacetClaim {
    let mut digest = vec![0x01, 0x55, 0x1e, 0x20];
    digest.extend([seed; 32]);
    FacetClaim {
        origin_document: DocId::new(b"mutation-document"),
        facet_key: FacetKey::from_text("org.example.note", "main").expect("facet key"),
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
        }
    );
    let operator = test
        .call(ResolveActor {
            capability: test.root(),
        })
        .await
        .expect("operator");
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
        test.call(ResolveActor {
            capability: test.root()
        })
        .await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn an_opened_actor_carries_a_verifiable_attribution_certificate() {
    let test = boot_authority().await;
    let actor = test.subsystem("docs").await;
    assert_eq!(
        actor.url,
        format!("db+actor://{}/docs", test.authority.node_issuer)
    );
    assert_eq!(
        actor.parent_actor_id,
        Some(test.authority.service.operator_uuid())
    );
    assert!(actor.public_key.to_string().starts_with('z'));

    // The certificate is exactly what dmeta stores, and it verifies against the
    // issuer a verifier is told to expect — not against its own `iss`.
    let metadata = ActorMetadata {
        pubkey: actor.public_key.clone(),
        actor_url: actor.url.clone(),
        delegation: Some(actor.certificate.clone()),
    };
    Attribution::verify(
        &actor.certificate,
        &test.authority.node_issuer,
        actor.actor_id,
        &metadata,
    )
    .expect("attribution certificate");

    let resolved = test
        .call(ResolveActor {
            capability: actor.capability,
        })
        .await
        .expect("resolve");
    assert_eq!(resolved, actor);
    test.stop.stop().await;
}

#[tokio::test]
async fn actor_paths_extend_their_parent_by_exactly_one_segment() {
    let test = boot_authority().await;
    let runtime = test.subsystem("rt").await;
    let plugs = test
        .open(runtime.capability, "rt/plugs", Rights::all())
        .await
        .expect("nested actor");
    assert!(plugs.url.ends_with("/rt/plugs"), "{}", plugs.url);
    assert_eq!(plugs.parent_actor_id, Some(runtime.actor_id));

    for (parent, path) in [
        (runtime.capability, "other"),
        (runtime.capability, "rt2"),
        (runtime.capability, "rt"),
        (runtime.capability, "rt/"),
        (runtime.capability, "rt/plugs/ocr"),
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
    assert_eq!(narrower.actor_id, first.actor_id);
    assert_eq!(narrower.public_key, first.public_key);
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
        test.call(ResolveActor {
            capability: first.capability
        })
        .await,
        AuthorityServiceError::InvalidCapability
    );
    let rotated = test.open(test.root(), "docs", Rights::all()).await.unwrap();
    assert_eq!(rotated.actor_id, first.actor_id);
    assert_eq!(rotated.certificate, first.certificate);
    assert_ne!(rotated.capability, first.capability);
    test.stop.stop().await;
}

#[tokio::test]
async fn only_an_actors_own_capability_acts_as_it() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let namespace = test.namespace(docs.actor_id).await;
    assert_refused!(
        test.mint(docs.capability, "edit doc X", OBJECT).await,
        AuthorityServiceError::Unauthorized
    );
    let edit = test
        .mint(namespace.capability, "edit doc X", OBJECT)
        .await
        .unwrap();
    assert_eq!(edit.issuer_id, docs.actor_id);

    assert_refused!(
        test.sign(edit.capability_id, facet_claim(docs.actor_id, 1))
            .await,
        AuthorityServiceError::UnrecognizedActor
    );
    assert_refused!(
        test.call(ResolveActor {
            capability: edit.capability_id
        })
        .await,
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
        test.sign(opener.capability, facet_claim(docs.actor_id, 2))
            .await,
        AuthorityServiceError::Unauthorized
    );
    assert_refused!(
        test.call(ResolveActor {
            capability: opener.capability
        })
        .await,
        AuthorityServiceError::Unauthorized
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn capabilities_only_attenuate_and_labels_cannot_forge_issuance() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let namespace = test.namespace(docs.actor_id).await;
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
            issuer_id: docs.actor_id,
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
    for counterfeit in [derived, named] {
        assert_refused!(
            test.call(ResolveActor {
                capability: counterfeit.capability_id
            })
            .await,
            AuthorityServiceError::UnrecognizedActor
        );
        assert_refused!(
            test.sign(counterfeit.capability_id, facet_claim(docs.actor_id, 4))
                .await,
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
    assert_eq!(wire.as_object().unwrap().len(), 4);
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
    let before = test.authority.service.state.lock().await.caps.len();
    assert_eq!(test.namespace(namespace.namespace_id).await, namespace);
    assert_eq!(test.authority.service.state.lock().await.caps.len(), before);
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
    assert_eq!(
        test.authority.service.state.lock().await.caps.len(),
        before - 3
    );
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
            actor: Some(actor)
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
    test.sign(docs.capability, facet_claim(docs.actor_id, 7))
        .await
        .expect("signature");
    let signed = test
        .call(SignFacetClaim {
            actor: docs.capability,
            claim: facet_claim(docs.actor_id, 8),
        })
        .await
        .unwrap();
    assert_eq!(signed.actor_public_key, docs.public_key);

    // A claim naming another actor is refused, including the actor's parent.
    for actor in [Uuid7::new(), test.authority.service.operator_uuid()] {
        assert_refused!(
            test.sign(docs.capability, facet_claim(actor, 7)).await,
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
        test.call(ResolveActor { capability: forged }).await,
        AuthorityServiceError::InvalidCapability
    );
    assert_refused!(
        test.sign(forged, facet_claim(docs.actor_id, 1)).await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn reboot_loses_handles_but_preserves_actors_and_keys() {
    let secrets = keyring().await;
    let directory = tempfile::tempdir().expect("authority state directory");
    let url = format!(
        "sqlite://{}",
        directory.path().join("authority.sqlite").display()
    );
    let sql = SqlCtx::url(&url).await.unwrap();
    let (authority, stop) = boot(&secrets, sql.clone()).await.unwrap();
    let first = TestAuthority { authority, stop };
    let docs = first.subsystem("docs").await;
    let plugin = first.subsystem("plugin").await;
    let namespace = first.namespace(docs.actor_id).await;
    let edit = first
        .mint(namespace.capability, "edit doc X", OBJECT)
        .await
        .unwrap();
    let derived_identity = first
        .attenuate(docs.capability, Rights::all())
        .await
        .unwrap();
    let claim = facet_claim(docs.actor_id, 9);
    let signature = first
        .call(SignFacetClaim {
            actor: docs.capability,
            claim: claim.clone(),
        })
        .await
        .unwrap();
    let operator = first.authority.service.operator_uuid();
    let issuer = first.authority.node_issuer.clone();
    let root = first.root();
    first.stop.stop().await;
    drop(first.authority);

    let (authority, stop) = boot(&secrets, SqlCtx::url(&url).await.unwrap())
        .await
        .unwrap();
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
            reopened.call(ResolveActor { capability }).await,
            AuthorityServiceError::InvalidCapability
        );
    }
    let reopened_docs = reopened.subsystem("docs").await;
    assert_ne!(reopened_docs.capability, docs.capability);
    assert_eq!(
        reopened_docs,
        Actor {
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
        .call(SignFacetClaim {
            actor: reopened_docs.capability,
            claim,
        })
        .await
        .unwrap();
    assert_eq!(reopened_signature, signature);
    reopened_signature.verify().unwrap();
    let forbidden: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_schema
          WHERE name IN ('authority_caps', 'authority_actor_issuances', 'authority_grants')",
    )
    .fetch_one(&sql.read_pool)
    .await
    .unwrap();
    assert_eq!(forbidden, 0);
    let durable_root: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pragma_table_info('authority_node') WHERE name = 'root_cap'",
    )
    .fetch_one(&sql.read_pool)
    .await
    .unwrap();
    assert_eq!(durable_root, 0);
    reopened.stop.stop().await;

    // The sealed keys open only under the wrapping key that sealed them, so a
    // keyring that lost it is refused rather than given fresh identities.
    let refused = boot(&keyring().await, SqlCtx::url(&url).await.unwrap()).await;
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
            }
        );
        assert_refused!(
            test.call(ResolveActor {
                capability: derived.capability_id,
            })
            .await,
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
    let before = test.authority.service.state.lock().await.caps.len();
    let changes = test.authority.service.changes.subscribe();
    for dependency in [CapId::new_random(), revoked.capability, gate.capability_id] {
        assert_refused!(
            test.call(MintCapability {
                issuer: namespace.capability,
                parent: None,
                dependencies: vec![signer.capability, dependency],
                object: "rejected".into(),
                rights: Rights::all(),
            })
            .await,
            AuthorityServiceError::InvalidCapability
        );
        assert_eq!(test.authority.service.state.lock().await.caps.len(), before);
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
            })
            .await,
            AuthorityServiceError::Unauthorized
        );
        assert_eq!(test.authority.service.state.lock().await.caps.len(), before);
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
        let locked = service.state.lock().await;
        let before = locked.caps.len();
        let mint = service.call(Envelope::new(
            RequestContext::root(),
            MintCapability {
                issuer: namespace.capability,
                parent: None,
                dependencies: vec![gate.capability_id],
                object: "dependent".into(),
                rights: Rights::all(),
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
        drop(locked);
        let (minted, retracted) = tokio::join!(mint, retract);
        retracted.unwrap();
        // Retraction deletes the gate and whatever depends on it, so either order leaves the
        // table one row smaller than before the race.
        if retract_first {
            assert!(matches!(
                minted,
                Err(AuthorityServiceError::InvalidCapability)
            ));
            assert_eq!(service.state.lock().await.caps.len(), before - 1);
        } else {
            let minted = minted.unwrap().output;
            assert_eq!(service.state.lock().await.caps.len(), before - 1);
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
    let before = test.authority.service.state.lock().await.caps.len();
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
            })
            .await
            .unwrap_err(),
            error
        );
        assert_eq!(test.authority.service.state.lock().await.caps.len(), before);
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
            })
            .await
            .unwrap();
    }
    let before = test.authority.service.state.lock().await.caps.len();
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
            })
            .await,
            AuthorityServiceError::Escalation
        );
        assert_eq!(test.authority.service.state.lock().await.caps.len(), before);
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
    };
    let accepted = test.call(request.clone()).await.unwrap();
    {
        let state = test.authority.service.state.lock().await;
        let chain = super::cap::walk(&state, &accepted.capability_id).unwrap();
        assert_eq!(chain.ancestors.len() + 1, super::cap::MAX_CHAIN_NODES);
        assert_eq!(
            chain.cap.dependencies,
            [namespace.capability, branches[0], branches[1]]
        );
    }
    let extra = test
        .mint(namespace.capability, "extra gate", Rights::empty())
        .await
        .unwrap();
    let before = test.authority.service.state.lock().await.caps.len();
    let mut too_wide = request;
    too_wide.dependencies.push(extra.capability_id);
    assert_refused!(
        test.call(too_wide.clone()).await,
        AuthorityServiceError::Escalation
    );
    assert_eq!(test.authority.service.state.lock().await.caps.len(), before);
    too_wide.dependencies = gates.clone();
    too_wide.dependencies.extend(branches);
    too_wide.dependencies.push(extra.capability_id);
    assert_refused!(test.call(too_wide).await, AuthorityServiceError::Escalation);
    assert_eq!(test.authority.service.state.lock().await.caps.len(), before);
    assert_refused!(
        test.attenuate(accepted.capability_id, Rights::USE).await,
        AuthorityServiceError::Escalation
    );
    assert_eq!(test.authority.service.state.lock().await.caps.len(), before);
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
    let sql = SqlCtx::memory().await.unwrap();
    let (first, second) = tokio::join!(boot(&secrets, sql.clone()), boot(&secrets, sql.clone()));
    let (authority, stop) = first.unwrap();
    let first = TestAuthority { authority, stop };
    let subject = first.subsystem("subject").await;
    let namespace = first.namespace(subject.actor_id).await;
    let held = first
        .mint(namespace.capability, "shared", Rights::all())
        .await
        .unwrap();
    let (authority, stop) = second.unwrap();
    let second = TestAuthority { authority, stop };
    let second_subject = second.subsystem("subject").await;
    assert_eq!(second_subject.actor_id, subject.actor_id);
    assert_eq!(second_subject.public_key, subject.public_key);
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
    let (third, third_stop) = boot(&secrets, sql).await.unwrap();
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
        .sign(subject.capability, facet_claim(subject.actor_id, 3))
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
        let sql = SqlCtx::memory().await.unwrap();
        let (authority, stop) = boot(&secrets, sql.clone()).await.unwrap();
        let first = TestAuthority { authority, stop };
        let subject = first.subsystem("subject").await;
        sqlx::query(
            "CREATE TABLE authority_grants (
                 issuer TEXT NOT NULL
               , subject TEXT NOT NULL REFERENCES authority_actors (uuid)
               , object TEXT NOT NULL
               , rights INTEGER NOT NULL
               , PRIMARY KEY (issuer, subject, object)
             )",
        )
        .execute(&sql.write_pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO authority_grants
                  ( issuer
                  , subject
                  , object
                  , rights
                  )
             VALUES (?1, ?2, 'kept', ?3)",
        )
        .bind(Uuid7::new().to_string())
        .bind(subject.actor_id.to_string())
        .bind(Rights::USE.bits_i64())
        .execute(&sql.write_pool)
        .await
        .unwrap();
        if let Some(statement) = statement {
            sqlx::query(statement)
                .execute(&sql.write_pool)
                .await
                .unwrap();
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
                tables.push(
                    sqlx::query_scalar::<_, String>(query)
                        .fetch_one(&sql.read_pool)
                        .await
                        .unwrap(),
                );
            }
            tables
        };
        let before = snapshot().await;
        let result = boot(&secrets, sql.clone()).await;
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
            .sign(subject.capability, facet_claim(subject.actor_id, 4))
            .await
            .unwrap();
        assert_eq!(first.subsystem("subject").await, subject);
        first.stop.stop().await;
    }
}

#[tokio::test]
async fn an_empty_central_grant_table_does_not_block_boot_or_get_dropped() {
    let secrets = keyring().await;
    let sql = SqlCtx::memory().await.unwrap();
    let (first, first_stop) = boot(&secrets, sql.clone()).await.unwrap();
    let schema = "CREATE TABLE authority_grants (
         issuer TEXT NOT NULL
       , subject TEXT NOT NULL REFERENCES authority_actors (uuid)
       , object TEXT NOT NULL
       , rights INTEGER NOT NULL
       , PRIMARY KEY (issuer, subject, object)
     )";
    sqlx::query(schema).execute(&sql.write_pool).await.unwrap();
    let (second, second_stop) = boot(&secrets, sql.clone()).await.unwrap();
    assert_ne!(first.root, second.root);
    assert_eq!(
        first.service.operator_uuid(),
        second.service.operator_uuid()
    );
    assert_eq!(first.node_issuer, second.node_issuer);
    let persisted: String =
        sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE name = 'authority_grants'")
            .fetch_one(&sql.read_pool)
            .await
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
        ("irpc", Client::irpc(irpc_client)),
        (
            "xrpc",
            Client::xrpc(XrpcClient::new(base, BODY_LIMIT).expect("HTTP client")),
        ),
    ];

    for (name, client) in &transports {
        let actor: Actor = call(
            client,
            OpenActor {
                parent: test.root(),
                path: format!("over-{name}"),
                rights: Rights::all(),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        let namespace = test.namespace(actor.actor_id).await;
        let edit = call(
            client,
            MintCapability {
                issuer: namespace.capability,
                parent: None,
                dependencies: vec![actor.capability],
                object: "edit doc X".into(),
                rights: Rights::USE,
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
        assert_refused!(
            call(
                client,
                ResolveActor {
                    capability: derived.capability_id
                }
            )
            .await,
            AuthorityServiceError::UnrecognizedActor
        );
        let signed = call(
            client,
            SignFacetClaim {
                actor: actor.capability,
                claim: facet_claim(actor.actor_id, 3),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        signed.verify().expect("core verification");
        let refusal = call(
            client,
            SignFacetClaim {
                actor: edit.capability_id,
                claim: facet_claim(actor.actor_id, 3),
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
