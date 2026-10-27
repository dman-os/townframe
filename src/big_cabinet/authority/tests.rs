//! Service-level tests: every assertion goes through `Client<AuthorityProtocol>`,
//! so they exercise the same hosted path a real consumer uses.

use super::AuthorityMessage;
use super::{
    Actor, AttenuateCapability, Authority, AuthorityError, AuthorityProtocol,
    AuthorityServiceError, CapId, Capability, CheckGrant, GrantAccess, InspectCapability,
    MintCapability, OPERATOR_OBJECT, OpenActor, ResolveActor, RetractCapability, RevokeAccess,
    Rights, SignFacetClaim, boot,
};
use big_cabinet_core::FacetKey;
use big_cabinet_core::dmeta::{
    ActorMetadata, Attribution, Cid, ClaimOperation, DocId, FacetClaim, Uuid7,
};
use irpc::{Channels, WithChannels, channel::none::NoReceiver};
use secrets_rs::SecretStore;
use service_utils_rs::{
    Admission, Client, ClientError, Envelope, IrpcAdapter, Operation, Reply, RequestContext, Route,
    StopToken, XrpcClient, XrpcRouter, xrpc,
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

    async fn mint(
        &self,
        issuer: CapId,
        object: &str,
        rights: Rights,
    ) -> Result<Capability, AuthorityServiceError> {
        self.call(MintCapability {
            issuer,
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

    async fn grant(
        &self,
        via: CapId,
        subject: Uuid7,
        object: &str,
        rights: Rights,
    ) -> Result<(), AuthorityServiceError> {
        self.call(GrantAccess {
            via,
            subject,
            object: object.to_owned(),
            rights,
        })
        .await
    }

    async fn revoke(
        &self,
        via: CapId,
        subject: Uuid7,
        object: &str,
    ) -> Result<(), AuthorityServiceError> {
        self.call(RevokeAccess {
            via,
            subject,
            object: object.to_owned(),
        })
        .await
    }

    async fn check(&self, issuer: &Actor, subject: &Actor, object: &str) -> Rights {
        self.call(CheckGrant {
            via: issuer.capability,
            subject: subject.uuid,
            object: object.to_owned(),
        })
        .await
        .expect("grant check")
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
            id: test.root(),
            object: OPERATOR_OBJECT.to_owned(),
            rights: Rights::all(),
            issuer: root.issuer,
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
    assert_eq!(operator.parent_actor, None);
    assert_refused!(
        test.retract(test.root(), test.root()).await,
        AuthorityServiceError::Unauthorized
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
        actor.parent_actor,
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
        actor.uuid,
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
    assert_eq!(plugs.parent_actor, Some(runtime.uuid));

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
    assert_eq!(narrower.uuid, first.uuid);
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
    assert_eq!(rotated.uuid, first.uuid);
    assert_eq!(rotated.certificate, first.certificate);
    assert_ne!(rotated.capability, first.capability);
    test.stop.stop().await;
}

/// Identity, signing, naming, and opening children are an actor's own powers.
/// A capability derived from an actor — however it was derived — is not that
/// actor, so a holder of a domain capability can never act as the domain.
#[tokio::test]
async fn only_an_actors_own_capability_acts_as_it() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let edit = test
        .mint(docs.capability, "edit doc X", OBJECT)
        .await
        .unwrap();
    assert_eq!(edit.issuer, docs.uuid);

    assert_refused!(
        test.sign(edit.id, facet_claim(docs.uuid, 1)).await,
        AuthorityServiceError::UnrecognizedActor
    );
    assert_refused!(
        test.call(ResolveActor {
            capability: edit.id
        })
        .await,
        AuthorityServiceError::UnrecognizedActor
    );
    assert_refused!(
        test.open(edit.id, "docs/sneaky", OBJECT).await,
        AuthorityServiceError::UnrecognizedActor
    );
    assert_refused!(
        test.mint(edit.id, "edit doc Y", Rights::USE).await,
        AuthorityServiceError::UnrecognizedActor
    );
    assert_refused!(
        test.call(CheckGrant {
            via: edit.id,
            subject: docs.uuid,
            object: "edit doc X".into(),
        })
        .await,
        AuthorityServiceError::UnrecognizedActor
    );

    // An actor capability without `use` does not identify or sign either.
    let opener = test
        .open(test.root(), "docs", Rights::MINT_CHILD)
        .await
        .unwrap();
    assert_refused!(
        test.sign(opener.capability, facet_claim(docs.uuid, 2))
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
    let edit = test
        .mint(docs.capability, "edit doc X", Rights::USE | Rights::NARROW)
        .await
        .unwrap();
    let copy = test.attenuate(edit.id, Rights::USE).await.unwrap();
    assert_eq!(
        copy,
        Capability {
            id: copy.id,
            object: edit.object.clone(),
            rights: Rights::USE,
            issuer: docs.uuid,
        }
    );

    assert_refused!(
        test.attenuate(edit.id, Rights::USE | Rights::DELEGATE)
            .await,
        AuthorityServiceError::Escalation
    );
    assert_refused!(
        test.attenuate(copy.id, Rights::USE).await,
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
        .mint(docs.capability, &registered.object, Rights::all())
        .await
        .unwrap();
    assert_eq!(derived.object, registered.object);
    assert_eq!(derived.issuer, registered.issuer);
    assert_eq!(named.object, registered.object);
    assert_ne!(named.issuer, registered.issuer);
    for counterfeit in [derived, named] {
        assert_refused!(
            test.call(ResolveActor {
                capability: counterfeit.id
            })
            .await,
            AuthorityServiceError::UnrecognizedActor
        );
        assert_refused!(
            test.sign(counterfeit.id, facet_claim(docs.uuid, 4)).await,
            AuthorityServiceError::UnrecognizedActor
        );
        assert_refused!(
            test.open(counterfeit.id, "docs/fake", Rights::all()).await,
            AuthorityServiceError::UnrecognizedActor
        );
        assert_refused!(
            test.mint(counterfeit.id, "edit doc Y", Rights::USE).await,
            AuthorityServiceError::UnrecognizedActor
        );
        assert_refused!(
            test.call(CheckGrant {
                via: counterfeit.id,
                subject: docs.uuid,
                object: "edit doc X".into()
            })
            .await,
            AuthorityServiceError::UnrecognizedActor
        );
    }
    let root_copy = test.attenuate(test.root(), Rights::all()).await.unwrap();
    assert_refused!(
        test.open(root_copy.id, "fake", Rights::all()).await,
        AuthorityServiceError::UnrecognizedActor
    );
    // An actor's objects never carry more than its own capability does.
    let reader = test
        .open(test.root(), "reader", Rights::USE | Rights::NARROW)
        .await
        .unwrap();
    assert_refused!(
        test.mint(reader.capability, "read doc X", Rights::DELEGATE)
            .await,
        AuthorityServiceError::Escalation
    );
    let limited = test
        .open(test.root(), "limited", Rights::USE | Rights::MINT_CHILD)
        .await
        .unwrap();
    assert_refused!(
        test.mint(limited.capability, "read doc X", Rights::USE)
            .await,
        AuthorityServiceError::Unauthorized
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn inspection_exposes_uuid_handles_but_not_parent_authority() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let edit = test
        .mint(docs.capability, "edit doc X", Rights::USE)
        .await
        .unwrap();
    let wire = serde_json::to_value(&edit).unwrap();
    assert_eq!(wire.as_object().unwrap().len(), 4);
    assert!(wire.get("parent").is_none());
    assert!(wire.get("kind").is_none());
    let id = uuid::Uuid::parse_str(wire["id"].as_str().unwrap()).unwrap();
    assert_eq!(id.get_version_num(), 4);
    assert_eq!(id.to_string(), wire["id"].as_str().unwrap());
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
    assert_eq!(format!("{:?}", edit.id), "CapId([redacted])");
    let issuer_as_cap = serde_json::from_value::<CapId>(wire["issuer"].clone()).unwrap();
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
    let docs = test.subsystem("docs").await;
    let mut held = test
        .mint(docs.capability, "edit doc X", OBJECT)
        .await
        .unwrap();
    for _ in 3..64 {
        held = test.attenuate(held.id, OBJECT).await.unwrap();
    }
    assert_refused!(
        test.attenuate(held.id, OBJECT).await,
        AuthorityServiceError::Escalation
    );
    assert_eq!(
        test.call(InspectCapability {
            capability: held.id
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
    let docs = test.subsystem("docs").await;
    let other = test.subsystem("other").await;
    let edit = test
        .mint(docs.capability, "edit doc X", OBJECT)
        .await
        .unwrap();
    let copy = test.attenuate(edit.id, Rights::USE).await.unwrap();
    let held = test.attenuate(edit.id, Rights::USE).await.unwrap();

    assert_refused!(
        test.retract(other.capability, edit.id).await,
        AuthorityServiceError::Unauthorized
    );
    assert_refused!(
        test.retract(copy.id, edit.id).await,
        AuthorityServiceError::Unauthorized
    );
    // A holder may give up its own capability.
    test.retract(held.id, held.id).await.unwrap();

    test.retract(docs.capability, edit.id).await.unwrap();
    for gone in [edit.id, copy.id, held.id] {
        assert_refused!(
            test.call(InspectCapability { capability: gone }).await,
            AuthorityServiceError::InvalidCapability
        );
    }
    // Retracting an actor's capability takes its objects with it.
    let survivor = test
        .mint(docs.capability, "edit doc Y", OBJECT)
        .await
        .unwrap();
    test.retract(test.root(), docs.capability).await.unwrap();
    assert_refused!(
        test.call(InspectCapability {
            capability: survivor.id
        })
        .await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn grants_live_in_the_issuers_namespace_and_match_exactly() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let drawers = test.subsystem("drawers").await;
    let plugin = test.subsystem("plugin").await;

    assert_eq!(
        test.check(&docs, &plugin, "drawer D").await,
        Rights::empty()
    );
    test.grant(docs.capability, plugin.uuid, "drawer D", Rights::USE)
        .await
        .unwrap();
    assert_eq!(test.check(&docs, &plugin, "drawer D").await, Rights::USE);
    // Exact object, exact subject, and only in the namespace that wrote it.
    assert_eq!(
        test.check(&docs, &plugin, "drawer E").await,
        Rights::empty()
    );
    assert_eq!(
        test.check(&docs, &drawers, "drawer D").await,
        Rights::empty()
    );
    assert_eq!(
        test.check(&drawers, &plugin, "drawer D").await,
        Rights::empty()
    );

    // Granting again through the same capability replaces what it recorded.
    test.grant(
        docs.capability,
        plugin.uuid,
        "drawer D",
        Rights::USE | Rights::NARROW,
    )
    .await
    .unwrap();
    assert_eq!(
        test.check(&docs, &plugin, "drawer D").await,
        Rights::USE | Rights::NARROW
    );

    assert_refused!(
        test.grant(docs.capability, Uuid7::new(), "drawer D", Rights::USE)
            .await,
        AuthorityServiceError::ActorNotFound
    );
    let derived = test
        .attenuate(docs.capability, Rights::all())
        .await
        .unwrap();
    assert_refused!(
        test.grant(derived.id, plugin.uuid, "drawer D", Rights::USE)
            .await,
        AuthorityServiceError::UnrecognizedActor
    );
    assert_refused!(
        test.revoke(derived.id, plugin.uuid, "drawer D").await,
        AuthorityServiceError::UnrecognizedActor
    );
    let quiet = test
        .open(test.root(), "quiet", Rights::USE | Rights::NARROW)
        .await
        .unwrap();
    assert_refused!(
        test.grant(quiet.capability, plugin.uuid, "drawer D", Rights::USE)
            .await,
        AuthorityServiceError::Unauthorized
    );

    test.revoke(docs.capability, plugin.uuid, "drawer D")
        .await
        .unwrap();
    assert_eq!(
        test.check(&docs, &plugin, "drawer D").await,
        Rights::empty()
    );
    test.stop.stop().await;
}

/// A grant written through an object capability is that capability's
/// delegation: it is bounded by it, revocable through it, and withdrawn with it.
#[tokio::test]
async fn delegated_grants_stand_while_the_capability_they_came_through_lives() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    let plugin = test.subsystem("plugin").await;
    let helper = test.subsystem("helper").await;
    // What docs hands a user who owns drawer D.
    let owner = test
        .mint(
            docs.capability,
            "drawer D",
            Rights::USE | Rights::NARROW | Rights::DELEGATE,
        )
        .await
        .unwrap();

    test.grant(owner.id, plugin.uuid, "drawer D", Rights::USE)
        .await
        .unwrap();
    assert_eq!(test.check(&docs, &plugin, "drawer D").await, Rights::USE);
    assert_refused!(
        test.grant(owner.id, plugin.uuid, "drawer E", Rights::USE)
            .await,
        AuthorityServiceError::Escalation
    );
    let user = test
        .attenuate(owner.id, Rights::USE | Rights::DELEGATE)
        .await
        .unwrap();
    assert_refused!(
        test.grant(user.id, plugin.uuid, "drawer D", Rights::NARROW)
            .await,
        AuthorityServiceError::Escalation
    );

    // Revoking through a capability reaches grants written through what was
    // derived from it, and nothing else.
    test.grant(user.id, helper.uuid, "drawer D", Rights::USE)
        .await
        .unwrap();
    test.grant(docs.capability, helper.uuid, "drawer D", Rights::NARROW)
        .await
        .unwrap();
    assert_eq!(
        test.check(&docs, &helper, "drawer D").await,
        Rights::USE | Rights::NARROW
    );
    test.revoke(owner.id, helper.uuid, "drawer D")
        .await
        .unwrap();
    assert_eq!(test.check(&docs, &helper, "drawer D").await, Rights::NARROW);

    // Retracting the capability withdraws what was granted through it; the
    // issuer's own grant stands.
    test.grant(docs.capability, plugin.uuid, "drawer D", Rights::NARROW)
        .await
        .unwrap();
    test.retract(docs.capability, owner.id).await.unwrap();
    assert_eq!(test.check(&docs, &plugin, "drawer D").await, Rights::NARROW);
    test.stop.stop().await;
}

#[tokio::test]
async fn signing_binds_the_claim_to_the_presenting_actor() {
    let test = boot_authority().await;
    let docs = test.subsystem("docs").await;
    test.sign(docs.capability, facet_claim(docs.uuid, 7))
        .await
        .expect("signature");
    let signed = test
        .call(SignFacetClaim {
            actor: docs.capability,
            claim: facet_claim(docs.uuid, 8),
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
        test.sign(forged, facet_claim(docs.uuid, 1)).await,
        AuthorityServiceError::InvalidCapability
    );
    assert_refused!(
        test.grant(forged, docs.uuid, "drawer D", Rights::USE).await,
        AuthorityServiceError::InvalidCapability
    );
    test.stop.stop().await;
}

#[tokio::test]
async fn actors_capabilities_and_grants_survive_a_reopen() {
    let secrets = keyring().await;
    let directory = tempfile::tempdir().expect("authority state directory");
    let url = format!(
        "sqlite://{}",
        directory.path().join("authority.sqlite").display()
    );

    let (first, stop) = boot(&secrets, SqlCtx::url(&url).await.unwrap())
        .await
        .expect("boot");
    let first = TestAuthority {
        authority: first,
        stop,
    };
    let docs = first.subsystem("docs").await;
    let plugin = first.subsystem("plugin").await;
    let edit = first
        .mint(docs.capability, "edit doc X", OBJECT)
        .await
        .unwrap();
    first
        .grant(docs.capability, plugin.uuid, "drawer D", Rights::USE)
        .await
        .unwrap();
    let derived_identity = first
        .attenuate(docs.capability, Rights::all())
        .await
        .unwrap();
    let identity = first
        .call(InspectCapability {
            capability: docs.capability,
        })
        .await
        .unwrap();
    let operator = first.authority.service.operator_uuid();
    let issuer = first.authority.node_issuer.clone();
    let root = first.root();
    first.stop.stop().await;

    let (reopened, stop) = boot(&secrets, SqlCtx::url(&url).await.unwrap())
        .await
        .expect("reopen");
    let reopened = TestAuthority {
        authority: reopened,
        stop,
    };
    assert_eq!(reopened.root(), root);
    assert_eq!(reopened.authority.service.operator_uuid(), operator);
    assert_eq!(reopened.authority.node_issuer, issuer);
    assert_eq!(reopened.subsystem("docs").await, docs);
    assert_eq!(
        reopened
            .call(InspectCapability {
                capability: docs.capability
            })
            .await
            .unwrap(),
        identity
    );
    assert_refused!(
        reopened
            .call(ResolveActor {
                capability: derived_identity.id
            })
            .await,
        AuthorityServiceError::UnrecognizedActor
    );
    reopened
        .call(InspectCapability {
            capability: edit.id,
        })
        .await
        .expect("capability survives");
    assert_eq!(
        reopened.check(&docs, &plugin, "drawer D").await,
        Rights::USE
    );
    reopened
        .sign(docs.capability, facet_claim(docs.uuid, 9))
        .await
        .expect("signing survives");
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

/// The protocol is one hosted handler: direct, IRPC, and XRPC callers get the
/// same answers and the same refusals.
#[tokio::test]
async fn the_protocol_answers_alike_over_every_transport() {
    const BODY_LIMIT: usize = 64 * 1024;
    let test = boot_authority().await;
    let (irpc_client, irpc_stop) = IrpcAdapter::local::<_, AuthorityProtocol>(
        Arc::clone(&test.authority.host),
        Admission::new(4, 1),
        8,
    );
    let mut router = XrpcRouter::new(BODY_LIMIT);
    AuthorityProtocol::mount(&mut router, Arc::clone(&test.authority.host))
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
        let edit = call(
            client,
            MintCapability {
                issuer: actor.capability,
                object: "edit doc X".into(),
                rights: Rights::USE,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        let inspected = call(
            client,
            InspectCapability {
                capability: edit.id,
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
                    capability: derived.id
                }
            )
            .await,
            AuthorityServiceError::UnrecognizedActor
        );
        let signed = call(
            client,
            SignFacetClaim {
                actor: actor.capability,
                claim: facet_claim(actor.uuid, 3),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        signed.verify().expect("core verification");
        assert_eq!(
            call(
                client,
                CheckGrant {
                    via: actor.capability,
                    subject: actor.uuid,
                    object: "edit doc X".into(),
                }
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}")),
            Rights::empty(),
            "{name}"
        );
        let refusal = call(
            client,
            SignFacetClaim {
                actor: edit.id,
                claim: facet_claim(actor.uuid, 3),
            },
        )
        .await;
        assert!(
            matches!(refusal, Err(AuthorityServiceError::UnrecognizedActor)),
            "{name}: {refusal:?}"
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
