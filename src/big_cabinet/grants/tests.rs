use super::*;
use crate::authority::Authority;
use crate::authority::{self};
use crate::capability::DomainCapabilities;
use big_cabinet_core::authority::AttenuateCapability;
use big_cabinet_core::authority::Capability;
use secrets_rs::SecretStore;
use service_utils_rs::StopToken;
use std::sync::Arc;

struct Fixture {
    sql: Database,
    secrets: SecretStore,
    authority: Authority,
    namespace: Namespace,
    grants: Grants,
    stop: StopToken,
}

impl Fixture {
    async fn new() -> Self {
        let sql = sqlite_rs::test_support::db(&[]).unwrap().clone();
        let store = keyring_core::mock::Store::new().unwrap() as Arc<keyring_core::CredentialStore>;
        let secrets = SecretStore::boot_with_store(store).await.unwrap();
        let (authority, stop) = authority::boot(&secrets, sql.clone()).await.unwrap();
        let namespace = authority
            .service
            .open_namespace(Uuid7::new())
            .await
            .unwrap();
        let grants = Grants::open(sql.clone(), authority.client.clone(), namespace)
            .await
            .unwrap();
        Self {
            sql,
            secrets,
            authority,
            namespace,
            grants,
            stop,
        }
    }

    async fn proof(&self, subject: Uuid7, object: &str) -> GrantProof {
        self.grants
            .matching(&RequestContext::root(), subject, &[object.into()])
            .await
            .unwrap()
            .pop()
            .unwrap()
    }

    async fn mint(
        &self,
        dependencies: Vec<CapId>,
    ) -> Result<Capability, ClientError<AuthorityServiceError>> {
        self.authority
            .client
            .call(
                RequestContext::root(),
                MintCapability {
                    issuer: self.namespace.capability,
                    parent: None,
                    dependencies,
                    object: "scoped resource".into(),
                    rights: Rights::USE | Rights::NARROW,
                },
            )
            .await
    }

    async fn assert_dead(&self, capability: CapId) {
        assert!(matches!(
            self.authority
                .client
                .call(RequestContext::root(), InspectCapability { capability })
                .await,
            Err(ClientError::Operation(
                AuthorityServiceError::InvalidCapability
            ))
        ));
    }
}

#[tokio::test]
async fn batched_entitlements_are_exact_and_namespace_local() {
    let fixture = Fixture::new().await;
    let context = RequestContext::root();
    let subject = Uuid7::new();
    fixture
        .grants
        .grant(&context, subject, "document A".into(), Rights::USE, None)
        .await
        .unwrap();
    let proofs = fixture
        .grants
        .matching(
            &context,
            subject,
            &[
                "document A".into(),
                "document B".into(),
                "document A".into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        proofs.iter().map(|proof| proof.rights).collect::<Vec<_>>(),
        vec![Rights::USE, Rights::empty(), Rights::USE]
    );
    assert_eq!(proofs[0].dependencies, proofs[2].dependencies);
    assert!(proofs[1].dependencies.is_empty());
    assert!(
        fixture
            .proof(Uuid7::new(), "document A")
            .await
            .rights
            .is_empty()
    );
    let namespace = fixture
        .authority
        .service
        .open_namespace(Uuid7::new())
        .await
        .unwrap();
    let other = Grants::open(
        fixture.sql.clone(),
        fixture.authority.client.clone(),
        namespace,
    )
    .await
    .unwrap();
    assert!(
        other
            .matching(&context, subject, &["document A".into()])
            .await
            .unwrap()[0]
            .rights
            .is_empty()
    );
    fixture.stop.stop().await;
}

#[tokio::test]
async fn withdrawal_invalidates_old_proofs_and_transitive_derivations() {
    let fixture = Fixture::new().await;
    let context = RequestContext::root();
    let subject = Uuid7::new();
    fixture
        .grants
        .grant(&context, subject, "document".into(), Rights::USE, None)
        .await
        .unwrap();
    let proof = fixture.proof(subject, "document").await;
    let minted = fixture.mint(proof.dependencies.clone()).await.unwrap();
    let narrowed = fixture
        .authority
        .client
        .call(
            context.child(),
            AttenuateCapability {
                capability: minted.capability_id,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    let other = fixture
        .authority
        .service
        .open_namespace(Uuid7::new())
        .await
        .unwrap();
    let cross_domain = fixture
        .authority
        .client
        .call(
            context.child(),
            MintCapability {
                issuer: other.capability,
                parent: Some(minted.capability_id),
                dependencies: Vec::new(),
                object: "another domain".into(),
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    fixture
        .grants
        .revoke(&context, subject, &["document".into()])
        .await
        .unwrap();
    for capability in [
        minted.capability_id,
        narrowed.capability_id,
        cross_domain.capability_id,
    ] {
        fixture.assert_dead(capability).await;
    }
    assert!(matches!(
        fixture.mint(proof.dependencies).await,
        Err(ClientError::Operation(
            AuthorityServiceError::InvalidCapability
        ))
    ));
    assert!(fixture.proof(subject, "document").await.rights.is_empty());
    fixture
        .grants
        .grant(&context, subject, "document".into(), Rights::USE, None)
        .await
        .unwrap();
    fixture
        .mint(fixture.proof(subject, "document").await.dependencies)
        .await
        .unwrap();
    fixture.assert_dead(minted.capability_id).await;
    fixture.stop.stop().await;
}

#[tokio::test]
async fn completed_withdrawal_cannot_be_raced_by_publication() {
    let fixture = Fixture::new().await;
    let context = RequestContext::root();
    let subject = Uuid7::new();
    for index in 0..16 {
        let object = format!("document {index}");
        fixture
            .grants
            .grant(&context, subject, object.clone(), Rights::USE, None)
            .await
            .unwrap();
        let proof = fixture.proof(subject, &object).await;
        let objects = [object];
        let (minted, revoked) = tokio::join!(
            fixture.mint(proof.dependencies),
            fixture.grants.revoke(&context, subject, &objects),
        );
        revoked.unwrap();
        match minted {
            Ok(capability) => fixture.assert_dead(capability.capability_id).await,
            Err(ClientError::Operation(AuthorityServiceError::InvalidCapability)) => {}
            Err(error) => panic!("unexpected mint failure: {error}"),
        }
    }
    fixture.stop.stop().await;
}

#[tokio::test]
async fn grant_changes_retract_before_waiting_for_the_shared_writer() {
    use big_cabinet_core::authority::CapabilityStatus;
    use big_cabinet_core::authority::ObserveCapability;
    use futures::StreamExt;

    let fixture = Fixture::new().await;
    let context = RequestContext::root();
    let subject = Uuid7::new();
    for replacement in [Some(Rights::USE | Rights::NARROW), None] {
        fixture
            .grants
            .grant(&context, subject, "document".into(), Rights::USE, None)
            .await
            .unwrap();
        let capability = fixture
            .mint(fixture.proof(subject, "document").await.dependencies)
            .await
            .unwrap();
        let mut events = fixture
            .authority
            .client
            .subscribe(
                context.child(),
                ObserveCapability {
                    capability: capability.capability_id,
                },
            )
            .await
            .unwrap();
        assert_eq!(events.next().await, Some(Ok(CapabilityStatus::Live)));
        let writer = fixture.sql.begin_write().await.unwrap();
        let change = async {
            match replacement {
                Some(rights) => {
                    fixture
                        .grants
                        .grant(&context, subject, "document".into(), rights, None)
                        .await
                }
                None => {
                    fixture
                        .grants
                        .revoke(&context, subject, &["document".into()])
                        .await
                }
            }
        };
        tokio::pin!(change);
        tokio::select! {
            result = &mut change => panic!("grant change completed while writer was leased: {result:?}"),
            event = events.next() => assert_eq!(event, Some(Ok(CapabilityStatus::Retracted))),
        }
        assert_eq!(
            sqlite::get_grant(
                &fixture.sql,
                fixture.namespace.namespace_id.to_string(),
                subject.to_string(),
                "document".into(),
            )
            .await
            .unwrap(),
            Some(Rights::USE.bits_i64()),
        );
        writer.rollback().await.unwrap();
        change.await.unwrap();
        assert_eq!(
            fixture.proof(subject, "document").await.rights,
            replacement.unwrap_or_else(Rights::empty),
        );
        fixture.assert_dead(capability.capability_id).await;
    }
    fixture.stop.stop().await;
}

#[tokio::test]
async fn temporary_delegation_tracks_its_source_without_promoting_to_durable() {
    let fixture = Fixture::new().await;
    let context = RequestContext::root();
    let subject = Uuid7::new();
    let source = fixture
        .authority
        .client
        .call(
            context.child(),
            MintCapability {
                issuer: fixture.namespace.capability,
                parent: None,
                dependencies: Vec::new(),
                object: "delegation source".into(),
                rights: Rights::all(),
            },
        )
        .await
        .unwrap();
    fixture
        .grants
        .grant(
            &context,
            subject,
            "temporary".into(),
            Rights::USE,
            Some(source.capability_id),
        )
        .await
        .unwrap();
    fixture
        .grants
        .grant(&context, subject, "durable".into(), Rights::USE, None)
        .await
        .unwrap();
    fixture
        .grants
        .grant(
            &context,
            subject,
            "durable".into(),
            Rights::USE,
            Some(source.capability_id),
        )
        .await
        .unwrap();
    let temporary = fixture
        .mint(fixture.proof(subject, "temporary").await.dependencies)
        .await
        .unwrap();
    let durable = fixture
        .mint(fixture.proof(subject, "durable").await.dependencies)
        .await
        .unwrap();
    fixture
        .authority
        .client
        .call(
            context.child(),
            RetractCapability {
                via: source.capability_id,
                capability: source.capability_id,
            },
        )
        .await
        .unwrap();
    fixture.assert_dead(temporary.capability_id).await;
    assert!(fixture.proof(subject, "temporary").await.rights.is_empty());
    fixture
        .authority
        .client
        .call(
            context.child(),
            InspectCapability {
                capability: durable.capability_id,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        sqlite::get_grant(
            &fixture.sql,
            fixture.namespace.namespace_id.to_string(),
            subject.to_string(),
            "temporary".into(),
        )
        .await
        .unwrap(),
        None,
    );
    let weak = fixture.mint(Vec::new()).await.unwrap();
    assert!(matches!(
        fixture
            .grants
            .grant(
                &context,
                subject,
                "refused".into(),
                Rights::USE,
                Some(weak.capability_id)
            )
            .await,
        Err(GrantError::Authority(ClientError::Operation(
            AuthorityServiceError::Escalation
        )))
    ));
    fixture.stop.stop().await;
}

#[tokio::test]
async fn restart_preserves_standing_grants_without_adopting_live_handles() {
    let fixture = Fixture::new().await;
    let context = RequestContext::root();
    let subject = Uuid7::new();
    fixture
        .grants
        .grant(&context, subject, "durable".into(), Rights::USE, None)
        .await
        .unwrap();
    let source = fixture
        .authority
        .client
        .call(
            context.child(),
            MintCapability {
                issuer: fixture.namespace.capability,
                parent: None,
                dependencies: Vec::new(),
                object: "delegation source".into(),
                rights: Rights::all(),
            },
        )
        .await
        .unwrap();
    fixture
        .grants
        .grant(
            &context,
            subject,
            "temporary".into(),
            Rights::USE,
            Some(source.capability_id),
        )
        .await
        .unwrap();
    let old = fixture
        .mint(fixture.proof(subject, "durable").await.dependencies)
        .await
        .unwrap();
    let (other, stop) = authority::boot(&fixture.secrets, fixture.sql.clone())
        .await
        .unwrap();
    let namespace = other
        .service
        .open_namespace(fixture.namespace.namespace_id)
        .await
        .unwrap();
    let grants = Grants::open(fixture.sql.clone(), other.client.clone(), namespace)
        .await
        .unwrap();
    let proofs = grants
        .matching(&context, subject, &["durable".into(), "temporary".into()])
        .await
        .unwrap();
    assert_eq!(
        proofs.iter().map(|proof| proof.rights).collect::<Vec<_>>(),
        vec![Rights::USE, Rights::empty()]
    );
    assert!(matches!(
        other
            .client
            .call(
                context.child(),
                InspectCapability {
                    capability: old.capability_id
                }
            )
            .await,
        Err(ClientError::Operation(
            AuthorityServiceError::InvalidCapability
        ))
    ));
    fixture
        .authority
        .client
        .call(
            context.child(),
            InspectCapability {
                capability: old.capability_id,
            },
        )
        .await
        .unwrap();
    fixture
        .authority
        .client
        .call(
            context.child(),
            InspectCapability {
                capability: source.capability_id,
            },
        )
        .await
        .unwrap();
    stop.stop().await;
    fixture.stop.stop().await;
}

#[tokio::test]
async fn domain_shutdown_retires_grant_support_even_if_authority_stays_alive() {
    let fixture = Fixture::new().await;
    let context = RequestContext::root();
    let subject = Uuid7::new();
    fixture
        .grants
        .grant(&context, subject, "document".into(), Rights::USE, None)
        .await
        .unwrap();
    let old = fixture
        .mint(fixture.proof(subject, "document").await.dependencies)
        .await
        .unwrap();
    DomainCapabilities::new(fixture.authority.client.clone(), fixture.namespace, "test/")
        .stop_token()
        .stop()
        .await;
    fixture.assert_dead(old.capability_id).await;
    let namespace = fixture
        .authority
        .service
        .open_namespace(fixture.namespace.namespace_id)
        .await
        .unwrap();
    assert_ne!(namespace.capability, fixture.namespace.capability);
    let grants = Grants::open(
        fixture.sql.clone(),
        fixture.authority.client.clone(),
        namespace,
    )
    .await
    .unwrap();
    let proof = grants
        .matching(&context, subject, &["document".into()])
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(proof.rights, Rights::USE);
    fixture
        .authority
        .client
        .call(
            context.child(),
            MintCapability {
                issuer: namespace.capability,
                parent: None,
                dependencies: proof.dependencies,
                object: "reacquired".into(),
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    assert!(
        fixture
            .grants
            .matching(&context, subject, &["document".into()])
            .await
            .is_err()
    );
    fixture.stop.stop().await;
}

#[tokio::test]
async fn replay_does_not_retract_but_replacement_does() {
    let fixture = Fixture::new().await;
    let context = RequestContext::root();
    let subject = Uuid7::new();
    fixture
        .grants
        .grant(&context, subject, "document".into(), Rights::USE, None)
        .await
        .unwrap();
    let minted = fixture
        .mint(fixture.proof(subject, "document").await.dependencies)
        .await
        .unwrap();
    fixture
        .grants
        .grant(&context, subject, "document".into(), Rights::USE, None)
        .await
        .unwrap();
    fixture
        .authority
        .client
        .call(
            context.child(),
            InspectCapability {
                capability: minted.capability_id,
            },
        )
        .await
        .unwrap();
    fixture
        .grants
        .grant(
            &context,
            subject,
            "document".into(),
            Rights::USE | Rights::NARROW,
            None,
        )
        .await
        .unwrap();
    fixture.assert_dead(minted.capability_id).await;
    assert_eq!(
        fixture.proof(subject, "document").await.rights,
        Rights::USE | Rights::NARROW
    );
    fixture.stop.stop().await;
}
