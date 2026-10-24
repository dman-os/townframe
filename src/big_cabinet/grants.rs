use crate::authority::{
    AuthorityProtocol, AuthorityServiceError, CapId, InspectCapability, MintCapability, Namespace,
    RetractCapability, Rights,
};
use big_cabinet_core::dmeta::Uuid7;
use service_utils_rs::{Client, ClientError, RequestContext};
use sqlx::Row;
use sqlx_utils_rs::SqlCtx;
use std::collections::HashMap;
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum GrantError {
    #[error("grant storage failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("grant capability failed: {0}")]
    Authority(#[from] ClientError<AuthorityServiceError>),
}

#[derive(Default)]
struct State {
    gates: HashMap<(Uuid7, String), CapId>,
    temporary: HashMap<(Uuid7, String), HashMap<CapId, Rights>>,
}

pub struct Grants {
    sql: SqlCtx,
    authority: Client<AuthorityProtocol>,
    namespace: Namespace,
    state: Mutex<State>,
}

pub struct GrantProof {
    pub rights: Rights,
    pub dependencies: Vec<CapId>,
}

impl Grants {
    pub async fn open(
        sql: SqlCtx,
        authority: Client<AuthorityProtocol>,
        namespace: Namespace,
    ) -> Result<Self, GrantError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS cabinet_domain_grants (
                 namespace TEXT NOT NULL
               , subject TEXT NOT NULL
               , object TEXT NOT NULL
               , rights INTEGER NOT NULL
               , PRIMARY KEY (namespace, subject, object)
             )",
        )
        .execute(&sql.write_pool)
        .await?;
        Ok(Self {
            sql,
            authority,
            namespace,
            state: Mutex::new(State::default()),
        })
    }

    async fn ensure_live(&self, context: &RequestContext) -> Result<(), GrantError> {
        self.authority
            .call(
                context.child(),
                InspectCapability {
                    capability: self.namespace.capability,
                },
            )
            .await?;
        Ok(())
    }

    async fn retract_gate(
        &self,
        context: &RequestContext,
        state: &mut State,
        key: &(Uuid7, String),
    ) -> Result<(), GrantError> {
        if let Some(capability) = state.gates.get(key).copied() {
            self.authority
                .call(
                    context.child(),
                    RetractCapability {
                        via: self.namespace.capability,
                        capability,
                    },
                )
                .await?;
            state.gates.remove(key);
        }
        Ok(())
    }

    pub async fn grant(
        &self,
        context: &RequestContext,
        subject: Uuid7,
        object: String,
        rights: Rights,
        source: Option<CapId>,
    ) -> Result<(), GrantError> {
        let mut state = self.state.lock().await;
        self.ensure_live(context).await?;
        let key = (subject, object);
        if let Some(source) = source {
            let cap = self
                .authority
                .call(context.child(), InspectCapability { capability: source })
                .await?;
            if !cap.rights.contains(Rights::DELEGATE) || !rights.is_subset_of(cap.rights) {
                return Err(ClientError::Operation(AuthorityServiceError::Escalation).into());
            }
            if state
                .temporary
                .get(&key)
                .and_then(|grants| grants.get(&source))
                == Some(&rights)
            {
                return Ok(());
            }
            self.retract_gate(context, &mut state, &key).await?;
            state
                .temporary
                .entry(key)
                .or_default()
                .insert(source, rights);
        } else {
            let mut transaction = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            let existing: Option<i64> = sqlx::query_scalar(
                "SELECT rights
                   FROM cabinet_domain_grants
                  WHERE namespace = ?1
                    AND subject = ?2
                    AND object = ?3",
            )
            .bind(self.namespace.namespace_id.to_string())
            .bind(subject.to_string())
            .bind(&key.1)
            .fetch_optional(&mut *transaction)
            .await?;
            if existing == Some(rights.bits_i64()) {
                transaction.commit().await?;
                return Ok(());
            }
            self.retract_gate(context, &mut state, &key).await?;
            sqlx::query(
                "INSERT INTO cabinet_domain_grants
                       ( namespace
                       , subject
                       , object
                       , rights
                       )
                 VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (namespace, subject, object)
                     DO UPDATE SET rights = excluded.rights",
            )
            .bind(self.namespace.namespace_id.to_string())
            .bind(subject.to_string())
            .bind(&key.1)
            .bind(rights.bits_i64())
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
        Ok(())
    }

    pub async fn revoke(
        &self,
        context: &RequestContext,
        subject: Uuid7,
        objects: &[String],
    ) -> Result<(), GrantError> {
        let mut state = self.state.lock().await;
        self.ensure_live(context).await?;
        let mut transaction = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        for object in objects {
            let key = (subject, object.clone());
            self.retract_gate(context, &mut state, &key).await?;
            state.temporary.remove(&key);
        }
        sqlx::query(
            "DELETE FROM cabinet_domain_grants
              WHERE namespace = ?1
                AND subject = ?2
                AND object IN (SELECT value FROM json_each(?3))",
        )
        .bind(self.namespace.namespace_id.to_string())
        .bind(subject.to_string())
        .bind(serde_json::to_string(objects).expect("grant objects serialize"))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn matching(
        &self,
        context: &RequestContext,
        subject: Uuid7,
        objects: &[String],
    ) -> Result<Vec<GrantProof>, GrantError> {
        let mut state = self.state.lock().await;
        self.ensure_live(context).await?;
        let rows = sqlx::query(
            "SELECT requested.value AS object
                  , COALESCE(grants.rights, 0) AS rights
               FROM json_each(?3) AS requested
               LEFT JOIN cabinet_domain_grants AS grants
                 ON grants.namespace = ?1
                AND grants.subject = ?2
                AND grants.object = requested.value
              ORDER BY CAST(requested.key AS INTEGER)",
        )
        .bind(self.namespace.namespace_id.to_string())
        .bind(subject.to_string())
        .bind(serde_json::to_string(objects).expect("grant objects serialize"))
        .fetch_all(&self.sql.read_pool)
        .await?;
        let mut proofs = Vec::with_capacity(rows.len());
        for row in rows {
            let object: String = row.try_get("object")?;
            let key = (subject, object);
            let mut rights = Rights::from_bits(row.try_get("rights")?)
                .expect("domain grant rights are validated at insertion");
            let mut dependencies = Vec::new();
            if let Some(grants) = state.temporary.get_mut(&key) {
                let mut sources: Vec<_> = grants
                    .iter()
                    .map(|(source, held)| (*source, *held))
                    .collect();
                sources.sort_unstable_by_key(|(source, _)| *source.as_bytes());
                for (source, held) in sources {
                    if held.is_subset_of(rights) {
                        continue;
                    }
                    match self
                        .authority
                        .call(context.child(), InspectCapability { capability: source })
                        .await
                    {
                        Ok(_) => {
                            rights = rights | held;
                            dependencies.push(source);
                        }
                        Err(ClientError::Operation(AuthorityServiceError::InvalidCapability)) => {
                            grants.remove(&source);
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            if !rights.is_empty() {
                let gate = match state.gates.get(&key).copied() {
                    Some(gate) => gate,
                    None => {
                        let cap = self
                            .authority
                            .call(
                                context.child(),
                                MintCapability {
                                    issuer: self.namespace.capability,
                                    parent: None,
                                    dependencies: Vec::new(),
                                    object: format!("grant gate {}", Uuid7::new()),
                                    rights: Rights::all(),
                                },
                            )
                            .await?;
                        assert!(state.gates.insert(key, cap.capability_id).is_none());
                        cap.capability_id
                    }
                };
                dependencies.push(gate);
            }
            proofs.push(GrantProof {
                rights,
                dependencies,
            });
        }
        Ok(proofs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::{self, AttenuateCapability, Authority, Capability};
    use crate::capability::DomainCapabilities;
    use secrets_rs::SecretStore;
    use service_utils_rs::StopToken;
    use std::sync::Arc;

    struct Fixture {
        sql: SqlCtx,
        secrets: SecretStore,
        authority: Authority,
        namespace: Namespace,
        grants: Grants,
        stop: StopToken,
    }

    impl Fixture {
        async fn new() -> Self {
            let sql = SqlCtx::memory().await.unwrap();
            let store =
                keyring_core::mock::Store::new().unwrap() as Arc<keyring_core::CredentialStore>;
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
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM cabinet_domain_grants WHERE object = 'temporary'",
        )
        .fetch_one(&fixture.sql.read_pool)
        .await
        .unwrap();
        assert_eq!(count, 0);
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
}
