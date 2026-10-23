use super::*;
use sqlx::Row as _;
use std::collections::BTreeMap;

impl AuthorityService {
    pub(super) async fn check_grants(
        &self,
        request: CheckGrants,
    ) -> Result<Vec<Rights>, AuthorityError> {
        let mut transaction = self.read().await?;
        let via = cap::walk(&mut transaction, &request.via).await?;
        let issuer = actor::recognize(&mut transaction, &via, Rights::USE).await?;
        let objects = serde_json::to_string(&request.objects).expect("object strings serialize");
        let rows = sqlx::query(
            r#"WITH RECURSIVE candidates AS (
                   SELECT object
                        , granted_via
                        , rights
                     FROM authority_grants
                    WHERE issuer = ?1
                      AND subject = ?2
                      AND object IN (SELECT value FROM json_each(?3))
               ), seeds AS (
                   SELECT DISTINCT granted_via
                     FROM candidates
                    WHERE granted_via IS NOT NULL
               ), lineage(granted_via, parent_id, retracted, depth) AS (
                   SELECT seeds.granted_via
                        , caps.parent_id
                        , caps.retracted
                        , 0
                     FROM seeds
                     JOIN authority_caps AS caps ON caps.cap_id = seeds.granted_via
                   UNION ALL
                   SELECT lineage.granted_via
                        , caps.parent_id
                        , caps.retracted
                        , lineage.depth + 1
                     FROM lineage
                     JOIN authority_caps AS caps ON caps.cap_id = lineage.parent_id
                    WHERE lineage.depth + 1 < ?4
               ), live AS (
                   SELECT granted_via
                     FROM lineage
                    GROUP BY granted_via
                   HAVING MAX(retracted) = 0
                      AND MAX(parent_id IS NULL) = 1
               )
               SELECT object
                    , rights
                 FROM candidates
                WHERE granted_via IS NULL
                   OR granted_via IN (SELECT granted_via FROM live)"#,
        )
        .bind(issuer.to_string())
        .bind(request.subject_id.to_string())
        .bind(objects)
        .bind(i64::try_from(cap::MAX_CHAIN_DEPTH).expect("chain depth fits i64"))
        .fetch_all(&mut *transaction)
        .await?;
        let mut held = BTreeMap::<String, Rights>::new();
        for row in rows {
            let object: String = row.try_get("object")?;
            let bits: i64 = row.try_get("rights")?;
            let rights = Rights::from_bits(bits)
                .ok_or_else(|| AuthorityError::StateCorrupt(format!("rights bits {bits:#x}")))?;
            let entry = held.entry(object).or_default();
            *entry = *entry | rights;
        }
        Ok(request
            .objects
            .iter()
            .map(|object| held.get(object).copied().unwrap_or_default())
            .collect())
    }
}

impl Handler<CheckGrants> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<CheckGrants>,
    ) -> Result<ResponseEnvelope<Vec<Rights>>, AuthorityServiceError> {
        Ok(self.check_grants(request.params).await?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use service_utils_rs::RequestContext;

    async fn fixture() -> (Authority, StopToken) {
        let keyring =
            keyring_core::mock::Store::new().unwrap() as Arc<keyring_core::CredentialStore>;
        let secrets = SecretStore::boot_with_store(keyring).await.unwrap();
        boot(&secrets, SqlCtx::memory().await.unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn resolution_only_recognizes_exact_registered_identity_issuances() {
        let (authority, stop) = fixture().await;
        let context = RequestContext::root();
        let root = authority
            .client
            .call(
                context.child(),
                ResolveActor {
                    capability: authority.root,
                },
            )
            .await
            .unwrap();
        let resolved = authority
            .client
            .call(
                context.child(),
                ResolveAuthority {
                    capability: authority.root,
                },
            )
            .await
            .unwrap();
        assert_eq!(resolved.actor, Some(root));
        assert_eq!(resolved.capability.capability_id, authority.root);
        let forged = authority
            .client
            .call(
                context.child(),
                MintCapability {
                    issuer: authority.root,
                    object: OPERATOR_OBJECT.to_owned(),
                    rights: Rights::all(),
                },
            )
            .await
            .unwrap();
        let resolved = authority
            .client
            .call(
                context.child(),
                ResolveAuthority {
                    capability: forged.capability_id,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            resolved,
            ResolvedAuthority {
                capability: forged,
                actor: None
            }
        );
        let attenuated = authority
            .client
            .call(
                context.child(),
                AttenuateCapability {
                    capability: authority.root,
                    rights: Rights::USE | Rights::NARROW,
                },
            )
            .await
            .unwrap();
        let resolved = authority
            .client
            .call(
                context.child(),
                ResolveAuthority {
                    capability: attenuated.capability_id,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            resolved,
            ResolvedAuthority {
                capability: attenuated.clone(),
                actor: None
            }
        );
        authority
            .client
            .call(
                context.child(),
                RetractCapability {
                    via: authority.root,
                    capability: attenuated.capability_id,
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            authority
                .client
                .call(
                    context.child(),
                    ResolveAuthority {
                        capability: attenuated.capability_id,
                    }
                )
                .await,
            Err(service_utils_rs::ClientError::Operation(
                AuthorityServiceError::InvalidCapability
            )),
        ));
        stop.stop().await;
    }

    #[tokio::test]
    async fn grant_batch_preserves_order_namespace_and_delegation_liveness() {
        let (authority, stop) = fixture().await;
        let context = RequestContext::root();
        let subject = authority
            .client
            .call(
                context.child(),
                OpenActor {
                    parent: authority.root,
                    path: "subject".to_owned(),
                    rights: Rights::all(),
                },
            )
            .await
            .unwrap();
        let other = authority
            .client
            .call(
                context.child(),
                OpenActor {
                    parent: authority.root,
                    path: "other".to_owned(),
                    rights: Rights::all(),
                },
            )
            .await
            .unwrap();
        let parent = authority
            .client
            .call(
                context.child(),
                MintCapability {
                    issuer: authority.root,
                    object: "read".to_owned(),
                    rights: Rights::all(),
                },
            )
            .await
            .unwrap();
        let child = authority
            .client
            .call(
                context.child(),
                AttenuateCapability {
                    capability: parent.capability_id,
                    rights: Rights::USE | Rights::NARROW | Rights::DELEGATE,
                },
            )
            .await
            .unwrap();
        for (via, object, rights) in [
            (authority.root, "read", Rights::USE),
            (child.capability_id, "read", Rights::NARROW),
            (authority.root, "write", Rights::USE),
            (other.capability, "read", Rights::all()),
        ] {
            authority
                .client
                .call(
                    context.child(),
                    GrantAccess {
                        via,
                        subject_id: subject.actor_id,
                        object: object.to_owned(),
                        rights,
                    },
                )
                .await
                .unwrap();
        }
        let request = CheckGrants {
            via: authority.root,
            subject_id: subject.actor_id,
            objects: ["write", "missing", "read", "read"]
                .map(str::to_owned)
                .to_vec(),
        };
        assert_eq!(
            authority
                .client
                .call(context.child(), request.clone())
                .await
                .unwrap(),
            vec![
                Rights::USE,
                Rights::empty(),
                Rights::USE | Rights::NARROW,
                Rights::USE | Rights::NARROW
            ],
        );
        authority
            .client
            .call(
                context.child(),
                RetractCapability {
                    via: authority.root,
                    capability: parent.capability_id,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            authority
                .client
                .call(context.child(), request.clone())
                .await
                .unwrap(),
            vec![Rights::USE, Rights::empty(), Rights::USE, Rights::USE],
        );
        authority
            .client
            .call(
                context.child(),
                RevokeAccess {
                    via: authority.root,
                    subject_id: subject.actor_id,
                    object: "read".to_owned(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            authority
                .client
                .call(context.child(), request)
                .await
                .unwrap(),
            vec![
                Rights::USE,
                Rights::empty(),
                Rights::empty(),
                Rights::empty()
            ],
        );
        let objects: Vec<String> = (0..1_200).map(|index| format!("absent-{index}")).collect();
        assert_eq!(
            authority
                .client
                .call(
                    context.child(),
                    CheckGrants {
                        via: authority.root,
                        subject_id: subject.actor_id,
                        objects,
                    }
                )
                .await
                .unwrap(),
            vec![Rights::empty(); 1_200],
        );
        assert_eq!(
            authority
                .client
                .call(
                    context.child(),
                    CheckGrants {
                        via: authority.root,
                        subject_id: subject.actor_id,
                        objects: Vec::new(),
                    }
                )
                .await
                .unwrap(),
            Vec::<Rights>::new(),
        );
        assert!(matches!(
            authority
                .client
                .call(
                    context.child(),
                    CheckGrants {
                        via: CapId::new_random(),
                        subject_id: subject.actor_id,
                        objects: Vec::new(),
                    }
                )
                .await,
            Err(service_utils_rs::ClientError::Operation(
                AuthorityServiceError::InvalidCapability
            )),
        ));
        stop.stop().await;
    }
}
