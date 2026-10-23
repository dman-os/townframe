use super::*;
use futures::{StreamExt, stream};
use service_utils_rs::EventStream;

impl AuthorityService {
    pub(super) fn publish_change(&self) {
        self.changes.send_modify(|revision| {
            *revision = revision
                .checked_add(1)
                .expect("authority revision overflow");
        });
    }
}

impl Handler<ObserveCapability> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<ObserveCapability>,
    ) -> Result<
        ResponseEnvelope<EventStream<CapabilityStatus, AuthorityServiceError>>,
        AuthorityServiceError,
    > {
        let capability = request.params.capability;
        let revisions = self.changes.clone();
        let changes = revisions.subscribe();
        let mut transaction = self.read().await?;
        cap::walk(&mut transaction, &capability)
            .await?
            .require(Rights::USE)?;
        drop(transaction);

        let state = Some((self.sql.clone(), revisions, changes));
        let updates = stream::unfold(state, move |state| async move {
            let (sql, revisions, mut changes) = state?;
            changes
                .changed()
                .await
                .expect("the stream retains its authority revision sender");
            let status = async {
                let mut transaction = sql.read_pool.begin().await?;
                cap::walk(&mut transaction, &capability)
                    .await?
                    .require(Rights::USE)
            }
            .await;
            match status {
                Ok(()) => Some((Ok(CapabilityStatus::Live), Some((sql, revisions, changes)))),
                Err(AuthorityError::InvalidCapability) => {
                    Some((Ok(CapabilityStatus::Retracted), None))
                }
                Err(error) => Some((Err(error.into()), None)),
            }
        });
        let events: EventStream<CapabilityStatus, AuthorityServiceError> =
            Box::pin(stream::once(async { Ok(CapabilityStatus::Live) }).chain(updates));
        Ok(events.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use service_utils_rs::ClientError;

    async fn boot_authority() -> (Authority, StopToken) {
        let keyring = keyring_core::mock::Store::new().expect("mock keyring")
            as Arc<keyring_core::CredentialStore>;
        let secrets = SecretStore::boot_with_store(keyring)
            .await
            .expect("secret store");
        let sql = SqlCtx::memory().await.expect("authority database");
        boot(&secrets, sql).await.expect("authority boot")
    }

    #[tokio::test]
    async fn committed_changes_wake_live_capabilities_and_ancestor_retraction_ends_them() {
        let (authority, stop) = boot_authority().await;
        let context = RequestContext::root();
        let issuer = authority
            .client
            .call(
                context.child(),
                OpenActor {
                    parent: authority.root,
                    path: "issuer".into(),
                    rights: Rights::all(),
                },
            )
            .await
            .unwrap();
        let signer = authority
            .client
            .call(
                context.child(),
                OpenActor {
                    parent: authority.root,
                    path: "signer".into(),
                    rights: Rights::USE,
                },
            )
            .await
            .unwrap();
        let capability = authority
            .client
            .call(
                context.child(),
                MintCapability {
                    issuer: issuer.capability,
                    object: "observed object".into(),
                    rights: Rights::USE,
                },
            )
            .await
            .unwrap();
        let mut events = authority
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

        authority
            .client
            .call(
                context.child(),
                GrantAccess {
                    via: authority.root,
                    subject_id: signer.actor_id,
                    object: "unrelated object".into(),
                    rights: Rights::USE,
                },
            )
            .await
            .unwrap();
        assert_eq!(events.next().await, Some(Ok(CapabilityStatus::Live)));
        assert_eq!(
            authority
                .client
                .call(
                    context.child(),
                    CheckGrant {
                        via: authority.root,
                        subject_id: signer.actor_id,
                        object: "unrelated object".into(),
                    }
                )
                .await
                .unwrap(),
            Rights::USE,
        );
        authority
            .client
            .call(
                context.child(),
                RevokeAccess {
                    via: authority.root,
                    subject_id: signer.actor_id,
                    object: "unrelated object".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(events.next().await, Some(Ok(CapabilityStatus::Live)));
        assert_eq!(
            authority
                .client
                .call(
                    context.child(),
                    CheckGrant {
                        via: authority.root,
                        subject_id: signer.actor_id,
                        object: "unrelated object".into(),
                    }
                )
                .await
                .unwrap(),
            Rights::empty(),
        );
        authority
            .client
            .call(
                context.child(),
                RetractCapability {
                    via: authority.root,
                    capability: signer.capability,
                },
            )
            .await
            .unwrap();
        assert_eq!(events.next().await, Some(Ok(CapabilityStatus::Live)));
        authority
            .client
            .call(
                context.child(),
                RetractCapability {
                    via: authority.root,
                    capability: issuer.capability,
                },
            )
            .await
            .unwrap();
        assert_eq!(events.next().await, Some(Ok(CapabilityStatus::Retracted)));
        assert_eq!(events.next().await, None);
        stop.stop().await;
    }

    #[tokio::test]
    async fn invalid_admission_and_failed_mutations_do_not_publish() {
        let (authority, stop) = boot_authority().await;
        let context = RequestContext::root();
        let no_use = authority
            .client
            .call(
                context.child(),
                AttenuateCapability {
                    capability: authority.root,
                    rights: Rights::NARROW,
                },
            )
            .await
            .unwrap();
        let ancestor = authority
            .client
            .call(
                context.child(),
                AttenuateCapability {
                    capability: authority.root,
                    rights: Rights::all(),
                },
            )
            .await
            .unwrap();
        let descendant = authority
            .client
            .call(
                context.child(),
                AttenuateCapability {
                    capability: ancestor.capability_id,
                    rights: Rights::USE,
                },
            )
            .await
            .unwrap();
        authority
            .client
            .call(
                context.child(),
                RetractCapability {
                    via: authority.root,
                    capability: ancestor.capability_id,
                },
            )
            .await
            .unwrap();
        for (capability, expected) in [
            (
                CapId::new_random(),
                AuthorityServiceError::InvalidCapability,
            ),
            (no_use.capability_id, AuthorityServiceError::Unauthorized),
            (
                ancestor.capability_id,
                AuthorityServiceError::InvalidCapability,
            ),
            (
                descendant.capability_id,
                AuthorityServiceError::InvalidCapability,
            ),
        ] {
            let result = authority
                .client
                .subscribe(context.child(), ObserveCapability { capability })
                .await;
            match result {
                Err(ClientError::Operation(error)) => assert_eq!(error, expected),
                Err(error) => panic!("unexpected subscription failure: {error:?}"),
                Ok(_) => panic!("invalid observation was admitted"),
            }
        }
        let changes = authority.service.changes.subscribe();
        let failed = authority
            .client
            .call(
                context.child(),
                GrantAccess {
                    via: authority.root,
                    subject_id: Uuid7::new(),
                    object: "unrelated object".into(),
                    rights: Rights::USE,
                },
            )
            .await;
        assert!(matches!(
            failed,
            Err(ClientError::Operation(AuthorityServiceError::ActorNotFound))
        ));
        let failed = authority
            .client
            .call(
                context.child(),
                RevokeAccess {
                    via: CapId::new_random(),
                    subject_id: authority.service.operator_uuid(),
                    object: "unrelated object".into(),
                },
            )
            .await;
        assert!(matches!(
            failed,
            Err(ClientError::Operation(
                AuthorityServiceError::InvalidCapability
            ))
        ));
        let failed = authority
            .client
            .call(
                context.child(),
                RetractCapability {
                    via: authority.root,
                    capability: authority.root,
                },
            )
            .await;
        assert!(matches!(
            failed,
            Err(ClientError::Operation(AuthorityServiceError::Unauthorized))
        ));
        assert!(!changes.has_changed().unwrap());
        stop.stop().await;
    }

    #[tokio::test]
    async fn changes_after_admission_before_first_poll_are_retained() {
        let (authority, stop) = boot_authority().await;
        let context = RequestContext::root();
        let response = authority
            .service
            .call(Envelope::new(
                context.child(),
                ObserveCapability {
                    capability: authority.root,
                },
            ))
            .await
            .unwrap();
        authority
            .client
            .call(
                context.child(),
                GrantAccess {
                    via: authority.root,
                    subject_id: authority.service.operator_uuid(),
                    object: "unrelated object".into(),
                    rights: Rights::USE,
                },
            )
            .await
            .unwrap();
        let mut events = response.output;
        assert_eq!(events.next().await, Some(Ok(CapabilityStatus::Live)));
        assert_eq!(events.next().await, Some(Ok(CapabilityStatus::Live)));
        drop(events);
        stop.stop().await;
    }
}
