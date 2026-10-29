use super::*;
use futures::StreamExt;
use futures::stream;
use service_utils_rs::EventStream;

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
        // Subscribed before the admission check, so a retraction the ledger runs after the
        // check bumps a revision this stream has not seen yet.
        let changes = revisions.subscribe();
        self.resolve(capability).await?.require(Rights::USE)?;
        let state = Some((self.ledger.clone(), revisions, changes));
        let updates = stream::unfold(state, move |state| async move {
            let (ledger, revisions, mut changes) = state?;
            changes
                .changed()
                .await
                .expect("the stream retains its authority revision sender");
            let status = cap::ask(&ledger, |reply| cap::Command::Resolve(capability, reply)).await;
            match status {
                Ok(_) => Some((
                    Ok(CapabilityStatus::Live),
                    Some((ledger, revisions, changes)),
                )),
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
    use service_utils_rs::RequestContext;

    async fn boot_authority() -> (Authority, StopToken) {
        let keyring = keyring_core::mock::Store::new().expect("mock keyring")
            as Arc<keyring_core::CredentialStore>;
        let secrets = SecretStore::boot_with_store(keyring)
            .await
            .expect("secret store");
        boot(
            &secrets,
            sqlite_rs::test_support::db(&[])
                .expect("authority database")
                .clone(),
        )
        .await
        .expect("authority boot")
    }

    #[tokio::test]
    async fn retractions_wake_live_observers_and_ancestor_retraction_ends_observation() {
        let (authority, stop) = boot_authority().await;
        let namespace = authority
            .service
            .open_namespace(Uuid7::new())
            .await
            .unwrap();
        let capability = authority
            .client
            .call(
                RequestContext::root(),
                MintCapability {
                    issuer: namespace.capability,
                    parent: None,
                    dependencies: Vec::new(),
                    object: "observed object".into(),
                    rights: Rights::USE,
                },
            )
            .await
            .unwrap();
        let mut events = authority
            .client
            .subscribe(
                RequestContext::root(),
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
                RequestContext::root(),
                RetractCapability {
                    via: authority.root,
                    capability: authority.root,
                },
            )
            .await
            .unwrap();
        assert_eq!(events.next().await, Some(Ok(CapabilityStatus::Live)));
        authority
            .client
            .call(
                RequestContext::root(),
                RetractCapability {
                    via: namespace.capability,
                    capability: namespace.capability,
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
        let namespace = authority
            .service
            .open_namespace(Uuid7::new())
            .await
            .unwrap();
        let no_use = authority
            .client
            .call(
                RequestContext::root(),
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
                RequestContext::root(),
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
                RequestContext::root(),
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
                RequestContext::root(),
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
            match authority
                .client
                .subscribe(RequestContext::root(), ObserveCapability { capability })
                .await
            {
                Err(ClientError::Operation(error)) => assert_eq!(error, expected),
                Err(error) => panic!("unexpected subscription failure: {error:?}"),
                Ok(_) => panic!("invalid observation was admitted"),
            }
        }
        let changes = authority.service.changes.subscribe();
        assert!(matches!(
            authority
                .client
                .call(
                    RequestContext::root(),
                    RetractCapability {
                        via: CapId::new_random(),
                        capability: authority.root,
                    }
                )
                .await,
            Err(ClientError::Operation(
                AuthorityServiceError::InvalidCapability
            ))
        ));
        assert!(matches!(
            authority
                .client
                .call(
                    RequestContext::root(),
                    RetractCapability {
                        via: namespace.capability,
                        capability: authority.root,
                    }
                )
                .await,
            Err(ClientError::Operation(AuthorityServiceError::Unauthorized))
        ));
        assert!(!changes.has_changed().unwrap());
        stop.stop().await;
    }

    #[tokio::test]
    async fn changes_after_admission_before_first_poll_are_retained() {
        let (authority, stop) = boot_authority().await;
        let response = authority
            .service
            .call(Envelope::new(
                RequestContext::root(),
                ObserveCapability {
                    capability: authority.root,
                },
            ))
            .await
            .unwrap();
        authority
            .client
            .call(
                RequestContext::root(),
                RetractCapability {
                    via: authority.root,
                    capability: authority.root,
                },
            )
            .await
            .unwrap();
        let mut events = response.output;
        assert_eq!(events.next().await, Some(Ok(CapabilityStatus::Live)));
        assert_eq!(events.next().await, Some(Ok(CapabilityStatus::Retracted)));
        assert_eq!(events.next().await, None);
        stop.stop().await;
    }
}
