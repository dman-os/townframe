use super::{
    ActorProvisionIntent, AuthorityError, AuthorityRepo, AuthorityStateStore, NODE_SERVICE,
};
use secrets_rs::SecretStore;
use secrets_rs::{SecretsError, SigningKeyHandle};
use sqlx::{Sqlite, Transaction};
use std::sync::Arc;

#[cfg(test)]
#[allow(dead_code)]
pub(super) mod provisioning_test_faults {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;

    static FAIL_ACTOR_PUBLICATION: AtomicBool = AtomicBool::new(false);
    static FORCE_ACTOR_COMMIT_UNKNOWN: AtomicBool = AtomicBool::new(false);
    static FAIL_ROOT_INSTALL: AtomicBool = AtomicBool::new(false);
    static FAIL_ACTOR_INTENT_BIND: AtomicBool = AtomicBool::new(false);
    static FAIL_ACTOR_INTENT_REMOVAL: AtomicBool = AtomicBool::new(false);
    static CLEANUP_PRE_DELETE_BARRIER_ACTIVE: AtomicBool = AtomicBool::new(false);
    static CLEANUP_PRE_DELETE_REACHED: AtomicBool = AtomicBool::new(false);
    static CLEANUP_PRE_DELETE_RELEASED: AtomicBool = AtomicBool::new(false);
    static CLEANUP_PRE_DELETE_NOTIFY: Notify = Notify::const_new();
    static CLEANUP_PRE_RELEASE_NOTIFY: Notify = Notify::const_new();
    static SERIALIZED_BEGIN_PROBE_ACTIVE: AtomicBool = AtomicBool::new(false);
    static SERIALIZED_BEGIN_PROBE_REACHED: AtomicBool = AtomicBool::new(false);
    static SERIALIZED_BEGIN_PROBE_NOTIFY: Notify = Notify::const_new();
    static CLEANUP_DELETE_BARRIER_ACTIVE: AtomicBool = AtomicBool::new(false);
    static CLEANUP_DELETE_REACHED: AtomicBool = AtomicBool::new(false);
    static CLEANUP_DELETE_RELEASED: AtomicBool = AtomicBool::new(false);
    static CLEANUP_DELETE_NOTIFY: Notify = Notify::const_new();
    static CLEANUP_RELEASE_NOTIFY: Notify = Notify::const_new();
    static ACTOR_PUBLICATION_BARRIER_ACTIVE: AtomicBool = AtomicBool::new(false);
    static ACTOR_PUBLICATION_REACHED: AtomicBool = AtomicBool::new(false);
    static ACTOR_PUBLICATION_RELEASED: AtomicBool = AtomicBool::new(false);
    static ACTOR_PUBLICATION_NOTIFY: Notify = Notify::const_new();
    static ACTOR_RELEASE_NOTIFY: Notify = Notify::const_new();

    pub fn fail_actor_publication() {
        FAIL_ACTOR_PUBLICATION.store(true, Ordering::Release);
    }

    pub fn fail_root_install() {
        FAIL_ROOT_INSTALL.store(true, Ordering::Release);
    }

    pub fn fail_actor_intent_binding() {
        FAIL_ACTOR_INTENT_BIND.store(true, Ordering::Release);
    }

    pub fn take_actor_intent_binding_failure() -> bool {
        FAIL_ACTOR_INTENT_BIND.swap(false, Ordering::AcqRel)
    }

    pub fn take_root_install_failure() -> bool {
        FAIL_ROOT_INSTALL.swap(false, Ordering::AcqRel)
    }

    pub fn take_actor_publication_failure() -> bool {
        FAIL_ACTOR_PUBLICATION.swap(false, Ordering::AcqRel)
    }

    pub fn force_actor_commit_unknown() {
        FORCE_ACTOR_COMMIT_UNKNOWN.store(true, Ordering::Release);
    }

    pub fn take_actor_commit_unknown() -> bool {
        FORCE_ACTOR_COMMIT_UNKNOWN.swap(false, Ordering::AcqRel)
    }

    pub fn fail_actor_intent_removal() {
        FAIL_ACTOR_INTENT_REMOVAL.store(true, Ordering::Release);
    }

    pub fn take_actor_intent_removal_failure() -> bool {
        FAIL_ACTOR_INTENT_REMOVAL.swap(false, Ordering::AcqRel)
    }

    pub fn arm_cleanup_pre_delete_barrier() {
        CLEANUP_PRE_DELETE_BARRIER_ACTIVE.store(true, Ordering::Release);
        CLEANUP_PRE_DELETE_REACHED.store(false, Ordering::Release);
        CLEANUP_PRE_DELETE_RELEASED.store(false, Ordering::Release);
    }

    pub async fn before_cleanup_delete() {
        if !CLEANUP_PRE_DELETE_BARRIER_ACTIVE.load(Ordering::Acquire) {
            return;
        }
        CLEANUP_PRE_DELETE_REACHED.store(true, Ordering::Release);
        CLEANUP_PRE_DELETE_NOTIFY.notify_waiters();
        while !CLEANUP_PRE_DELETE_RELEASED.load(Ordering::Acquire) {
            CLEANUP_PRE_RELEASE_NOTIFY.notified().await;
        }
    }

    pub async fn wait_cleanup_pre_delete_barrier() {
        while !CLEANUP_PRE_DELETE_REACHED.load(Ordering::Acquire) {
            CLEANUP_PRE_DELETE_NOTIFY.notified().await;
        }
    }

    pub fn release_cleanup_pre_delete_barrier() {
        CLEANUP_PRE_DELETE_BARRIER_ACTIVE.store(false, Ordering::Release);
        CLEANUP_PRE_DELETE_RELEASED.store(true, Ordering::Release);
        CLEANUP_PRE_RELEASE_NOTIFY.notify_waiters();
    }

    pub fn arm_serialized_begin_probe() {
        SERIALIZED_BEGIN_PROBE_ACTIVE.store(true, Ordering::Release);
        SERIALIZED_BEGIN_PROBE_REACHED.store(false, Ordering::Release);
    }

    pub async fn wait_serialized_begin_probe() {
        while !SERIALIZED_BEGIN_PROBE_REACHED.load(Ordering::Acquire) {
            SERIALIZED_BEGIN_PROBE_NOTIFY.notified().await;
        }
    }

    pub fn notify_serialized_begin_probe() {
        if SERIALIZED_BEGIN_PROBE_ACTIVE.swap(false, Ordering::AcqRel) {
            SERIALIZED_BEGIN_PROBE_REACHED.store(true, Ordering::Release);
            SERIALIZED_BEGIN_PROBE_NOTIFY.notify_waiters();
        }
    }

    pub fn arm_cleanup_delete_barrier() {
        CLEANUP_DELETE_BARRIER_ACTIVE.store(true, Ordering::Release);
        CLEANUP_DELETE_REACHED.store(false, Ordering::Release);
        CLEANUP_DELETE_RELEASED.store(false, Ordering::Release);
    }

    pub async fn wait_cleanup_delete_barrier() {
        while !CLEANUP_DELETE_REACHED.load(Ordering::Acquire) {
            CLEANUP_DELETE_NOTIFY.notified().await;
        }
    }

    pub async fn after_cleanup_delete() {
        if !CLEANUP_DELETE_BARRIER_ACTIVE.load(Ordering::Acquire) {
            return;
        }
        CLEANUP_DELETE_REACHED.store(true, Ordering::Release);
        CLEANUP_DELETE_NOTIFY.notify_waiters();
        while !CLEANUP_DELETE_RELEASED.load(Ordering::Acquire) {
            CLEANUP_RELEASE_NOTIFY.notified().await;
        }
    }

    pub fn release_cleanup_delete_barrier() {
        CLEANUP_DELETE_BARRIER_ACTIVE.store(false, Ordering::Release);
        CLEANUP_DELETE_RELEASED.store(true, Ordering::Release);
        CLEANUP_RELEASE_NOTIFY.notify_waiters();
    }

    pub fn arm_actor_publication_barrier() {
        ACTOR_PUBLICATION_BARRIER_ACTIVE.store(true, Ordering::Release);
        ACTOR_PUBLICATION_REACHED.store(false, Ordering::Release);
        ACTOR_PUBLICATION_RELEASED.store(false, Ordering::Release);
    }

    pub async fn wait_actor_publication_barrier() {
        while !ACTOR_PUBLICATION_REACHED.load(Ordering::Acquire) {
            ACTOR_PUBLICATION_NOTIFY.notified().await;
        }
    }

    pub async fn before_actor_publication() {
        if !ACTOR_PUBLICATION_BARRIER_ACTIVE.load(Ordering::Acquire) {
            return;
        }
        ACTOR_PUBLICATION_REACHED.store(true, Ordering::Release);
        ACTOR_PUBLICATION_NOTIFY.notify_waiters();
        while !ACTOR_PUBLICATION_RELEASED.load(Ordering::Acquire) {
            ACTOR_RELEASE_NOTIFY.notified().await;
        }
    }

    pub fn release_actor_publication_barrier() {
        ACTOR_PUBLICATION_BARRIER_ACTIVE.store(false, Ordering::Release);
        ACTOR_PUBLICATION_RELEASED.store(true, Ordering::Release);
        ACTOR_RELEASE_NOTIFY.notify_waiters();
    }
}

#[allow(clippy::items_after_test_module)]
impl AuthorityRepo {
    pub(super) async fn reconcile_cleanup_intent(
        store: &Arc<SecretStore>,
        transaction: &mut Transaction<'_, Sqlite>,
        intent: &ActorProvisionIntent,
    ) -> Result<(), AuthorityError> {
        let Some(expected) = intent.public_key else {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: intent.key_name.clone(),
                reason: "cleanup phase has no exact public-key binding".into(),
            });
        };
        if let Some(binding) = AuthorityStateStore::actor_binding_tx(transaction, &intent.key_name)
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: intent.key_name.clone(),
                reason: format!("cleanup registry recheck is unresolved: {error}"),
            })?
        {
            if binding != expected {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: "cleanup phase found a conflicting registry public key".into(),
                });
            }
            AuthorityStateStore::remove_actor_intent_tx(transaction, intent)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("cleanup intent removal is unresolved: {error}"),
                })?;
            return Ok(());
        }
        let signer =
            match SigningKeyHandle::open(Arc::clone(store), NODE_SERVICE, &intent.key_name).await {
                Ok(signer) => signer,
                Err(SecretsError::Missing(_)) => {
                    AuthorityStateStore::remove_actor_intent_tx(transaction, intent)
                        .await
                        .map_err(|error| AuthorityError::ProvisioningUnresolved {
                            key_name: intent.key_name.clone(),
                            reason: format!("cleanup intent removal is unresolved: {error}"),
                        })?;
                    return Ok(());
                }
                Err(error) => {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name: intent.key_name.clone(),
                        reason: format!("cleanup key read outcome is unknown: {error}"),
                    });
                }
            };
        let actual =
            signer
                .public_key()
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("cleanup public-key read outcome is unknown: {error}"),
                })?;
        if actual != expected {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: intent.key_name.clone(),
                reason: "cleanup keyring public key disagrees with exact intent".into(),
            });
        }
        #[cfg(test)]
        provisioning_test_faults::before_cleanup_delete().await;
        store
            .delete_secret(NODE_SERVICE, &intent.key_name)
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: intent.key_name.clone(),
                reason: format!("cleanup deletion outcome is unresolved: {error}"),
            })?;
        #[cfg(test)]
        provisioning_test_faults::after_cleanup_delete().await;
        AuthorityStateStore::remove_actor_intent_tx(transaction, intent)
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: intent.key_name.clone(),
                reason: format!("cleanup intent removal is unresolved: {error}"),
            })
    }

    pub(super) async fn reconcile_actor_intents(
        store: &Arc<SecretStore>,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), AuthorityError> {
        for intent in AuthorityStateStore::actor_intents_tx(transaction).await? {
            if intent.cleanup_pending {
                Self::reconcile_cleanup_intent(store, transaction, &intent).await?;
                continue;
            }
            let binding =
                AuthorityStateStore::actor_binding_tx(transaction, &intent.key_name).await?;
            if let Some(binding) = binding {
                if intent
                    .public_key
                    .is_some_and(|expected| expected != binding)
                {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name: intent.key_name,
                        reason: "registry public key disagrees with provisioning intent".into(),
                    });
                }
                let signer =
                    match SigningKeyHandle::open(Arc::clone(store), NODE_SERVICE, &intent.key_name)
                        .await
                    {
                        Ok(signer) => signer,
                        Err(error) => {
                            return Err(AuthorityError::ProvisioningUnresolved {
                                key_name: intent.key_name,
                                reason: format!("registered signer cannot be opened: {error}"),
                            });
                        }
                    };
                let actual = signer.public_key().await.map_err(|error| {
                    AuthorityError::ProvisioningUnresolved {
                        key_name: intent.key_name.clone(),
                        reason: format!(
                            "registered signer public-key read outcome is unknown: {error}"
                        ),
                    }
                })?;
                if actual != binding {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name: intent.key_name,
                        reason: "registered signer public key changed".into(),
                    });
                }
                AuthorityStateStore::remove_actor_intent_tx(transaction, &intent)
                    .await
                    .map_err(|error| AuthorityError::ProvisioningUnresolved {
                        key_name: intent.key_name.clone(),
                        reason: format!("actor intent removal is unresolved: {error}"),
                    })?;
                continue;
            }
            let signer =
                match SigningKeyHandle::open(Arc::clone(store), NODE_SERVICE, &intent.key_name)
                    .await
                {
                    Ok(signer) => signer,
                    Err(SecretsError::Missing(_)) => continue,
                    Err(error) => {
                        return Err(AuthorityError::ProvisioningUnresolved {
                            key_name: intent.key_name,
                            reason: format!("key ownership/read outcome is unknown: {error}"),
                        });
                    }
                };
            let public_key = signer.public_key().await.map_err(|error| {
                AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("public-key read outcome is unknown: {error}"),
                }
            })?;
            if intent.preexisting {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name,
                    reason: "pre-existing key has no exact actor registry binding".into(),
                });
            }
            if intent
                .public_key
                .is_some_and(|expected| expected != public_key)
            {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name,
                    reason: "keyring public key disagrees with provisioning intent".into(),
                });
            }
            if intent.public_key.is_none() {
                AuthorityStateStore::set_actor_intent_public_key_tx(
                    transaction,
                    &intent,
                    &public_key,
                )
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("recovered actor intent binding is unresolved: {error}"),
                })?;
            }
            AuthorityStateStore::insert_actor_tx(transaction, &public_key, &intent.key_name)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("actor registry publication is unresolved: {error}"),
                })?;
            AuthorityStateStore::remove_actor_intent_tx(transaction, &intent)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("actor intent removal is unresolved: {error}"),
                })?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn reconcile_pending(&self) -> Result<(), AuthorityError> {
        let mut transaction = self.state.begin_serialized().await?;
        Self::reconcile_actor_intents(&self.store, &mut transaction).await?;
        if let Err(error) = transaction.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "pending actor reconciliation commit outcome is unknown: {error}"
            )));
        }
        Ok(())
    }
}
