//! Actor identities, capabilities, grants, and signing for Cabinet.
//!
//! Four independent parts (ADR 017): a capability table (caps say what can be
//! done, not who holds them), an actor registry (persistent subsystem
//! identities with node-signed attribution certificates), a generic grant
//! ledger (opaque subject/object/access rows), and a signer that signs as the
//! actor a cap chain roots at. Domain services name objects and decide
//! containment; this module never interprets an object string.

use std::{path::Path, sync::Arc};

use big_cabinet_core::dmeta::{Bytes, FacetClaim, MultibaseKey, SignedFacetClaim, Uuid7};
use rand::RngCore;
use secrets_rs::{SecretStore, SecretsError, SigningKeyHandle};
use thiserror::Error;

mod actor;
mod cap;
mod ledger;
mod provisioning;
mod signer;
mod store;

#[cfg(test)]
mod tests;

use actor::ActorRecord;
use store::AuthorityStateStore;

pub type ActorPublicKey = [u8; 32];
pub type CapId = [u8; 16];

const NODE_SERVICE: &str = "big_cabinet.authority";
const NODE_KEY: &str = "node";
const OPERATOR_KEY: &str = "operator";
const OPERATOR_PATH: &str = "operator";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Rights(u32);

impl Rights {
    pub const USE: Self = Self(1);
    pub const NARROW: Self = Self(2);
    pub const MINT_CHILD: Self = Self(4);
    pub const DELEGATE: Self = Self(8);

    pub fn all() -> Self {
        Self(Self::USE.0 | Self::NARROW.0 | Self::MINT_CHILD.0 | Self::DELEGATE.0)
    }
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
    pub fn intersects(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }
    fn is_subset_of(self, other: Self) -> bool {
        other.contains(self)
    }
    fn bits_i64(self) -> i64 {
        i64::from(self.0)
    }
    fn from_bits(bits: u32) -> Self {
        Self(bits)
    }
}

impl std::ops::BitOr for Rights {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

#[derive(Clone, Debug)]
pub(super) struct CapRecord {
    object: String,
    rights: Rights,
    parent: Option<CapId>,
    root_actor: Option<ActorPublicKey>,
    retracted: bool,
}

/// A subsystem actor as issued by the registry.
#[derive(Clone, Debug)]
pub struct Actor {
    pub uuid: Uuid7,
    pub url: String,
    pub public_key: ActorPublicKey,
    /// Node-signed attribution certificate bytes for dmeta `ActorMetadata.delegation`.
    pub certificate: Bytes,
    pub cap: CapId,
}

#[derive(Clone, Copy, Debug)]
pub struct VerifiedCap {
    pub cap: CapId,
    pub root_actor: Option<ActorPublicKey>,
}

pub(super) struct ActorProvisionIntent {
    intent_id: [u8; 16],
    key_name: String,
    public_key: Option<ActorPublicKey>,
    preexisting: bool,
    cleanup_pending: bool,
}

#[derive(Debug, Error)]
pub enum AuthorityError {
    #[error("capability is invalid, unknown, retracted, or has been tampered with")]
    InvalidCap,
    #[error("capability does not authorize this operation")]
    Unauthorized,
    #[error("a minted capability is not an attenuation of its parent")]
    Escalation,
    #[error("actor path does not extend the minting subsystem's path")]
    InvalidActorPath,
    #[error("actor has no signing key registered with AuthorityRepo")]
    ActorSignerMissing,
    #[error("persisted authority state is corrupt: {0}")]
    StateCorrupt(String),
    #[error("authority storage failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("authority storage filesystem operation failed: {0}")]
    StorageIo(#[from] std::io::Error),
    #[error("secret-store signing failed: {0}")]
    Secrets(#[from] SecretsError),
    #[error("authority commit outcome is unknown; reconcile before retrying: {0}")]
    CommitUnknown(String),
    #[error("actor provisioning requires reconciliation for {key_name}: {reason}")]
    ProvisioningUnresolved { key_name: String, reason: String },
    #[error("facet claim is invalid: {0}")]
    FacetClaim(#[from] big_cabinet_core::dmeta::DmetaError),
    #[error("attribution certificate is invalid: {0}")]
    Attribution(#[from] big_cabinet_core::dmeta::AttributionError),
}

fn fixed_bytes<const N: usize>(bytes: Vec<u8>) -> Result<[u8; N], AuthorityError> {
    bytes.try_into().map_err(|_| {
        AuthorityError::StateCorrupt(format!("authority key has wrong width; expected {N} bytes"))
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The Cabinet actor/capability boundary. The SQLite path is the owner
/// boundary; private keys remain named handles inside `secrets_rs`.
#[derive(Clone)]
pub struct AuthorityRepo {
    store: Arc<SecretStore>,
    state: AuthorityStateStore,
    node_signer: SigningKeyHandle,
    node_public_key: ActorPublicKey,
    operator_public_key: ActorPublicKey,
    operator_uuid: Uuid7,
    root_cap: CapId,
}

impl AuthorityRepo {
    /// Boot against a durable state path; returns the repo and the operator root cap ("is operator").
    pub async fn boot(
        store: Arc<SecretStore>,
        state_path: impl AsRef<Path>,
    ) -> Result<(Self, CapId), AuthorityError> {
        let state = AuthorityStateStore::boot(state_path).await?;
        let mut reconciliation = state.begin_serialized().await?;
        Self::reconcile_actor_intents(&store, &mut reconciliation).await?;
        state
            .reconcile_boot_intents(&store, &mut reconciliation)
            .await?;
        if let Err(error) = reconciliation.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "provisioning reconciliation commit outcome is unknown: {error}"
            )));
        }

        let node_preexisting = store.get_secret(NODE_SERVICE, NODE_KEY).await?.is_some();
        let operator_preexisting = store
            .get_secret(NODE_SERVICE, OPERATOR_KEY)
            .await?
            .is_some();
        state
            .create_boot_intents(node_preexisting, operator_preexisting)
            .await?;

        let mut bootstrap = state.begin_serialized().await?;
        let (node_expected, node_preexisting) = if let Some(intent) =
            AuthorityStateStore::boot_intent_tx(&mut bootstrap, NODE_KEY).await?
        {
            intent
        } else {
            let preexisting = store.get_secret(NODE_SERVICE, NODE_KEY).await?.is_some();
            AuthorityStateStore::insert_boot_intent_tx(&mut bootstrap, NODE_KEY, preexisting)
                .await?;
            AuthorityStateStore::boot_intent_tx(&mut bootstrap, NODE_KEY)
                .await?
                .expect("boot intent inserted under the write reservation")
        };
        let (operator_expected, operator_preexisting) = if let Some(intent) =
            AuthorityStateStore::boot_intent_tx(&mut bootstrap, OPERATOR_KEY).await?
        {
            intent
        } else {
            let preexisting = store
                .get_secret(NODE_SERVICE, OPERATOR_KEY)
                .await?
                .is_some();
            AuthorityStateStore::insert_boot_intent_tx(&mut bootstrap, OPERATOR_KEY, preexisting)
                .await?;
            AuthorityStateStore::boot_intent_tx(&mut bootstrap, OPERATOR_KEY)
                .await?
                .expect("boot intent inserted under the write reservation")
        };
        if node_preexisting && store.get_secret(NODE_SERVICE, NODE_KEY).await?.is_none() {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: NODE_KEY.into(),
                reason: "pre-existing node key disappeared across intent reacquisition".into(),
            });
        }
        if operator_preexisting
            && store
                .get_secret(NODE_SERVICE, OPERATOR_KEY)
                .await?
                .is_none()
        {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: OPERATOR_KEY.into(),
                reason: "pre-existing operator key disappeared across intent reacquisition".into(),
            });
        }
        let node_signer =
            match SigningKeyHandle::get_or_create(Arc::clone(&store), NODE_SERVICE, NODE_KEY).await
            {
                Ok(signer) => signer,
                Err(error) => {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name: NODE_KEY.into(),
                        reason: format!("node key creation/read outcome is unknown: {error}"),
                    });
                }
            };
        let node_public_key = node_signer.public_key().await.map_err(|error| {
            AuthorityError::ProvisioningUnresolved {
                key_name: NODE_KEY.into(),
                reason: format!("node public-key read outcome is unknown: {error}"),
            }
        })?;
        if node_expected.is_some_and(|expected| expected != node_public_key) {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: NODE_KEY.into(),
                reason: "node key disagrees with durable boot intent".into(),
            });
        }
        if node_expected.is_none() {
            AuthorityStateStore::bind_boot_intent_tx(&mut bootstrap, NODE_KEY, &node_public_key)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: NODE_KEY.into(),
                    reason: format!("node boot intent binding is unresolved: {error}"),
                })?;
        }
        let node_signer = node_signer.bind_public_key(node_public_key);
        let operator_signer =
            match SigningKeyHandle::get_or_create(Arc::clone(&store), NODE_SERVICE, OPERATOR_KEY)
                .await
            {
                Ok(signer) => signer,
                Err(error) => {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name: OPERATOR_KEY.into(),
                        reason: format!("operator key creation/read outcome is unknown: {error}"),
                    });
                }
            };
        let operator_public_key = operator_signer.public_key().await.map_err(|error| {
            AuthorityError::ProvisioningUnresolved {
                key_name: OPERATOR_KEY.into(),
                reason: format!("operator public-key read outcome is unknown: {error}"),
            }
        })?;
        if operator_expected.is_some_and(|expected| expected != operator_public_key) {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: OPERATOR_KEY.into(),
                reason: "operator key disagrees with durable boot intent".into(),
            });
        }
        if operator_expected.is_none() {
            AuthorityStateStore::bind_boot_intent_tx(
                &mut bootstrap,
                OPERATOR_KEY,
                &operator_public_key,
            )
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: OPERATOR_KEY.into(),
                reason: format!("operator boot intent binding is unresolved: {error}"),
            })?;
        }
        let operator_signer = operator_signer.bind_public_key(operator_public_key);

        // Operator actor record + root cap, created on first boot.
        let existing =
            AuthorityStateStore::actor_by_key_tx(&mut bootstrap, &operator_public_key).await?;
        let _existing_is_none = existing.is_none();
        let prior_root = AuthorityStateStore::root_cap_tx(&mut bootstrap).await?;
        let (operator_uuid, root_cap) = if let Some(record) = existing
            && let Some(uuid_text) = record.uuid.clone()
            && let Some((cap_id, stored_uuid)) = prior_root
            && stored_uuid == uuid_text
        {
            let uuid = uuid_text
                .parse::<uuid::Uuid>()
                .map_err(|_| AuthorityError::StateCorrupt("operator uuid".into()))?;
            (Uuid7::try_from(uuid)?, cap_id)
        } else {
            let operator_uuid = Uuid7::new();
            let url = actor::actor_url(&node_public_key, OPERATOR_PATH);
            let cert = actor::issue_certificate(
                &node_signer,
                &node_public_key,
                operator_uuid,
                &operator_public_key,
                &url,
            )
            .await?;
            {
                let _aff = sqlx::query(
                    "INSERT OR IGNORE INTO authority_actors (public_key, key_name) VALUES (?1, ?2)",
                )
                .bind(operator_public_key.as_slice())
                .bind(OPERATOR_KEY)
                .execute(&mut *bootstrap)
                .await?;
            }
            AuthorityStateStore::set_actor_binding_tx(
                &mut bootstrap,
                &operator_public_key,
                &operator_uuid.to_string(),
                &url,
                None,
                cert.as_slice(),
            )
            .await?;
            let root_cap = cap::new_cap_id();
            AuthorityStateStore::insert_cap_tx(
                &mut bootstrap,
                &root_cap,
                "is operator",
                Rights::all(),
                None,
                Some(&operator_public_key),
            )
            .await?;
            AuthorityStateStore::install_root_cap_tx(
                &mut bootstrap,
                &root_cap,
                &operator_uuid.to_string(),
            )
            .await?;
            (operator_uuid, root_cap)
        };
        let _unused = operator_signer;
        sqlx::query("DELETE FROM authority_boot_intents WHERE key_name IN (?1, ?2)")
            .bind(NODE_KEY)
            .bind(OPERATOR_KEY)
            .execute(&mut *bootstrap)
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: NODE_KEY.into(),
                reason: format!("final boot intent cleanup is unresolved: {error}"),
            })?;
        if let Err(error) = bootstrap.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "boot initialization: {error}"
            )));
        }
        let repo = Self {
            store,
            state,
            node_signer,
            node_public_key,
            operator_public_key,
            operator_uuid,
            root_cap,
        };
        Ok((repo, root_cap))
    }

    pub fn node_public_key(&self) -> ActorPublicKey {
        self.node_public_key
    }

    pub fn operator_public_key(&self) -> ActorPublicKey {
        self.operator_public_key
    }

    pub fn operator_uuid(&self) -> Uuid7 {
        self.operator_uuid
    }

    pub fn root_cap(&self) -> CapId {
        self.root_cap
    }

    /// Create a persistent subsystem actor under `parent`. Caller supplies the
    /// child path (e.g. `rt/plugs/ocr` under `rt`); the registry enforces that
    /// it extends the parent actor's path. Returns the actor binding and its
    /// "is subsystem" cap.
    pub async fn create_actor(
        &self,
        parent: &CapId,
        child_path: &str,
    ) -> Result<Actor, AuthorityError> {
        let mut transaction = self.state.begin_serialized().await?;
        let parent_record = cap::verify_chain(&mut transaction, parent).await?;
        if !parent_record.rights.contains(Rights::MINT_CHILD) {
            return Err(AuthorityError::Unauthorized);
        }
        let Some(parent_key) = cap::effective_root_actor(&mut transaction, parent).await? else {
            return Err(AuthorityError::Unauthorized);
        };
        let parent_actor = AuthorityStateStore::actor_by_key_tx(&mut transaction, &parent_key)
            .await?
            .ok_or(AuthorityError::ActorSignerMissing)?;
        let parent_path = parent_actor
            .url
            .as_deref()
            .and_then(|url| {
                url.split_once("://").map(|(_, rest)| {
                    rest.split_once('/')
                        .map(|(_, path)| path.to_owned())
                        .unwrap_or_default()
                })
            })
            .unwrap_or_default();
        // Operator path is the root prefix; subsystem paths live beneath it only
        // when minted by the operator itself. Other subsystems extend their own.
        let effective_parent = if parent_actor.key_name == OPERATOR_KEY {
            String::new()
        } else {
            parent_path
        };
        actor::check_child_path(&effective_parent, child_path)?;
        transaction.rollback().await?;

        let public_key = self.provision_actor_key().await?;
        let actor_uuid = Uuid7::new();
        let url = actor::actor_url(&self.node_public_key, child_path.trim_matches('/'));
        let certificate = actor::issue_certificate(
            &self.node_signer,
            &self.node_public_key,
            actor_uuid,
            &public_key,
            &url,
        )
        .await?;

        let mut transaction = self.state.begin_serialized().await?;
        let object = format!("is subsystem {}", child_path.trim_matches('/'));
        let cap_id = cap::new_cap_id();
        AuthorityStateStore::set_actor_binding_tx(
            &mut transaction,
            &public_key,
            &actor_uuid.to_string(),
            &url,
            parent_actor.uuid.as_deref(),
            certificate.as_slice(),
        )
        .await?;
        AuthorityStateStore::insert_cap_tx(
            &mut transaction,
            &cap_id,
            &object,
            Rights::USE | Rights::NARROW | Rights::MINT_CHILD,
            Some(parent),
            Some(&public_key),
        )
        .await?;
        if let Err(error) = transaction.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "actor creation commit: {error}"
            )));
        }
        Ok(Actor {
            uuid: actor_uuid,
            url,
            public_key,
            certificate,
            cap: cap_id,
        })
    }

    async fn provision_actor_key(&self) -> Result<ActorPublicKey, AuthorityError> {
        let mut name_bytes = [0_u8; 16];
        rand::rng().fill_bytes(&mut name_bytes);
        let key_name = format!("actor-{}", hex(&name_bytes));
        let preexisting = self
            .store
            .get_secret(NODE_SERVICE, &key_name)
            .await?
            .is_some();
        let intent_id = self
            .state
            .create_actor_intent(&key_name, preexisting)
            .await?;
        let mut transaction = match self.state.begin_serialized().await {
            Ok(transaction) => transaction,
            Err(error) => {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: format!("could not reacquire intent reservation: {error}"),
                });
            }
        };
        let Some(intent) =
            AuthorityStateStore::actor_intent_tx(&mut transaction, &intent_id, &key_name).await?
        else {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name,
                reason: "provisioning intent was reconciled before creator reacquired it".into(),
            });
        };
        if intent.preexisting {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name,
                reason: "random actor key name was already occupied before intent publication"
                    .into(),
            });
        }
        if self
            .store
            .get_secret(NODE_SERVICE, &key_name)
            .await?
            .is_some()
        {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name,
                reason: "random actor key appeared before creator reacquired intent".into(),
            });
        }
        let signer =
            match SigningKeyHandle::get_or_create(Arc::clone(&self.store), NODE_SERVICE, &key_name)
                .await
            {
                Ok(signer) => signer,
                Err(error) => {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name,
                        reason: format!("key creation/read outcome is unknown: {error}"),
                    });
                }
            };
        let public_key = match signer.public_key().await {
            Ok(public_key) => public_key,
            Err(error) => {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: format!("public-key read outcome is unknown: {error}"),
                });
            }
        };
        if public_key == self.node_public_key || public_key == self.operator_public_key {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name,
                reason: "provisioned key has a reserved authority identity".into(),
            });
        }
        if intent
            .public_key
            .is_some_and(|expected| expected != public_key)
        {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name,
                reason: "keyring public key disagrees with durable intent".into(),
            });
        }
        if intent.public_key.is_none() {
            AuthorityStateStore::set_actor_intent_public_key_tx(
                &mut transaction,
                &intent,
                &public_key,
            )
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: key_name.clone(),
                reason: format!("actor intent binding is unresolved: {error}"),
            })?;
        }
        #[cfg(test)]
        provisioning::provisioning_test_faults::before_actor_publication().await;
        if let Some(binding) =
            AuthorityStateStore::actor_binding_tx(&mut transaction, &key_name).await?
        {
            if binding != public_key {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: "existing registry binding has a different public key".into(),
                });
            }
        } else if let Err(publication_error) =
            AuthorityStateStore::insert_actor_tx(&mut transaction, &public_key, &key_name).await
        {
            // First make the compensation phase durable. The external delete
            // must never precede this commit: cancellation or shutdown after
            // deletion can then be recovered from the pending intent.
            let owned_by_registry =
                AuthorityStateStore::actor_key_name_tx(&mut transaction, &public_key)
                    .await
                    .map_err(|error| AuthorityError::ProvisioningUnresolved {
                        key_name: key_name.clone(),
                        reason: format!("actor publication recheck is unresolved: {error}"),
                    })?;
            if owned_by_registry.is_some() {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: format!("actor publication is unresolved: {publication_error}"),
                });
            }
            AuthorityStateStore::mark_actor_cleanup_pending_tx(
                &mut transaction,
                &intent,
                &public_key,
            )
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: key_name.clone(),
                reason: format!("could not durably enter cleanup phase: {error}"),
            })?;
            if let Err(commit_error) = transaction.commit().await {
                return Err(AuthorityError::CommitUnknown(format!(
                    "actor cleanup phase commit outcome is unknown for {key_name}: {commit_error}"
                )));
            }
            // Reacquire and keep the shared write reservation through the
            // exact recheck, external delete, and intent removal. A second
            // owner can therefore not publish or clean the same intent while
            // this owner is between phase commit and finish.
            let mut finish = self.state.begin_serialized().await.map_err(|error| {
                AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.clone(),
                    reason: format!("cleanup reacquisition is unresolved: {error}"),
                }
            })?;
            let current =
                AuthorityStateStore::actor_intent_tx(&mut finish, &intent.intent_id, &key_name)
                    .await
                    .map_err(|error| AuthorityError::ProvisioningUnresolved {
                        key_name: key_name.clone(),
                        reason: format!("cleanup intent recheck is unresolved: {error}"),
                    })?;
            let Some(current) = current else {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: "cleanup phase was already finalized by another owner".into(),
                });
            };
            if !current.cleanup_pending || current.public_key != Some(public_key) {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: "cleanup intent binding changed before deletion".into(),
                });
            }
            if let Some(binding) = AuthorityStateStore::actor_binding_tx(&mut finish, &key_name)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.clone(),
                    reason: format!("cleanup registry recheck is unresolved: {error}"),
                })?
            {
                if binding == public_key {
                    AuthorityStateStore::remove_actor_intent_tx(&mut finish, &current)
                        .await
                        .map_err(|error| AuthorityError::ProvisioningUnresolved {
                            key_name: key_name.clone(),
                            reason: format!("cleanup intent removal is unresolved: {error}"),
                        })?;
                    if let Err(error) = finish.commit().await {
                        return Err(AuthorityError::CommitUnknown(format!(
                            "cleanup publication recheck commit is unknown for {key_name}: {error}"
                        )));
                    }
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name,
                        reason: "publication completed before cleanup owner recheck".into(),
                    });
                }
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: "cleanup found a conflicting registry public key".into(),
                });
            }
            match SigningKeyHandle::open(Arc::clone(&self.store), NODE_SERVICE, &key_name).await {
                Ok(signer) => {
                    let actual = signer.public_key().await.map_err(|error| {
                        AuthorityError::ProvisioningUnresolved {
                            key_name: key_name.clone(),
                            reason: format!("cleanup public-key read outcome is unknown: {error}"),
                        }
                    })?;
                    if actual != public_key {
                        return Err(AuthorityError::ProvisioningUnresolved {
                            key_name,
                            reason: "cleanup keyring public key disagrees with intent".into(),
                        });
                    }
                }
                Err(SecretsError::Missing(_)) => {
                    AuthorityStateStore::remove_actor_intent_tx(&mut finish, &current)
                        .await
                        .map_err(|error| AuthorityError::ProvisioningUnresolved {
                            key_name: key_name.clone(),
                            reason: format!("cleanup intent removal is unresolved: {error}"),
                        })?;
                    if let Err(error) = finish.commit().await {
                        return Err(AuthorityError::CommitUnknown(format!(
                            "cleanup missing-key finish commit is unknown for {key_name}: {error}"
                        )));
                    }
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name,
                        reason: "cleanup was already completed by another owner".into(),
                    });
                }
                Err(error) => {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name,
                        reason: format!("cleanup key read outcome is unknown: {error}"),
                    });
                }
            }
            #[cfg(test)]
            provisioning::provisioning_test_faults::before_cleanup_delete().await;
            self.store
                .delete_secret(NODE_SERVICE, &key_name)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.clone(),
                    reason: format!("exact-key cleanup failed: {error}"),
                })?;
            #[cfg(test)]
            provisioning::provisioning_test_faults::after_cleanup_delete().await;
            AuthorityStateStore::remove_actor_intent_tx(&mut finish, &current)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.clone(),
                    reason: format!("cleanup intent removal is unresolved: {error}"),
                })?;
            if let Err(error) = finish.commit().await {
                return Err(AuthorityError::CommitUnknown(format!(
                    "actor cleanup finish commit outcome is unknown for {key_name}: {error}"
                )));
            }
            return Err(publication_error);
        }
        AuthorityStateStore::remove_actor_intent_tx(&mut transaction, &intent)
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: key_name.clone(),
                reason: format!("actor intent removal is unresolved: {error}"),
            })?;
        let commit = transaction.commit().await;
        #[cfg(test)]
        if provisioning::provisioning_test_faults::take_actor_commit_unknown() {
            return match commit {
                Ok(()) => Err(AuthorityError::CommitUnknown(format!(
                    "injected actor {key_name} publication commit outcome is unknown"
                ))),
                Err(error) => Err(AuthorityError::CommitUnknown(format!(
                    "actor {key_name} publication commit outcome is unknown: {error}"
                ))),
            };
        }
        match commit {
            Ok(()) => Ok(public_key),
            Err(error) => Err(AuthorityError::CommitUnknown(format!(
                "actor {key_name} publication commit outcome is unknown: {error}"
            ))),
        }
    }

    /// Mint a child cap. The domain decides that `object` is narrower than the
    /// parent's object; this only enforces the mechanic: parent allows minting
    /// or narrowing, and rights only shrink.
    pub async fn mint(
        &self,
        parent: &CapId,
        object: &str,
        rights: Rights,
    ) -> Result<CapId, AuthorityError> {
        let mut transaction = self.state.begin_serialized().await?;
        let parent_record = cap::verify_chain(&mut transaction, parent).await?;
        if !parent_record
            .rights
            .intersects(Rights::MINT_CHILD | Rights::NARROW)
        {
            return Err(AuthorityError::Unauthorized);
        }
        cap::check_subset(rights, parent_record.rights)?;
        let root_actor = cap::effective_root_actor(&mut transaction, parent).await?;
        let cap_id = cap::new_cap_id();
        AuthorityStateStore::insert_cap_tx(
            &mut transaction,
            &cap_id,
            object,
            rights,
            Some(parent),
            root_actor.as_ref(),
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|error| AuthorityError::CommitUnknown(format!("mint commit: {error}")))?;
        Ok(cap_id)
    }

    /// Check a presented cap against the table: issued here, unretracted, and
    /// every chain step narrowed or held.
    pub async fn verify_issued(&self, cap_id: &CapId) -> Result<VerifiedCap, AuthorityError> {
        let mut transaction = self.state.pool.begin().await?;
        let result = async {
            let record = cap::verify_chain(&mut transaction, cap_id).await?;
            let root_actor = cap::effective_root_actor(&mut transaction, cap_id).await?;
            let _unused = record;
            Ok::<_, AuthorityError>(VerifiedCap {
                cap: *cap_id,
                root_actor,
            })
        }
        .await;
        transaction.rollback().await?;
        result
    }

    pub async fn retract(&self, cap_id: &CapId) -> Result<(), AuthorityError> {
        let mut transaction = self.state.begin_serialized().await?;
        AuthorityStateStore::retract_cap_tx(&mut transaction, cap_id).await?;
        transaction
            .commit()
            .await
            .map_err(|error| AuthorityError::CommitUnknown(format!("retract commit: {error}")))?;
        Ok(())
    }

    /// Did the registry mint this subsystem cap, and which actor does it root at?
    pub async fn actor_of(&self, cap_id: &CapId) -> Result<Option<Actor>, AuthorityError> {
        let mut transaction = self.state.pool.begin().await?;
        let result = async {
            let record = cap::verify_chain(&mut transaction, cap_id).await?;
            if !record.object.starts_with("is subsystem ") && record.object != "is operator" {
                return Ok(None);
            }
            let Some(key) = cap::effective_root_actor(&mut transaction, cap_id).await? else {
                return Ok(None);
            };
            let Some(actor) = AuthorityStateStore::actor_by_key_tx(&mut transaction, &key).await?
            else {
                return Ok(None);
            };
            actor_to_public(actor, *cap_id)
        }
        .await;
        transaction.rollback().await?;
        result
    }

    pub async fn actor_by_uuid(&self, uuid: Uuid7) -> Result<Option<Actor>, AuthorityError> {
        let mut transaction = self.state.pool.begin().await?;
        let result = async {
            let Some(actor) =
                AuthorityStateStore::actor_by_uuid_tx(&mut transaction, &uuid.to_string()).await?
            else {
                return Ok(None);
            };
            // Cap is looked up by root_actor match.
            let cap_id = find_actor_cap(&mut transaction, &actor.public_key).await?;
            actor_to_public(actor, cap_id.unwrap_or([0; 16]))
        }
        .await;
        transaction.rollback().await?;
        result
    }

    pub async fn grant(
        &self,
        subject: &str,
        object: &str,
        access: &str,
    ) -> Result<(), AuthorityError> {
        let mut transaction = self.state.begin_serialized().await?;
        ledger::grant(&mut transaction, subject, object, access).await?;
        transaction
            .commit()
            .await
            .map_err(|error| AuthorityError::CommitUnknown(format!("grant commit: {error}")))?;
        Ok(())
    }

    pub async fn revoke_grant(
        &self,
        subject: &str,
        object: &str,
        access: &str,
    ) -> Result<(), AuthorityError> {
        let mut transaction = self.state.begin_serialized().await?;
        ledger::revoke(&mut transaction, subject, object, access).await?;
        transaction.commit().await.map_err(|error| {
            AuthorityError::CommitUnknown(format!("revoke grant commit: {error}"))
        })?;
        Ok(())
    }

    pub async fn has_grant(
        &self,
        subject: &str,
        object: &str,
        access: &str,
    ) -> Result<bool, AuthorityError> {
        let mut transaction = self.state.pool.begin().await?;
        let result = ledger::has(&mut transaction, subject, object, access).await;
        transaction.rollback().await?;
        result
    }

    /// Sign a v2 facet claim as the actor the cap chain roots at. Which drawer
    /// authorizes the claim's document is DocumentsRepo's check, done before
    /// this call; here we only bind the claim's actor UUID to the root actor.
    pub async fn sign_as(
        &self,
        cap_id: &CapId,
        claim: &FacetClaim,
    ) -> Result<SignedFacetClaim, AuthorityError> {
        let mut transaction = self.state.begin_serialized().await?;
        let signed = signer::sign_as_in(self, &mut transaction, cap_id, claim).await?;
        transaction.rollback().await?;
        Ok(signed)
    }

    /// Verify a historical claim's actor signature without consulting current
    /// grant or cap state. The signed key must be the actor key named.
    pub fn verify_claim(&self, signed: &SignedFacetClaim) -> Result<(), AuthorityError> {
        signed.verify()?;
        Ok(())
    }

    pub fn node_issuer(&self) -> MultibaseKey {
        MultibaseKey::try_from(self.node_public_key.as_slice()).expect("node key is 32 bytes")
    }
}

fn actor_to_public(record: ActorRecord, cap_id: CapId) -> Result<Option<Actor>, AuthorityError> {
    let (Some(uuid_text), Some(url), Some(cert)) = (
        record.uuid.clone(),
        record.url.clone(),
        record.certificate.clone(),
    ) else {
        return Ok(None);
    };
    let uuid = uuid_text
        .parse::<uuid::Uuid>()
        .map_err(|_| AuthorityError::StateCorrupt("actor uuid".into()))?;
    Ok(Some(Actor {
        uuid: Uuid7::try_from(uuid)?,
        url,
        public_key: record.public_key,
        certificate: Bytes::from(cert),
        cap: cap_id,
    }))
}

async fn find_actor_cap(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    public_key: &ActorPublicKey,
) -> Result<Option<CapId>, AuthorityError> {
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT cap_id FROM authority_caps WHERE root_actor = ?1 AND object LIKE 'is subsystem %' ORDER BY rowid LIMIT 1",
    )
    .bind(public_key.as_slice())
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|row| fixed_bytes(row.try_get("cap_id")?))
        .transpose()
}
