// Todo
// - [ ] Spaces
// - [ ] Encryption keys
//  - [ ] HPKE per agent
// - [ ] Recovery keys
//  - [ ] encyrption recovery keys
//  - [ ] signing recovery keys
//
// Operations we need to support:
//  - Add a new agent
//      - Signed by another principal
//  - Add a new subgaent
//      - Use KDF to derive subagent from a local agent
//  - Add a space
//  - Rotate space key
//  - Add a recovery key

use crate::interlude::*;

use crate::stores::{AmStore, AmStoreHandle};

pub type AgentId = Url;

structstruck::strike! {
    #[structstruck::each[derive(Reconcile, Hydrate)]]
    struct KeysRepoStore {
        genesis: Option<struct RepoGenesis {
            repo_id: String,
            deets: struct RepoGenesisDeets {
                #![derive(Serialize)]

                #[autosurgeon(with = "am_utils_rs::codecs::through_str")]
                agent: Url,
                nonce: String,
                init_version: String,
                #[autosurgeon(with = "am_utils_rs::codecs::date")]
                created_at: Timestamp,
                // #[autosurgeon(with = "am_utils_rs::codecs::through_str")]
                // recovery_key: Url,
            }
        }>,
        #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
        agent_keys: HashMap<AgentId, struct AgentPubKeyDeets {
            pubkey: PubKey,
            #[autosurgeon(with = "am_utils_rs::codecs::date")]
            issued_at: Timestamp,
            #[autosurgeon(with = "am_utils_rs::codecs::through_str")]
            issued_by: AgentId,
            proof_alg: String,
            proof_sig: String,
        }>,
        #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
        recovery_keys: HashMap<AgentId, struct RecoveryPubKeyDeets {
            pubkey: String,
            issuer: String,
            proof: String,
        }>,
    }
}

impl AmStore for KeysRepoStore {
    fn prop() -> Cow<'static, str> {
        todo!()
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Reconcile, Hydrate)]]
    struct EncKeysStore {
        spaces: HashMap<String, struct Space {
            epoch: u32,
            deets: enum SpaceDeets {
                Blobs
            },
            recipients: HashMap<
                String,
                struct AgentEncKeyDeets {
                    pubkey: String,
                    alg: String,
                    encapsulated_key: String,
                    ciphertext: String,
                }
            >,
        }>,
    }
}

impl AmStore for EncKeysStore {
    fn prop() -> Cow<'static, str> {
        todo!()
    }
}

pub struct KeysRepo {
    secret_repo: crate::secrets::SecretRepo,
    store: AmStoreHandle<KeysRepoStore>,
    enckey_store: AmStoreHandle<EncKeysStore>,
}

impl KeysRepo {
    pub async fn load(
        app_doc_handle: big_repo::BigDocHandle,
        key_doc_handle: big_repo::BigDocHandle,
        secret_repo: crate::secrets::SecretRepo,
        local_user_path: daybook_types::doc::UserPathBuf,
    ) -> Res<Self> {
        let local_user_path =
            daybook_types::doc::user_path::for_repo(local_user_path, "keys-repo")?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);
        Ok(Self {
            secret_repo,
            store: {
                let store_val = KeysRepoStore::load(&app_doc_handle).await?;
                crate::stores::AmStoreHandle::new(
                    store_val,
                    app_doc_handle.clone(),
                    local_actor_id.clone(),
                )
            },
            enckey_store: {
                let store_val = EncKeysStore::load(&key_doc_handle).await?;
                crate::stores::AmStoreHandle::new(
                    store_val,
                    app_doc_handle.clone(),
                    local_actor_id.clone(),
                )
            },
        })
    }

    pub async fn genesis(&self) -> Res<GenesisResult> {
        if let Some(res) = self
            .store
            .query_sync(|store| {
                store.genesis.as_ref().map(|gen| GenesisResult::PreSuccess {
                    repo_id: gen.repo_id.clone(),
                    agent_url: gen.deets.agent.clone(),
                })
            })
            .await
        {
            return Ok(res);
        }
        let agent_skey = ed25519_dalek::SigningKey::from_bytes(&rand::random());
        let agent_pkey = PubKey(agent_skey.verifying_key());
        let agent_id = repo_agent_url(&agent_pkey);
        let now = Timestamp::now();
        // let recovery_key = ed25519_dalek::SigningKey::generate(&mut rand::random());
        let deets = RepoGenesisDeets {
            init_version: "daybook.repo.gensis.v1".into(),
            agent: agent_id.clone(),
            nonce: utils_rs::hash::encode_base58_multibase(rand::random::<[u8; 32]>()),
            created_at: now,
        };
        let deets_drisl = atproto_dasl::drisl::to_vec(&deets).expect(ERROR_JSON);
        let hash = blake3::hash(&deets_drisl);
        let repo_id = format!(
            "db-repo-{}",
            utils_rs::hash::encode_base58_multihash_blake3(*hash.as_bytes())
        );
        let (repo_id, _) = self
            .store
            .mutate_sync(|store| match &store.genesis {
                Some(gen) => gen.repo_id.clone(),
                None => {
                    store.genesis = Some(RepoGenesis {
                        repo_id: repo_id.clone(),
                        deets,
                    });
                    store.agent_keys.insert(
                        agent_id,
                        AgentPubKeyDeets {
                            pubkey: agent_pkey.clone(),
                            issued_by: repo_agent_genesis(),
                            issued_at: now,
                            proof_alg: "genesis".into(),
                            proof_sig: "genesis".into(),
                        },
                    );
                    repo_id
                }
            })
            .await?;
        Ok(GenesisResult::Success {
            repo_id,
            agent_pkey,
            agent_skey: PriKey(agent_skey),
        })
    }
}

pub enum GenesisResult {
    Success {
        repo_id: String,
        agent_pkey: PubKey,
        agent_skey: PriKey,
    },
    PreSuccess {
        repo_id: String,
        agent_url: Url,
    },
}

fn repo_agent_genesis() -> Url {
    format!("db+agent://genesis/")
        .parse()
        .expect(ERROR_IMPOSSIBLE)
}

fn repo_agent_url(pubkey: &PubKey) -> Url {
    format!("db+agent://repo-{pubkey}/")
        .parse()
        .expect(ERROR_IMPOSSIBLE)
}

fn recovery_agent_url(pubkey: &PubKey) -> Url {
    format!("db+agent://recovery-{pubkey}/")
        .parse()
        .expect(ERROR_IMPOSSIBLE)
}

pub struct PriKey(ed25519_dalek::SigningKey);

#[derive(Clone)]
pub struct PubKey(ed25519_dalek::VerifyingKey);

impl PubKey {
    const MULTIKEY_PREFIX: [u8; 2] = 0xED01_u16.to_be_bytes();

    #[must_use]
    pub fn new(bytes: &[u8; 32]) -> Result<Self, ed25519_dalek::SignatureError> {
        Ok(Self(ed25519_dalek::VerifyingKey::from_bytes(bytes)?))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0.as_bytes()
    }

    pub fn to_multikey(&self) -> [u8; 34] {
        let mut buf = [0; 34];
        buf.copy_from_slice(&Self::MULTIKEY_PREFIX);
        buf[2..].copy_from_slice(self.0.as_bytes());
        buf
    }
}

impl std::fmt::Display for PubKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // FIXME: use fixed size stack buffer to write string onto and then write that onto the
        // formatter
        write!(
            formatter,
            "{}",
            utils_rs::hash::encode_base58_multibase(self.to_multikey())
        )
    }
}

impl std::fmt::Debug for PubKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
/// Error decoding Multikey ed25519 PubKey
pub enum DecodeError {
    /// bad prefix
    BadPrefix,
    /// invalid bs58
    BadBs58,
    /// bad key: {0}
    BadKey(#[from] ed25519_dalek::SignatureError),
}

impl std::str::FromStr for PubKey {
    type Err = DecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let buf: [u8; 35] = bs58::decode(value.as_bytes())
            .into_array_const()
            .map_err(|_| DecodeError::BadBs58)?;
        if buf[0] != b'z' {
            return Err(DecodeError::BadPrefix);
        }
        if &buf[..2] != &Self::MULTIKEY_PREFIX[..] {
            return Err(DecodeError::BadPrefix);
        }
        let mut real_buf = [0; 32];
        real_buf.copy_from_slice(&buf[3..]);
        Ok(Self::new(&real_buf)?)
    }
}

impl Serialize for PubKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            format!("{self}").serialize(serializer)
        } else {
            serializer.serialize_bytes(&self.to_multikey())
        }
    }
}

impl<'de> serde::Deserialize<'de> for PubKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let str = String::deserialize(deserializer)?;
            Ok(str.parse().map_err(serde::de::Error::custom)?)
        } else {
            struct MyVisitor;
            impl<'de> serde::de::Visitor<'de> for MyVisitor {
                type Value = [u8; 32];

                fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
                    fmt.write_str("a 34 length byte string")
                }

                fn visit_bytes<E>(self, val: &[u8]) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    if val.len() != 34 {
                        return Err(serde::de::Error::invalid_length(
                            val.len(),
                            &"34 length byte array",
                        ));
                    }
                    if &val[..2] != &PubKey::MULTIKEY_PREFIX[..] {
                        return Err(serde::de::Error::custom("valid multikey prefix xED01_u16"));
                    }
                    let mut buf = [0u8; 32];
                    buf.copy_from_slice(val);
                    Ok(buf)
                }
            }
            let buf = deserializer.deserialize_bytes(MyVisitor)?;
            Self::new(&buf).map_err(serde::de::Error::custom)
        }
    }
}

impl autosurgeon::Reconcile for PubKey {
    type Key<'a> = autosurgeon::reconcile::NoKey;

    fn reconcile<R: autosurgeon::Reconciler>(&self, mut reconciler: R) -> Result<(), R::Error> {
        reconciler.bytes(self.0)
    }
}

impl autosurgeon::Hydrate for PubKey {
    fn hydrate_bytes(bytes: &[u8]) -> Result<Self, autosurgeon::HydrateError> {
        if bytes.len() != 32 {
            return Err(autosurgeon::HydrateError::unexpected(
                "PubKey in 34 length multikey byte array",
                format!("bytestring has byte length of {}", bytes.len()),
            ));
        }
        if &bytes[..2] != &Self::MULTIKEY_PREFIX[..] {
            return Err(autosurgeon::HydrateError::unexpected(
                "valid multikey prefix xED01_u16",
                "not the right prefix".into(),
            ));
        }
        let mut buf = [0_u8; 32];
        buf.copy_from_slice(&bytes[2..]);
        Self::new(&buf).map_err(|err| {
            autosurgeon::HydrateError::unexpected(
                "valid multikey encoded ed25519_dalek public key",
                format!("invalid pub key: {err}"),
            )
        })
    }
}
