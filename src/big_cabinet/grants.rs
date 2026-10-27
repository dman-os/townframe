use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::InspectCapability;
use big_cabinet_core::authority::MintCapability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::Uuid7;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use sqlite_rs::Database;
use sqlite_rs::Json;
use sqlite_rs::SqlError;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use utils_rs::expect_tags::ERROR_ACTOR;
use utils_rs::expect_tags::ERROR_CHANNEL;

mod sqlite {
    include!("grants/sqlite/gen.rs");
}

#[derive(Debug, thiserror::Error)]
pub enum GrantError {
    #[error("grant storage failed: {0}")]
    Storage(#[from] SqlError),
    #[error("grant capability failed: {0}")]
    Authority(#[from] ClientError<AuthorityServiceError>),
}

/// Domain grants for one namespace. Every operation runs on a task that owns the gate and
/// temporary-delegation caches, one operation at a time from its first authority call to its
/// last write: a lookup that mints a gate cannot interleave with a revoke that retracts it,
/// so no caller is handed a proof that a concurrent revoke already invalidated.
pub struct Grants {
    stores: Stores,
    commands: mpsc::Sender<Command>,
}

/// What an operation talks to. Each command carries its own copy, so the task holds the
/// authority client and the database only while it runs a command: dropping [`Grants`]
/// releases them at once, not whenever the idle task gets to notice its closed inbox.
#[derive(Clone)]
struct Stores {
    sql: Database,
    authority: Client<AuthorityProtocol>,
    namespace: Namespace,
}

#[derive(Default)]
struct State {
    gates: HashMap<(Uuid7, String), CapId>,
    temporary: HashMap<(Uuid7, String), HashMap<CapId, Rights>>,
}

pub struct GrantProof {
    pub rights: Rights,
    pub dependencies: Vec<CapId>,
}

type Reply<T> = oneshot::Sender<Result<T, GrantError>>;

struct Command {
    stores: Stores,
    context: RequestContext,
    operation: Operation,
}

enum Operation {
    Grant {
        subject: Uuid7,
        object: String,
        rights: Rights,
        source: Option<CapId>,
        reply: Reply<()>,
    },
    Revoke {
        subject: Uuid7,
        objects: Vec<String>,
        reply: Reply<()>,
    },
    Matching {
        subject: Uuid7,
        objects: Vec<String>,
        reply: Reply<Vec<GrantProof>>,
    },
}

impl Grants {
    pub async fn open(
        sql: Database,
        authority: Client<AuthorityProtocol>,
        namespace: Namespace,
    ) -> Result<Self, GrantError> {
        sql.apply_migrations(sqlite::MIGRATION_TABLE, sqlite::MIGRATIONS)
            .await?;
        let (commands, mut inbox) = mpsc::channel::<Command>(64);
        // The task ends once the last `Grants` handle, and with it the last sender, drops.
        tokio::spawn(async move {
            let mut state = State::default();
            while let Some(command) = inbox.recv().await {
                state.run(command).await;
            }
        });
        Ok(Self {
            stores: Stores {
                sql,
                authority,
                namespace,
            },
            commands,
        })
    }

    async fn ask<T>(
        &self,
        context: &RequestContext,
        operation: impl FnOnce(Reply<T>) -> Operation,
    ) -> Result<T, GrantError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(Command {
                stores: self.stores.clone(),
                context: context.clone(),
                operation: operation(reply),
            })
            .await
            .expect(ERROR_ACTOR);
        answer.await.expect(ERROR_CHANNEL)
    }

    pub async fn grant(
        &self,
        context: &RequestContext,
        subject: Uuid7,
        object: String,
        rights: Rights,
        source: Option<CapId>,
    ) -> Result<(), GrantError> {
        self.ask(context, |reply| Operation::Grant {
            subject,
            object,
            rights,
            source,
            reply,
        })
        .await
    }

    pub async fn revoke(
        &self,
        context: &RequestContext,
        subject: Uuid7,
        objects: &[String],
    ) -> Result<(), GrantError> {
        self.ask(context, |reply| Operation::Revoke {
            subject,
            objects: objects.to_vec(),
            reply,
        })
        .await
    }

    pub async fn matching(
        &self,
        context: &RequestContext,
        subject: Uuid7,
        objects: &[String],
    ) -> Result<Vec<GrantProof>, GrantError> {
        self.ask(context, |reply| Operation::Matching {
            subject,
            objects: objects.to_vec(),
            reply,
        })
        .await
    }
}

/// Run `operation` for a caller still waiting on `reply`, abandoning it at its next await if
/// the caller gives up: the same cancellation the caller would have applied had it run the
/// operation itself. A caller that gives up after the operation finished loses only the answer.
async fn answer<T>(mut reply: oneshot::Sender<T>, operation: impl Future<Output = T>) {
    let result = tokio::select! {
        biased;
        () = reply.closed() => return,
        result = operation => result,
    };
    drop(reply.send(result));
}

impl Stores {
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
}

impl State {
    async fn run(&mut self, command: Command) {
        let Command {
            stores,
            context,
            operation,
        } = command;
        match operation {
            Operation::Grant {
                subject,
                object,
                rights,
                source,
                reply,
            } => {
                answer(
                    reply,
                    self.grant(&stores, &context, subject, object, rights, source),
                )
                .await;
            }
            Operation::Revoke {
                subject,
                objects,
                reply,
            } => answer(reply, self.revoke(&stores, &context, subject, objects)).await,
            Operation::Matching {
                subject,
                objects,
                reply,
            } => answer(reply, self.matching(&stores, &context, subject, objects)).await,
        }
    }

    async fn retract_gate(
        &mut self,
        stores: &Stores,
        context: &RequestContext,
        key: &(Uuid7, String),
    ) -> Result<(), GrantError> {
        if let Some(capability) = self.gates.get(key).copied() {
            stores
                .authority
                .call(
                    context.child(),
                    RetractCapability {
                        via: stores.namespace.capability,
                        capability,
                    },
                )
                .await?;
            self.gates.remove(key);
        }
        Ok(())
    }

    async fn grant(
        &mut self,
        stores: &Stores,
        context: &RequestContext,
        subject: Uuid7,
        object: String,
        rights: Rights,
        source: Option<CapId>,
    ) -> Result<(), GrantError> {
        stores.ensure_live(context).await?;
        let key = (subject, object);
        if let Some(source) = source {
            let cap = stores
                .authority
                .call(context.child(), InspectCapability { capability: source })
                .await?;
            if !cap.rights.contains(Rights::DELEGATE) || !rights.is_subset_of(cap.rights) {
                return Err(ClientError::Operation(AuthorityServiceError::Escalation).into());
            }
            if self
                .temporary
                .get(&key)
                .and_then(|grants| grants.get(&source))
                == Some(&rights)
            {
                return Ok(());
            }
            self.retract_gate(stores, context, &key).await?;
            self.temporary
                .entry(key)
                .or_default()
                .insert(source, rights);
        } else {
            let existing = sqlite::get_grant(
                &stores.sql,
                stores.namespace.namespace_id.to_string(),
                subject.to_string(),
                key.1.clone(),
            )
            .await?;
            if existing == Some(rights.bits_i64()) {
                return Ok(());
            }
            self.retract_gate(stores, context, &key).await?;
            let transaction = stores.sql.begin_write().await?;
            sqlite::put_grant(
                &transaction,
                stores.namespace.namespace_id.to_string(),
                subject.to_string(),
                key.1,
                rights.bits_i64(),
            )
            .await?;
            transaction.commit().await?;
        }
        Ok(())
    }

    async fn revoke(
        &mut self,
        stores: &Stores,
        context: &RequestContext,
        subject: Uuid7,
        objects: Vec<String>,
    ) -> Result<(), GrantError> {
        stores.ensure_live(context).await?;
        for object in &objects {
            let key = (subject, object.clone());
            self.retract_gate(stores, context, &key).await?;
            self.temporary.remove(&key);
        }
        let transaction = stores.sql.begin_write().await?;
        sqlite::revoke_grants(
            &transaction,
            stores.namespace.namespace_id.to_string(),
            subject.to_string(),
            Json(objects),
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn matching(
        &mut self,
        stores: &Stores,
        context: &RequestContext,
        subject: Uuid7,
        objects: Vec<String>,
    ) -> Result<Vec<GrantProof>, GrantError> {
        stores.ensure_live(context).await?;
        let rows = sqlite::matching_grants(
            &stores.sql,
            Json(objects),
            stores.namespace.namespace_id.to_string(),
            subject.to_string(),
        )
        .await?;
        let mut proofs = Vec::with_capacity(rows.len());
        for row in rows {
            let key = (subject, row.object);
            let mut rights = Rights::from_bits(row.rights)
                .expect("domain grant rights are validated at insertion");
            let mut dependencies = Vec::new();
            if let Some(grants) = self.temporary.get_mut(&key) {
                let mut sources: Vec<_> = grants
                    .iter()
                    .map(|(source, held)| (*source, *held))
                    .collect();
                sources.sort_unstable_by_key(|(source, _)| *source.as_bytes());
                for (source, held) in sources {
                    if held.is_subset_of(rights) {
                        continue;
                    }
                    match stores
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
                let gate = match self.gates.get(&key).copied() {
                    Some(gate) => gate,
                    None => {
                        let cap = stores
                            .authority
                            .call(
                                context.child(),
                                MintCapability {
                                    issuer: stores.namespace.capability,
                                    parent: None,
                                    dependencies: Vec::new(),
                                    object: format!("grant gate {}", Uuid7::new()),
                                    rights: Rights::all(),
                                },
                            )
                            .await?;
                        assert!(self.gates.insert(key, cap.capability_id).is_none());
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
mod tests;
