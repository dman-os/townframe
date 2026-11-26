//! Answering the evaluated queries from the tables, by indexed reads.
//!
//! A subject that is not disputed has a live set equal to its positive reach, so its questions
//! are reads of `keyline_reach`. A disputed subject's are reads of `keyline_live`, which every
//! insert that could change it has already brought up to date inside its own transaction
//! (`stratum2::refresh_affected`): a read never computes anything and never writes.

use std::collections::BTreeMap;

use keyhive_crypto::digest::Digest;
use keyhive_crypto::identifier::Identifier;
use keyline::delegation::Delegation;
use keyline::power::Power;
use sqlite_rs::QueryExecutor;

use crate::error::Error;
use crate::rows;
use crate::sqlite::queries;

/// An evaluated question.
#[derive(Clone, Debug)]
pub enum Question {
    EffectivePower(Identifier, Identifier),
    Members(Identifier),
    IsLive(Digest<Delegation>),
    ConveyedPower(Digest<Delegation>),
}

/// An answer to a [`Question`], of the same shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    Power(Option<Power>),
    Members(BTreeMap<Identifier, Power>),
    Live(bool),
}

const ADMIN: i64 = 3;

fn bytes(identifier: Identifier) -> Vec<u8> {
    identifier.to_bytes().to_vec()
}

/// Where a subject's standing is stored.
enum Source {
    Reach,
    Live,
}

async fn source(q: &impl QueryExecutor, subject: Identifier) -> Result<Source, Error> {
    Ok(if queries::is_disputed(q, bytes(subject)).await? {
        Source::Live
    } else {
        Source::Reach
    })
}

/// `node`'s standing over `subject`, as a level, in context `ctx` of the live set: itself at
/// Admin unless the context excludes it.
async fn live_standing(
    q: &impl QueryExecutor,
    subject: Identifier,
    ctx: i64,
    node: Identifier,
) -> Result<Option<i64>, Error> {
    if subject == node {
        return Ok(if queries::is_excluded(q, ctx, bytes(node)).await? {
            None
        } else {
            Some(ADMIN)
        });
    }
    Ok(queries::live_standing(q, bytes(subject), ctx, bytes(node)).await?)
}

pub async fn ask(q: &impl QueryExecutor, question: &Question) -> Result<Reply, Error> {
    match *question {
        Question::EffectivePower(subject, audience) => {
            if subject == audience {
                return Ok(Reply::Power(Some(Power::Admin)));
            }
            let level = match source(q, subject).await? {
                Source::Reach => queries::reach_standing(q, bytes(subject), bytes(audience)).await?,
                Source::Live => live_standing(q, subject, 0, audience).await?,
            };
            Ok(Reply::Power(level.map(rows::power_from_sql)))
        }
        Question::Members(subject) => {
            let members = match source(q, subject).await? {
                Source::Reach => queries::reach_members(q, bytes(subject))
                    .await?
                    .into_iter()
                    .map(|row| (row.n, row.lvl))
                    .collect::<Vec<_>>(),
                Source::Live => queries::live_members(q, bytes(subject))
                    .await?
                    .into_iter()
                    .map(|row| (row.n, row.lvl))
                    .collect::<Vec<_>>(),
            };
            Ok(Reply::Members(
                members
                    .into_iter()
                    .map(|(node, level)| {
                        (
                            rows::identifier_from_sql(&node),
                            rows::power_from_sql(level),
                        )
                    })
                    .collect(),
            ))
        }
        Question::IsLive(delegation) | Question::ConveyedPower(delegation) => {
            let conveyed = matches!(question, Question::ConveyedPower(_));
            let none = Ok(if conveyed {
                Reply::Power(None)
            } else {
                Reply::Live(false)
            });
            let Some(row) = queries::delegation_row(q, delegation.as_slice().to_vec()).await?
            else {
                return none;
            };
            let subject = rows::identifier_from_sql(&row.subject);
            let issuer = rows::identifier_from_sql(&row.issuer);
            // The issuer's standing over the subject on routes that avoid what the delegation's
            // own revocations cover.
            let standing = match source(q, subject).await? {
                Source::Reach if issuer == subject => Some(ADMIN),
                Source::Reach => queries::reach_standing(q, bytes(subject), bytes(issuer)).await?,
                Source::Live => {
                    let (ctx, total) =
                        match queries::dispute_of(q, delegation.as_slice().to_vec()).await? {
                            Some(row) => (row.ctx, row.total == 1),
                            None => (0, false),
                        };
                    if total {
                        None
                    } else {
                        live_standing(q, subject, ctx, issuer).await?
                    }
                }
            };
            Ok(match standing {
                None => return none,
                Some(_) if !conveyed => Reply::Live(true),
                Some(level) => Reply::Power(Some(rows::power_from_sql(level.min(row.power)))),
            })
        }
    }
}
