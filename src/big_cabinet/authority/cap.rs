use super::AuthorityError;
use super::store::{self, CapRow};
use super::{CapId, Rights};
use big_cabinet_core::dmeta::Uuid7;
use sqlx::SqliteConnection;

const MAX_CHAIN_DEPTH: usize = 64;

pub(super) struct Chain {
    pub(super) cap: CapRow,
    pub(super) ancestors: Vec<CapId>,
}

impl Chain {
    pub(super) fn require(&self, right: Rights) -> Result<(), AuthorityError> {
        if self.cap.rights.contains(right) {
            Ok(())
        } else {
            Err(AuthorityError::Unauthorized)
        }
    }

    pub(super) fn bound(&self, rights: Rights) -> Result<(), AuthorityError> {
        if rights.is_subset_of(self.cap.rights) {
            Ok(())
        } else {
            Err(AuthorityError::Escalation)
        }
    }

    pub(super) fn child(
        &self,
        object: String,
        rights: Rights,
        issuer: Uuid7,
    ) -> Result<CapRow, AuthorityError> {
        self.bound(rights)?;
        if self.ancestors.len() + 1 >= MAX_CHAIN_DEPTH {
            return Err(AuthorityError::Escalation);
        }
        Ok(CapRow {
            id: CapId::new_random(),
            object,
            rights,
            parent: Some(self.cap.id),
            issuer,
            retracted: false,
        })
    }
}

pub(super) async fn walk(
    conn: &mut SqliteConnection,
    cap: &CapId,
) -> Result<Chain, AuthorityError> {
    let mut rows = store::chain(conn, cap, MAX_CHAIN_DEPTH).await?;
    let complete = rows.last().is_some_and(|root| root.parent.is_none());
    if !complete || rows.iter().any(|row| row.retracted) {
        return Err(AuthorityError::InvalidCapability);
    }
    let ancestors = rows.iter().skip(1).map(|row| row.id).collect();
    rows.truncate(1);
    Ok(Chain {
        cap: rows.pop().expect("a complete chain has the presented row"),
        ancestors,
    })
}

pub(super) fn may_retract(target: &Chain, via: &CapId) -> Result<(), AuthorityError> {
    if target.cap.id == *via || target.ancestors.contains(via) {
        Ok(())
    } else {
        Err(AuthorityError::Unauthorized)
    }
}

pub(super) async fn descends_from(
    conn: &mut SqliteConnection,
    cap: &CapId,
    ancestor: &CapId,
) -> Result<bool, AuthorityError> {
    Ok(store::chain(conn, cap, MAX_CHAIN_DEPTH)
        .await?
        .iter()
        .any(|row| row.id == *ancestor))
}
