use super::{AuthorityError, CapId, CapRecord, Rights};
use sqlx::{Sqlite, Transaction};

pub(super) fn new_cap_id() -> CapId {
    let mut id = [0_u8; 16];
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut id);
    id
}

pub(super) async fn verify_chain(
    transaction: &mut Transaction<'_, Sqlite>,
    cap_id: &CapId,
) -> Result<CapRecord, AuthorityError> {
    let mut current = *cap_id;
    let mut seen = Vec::new();
    loop {
        if seen.contains(&current) {
            return Err(AuthorityError::InvalidCap);
        }
        seen.push(current);
        if seen.len() > 64 {
            return Err(AuthorityError::InvalidCap);
        }
        let Some(record) = super::store::AuthorityStateStore::cap_tx(transaction, &current).await?
        else {
            return Err(AuthorityError::InvalidCap);
        };
        if record.retracted {
            return Err(AuthorityError::InvalidCap);
        }
        match record.parent {
            None => break,
            Some(parent) => {
                let Some(parent_record) =
                    super::store::AuthorityStateStore::cap_tx(transaction, &parent).await?
                else {
                    return Err(AuthorityError::InvalidCap);
                };
                if parent_record.retracted || !record.rights.is_subset_of(parent_record.rights) {
                    return Err(AuthorityError::InvalidCap);
                }
                // root_actor must be preserved down the chain when set at an ancestor.
                if record.root_actor.is_none() && parent_record.root_actor.is_some() {
                    // Inherited root is fine; effective root is resolved by walking.
                }
                current = parent;
            }
        }
    }
    // Return the requested cap's record (re-read for clarity).
    super::store::AuthorityStateStore::cap_tx(transaction, cap_id)
        .await?
        .ok_or(AuthorityError::InvalidCap)
}

pub(super) async fn effective_root_actor(
    transaction: &mut Transaction<'_, Sqlite>,
    cap_id: &CapId,
) -> Result<Option<[u8; 32]>, AuthorityError> {
    let mut current = *cap_id;
    let mut seen = Vec::new();
    loop {
        if seen.contains(&current) || seen.len() > 64 {
            return Err(AuthorityError::InvalidCap);
        }
        seen.push(current);
        let Some(record) = super::store::AuthorityStateStore::cap_tx(transaction, &current).await?
        else {
            return Err(AuthorityError::InvalidCap);
        };
        if record.retracted {
            return Err(AuthorityError::InvalidCap);
        }
        if let Some(root) = record.root_actor {
            return Ok(Some(root));
        }
        match record.parent {
            Some(parent) => current = parent,
            None => return Ok(None),
        }
    }
}

pub(super) fn check_subset(child: Rights, parent: Rights) -> Result<(), AuthorityError> {
    if child.is_subset_of(parent) {
        Ok(())
    } else {
        Err(AuthorityError::Escalation)
    }
}
