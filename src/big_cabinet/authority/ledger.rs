use super::{AuthorityError, AuthorityStateStore};
use sqlx::{Sqlite, Transaction};

pub(super) async fn grant(
    transaction: &mut Transaction<'_, Sqlite>,
    subject: &str,
    object: &str,
    access: &str,
) -> Result<(), AuthorityError> {
    AuthorityStateStore::grant_tx(transaction, subject, object, access).await
}

pub(super) async fn revoke(
    transaction: &mut Transaction<'_, Sqlite>,
    subject: &str,
    object: &str,
    access: &str,
) -> Result<(), AuthorityError> {
    AuthorityStateStore::revoke_grant_tx(transaction, subject, object, access).await
}

pub(super) async fn has(
    transaction: &mut Transaction<'_, Sqlite>,
    subject: &str,
    object: &str,
    access: &str,
) -> Result<bool, AuthorityError> {
    AuthorityStateStore::has_grant_tx(transaction, subject, object, access).await
}
