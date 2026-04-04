use crate::interlude::*;

use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct RepoIdentity {
    pub repo_id: String,
    pub iroh_secret_key: iroh::SecretKey,
    pub iroh_public_key: iroh::PublicKey,
}

pub struct SecretRepo;

impl SecretRepo {
    const KEYRING_USERNAME: &'static str = "iroh_secret_key_v1";

    pub async fn load_or_init_identity(sql: &SqlitePool, repo_id: &str) -> Res<RepoIdentity> {
        let repo_id = repo_id.to_string();
        let fallback_secret = load_or_init_fallback_secret(sql, &repo_id).await?;
        let fallback_secret_hex = data_encoding::HEXLOWER.encode(&fallback_secret.to_bytes());
        if !should_use_keyring() {
            let public = fallback_secret.public();
            return Ok(RepoIdentity {
                repo_id,
                iroh_secret_key: fallback_secret.clone(),
                iroh_public_key: public,
            });
        }
        let service_name = format!("daybook.repo.{repo_id}");
        let entry =
            keyring::Entry::new(&service_name, Self::KEYRING_USERNAME).wrap_err_with(|| {
                format!("failed creating keyring entry for iroh secret key ({service_name})")
            })?;
        let secret = match entry.get_password() {
            Ok(secret_hex) => match decode_secret_hex(&secret_hex) {
                Ok(keyring_secret) => {
                    if keyring_secret.to_bytes() != fallback_secret.to_bytes() {
                        warn!("keyring and fallback iroh secrets diverged; repairing keyring from fallback");
                        entry
                            .set_password(&fallback_secret_hex)
                            .wrap_err("failed repairing keyring secret from fallback value")?;
                        fallback_secret.clone()
                    } else {
                        keyring_secret
                    }
                }
                Err(err) => {
                    warn!(
                        ?err,
                        "invalid iroh secret key in keyring, repairing from fallback"
                    );
                    entry
                        .set_password(&fallback_secret_hex)
                        .wrap_err("failed repairing keyring secret from fallback value")?;
                    fallback_secret.clone()
                }
            },
            Err(keyring::Error::NoEntry) => {
                entry
                    .set_password(&fallback_secret_hex)
                    .wrap_err("failed backfilling keyring from fallback secret")?;
                fallback_secret.clone()
            }
            Err(err) => {
                return Err(eyre::eyre!(err))
                    .wrap_err("failed reading iroh secret key from keyring");
            }
        };

        let public = secret.public();
        Ok(RepoIdentity {
            repo_id,
            iroh_secret_key: secret,
            iroh_public_key: public,
        })
    }

    pub async fn set_identity_from_secret_hex(
        sql: &SqlitePool,
        repo_id: &str,
        secret_hex: &str,
    ) -> Res<RepoIdentity> {
        let secret = decode_secret_hex(secret_hex)?;
        let encoded = data_encoding::HEXLOWER.encode(&secret.to_bytes());
        let fallback_key = fallback_secret_key(repo_id);
        sqlx::query(
            "INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(&fallback_key)
        .bind(&encoded)
        .execute(sql)
        .await?;
        if should_use_keyring() {
            let service_name = format!("daybook.repo.{repo_id}");
            let entry =
                keyring::Entry::new(&service_name, Self::KEYRING_USERNAME).wrap_err_with(|| {
                    format!(
                        "failed creating keyring entry for provisioned clone identity ({service_name})"
                    )
                })?;
            entry
                .set_password(&encoded)
                .wrap_err("failed setting keyring secret from provisioned clone identity")?;
        }
        let public = secret.public();
        Ok(RepoIdentity {
            repo_id: repo_id.to_string(),
            iroh_secret_key: secret,
            iroh_public_key: public,
        })
    }
}

fn is_keyring_disabled() -> bool {
    std::env::var("DAYB_DISABLE_KEYRING")
        .map(|value| value == "1")
        .unwrap_or(false)
}

fn has_persistent_keyring_backend() -> bool {
    match keyring::default::default_credential_builder().persistence() {
        keyring::credential::CredentialPersistence::EntryOnly
        | keyring::credential::CredentialPersistence::ProcessOnly => false,
        keyring::credential::CredentialPersistence::UntilReboot
        | keyring::credential::CredentialPersistence::UntilDelete => true,
        _ => false,
    }
}

fn should_use_keyring() -> bool {
    if is_keyring_disabled() {
        return false;
    }
    if !has_persistent_keyring_backend() {
        warn!(
            "keyring backend is not persistent on this target/build; using fallback secret store"
        );
        return false;
    }
    true
}

fn fallback_secret_key(repo_id: &str) -> String {
    format!("iroh_secret_key_fallback_v1:{repo_id}")
}

async fn load_or_init_fallback_secret(sql: &SqlitePool, repo_id: &str) -> Res<iroh::SecretKey> {
    let fallback_key = fallback_secret_key(repo_id);
    let generated = iroh::SecretKey::generate(&mut rand::rng());
    let encoded = data_encoding::HEXLOWER.encode(&generated.to_bytes());
    if ensure_fallback_secret(sql, repo_id, &encoded).await? {
        return Ok(generated);
    }

    let secret_hex = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
        .bind(&fallback_key)
        .fetch_optional(sql)
        .await?
        .ok_or_eyre("fallback iroh secret key missing after insert-or-ignore")?;
    decode_secret_hex(&secret_hex)
}

async fn ensure_fallback_secret(sql: &SqlitePool, repo_id: &str, encoded: &str) -> Res<bool> {
    let fallback_key = fallback_secret_key(repo_id);
    let result = sqlx::query("INSERT OR IGNORE INTO kvstore(key, value) VALUES (?1, ?2)")
        .bind(&fallback_key)
        .bind(encoded)
        .execute(sql)
        .await?;
    Ok(result.rows_affected() == 1)
}

#[cfg(test)]
pub async fn force_set_fallback_secret_for_tests(
    sql: &SqlitePool,
    repo_id: &str,
    secret: &iroh::SecretKey,
) -> Res<()> {
    let encoded = data_encoding::HEXLOWER.encode(&secret.to_bytes());
    let fallback_key = fallback_secret_key(repo_id);
    sqlx::query(
        "INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(&fallback_key)
    .bind(&encoded)
    .execute(sql)
    .await?;
    Ok(())
}

fn decode_secret_hex(secret_hex: &str) -> Res<iroh::SecretKey> {
    let raw = data_encoding::HEXLOWER
        .decode(secret_hex.as_bytes())
        .wrap_err("invalid encoded iroh secret key")?;
    if raw.len() != 32 {
        eyre::bail!("invalid iroh secret key length: {}", raw.len());
    }
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(&raw);
    Ok(iroh::SecretKey::from_bytes(&bytes))
}
