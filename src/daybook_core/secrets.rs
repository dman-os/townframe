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
        if std::env::var("DAYB_DISABLE_KEYRING")
            .map(|value| value == "1")
            .unwrap_or(false)
        {
            let secret = load_or_init_fallback_secret(sql, &repo_id).await?;
            let public = secret.public();
            return Ok(RepoIdentity {
                repo_id,
                iroh_secret_key: secret,
                iroh_public_key: public,
            });
        }
        let service_name = format!("daybook.repo.{repo_id}");
        let secret = match keyring::Entry::new(&service_name, Self::KEYRING_USERNAME) {
            Ok(entry) => match entry.get_password() {
                Ok(secret_hex) => {
                    let secret = decode_secret_hex(&secret_hex)?;
                    let encoded = data_encoding::HEXLOWER.encode(&secret.to_bytes());
                    set_fallback_secret(sql, &repo_id, &encoded).await?;
                    secret
                }
                Err(keyring::Error::NoEntry) => {
                    let secret = load_or_init_fallback_secret(sql, &repo_id).await?;
                    let encoded = data_encoding::HEXLOWER.encode(&secret.to_bytes());
                    if let Err(err) = entry.set_password(&encoded) {
                        warn!(?err, "failed backfilling keyring from fallback secret");
                    }
                    secret
                }
                Err(err) => {
                    warn!(
                        ?err,
                        "error reading iroh secret key from keyring, using fallback"
                    );
                    load_or_init_fallback_secret(sql, &repo_id).await?
                }
            },
            Err(err) => {
                warn!(
                    ?err,
                    "error creating keyring entry, using fallback secret store"
                );
                load_or_init_fallback_secret(sql, &repo_id).await?
            }
        };

        let public = secret.public();
        Ok(RepoIdentity {
            repo_id,
            iroh_secret_key: secret,
            iroh_public_key: public,
        })
    }
}

fn fallback_secret_key(repo_id: &str) -> String {
    format!("iroh_secret_key_fallback_v1:{repo_id}")
}

async fn load_or_init_fallback_secret(sql: &SqlitePool, repo_id: &str) -> Res<iroh::SecretKey> {
    let fallback_key = fallback_secret_key(repo_id);
    let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
        .bind(&fallback_key)
        .fetch_optional(sql)
        .await?;
    if let Some(secret_hex) = rec {
        return decode_secret_hex(&secret_hex);
    }
    let generated = iroh::SecretKey::generate(&mut rand::rng());
    let encoded = data_encoding::HEXLOWER.encode(&generated.to_bytes());
    set_fallback_secret(sql, repo_id, &encoded).await?;
    Ok(generated)
}

async fn set_fallback_secret(sql: &SqlitePool, repo_id: &str, encoded: &str) -> Res<()> {
    let fallback_key = fallback_secret_key(repo_id);
    sqlx::query(
        "INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(&fallback_key)
    .bind(encoded)
    .execute(sql)
    .await?;
    Ok(())
}

#[cfg(test)]
pub async fn force_set_fallback_secret_for_tests(
    sql: &SqlitePool,
    repo_id: &str,
    secret: &iroh::SecretKey,
) -> Res<()> {
    let encoded = data_encoding::HEXLOWER.encode(&secret.to_bytes());
    set_fallback_secret(sql, repo_id, &encoded).await
}

fn decode_secret_hex(secret_hex: &str) -> Res<iroh::SecretKey> {
    let raw = data_encoding::HEXLOWER
        .decode(secret_hex.as_bytes())
        .wrap_err("invalid encoded iroh secret key in keyring")?;
    if raw.len() != 32 {
        eyre::bail!("invalid iroh secret key length in keyring: {}", raw.len());
    }
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(&raw);
    Ok(iroh::SecretKey::from_bytes(&bytes))
}
