//! One-time storage transition; generic vtree paths are always literal UTF-8 keys.

use sqlx::{Row, SqlitePool};

use crate::backends::tokio_fs::names::{legacy_path, legacy_target};

use super::{StoreError, encoding, queries};

pub(super) const PAGE_SIZE: i64 = 256;

pub(super) async fn ensure_format(pool: &SqlitePool) -> Result<(), StoreError> {
    // Serialize concurrent opens before reading the marker that controls conversion.
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let format: String = sqlx::query_scalar(queries::FORMAT)
        .fetch_one(&mut *transaction)
        .await?;
    if format == encoding::FORMAT {
        transaction.commit().await?;
        return Ok(());
    }
    if format != "unix-bytes-v1" {
        return Err(StoreError::Format(format));
    }
    sqlx::query(queries::CONVERSION_CREATE)
        .execute(&mut *transaction)
        .await?;
    let mut after: Option<(i64, Vec<u8>)> = None;
    loop {
        let rows = if let Some((owner, path)) = &after {
            sqlx::query(queries::CONVERSION_AFTER)
                .bind(owner)
                .bind(path)
                .bind(PAGE_SIZE)
                .fetch_all(&mut *transaction)
                .await?
        } else {
            sqlx::query(queries::CONVERSION_START)
                .bind(PAGE_SIZE)
                .fetch_all(&mut *transaction)
                .await?
        };
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let owner: i64 = row.try_get("backend_id")?;
            let original: Vec<u8> = row.try_get("path")?;
            let path =
                legacy_path(&original).map_err(|error| StoreError::Encoding(error.to_string()))?;
            let kind: i64 = row.try_get("kind")?;
            if path.is_root() && kind != 1 {
                return Err(StoreError::RootKind);
            }
            let target: Option<Vec<u8>> = row.try_get("target")?;
            let target = match (kind, target) {
                (0 | 1, None) => None,
                (2, Some(bytes)) => Some(encoding::encode_target(
                    &legacy_target(&bytes)
                        .map_err(|error| StoreError::Encoding(error.to_string()))?,
                )?),
                _ => {
                    return Err(StoreError::Encoding(
                        "invalid legacy entry kind/target".into(),
                    ));
                }
            };
            sqlx::query(queries::CONVERSION_INSERT)
                .bind(owner)
                .bind(encoding::encode_path(&path))
                .bind(kind)
                .bind(row.try_get::<Option<i64>, _>("source_backend_id")?)
                .bind(row.try_get::<Option<Vec<u8>>, _>("source_output")?)
                .bind(row.try_get::<Option<Vec<u8>>, _>("source_version")?)
                .bind(row.try_get::<Option<i64>, _>("size")?)
                .bind(target)
                .execute(&mut *transaction)
                .await?;
            after = Some((owner, original));
        }
    }
    sqlx::query(queries::CONVERSION_CLEAR)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(queries::CONVERSION_INSTALL)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(queries::SET_FORMAT)
        .bind(encoding::FORMAT)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(queries::CONVERSION_DROP)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
}
