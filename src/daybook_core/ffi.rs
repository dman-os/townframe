use crate::interlude::*;

use super::Doc;

uniffi::custom_type!(OffsetDateTime, i64, {
    remote,
    lower: |dt| dt.unix_timestamp(),
    try_lift: |int| OffsetDateTime::from_unix_timestamp(int)
        .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
});

uniffi::custom_type!(Uuid, Vec<u8>, {
    remote,
    lower: |uuid| uuid.as_bytes().to_vec(),
    try_lift: |bytes: Vec<u8>| {
        uuid::Uuid::from_slice(&bytes)
            .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
    }
});

#[derive(Debug, thiserror::Error, uniffi::Object)]
#[error(transparent)]
pub struct FfiError {
    inner: eyre::Report,
}

impl From<eyre::Report> for FfiError {
    fn from(inner: eyre::Report) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl FfiError {
    fn message(&self) -> String {
        format!("{}", self.inner)
    }
}

#[derive(uniffi::Object)]
struct DocsRepo {
    ctx: SharedCtx,
}

#[uniffi::export]
impl DocsRepo {
    #[uniffi::constructor]
    fn new(ctx: SharedCtx) -> Arc<Self> {
        Arc::new(Self { ctx })
    }

    async fn get(&self, id: Uuid) -> Result<Option<Doc>, FfiError> {
        todo!()
    }

    async fn set(&self, doc: Doc) -> Result<(), FfiError> {
        todo!()
    }

    async fn list(&self) -> Result<Vec<Doc>, FfiError> {
        Ok(vec![
            Doc {
                id: Uuid::new_v4(),
                timestamp: OffsetDateTime::now_utc(),
            },
            Doc {
                id: Uuid::new_v4(),
                timestamp: OffsetDateTime::now_utc(),
            },
        ])
    }
}

#[uniffi::export]
impl Ctx {
    #[uniffi::constructor]
    fn for_ffi() -> Result<SharedCtx, FfiError> {
        Ok(Ctx::new()?)
    }
}
