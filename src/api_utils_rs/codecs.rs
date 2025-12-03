/// Serde codec for `Datetime` that delegates to `utils_rs::codecs::sane_iso8601`
/// via `OffsetDateTime`.
pub mod datetime {
    use crate::interlude::*;
    use crate::wit::townframe::api_utils::utils::Datetime;
    use utils_rs::codecs::sane_iso8601;

    pub fn serialize<S: serde::Serializer>(
        datetime: &Datetime,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        // Convert Datetime to OffsetDateTime
        let offset_dt = OffsetDateTime::from_unix_timestamp(datetime.seconds as i64)
            .map_err(|e| serde::ser::Error::custom(format!("invalid timestamp: {e}")))?;
        let offset_dt = offset_dt
            .replace_nanosecond(datetime.nanoseconds)
            .map_err(|e| serde::ser::Error::custom(format!("invalid nanoseconds: {e}")))?;
        // Delegate to sane_iso8601
        sane_iso8601::serialize(&offset_dt, serializer)
    }

    pub fn deserialize<'a, D: serde::Deserializer<'a>>(
        deserializer: D,
    ) -> Result<Datetime, D::Error> {
        // Deserialize as OffsetDateTime using sane_iso8601
        let offset_dt = sane_iso8601::deserialize(deserializer)?;
        // Convert to Datetime using From impl
        Ok(offset_dt.into())
    }

    pub mod option {
        use super::*;

        pub fn serialize<S: serde::Serializer>(
            option: &Option<Datetime>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            use serde::Serialize;
            match option {
                Some(dt) => {
                    let offset_dt = OffsetDateTime::from_unix_timestamp(dt.seconds as i64)
                        .map_err(|e| {
                            serde::ser::Error::custom(format!("invalid timestamp: {e}"))
                        })?;
                    let offset_dt = offset_dt.replace_nanosecond(dt.nanoseconds).map_err(|e| {
                        serde::ser::Error::custom(format!("invalid nanoseconds: {e}"))
                    })?;
                    Some(offset_dt.format(&sane_iso8601::FORMAT))
                        .transpose()
                        .map_err(time::error::Format::into_invalid_serde_value::<S>)?
                        .serialize(serializer)
                }
                None => serializer.serialize_none(),
            }
        }

        pub fn deserialize<'a, D: serde::Deserializer<'a>>(
            deserializer: D,
        ) -> Result<Option<Datetime>, D::Error> {
            use utils_rs::codecs::sane_iso8601::option;
            // Delegate to sane_iso8601::option::deserialize and convert
            option::deserialize(deserializer).map(|opt| opt.map(Into::into))
        }
    }
}
