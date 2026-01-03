/// Serde codec for `Datetime` via `Timestamp`.
pub mod datetime {
    use crate::interlude::*;
    use crate::wit::townframe::api_utils::utils::Datetime;

    pub fn serialize<S: serde::Serializer>(
        datetime: &Datetime,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        // Convert Datetime to Timestamp
        let ts = Timestamp::from_second(datetime.seconds as i64)
            .map_err(|err| serde::ser::Error::custom(format!("invalid timestamp: {err}")))?
            .checked_add(std::time::Duration::from_nanos(datetime.nanoseconds.into()))
            .map_err(|err| serde::ser::Error::custom(format!("invalid nanoseconds: {err}")))?;
        ts.serialize(serializer)
    }

    pub fn deserialize<'a, D: serde::Deserializer<'a>>(
        deserializer: D,
    ) -> Result<Datetime, D::Error> {
        let ts = Timestamp::deserialize(deserializer)?;
        Ok(ts.into())
    }

    pub mod option {
        use super::*;

        pub fn serialize<S: serde::Serializer>(
            option: &Option<Datetime>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            match option {
                Some(dt) => super::serialize(dt, serializer),
                None => serializer.serialize_none(),
            }
        }

        pub fn deserialize<'a, D: serde::Deserializer<'a>>(
            deserializer: D,
        ) -> Result<Option<Datetime>, D::Error> {
            Option::<Timestamp>::deserialize(deserializer).map(|opt| opt.map(Into::into))
        }
    }
}
