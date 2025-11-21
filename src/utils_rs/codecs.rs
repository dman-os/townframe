// crate::time::serde::format_description!(sane_iso8601, OffsetDateTime, FORMAT);
pub mod sane_iso8601 {
    use crate::interlude::*;
    use time::format_description::well_known::{
        iso8601::{self, TimePrecision},
        Iso8601,
    };

    const CONFIG: iso8601::EncodedConfig = iso8601::Config::DEFAULT
        .set_year_is_six_digits(false)
        .set_time_precision(TimePrecision::Second {
            decimal_digits: None,
        })
        .encode();
    pub const FORMAT: Iso8601<CONFIG> = Iso8601::<CONFIG>;

    use time::OffsetDateTime as __TimeSerdeType;

    struct Visitor;
    struct OptionVisitor;
    impl serde::de::Visitor<'_> for Visitor {
        type Value = __TimeSerdeType;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!(
                "a(n) `OffsetDateTime` in the format \"{0}\"",
                "Iso8601"
            ))
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<__TimeSerdeType, E> {
            __TimeSerdeType::parse(value, &FORMAT).map_err(E::custom)
        }
    }
    impl<'a> serde::de::Visitor<'a> for OptionVisitor {
        type Value = Option<__TimeSerdeType>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!(
                "an `Option<OffsetDateTime>` in the format \"{0}\"",
                "Iso8601"
            ))
        }
        fn visit_some<D: serde::de::Deserializer<'a>>(
            self,
            deserializer: D,
        ) -> Result<Option<__TimeSerdeType>, D::Error> {
            deserializer.deserialize_any(Visitor).map(Some)
        }
        fn visit_none<E: serde::de::Error>(self) -> Result<Option<__TimeSerdeType>, E> {
            Ok(None)
        }
    }
    pub fn serialize<S: serde::Serializer>(
        datetime: &__TimeSerdeType,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        use serde::Serialize;
        datetime
            .format(&FORMAT)
            .map_err(time::error::Format::into_invalid_serde_value::<S>)?
            .serialize(serializer)
    }

    pub fn deserialize<'a, D: serde::Deserializer<'a>>(
        deserializer: D,
    ) -> Result<__TimeSerdeType, D::Error> {
        deserializer.deserialize_any(Visitor)
    }

    pub mod option {
        use super::*;

        pub fn serialize<S: serde::Serializer>(
            option: &Option<__TimeSerdeType>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            use serde::Serialize;
            option
                .map(|datetime| datetime.format(&FORMAT))
                .transpose()
                .map_err(time::error::Format::into_invalid_serde_value::<S>)?
                .serialize(serializer)
        }
        pub fn deserialize<'a, D: serde::Deserializer<'a>>(
            deserializer: D,
        ) -> Result<Option<__TimeSerdeType>, D::Error> {
            deserializer.deserialize_option(OptionVisitor)
        }
    }
}
