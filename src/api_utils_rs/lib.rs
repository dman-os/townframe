pub mod api;
pub mod codecs;
pub mod macros;
// pub mod testing;
pub mod errs;

pub mod prelude {
    pub use utils_rs::prelude::*;

    pub use crate::api::*;
    pub use crate::interlude::*;

    pub use crate::wit::townframe::api_utils::utils::{Datetime, Uuid};
    pub use crate::wit::utils::{ErrorInternal, ErrorsValidation};
    pub use crate::wit::wasi::clocks::wall_clock;

    pub use axum_extra;
    pub use dotenv_flow;
    pub use educe;
    pub use garde::Validate;
    pub use regex;
    pub use tokio;
    pub use tower;
}

mod interlude {
    pub use utils_rs::prelude::*;

    pub use crate::internal_err;
    pub use axum::{self, response::IntoResponse, Json};
    pub use utoipa::{self, openapi};
}

pub mod wit {
    wit_bindgen::generate!({
        // generate_all,
        async: true,
        additional_derives: [serde::Serialize, serde::Deserialize],
        with: {
            "wasi:keyvalue/store@0.2.0-draft": generate,
            "wasi:keyvalue/atomics@0.2.0-draft": generate,
            "wasi:logging/logging@0.1.0-draft": generate,
            "wasmcloud:postgres/types@0.1.1-draft": generate,
            "wasmcloud:postgres/query@0.1.1-draft": generate,
            "wasi:io/poll@0.2.6": generate,
            "wasi:clocks/monotonic-clock@0.2.6": generate,
            "wasi:clocks/wall-clock@0.2.6": generate,

            "townframe:api-utils/utils/errors-validation": crate::errs::ErrorsValidation,
            "townframe:api-utils/utils/error-internal": crate::errs::ErrorInternal,
        }
    });

    pub mod utils {
        pub use crate::errs::{ErrorInternal, ErrorsValidation};
        pub use crate::wit::townframe::api_utils::utils::*;
    }
    use crate::interlude::utoipa;
    use crate::interlude::OffsetDateTime;
    use crate::interlude::*;

    use townframe::api_utils::utils::Datetime;
    use wasmcloud::postgres::types::PgValue;

    // impl From<String> for crate::wit::townframe::api_utils::utils::ErrorInternal {}

    impl PgValue {
        pub fn to_text(self) -> String {
            match self {
                PgValue::Text(val) => val,
                val => panic!("was expecting text, got {val:?}"),
            }
        }

        pub fn to_datetime(self) -> Datetime {
            match self {
                PgValue::TimestampTz(val) => {
                    let date = match val.timestamp.date {
                        wasmcloud::postgres::types::Date::PositiveInfinity => time::Date::MAX,
                        wasmcloud::postgres::types::Date::NegativeInfinity => time::Date::MIN,
                        wasmcloud::postgres::types::Date::Ymd((year, month, date)) => {
                            if month > 12 {
                                panic!("invalid month: {val:?}")
                            }
                            if date > 31 {
                                panic!("invalid date: {val:?}")
                            }
                            match time::Date::from_calendar_date(
                                year,
                                match (month as u8).try_into() {
                                    Ok(val) => val,
                                    Err(err) => panic!("invalid month: {err} - {val:?}"),
                                },
                                date as u8,
                            ) {
                                Ok(val) => val,
                                Err(err) => panic!("invalid date: {err} - {val:?}"),
                            }
                        }
                    };
                    if val.timestamp.time.hour > 23
                        || val.timestamp.time.min > 59
                        || val.timestamp.time.sec > 59
                        || val.timestamp.time.micro > 1_000_000
                    {
                        panic!("invalid time: {val:?}");
                    }
                    let time = match time::Time::from_hms_micro(
                        val.timestamp.time.hour as u8,
                        val.timestamp.time.min as u8,
                        val.timestamp.time.sec as u8,
                        val.timestamp.time.micro,
                    ) {
                        Ok(val) => val,
                        Err(err) => panic!("invalid time: {err} - {val:?} "),
                    };
                    let offset = time::UtcOffset::from_whole_seconds(match val.offset {
                        wasmcloud::postgres::types::Offset::EasternHemisphereSecs(secs) => secs,
                        wasmcloud::postgres::types::Offset::WesternHemisphereSecs(secs) => -secs,
                    })
                    .expect("invalid offset");
                    OffsetDateTime::new_in_offset(date, time, offset).into()
                }
                val => panic!("was expecting timestamptz, got {val:?}"),
            }
        }
    }

    impl From<OffsetDateTime> for Datetime {
        fn from(value: OffsetDateTime) -> Self {
            let seconds = value.unix_timestamp();
            if seconds < 0 {
                panic!("unsupported time: before unix epoch");
            }
            Self {
                seconds: seconds.try_into().unwrap(),
                nanoseconds: value.nanosecond(),
            }
        }
    }
    impl utoipa::ToSchema for Datetime {}
    impl utoipa::PartialSchema for Datetime {
        fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
            todo!()
            // <OffsetDateTime as utoipa::PartialSchema>::schema()
        }
    }
}
