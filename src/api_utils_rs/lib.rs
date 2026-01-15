pub mod api;
pub mod codecs;
pub mod errs;
pub mod macros;

pub mod prelude {
    pub use utils_rs::prelude::*;

    pub use crate::api::*;
    pub use crate::interlude::*;

    pub use crate::wit::townframe::api_utils::utils::Datetime;
    pub use crate::wit::utils::{ErrorInternal, ErrorsValidation};
    pub use crate::wit::wasi::clocks::wall_clock;

    pub use dotenv_flow;
    pub use educe;
    pub use garde::Validate;
    pub use regex;
    pub use tokio;
}

mod interlude {
    pub use utils_rs::prelude::*;

    pub use crate::internal_err;
    pub use utoipa::{self, openapi};
}

pub mod wit {
    wit_bindgen::generate!({
        // generate_all,
        // async: true,
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
            "wasi:config/runtime@0.2.0-draft": generate,

            "townframe:api-utils/utils/errors-validation": crate::errs::ErrorsValidation,
            "townframe:api-utils/utils/error-internal": crate::errs::ErrorInternal,
        }
    });

    pub mod utils {
        pub use crate::errs::{ErrorInternal, ErrorsValidation};
        pub use crate::wit::townframe::api_utils::utils::*;
    }
    use crate::interlude::utoipa;
    use crate::interlude::Timestamp;
    use crate::interlude::*;

    use townframe::api_utils::utils::Datetime;
    use wasmcloud::postgres::types::PgValue;

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
                        wasmcloud::postgres::types::Date::PositiveInfinity => {
                            jiff::civil::Date::MAX
                        }
                        wasmcloud::postgres::types::Date::NegativeInfinity => {
                            jiff::civil::Date::MIN
                        }
                        wasmcloud::postgres::types::Date::Ymd((year, month, date)) => {
                            let Ok(year) = year.try_into() else {
                                panic!("unsupported year value: {year}");
                            };
                            let Ok(month) = month.try_into() else {
                                panic!("unsupported month value: {month}");
                            };
                            let Ok(date) = date.try_into() else {
                                panic!("unsupported date value: {date}");
                            };
                            match jiff::civil::Date::new(year, month, date) {
                                Ok(val) => val,
                                Err(err) => panic!("unsupported date: {err} - {val:?}"),
                            }
                        }
                    };
                    let wasmcloud::postgres::types::Time {
                        hour,
                        min,
                        sec,
                        micro,
                    } = val.timestamp.time;
                    let Ok(hour) = hour.try_into() else {
                        panic!("unsupported hour value: {hour}");
                    };
                    let Ok(min) = min.try_into() else {
                        panic!("unsupported minute value: {min}");
                    };
                    let Ok(sec) = sec.try_into() else {
                        panic!("unsupported second value: {sec}");
                    };
                    let Ok(micro) = std::time::Duration::from_micros(micro.into())
                        .subsec_nanos()
                        .try_into()
                    else {
                        panic!("unsupported microsecond value: {micro}");
                    };
                    let time = match jiff::civil::Time::new(hour, min, sec, micro) {
                        Ok(val) => val,
                        Err(err) => panic!("unsupported time: {err} - {val:?} "),
                    };
                    let offset = match val.offset {
                        wasmcloud::postgres::types::Offset::EasternHemisphereSecs(secs) => secs,
                        wasmcloud::postgres::types::Offset::WesternHemisphereSecs(secs) => -secs,
                    };
                    let Ok(offset) = offset.try_into() else {
                        panic!("unsupported offset value: {offset}");
                    };
                    let offset = match jiff::tz::Offset::from_hours(offset) {
                        Ok(val) => val,
                        Err(err) => panic!("unsupported offset: {err} - {val:?} "),
                    };
                    let tz = match date.to_datetime(time).to_zoned(offset.to_time_zone()) {
                        Ok(val) => val,
                        Err(err) => panic!("unsupported timestamptz: {err} - {val:?} "),
                    };
                    let ts = tz.timestamp();
                    ts.into()
                }
                val => panic!("was expecting timestamptz, got {val:?}"),
            }
        }
    }

    impl From<Datetime> for Timestamp {
        fn from(value: Datetime) -> Timestamp {
            match Timestamp::from_second(value.seconds as i64).and_then(|ts| {
                ts.checked_add(std::time::Duration::from_nanos(value.nanoseconds.into()))
            }) {
                Ok(val) => val,
                Err(err) => panic!("unsupported Datetime: {value:?} - {err:?}"),
            }
        }
    }
    impl From<Timestamp> for Datetime {
        fn from(value: Timestamp) -> Self {
            let seconds = value.as_second();
            if seconds < 0 {
                panic!("unsupported time seconds: {value:?}");
            }
            let nanoseconds = value.subsec_nanosecond();
            if nanoseconds < 0 {
                panic!("unsupported time nanoseconds: {value:?}");
            }
            Self {
                seconds: seconds as u64,
                nanoseconds: nanoseconds as u32,
            }
        }
    }
    impl utoipa::ToSchema for Datetime {}
    impl utoipa::PartialSchema for Datetime {
        fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
            todo!()
        }
    }
}
