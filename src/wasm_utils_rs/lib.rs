pub mod codecs;

mod interlude {
    pub use utils_rs::prelude::Deserialize;
    pub use utils_rs::prelude::Serialize;
    pub use utils_rs::prelude::Timestamp;
}

pub mod wit {
    wit_bindgen::generate!({
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
            "townframe:api-utils/utils": generate,
        }
    });

    pub mod utils {
        pub use crate::wit::townframe::api_utils::utils::Datetime;
        pub use crate::wit::townframe::api_utils::utils::ErrorInternal;
        pub use crate::wit::townframe::api_utils::utils::ErrorsValidation;
        pub use crate::wit::townframe::api_utils::utils::Json;
        pub use crate::wit::townframe::api_utils::utils::Uuid;
        pub use crate::wit::townframe::api_utils::utils::noop;
    }

    use crate::interlude::*;
    use townframe::api_utils::utils::Datetime;

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
}
