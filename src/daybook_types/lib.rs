//! Daybook types crate
//!
//! This crate provides type definitions for daybook with feature-gated support
//! for automerge, uniffi, and wit bindings.

mod interlude {
    pub use serde::{Deserialize, Serialize};

    pub use utils_rs::prelude::*;
}

pub mod doc;
#[cfg(test)]
mod test;

#[cfg(feature = "wit")]
pub mod wit;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

#[cfg(feature = "uniffi")]
custom_type_set!();

#[macro_export]
#[allow(clippy::crate_in_macro_def)]
macro_rules! custom_type_set {
    () => {
        use crate::interlude::*;

        uniffi::custom_type!(Timestamp, i64, {
            remote,
            lower: |dt| dt.as_second(),
            try_lift: |int| Timestamp::from_second(int)
                .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
        });

        uniffi::custom_type!(PathBuf, String, {
            remote,
            lower: |path| path.into_os_string().into_string().expect(ERROR_UTF8),
            try_lift: |str| Ok(PathBuf::from(str)),
        });

        type Json = serde_json::Value;

        uniffi::custom_type!(Json, String, {
            remote,
            lower: |json| serde_json::to_string(&json).expect(ERROR_JSON),
            try_lift: |str| serde_json::from_str(&str)
                .map_err(|err| uniffi::deps::anyhow::anyhow!(err)),
        });

        uniffi::custom_type!(Uuid, Vec<u8>, {
            remote,
            lower: |uuid| uuid.as_bytes().to_vec(),
            try_lift: |bytes: Vec<u8>| {
                Uuid::from_slice(&bytes)
                    .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
            }
        });

        use $crate::doc::ChangeHashSet;
        uniffi::custom_type!(ChangeHashSet, Vec<String>, {
            remote,
            lower: |hash| utils_rs::am::serialize_commit_heads(&hash.0),
            try_lift: |strings: Vec<String>| {
                Ok(ChangeHashSet(utils_rs::am::parse_commit_heads(&strings).to_anyhow()?))
            }
        });

    };
}
#[macro_export]
#[allow(clippy::crate_in_macro_def)]
macro_rules! define_enum_and_tag {
    (@item
        $(#[$attr:meta])*
        struct $key:ident $body:tt
    ) => {
        // #[derive(Debug, Clone, Serialize, Deserialize)]
        // #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
        $(#[$attr])*
        pub struct $key $body
    };
    (@item
        $(#[$attr:meta])*
        // NOTE: parens around alias to allow $body::tt to capture the whole path
        type $key:ident ($alias:path)
    ) => {
        $(#[$attr])*
        pub type $key = $alias;
    };
    (@item
        $(#[$attr:meta])*
        enum $key:ident $body:tt
    ) => {
        // #[derive(Debug, Clone, Serialize, Deserialize)]
        // #[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
        $(#[$attr])*
        pub enum $key $body
    };
    (
        $reverse_domain_name:literal,
        $(#[$attr_tag:meta])*
        $tag_ty_name:ident,
        $(#[$attr_pay:meta])*
        $ty_name:ident {
            $(
                $(#[$attr_item:meta])*
                // NOTE: $kind comes after $key to avoid ambiguity with tt capturin
                // the $attr_item for some reason
                $key:ident $kind:tt $body:tt
            ),*
            $(,)?
        }
    ) => {
        $(#[$attr_pay])*
        pub enum $ty_name {
            $(
                $key($key),
            )*
        }

        impl $ty_name {
            pub fn tag(&self) -> $tag_ty_name {
                match self {
                    $(
                        Self::$key(..) => $tag_ty_name::$key,
                    )*
                }
            }
        }

        $(
            crate::define_enum_and_tag!(@item
                $(#[$attr_item])*
                $kind $key $body
            );

            // impl From<$key> for $ty_name {
            //     fn from(val: $key) -> Self {
            //         Self::$key(val)
            //     }
            // }
        )*

        $(#[$attr_tag])*
        pub enum $tag_ty_name {
            $(
                $key,
            )*
        }

        impl $tag_ty_name {
            pub const ALL: &[Self] = &[
                $(Self::$key,)*
            ];

            pub const ALL_STR: &[&str] = &[
                $(concat!($reverse_domain_name, stringify!($key:lower)),)*
            ];

            pub fn as_str(&self) -> &'static str {
                match self {
                    $(
                        Self::$key =>
                            concat!(
                                $reverse_domain_name,
                                pastey::paste! {
                                    stringify!([<$key:lower>])
                                }
                            ),
                    )*
                }
            }

            #[allow(clippy::should_implement_trait)]
            pub fn from_str(input: &str) -> Option<Self> {
                $(
                    if input.eq_ignore_ascii_case(concat!(
                        $reverse_domain_name,
                        pastey::paste! {
                            stringify!([<$key:lower>])
                        }
                    )) {
                        return Some(Self::$key);
                    }
                )*
                None
            }
        }

        impl std::fmt::Display for $tag_ty_name {
            fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(fmt, "{}", self.as_str())
            }
        }

        impl From<$tag_ty_name> for String {
            fn from(tag: $tag_ty_name) -> Self {
                tag.as_str().into()
            }
        }

        impl serde::Serialize for $tag_ty_name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $tag_ty_name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct Visitor;

                impl<'de> serde::de::Visitor<'de> for Visitor {
                    type Value = $tag_ty_name;

                    fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
                        fmt.write_str(stringify!($ty_name))
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        $tag_ty_name::from_str(value).ok_or_else(|| E::unknown_variant(value, $tag_ty_name::ALL_STR))
                    }
                }

                deserializer.deserialize_str(Visitor)
            }
        }
    };
}
