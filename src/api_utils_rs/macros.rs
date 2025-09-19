// This will render a generic  error message if the `censor_internal_errors`
// flag is on
#[macro_export]
macro_rules! internal_err {
    {$msg:expr} =>{
        // panic!($msg)
        Error::Internal (
            #[cfg(not(feature = "censor_internal_errors"))]
            format!($msg).into(),
            #[cfg(feature = "censor_internal_errors")]
            format!("internal server error").into(),
        )
    }
}

#[macro_export]
macro_rules! list_request {
    ($sorting_field:ty) => {
        #[derive(
            Debug, serde::Serialize, serde::Deserialize, validator::Validate, utoipa::IntoParams,
        )]
        #[serde(crate = "serde", rename_all = "camelCase")]
        #[validate(schema(function = "validate_list_req"))]
        pub struct Request {
            #[serde(skip)]
            #[param(value_type = Option<String>)]
            pub auth_token: Option<$crate::BearerToken>,
            #[validate(range(min = 1, max = 100))]
            #[param(minimum = 1, maximum = 100)]
            pub limit: Option<usize>,
            pub after_cursor: Option<String>,
            pub before_cursor: Option<String>,
            pub filter: Option<String>,
            pub sorting_field: Option<$sorting_field>,
            #[param(value_type = Option<SortingOrder>)]
            pub sorting_order: Option<$crate::utils::SortingOrder>,
        }

        fn validate_list_req(req: &Request) -> Result<(), validator::ValidationError> {
            $crate::utils::validate_list_req(
                req.after_cursor.as_ref().map(|s| &s[..]),
                req.before_cursor.as_ref().map(|s| &s[..]),
                req.filter.as_ref().map(|s| &s[..]),
                req.sorting_field,
                req.sorting_order,
            )
        }
    };
}

#[macro_export]
macro_rules! list_response {
    ($item_ty:ty) => {
        #[derive(serde::Serialize, utoipa::ToSchema)]
        #[serde(crate = "serde", rename_all = "camelCase")]
        pub struct Response {
            pub cursor: Option<String>,
            pub items: Vec<$item_ty>,
        }
    };
}
/// TODO: DRY me up
/// This assumues utoipa is in scope
#[macro_export]
macro_rules! alias_and_ref {
    ($aliased_type:ty, $alias_name:ident, $ref_name:ident) => {
        pub type $alias_name = $aliased_type;
        #[derive(educe::Educe)]
        #[educe(Deref)]
        pub struct $ref_name($alias_name);
        impl From<$alias_name> for $ref_name {
            fn from(inner: $alias_name) -> Self {
                Self(inner)
            }
        }
        impl $crate::ToRefOrSchema for $ref_name {
            fn schema_name() -> &'static str {
                stringify!($alias_name)
            }

            fn ref_or_schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::schema::Ref::from_schema_name(Self::schema_name()).into()
            }
        }
    };
    ($aliased_type:ty, $alias_name:ident, $ref_name:ident, ser) => {
        pub type $alias_name = $aliased_type;
        #[derive(educe::Educe, serde::Serialize)]
        #[serde(crate = "serde")]
        #[educe(Deref)]
        pub struct $ref_name($alias_name);
        impl From<$alias_name> for $ref_name {
            fn from(inner: $alias_name) -> Self {
                Self(inner)
            }
        }
        impl $crate::ToRefOrSchema for $ref_name {
            fn schema_name() -> &'static str {
                stringify!($alias_name)
            }

            fn ref_or_schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::schema::Ref::from_schema_name(Self::schema_name()).into()
            }
        }
    };
    ($aliased_type:ty, $alias_name:ident, $ref_name:ident, de) => {
        pub type $alias_name = $aliased_type;
        #[derive(Debug, educe::Educe, serde::Deserialize)]
        #[serde(crate = "serde")]
        #[educe(Deref)]
        pub struct $ref_name($alias_name);
        impl From<$alias_name> for $ref_name {
            fn from(inner: $alias_name) -> Self {
                Self(inner)
            }
        }
        impl $crate::ToRefOrSchema for $ref_name {
            fn schema_name() -> &'static str {
                stringify!($alias_name)
            }

            fn ref_or_schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::schema::Ref::from_schema_name(Self::schema_name()).into()
            }
        }
    };
    ($aliased_type:ty, $alias_name:ident, $ref_name:ident, ser, de) => {
        pub type $alias_name = $aliased_type;
        #[derive(educe::Educe, serde::Serialize, serde::Deserialize)]
        #[serde(crate = "serde")]
        #[educe(Deref)]
        pub struct $ref_name($alias_name);
        impl From<$alias_name> for $ref_name {
            fn from(inner: $alias_name) -> Self {
                Self(inner)
            }
        }
        impl $crate::ToRefOrSchema for $ref_name {
            fn schema_name() -> &'static str {
                stringify!($alias_name)
            }

            fn ref_or_schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::schema::Ref::from_schema_name(Self::schema_name()).into()
            }
        }
    };
}

/* /// Implement [`From`] [`crate::auth::authorize::Error`] for the provided type
/// This expects the standard unit `AccessDenied` and the struct `Internal`
/// variant on the `Error` enum
#[macro_export]
macro_rules! impl_from_auth_err {
    ($errty:ident) => {
        impl From<$crate::auth::authorize::Error> for $errty {
            fn from(err: $crate::auth::authorize::Error) -> Self {
                use $crate::auth::authorize::Error;
                match err {
                    Error::Unauthorized | Error::InvalidToken => Self::AccessDenied,
                    Error::Internal { message } => Self::Internal { message },
                }
            }
        }
    };
} */
