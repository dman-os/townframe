use super::*;
use automerge::ObjId;

use autosurgeon::{HydrateError, ReadDoc, Reconciler};

pub mod json;
pub use json::ThroughJson;

pub mod date {
    use super::*;
    use crate::codecs::sane_iso8601::FORMAT;

    pub fn reconcile<R: autosurgeon::Reconciler>(
        ts: &OffsetDateTime,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        reconciler.timestamp(ts.unix_timestamp())
    }

    pub fn hydrate<'a, D: autosurgeon::ReadDoc>(
        doc: &D,
        obj: &ObjId,
        prop: autosurgeon::Prop<'a>,
    ) -> Result<OffsetDateTime, autosurgeon::HydrateError> {
        use automerge::{ScalarValue, Value};

        match doc.get(obj, &prop)? {
            Some((Value::Scalar(s), _)) => match s.as_ref() {
                ScalarValue::Timestamp(ts) => match OffsetDateTime::from_unix_timestamp(*ts) {
                    Ok(dt) => return Ok(dt),
                    Err(err) => {
                        return Err(autosurgeon::HydrateError::unexpected(
                            "a valid timestamp",
                            format!("error converting timestamp: {err}"),
                        ));
                    }
                },
                ScalarValue::Str(val) => OffsetDateTime::parse(&val, &FORMAT).map_err(|err| {
                    autosurgeon::HydrateError::unexpected(
                        "a valid ISO 8601 timestamp string",
                        format!("error parsing ISO 8601 timestamp '{val}': {err}"),
                    )
                }),
                _ => {
                    return Err(autosurgeon::HydrateError::unexpected(
                        "a string or timestamp",
                        format!("unexpected scalar type: {:?}", s),
                    ));
                }
            },
            _ => {
                return Err(autosurgeon::HydrateError::unexpected(
                    "a scalar value",
                    "value is not a scalar".to_string(),
                ));
            }
        }
    }
}

pub mod skip {
    use super::*;

    pub fn reconcile<T: Default, R: Reconciler>(
        _value: &T,
        _reconciler: R,
    ) -> Result<(), R::Error> {
        Ok(())
    }

    pub fn hydrate<'a, D: ReadDoc, T: Default>(
        _doc: &D,
        _obj: &ObjId,
        _prop: autosurgeon::Prop<'a>,
    ) -> Result<T, HydrateError> {
        Ok(T::default())
    }
}

pub mod through_str {
    use super::*;

    use std::str::FromStr;

    pub fn reconcile<T: AsRef<str>, R: Reconciler>(
        value: &T,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        reconciler.str(value)
    }

    pub fn hydrate<'a, D: ReadDoc, T: FromStr>(
        doc: &D,
        obj: &ObjId,
        prop: autosurgeon::Prop<'a>,
    ) -> Result<T, HydrateError> {
        use automerge::{ScalarValue, Value};
        let string = match doc.get(obj, &prop)? {
            Some((Value::Scalar(s), _)) => {
                match s.as_ref() {
                    // If stored as a string (new format), use it directly
                    ScalarValue::Str(s) => s.to_string(),
                    // If stored as a timestamp (old format), convert to ISO 8601
                    _ => {
                        return Err(autosurgeon::HydrateError::unexpected(
                            "a string",
                            format!("unexpected scalar type: {:?}", s),
                        ));
                    }
                }
            }
            _ => {
                return Err(autosurgeon::HydrateError::unexpected(
                    "a scalar value",
                    "value is not a scalar".to_string(),
                ));
            }
        };
        match string.parse() {
            Ok(val) => Ok(val),
            Err(_err) => Err(autosurgeon::HydrateError::unexpected(
                format!("a string repr of {}", std::any::type_name::<T>()),
                format!("failure parsing string"),
            )),
        }
    }
}

