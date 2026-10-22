use std::sync::Arc;
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub trait IntoValue {
    fn into_value(self) -> SqlValue;
}
impl IntoValue for SqlValue {
    fn into_value(self) -> SqlValue {
        self
    }
}
impl IntoValue for bool {
    fn into_value(self) -> SqlValue {
        SqlValue::Integer(i64::from(self))
    }
}
impl IntoValue for &String {
    fn into_value(self) -> SqlValue {
        SqlValue::Text(self.clone())
    }
}
impl IntoValue for i64 {
    fn into_value(self) -> SqlValue {
        SqlValue::Integer(self)
    }
}
impl IntoValue for i32 {
    fn into_value(self) -> SqlValue {
        SqlValue::Integer(i64::from(self))
    }
}
impl IntoValue for f64 {
    fn into_value(self) -> SqlValue {
        SqlValue::Real(self)
    }
}
impl IntoValue for String {
    fn into_value(self) -> SqlValue {
        SqlValue::Text(self)
    }
}
impl IntoValue for &str {
    fn into_value(self) -> SqlValue {
        SqlValue::Text(self.to_string())
    }
}
impl IntoValue for Vec<u8> {
    fn into_value(self) -> SqlValue {
        SqlValue::Blob(self)
    }
}
impl IntoValue for &[u8] {
    fn into_value(self) -> SqlValue {
        SqlValue::Blob(self.to_vec())
    }
}
impl<T: IntoValue> IntoValue for Option<T> {
    fn into_value(self) -> SqlValue {
        match self {
            Some(value) => value.into_value(),
            None => SqlValue::Null,
        }
    }
}

/// Decode a value out of a row. This is the whole row-mapping surface:
/// codegen emits calls to `row.get::<T>("column")`.
pub trait FromValue: Sized {
    fn from_value(value: &SqlValue, column: &str) -> SqlResult<Self>;
}

impl FromValue for SqlValue {
    fn from_value(value: &SqlValue, _: &str) -> SqlResult<Self> {
        Ok(value.clone())
    }
}

macro_rules! from_value {
    ($t:ty, $pat:pat => $out:expr) => {
        impl FromValue for $t {
            fn from_value(value: &SqlValue, column: &str) -> SqlResult<Self> {
                match value {
                    $pat => Ok($out),
                    other => Err(SqlError::Decode {
                        column: column.to_string(),
                        message: format!("expected {}, got {other:?}", stringify!($t)),
                    }),
                }
            }
        }
    };
}
from_value!(i64, SqlValue::Integer(value) => *value);
from_value!(String, SqlValue::Text(value) => value.clone());
from_value!(Vec<u8>, SqlValue::Blob(value) => value.clone());
from_value!(bool, SqlValue::Integer(value) => *value != 0);
impl FromValue for f64 {
    fn from_value(value: &SqlValue, column: &str) -> SqlResult<Self> {
        match value {
            SqlValue::Real(value) => Ok(*value),
            // SQLite is dynamically typed: an integral REAL may come back
            // as INTEGER storage.
            SqlValue::Integer(value) => Ok(*value as f64),
            other => Err(SqlError::Decode {
                column: column.to_string(),
                message: format!("expected f64, got {other:?}"),
            }),
        }
    }
}
impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &SqlValue, column: &str) -> SqlResult<Self> {
        match value {
            SqlValue::Null => Ok(None),
            value => T::from_value(value, column).map(Some),
        }
    }
}
impl FromValue for u64 {
    fn from_value(value: &SqlValue, column: &str) -> SqlResult<Self> {
        let integer = i64::from_value(value, column)?;
        u64::try_from(integer).map_err(|_| SqlError::Decode {
            column: column.to_string(),
            message: format!("negative integer {integer} does not fit u64"),
        })
    }
}

/// Column names once, positional values per row. (Carrying the column
/// name on every row entry is the WIT v1 mistake not repeated here.)
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResultSet {
    pub columns: Arc<[String]>,
    pub rows: Vec<Row>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub(crate) columns: Arc<[String]>,
    pub(crate) values: Vec<SqlValue>,
}

impl Row {
    pub fn from_values(columns: Arc<[String]>, values: Vec<SqlValue>) -> Self {
        Self { columns, values }
    }
    pub fn into_values(self) -> Vec<SqlValue> {
        self.values
    }
    pub fn get<T: FromValue>(&self, column: &str) -> SqlResult<T> {
        let idx = self
            .columns
            .iter()
            .position(|name| name == column)
            .ok_or_else(|| SqlError::Decode {
                column: column.to_string(),
                message: "column not present in result".into(),
            })?;
        T::from_value(&self.values[idx], column)
    }

    pub fn value_at(&self, index: usize) -> Option<&SqlValue> {
        self.values.get(index)
    }
    pub fn get_at<T: FromValue>(&self, index: usize) -> SqlResult<T> {
        let value = self.values.get(index).ok_or_else(|| SqlError::Decode {
            column: format!("#{index}"),
            message: "column index out of range".into(),
        })?;
        T::from_value(value, self.columns.get(index).map_or("#?", String::as_str))
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExecuteResult {
    pub rows_affected: u64,
    pub last_insert_rowid: i64,
    pub result_set: ResultSet,
}

/// Mirrors `townframe:sqlite`'s `query-error` so the host maps ~1:1.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("decode error at column '{column}': {message}")]
    Decode { column: String, message: String },
    #[error("query returned no rows")]
    NoRows,
    #[error("query returned more than one row")]
    TooManyRows,
    #[error("sqlite runtime closed")]
    Closed,
    #[error("{0}")]
    Unexpected(String),
}

pub type SqlResult<T> = Result<T, SqlError>;
