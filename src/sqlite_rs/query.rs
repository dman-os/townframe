use crate::{ExecuteResult, FromValue, IntoValue, ResultSet, Row, SqlError, SqlResult, SqlValue};

pub trait IntoParams: Send {
    fn into_params(self) -> Vec<SqlValue>;
}

impl IntoParams for () {
    fn into_params(self) -> Vec<SqlValue> {
        Vec::new()
    }
}

impl IntoParams for &[SqlValue] {
    fn into_params(self) -> Vec<SqlValue> {
        self.to_vec()
    }
}

impl<const N: usize> IntoParams for &[SqlValue; N] {
    fn into_params(self) -> Vec<SqlValue> {
        self.as_slice().into_params()
    }
}

impl IntoParams for &Vec<SqlValue> {
    fn into_params(self) -> Vec<SqlValue> {
        self.as_slice().into_params()
    }
}

impl IntoParams for Vec<SqlValue> {
    fn into_params(self) -> Vec<SqlValue> {
        self
    }
}

macro_rules! tuple_params {
    ($($ty:ident:$index:tt),+) => {
        impl<$($ty: IntoValue + Send),+> IntoParams for ($($ty,)+) {
            fn into_params(self) -> Vec<SqlValue> {
                vec![$(self.$index.into_value(),)+]
            }
        }
    };
}

tuple_params!(A:0);
tuple_params!(A:0, B:1);
tuple_params!(A:0, B:1, C:2);
tuple_params!(A:0, B:1, C:2, D:3);
tuple_params!(A:0, B:1, C:2, D:3, E:4);
tuple_params!(A:0, B:1, C:2, D:3, E:4, F:5);
tuple_params!(A:0, B:1, C:2, D:3, E:4, F:5, G:6);
tuple_params!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7);

#[derive(Clone, Debug, Default)]
pub struct Sql {
    text: String,
    params: Vec<SqlValue>,
}

impl Sql {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            params: Vec::new(),
        }
    }

    pub fn push(&mut self, text: &str) -> &mut Self {
        self.text.push_str(text);
        self
    }

    pub fn bind(&mut self, value: impl IntoValue) -> &mut Self {
        self.text.push('?');
        self.params.push(value.into_value());
        self
    }

    pub fn ident(&mut self, name: &str) -> &mut Self {
        assert!(valid_identifier(name), "invalid SQL identifier: {name:?}");
        self.text.push('"');
        self.text.push_str(name);
        self.text.push('"');
        self
    }

    pub fn bind_list<T: IntoValue>(&mut self, values: impl IntoIterator<Item = T>) -> &mut Self {
        self.push("(");
        for (index, value) in values.into_iter().enumerate() {
            if index != 0 {
                self.push(", ");
            }
            self.bind(value);
        }
        self.push(")")
    }

    pub fn bind_tuples<T: BindTuple>(&mut self, rows: impl IntoIterator<Item = T>) -> &mut Self {
        self.push("(");
        for (index, row) in rows.into_iter().enumerate() {
            if index != 0 {
                self.push(", ");
            }
            row.bind_tuple(self);
        }
        self.push(")")
    }

    pub fn as_parts(&self) -> (&str, &[SqlValue]) {
        (&self.text, &self.params)
    }

    pub async fn query(&self, executor: &impl QueryExecutor) -> SqlResult<ResultSet> {
        executor.query(&self.text, &self.params).await
    }

    pub async fn execute(&self, executor: &impl Executor) -> SqlResult<ExecuteResult> {
        executor.execute(&self.text, &self.params).await
    }
}

pub(crate) fn valid_identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// A query destination shared by databases, write transactions, read snapshots and savepoints.
/// Implementations must preserve submission order for operations on the same transaction.
pub trait QueryExecutor: Sync {
    fn query(
        &self,
        sql: &str,
        params: impl IntoParams,
    ) -> impl Future<Output = SqlResult<ResultSet>> + Send;
}

/// A query destination that also permits writes. Read snapshots deliberately do not implement it.
pub trait Executor: QueryExecutor {
    fn execute(
        &self,
        sql: &str,
        params: impl IntoParams,
    ) -> impl Future<Output = SqlResult<ExecuteResult>> + Send;
}

/// Decode an owned result row into a domain type using named fields or positional tuple elements.
/// Return `SqlError::Decode` for incompatible values rather than substituting defaults.
pub trait FromRow: Sized {
    fn from_row(row: &Row) -> SqlResult<Self>;
}

impl FromRow for Row {
    fn from_row(row: &Row) -> SqlResult<Self> {
        Ok(row.clone())
    }
}

macro_rules! scalar_rows {
    ($($ty:ty),+ $(,)?) => {$(
        impl FromRow for $ty {
            fn from_row(row: &Row) -> SqlResult<Self> {
                row.require_width(1)?;
                row.get_at(0)
            }
        }
    )+};
}
scalar_rows!(i64, u64, f64, bool, String, Vec<u8>, SqlValue);

impl<T: FromValue> FromRow for Option<T> {
    fn from_row(row: &Row) -> SqlResult<Self> {
        row.require_width(1)?;
        row.get_at(0)
    }
}

/// A fixed-width tuple of bound values. Implemented for tuples of one through twelve elements.
pub trait BindTuple {
    fn bind_tuple(self, sql: &mut Sql);
}

macro_rules! tuples {
    ($($ty:ident:$index:tt),+) => {
        impl<$($ty: FromValue),+> FromRow for ($($ty,)+) {
            fn from_row(row: &Row) -> SqlResult<Self> {
                row.require_width([$(stringify!($ty)),+].len())?;
                Ok(($(row.get_at::<$ty>($index)?,)+))
            }
        }
        impl<$($ty: IntoValue),+> BindTuple for ($($ty,)+) {
            fn bind_tuple(self, sql: &mut Sql) {
                sql.push("(");
                $(if $index != 0 { sql.push(", "); } sql.bind(self.$index);)+
                sql.push(")");
            }
        }
    };
}
tuples!(A:0);
tuples!(A:0, B:1);
tuples!(A:0, B:1, C:2);
tuples!(A:0, B:1, C:2, D:3);
tuples!(A:0, B:1, C:2, D:3, E:4);
tuples!(A:0, B:1, C:2, D:3, E:4, F:5);
tuples!(A:0, B:1, C:2, D:3, E:4, F:5, G:6);
tuples!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7);
tuples!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8);
tuples!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9);
tuples!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10);
tuples!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11);

impl Row {
    pub fn require_width(&self, expected: usize) -> SqlResult<()> {
        if self.values.len() != expected {
            return Err(SqlError::Decode {
                column: "<row>".into(),
                message: format!("expected {expected} columns, got {}", self.values.len()),
            });
        }
        Ok(())
    }
}

impl ResultSet {
    pub fn one<T: FromRow>(self) -> SqlResult<T> {
        self.one_with(T::from_row)
    }

    pub fn optional<T: FromRow>(self) -> SqlResult<Option<T>> {
        self.optional_with(T::from_row)
    }

    pub fn all<T: FromRow>(self) -> SqlResult<Vec<T>> {
        self.all_with(T::from_row)
    }

    pub fn one_with<T>(self, decode: impl Fn(&Row) -> SqlResult<T>) -> SqlResult<T> {
        self.optional_with(decode)?.ok_or(SqlError::NoRows)
    }

    pub fn optional_with<T>(self, decode: impl Fn(&Row) -> SqlResult<T>) -> SqlResult<Option<T>> {
        if self.rows.len() > 1 {
            return Err(SqlError::TooManyRows);
        }
        self.rows.first().map(decode).transpose()
    }

    pub fn all_with<T>(self, decode: impl Fn(&Row) -> SqlResult<T>) -> SqlResult<Vec<T>> {
        self.rows.iter().map(decode).collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Json<T>(pub T);

impl<T: serde::Serialize> Json<T> {
    pub fn into_value(self) -> SqlResult<SqlValue> {
        serde_json::to_string(&self.0)
            .map(SqlValue::Text)
            .map_err(|error| SqlError::InvalidParams(error.to_string()))
    }
}

impl<T: serde::de::DeserializeOwned> FromValue for Json<T> {
    fn from_value(value: &SqlValue, column: &str) -> SqlResult<Self> {
        let SqlValue::Text(text) = value else {
            return Err(SqlError::Decode {
                column: column.into(),
                message: "expected JSON text".into(),
            });
        };
        serde_json::from_str(text)
            .map(Self)
            .map_err(|error| SqlError::Decode {
                column: column.into(),
                message: error.to_string(),
            })
    }
}

impl<T: serde::de::DeserializeOwned> FromRow for Json<T> {
    fn from_row(row: &Row) -> SqlResult<Self> {
        row.require_width(1)?;
        row.get_at(0)
    }
}
