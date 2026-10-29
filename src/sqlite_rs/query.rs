use crate::ExecuteResult;
use crate::FromValue;
use crate::IntoValue;
use crate::ResultSet;
use crate::Row;
use crate::SqlError;
use crate::SqlResult;
use crate::SqlValue;

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

/// Async backend used by component guests. Native and WASM callers share the
/// same parameter conversion, result decoding, and cardinality APIs through
/// `BackendRef`; only the transport is backend-specific.
pub trait SqliteBackend: Sync {
    fn query(
        &self,
        sql: String,
        params: Vec<SqlValue>,
    ) -> impl std::future::Future<Output = SqlResult<ResultSet>> + Send;

    fn execute(
        &self,
        sql: String,
        params: Vec<SqlValue>,
    ) -> impl std::future::Future<Output = SqlResult<ExecuteResult>> + Send;

    fn execute_batch(
        &self,
        sql: String,
    ) -> impl std::future::Future<Output = SqlResult<ExecuteResult>> + Send;
}

#[derive(Clone, Copy)]
pub struct BackendRef<'a, B: ?Sized>(&'a B);

impl<'a, B: ?Sized> BackendRef<'a, B> {
    pub fn new(backend: &'a B) -> Self {
        Self(backend)
    }

    pub async fn query(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams,
    ) -> SqlResult<ResultSet>
    where
        B: SqliteBackend,
    {
        self.0.query(sql.into(), params.into_params()).await
    }

    pub async fn execute(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams,
    ) -> SqlResult<ExecuteResult>
    where
        B: SqliteBackend,
    {
        self.0.execute(sql.into(), params.into_params()).await
    }

    pub async fn execute_batch(&self, sql: impl Into<String>) -> SqlResult<ExecuteResult>
    where
        B: SqliteBackend,
    {
        self.0.execute_batch(sql.into()).await
    }
}

impl<B: SqliteBackend + ?Sized> QueryExecutor for BackendRef<'_, B> {
    fn query(
        &self,
        sql: &str,
        params: impl IntoParams,
    ) -> impl std::future::Future<Output = SqlResult<ResultSet>> + Send {
        self.0.query(sql.to_owned(), params.into_params())
    }
}

impl<B: SqliteBackend + ?Sized> Executor for BackendRef<'_, B> {
    fn execute(
        &self,
        sql: &str,
        params: impl IntoParams,
    ) -> impl std::future::Future<Output = SqlResult<ExecuteResult>> + Send {
        self.0.execute(sql.to_owned(), params.into_params())
    }
}

/// Implement `SqliteBackend` for the generated `townframe:sqlite` resources.
///
/// The guest supplies its generated `Connection` and `Transaction` types and
/// their generated `types` module. Keeping the conversion here prevents each
/// guest from growing a second SQL value/result model.
#[macro_export]
macro_rules! impl_wit_sqlite_backend {
    ($resource:ty, $types:ident $(,)?) => {
        impl $crate::SqliteBackend for $resource {
            fn query(
                &self,
                sql: String,
                params: Vec<$crate::SqlValue>,
            ) -> impl std::future::Future<Output = $crate::SqlResult<$crate::ResultSet>> + Send
            {
                async move {
                    let params = params
                        .into_iter()
                        .map(|value| match value {
                            $crate::SqlValue::Null => $types::SqlValue::Null,
                            $crate::SqlValue::Integer(value) => $types::SqlValue::Integer(value),
                            $crate::SqlValue::Real(value) => $types::SqlValue::Real(value),
                            $crate::SqlValue::Text(value) => $types::SqlValue::Text(value),
                            $crate::SqlValue::Blob(value) => $types::SqlValue::Blob(value),
                        })
                        .collect();
                    let result = <$resource>::query(self, sql, params).await;
                    result
                        .map(|set| {
                            let columns: std::sync::Arc<[String]> = set.columns.into();
                            let rows = set
                                .rows
                                .into_iter()
                                .map(|row| {
                                    $crate::Row::from_values(
                                        columns.clone(),
                                        row.into_iter()
                                            .map(|value| match value {
                                                $types::SqlValue::Null => $crate::SqlValue::Null,
                                                $types::SqlValue::Integer(value) => {
                                                    $crate::SqlValue::Integer(value)
                                                }
                                                $types::SqlValue::Real(value) => {
                                                    $crate::SqlValue::Real(value)
                                                }
                                                $types::SqlValue::Text(value) => {
                                                    $crate::SqlValue::Text(value)
                                                }
                                                $types::SqlValue::Blob(value) => {
                                                    $crate::SqlValue::Blob(value)
                                                }
                                            })
                                            .collect(),
                                    )
                                })
                                .collect();
                            $crate::ResultSet { columns, rows }
                        })
                        .map_err(|error| match error {
                            $types::QueryError::InvalidParams(message) => {
                                $crate::SqlError::InvalidParams(message)
                            }
                            $types::QueryError::InvalidQuery(message) => {
                                $crate::SqlError::InvalidQuery(message)
                            }
                            $types::QueryError::Unexpected(message) => {
                                $crate::SqlError::Unexpected(message)
                            }
                        })
                }
            }

            fn execute(
                &self,
                sql: String,
                params: Vec<$crate::SqlValue>,
            ) -> impl std::future::Future<Output = $crate::SqlResult<$crate::ExecuteResult>> + Send
            {
                async move {
                    let params = params
                        .into_iter()
                        .map(|value| match value {
                            $crate::SqlValue::Null => $types::SqlValue::Null,
                            $crate::SqlValue::Integer(value) => $types::SqlValue::Integer(value),
                            $crate::SqlValue::Real(value) => $types::SqlValue::Real(value),
                            $crate::SqlValue::Text(value) => $types::SqlValue::Text(value),
                            $crate::SqlValue::Blob(value) => $types::SqlValue::Blob(value),
                        })
                        .collect();
                    let result = <$resource>::execute(self, sql, params).await;
                    result
                        .map(|result| {
                            let columns: std::sync::Arc<[String]> =
                                result.result_set.columns.into();
                            let rows = result
                                .result_set
                                .rows
                                .into_iter()
                                .map(|row| {
                                    $crate::Row::from_values(
                                        columns.clone(),
                                        row.into_iter()
                                            .map(|value| match value {
                                                $types::SqlValue::Null => $crate::SqlValue::Null,
                                                $types::SqlValue::Integer(value) => {
                                                    $crate::SqlValue::Integer(value)
                                                }
                                                $types::SqlValue::Real(value) => {
                                                    $crate::SqlValue::Real(value)
                                                }
                                                $types::SqlValue::Text(value) => {
                                                    $crate::SqlValue::Text(value)
                                                }
                                                $types::SqlValue::Blob(value) => {
                                                    $crate::SqlValue::Blob(value)
                                                }
                                            })
                                            .collect(),
                                    )
                                })
                                .collect();
                            $crate::ExecuteResult {
                                rows_affected: result.rows_affected,
                                last_insert_rowid: result.last_insert_rowid,
                                result_set: $crate::ResultSet { columns, rows },
                            }
                        })
                        .map_err(|error| match error {
                            $types::QueryError::InvalidParams(message) => {
                                $crate::SqlError::InvalidParams(message)
                            }
                            $types::QueryError::InvalidQuery(message) => {
                                $crate::SqlError::InvalidQuery(message)
                            }
                            $types::QueryError::Unexpected(message) => {
                                $crate::SqlError::Unexpected(message)
                            }
                        })
                }
            }

            fn execute_batch(
                &self,
                sql: String,
            ) -> impl std::future::Future<Output = $crate::SqlResult<$crate::ExecuteResult>> + Send
            {
                async move {
                    <$resource>::execute_batch(self, sql)
                        .await
                        .map(|result| $crate::ExecuteResult {
                            rows_affected: result.rows_affected,
                            last_insert_rowid: result.last_insert_rowid,
                            result_set: $crate::ResultSet::default(),
                        })
                        .map_err(|error| match error {
                            $types::QueryError::InvalidParams(message) => {
                                $crate::SqlError::InvalidParams(message)
                            }
                            $types::QueryError::InvalidQuery(message) => {
                                $crate::SqlError::InvalidQuery(message)
                            }
                            $types::QueryError::Unexpected(message) => {
                                $crate::SqlError::Unexpected(message)
                            }
                        })
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct FakeBackend;

    impl SqliteBackend for FakeBackend {
        fn query(
            &self,
            _sql: String,
            _params: Vec<SqlValue>,
        ) -> impl std::future::Future<Output = SqlResult<ResultSet>> + Send {
            std::future::ready(Ok(ResultSet {
                columns: Arc::from([String::from("value")]),
                rows: vec![Row::from_values(
                    Arc::from([String::from("value")]),
                    vec![SqlValue::Integer(7)],
                )],
            }))
        }

        fn execute(
            &self,
            _sql: String,
            _params: Vec<SqlValue>,
        ) -> impl std::future::Future<Output = SqlResult<ExecuteResult>> + Send {
            std::future::ready(Err(SqlError::InvalidQuery("fake failure".into())))
        }

        fn execute_batch(
            &self,
            _sql: String,
        ) -> impl std::future::Future<Output = SqlResult<ExecuteResult>> + Send {
            std::future::ready(Ok(ExecuteResult {
                rows_affected: 3,
                ..ExecuteResult::default()
            }))
        }
    }

    #[tokio::test]
    async fn backend_ref_uses_native_query_surface_and_cardinality() {
        let backend = FakeBackend;
        let sqlite = BackendRef::new(&backend);
        let set = sqlite
            .query("SELECT ?1", vec![7_i64.into_value()])
            .await
            .unwrap();
        assert_eq!(set.one::<i64>().unwrap(), 7);
        assert_eq!(
            sqlite
                .execute_batch("CREATE TABLE marker (id INTEGER)")
                .await
                .unwrap()
                .rows_affected,
            3
        );
        assert_eq!(
            sqlite.execute("INSERT", ()).await.unwrap_err().to_string(),
            "invalid query: fake failure"
        );
    }
}
