use api_utils_rs::wit::wasmcloud::postgres;
use api_utils_rs::wit::wasmcloud::postgres::types::PgValue;

use crate::interlude::*;

pub use daybook_types::types::doc::doc_create::*;
// pub use crate::gen::doc::wit::exports::townframe::daybook_api::doc_create::GuestHandler;
pub use crate::wit::exports::townframe::daybook_api::doc_create::GuestService;
use daybook_types::types::doc::Doc;

impl GuestService for DocCreate {
    fn new() -> Self {
        Self
    }
    fn serve(&self, inp: Input) -> Result<Doc, Error> {
        let cx = crate::cx();

        garde::Validate::validate(&inp).map_err(ErrorsValidation::from)?;

        // _cx.kanidm.idm_person_account_create()
        let StdDb::PgWasi {} = &cx.db else {
            panic!("unsupported db");
        };
        let rows = postgres::query::query(
            r#"
        SELECT
            id as "id!"
            ,created_at as "created_at!"
            ,updated_at as "updated_at!"
        FROM doc.create_doc($1)
            "#
            .into(),
            &[PgValue::Text(inp.id.clone())],
        )
        .map_err(|err| {
            use postgres::types::QueryError::*;
            match err {
                Unexpected(msg)
                    if msg.contains(
                        "duplicate key value violates unique constraint \"docs_pkey\"",
                    ) =>
                {
                    ErrorIdOccupied { id: inp.id }.into()
                }
                Unexpected(msg) | InvalidParams(msg) | InvalidQuery(msg) => {
                    internal_err!("db error: {msg}")
                }
            }
        })?;
        let mut rows = rows_to_objs(rows);
        let mut out = rows
            .into_iter()
            .next()
            .ok_or_else(|| internal_err!("bad response, no row found"))?;
        let out = Doc {
            id: out.swap_remove("id!").expect("bad response").to_text(),
            created_at: out
                .swap_remove("created_at!")
                .expect("bad response")
                .to_datetime(),
            updated_at: out
                .swap_remove("updated_at!")
                .expect("bad response")
                .to_datetime(),
        };
        Ok(out)
    }
}

fn rows_to_objs(rows: Vec<Vec<postgres::types::ResultRowEntry>>) -> Vec<IndexMap<String, PgValue>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|col| (col.column_name, col.value))
                .collect::<IndexMap<_, _>>()
        })
        .collect()
}

// #[async_trait]
// impl Endpoint for DocCreate {
//     type Input = Input;
//     type Response = Output;
//     type Error = Error;
//     type Cx = Context;
//
//     async fn handle(
//         &self,
//         cx: &Self::Cx,
//         request: Self::Input,
//     ) -> Result<Self::Response, Self::Error> {
//     }
// }

#[cfg(test)]
mod test {
    use super::Input;
    use crate::interlude::*;

    fn fixture_request() -> Input {
        serde_json::from_value(fixture_request_json()).unwrap()
    }

    fn fixture_request_json() -> serde_json::Value {
        serde_json::json!({
            "id": "123",
        })
    }

    utils_rs::table_tests! {
        validate,
        (request, err_path),
        {
            match garde::Validate::validate(&request) {
                Ok(()) => {
                    if let Some(err_path) = err_path {
                        panic!("validation succeeded, was expecting err on field: {err_path} {request:?}");
                    }
                }
                Err(err) => {
                    let err_path = err_path.expect("unexpected validation failure");
                    if let None = err.iter().find(|(path, err)| err_path == format!("{path}")) {
                        panic!("validation didn't fail on expected field: {err_path}, {err:?}");
                    }
                }
            }
        }
    }

    validate! {
        rejects_empty_ids: (
            Input {
                id: "".into(),
                ..fixture_request()
            },
            Some("id"),
        ),
    }
}
