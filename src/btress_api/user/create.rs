use api_utils_rs::wit::wasmcloud::postgres;
use api_utils_rs::wit::wasmcloud::postgres::types::PgValue;

use crate::interlude::*;

pub use crate::gen::user::user_create::*;
// pub use crate::gen::user::wit::exports::townframe::btress_api::user_create::GuestHandler;
use crate::gen::user::User;
pub use crate::wit::exports::townframe::btress_api::user_create::GuestService;

impl GuestService for UserCreate {
    #[allow(async_fn_in_trait)]
    fn serve(&self, inp: Input) -> Result<User, Error> {
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
            ,email::TEXT as "email?"
            ,username::TEXT as "username!"
        FROM auth.create_user($1, $2)
            "#
            .into(),
            &[
                PgValue::Text(inp.username.clone()),
                inp.email
                    .clone()
                    .map(PgValue::Text)
                    .unwrap_or_else(|| PgValue::Null),
            ],
        )
        .map_err(|err| {
            use postgres::types::QueryError::*;
            match err {
                Unexpected(msg) if msg.contains("users_username_key") => ErrorUsernameOccupied {
                    username: inp.username,
                }
                .into(),
                Unexpected(msg) if msg.contains("users_email_key") => {
                    ErrorEmailOccupied { email: inp.email }.into()
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
        let out = User {
            id: out.swap_remove("id!").expect("bad response").to_text(),
            created_at: out
                .swap_remove("created_at!")
                .expect("bad response")
                .to_datetime(),
            updated_at: out
                .swap_remove("updated_at!")
                .expect("bad response")
                .to_datetime(),
            email: Some(out.swap_remove("email?").expect("bad response").to_text()),
            username: out
                .swap_remove("username!")
                .expect("bad response")
                .to_text(),
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
// impl Endpoint for UserCreate {
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

    use crate::user::testing::*;

    fn fixture_request() -> Input {
        serde_json::from_value(fixture_request_json()).unwrap()
    }

    fn fixture_request_json() -> serde_json::Value {
        serde_json::json!({
            "username": "whish_box12",
            "email": "multis@cream.mux",
            "password": "lovebite",
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
        rejects_too_short_usernames: (
            Input {
                username: "st".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_too_long_usernames: (
            Input {
                username: "doo-doo-do-doo-dooooo-do-do-dooooood".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_usernames_that_ends_with_dashes: (
            Input {
                username: "wrenz-".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_usernames_that_start_with_dashes: (
            Input {
                username: "-wrenz".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_usernames_that_ends_with_underscore: (
            Input {
                username: "belle_".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_usernames_that_start_with_underscore: (
            Input {
                username: "_belle".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_usernames_with_white_space: (
            Input {
                username: "daddy yo".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_too_short_passwords: (
            Input {
                password: "short".into(),
                ..fixture_request()
            },
            Some("password"),
        ),
        rejects_invalid_emails: (
            Input {
                email: Some("invalid".into()),
                ..fixture_request()
            },
            Some("email"),
        ),
    }

    // macro_rules! integ {
    //     ($(
    //         $name:ident: {
    //             status: $status:expr,
    //             body: $json_body:expr,
    //             $(check_json: $check_json:expr,)?
    //             $(extra_assertions: $extra_fn:expr,)?
    //         },
    //     )*) => {
    //         mod integ {
    //             use super::*;
    //             api_utils_rs::integration_table_tests! {
    //                 $(
    //                     $name: {
    //                         uri: "/users",
    //                         method: "POST",
    //                         status: $status,
    //                         router: crate::user::router(),
    //                         cx_fn: crate::utils::testing::cx_fn,
    //                         body: $json_body,
    //                         $(check_json: $check_json,)?
    //                         $(extra_assertions: $extra_fn,)?
    //                     },
    //                 )*
    //             }
    //         }
    //     };
    // }
    //
    // integ! {
    //     works: {
    //         status: http::StatusCode::CREATED,
    //         body: fixture_request_json(),
    //         check_json: fixture_request_json().remove_keys_from_obj(&["password"]),
    //         extra_assertions: &|EAArgs { test_cx, response_json, .. }| {
    //             Box::pin(async move {
    //                 let cx = state_fn(test_cx).await.expect_or_log("error making state");
    //                 let req_body_json = fixture_request_json();
    //                 let resp_body_json = response_json.unwrap();
    //                 // // TODO: use super user token
    //                 // let token = authenticate::Authenticate.handle(&cx,authenticate::Input{
    //                 //     identifier: req_body_json["username"].as_str().unwrap().into(),
    //                 //     password: req_body_json["password"].as_str().unwrap().into()
    //                 // }).await.unwrap_or_log().token;
    //
    //                 let app = crate::user::router().with_state(cx);
    //                 let resp = app
    //                     .oneshot(
    //                         http::::builder()
    //                             .method("GET")
    //                             .uri(format!("/users/{}", resp_body_json["id"].as_str().unwrap()))
    //                             // .header(
    //                             //     http::header::AUTHORIZATION,
    //                             //     format!("Bearer {token}"),
    //                             // )
    //                             .body(axum::body::Body::empty())
    //                             .unwrap_or_log(),
    //                     )
    //                     .await
    //                     .unwrap_or_log();
    //                 assert_eq!(resp.status(), http::StatusCode::OK);
    //                 let body = resp.into_body();
    //                 let body = axum::body::to_bytes(body, 1024 * 1024 * 1024).await.unwrap_or_log();
    //                 let body = serde_json::from_slice(&body).unwrap_or_log();
    //                 check_json(
    //                     ("expected", &req_body_json.remove_keys_from_obj(&["password"])),
    //                     ("response", &body),
    //                 );
    //             })
    //         },
    //     },
    //     email_is_optional: {
    //         status: http::StatusCode::CREATED,
    //         body: fixture_request_json().remove_keys_from_obj(&["email"]),
    //         check_json: fixture_request_json().remove_keys_from_obj(&["password", "email"]),
    //     },
    //     fails_if_username_occupied: {
    //         status: http::StatusCode::BAD_REQUEST,
    //         body: fixture_request_json().destructure_into_self(
    //             serde_json::json!({ "username": USER_01.username })
    //         ),
    //         check_json: serde_json::json!({
    //             "error": "usernameOccupied"
    //         }),
    //     },
    //     /*
    //     // FIXME:
    //     fails_if_email_occupied: {
    //         status: http::StatusCode::BAD_REQUEST,
    //         body: fixture_request_json().destructure_into_self(
    //             serde_json::json!({ "email": USER_01_EMAIL })
    //         ),
    //         check_json: serde_json::json!({
    //             "error": "emailOccupied"
    //         }),
    //     },*/
    // }
}
