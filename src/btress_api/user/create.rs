use crate::interlude::*;

#[derive(Debug, Clone)]
pub struct CreateUser;

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema, garde::Validate)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    #[schema(min_length = 3, max_length = 25, pattern = "super::USERNAME_REGEX")]
    #[garde(ascii, length(min = 3, max = 25), pattern(super::USERNAME_REGEX))]
    pub username: String,
    /// Must be a valid email string
    #[garde(email)]
    pub email: Option<String>,
    #[schema(min_length = 8, max_length = 1024)]
    #[garde(length(min = 8, max = 1024))]
    pub password: String,
}

pub type Response = SchemaRef<super::User>;

#[derive(
    Debug, Serialize, thiserror::Error, displaydoc::Display, macros::HttpError, utoipa::ToSchema,
)]
#[serde(rename_all = "camelCase", tag = "error")]
pub enum Error {
    /// username occupied {username:?}
    #[http(code(StatusCode::BAD_REQUEST), desc("Username occupied"))]
    UsernameOccupied { username: String },
    /// email occupied {email:?}
    #[http(code(StatusCode::BAD_REQUEST), desc("Email occupied"))]
    EmailOccupied { email: String },
    /// invalid input: {issues:?}
    #[http(code(StatusCode::BAD_REQUEST), desc("Invalid input"))]
    InvalidInput {
        #[from]
        issues: ValidationErrors,
    },
    /// internal server error: {message}
    #[http(code(StatusCode::INTERNAL_SERVER_ERROR), desc("Internal server error"))]
    Internal { message: String },
}

#[async_trait]
impl Endpoint for CreateUser {
    type Request = Request;
    type Response = Response;
    type Error = Error;
    type Cx = Context;

    async fn handle(
        &self,
        cx: &Self::Cx,
        request: Self::Request,
    ) -> Result<Self::Response, Self::Error> {
        garde::Validate::validate(&request).map_err(ValidationErrors::from)?;

        let pass_hash = {
            use argon2::PasswordHasher;
            let salt_hash = cx.config.pass_salt_hash.clone();
            let argon2 = cx.argon2.clone();
            tokio::task::spawn_blocking(move || {
                argon2
                    .hash_password(request.password.as_bytes(), salt_hash.as_ref())
                    .expect_or_log("argon2 err")
                    .serialize()
            })
            .await
            .expect_or_log("tokio err")
        };

        // _cx.kanidm.idm_person_account_create()
        let StdDb::Pg { db_pool } = &cx.db else {
            panic!("unsupported db");
        };
        let out = sqlx::query_as!(
            super::User,
            r#"
SELECT 
    id as "id!"
    ,created_at as "created_at!"
    ,updated_at as "updated_at!"
    ,email::TEXT as "email?"
    ,username::TEXT as "username!"
FROM auth.create_user($1, $2, $3)
        "#,
            &request.username,
            request.email.as_ref(),
            pass_hash.as_ref()
        )
        .fetch_one(db_pool)
        .await
        .map_err(|err| match &err {
            sqlx::Error::Database(boxed) if boxed.constraint().is_some() => {
                match boxed.constraint().unwrap() {
                    "users_username_key" => Error::UsernameOccupied {
                        username: request.username,
                    },
                    "users_email_key" => Error::EmailOccupied {
                        email: request.email.unwrap(),
                    },
                    _ => internal_err!("db error: {err}"),
                }
            }
            _ => internal_err!("db error: {err}"),
        })?;
        Ok(out.into())
    }
}

impl HttpEndpoint for CreateUser {
    const SUCCESS_CODE: StatusCode = StatusCode::CREATED;
    const METHOD: Method = Method::Post;
    const PATH: &'static str = "/users";

    type SharedCx = SharedContext;
    type HttpRequest = (Json<Request>,);

    fn request((Json(req),): Self::HttpRequest) -> Result<Self::Request, Self::Error> {
        Ok(req)
    }

    fn response(resp: Self::Response) -> HttpResponse {
        Json(resp).into_response()
    }
}

impl DocumentedEndpoint for CreateUser {
    const TAG: &'static Tag = &super::TAG;
}

// #[cfg(test)]
mod test {
    use super::Request;
    use crate::interlude::*;

    use crate::user::testing::*;

    fn fixture_request() -> Request {
        serde_json::from_value(fixture_request_json()).unwrap()
    }

    fn fixture_request_json() -> serde_json::Value {
        serde_json::json!({
            "username": "whish_box12",
            "email": "multis@cream.mux",
            "password": "lovebite",
        })
    }

    api_utils_rs::table_tests! {
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
            Request {
                username: "st".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_too_long_usernames: (
            Request {
                username: "doo-doo-do-doo-dooooo-do-do-dooooood".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_usernames_that_ends_with_dashes: (
            Request {
                username: "wrenz-".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_usernames_that_start_with_dashes: (
            Request {
                username: "-wrenz".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_usernames_that_ends_with_underscore: (
            Request {
                username: "belle_".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_usernames_that_start_with_underscore: (
            Request {
                username: "_belle".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_usernames_with_white_space: (
            Request {
                username: "daddy yo".into(),
                ..fixture_request()
            },
            Some("username"),
        ),
        rejects_too_short_passwords: (
            Request {
                password: "short".into(),
                ..fixture_request()
            },
            Some("password"),
        ),
        rejects_invalid_emails: (
            Request {
                email: Some("invalid".into()),
                ..fixture_request()
            },
            Some("email"),
        ),
    }

    macro_rules! integ {
        ($(
            $name:ident: {
                status: $status:expr,
                body: $json_body:expr,
                $(check_json: $check_json:expr,)?
                $(extra_assertions: $extra_fn:expr,)?
            },
        )*) => {
            mod integ {
                use super::*;
                api_utils_rs::integration_table_tests! {
                    $(
                        $name: {
                            uri: "/users",
                            method: "POST",
                            status: $status,
                            router: crate::user::router(),
                            cx_fn: crate::utils::testing::cx_fn,
                            body: $json_body,
                            $(check_json: $check_json,)?
                            $(extra_assertions: $extra_fn,)?
                        },
                    )*
                }
            }
        };
    }

    integ! {
        works: {
            status: http::StatusCode::CREATED,
            body: fixture_request_json(),
            check_json: fixture_request_json().remove_keys_from_obj(&["password"]),
            extra_assertions: &|EAArgs { test_cx, response_json, .. }| {
                Box::pin(async move {
                    let cx = state_fn(test_cx).await.expect_or_log("error making state");
                    let req_body_json = fixture_request_json();
                    let resp_body_json = response_json.unwrap();
                    // // TODO: use super user token
                    // let token = authenticate::Authenticate.handle(&cx,authenticate::Request{
                    //     identifier: req_body_json["username"].as_str().unwrap().into(),
                    //     password: req_body_json["password"].as_str().unwrap().into()
                    // }).await.unwrap_or_log().token;

                    let app = crate::user::router().with_state(cx);
                    let resp = app
                        .oneshot(
                            http::Request::builder()
                                .method("GET")
                                .uri(format!("/users/{}", resp_body_json["id"].as_str().unwrap()))
                                // .header(
                                //     http::header::AUTHORIZATION,
                                //     format!("Bearer {token}"),
                                // )
                                .body(axum::body::Body::empty())
                                .unwrap_or_log(),
                        )
                        .await
                        .unwrap_or_log();
                    assert_eq!(resp.status(), http::StatusCode::OK);
                    let body = resp.into_body();
                    let body = axum::body::to_bytes(body, 1024 * 1024 * 1024).await.unwrap_or_log();
                    let body = serde_json::from_slice(&body).unwrap_or_log();
                    check_json(
                        ("expected", &req_body_json.remove_keys_from_obj(&["password"])),
                        ("response", &body),
                    );
                })
            },
        },
        email_is_optional: {
            status: http::StatusCode::CREATED,
            body: fixture_request_json().remove_keys_from_obj(&["email"]),
            check_json: fixture_request_json().remove_keys_from_obj(&["password", "email"]),
        },
        fails_if_username_occupied: {
            status: http::StatusCode::BAD_REQUEST,
            body: fixture_request_json().destructure_into_self(
                serde_json::json!({ "username": USER_01.username })
            ),
            check_json: serde_json::json!({
                "error": "usernameOccupied"
            }),
        },
        /*
        // FIXME:
        fails_if_email_occupied: {
            status: http::StatusCode::BAD_REQUEST,
            body: fixture_request_json().destructure_into_self(
                serde_json::json!({ "email": USER_01_EMAIL })
            ),
            check_json: serde_json::json!({
                "error": "emailOccupied"
            }),
        },*/
    }
}
