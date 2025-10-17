use crate::interlude::*;


fn fixture_request_json() -> serde_json::Value {
    serde_json::json!({
        "id": "123",
    })
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
            crate::integration_table_tests! {
                $(
                    $name: {
                        app: "daybook",
                        path: "/doc",
                        method: POST,
                        status: $status,
                        cx_fn: crate::test_cx,
                        body: $json_body,
                        $(check_json: $check_json,)?
                        $(extra_assertions: $extra_fn,)?
                        print_response: true,
                    },
                )*
            }
        }
    };
}

integ! {
    works: {
        status: StatusCode::CREATED,
        body: fixture_request_json(),
        check_json: fixture_request_json().remove_keys_from_obj(&["password"]),
        extra_assertions: &|EAArgs { test_cx, body_json, http_client, .. }| {
            Box::pin(async move {
                return Ok(());
                let req_body_json = fixture_request_json();
                let body_json = body_json.unwrap();
                // // TODO: use super user token
                // let token = authenticate::Authenticate.handle(&cx,authenticate::Input{
                //     identifier: req_body_json["username"].as_str().unwrap().into(),
                //     password: req_body_json["password"].as_str().unwrap().into()
                // }).await.unwrap_or_log().token;

                let resp = http_client
                    .get(
                        format!("/users/{}", body_json["id"].as_str().unwrap())
                    )
                    .send()
                    .await?;
                assert_eq!(resp.status(), StatusCode::OK);
                let body = resp.json().await?;
                assert_eq_json(
                    ("expected", &req_body_json.remove_keys_from_obj(&["password"])),
                    ("response", &body),
                );
                Ok(())
            })
        },
    },
    fails_if_id_occupied: {
        status: StatusCode::BAD_REQUEST,
        body: fixture_request_json().destructure_into_self(
            serde_json::json!({ "id": DOC_01_ID })
        ),
        check_json: serde_json::json!({
            "IdOccupied": {
                "id": DOC_01_ID
            }
        }),
    },
}
