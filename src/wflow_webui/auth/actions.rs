use crate::interlude::*;

#[derive(Serialize, Deserialize)]
struct Oauth2GrantState {
    verifier: String,
}

#[cfg(feature = "ssr")]
pub async fn build_oauth_authorize_url(
    cx: &SharedServerCtx,
    session: &crate::server::session::Session,
    origin: Option<String>,
) -> Res<String> {
    let (code, code_verifer) = oauth2::PkceCodeChallenge::new_random_sha256();

    let base = &cx.config.kanidm_url;
    let client_id = &cx.config.kanidm_client_id;
    let redirect_uri = format!("{}/auth/redirect", cx.config.self_base_url);
    let redirect_uri = percent_encoding::percent_encode(
        redirect_uri.as_bytes(),
        percent_encoding::NON_ALPHANUMERIC,
    )
    .to_string();
    let code_challenge = code.as_str();
    let state_raw = origin.unwrap_or_else(|| "/".to_string());
    let state =
        percent_encoding::percent_encode(state_raw.as_bytes(), percent_encoding::NON_ALPHANUMERIC)
            .to_string();
    let scope = percent_encoding::percent_encode(
        "openid profile email".as_bytes(),
        percent_encoding::NON_ALPHANUMERIC,
    )
    .to_string();
    let url = format!(
        "{base}/ui/oauth2\
            ?redirect_uri={redirect_uri}\
            &client_id={client_id}\
            &response_type=code\
            &code_challenge={code_challenge}\
            &code_challenge_method=S256\
            &state={state}\
            &scope={scope}"
    );
    let _ = url.parse::<Uri>().expect_or_log("bad uri construction");

    session
        .kv_set(
            "oauth2:code_verifier",
            serde_json::to_string(&Oauth2GrantState {
                verifier: code_verifer.into_secret(),
            })
            .expect_or_log("json error"),
        )
        .await?;

    Ok(url)
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Oauth2TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    token_type: String,
    expires_in: Option<u64>,
    scope: Option<String>,
}

#[cfg(feature = "ssr")]
pub async fn exchange_code_for_tokens_core(
    cx: &SharedServerCtx,
    session: &crate::server::session::Session,
    cookie_jar: &crate::server::session::CookieJar,
    code: &str,
) -> Res<Oauth2TokenResponse> {
    // Load PKCE verifier
    let verifier = session.kv_get("oauth2:code_verifier").await?;
    let verifier: Option<Oauth2GrantState> = verifier.and_then(|s| serde_json::from_str(&s).ok());
    let Some(verifier) = verifier else {
        return Err(ferr!("missing code verifier"));
    };

    let token_endpoint = format!("{}/oauth2/token", cx.config.kanidm_url);
    let params = [
        ("grant_type", "authorization_code"),
        ("client_id", cx.config.kanidm_client_id.as_str()),
        ("code", code),
        (
            "redirect_uri",
            &format!("{}/auth/redirect", cx.config.self_base_url),
        ),
        ("code_verifier", verifier.verifier.as_str()),
    ];

    let client = reqwest::Client::new();
    let resp = client.post(&token_endpoint).form(&params).send().await?;
    let resp = resp.error_for_status()?;
    let mut token_resp = resp.json::<Oauth2TokenResponse>().await?;

    if let Some(refresh) = token_resp.refresh_token.take() {
        let _ = session.kv_set("oauth2:refresh_token", refresh).await;
    }

    cookie_jar
        .add(
            crate::server::session::Cookie::build(("ISIS_ACCESS", token_resp.access_token.clone()))
                .http_only(true)
                .same_site(axum_extra::extract::cookie::SameSite::Lax)
                .secure(!cfg!(debug_assertions))
                .build(),
        )
        .await;
    Ok(token_resp)
}

pub async fn check_email_occupied() {}
