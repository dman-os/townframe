use crate::interlude::*;

mod theme;

use leptos_meta::{MetaTags, Stylesheet, Title};

// Auth component base (hosted on its own port; the session cookie is
// host-scoped so the sysadmin's localhost origin shares it).
const AUTH_BASE: &str = "http://localhost:8071";
// Where the magic-link verify should redirect back to (this app).
const SELF_URL: &str = "http://localhost:3000/";

pub fn shell(options: LeptosOptions) -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8"/>
                <meta name="viewport" content="width=device-width, initial-scale=1"/>
                <script>{theme::init_script()}</script>
                <AutoReload options=options.clone() />
                <HydrationScripts options root="http://localhost:3001" />
                <MetaTags/>
            </head>
            <body>
            <App/>
            </body>
        </html>
    }
}

#[component]
pub fn App() -> impl IntoView {
    view! {
        <Stylesheet id="leptos" href="http://localhost:3001/pkg/btress_sysadmin.css"/>
        <Title text="btress sysadmin"/>
        <div style="max-width:24rem; margin:4rem auto; font-family:system-ui, sans-serif;">
            <h1>"btress sysadmin"</h1>
            <LoginCard/>
        </div>
    }
}

/// GET/POST a JSON request to the auth component and return the response body
/// text. Client-only (runs in the hydrated wasm build).
#[cfg(feature = "hydrate")]
async fn fetch_auth_text(
    path: &str,
    method: &str,
    body: Option<String>,
) -> Result<String, wasm_bindgen::JsValue> {
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;

    let window = web_sys::window().expect("window missing");
    let init = web_sys::RequestInit::new();
    init.set_method(method);
    init.set_credentials(web_sys::RequestCredentials::Include);
    if let Some(body) = body {
        let headers = web_sys::Headers::new()?;
        headers.set("Content-Type", "application/json")?;
        init.set_headers(&headers);
        init.set_body(&wasm_bindgen::JsValue::from_str(&body));
    }
    let request = web_sys::Request::new_with_str_and_init(&format!("{AUTH_BASE}{path}"), &init)?;
    let response: web_sys::Response = JsFuture::from(window.fetch_with_request(&request))
        .await?
        .dyn_into()?;
    let text = JsFuture::from(response.text()?).await?;
    Ok(text.as_string().unwrap_or_default())
}

/// Extract the signed-in user's email from the get-session JSON text, if a
/// session exists (`"session":{...}` rather than `"session":null`).
#[cfg(feature = "hydrate")]
fn session_email(json_text: &str) -> Option<String> {
    if !json_text.contains("\"session\":{") {
        return None;
    }
    let key = "\"email\":\"";
    let start = json_text.find(key)? + key.len();
    let rest = &json_text[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

#[component]
fn LoginCard() -> impl IntoView {
    let (logged_in_as, set_logged_in_as) = signal(None::<String>);
    let (notice, set_notice) = signal(String::new());

    // Client-side session check (mirrors gursha: the app never does server-side
    // auth — the browser talks to the auth component directly).
    #[cfg(feature = "hydrate")]
    {
        leptos::task::spawn_local(async move {
            if let Ok(text) = fetch_auth_text("/api/auth/get-session", "GET", None).await
                && let Some(email) = session_email(&text)
            {
                set_logged_in_as.set(Some(email));
            }
        });
    }

    let on_submit = {
        move |ev: web_sys::SubmitEvent| {
            ev.prevent_default();
            #[cfg(feature = "hydrate")]
            {
                use wasm_bindgen::JsCast;
                leptos::task::spawn_local(async move {
                    let email = ev
                        .current_target()
                        .and_then(|target| target.dyn_into::<web_sys::HtmlFormElement>().ok())
                        .and_then(|form| web_sys::FormData::new_with_form(&form).ok())
                        .and_then(|fd| fd.get("email").as_string())
                        .unwrap_or_default();
                    let body = format!(r#"{{"email":"{email}","callbackURL":"{SELF_URL}"}}"#);
                    match fetch_auth_text("/api/auth/sign-in/magic-link", "POST", Some(body)).await
                    {
                        Ok(_) => set_notice.set("Check your email for the login link.".to_string()),
                        Err(_) => set_notice.set("Something went wrong — try again.".to_string()),
                    }
                });
            }
        }
    };

    view! {
        <Show when=move || logged_in_as.get().is_some() fallback=move || view! {
            <form on:submit=on_submit>
                <label for="email">"Email address"</label>
                <input
                    id="email"
                    name="email"
                    type="email"
                    required
                    style="width:100%; padding:0.5rem; margin:0.25rem 0 0.75rem; box-sizing:border-box;"
                />
                <button type="submit">"Send magic link"</button>
            </form>
            <p style="color:gray;">{move || notice.get()}</p>
        }>
            <p>"Logged in as " <strong>{move || logged_in_as.get().unwrap_or_default()}</strong></p>
            <a href="http://localhost:8071/api/auth/sign-out">"Sign out"</a>
        </Show>
    }
}
