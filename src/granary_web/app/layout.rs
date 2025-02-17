use leptos::prelude::*;
use leptos_router::components::{Route, Router, Routes};
use leptos_router::path;

// The #[component] macro marks a function as a reusable component
// Components are the building blocks of your user interface
// They define a reusable unit of behavior
#[component]
pub fn App() -> impl IntoView {
    view! {
        <Router>
            <nav>
            </nav>
            <main>
                <Routes fallback=NotFoundPage >
                    <Route path=path!("/") view=Home/>
                    <Route path=path!("/redirect/signin") view=RedirectSignin/>
                </Routes>
            </main>
        </Router>
    }
}

#[component]
fn Home() -> impl IntoView {
    let handle_sign_in = move |_| {
        // Redirect to OIDC authorization endpoint with required parameters
        let auth_url = format!(
            "http://localhost:8181/oauth/v2/authorize\
            ?response_type=code\
            &client_id=307470805355266051\
            &redirect_uri={}/redirect/signin\
            &scope=openid",
            window().location().origin().unwrap()
        );
        window().location().set_href(&auth_url).unwrap();
    };

    view! {
        <div class="landing-page">
            <h1>"Welcome to Granary"</h1>
            <p>"A secure and efficient storage solution"</p>
            <button
                class="sign-in-button"
                on:click=handle_sign_in
            >
                "Sign in"
            </button>
        </div>
    }
}

#[component]
fn RedirectSignin() -> impl IntoView {
    let userinfo = LocalResource::new(|| async move {
        gloo::net::http::Request::get("http://localhost:8181/oidc/v1/userinfo")
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()
    });

    view! {
        <div>
            <h2>"Processing authentication..."</h2>
            <Suspense fallback=move || view! { <p>"Loading user info..."</p> }>
                {move || {
                    userinfo.get().map(|data| {
                        view! {
                            <pre>
                                {serde_json::to_string_pretty(&*data).unwrap()}
                            </pre>
                        }
                    })
                }}
            </Suspense>
        </div>
    }
}

#[component]
fn NotFoundPage() -> impl IntoView {
    "Route not found"
}
