use crate::interlude::*;

use leptos_oidc::*;
use leptos_router::components::*;
use leptos_router::path;

// The #[component] macro marks a function as a reusable component
// Components are the building blocks of your user interface
// They define a reusable unit of behavior
#[component]
pub fn App() -> impl IntoView {
    provide_context(Auth::signal());
    Auth::init(AuthParameters {
        issuer: "http://localhost:8181/oauth/v2/authorize".to_string(),
        client_id: "307470805355266051".to_string(),
        redirect_uri: "http://localhost:3000/redirect/signin".to_string(),
        post_logout_redirect_uri: "http://localhost:3000/logout".to_string(),
        challenge: leptos_oidc::Challenge::S256,
        scope: Some("openid%20profile%20email".to_string()),
        audience: None,
    });
    view! {
        <Router>
            <nav>
            </nav>
            <main>
                <AuthErrorContext><AuthErrorPage></AuthErrorPage></AuthErrorContext>
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
    let auth_loading = || view! { <div>Auth is loading</div> };
    let unauthenticated = || {
        view! {
            <LoginLink class="text-login">Sign in</LoginLink>
        }
    };

    view! {
        <div class="landing-page">
            <h1>"Welcome to Granary"</h1>
            <p>"A secure and efficient storage solution"</p>
            <AuthLoaded fallback=auth_loading>
                <Authenticated unauthenticated=unauthenticated>
                    <Profile/>
                </Authenticated>
            </AuthLoaded>
        </div>
    }
}

#[component]
pub fn Profile() -> impl IntoView {
    let auth = use_context::<AuthSignal>().expect("AuthStore not initialized in error page");
    let user = Signal::derive(move || {
        auth.with(|auth| {
            auth.authenticated()
                .map(|auth| auth.decoded_access_token::<serde_json::Value>(Algorithm::RS256, &[]))
                .flatten()
        })
    });

    view! {
        <h1>Profile</h1>

        <LogoutLink class="text-logout">Sign out</LogoutLink>
        // Your Profile Page
        { move || {
            view! {
                <p>{move || format!("{user:?}")}</p>
            }
        }}

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
pub fn AuthErrorPage() -> impl IntoView {
    let auth =
        use_context::<AuthSignal>().expect("AuthErrorContext: RwSignal<AuthStore> not present");
    let error_message = move || auth.get().error().map(|error| format!("{error:?}"));

    view! {
        <h1>Error occurred</h1>
        <p>There was an error in the authentication process!</p>
        { error_message }
    }
}

#[component]
fn NotFoundPage() -> impl IntoView {
    "Route not found"
}
