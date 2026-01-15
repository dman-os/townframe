use thaw::*;

mod actions;
use crate::interlude::*;

#[derive(Clone, Default)]
pub struct AuthState {
    pub user_id: Option<Uuid>,
}

pub type AuthSignal = RwSignal<AuthState>;

#[component]
pub fn AuthHeader() -> impl IntoView {
    let auth = expect_context::<crate::auth::AuthSignal>();
    view! {
        {move ||  if let Some(_user_id) = auth.get().user_id {
           view! {
               <div class="flex gap-2">
                   <SignInButton />
                   <SignUpButton />
               </div>
           }
       } else {
           view! {
               <div class="flex gap-2">
                   <SignInButton />
                   <SignUpButton />
               </div>
           }
       }}
    }
}

#[component]
fn SignInButton() -> impl IntoView {
    use leptos_router::hooks::use_location;
    let action = ServerAction::<StartAuthGrantFlow>::new();
    let loc = use_location();
    let on_click = move |_| {
        let origin = format!("{}{}", loc.pathname.get(), loc.search.get());
        action.dispatch(StartAuthGrantFlow {
            origin: Some(origin),
        });
    };
    view! {
        <Button on:click=on_click>Sign In</Button>
    }
}

#[component]
fn SignUpButton() -> impl IntoView {
    view! {
        <a href="/signup">
            <Button >Sign Up</Button>
        </a>
    }
}

#[server]
async fn start_auth_grant_flow(origin: Option<String>) -> Result<(), ServerFnError> {
    let cx = expect_context::<SharedServerCtx>();
    let session = cx.session().await;

    #[cfg(feature = "ssr")]
    {
        match actions::build_oauth_authorize_url(&cx, &session, origin).await {
            Ok(url) => {
                leptos_axum::redirect(&url);
            }
            Err(_e) => {
                return Err(ServerFnError::ServerError("auth url error".into()));
            }
        }
    }
    Ok(())
}

#[component]
pub fn RedirectOauth() -> impl IntoView {
    use leptos_router::hooks::{use_navigate, use_query_map};

    let toaster = ToasterInjection::expect_context();

    let query = use_query_map();
    let code = move || query().get("code");
    let target = move || {
        if let Some(st) = query().get("state") {
            let decoded = percent_encoding::percent_decode_str(&st)
                .decode_utf8()
                .map(|chr| chr.to_string())
                .unwrap_or_else(|_| "/".to_string());
            decoded
        } else {
            "/".to_string()
        }
    };

    let exchange = ServerAction::<ExchangeCodeForTokens>::new();
    let nav = use_navigate();

    Effect::new(move |_| {
        if let Some(code) = code() {
            exchange.dispatch(ExchangeCodeForTokens { code });
        }
    });

    let value = exchange.value();

    let pending = exchange.pending();
    Effect::new(move |_| {
        if pending.get() {
            toaster.dispatch_toast(
                move || {
                    view! {
                        Signing in...
                    }
                },
                default(),
            );
        }
    });

    Effect::new(move |_| {
        if let Some(res) = value.get() {
            match res {
                Ok(_) => {
                    toaster.dispatch_toast(
                        move || {
                            view! {
                                Success
                            }
                        },
                        default(),
                    );
                    nav(&target(), Default::default());
                }
                Err(_err) => {
                    toaster.dispatch_toast(
                        move || {
                            view! {
                                Sorry, that failed
                            }
                        },
                        default(),
                    );
                    nav(&target(), Default::default());
                }
            }
        }
    });

    view! {
        <div class="w-full min-h-[60vh] flex flex-col gap-8 items-center justify-center">
            <Spinner />
        </div>
    }
}

#[server]
async fn exchange_code_for_tokens(
    code: String,
) -> Result<actions::Oauth2TokenResponse, ServerFnError> {
    let cx = expect_context::<SharedServerCtx>();
    let session = cx.session().await;
    let cookie_jar = cx.cookie_jar().await;

    #[cfg(feature = "ssr")]
    {
        actions::exchange_code_for_tokens_core(&cx, &session, &cookie_jar, &code)
            .await
            .map_err(|err| ServerFnError::ServerError(err.to_string()))
    }
}

#[component]
pub fn SignUpPage() -> impl IntoView {
    let action = ServerAction::<SignUp>::new();
    let value = action.value();
    // check if the server has returned an error
    let _has_error = move || value.with(|val| matches!(val, Some(Err(_))));
    view! {
        <div class="flex flex-col items-center justify-center min-h-svh">
            <div class="flex flex-col w-full max-w-sm gap-6">
                <h1>Sign Up</h1>
                <ActionForm action=action>
                    <div  class="flex flex-col gap-2">
                        <Input input_type=InputType::Email placeholder="email" name="email" />
                        <button r#type="submit">Sign Up</button>
                    </div>
                </ActionForm>
            </div>
        </div>
    }
}

#[server]
async fn sign_up(email: String) -> Result<(), ServerFnError> {
    info!(?email, "signing up");
    Ok(())
}
