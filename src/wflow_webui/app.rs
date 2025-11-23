use crate::interlude::*;
use leptos_meta::{provide_meta_context, MetaTags, Stylesheet, Title};
use leptos_router::{
    components::{Route, Router, Routes},
    path,
};

pub fn shell(options: LeptosOptions) -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <thaw::ssr::SSRMountStyleProvider>
            <html lang="en">
                <head>
                    <meta charset="utf-8"/>
                    <meta name="viewport" content="width=device-width, initial-scale=1"/>
                    <AutoReload options=options.clone() />
                    <HydrationScripts options/>
                    <MetaTags/>
                </head>
                <body>
                    <App/>
                </body>
            </html>
        </thaw::ssr::SSRMountStyleProvider>
    }
}

#[component]
pub fn App() -> impl IntoView {
    // Provides context that manages stylesheets, titles, meta tags, etc.
    provide_meta_context();

    {
        let auth_signal = RwSignal::new(crate::auth::AuthState { user_id: default() });
        provide_context(auth_signal);
    }

    view! {
        // injects a stylesheet into the document <head>
        // id=leptos means cargo-leptos will hot-reload this stylesheet
        <Stylesheet id="leptos" href="/pkg/web.css"/>

        // sets the document title
        <Title text="Welcome to Leptos"/>

        <thaw::ConfigProvider>
        // <thaw::ToasterProvider>
        // content for this welcome page
            <Router>
                <main>
                    <AppHeader />
                    <Routes fallback=|| "Page not found.".into_view()>
                        <Route path=path!("") view=HomePage/>
                        <Route path=path!("/signup") view=crate::auth::SignUpPage/>
                        <Route path=path!("/auth/redirect") view=crate::auth::RedirectOauth/>
                        // <Route path=path!("/game") view=game::Game/>
                    </Routes>
                </main>
            </Router>
        // </thaw::ToasterProvider>
        </thaw::ConfigProvider>
    }
}

/// Renders the home page of your application.
#[component]
fn HomePage() -> impl IntoView {
    // Creates a reactive value to update the button
    let count = RwSignal::new(0);
    let action = ServerAction::<MyAction>::new();
    let on_click = move |_| {
        *count.write() += 1;
        action.dispatch(MyAction {});
    };

    view! {
        <h1>"Welcome to Leptos!"</h1>
        <button on:click=on_click>"Click Me: " {count}</button>
    }
}

#[component]
fn AppHeader() -> impl IntoView {
    view! {
        <header class="w-full">
            <div class="p-6 w-full flex justify-between">
                <span>ISIS</span>
                <crate::auth::AuthHeader />
            </div>
        </header>
    }
}

#[component]
fn NotFoundPage() -> impl IntoView {
    "Route not found"
}

#[server]
async fn my_action() -> Result<(), ServerFnError> {
    info!("what's upa");
    let cx = expect_context::<SharedServerCtx>();
    let _session = cx.session().await;
    info!("yes");
    Ok(())
}
