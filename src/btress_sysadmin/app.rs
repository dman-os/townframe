use crate::interlude::*;

mod theme;

use leptos_meta::{MetaTags, Stylesheet, Title, provide_meta_context};

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
        // injects a stylesheet into the document <head>
        // id=leptos means cargo-leptos will hot-reload this stylesheet
        <Stylesheet id="leptos" href="http://localhost:3001/pkg/btress_sysadmin.css"/>

        // sets the document title
        <Title text="Welcome to Leptos"/>

        <div>Hello wasmcloud + leptos!</div>
    }
}
