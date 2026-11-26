// The hydrating client's entry point (feature `web`). Without it the bin is empty: cargo wants a
// bin target next to the component's lib, and nothing runs on WASI.
#[cfg(feature = "web")]
fn main() {
    dioxus::launch(intxt_web::app::app);
}

#[cfg(not(feature = "web"))]
fn main() {}
