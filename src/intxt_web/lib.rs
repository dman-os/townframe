//! A read-only viewer of intxts as a Dioxus fullstack app: a `wasi:http/handler` component that
//! server-renders every page, and the hydrating client that navigates between them.
//!
//! The page is `view(state)`: [`ui`] renders an [`view::IntxtView`] and nothing under it reads
//! anything but its props, so the review micro renders the same markup from a view it builds
//! itself. [`app`] is the part that is the viewer: the routes, and the server functions that load
//! what a route shows. [`source`] has the fixture and the seam a node-backed source implements,
//! [`node`] reads a day node over its XRPC, and [`server`] chooses between them from the
//! component's environment. All three are server only (feature `server`): the node, its
//! capabilities and its transport never reach the client build (feature `web`). There are no
//! mutations or accounts.

pub mod app;
pub mod diff;
pub mod feed;
pub mod files;
pub mod guide;
pub mod markdown;
pub mod people;
pub mod session;
pub mod ui;
pub mod view;

#[cfg(feature = "server")]
pub mod node;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub mod source;

// The wasi bindings only exist for the component; the rest builds and tests on the host.
#[cfg(all(feature = "server", target_os = "wasi"))]
mod wasi_http;

#[cfg(test)]
mod tests {
    /// (`method_core` and `service_core_rs` stay in the tree: they are the datum vocabulary
    /// `big_cabinet_core` itself is made of, not a transport.)
    /// The node, its capabilities and its transport are the server's: the hydrating client's
    /// dependency tree must hold none of them, or the client would carry code for reading a node it
    /// can never reach (and would be one dependency away from carrying a capability).
    #[test]
    fn the_client_build_has_no_node_transport() {
        let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
        let output = std::process::Command::new(env!("CARGO"))
            .args(["tree", "--manifest-path", manifest, "--locked", "--offline"])
            .args(["--no-default-features", "--features", "web"])
            .args(["--target", "wasm32-unknown-unknown", "-e", "normal"])
            .args(["--prefix", "none", "--format", "{p}"])
            .output()
            .expect("cargo runs");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let tree = String::from_utf8(output.stdout).expect("cargo tree prints utf-8");
        let packages: Vec<&str> = tree
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .collect();
        for server_only in [
            "big_blobs_core",
            "service_client_core_rs",
            "dioxus-server",
            "wit-bindgen",
        ] {
            assert!(
                !packages.contains(&server_only),
                "{server_only} is in the client's tree:\n{tree}"
            );
        }
        // The check looks at the right tree: the client has the view model's own dependencies.
        for client in ["intxt_core", "dioxus-web"] {
            assert!(packages.contains(&client), "{client} is not in:\n{tree}");
        }
    }
}
