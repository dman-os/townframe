//! Finds or holds a node, as `day` does (ADR 033 section 5): a host reaches the node in a
//! directory through the socket of the process that holds it, or, when none does, boots it in
//! this process and becomes the holder.
//!
//! - [`NodeDir`] is the node directory: its settings, the boot and node locks, and the socket.
//! - [`NodeClient`] is the one handle a host calls through, whichever way it reached the node.
//! - [`Booted`] is a node booted in this process, for the host that holds it for the long run
//!   (`day node run`) instead of for one command.

#[cfg(not(unix))]
compile_error!(
    "big_node_holder needs Unix domain sockets and file locks; there is no Windows transport yet"
);

mod interlude {
    pub use utils_rs::prelude::Arc;
    pub use utils_rs::prelude::Deserialize;
    pub use utils_rs::prelude::ERROR_TOKIO;
    pub use utils_rs::prelude::EyreOptExt;
    pub use utils_rs::prelude::Path;
    pub use utils_rs::prelude::PathBuf;
    pub use utils_rs::prelude::Res;
    pub use utils_rs::prelude::Serialize;
    pub use utils_rs::prelude::WrapErr;
    pub use utils_rs::prelude::default;
    pub use utils_rs::prelude::eyre;
}

mod booted;
mod client;
mod dir;

pub use booted::Booted;
pub use client::CONNECT_DEADLINE;
pub use client::Mode;
pub use client::NodeClient;
pub use client::Reply;
pub use client::ensure_nsid;
pub use dir::DevSettings;
pub use dir::LockHeld;
pub use dir::LockRole;
pub use dir::NodeDir;
pub use dir::NodeLock;
pub use dir::NodeSettings;
pub use dir::PeerSettings;
pub use dir::SocketState;
