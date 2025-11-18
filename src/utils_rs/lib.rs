mod macros;
pub mod testing;

#[cfg(feature = "automerge")]
pub mod am;

pub mod prelude {
    pub use crate::interlude::*;

    pub use dashmap;
    pub use dotenv_flow;
    pub use educe;
    pub use regex;
    pub use serde_json;
    pub use tokio;
}

mod interlude {
    pub use crate::{default, CHeapStr, DHashMap, JsonExt, ToAnyhow, ToEyre};

    pub use std::{
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock},
    };

    pub use async_trait::async_trait;
    pub use color_eyre::eyre::{
        self as eyre, format_err as ferr, OptionExt as EyreOptExt, Result as Res, WrapErr,
    };
    pub use indexmap::{indexmap, IndexMap};
    pub use serde::{Deserialize, Serialize};
    pub use serde_json::json;
    pub use time::{self, OffsetDateTime};
    pub use uuid::{self, Uuid};

    pub use crate::expect_tags::*;

    pub use tracing::{self, debug, error, info, trace, warn};
    pub use tracing_futures::Instrument;
    pub use tracing_unwrap::*;

    pub use futures::FutureExt;
}

use crate::interlude::*;

use std::io::Write;

mod expect_tags {
    pub const ERROR_TOKIO: &str = "tokio error";
    pub const ERROR_CHANNEL: &str = "channel error";
    pub const ERROR_JSON: &str = "json error";
}

#[inline]
pub fn default<T: Default>() -> T {
    T::default()
}

pub fn eyre_to_anyhow(err: eyre::Report) -> anyhow::Error {
    let err: Box<dyn std::error::Error + Send + Sync + 'static> = Box::from(err);
    anyhow::anyhow!(err)
}

pub fn anyhow_to_eyre(err: anyhow::Error) -> eyre::Report {
    let err: Box<dyn std::error::Error + Send + Sync + 'static> = Box::from(err);
    eyre::format_err!(err)
}

pub trait ToEyre {
    type Out;
    fn to_eyre(self) -> Self::Out;
}

impl<T> ToEyre for Result<T, anyhow::Error> {
    type Out = Result<T, eyre::Report>;

    fn to_eyre(self) -> Self::Out {
        self.map_err(anyhow_to_eyre)
    }
}

pub trait ToAnyhow {
    type Out;
    fn to_anyhow(self) -> Self::Out;
}

impl<T> ToAnyhow for Result<T, eyre::Report> {
    type Out = Result<T, anyhow::Error>;

    fn to_anyhow(self) -> Self::Out {
        self.map_err(eyre_to_anyhow)
    }
}

// NOTE: only use these in actors or single writer scenarios
pub type DHashMap<K, V> = dashmap::DashMap<K, V, ahash::random_state::RandomState>;
pub type DHashMapRef<'a, K, V> = dashmap::mapref::one::Ref<'a, K, V>;
pub type DHashMapMutRef<'a, K, V> = dashmap::mapref::one::RefMut<'a, K, V>;

pub use cheapstr::CHeapStr;

// Ensure that the `tracing` stack is only initialised once using `once_cell`
// isn't required in cargo-nextest since each test runs in a new process
pub fn setup_tracing_once() {
    static TRACING: std::sync::Once = std::sync::Once::new();
    TRACING.call_once(|| {
        setup_tracing().expect("setup tracing error");
    });
}

pub fn setup_tracing() -> eyre::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    let filter = {
        if std::env::var("RUST_BACKTRACE").is_err() {
            std::env::set_var("RUST_BACKTRACE", "1");
        }
        std::env::var("RUST_LOG").ok()
    };

    #[cfg(target_arch = "wasm32")]
    let filter: Option<String> = None;

    let filter = filter.unwrap_or_else(|| "info".into());

    use tracing_subscriber::prelude::*;
    let registry = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(filter))
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_timer(tracing_subscriber::fmt::time::uptime()),
        )
        .with(tracing_error::ErrorLayer::default());

    #[cfg(target_os = "android")]
    let registry = registry.with(tracing_android::layer("org.example.daybook")?);

    registry.try_init().map_err(|err| eyre::eyre!(err))?;

    // color_eyre::install()?;
    let (eyre_panic_hook, eyre_hook) =
        color_eyre::config::HookBuilder::default().try_into_hooks()?;
    std::panic::set_hook(Box::new(move |panic_info| {
        let report = eyre_panic_hook.panic_report(panic_info);
        tracing::error!("{report}");

        // - Tokio does not exit the process when a task panics, so we define a custom
        //   panic hook to implement this behaviour.
        std::process::exit(1);
    }));
    eyre_hook.install()?;

    Ok(())
}

mod cheapstr {
    use crate::interlude::*;

    use std::{
        borrow::Cow,
        hash::{Hash, Hasher},
    };
    // lifted from github.com/bevyengine/bevy 's bevy_core/Name struct
    // MIT/APACHE2 licence
    #[derive(Clone, Serialize, Deserialize)]
    #[serde(crate = "serde", from = "String", into = "String")]
    pub struct CHeapStr {
        hash: u64,
        // make a cow that's backed by Arc<str>
        string: Cow<'static, str>,
    }

    impl CHeapStr {
        /// Creates a new [`IdUnique`] from any string-like type.
        pub fn new(string: impl Into<Cow<'static, str>>) -> Self {
            let string = string.into();
            let mut id = Self { string, hash: 0 };
            id.update_hash();
            id
        }

        /// Gets the name of the entity as a `&str`.
        #[inline]
        pub fn as_str(&self) -> &str {
            &self.string
        }

        fn update_hash(&mut self) {
            let mut hasher = ahash::AHasher::default();
            self.string.hash(&mut hasher);
            self.hash = hasher.finish();
        }
    }

    impl<T> From<T> for CHeapStr
    where
        T: Into<Cow<'static, str>>,
    {
        #[inline(always)]
        fn from(string: T) -> Self {
            Self::new(string)
        }
    }

    impl Hash for CHeapStr {
        fn hash<H: Hasher>(&self, state: &mut H) {
            state.write_u64(self.hash);
        }
    }

    impl PartialEq for CHeapStr {
        fn eq(&self, other: &Self) -> bool {
            if self.hash != other.hash {
                // Makes the common case of two strings not been equal very fast
                return false;
            }

            self.string.eq(&other.string)
        }
    }

    impl Eq for CHeapStr {}

    impl PartialOrd for CHeapStr {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for CHeapStr {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.string.cmp(&other.string)
        }
    }

    impl std::ops::Deref for CHeapStr {
        type Target = Cow<'static, str>;

        fn deref(&self) -> &Self::Target {
            &self.string
        }
    }

    impl std::borrow::Borrow<str> for CHeapStr {
        fn borrow(&self) -> &str {
            &self[..]
        }
    }

    impl From<CHeapStr> for String {
        fn from(value: CHeapStr) -> String {
            // FIXME: optmize this
            /* let string = if let Some(s) = Arc::get_mut(&mut self.0) {
                unsafe {
                    String::from_raw_parts(
                        s as *mut str as *mut u8,
                        s.len(),
                        s.len()
                    )
                }
            } else {
                (&self.0[..]).to_string()
            };
            std::mem::forget(self.0);
            string */
            value.string.into_owned()
        }
    }

    impl std::fmt::Display for CHeapStr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.string.fmt(f)
        }
    }

    impl std::fmt::Debug for CHeapStr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.string.fmt(f)
        }
    }
}

#[cfg(feature = "hash")]
pub mod hash {
    use super::*;

    const SHA2_256: u64 = 0x12;
    pub fn hash_obj<T: serde::Serialize>(obj: &T) -> String {
        use sha2::Digest;
        let mut hash = sha2::Sha256::new();
        json_canon::to_writer(&mut hash, obj).expect("error serializing manifest");
        let hash = hash.finalize();

        let hash =
            multihash::Multihash::<32>::wrap(SHA2_256, &hash[..]).expect("error multihashing");
        encode_base32_multibase(hash.digest())
    }

    pub fn hash_str(string: &str) -> String {
        hash_bytes(string.as_bytes())
    }

    pub fn hash_bytes(bytes: &[u8]) -> String {
        use sha2::Digest;
        let mut hash = sha2::Sha256::new();
        hash.write_all(bytes).expect("error writing to hasher");
        let hash = hash.finalize();

        let hash =
            multihash::Multihash::<32>::wrap(SHA2_256, &hash[..]).expect("error multihashing");
        encode_base32_multibase(hash.digest())
    }

    pub async fn hash_reader<T: tokio::io::AsyncRead>(reader: T) -> Res<String> {
        use sha2::Digest;
        use tokio::io::*;
        let mut hash = sha2::Sha256::new();
        let mut buf = vec![0u8; 65536];

        let reader = tokio::io::BufReader::new(reader);

        let mut reader = std::pin::pin!(reader);

        loop {
            // Read a chunk of data
            let bytes_read = reader.read(&mut buf).await?;

            // Break the loop if we reached EOF
            if bytes_read == 0 {
                break;
            }
            hash.write_all(&buf[..bytes_read])
                .expect("error writing to hasher");
        }
        let hash = hash.finalize();

        let hash =
            multihash::Multihash::<32>::wrap(SHA2_256, &hash[..]).expect("error multihashing");
        let hash = encode_base32_multibase(hash.digest());
        Ok(hash)
    }

    pub fn encode_base32_multibase<T: AsRef<[u8]>>(source: T) -> String {
        let mut base32 = data_encoding::BASE32_NOPAD.encode(source.as_ref());
        base32.make_ascii_lowercase();
        format!("b{base32}")
    }

    // Consider z-base32 https://en.wikipedia.org/wiki/Base32#z-base-32
    pub fn decode_base32_multibase(source: &str) -> eyre::Result<Vec<u8>> {
        match (
            &source[0..1],
            data_encoding::BASE32_NOPAD.decode(source[1..].to_uppercase().as_bytes()),
        ) {
            ("b", Ok(bytes)) => Ok(bytes),
            (prefix, Ok(_)) => Err(eyre::format_err!(
                "unexpected multibase prefix for base32 multibase: {prefix}"
            )),
            (_, Err(err)) => Err(eyre::format_err!("error decoding base32 ({source}): {err}")),
        }
    }

    pub fn encode_hex_multibase<T: AsRef<[u8]>>(source: T) -> String {
        format!(
            "f{}",
            data_encoding::HEXLOWER_PERMISSIVE.encode(source.as_ref())
        )
    }

    pub fn decode_hex_multibase(source: &str) -> eyre::Result<Vec<u8>> {
        match (
            &source[0..1],
            data_encoding::HEXLOWER_PERMISSIVE.decode(&source.as_bytes()[1..]),
        ) {
            ("f", Ok(bytes)) => Ok(bytes),
            (prefix, Ok(_)) => Err(eyre::format_err!(
                "unexpected multibase prefix for hex multibase: {prefix}"
            )),
            (_, Err(err)) => Err(eyre::format_err!("error decoding hex: {err}")),
        }
    }
}

/*

/// A simpler version of [`tokio::fs::try_exists`] that returns
/// false on a non-existent file and not just on a broken symlink.
#[inline(always)]
pub async fn file_exists(path: &Path) -> Result<bool, std::io::Error> {
    match tokio::fs::try_exists(path).await {
        Ok(true) => Ok(true),
        Ok(false) => Ok(false),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub async fn find_entry_recursive(from: &Path, name: &str) -> Res<Option<PathBuf>> {
    let mut cur = from;
    loop {
        let location = cur.join(name);
        match tokio::fs::try_exists(&location).await {
            Ok(true) => {
                return Ok(Some(location));
            }
            Err(err) if err.kind() != std::io::ErrorKind::NotFound => {
                return Err(err).wrap_err("error on file stat");
            }
            _ => {
                let Some(next_cur) = cur.parent() else {
                    return Ok(None);
                };
                cur = next_cur;
            }
        }
    }
}
*/

pub fn find_entry_recursive_sync(from: &Path, name: &str) -> Res<Option<PathBuf>> {
    let mut cur = from;
    loop {
        let location = cur.join(name);
        match std::fs::exists(&location) {
            Ok(true) => {
                return Ok(Some(location));
            }
            Err(err) if err.kind() != std::io::ErrorKind::NotFound => {
                return Err(err).wrap_err("error on file stat");
            }
            _ => {
                let Some(next_cur) = cur.parent() else {
                    return Ok(None);
                };
                cur = next_cur;
            }
        }
    }
}
pub trait JsonExt {
    fn remove_keys_from_obj(self, keys: &[&str]) -> Self;
    fn destructure_into_self(self, from: Self) -> Self;
}
impl JsonExt for serde_json::Value {
    fn remove_keys_from_obj(self, keys: &[&str]) -> Self {
        match self {
            serde_json::Value::Object(mut map) => {
                for key in keys {
                    map.remove(*key);
                }
                serde_json::Value::Object(map)
            }
            json => panic!("provided json was not an object: {:?}", json),
        }
    }
    fn destructure_into_self(self, from: Self) -> Self {
        match (self, from) {
            (serde_json::Value::Object(mut first), serde_json::Value::Object(second)) => {
                for (key, value) in second.into_iter() {
                    first.insert(key, value);
                }
                serde_json::Value::Object(first)
            }
            (first, second) => panic!(
                "provided jsons weren't objects: first {:?}, second: {:?}",
                first, second
            ),
        }
    }
}

/// This baby doesn't work on generic types
pub fn type_name_raw<T>() -> &'static str {
    let name = std::any::type_name::<T>();
    match &name.rfind(':') {
        Some(pos) => &name[pos + 1..name.len()],
        None => name,
    }
}

#[test]
fn test_type_name_macro() {
    struct Foo {}
    assert_eq!("Foo", type_name_raw::<Foo>());
}

pub fn get_env_var<K>(key: K) -> eyre::Result<String>
where
    K: AsRef<std::ffi::OsStr>,
{
    match std::env::var(key.as_ref()) {
        Ok(val) => Ok(val),
        Err(err) => Err(eyre::eyre!(
            "error geting env var {:?}: {err}",
            key.as_ref()
        )),
    }
}

pub fn dotenv_hierarchical() -> Res<Vec<PathBuf>> {
    let preferred_environment = std::env::var("DOTENV_ENV").ok();

    let candidate_filenames = match preferred_environment {
        // the file name that comes first overrides those that come later
        None => vec![".env.local".to_string(), ".env".to_string()],
        Some(ref env_name) => vec![
            format!(".env.{env_name}.local"),
            ".env.local".to_string(),
            format!(".env.{env_name}"),
            ".env".to_string(),
        ],
    };
    let mut path_bufs = vec![];
    let cwd = std::env::current_dir()?;
    let mut found_vars: std::collections::HashMap<String, String> = default();
    for env_filename in candidate_filenames {
        let mut find_root = cwd.clone();
        loop {
            let Some(file_path) = find_entry_recursive_sync(&find_root, &env_filename)? else {
                break;
            };
            for var in dotenv_flow::from_path_iter(&file_path)? {
                let (key, val) = var?;
                // we prefer vars found in files deeper in the tree
                found_vars.entry(key).or_insert(val);
            }
            let parent = file_path
                .parent()
                .unwrap()
                .parent()
                .map(|path| path.to_owned());
            path_bufs.push(file_path);
            let Some(parent) = parent else {
                break;
            };
            find_root = parent.to_owned();
        }
    }
    for (key, val) in found_vars {
        std::env::set_var(key, val);
    }

    Ok(path_bufs)
}
