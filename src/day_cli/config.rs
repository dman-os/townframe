use crate::interlude::*;

/// Where the node lives, and where its intxts can be read on the web. Everything about the node
/// itself is read from the directory (see [`crate::context::NodeDir`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliConfig {
    pub node_path: PathBuf,
    /// The base URL of an intxt web viewer (`src/intxt_web`), without a trailing `/`. When set,
    /// `day x intxt show` prints the intxt's viewer URL and `day x intxt pr-block` links it.
    pub viewer_url: Option<String>,
}

/// The environment variable that sets [`CliConfig::viewer_url`].
const VIEWER_ENV: &str = "DAY_INTXT_VIEWER";

impl CliConfig {
    /// Resolves the node directory. Precedence: the `--node` flag, then `DAY_NODE` (clap folds
    /// both into `node_flag`), then the platform data directory. The viewer is `DAY_INTXT_VIEWER`.
    pub fn source(node_flag: Option<&Path>) -> Res<Self> {
        let cwd = std::env::current_dir()?;
        let node_path = match node_flag {
            Some(path) => std::path::absolute(cwd.join(path))
                .wrap_err_with(|| format!("error absolutizing node path {path:?}"))?,
            None => {
                let dirs = directories::ProjectDirs::from("org", "townframe", "day")
                    .ok_or_eyre("failed to get xdg directories")?;
                dirs.data_dir().into()
            }
        };
        let viewer_url = match std::env::var(VIEWER_ENV) {
            Ok(value) => Self::viewer_from(&value)?,
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => return Err(error).wrap_err(VIEWER_ENV),
        };
        Ok(Self {
            node_path,
            viewer_url,
        })
    }

    /// A viewer base URL as the user wrote it: empty means unset, anything else must be an
    /// `http(s)` URL, and a trailing `/` is dropped so paths join with one.
    fn viewer_from(value: &str) -> Res<Option<String>> {
        let value = value.trim();
        if value.is_empty() {
            return Ok(None);
        }
        eyre::ensure!(
            value.starts_with("http://") || value.starts_with("https://"),
            "{VIEWER_ENV} must be an http(s) URL, not {value:?}"
        );
        Ok(Some(value.trim_end_matches('/').to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_viewer_is_an_http_url_without_a_trailing_slash_or_nothing() {
        assert_eq!(CliConfig::viewer_from("").unwrap(), None);
        assert_eq!(CliConfig::viewer_from("  ").unwrap(), None);
        assert_eq!(
            CliConfig::viewer_from("https://v.example/")
                .unwrap()
                .as_deref(),
            Some("https://v.example")
        );
        assert_eq!(
            CliConfig::viewer_from("http://localhost:8080")
                .unwrap()
                .as_deref(),
            Some("http://localhost:8080")
        );
        assert!(CliConfig::viewer_from("v.example").is_err());
    }
}
