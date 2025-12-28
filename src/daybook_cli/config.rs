use crate::interlude::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct CliConfig {
    pub repo_path: PathBuf,
}

impl CliConfig {
    pub async fn source() -> Res<Self> {
        let cwd = std::env::current_dir()?;
        let repo_path = match path_from_env(&cwd, "DAYB_REPO_PATH")? {
            Some(path) => path,
            None => {
                let dirs = directories::ProjectDirs::from("org", "daybook", "daybook")
                    .ok_or_eyre("failed to get xdg directories")?;
                dirs.data_dir().into()
            }
        };
        Ok(Self { repo_path })
    }
}

fn path_from_env(cwd: &Path, env_name: &str) -> Res<Option<PathBuf>> {
    let path = match std::env::var(env_name) {
        Ok(path) => Some(PathBuf::from(path)),
        Err(std::env::VarError::NotUnicode(os_str)) => Some(PathBuf::from(os_str)),
        Err(std::env::VarError::NotPresent) => None,
    };

    if let Some(path) = path {
        let path = cwd.join(&path);

        Ok(Some(std::path::absolute(&path).wrap_err_with(|| {
            format!("error absolutizing path {path:?} from env ${env_name}")
        })?))
    } else {
        Ok(None)
    }
}
