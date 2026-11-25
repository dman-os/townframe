//! `day <name> <args…>` for a `<name>` that is not one of `day`'s own subcommands: runs the
//! executable `day-<name>` found on `PATH`, as `git` and `cargo` do. clap tries the built-in
//! subcommands first (`allow_external_subcommands` only takes what none of them matches), so a
//! plugin can never shadow a built-in.

use crate::interlude::*;

use std::ffi::OsStr;
use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;

/// The first executable regular file named `file` in the directories of `path` (a `PATH` value).
pub(crate) fn find_on_path(file: &str, path: &OsStr) -> Option<PathBuf> {
    std::env::split_paths(path)
        .map(|dir| dir.join(file))
        .find(|candidate| {
            std::fs::metadata(candidate)
                .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        })
}

/// The plugin `day-<name>` on `path`. A name with a path separator is never a plugin's.
pub(crate) fn find(name: &OsStr, path: &OsStr) -> Option<PathBuf> {
    let name = name.to_str().filter(|name| !name.contains('/'))?;
    find_on_path(&format!("day-{name}"), path)
}

/// Replaces this process with the plugin `args[0]` names, passing it `args[1..]` and the
/// resolved node directory as `DAY_NODE`. The plugin's signals and exit status are then its
/// own. `Ok(None)` when no `day-<name>` is on `PATH`; on success this never returns.
pub(crate) async fn exec(args: &[OsString]) -> Res<Option<std::convert::Infallible>> {
    let (name, rest) = args
        .split_first()
        .expect("clap's external subcommand carries its name");
    let path = std::env::var_os("PATH").unwrap_or_default();
    let Some(plugin) = find(name, &path) else {
        return Ok(None);
    };
    let node_dir = lazy::config().await?.node_dir.clone();
    let error = std::process::Command::new(&plugin)
        .args(rest)
        .env("DAY_NODE", node_dir.root())
        .exec();
    Err(error).wrap_err_with(|| format!("running {}", plugin.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn script(dir: &Path, name: &str, mode: u32) {
        let path = dir.join(name);
        std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    #[test]
    fn a_plugin_is_an_executable_day_dash_name_on_the_given_path() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        script(first.path(), "day-notexec", 0o644);
        script(second.path(), "day-hello", 0o755);
        script(second.path(), "day-notexec", 0o755);
        std::fs::create_dir(second.path().join("day-dir")).unwrap();
        let path = std::env::join_paths([first.path(), second.path()]).unwrap();

        assert_eq!(
            find(OsStr::new("hello"), &path),
            Some(second.path().join("day-hello"))
        );
        // The first directory's non-executable file is skipped, not taken.
        assert_eq!(
            find(OsStr::new("notexec"), &path),
            Some(second.path().join("day-notexec"))
        );
        assert_eq!(find(OsStr::new("absent"), &path), None);
        assert_eq!(find(OsStr::new("dir"), &path), None);
        assert_eq!(find(OsStr::new("../hello"), &path), None);
        assert_eq!(find(OsStr::new("hello"), OsStr::new("")), None);
    }
}
