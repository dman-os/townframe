//! `day x intxt doctor`: a read-only report on how intxt is set up on this machine and in this
//! worktree, one line per check with the command that fixes what is not ok. It changes nothing,
//! and never boots the node in this process: the node-side checks run only against a server that
//! answers on the socket ([`day_node::NodeDir::probe_socket`]).

use super::hooks;
use super::package::Packaging;
use super::package::Unreadable;
use crate::interlude::*;

use big_nodule_core::protocol::ListNodules;
use day_node::SocketState;
use intxt_capture::git_hooks::PostRewrite;
use intxt_capture::host;
use intxt_capture::identity;
use intxt_core::client::Caller;
use service_utils_rs::RequestContext;
use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Status {
    Ok,
    Warn,
    Fail,
    /// Not run here: it needs what this machine or directory does not have (a server, a git
    /// worktree). `fix`, when there is one, is what would let it run.
    Skip,
}

/// One line of the report; `--json` prints these as they are.
#[derive(Debug, Serialize)]
struct Check {
    check: &'static str,
    status: Status,
    detail: String,
    /// The command that fixes it; none for an `ok`.
    fix: Option<String>,
}

impl Check {
    fn ok(check: &'static str, detail: impl Into<String>) -> Self {
        Self {
            check,
            status: Status::Ok,
            detail: detail.into(),
            fix: None,
        }
    }

    fn warn(check: &'static str, detail: impl Into<String>, fix: impl Into<String>) -> Self {
        Self {
            check,
            status: Status::Warn,
            detail: detail.into(),
            fix: Some(fix.into()),
        }
    }

    fn skip(check: &'static str, detail: impl Into<String>, fix: Option<&str>) -> Self {
        Self {
            check,
            status: Status::Skip,
            detail: detail.into(),
            fix: fix.map(str::to_owned),
        }
    }

    fn fail(check: &'static str, detail: impl Into<String>, fix: impl Into<String>) -> Self {
        Self {
            check,
            status: Status::Fail,
            detail: detail.into(),
            fix: Some(fix.into()),
        }
    }
}

const START_SERVER: &str = "day node start --bg";

/// Runs every check in order and prints the report; the exit status is 1 when a check failed.
pub(super) async fn run(json: bool) -> Res<ExitCode> {
    let config = lazy::config().await?;
    let dir = &config.node_dir;
    let cwd = std::env::current_dir()?;
    let identity = identity::resolve(&cwd)?;

    let mut checks = vec![binaries()];
    let (node, socket) = node(dir).await;
    checks.push(node);
    let (server, live) = server(socket);
    checks.push(server);

    // The node-side checks read what the server holds; without one they are skipped, and the
    // checks that follow read only files.
    let mut titles = Vec::new();
    if live {
        let client = lazy::node().await?;
        checks.push(cartridge(&client).await?);
        let packaging = Packaging::new(&client, &cwd, dir.root());
        let (intxts, found) = intxts(&packaging).await?;
        checks.push(intxts);
        titles = found;
    } else {
        for check in ["cartridge", "intxts"] {
            checks.push(Check::skip(
                check,
                "it needs the node's server",
                Some(START_SERVER),
            ));
        }
    }
    checks.push(claude_hooks(
        identity.as_ref().map_or(&cwd, |found| &found.root),
    ));
    match &identity {
        Some(identity) => {
            checks.push(git(identity)?);
            checks.push(worktree(identity, &titles)?);
        }
        None => {
            for check in ["git", "worktree"] {
                checks.push(Check::skip(check, "not in a git worktree", None));
            }
        }
    }

    let mut out = crate::out::out();
    if json {
        writeln!(out, "{}", serde_json::to_string_pretty(&checks)?)?;
    } else {
        let mut table = super::table(["STATUS", "CHECK", "DETAIL", "FIX"]);
        for found in &checks {
            let status = match found.status {
                Status::Ok => "ok",
                Status::Warn => "warn",
                Status::Fail => "fail",
                Status::Skip => "skip",
            };
            table.add_row([
                status,
                found.check,
                &found.detail,
                found.fix.as_deref().unwrap_or(""),
            ]);
        }
        writeln!(out, "{table}")?;
    }
    Ok(if checks.iter().any(|found| found.status == Status::Fail) {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

/// `day` and `intxt` on `PATH`, `intxt` being `day` (the D84 interim: a link to it).
fn binaries() -> Check {
    const LINK: &str = r#"ln -s "$(command -v day)" ~/.cargo/bin/intxt"#;
    let path = std::env::var_os("PATH").unwrap_or_default();
    let find = |name| crate::cmds::plugin::find_on_path(name, &path);
    let Some(day) = find("day") else {
        return Check::fail(
            "binaries",
            "`day` is not on PATH",
            "cargo install --path src/day_cli",
        );
    };
    let Some(intxt) = find("intxt") else {
        return Check::fail(
            "binaries",
            "`intxt` is not on PATH, and the recording hooks call it",
            LINK,
        );
    };
    let (real_day, real_intxt) = (day.canonicalize(), intxt.canonicalize());
    if real_day.is_err() || real_day.as_ref().ok() != real_intxt.as_ref().ok() {
        return Check::fail(
            "binaries",
            format!(
                "`intxt` at {} is not `day` at {}",
                intxt.display(),
                day.display()
            ),
            format!("rm {} && {LINK}", intxt.display()),
        );
    }
    match std::process::Command::new(&day).arg("--version").output() {
        Ok(output) if output.status.success() => Check::ok(
            "binaries",
            format!(
                "{} at {}; `intxt` at {} is the same binary",
                // `--version` is several lines (build details follow the first).
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .next()
                    .unwrap_or_default(),
                day.display(),
                intxt.display()
            ),
        ),
        Ok(output) => Check::fail(
            "binaries",
            format!(
                "`{} --version` failed: {}",
                day.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            ),
            "reinstall day",
        ),
        Err(error) => Check::fail(
            "binaries",
            format!("running `{} --version`: {error}", day.display()),
            "reinstall day",
        ),
    }
}

/// The node directory and its `node.json`, read with this binary. Also answers the socket's
/// state when the node can be read at all (the socket's name is the node's id).
async fn node(dir: &day_node::NodeDir) -> (Check, Option<Res<SocketState>>) {
    if !dir.is_initialized() {
        return (
            Check::fail(
                "node",
                format!("no node at {}", dir.root().display()),
                format!("day node init --node {}", dir.root().display()),
            ),
            None,
        );
    }
    match dir.settings() {
        Ok(settings) => (
            Check::ok(
                "node",
                format!("{} (node {})", dir.root().display(), settings.node_id()),
            ),
            Some(dir.probe_socket().await),
        ),
        Err(error) => {
            let text = format!("{error:#}");
            // A binary older than the node's format (a field it does not know, a version it
            // does not understand) is the case a newer install fixes.
            let fix = if text.contains("unknown field") || text.contains("understands version") {
                "reinstall day".to_owned()
            } else {
                format!("repair {}/node.json", dir.root().display())
            };
            (Check::fail("node", text, fix), None)
        }
    }
}

/// Whether a server answers on the node's socket, and whether it does (the node-side checks
/// need that).
fn server(socket: Option<Res<SocketState>>) -> (Check, bool) {
    match socket {
        None => (
            Check::skip(
                "server",
                "the node cannot be read",
                Some("fix the node check above"),
            ),
            false,
        ),
        Some(Err(error)) => (
            Check::fail(
                "server",
                format!("{error:#}"),
                "remove the file at that path",
            ),
            false,
        ),
        Some(Ok(SocketState::Live)) => (Check::ok("server", "serving"), true),
        Some(Ok(SocketState::Stale)) => (
            Check::warn(
                "server",
                "a socket is left by a server that died; the next start replaces it",
                START_SERVER,
            ),
            false,
        ),
        Some(Ok(SocketState::Absent)) => (
            Check::ok(
                "server",
                "none is serving; recording starts one in the background (it stops by itself when idle)",
            ),
            false,
        ),
    }
}

/// Whether intxt's cartridge is installed and enabled on the node, and at which version.
async fn cartridge(client: &crate::client::NodeClient) -> Res<Check> {
    const INSTALL: &str = "day x intxt install <path to the intxt handler .wasm>";
    let caller = Caller::new(
        client,
        RequestContext::root(),
        None,
        "intxt-doctor".to_owned(),
    );
    let ours = intxt_core::cartridge::cartridge(&[]);
    let nodules = caller.call(ListNodules {}).await?.nodules;
    let Some(record) = nodules
        .into_iter()
        .find(|record| record.cartridge_id == ours.id)
    else {
        return Ok(Check::fail(
            "cartridge",
            "intxt is not installed on the node",
            INSTALL,
        ));
    };
    let version = record
        .version
        .as_ref()
        .map_or_else(|| "no version".to_owned(), ToString::to_string);
    Ok(if !record.enabled {
        Check::fail(
            "cartridge",
            format!("intxt {version} is installed but not enabled"),
            INSTALL,
        )
    } else if record.readiness != big_dispatch_core::Readiness::Ready {
        Check::fail(
            "cartridge",
            format!(
                "intxt {version} is enabled but not ready: {:?}",
                record.readiness
            ),
            INSTALL,
        )
    } else if record.version.as_ref() != Some(&ours.version) {
        Check::warn(
            "cartridge",
            format!(
                "intxt {version} is installed; this binary carries {}",
                ours.version
            ),
            INSTALL,
        )
    } else {
        Check::ok(
            "cartridge",
            format!("intxt {version} is installed and enabled"),
        )
    })
}

/// The intxts the node remembers, those that cannot be read named with the command that forgets
/// them. Also answers each readable one's drawer id and title, for the worktree check.
async fn intxts(packaging: &Packaging<'_>) -> Res<(Check, Vec<(String, String)>)> {
    let live = packaging.live_intxts().await?;
    let mut titles = Vec::new();
    let mut unreadable = Vec::new();
    for drawer in &live {
        match packaging.load(drawer).await? {
            Ok(view) => titles.push((drawer.to_string(), view.title.title)),
            Err(error) => unreadable.push(error),
        }
    }
    let check = if unreadable.is_empty() {
        Check::ok("intxts", format!("{} remembered, all readable", live.len()))
    } else {
        let mut detail = format!(
            "{} remembered, {} unreadable:",
            live.len(),
            unreadable.len()
        );
        for Unreadable { drawer, reason } in &unreadable {
            write!(detail, " {drawer} ({reason});")?;
        }
        let fix: Vec<_> = unreadable
            .iter()
            .map(|error| format!("day x intxt drop {}", error.drawer))
            .collect();
        Check::warn("intxts", detail.trim_end_matches(';'), fix.join("; "))
    };
    Ok((check, titles))
}

/// Claude Code hooks that record sessions, in the user's settings (which record every project)
/// and in the project's two settings files. A settings file that is not JSON fails the check.
fn claude_hooks(project: &Path) -> Check {
    let mut files = Vec::new();
    if let Some(home) = directories::BaseDirs::new() {
        files.push((
            "user-wide, so every project is recorded",
            home.home_dir().join(".claude/settings.json"),
        ));
    }
    files.push(("this project", project.join(".claude/settings.json")));
    files.push((
        "this project, local",
        project.join(".claude/settings.local.json"),
    ));
    // Running from the home directory names one file twice.
    let mut seen = Vec::new();
    files.retain(|(_, path)| {
        let new = !seen.contains(path);
        seen.push(path.clone());
        new
    });

    let (mut broken, mut found, mut incomplete) = (Vec::new(), Vec::new(), Vec::new());
    for (scope, path) in &files {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                broken.push(format!("{} cannot be read: {error}", path.display()));
                continue;
            }
        };
        match serde_json::from_slice(&bytes) {
            Err(error) => broken.push(format!("{} is not valid JSON: {error}", path.display())),
            Ok(settings) => {
                let (current, old) = hooks::recording(&settings);
                if current + old > 0 {
                    found.push(format!(
                        "{} ({scope}): {current} of {} events",
                        path.display(),
                        hooks::EVENTS.len()
                    ));
                }
                if old > 0 || (current > 0 && current < hooks::EVENTS.len()) {
                    incomplete.push(format!(
                        "intxt record install claude-code{}",
                        if scope.starts_with("user") {
                            " --user"
                        } else {
                            ""
                        }
                    ));
                }
            }
        }
    }
    if !broken.is_empty() {
        return Check::fail(
            "claude-hooks",
            broken.join("; "),
            "fix the syntax error in the file named, then run `intxt doctor` again",
        );
    }
    if found.is_empty() {
        return Check::warn(
            "claude-hooks",
            "no intxt recording hook in ~/.claude/settings.json or this project's settings",
            "intxt record install claude-code",
        );
    }
    if !incomplete.is_empty() {
        return Check::warn(
            "claude-hooks",
            format!(
                "{}; some events are missing or still run the old `day x intxt capture` command",
                found.join("; ")
            ),
            incomplete.join("; "),
        );
    }
    Check::ok("claude-hooks", found.join("; "))
}

/// Whether the clone's `post-rewrite` hook is intxt's, absent or someone else's.
fn git(identity: &identity::Identity) -> Res<Check> {
    Ok(
        match intxt_capture::git_hooks::inspect_post_rewrite(&identity.root)? {
            PostRewrite::Ours(path) => Check::ok(
                "git",
                format!("post-rewrite at {} is intxt's", path.display()),
            ),
            PostRewrite::Absent(path) => Check::warn(
                "git",
                format!(
                    "no post-rewrite hook at {}: it is installed on the first recording, and rewrites are matched by patch id until then",
                    path.display()
                ),
                "day x intxt capture install-git-hooks",
            ),
            PostRewrite::Foreign(path) => Check::warn(
                "git",
                format!(
                    "post-rewrite at {} is not intxt's, so it is left alone and rewrites are matched by patch id",
                    path.display()
                ),
                format!(
                    "add `intxt record git-rewrite \"$1\"` to {} (git's stdin passes through)",
                    path.display()
                ),
            ),
        },
    )
}

/// The intxt this worktree is on.
fn worktree(identity: &identity::Identity, titles: &[(String, String)]) -> Res<Check> {
    Ok(match host::recorded_worktree(identity)?.intxt {
        Some(drawer) => {
            let title = titles
                .iter()
                .find(|(known, _)| *known == drawer)
                .map_or_else(String::new, |(_, title)| format!("  \"{title}\""));
            Check::ok(
                "worktree",
                format!("{} is on intxt {drawer}{title}", identity.root.display()),
            )
        }
        None => Check::ok(
            "worktree",
            format!(
                "{} is on no intxt (`day x intxt use <intxt>` puts it on one)",
                identity.root.display()
            ),
        ),
    })
}
