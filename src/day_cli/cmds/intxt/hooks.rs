//! `day x intxt record install claude-code`: writes the Claude Code hook configuration that
//! records the operator's sessions. The merge is a pure function over the settings JSON
//! ([`merge`]); [`run`] reads, merges, prints what changed and writes atomically.

use super::capture::write_atomically;
use crate::interlude::*;

use serde_json::Value;

/// The command every hook runs: the stable `intxt` name (a link to `day`), not `day x intxt`.
const COMMAND: &str = "intxt record hook";

/// What earlier versions of this configuration ran; an entry with this command is updated in
/// place to [`COMMAND`].
const OLD_COMMANDS: &[&str] = &["day x intxt capture"];

/// The hook events recorded (ADR 039 §4), and whether the event filters on a tool name (and so
/// is given `"matcher": "*"`; the other events take no matcher, or match everything without one).
const EVENTS: &[(&str, bool)] = &[
    ("SessionStart", false),
    ("UserPromptSubmit", false),
    ("PreToolUse", true),
    ("PostToolUse", true),
    ("PostToolUseFailure", true),
    ("Stop", false),
    ("SubagentStop", false),
    ("PreCompact", false),
    ("SessionEnd", false),
];

/// What a merge did to one event, ordered so that the strongest outcome of an event's entries wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Change {
    Added,
    Unchanged,
    Updated,
}

/// Adds intxt's hook to every recorded event of `settings`, keeping every other key and hook.
/// An entry already running [`COMMAND`] is left alone, and one running an old command is updated
/// in place. Returns what happened per event; fails when `settings` or its `hooks` do not have
/// the shape Claude Code reads.
pub(super) fn merge(settings: &mut Value) -> Res<Vec<(&'static str, Change)>> {
    let root = settings
        .as_object_mut()
        .ok_or_else(|| eyre::eyre!("the settings are not a JSON object"))?;
    let hooks = root
        .entry("hooks")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| eyre::eyre!("`hooks` is not a JSON object"))?;
    let mut changes = Vec::with_capacity(EVENTS.len());
    for &(event, takes_matcher) in EVENTS {
        let groups = hooks
            .entry(event)
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or_else(|| eyre::eyre!("`hooks.{event}` is not a JSON array"))?;
        let mut outcome = Change::Added;
        let entries = groups
            .iter_mut()
            .filter_map(|group| group.get_mut("hooks")?.as_array_mut())
            .flatten();
        for entry in entries {
            let Some(command) = entry.get("command").and_then(Value::as_str) else {
                continue;
            };
            if command == COMMAND {
                outcome = outcome.max(Change::Unchanged);
            } else if OLD_COMMANDS.contains(&command) {
                entry["command"] = json!(COMMAND);
                outcome = outcome.max(Change::Updated);
            }
        }
        if outcome == Change::Added {
            let mut group = json!({ "hooks": [{ "type": "command", "command": COMMAND }] });
            if takes_matcher {
                group["matcher"] = json!("*");
            }
            groups.push(group);
        }
        changes.push((event, outcome));
    }
    Ok(changes)
}

/// The settings file the install writes: the project's by default, the user's with `--user`.
fn settings_path(user: bool) -> Res<PathBuf> {
    if user {
        let dirs = directories::BaseDirs::new()
            .ok_or_else(|| eyre::eyre!("no home directory to find ~/.claude/settings.json in"))?;
        Ok(dirs.home_dir().join(".claude/settings.json"))
    } else {
        Ok(std::env::current_dir()?.join(".claude/settings.json"))
    }
}

pub(super) fn run(user: bool, dry_run: bool) -> Res<ExitCode> {
    let path = settings_path(user)?;
    let mut settings = match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .wrap_err_with(|| format!("{} is not valid JSON; not touching it", path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => json!({}),
        Err(error) => return Err(error).wrap_err_with(|| format!("reading {}", path.display())),
    };
    let changes =
        merge(&mut settings).wrap_err_with(|| format!("merging into {}", path.display()))?;
    let rendered = serde_json::to_string_pretty(&settings)? + "\n";
    if dry_run {
        print!("{rendered}");
        return Ok(ExitCode::SUCCESS);
    }
    if changes
        .iter()
        .all(|(_, change)| *change == Change::Unchanged)
    {
        println!(
            "{} already records every event; nothing changed",
            path.display()
        );
    } else {
        write_atomically(&path, rendered.as_bytes())?;
        println!("wrote {}", path.display());
        for (event, change) in &changes {
            match change {
                Change::Added => println!("  {event}: added `{COMMAND}`"),
                Change::Updated => println!("  {event}: updated the old command to `{COMMAND}`"),
                Change::Unchanged => {}
            }
        }
    }
    // `intxt` is what the hooks call; `day-intxt` is what makes `day intxt` work (a `day` plugin).
    let on_path = std::env::var_os("PATH").unwrap_or_default();
    for (name, why) in [
        ("intxt", "the hooks call it"),
        ("day-intxt", "`day intxt` runs it"),
    ] {
        if super::super::plugin::find_on_path(name, &on_path).is_none() {
            println!(
                "`{name}` is not on your PATH, and {why}. Make it a link to `day`:\n  ln -s \"$(command -v day)\" ~/.local/bin/{name}"
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merged(mut settings: Value) -> (Value, Vec<(&'static str, Change)>) {
        let changes = merge(&mut settings).unwrap();
        (settings, changes)
    }

    #[test]
    fn an_empty_file_gets_every_event() {
        let (settings, changes) = merged(json!({}));
        assert!(changes.iter().all(|(_, change)| *change == Change::Added));
        insta::assert_json_snapshot!(settings);
    }

    #[test]
    fn unrelated_keys_and_hooks_are_kept() {
        let (settings, _) = merged(json!({
            "model": "opus",
            "permissions": { "allow": ["Bash(ls)"] },
            "hooks": {
                "PreToolUse": [
                    { "matcher": "Bash", "hooks": [{ "type": "command", "command": "./lint.sh" }] }
                ],
                "Notification": [{ "hooks": [{ "type": "command", "command": "notify" }] }]
            }
        }));
        insta::assert_json_snapshot!(settings);
    }

    #[test]
    fn an_old_capture_entry_is_updated_in_place() {
        let (settings, changes) = merged(json!({
            "hooks": {
                "Stop": [{ "hooks": [
                    { "type": "command", "command": "./before.sh" },
                    { "type": "command", "command": "day x intxt capture", "timeout": 30 }
                ] }]
            }
        }));
        assert!(changes.contains(&("Stop", Change::Updated)));
        insta::assert_json_snapshot!(settings);
    }

    #[test]
    fn a_second_run_changes_nothing() {
        let (once, _) = merged(json!({ "hooks": { "Stop": [{ "hooks": [
            { "type": "command", "command": "day x intxt capture" }
        ] }] } }));
        let (twice, changes) = merged(once.clone());
        assert_eq!(once, twice);
        assert!(
            changes
                .iter()
                .all(|(_, change)| *change == Change::Unchanged)
        );
        insta::assert_json_snapshot!(twice);
    }

    #[test]
    fn a_misshapen_file_is_an_error() {
        assert!(merge(&mut json!([])).is_err());
        assert!(merge(&mut json!({ "hooks": { "Stop": {} } })).is_err());
    }
}
