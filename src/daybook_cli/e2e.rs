//! Blackbox end-to-end tests for the `daybook_cli` binary.
//!
//! Philosophy: real blackbox testing. Every case spawns the actual
//! `daybook_cli` binary in a fresh sandbox temp dir (cwd = sandbox root) and
//! asserts only on process behavior — exit codes, merged stdout+stderr, and
//! state persisted by earlier invocations of the same case. Setup is done
//! *through the CLI itself* (`init`, `import`, ...), never through in-process
//! harness code.
//!
//! Cases are literate `.md` files in trycmd format. One file = one sandbox:
//! every ```console fence in the file runs sequentially in the same temp
//! dir, so a file documents a whole workflow (init -> import -> enable ->
//! list). Markdown outside the fences is free-form annotation.
//!
//! Each case file needs a sibling `*.out/` directory (containing at least a
//! `.keep`) to opt into the sandbox. The `.out/` comparison is a *subset*
//! check, so unlisted generated repo files are ignored.
//!
//! Run against the real binary: `cargo build -p daybook_cli` first, then
//! `cargo nextest run -p daybook_cli -E 'test(e2e)'`.

#![cfg(test)]

use trycmd::TestCases;

/// Path to the built `daybook_cli` binary.
///
/// trycmd resolves `$ daybook_cli ...` through cargo metadata by default,
/// which does not work from in-crate tests — unresolved bin names are
/// silently skipped. Registering the bin explicitly sidesteps that.
fn daybook_cli_path() -> std::path::PathBuf {
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target")
        });
    target_dir.join("debug/daybook_cli")
}

fn assert_bin_exists(path: &std::path::Path) {
    assert!(
        path.exists(),
        "e2e tests need the built binary at {}; run `cargo build -p daybook_cli` first",
        path.display()
    );
}

fn new_suite() -> TestCases {
    let daybook_cli = daybook_cli_path();
    assert_bin_exists(&daybook_cli);
    let daybook_cli_env = daybook_cli.to_string_lossy().into_owned();
    let suite = TestCases::new();
    // The CLI resolves the repo from DAYB_REPO_PATH (cwd is ignored): point
    // every case at a `repo` subdir of its own sandbox so tests are fully
    // isolated from each other and from the default ~/.local/share/daybook.
    suite.env("DAYB_REPO_PATH", "repo");
    // trycmd rewrites only top-level argv, so `$ daybook_cli` works in fences
    // but not: inside a `sh -c '...'` string the inner shell resolves the
    // command through PATH, where no entry exists. Expose the absolute binary
    // path as an env var for sh steps that capture output:
    // `$ sh -c 'ID=$($DAYBOOK_CLI touch); ...'`.
    suite.env("DAYBOOK_CLI", daybook_cli_env.clone());
    // The CLI reports success via tracing logs on stderr (compact format
    // with uptime timestamps). Suppress them suite-wide so snapshots
    // assert real output; clap/eyre errors still surface on stderr.
    suite.env("RUST_LOG", "error");
    // tracing logs render ANSI when piped; disable so only the color-eyre
    // report (which ignores NO_COLOR in this project) carries ANSI.
    suite.env("NO_COLOR", "1");
    suite.register_bin("daybook_cli", trycmd::schema::Bin::Path(daybook_cli));
    // The test plug OCI artifact lives outside the sandbox (built by
    // `xtask build-plug-oci` into target/oci). Register `sh` so cases can
    // copy it in with `$ sh -c 'cp -r "$PLUG_OCI" ./plug-oci'`, keeping the
    // whole flow in trycmd while staying portable.
    suite.register_bin("sh", trycmd::schema::Bin::Path("/bin/sh".into()));
    suite.env(
        "PLUG_OCI",
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../target/oci/@daybook/test"
        ),
    );
    suite
}

/// `init` command: repo bootstrap.
#[test]
fn init_cases() {
    new_suite().case(concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/init/*.md"));
}

/// `plugs` command: import/enable/disable/update/pending/list/show.
#[test]
fn plugs_cases() {
    new_suite().case(concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/plugs/*.md"));
}

/// `touch`/`ls`/`cat`/`dump` document lifecycle.
#[test]
fn docs_cases() {
    new_suite().case(concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/docs/*.md"));
}

/// `ed` editing with a fake EDITOR script.
#[test]
fn edit_cases() {
    new_suite().case(concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/edit/*.md"));
}

/// `sync`/`devices`/`clone` networking surface (sync itself is known-broken;
/// cases assert the deterministic surfaces only).
#[test]
fn sync_cases() {
    new_suite().case(concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/sync/*.md"));
}

/// `exec` plug command invocation.
#[test]
fn exec_cases() {
    new_suite().case(concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/exec/*.md"));
}
