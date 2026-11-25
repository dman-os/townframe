#!/usr/bin/env bash
# Decide which `checks` jobs a run needs, and write the answers as step outputs.
#
# Inputs (env): EVENT (github.event_name), LABELS (JSON array of
# PR label names), PLAN_RUNNER (command prefix that provides `cargo`; empty when already in the
# dev shell), GITHUB_OUTPUT (defaults to stdout for local runs).
#
# Everything that is not a pull request (main, nightly, dispatch) and every PR labelled
# `full-ci` runs the full suites. Other PRs run only what their changed files can affect:
#   - docs-only changes (markdown, docs/, agent skills) skip every Rust job;
#   - workspace-wide inputs (root manifests and lockfiles, flake, toolchain, .cargo, .config,
#     .github) fall back to the full suites;
#   - anything else goes through `cargo x guardrails plan`, whose `affected` set is the changed
#     crates plus their reverse dependencies. An empty plan, or one that reports a changed path
#     owned by no crate (`global_input_changed`), also falls back to the full suites.
# The heavy optional jobs (Nix packages, web E2E, CLI E2E) additionally need a path or crate
# trigger, or the `nix-heavy` / `e2e` labels.
set -euo pipefail

out() { echo "$1=$2" >> "${GITHUB_OUTPUT:-/dev/stdout}"; }
has_label() { jq -e --arg label "$1" 'index($label) != null' <<<"${LABELS:-[]}" > /dev/null; }

emit_full() {
  out rust true
  out full true
  out tests true
  out packages ""
  out test_args "--workspace --exclude day_cli"
  out nix true
  out e2e_web true
  out e2e_cli true
  exit 0
}

if [ "$EVENT" != pull_request ] || has_label full-ci; then
  emit_full
fi

docs_only='^(docs/|\.agents/|\.claude/)|\.md$|^(LICENSE|THIRD_PARTY_NOTICES)'
global='^(Cargo\.(toml|lock)|flake\.(nix|lock)|rust-toolchain(\.toml)?|\.cargo/|\.github/|\.config/|src/(wasm_utils_rs|btress_sysadmin)/Cargo\.(toml|lock))'
# Paths whose change can alter what the heavy jobs build or serve but that are not (or not only)
# visible through a crate's reverse dependencies: the Nix expressions, WIT, vendored patches,
# the JS workspace and the wasm guest workspace.
heavy_paths='^(src/nix/|patches/|wit/|x/|pnpm-lock\.yaml|pnpm-workspace\.yaml|package\.json|src/wasm_utils_rs/|src/micro_)'
cli_paths='^(src/wasm_utils_rs/|src/intxt_nodule|src/test_|wit/)'

# Diff against the merge base with main, not the PR base: in a stack a PR's base is the layer
# below it and only the tip runs CI, so the tip must cover the whole stack over main.
# --no-renames lists both sides of a rename, so a moved crate counts as changed at both paths.
mapfile -t files < <(git diff --no-renames --name-only origin/main...HEAD)
mapfile -t rust_files < <(printf '%s\n' "${files[@]}" | grep -Ev "$docs_only" | grep -v '^$' || true)

if [ "${#rust_files[@]}" -eq 0 ]; then
  out rust false
  out full false
  out tests false
  out packages ""
  out test_args ""
  out nix false
  out e2e_web false
  out e2e_cli false
  exit 0
fi

if printf '%s\n' "${rust_files[@]}" | grep -Eq "$global"; then
  emit_full
fi

plan=$(${PLAN_RUNNER:-} cargo x guardrails plan "${rust_files[@]}")
if [ "$(jq -r '.global_input_changed or (.affected | length == 0)' <<<"$plan")" = true ]; then
  emit_full
fi

affected=$(jq -r '.affected | join(" ")' <<<"$plan")
# `day_cli` tests need the built binary and run in the CLI E2E job, so the unit-test lane
# excludes it exactly as the full run does.
test_args=$(jq -r '.affected | map(select(. != "day_cli") | "-p " + .) | join(" ")' <<<"$plan")
affects() { jq -e --arg crate "$1" '.affected | index($crate) != null' <<<"$plan" > /dev/null; }
touches() { printf '%s\n' "${rust_files[@]}" | grep -Eq "$1"; }

nix=false
e2e_web=false
e2e_cli=false
if has_label nix-heavy || affects daytxt_web || touches "$heavy_paths"; then nix=true; fi
if has_label e2e || affects daytxt_web || touches "$heavy_paths"; then e2e_web=true; fi
if has_label e2e || affects day_cli || touches "$cli_paths"; then e2e_cli=true; fi

out rust true
out full false
out tests "$([ -n "$test_args" ] && echo true || echo false)"
out packages "$affected"
out test_args "$test_args"
out nix "$nix"
out e2e_web "$e2e_web"
out e2e_cli "$e2e_cli"
