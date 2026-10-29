#!/usr/bin/env bash

load_dotenv_safe_file() {
  local dotenv_path="$1"
  local line key value

  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      "" | \#*)
        continue
        ;;
      export\ *)
        line="${line#export }"
        ;;
    esac

    if [[ "$line" != *=* ]]; then
      printf 'load-dotenv-safe: skipping invalid line (missing =): %s\n' "$line" >&2
      continue
    fi

    key="${line%%=*}"
    value="${line#*=}"

    if [[ ! "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
      printf 'load-dotenv-safe: skipping invalid key: %s\n' "$key" >&2
      continue
    fi

    if [[ ${#value} -ge 2 && "${value:0:1}" == "\"" && "${value: -1}" == "\"" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ ${#value} -ge 2 && "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
      value="${value:1:${#value}-2}"
    fi

    export "$key=$value"
  done < "$dotenv_path"
}

scope_cargo_target_dir() {
  local configured="${CARGO_TARGET_DIR:-}"
  local worktree subdir
  if [[ -z "$configured" || "${TOWNFRAME_SHARED_TARGET_DIR:-0}" == "1" ]]; then
    return 0
  fi
  worktree="${PWD##*/}"
  if [[ -n "${CONTESTANT_SIDE:-}" && ("$CONTESTANT_SIDE" == "a" || "$CONTESTANT_SIDE" == "b") ]]; then
    subdir="side-$CONTESTANT_SIDE"
  elif [[ "$worktree" =~ ^generation-[0-9]+-([ab])$ ]]; then
    subdir="side-${BASH_REMATCH[1]}"
  else
    subdir="$worktree"
  fi

  if [[ "${configured##*/}" =~ ^generation-[0-9]+-([ab])$ ]]; then
    configured="${configured%/*}"
  fi

  if [[ "${configured##*/}" == "$subdir" ]]; then
    return 0
  fi
  export CARGO_TARGET_DIR="${configured%/}/$subdir"
  mkdir -p "$CARGO_TARGET_DIR"
}

configure_sccache() {
  [[ "${TOWNFRAME_SCCACHE:-0}" == "1" ]] || return 0
  if ! command -v sccache >/dev/null 2>&1; then
    printf 'load-dotenv-safe: TOWNFRAME_SCCACHE=1 but sccache is unavailable\n' >&2
    return 1
  fi
  local worktree_parent="${PWD%/*}"
  local target_dir="${CARGO_TARGET_DIR:-$PWD/target}"
  export RUSTC_WRAPPER=sccache
  export CARGO_INCREMENTAL=0
  export SCCACHE_DIR="${SCCACHE_DIR:-${target_dir%/*}/sccache-cache}"
  export SCCACHE_CACHE_SIZE="${SCCACHE_CACHE_SIZE:-8G}"
  export SCCACHE_BASEDIRS="${SCCACHE_BASEDIRS:-$worktree_parent}"
  mkdir -p "$SCCACHE_DIR"
}

if [[ -n "${BASH_SOURCE[0]:-}" && "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ $# -ne 1 ]]; then
    printf 'usage: %s <.env-file>\n' "$0" >&2
    exit 1
  fi
  load_dotenv_safe_file "$1"
  scope_cargo_target_dir
  configure_sccache
elif [[ $# -eq 1 ]]; then
  load_dotenv_safe_file "$1"
  scope_cargo_target_dir
  configure_sccache
fi
