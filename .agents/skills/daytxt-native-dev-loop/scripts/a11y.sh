#!/usr/bin/env bash
# Query or drive the Linux accessibility tree for one niri window.
#
#   a11y.sh [<app-id-or-title>]                         # defaults to --llm-query
#   a11y.sh --pid <pid> --interactive
#   a11y.sh --id <niri-window-id> --click '[title="Save"]'
#
# Set ACCESSIBILITY_CLI when the binary is not in one of the usual locations.
set -euo pipefail

subject=${1:-desktop}
shift || true

cli=${ACCESSIBILITY_CLI:-}
if [[ -z $cli ]]; then
  for candidate in \
    "$(command -v accessibility-cli 2>/dev/null || true)" \
    "$PWD/../accessibility-cli/target/release/accessibility-cli" \
    "$PWD/../accessibility-cli/target/debug/accessibility-cli" \
    "${CARGO_TARGET_DIR:-target}/debug/accessibility-cli"; do
    if [[ -n $candidate && -x $candidate ]]; then
      cli=$candidate
      break
    fi
  done
fi
if [[ -z $cli ]]; then
  echo "error: accessibility-cli not found; set ACCESSIBILITY_CLI" >&2
  exit 2
fi

if [[ $subject == --pid ]]; then
  pid=${1:-}
  if [[ -z $pid ]]; then
    echo "error: --pid requires a process id" >&2
    exit 2
  fi
  shift
elif [[ $subject == --id ]]; then
  id=${1:-}
  if [[ -z $id ]]; then
    echo "error: --id requires a niri window id" >&2
    exit 2
  fi
  shift
  pid=$(niri msg -j windows | python3 -c '
import json, sys
wanted = sys.argv[1]
for window in json.load(sys.stdin):
    if str(window["id"]) == wanted:
        print(window["pid"])
        break
else:
    print(f"error: no niri window id matched {wanted}", file=sys.stderr)
    sys.exit(3)
' "$id")
else
  pid=$(niri msg -j windows | python3 -c '
import json, sys
wanted = sys.argv[1].lower()
matches = [
    window for window in json.load(sys.stdin)
    if wanted in str(window.get("app_id", "")).lower()
    or wanted in str(window.get("title", "")).lower()
]
if not matches:
    print(f"error: no niri window matched {wanted!r}", file=sys.stderr)
    sys.exit(3)
if len(matches) != 1:
    print(f"error: {len(matches)} niri windows matched {wanted!r}; use --id or --pid", file=sys.stderr)
    for window in matches:
        print("  id={} pid={} title={!r}".format(window["id"], window["pid"], window.get("title")), file=sys.stderr)
    sys.exit(4)
print(matches[0]["pid"])
' "$subject")
fi

mode=(--llm-query)
case ${1:-} in
  --interactive|--llm-query)
    mode=($1)
    shift
    ;;
  --click|--press|--focus)
    if [[ -z ${2:-} ]]; then
      echo "error: $1 requires a selector" >&2
      exit 2
    fi
    mode=($1 "$2")
    shift 2
    ;;
esac

exec "$cli" --platform linux --pid "$pid" "${mode[@]}" "$@"
