#!/usr/bin/env bash
# Window-specific screenshot under niri.
#
#   window-shot.sh <out.png> [<match>]        # match app_id or title (substring, case-insensitive)
#   window-shot.sh <out.png> --pid <pid>
#   window-shot.sh <out.png> --id <niri-window-id>
#
# Default match is "desktop", which is the dx-launched native binary's app_id/title family.
# Exits non-zero with a bounded explanation when nothing matches, instead of dumping the window list.
set -euo pipefail

out=${1:-}
if [[ -z $out ]]; then
  echo "usage: window-shot.sh <out.png> [<match>|--pid <pid>|--id <id>]" >&2
  exit 2
fi
shift

mode=title
value=desktop
case ${1:-} in
  --pid) mode=pid; value=${2:-} ;;
  --id)  mode=id;  value=${2:-} ;;
  "")    ;;
  *)     value=$1 ;;
esac

if [[ $out != /* ]]; then
  echo "error: output path must be absolute (niri rejects relative paths): $out" >&2
  exit 2
fi

id=$(niri msg -j windows | python3 -c '
import json, sys
mode, value = sys.argv[1], sys.argv[2].lower()
try:
    windows = json.load(sys.stdin)
except json.JSONDecodeError:
    sys.exit("error: could not parse `niri msg -j windows` output; are you inside a niri session?")
matches = []
for w in windows:
    if mode == "id" and str(w["id"]) == value:
        matches.append(w); break
    if mode == "pid" and str(w["pid"]) == value:
        matches.append(w)
    if mode == "title" and (value in str(w.get("app_id", "")).lower()
                            or value in str(w.get("title", "")).lower()):
        matches.append(w)
if not matches:
    print("error: no window matched", file=sys.stderr); sys.exit(3)
if len(matches) > 1 and mode != "pid":
    print("error: %d windows matched %r:" % (len(matches), value), file=sys.stderr)
    for w in matches:
        print("  id=%s pid=%s app_id=%s title=%r" % (w["id"], w["pid"], w.get("app_id"), w.get("title")),
              file=sys.stderr)
    print("hint: pass --id <id> or --pid <pid>", file=sys.stderr)
    sys.exit(4)
print(matches[0]["id"])
' "$mode" "$value")

niri msg action screenshot-window --id "$id" --path "$out" >/dev/null

# The write is asynchronous; wait briefly rather than reporting a failed capture.
for _ in $(seq 1 50); do
  [[ -s $out ]] && { echo "$out"; exit 0; }
  sleep 0.05
done
echo "error: screenshot command returned but $out was not written" >&2
exit 5
