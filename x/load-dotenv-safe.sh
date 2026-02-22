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

if [[ -n "${BASH_SOURCE[0]:-}" && "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ $# -ne 1 ]]; then
    printf 'usage: %s <.env-file>\n' "$0" >&2
    exit 1
  fi
  load_dotenv_safe_file "$1"
elif [[ $# -eq 1 ]]; then
  load_dotenv_safe_file "$1"
fi
