#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "$0")"
TARGET_BRANCH="Web"
SOURCE_REF="HEAD"
PUSH_CHANGES="false"
PRINT_PATHS_ONLY="false"

usage() {
  cat <<EOF
Usage: ${SCRIPT_NAME} [options]

Builds the ${TARGET_BRANCH} branch contents from the source ref by keeping:
- all tracked files at the repository root
- the top-level SH6/ directory

Options:
  --branch NAME        Target branch name (default: Web)
  --source-ref REF     Source ref to publish from (default: HEAD)
  --push               Push the updated branch to origin
  --print-paths        Print the selected paths and exit
  --help               Show this message
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

while (($#)); do
  case "$1" in
    --branch)
      [[ $# -ge 2 ]] || { echo "--branch requires a value" >&2; exit 1; }
      TARGET_BRANCH="$2"
      shift 2
      ;;
    --source-ref)
      [[ $# -ge 2 ]] || { echo "--source-ref requires a value" >&2; exit 1; }
      SOURCE_REF="$2"
      shift 2
      ;;
    --push)
      PUSH_CHANGES="true"
      shift
      ;;
    --print-paths)
      PRINT_PATHS_ONLY="true"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "${repo_root}" ]]; then
  echo "Run this script from inside a git repository." >&2
  exit 1
fi
cd "${repo_root}"

if ! git rev-parse --verify --quiet "${SOURCE_REF}^{commit}" >/dev/null; then
  echo "Source ref not found: ${SOURCE_REF}" >&2
  exit 1
fi

declare -a publish_paths=()
while IFS= read -r entry; do
  [[ -z "${entry}" ]] && continue
  if [[ "${entry}" == "SH6" ]]; then
    publish_paths+=("${entry}")
    continue
  fi
  object_type="$(git cat-file -t "${SOURCE_REF}:${entry}" 2>/dev/null || true)"
  if [[ "${object_type}" == "blob" ]]; then
    publish_paths+=("${entry}")
  fi
done < <(git ls-tree --name-only "${SOURCE_REF}")

if [[ "${#publish_paths[@]}" -eq 0 ]]; then
  echo "No publishable paths found in ${SOURCE_REF}" >&2
  exit 1
fi

if [[ "${PRINT_PATHS_ONLY}" == "true" ]]; then
  printf '%s\n' "${publish_paths[@]}"
  exit 0
fi

tree_input="$(mktemp "${TMPDIR:-/tmp}/web-branch-tree.XXXXXX")"
cleanup() {
  rm -f "${tree_input}"
}
trap cleanup EXIT

while IFS= read -r -d '' entry; do
  path="${entry#*$'\t'}"
  for publish_path in "${publish_paths[@]}"; do
    if [[ "${path}" == "${publish_path}" ]]; then
      printf '%s\0' "${entry}" >> "${tree_input}"
      break
    fi
  done
done < <(git ls-tree -z "${SOURCE_REF}")

# Reuse the source commit's object IDs directly. This avoids downloading and
# rewriting every SH6 blob in a partial clone merely to publish a reduced tree.
desired_tree="$(git mktree -z --missing < "${tree_input}")"
parent_commit=""
if git ls-remote --exit-code --heads origin "${TARGET_BRANCH}" >/dev/null 2>&1; then
  remote_target_ref="refs/remotes/origin/${TARGET_BRANCH}"
  git fetch --filter=blob:none --no-tags origin \
    "+refs/heads/${TARGET_BRANCH}:${remote_target_ref}" >/dev/null
  parent_commit="$(git rev-parse "${remote_target_ref}^{commit}")"
  current_tree="$(git rev-parse "${parent_commit}^{tree}")"
  if [[ "${current_tree}" == "${desired_tree}" ]]; then
    log "No ${TARGET_BRANCH} branch changes detected."
    exit 0
  fi
fi

source_sha="$(git rev-parse --short "${SOURCE_REF}")"
declare -a commit_args=("${desired_tree}")
if [[ -n "${parent_commit}" ]]; then
  commit_args+=( -p "${parent_commit}" )
fi
new_commit="$(git commit-tree "${commit_args[@]}" <<EOF
Publish root files and SH6 to ${TARGET_BRANCH}

The Web branch is the reduced publish surface for browser-facing assets,
so it is rebuilt from ${SOURCE_REF} using only top-level files and SH6/.

Constraint: Web branch intentionally excludes archive subdirectories outside SH6
Confidence: high
Scope-risk: narrow
Reversibility: clean
Directive: Keep selection limited to root files plus SH6 unless the publish surface is explicitly expanded
Tested: scripts/sync-web-branch.sh --print-paths --source-ref ${SOURCE_REF}
Not-tested: remote publish from GitHub Actions end-to-end
Related: ${source_sha}
EOF
)"

git branch -f "${TARGET_BRANCH}" "${new_commit}" >/dev/null

log "Committed ${TARGET_BRANCH} branch update from ${SOURCE_REF}."
if [[ "${PUSH_CHANGES}" == "true" ]]; then
  git push -u origin "${TARGET_BRANCH}" >/dev/null
  log "Pushed ${TARGET_BRANCH} to origin."
fi
