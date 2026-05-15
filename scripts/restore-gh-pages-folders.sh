#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "$0")"

TOP_WORKFLOW="top-folder-gh-pages-restore.yml"
SUBTREE_WORKFLOW="subtree-gh-pages-restore.yml"

DEFAULT_MAX_BYTES="950000000"
DEFAULT_BATCH_TARGET_MB="500"
WATCH_INTERVAL="5"

usage() {
  cat <<EOF
Usage: ${SCRIPT_NAME} [options] [FOLDER ...]

Dispatches gh-pages restore workflows one subtree at a time.
When a folder exceeds --max-bytes, the script recursively splits the folder
into immediate child subtrees and restores those one by one.

Options:
  --source-repo REPO         Owner/name for source repository (default: current repo).
  --source-ref REF           Source ref to restore from (default: main).
  --max-bytes BYTES          Maximum allowed gh-pages payload size in bytes for the one-folder workflow.
                             0 disables checks and chunking.
  --batch-target-mb MB       Target batch size for grouped sibling subtree restores (default: 500).
  --cname NAME               Optional CNAME value to write during deployment.
  --reset-branch true|false   Pass through to workflow input (default: false).
  --help                     Show this message.

Examples:
  ${SCRIPT_NAME}
  ${SCRIPT_NAME} --source-ref main --max-bytes 950000000 ARRL CQWW
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

path_bytes() {
  python3 - "$1" <<'PY'
import os
import sys

target = sys.argv[1]
total = 0

if os.path.isfile(target):
    total = os.path.getsize(target)
else:
    for root, _, files in os.walk(target):
        for name in files:
            path = os.path.join(root, name)
            try:
                total += os.path.getsize(path)
            except OSError:
                pass

print(total)
PY
}

abort() {
  log "$*"
  exit 1
}

if ! command -v gh >/dev/null 2>&1; then
  abort "GitHub CLI is required (gh)."
fi

if ! command -v jq >/dev/null 2>&1; then
  abort "jq is required."
fi

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "${repo_root}" ]]; then
  abort "Run this script from inside a git repository."
fi
cd "${repo_root}"

SOURCE_REPO=""
SOURCE_REF="main"
MAX_BYTES="${DEFAULT_MAX_BYTES}"
BATCH_TARGET_MB="${DEFAULT_BATCH_TARGET_MB}"
CNAME_VALUE=""
RESET_BRANCH="false"

while (($#)); do
  case "$1" in
    --source-repo)
      [[ $# -ge 2 ]] || abort "--source-repo requires a value."
      SOURCE_REPO="$2"
      shift 2
      ;;
    --source-ref)
      [[ $# -ge 2 ]] || abort "--source-ref requires a value."
      SOURCE_REF="$2"
      shift 2
      ;;
    --max-bytes)
      [[ $# -ge 2 ]] || abort "--max-bytes requires a value."
      MAX_BYTES="$2"
      shift 2
      ;;
    --batch-target-mb)
      [[ $# -ge 2 ]] || abort "--batch-target-mb requires a value."
      BATCH_TARGET_MB="$2"
      shift 2
      ;;
    --cname)
      [[ $# -ge 2 ]] || abort "--cname requires a value."
      CNAME_VALUE="$2"
      shift 2
      ;;
    --reset-branch)
      [[ $# -ge 2 ]] || abort "--reset-branch requires true or false."
      case "${2,,}" in
        true|false|1|0)
          if [[ "${2,,}" == "1" ]]; then
            RESET_BRANCH="true"
          elif [[ "${2,,}" == "0" ]]; then
            RESET_BRANCH="false"
          else
            RESET_BRANCH="${2,,}"
          fi
          ;;
        *)
          abort "--reset-branch must be true or false."
          ;;
      esac
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --*)
      abort "Unknown option: $1"
      ;;
    *)
      break
      ;;
  esac
done

if [[ -z "${SOURCE_REPO}" ]]; then
  SOURCE_REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
fi

if ! [[ "${MAX_BYTES}" =~ ^[0-9]+$ ]]; then
  abort "--max-bytes must be a non-negative integer."
fi

if ! [[ "${BATCH_TARGET_MB}" =~ ^[0-9]+$ ]] || [[ "${BATCH_TARGET_MB}" -eq 0 ]]; then
  abort "--batch-target-mb must be a positive integer."
fi

BATCH_TARGET_BYTES="$(( BATCH_TARGET_MB * 1024 * 1024 ))"
if [[ "${MAX_BYTES}" -gt 0 && "${BATCH_TARGET_BYTES}" -gt "${MAX_BYTES}" ]]; then
  BATCH_TARGET_BYTES="${MAX_BYTES}"
fi

TOP_LEVEL_EXCLUDES=(
  ".git"
  ".github"
  "scripts"
  ".reconstructed_ledgers"
)

declare -a folders
if (($#)); then
  folders=("$@")
else
  while IFS= read -r -d '' folder_path; do
    folder_name="${folder_path#./}"
    folder_name="${folder_name%/}"
    skip=false
    for excluded in "${TOP_LEVEL_EXCLUDES[@]}"; do
      if [[ "${folder_name}" == "${excluded}" ]]; then
        skip=true
        break
      fi
    done
    if git check-ignore -q "${folder_name}/" || git check-ignore -q "${folder_name}"; then
      skip=true
    fi
    [[ "${skip}" == true ]] && continue
    folders+=("${folder_name}")
  done < <(find . -mindepth 1 -maxdepth 1 -type d -print0)
fi

if [[ "${#folders[@]}" -eq 0 ]]; then
  abort "No folders found to restore."
fi

dispatch_workflow() {
  local workflow="$1"
  shift
  local start_epoch
  local run_id
  local -a fields=("$@")

  start_epoch="$(date -u +%s)"
  if ! gh workflow run "${workflow}" --repo "${SOURCE_REPO}" "${fields[@]}" >/tmp/gh-workflow-dispatch.log 2>&1; then
    log "Workflow dispatch failed for ${workflow}."
    if [[ -f /tmp/gh-workflow-dispatch.log ]]; then
      sed -n '1,20p' /tmp/gh-workflow-dispatch.log
    fi
    return 1
  fi

  for _ in {1..60}; do
    run_id="$(gh run list --workflow "${workflow}" --repo "${SOURCE_REPO}" --limit 20 --json databaseId,createdAt \
      --jq "[.[] | select((.createdAt | fromdateiso8601) > ${start_epoch})] | sort_by(.createdAt) | reverse | .[0].databaseId // empty")"
    if [[ -n "${run_id}" && "${run_id}" != "null" ]]; then
      echo "${run_id}"
      return 0
    fi
    sleep 2
  done

  abort "Could not determine run id for ${workflow}."
}

wait_for_run() {
  local run_id="$1"
  gh run watch "${run_id}" --repo "${SOURCE_REPO}" --exit-status --interval "${WATCH_INTERVAL}"
}

run_single_folder() {
  local folder="$1"
  local reset="$2"
  local field_args=(
    "--field" "source_repo=${SOURCE_REPO}"
    "--field" "source_ref=${SOURCE_REF}"
    "--field" "top_folder=${folder}"
    "--field" "reset_branch=${reset}"
    "--field" "max_bytes=${MAX_BYTES}"
  )
  if [[ -n "${CNAME_VALUE}" ]]; then
    field_args+=( "--field" "cname=${CNAME_VALUE}" )
  fi

  local run_id
  local dispatch_epoch
  log "Restoring ${folder} with one-folder workflow."
  dispatch_epoch="$(date -u +%s)"
  run_id="$(dispatch_workflow "${TOP_WORKFLOW}" "${field_args[@]}")"
  if ! wait_for_run "${run_id}"; then
    abort "Top-folder restore failed for ${folder}. Run: https://github.com/${SOURCE_REPO}/actions/runs/${run_id}"
  fi
  wait_for_pages_deploy "${dispatch_epoch}"
}

run_subtree_restore() {
  local prefix="$1"
  local reset="$2"
  local -a field_args=(
    "--field" "source_repo=${SOURCE_REPO}"
    "--field" "source_ref=${SOURCE_REF}"
    "--field" "source_prefix=${prefix}"
    "--field" "reset_branch=${reset}"
    "--field" "max_bytes=${MAX_BYTES}"
  )
  if [[ -n "${CNAME_VALUE}" ]]; then
    field_args+=( "--field" "cname=${CNAME_VALUE}" )
  fi

  local run_id
  local dispatch_epoch
  log "Restoring subtree ${prefix}."
  dispatch_epoch="$(date -u +%s)"
  run_id="$(dispatch_workflow "${SUBTREE_WORKFLOW}" "${field_args[@]}")"
  if ! wait_for_run "${run_id}"; then
    abort "Subtree restore failed for ${prefix}. Run: https://github.com/${SOURCE_REPO}/actions/runs/${run_id}"
  fi
  wait_for_pages_deploy "${dispatch_epoch}"
}

run_subtree_batch() {
  local prefixes_file="$1"
  local reset="$2"
  local prefix_count
  local dispatch_epoch
  local run_id
  local single_prefix=""
  local -a field_args=(
    "--field" "source_repo=${SOURCE_REPO}"
    "--field" "source_ref=${SOURCE_REF}"
    "--field" "source_prefixes=@${prefixes_file}"
    "--field" "reset_branch=${reset}"
    "--field" "max_bytes=${MAX_BYTES}"
  )
  if [[ -n "${CNAME_VALUE}" ]]; then
    field_args+=( "--field" "cname=${CNAME_VALUE}" )
  fi

  prefix_count="$(wc -l < "${prefixes_file}" | tr -d ' ')"
  if [[ "${prefix_count}" -eq 1 ]]; then
    single_prefix="$(head -n 1 "${prefixes_file}" | tr -d '\r')"
  fi
  log "Restoring subtree batch (${prefix_count} prefixes)."
  dispatch_epoch="$(date -u +%s)"
  run_id="$(dispatch_workflow "${SUBTREE_WORKFLOW}" "${field_args[@]}")"
  if ! wait_for_run "${run_id}"; then
    if run_failed_due_to_size "${run_id}"; then
      if [[ "${prefix_count}" -gt 1 ]]; then
        split_and_run_batch "${prefixes_file}" "${reset}"
        return 0
      fi
      if [[ -n "${single_prefix}" ]]; then
        log "Single-prefix batch still exceeds total size; recursing into ${single_prefix}."
        restore_prefix "${single_prefix}" "${reset}"
        return 0
      fi
    fi
    abort "Subtree batch restore failed. Run: https://github.com/${SOURCE_REPO}/actions/runs/${run_id}"
  fi
  wait_for_pages_deploy "${dispatch_epoch}"
}

has_remote_folder() {
  local folder="$1"
  gh api "repos/${SOURCE_REPO}/contents/${folder}?ref=${SOURCE_REF}" --silent >/dev/null
}

max_file_exceeds() {
  local folder="$1"
  local too_big_file
  if [[ "${MAX_BYTES}" -eq 0 ]]; then
    echo ""
    return 0
  fi
  too_big_file="$(find "${folder}" -type f -size +"${MAX_BYTES}c" -print -quit 2>/dev/null || true)"
  if [[ -n "${too_big_file}" ]]; then
    echo "${too_big_file}"
  else
    echo ""
  fi
}

list_immediate_children() {
  python3 - "$1" <<'PY'
import os
import sys

target = sys.argv[1]
try:
    children = sorted(os.listdir(target))
except OSError:
    children = []

for name in children:
    path = os.path.join(target, name)
    if os.path.isdir(path) or os.path.isfile(path):
        print(path)
PY
}

wait_for_pages_deploy() {
  local start_epoch="$1"
  local run_id=""

  for _ in {1..120}; do
    run_id="$(gh run list --workflow pages-build-deployment --repo "${SOURCE_REPO}" --limit 20 --json databaseId,createdAt \
      --jq "[.[] | select((.createdAt | fromdateiso8601) > ${start_epoch})] | sort_by(.createdAt) | reverse | .[0].databaseId // empty")"
    if [[ -n "${run_id}" && "${run_id}" != "null" ]]; then
      break
    fi
    sleep 2
  done

  if [[ -z "${run_id}" || "${run_id}" == "null" ]]; then
    abort "Could not determine pages-build-deployment run after gh-pages update."
  fi

  log "Waiting for Pages deployment run ${run_id}."
  gh run watch "${run_id}" --repo "${SOURCE_REPO}" --exit-status --interval "${WATCH_INTERVAL}"
}

run_failed_due_to_size() {
  local run_id="$1"
  gh run view "${run_id}" --repo "${SOURCE_REPO}" --log-failed | grep -q "exceeds max_bytes"
}

split_and_run_batch() {
  local prefixes_file="$1"
  local reset="$2"
  local prefix_count
  local left_file
  local right_file

  prefix_count="$(wc -l < "${prefixes_file}" | tr -d ' ')"
  if [[ "${prefix_count}" -le 1 ]]; then
    abort "Single-prefix batch still exceeds max_bytes and cannot be split further."
  fi

  left_file="$(mktemp)"
  right_file="$(mktemp)"

  python3 - "$prefixes_file" "$left_file" "$right_file" <<'PY'
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
left = pathlib.Path(sys.argv[2])
right = pathlib.Path(sys.argv[3])

lines = [line for line in src.read_text(encoding="utf-8").splitlines() if line.strip()]
mid = max(1, len(lines) // 2)

left.write_text("".join(f"{line}\n" for line in lines[:mid]), encoding="utf-8")
right.write_text("".join(f"{line}\n" for line in lines[mid:]), encoding="utf-8")
PY

  log "Batch exceeded size limit; retrying as two smaller batches."
  run_subtree_batch "${left_file}" "${reset}"
  run_subtree_batch "${right_file}" "false"

  rm -f "${left_file}" "${right_file}"
}

restore_prefix() {
  local prefix="$1"
  local reset="$2"
  local prefix_bytes
  local large_file
  local child
  local child_bytes
  local batch_bytes=0
  local batch_file
  local batch_started=false
  local had_any_entry=false

  prefix_bytes="$(path_bytes "${prefix}")"
  large_file="$(max_file_exceeds "${prefix}")"

  if [[ "${MAX_BYTES}" -eq 0 || "${prefix_bytes}" -le "${MAX_BYTES}" ]]; then
    if [[ "${prefix}" == *"/"* ]]; then
      run_subtree_restore "${prefix}" "${reset}"
    else
      run_single_folder "${prefix}" "${reset}"
    fi
    return 0
  fi

  if [[ -n "${large_file}" ]]; then
    abort "Cannot split ${prefix}: single file ${large_file} is larger than --max-bytes=${MAX_BYTES}."
  fi

  log "Splitting ${prefix} (${prefix_bytes} bytes) into child subtrees."
  batch_file="$(mktemp)"
  while IFS= read -r child; do
    had_any_entry=true
    child_bytes="$(path_bytes "${child}")"

    if [[ "${MAX_BYTES}" -gt 0 && "${child_bytes}" -gt "${MAX_BYTES}" ]]; then
      if [[ "${batch_started}" == true ]]; then
        run_subtree_batch "${batch_file}" "${reset}"
        : > "${batch_file}"
        batch_bytes=0
        batch_started=false
        reset="false"
      fi
      restore_prefix "${child}" "${reset}"
      reset="false"
      continue
    fi

    if [[ "${batch_started}" == true ]] && (( batch_bytes + child_bytes > BATCH_TARGET_BYTES )); then
      run_subtree_batch "${batch_file}" "${reset}"
      : > "${batch_file}"
      batch_bytes=0
      batch_started=false
      reset="false"
    fi

    printf '%s\n' "${child}" >> "${batch_file}"
    batch_bytes=$(( batch_bytes + child_bytes ))
    batch_started=true
  done < <(list_immediate_children "${prefix}")

  if [[ "${batch_started}" == true ]]; then
    run_subtree_batch "${batch_file}" "${reset}"
    reset="false"
  fi

  rm -f "${batch_file}"

  if [[ "${had_any_entry}" != true ]]; then
    abort "Cannot split ${prefix}: no child files or directories found."
  fi
}

main() {
  local folder

  for folder in "${folders[@]}"; do
    if [[ "${folder}" == *"/"* ]]; then
      abort "Folder argument should be top-level only: ${folder}"
    fi
    if [[ ! -d "${folder}" ]]; then
      abort "Not a directory in the current checkout: ${folder}"
    fi
    if ! has_remote_folder "${folder}"; then
      abort "Folder not found in ${SOURCE_REPO}@${SOURCE_REF}: ${folder}"
    fi

    restore_prefix "${folder}" "${RESET_BRANCH}"
    # only the first folder is allowed to reset the branch, matching user's explicit flag
    RESET_BRANCH="false"
  done
}

main
