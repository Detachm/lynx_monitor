#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${ROOT_DIR}/artifacts}"
REFERENCE_DATE="${REFERENCE_DATE:-$(TZ=Asia/Shanghai date +%Y%m%d)}"
CONFIRM="${CONFIRM:-false}"

RUNTIME_ARCHIVE_AFTER_DAYS="${RUNTIME_ARCHIVE_AFTER_DAYS:-1}"
RUNTIME_DELETE_AFTER_DAYS="${RUNTIME_DELETE_AFTER_DAYS:-7}"
VALIDATION_MAX_BYTES="${VALIDATION_MAX_BYTES:-10737418240}"
VALIDATION_RETENTION_DAYS="${VALIDATION_RETENTION_DAYS:-1}"
PRUNE_DOCKER_BUILD_CACHE="${PRUNE_DOCKER_BUILD_CACHE:-false}"

confirm_args=(--dry-run)
if [[ "${CONFIRM}" == "true" || "${CONFIRM}" == "1" ]]; then
  confirm_args=(--confirm)
fi

for runtime_root in "${ARTIFACT_DIR}"/runtime-state "${ARTIFACT_DIR}"/runtime-state-live-*; do
  [[ -d "${runtime_root}" ]] || continue
  PYTHONPATH="${ROOT_DIR}/backend" python -m beast_market.ops_cli prune-runtime-state \
    --runtime-state-root "${runtime_root}" \
    --reference-date "${REFERENCE_DATE}" \
    --archive-after-days "${RUNTIME_ARCHIVE_AFTER_DAYS}" \
    --delete-after-days "${RUNTIME_DELETE_AFTER_DAYS}" \
    "${confirm_args[@]}"
done

validation_dir="${ARTIFACT_DIR}/production-validation"
if [[ -d "${validation_dir}" ]]; then
  validation_bytes="$(du -sb "${validation_dir}" | awk '{print $1}')"
  if (( validation_bytes > VALIDATION_MAX_BYTES )); then
    echo "production-validation is ${validation_bytes} bytes, above ${VALIDATION_MAX_BYTES}"
    if [[ "${CONFIRM}" == "true" || "${CONFIRM}" == "1" ]]; then
      if ss -ltn 2>/dev/null | grep -q ':9092' && [[ "${FORCE_VALIDATION_CLEANUP:-false}" != "true" ]]; then
        echo "skipping redpanda-data cleanup because port 9092 is listening; set FORCE_VALIDATION_CLEANUP=true to override"
      else
        rm -rf "${validation_dir}/redpanda-data"
      fi
      rm -rf "${validation_dir}/logs"
      find "${validation_dir}" -maxdepth 1 -type f \( -name 'runtime-health*.json' -o -name 'runtime-config*.json' -o -name '*.log' \) -mtime +"${VALIDATION_RETENTION_DAYS}" -delete
    else
      echo "dry-run: would remove ${validation_dir}/redpanda-data, ${validation_dir}/logs, and old validation JSON/log files"
    fi
  fi
fi

if [[ "${PRUNE_DOCKER_BUILD_CACHE}" == "true" || "${PRUNE_DOCKER_BUILD_CACHE}" == "1" ]]; then
  if [[ "${CONFIRM}" == "true" || "${CONFIRM}" == "1" ]]; then
    docker builder prune -f
  else
    docker builder prune --dry-run || true
  fi
fi
