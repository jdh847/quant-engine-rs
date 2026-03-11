#!/usr/bin/env bash
set -euo pipefail

# Fail fast on common accidental secret leaks in tracked files.
# This is not a substitute for a full secret-scanner, but it catches the usual foot-guns.

PATTERNS=(
  'ghp_[A-Za-z0-9]{20,}'
  'github_pat_[A-Za-z0-9_]{20,}'
  'AKIA[0-9A-Z]{16}'
  'AIza[0-9A-Za-z_-]{20,}'
  'xox[baprs]-[A-Za-z0-9-]{10,}'
  'sk-[A-Za-z0-9]{20,}'
  '-----BEGIN PRIVATE KEY-----'
  '-----BEGIN RSA PRIVATE KEY-----'
  '-----BEGIN OPENSSH PRIVATE KEY-----'
)

hits=0
for pat in "${PATTERNS[@]}"; do
  if git grep -nE "$pat" -- . >/dev/null 2>&1; then
    echo "Potential secret match: $pat"
    git grep -nE "$pat" -- . || true
    hits=1
  fi
done

if [[ "$hits" -ne 0 ]]; then
  echo "Secret scan failed. Remove secrets and rotate any exposed credentials."
  exit 1
fi

echo "Secret scan OK."

