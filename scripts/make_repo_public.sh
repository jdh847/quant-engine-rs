#!/usr/bin/env bash
set -euo pipefail

# Make the repo public using either:
# - GitHub CLI (gh) if installed and authenticated
# - GitHub API via curl using $GITHUB_TOKEN (classic PAT with `repo` scope or fine-grained with repo admin)
#
# Usage:
#   ./scripts/make_repo_public.sh jdh847/private-quant-bot-rust

repo="${1:-}"
if [[ -z "$repo" ]]; then
  echo "Usage: $0 <owner/repo>"
  exit 2
fi

if command -v gh >/dev/null 2>&1; then
  gh repo edit "$repo" --visibility public
  echo "Repo is now public: $repo"
  exit 0
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "Neither gh nor curl found; cannot proceed."
  exit 1
fi

token="${GITHUB_TOKEN:-}"
if [[ -z "$token" ]]; then
  cat <<'EOF'
GITHUB_TOKEN is not set.

Option A (recommended): install/login gh, then rerun.
Option B: export a token just for this command:
  export GITHUB_TOKEN=...   # do NOT paste tokens into chat/logs
EOF
  exit 2
fi

api="https://api.github.com/repos/$repo"
curl -fsSL -X PATCH \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $token" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  -d '{"private":false}' \
  "$api" >/dev/null

echo "Repo is now public: $repo"

