#!/usr/bin/env bash
set -euo pipefail

DEMO_ROOT=/home/azureuser/retail-data-demo
BRANCH=${DBT_CLOUD_DEPLOYMENT_BRANCH:-}
if [[ -z "$BRANCH" && -f "$DEMO_ROOT/.env" ]]; then
  BRANCH=$(awk -F= '$1 == "DBT_CLOUD_DEPLOYMENT_BRANCH" {print substr($0, index($0, "=") + 1)}' "$DEMO_ROOT/.env")
  BRANCH=${BRANCH%\"}
  BRANCH=${BRANCH#\"}
  BRANCH=${BRANCH%\'}
  BRANCH=${BRANCH#\'}
fi
BRANCH=${BRANCH:-demo/dbt-cloud-databricks}

cd "$DEMO_ROOT"
git fetch --quiet origin

base_ref=origin/main
if git show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
  base_ref="origin/$BRANCH"
fi

worktree=$(mktemp -d)
cleanup() {
  git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --quiet --detach "$worktree" "$base_ref"
rsync -a --delete \
  --exclude .user.yml \
  --exclude profiles.yml \
  --exclude logs/ \
  --exclude target/ \
  "$DEMO_ROOT/dbt/kmart_retail/" "$worktree/dbt/kmart_retail/"

git -C "$worktree" add dbt/kmart_retail
if ! git -C "$worktree" diff --cached --quiet; then
  git -C "$worktree" \
    -c user.name="Retail Data Demo" \
    -c user.email="retail-data-demo@localhost" \
    commit --quiet -m "Publish retail dbt project for Azure Databricks"
fi
git -C "$worktree" push --quiet origin "HEAD:refs/heads/$BRANCH"
echo "Shared dbt project published to origin/$BRANCH"
