#!/usr/bin/env bash
set -euo pipefail
subject="$1"
shift
git config user.name 'github-actions[bot]'
git config user.email '41898282+github-actions[bot]@users.noreply.github.com'
for path in "$@"; do
  if [ -e "$path" ]; then git add -- "$path"; fi
done
if git diff --cached --quiet; then exit 0; fi
git commit -m "$subject"
for attempt in 1 2 3 4 5; do
  if git pull --rebase origin main && git push origin HEAD:main; then exit 0; fi
  git rebase --abort 2>/dev/null || true
  sleep 4
done
exit 1
