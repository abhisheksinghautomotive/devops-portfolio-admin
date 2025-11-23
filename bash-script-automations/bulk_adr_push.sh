#!/usr/bin/env bash
set -euo pipefail

REPOS=(
  "python-validation-lib"
  "test-execution-orchestrator"
  "infra-bench-platform"
  "testbench-observability"
  "cloud-cost-optimizer"
  "hybrid-networking-simulation"
  "devops-portfolio-admin"
)

OWNER="abhisheksinghautomotive"
BRANCH="chore/adr-000-template"
TEMPLATE_PATH="$(pwd)/templates/ADRs/adr_template.md"

if [ ! -f "$TEMPLATE_PATH" ]; then
  echo "Place adr_template.md in $(pwd) before running."
  exit 1
fi

for repo in "${REPOS[@]}"; do
  echo "=== Processing ${repo} ==="
  rm -rf "${repo}"
  gh repo clone "${OWNER}/${repo}" "${repo}"
  cd "${repo}"

  # create branch
  git checkout -b "${BRANCH}" || git checkout "${BRANCH}" || true

  mkdir -p ADRs
  cp "$TEMPLATE_PATH" ADRs/ADR-000.md

  # update README safely
  if ! grep -q -E "(ADRs/|Design decisions / ADRs)" README.md 2>/dev/null; then
    printf "\n## Design decisions / ADRs\n\nSee the ADRs folder: ./ADRs/\n" >> README.md
    git add README.md
  fi

  git add ADRs/ADR-000.md || true
  if git diff --cached --quiet; then
    echo "No changes to commit for ${repo}."
  else
    git commit -m "chore(adr): add ADR folder and ADR-000 template"
    git push -u origin "${BRANCH}"
    # create PR (non-blocking)
    gh pr create --title "chore(adr): add ADR template (ADR-000)" \
                 --body "Adds ADR-000 template and ADRs folder." \
                 --base main || gh pr create --title "chore(adr): add ADR template (ADR-000)" --body "Adds ADR-000 template and ADRs folder." --base master || true
  fi

  cd ..
done

echo "Done."
