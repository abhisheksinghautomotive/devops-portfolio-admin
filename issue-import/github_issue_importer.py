#!/usr/bin/env python3
"""
gh-based issue importer — ensures labels & milestones exist before creating issues.

Behavior:
- Ensures labels exist (creates missing via `gh label create`)
- Ensures milestones exist:
    - If `gh milestone` command exists, would use it (not required here)
    - Otherwise uses `gh api` to list/create milestones via REST endpoints
- Skips issues that already exist (exact title match)
- Creates issues via `gh issue create`

Usage:
  export OWNER=your-github-username   # if JSON repo entries are "repo" not "owner/repo"
  gh auth login                       # one-time
  python github_issue_importer.py issues/python-validation-lib.json
"""
import json
import os
import subprocess
import sys
from typing import Dict, Any, List, Set, Tuple


def run(cmd: List[str]) -> Tuple[str, str, int]:
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.stdout.strip(), p.stderr.strip(), p.returncode


def gh_has_command(cmd: str) -> bool:
    out, err, code = run(["gh", "help", cmd])
    return code == 0


def build_repo_full(repo_field: str, default_owner: str) -> str:
    repo_field = repo_field.strip()
    if "/" in repo_field:
        return repo_field
    if not default_owner:
        raise ValueError(f"Repo '{repo_field}' missing owner; set OWNER env var.")
    return f"{default_owner}/{repo_field}"


def existing_labels(repo: str) -> Set[str]:
    out, err, code = run(["gh", "label", "list", "--repo", repo, "--json", "name"])
    if code != 0:
        out2, err2, code2 = run(["gh", "label", "list", "--repo", repo])
        if code2 != 0:
            return set()
        return set(line.strip() for line in out2.splitlines() if line.strip())
    try:
        arr = json.loads(out)
        return set(item["name"] for item in arr)
    except Exception:
        return set()


def create_label_if_missing(repo: str, name: str, color: str = "ededed", description: str = "") -> None:
    current = existing_labels(repo)
    if name in current:
        return
    cmd = ["gh", "label", "create", name, "--repo", repo, "--color", color]
    if description:
        cmd += ["--description", description]
    out, err, code = run(cmd)
    if code != 0:
        print(f"Warning: label create failed for {name} in {repo}: {err}")


def ensure_labels(repo: str, labels: List[str]) -> None:
    color_map = {
        "type:feature": "7dd3fc",
        "type:infra": "fb923c",
        "type:test": "34d399",
        "type:docs": "93c5fd",
        "type:security": "ef4444",
        "type:adr": "a78bfa",
        "priority:high": "b91c1c",
        "priority:medium": "f59e0b",
        "priority:low": "10b981",
    }
    desc_map = {
        "type:feature": "feature work",
        "type:infra": "infrastructure",
        "type:test": "tests",
        "type:docs": "documentation",
        "type:security": "security",
        "type:adr": "architecture decision record",
        "priority:high": "high priority",
    }

    existing = existing_labels(repo)
    for lb in labels:
        if lb in existing:
            continue
        color = color_map.get(lb, "ededed")
        desc = desc_map.get(lb, "")
        create_label_if_missing(repo, lb, color=color, description=desc)


# ------------------ Milestone handling ------------------

def existing_milestones_via_api(repo: str) -> Set[str]:
    # Uses gh api to GET /repos/{owner}/{repo}/milestones
    out, err, code = run(["gh", "api", f"/repos/{repo}/milestones", "--jq", ".[].title"])
    if code != 0:
        return set()
    return set(line.strip() for line in out.splitlines() if line.strip())


def create_milestone_via_api(repo: str, title: str) -> bool:
    # POST /repos/{owner}/{repo}/milestones with {title}
    out, err, code = run(["gh", "api", f"/repos/{repo}/milestones", "--method", "POST", "-f", f"title={title}"])
    if code != 0:
        print(f"Warning: failed to create milestone '{title}' in {repo}: {err}")
        return False
    return True


def ensure_milestone(repo: str, title: str) -> None:
    if not title:
        return
    # Prefer gh milestone subcommand if available (some gh builds don't include it)
    if gh_has_command("milestone"):
        # If gh milestone exists, we attempt to use it (but many gh builds do not have it)
        out, err, code = run(["gh", "milestone", "list", "--repo", repo, "--json", "title"])
        if code == 0:
            try:
                arr = json.loads(out)
                names = {i["title"] for i in arr}
                if title in names:
                    return
                cmd = ["gh", "milestone", "create", title, "--repo", repo]
                o, e, c = run(cmd)
                if c != 0:
                    print(f"Warning: failed to create milestone '{title}' via gh: {e}")
                return
            except Exception:
                pass
    # Fallback to gh api (works with your gh version)
    current = existing_milestones_via_api(repo)
    if title in current:
        return
    created = create_milestone_via_api(repo, title)
    if not created:
        # Non-fatal
        print(f"Warning: could not ensure milestone '{title}' in {repo}")


# ------------------ Issue creation ------------------

def issue_exists(repo: str, title: str) -> bool:
    out, err, code = run(["gh", "issue", "list", "--repo", repo, "--search", title, "--json", "title"])
    if code != 0:
        return False
    try:
        arr = json.loads(out)
        for it in arr:
            if it.get("title", "").strip() == title.strip():
                return True
    except Exception:
        pass
    return False


def create_issue(repo: str, issue: Dict[str, Any]) -> None:
    title = issue["title"]
    body = issue.get("body", "")
    labels = issue.get("labels", [])
    assignees = issue.get("assignees", [])
    milestone = issue.get("milestone", "")

    cmd: List[str] = ["gh", "issue", "create", "--repo", repo, "--title", title]
    if body:
        cmd += ["--body", body]
    for l in labels:
        cmd += ["--label", l]
    for a in assignees:
        cmd += ["--assignee", a]
    # Only pass milestone if it already exists (avoid gh error)
    if milestone:
        # check via API whether milestone exists (to avoid gh error if creation failed)
        current = existing_milestones_via_api(repo)
        if milestone in current:
            cmd += ["--milestone", milestone]
        else:
            # milestone not present; do not pass --milestone (issue will be created without it)
            print(f"    Note: milestone '{milestone}' not present in {repo}; creating issue without milestone")

    out, err, code = run(cmd)
    if code != 0:
        print(f"Error creating issue in {repo}: {err}")
    else:
        print(f"Created: {out.splitlines()[-1] if out else 'OK'}")


# ------------------ Main ------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python github_issue_importer.py <json-file>")
        sys.exit(1)

    json_path = sys.argv[1]
    default_owner = os.getenv("OWNER", "").strip() or None
    with open(json_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    # Quick check: ensure gh is available
    out, err, code = run(["gh", "--version"])
    if code != 0:
        print("Error: gh CLI not available or not authenticated. Run 'gh auth login' first.")
        sys.exit(1)

    for idx, issue in enumerate(data.get("issues", []), start=1):
        repo_field = issue.get("repo")
        if not repo_field:
            print(f"[{idx}] Skipping: missing repo in issue {issue.get('title')}")
            continue
        try:
            repo_full = build_repo_full(repo_field, default_owner)
        except ValueError as e:
            print(f"[{idx}] {e}")
            continue

        print(f"[{idx}] Preparing {repo_full} :: {issue.get('title')}")
        ensure_labels(repo_full, issue.get("labels", []))
        ensure_milestone(repo_full, issue.get("milestone", ""))
        if issue_exists(repo_full, issue.get("title", "")):
            print(f"    Skipped (exists): {issue.get('title')}")
            continue
        create_issue(repo_full, issue)


if __name__ == "__main__":
    main()
