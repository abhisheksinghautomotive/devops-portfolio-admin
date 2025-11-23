#!/usr/bin/env python3
"""
notion_sync.py

Adaptive GitHub -> Notion issue sync — now with Sprint mapping.

.env expected:
- NOTION_TOKEN
- NOTION_DATABASE_ID
- GITHUB_TOKEN
- REPOS                (comma-separated owner/repo)
Optional:
- DRY_RUN (1 default)
- RATE_LIMIT_SLEEP
- NOTION_PEOPLE_MAP_JSON (gh_login -> notion-user-id)
- PROJECT_MAP_JSON (repo-short -> Notion Project name)

Behavior highlights:
- Maps GH labels like "type:feature", "priority:high", "sprint:1" -> Notion selects Type/Priority/Sprint
- Uses milestone title as Sprint if milestone exists and label absent
- Sets Project via PROJECT_MAP_JSON (fallback "All")
- Adapts to Notion DB schema types (select, multi_select, people, rich_text)
- DRY_RUN prints payloads
"""
from __future__ import annotations
import os, sys, time, json, traceback
from typing import Dict, Any, List, Optional
import requests
from dotenv import load_dotenv

load_dotenv()

# required env
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPOS_RAW = os.getenv("REPOS", "").strip()

# optional env
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"
RATE_LIMIT_SLEEP = float(os.getenv("RATE_LIMIT_SLEEP", "0.2"))
NOTION_PEOPLE_MAP_JSON = os.getenv("NOTION_PEOPLE_MAP_JSON", "")
PROJECT_MAP_JSON = os.getenv("PROJECT_MAP_JSON", "")

if not NOTION_TOKEN or not NOTION_DATABASE_ID or not GITHUB_TOKEN or not REPOS_RAW:
    print("ERROR: Set NOTION_TOKEN, NOTION_DATABASE_ID, GITHUB_TOKEN and REPOS in .env")
    sys.exit(1)

try:
    NOTION_PEOPLE_MAP = json.loads(NOTION_PEOPLE_MAP_JSON) if NOTION_PEOPLE_MAP_JSON else {}
except Exception:
    NOTION_PEOPLE_MAP = {}

try:
    PROJECT_MAP = json.loads(PROJECT_MAP_JSON) if PROJECT_MAP_JSON else {}
except Exception:
    PROJECT_MAP = {}

NOTION_BASE = "https://api.notion.com/v1"
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
GITHUB_API = "https://api.github.com"
GH_HEADERS = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}

# Notion DB property names (ensure exact match)
PROP_TITLE = "Task"
PROP_GH_ID = "GitHub ID"
PROP_REPO = "Repo"
PROP_PROJECT = "Project"
PROP_TYPE = "Type"
PROP_PRIORITY = "Priority"
PROP_SPRINT = "Sprint"
PROP_LABELS = "Labels"
PROP_STATUS = "Status"
PROP_ASSIGNEE = "Assigned To"
PROP_BODY = "Notes"
PROP_ISSUE_URL = "GitHub Link"
PROP_CREATED = "Created At"
PROP_UPDATED = "Updated At"

def normalize_repo_list(raw: str) -> List[str]:
    items = [r.strip() for r in raw.replace("\n", ",").split(",")]
    return [r for r in items if r and "/" in r]

def get_db_schema() -> Dict[str, str]:
    url = f"{NOTION_BASE}/databases/{NOTION_DATABASE_ID}"
    r = requests.get(url, headers=NOTION_HEADERS, timeout=30)
    r.raise_for_status()
    j = r.json()
    props = j.get("properties", {})
    return {name: info.get("type") for name, info in props.items()}

DB_SCHEMA = get_db_schema()
print("Detected Notion DB schema:", DB_SCHEMA)

def gh_get_issues(owner: str, repo: str) -> List[Dict[str, Any]]:
    url = f"{GITHUB_API}/repos/{owner}/{repo}/issues"
    issues = []
    page = 1
    while True:
        params = {"state": "all", "per_page": 100, "page": page}
        r = requests.get(url, headers=GH_HEADERS, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        for it in batch:
            if "pull_request" in it:
                continue
            issues.append(it)
        if len(batch) < 100:
            break
        page += 1
        time.sleep(RATE_LIMIT_SLEEP)
    return issues

def notion_query_by_github_id(github_id: int) -> Optional[Dict[str, Any]]:
    url = f"{NOTION_BASE}/databases/{NOTION_DATABASE_ID}/query"
    payload = {"filter": {"property": PROP_GH_ID, "number": {"equals": github_id}}, "page_size": 1}
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0] if results else None

# payload builders
def select_payload(prop_name: str, value: str) -> Dict[str, Any]:
    t = DB_SCHEMA.get(prop_name)
    if t == "select":
        return {prop_name: {"select": {"name": value}}}
    if t == "multi_select":
        return {prop_name: {"multi_select": [{"name": value}]}}
    return {prop_name: {"rich_text": [{"text": {"content": value}}]}}

def multi_select_payload(prop_name: str, values: List[str]) -> Optional[Dict[str, Any]]:
    t = DB_SCHEMA.get(prop_name)
    if not values:
        if t == "multi_select":
            return {prop_name: {"multi_select": []}}
        return None
    if t == "multi_select":
        return {prop_name: {"multi_select": [{"name": v} for v in values]}}
    return None

def people_payload(prop_name: str, gh_login: Optional[str]) -> Dict[str, Any]:
    t = DB_SCHEMA.get(prop_name)
    if t == "people":
        if gh_login:
            notion_id = NOTION_PEOPLE_MAP.get(gh_login)
            if notion_id:
                return {prop_name: {"people": [{"id": notion_id}]}}
        return {prop_name: {"people": []}}
    return {prop_name: {"rich_text": [{"text": {"content": gh_login or ""}}]}}

def parse_label_map(labels: List[str]) -> Dict[str, str]:
    m = {}
    for l in labels:
        if ":" in l:
            k, v = l.split(":", 1)
            m[k.lower()] = v
    return m

def resolve_project_name_from_repo(repo_full: str) -> str:
    short = repo_full.split("/", 1)[1] if "/" in repo_full else repo_full
    if short in PROJECT_MAP:
        return PROJECT_MAP[short]
    for k, v in PROJECT_MAP.items():
        if k.lower() == short.lower():
            return v
    return "All"

def resolve_sprint_from_issue(labels: List[str], milestone: Optional[Dict[str, Any]]) -> Optional[str]:
    # label like "sprint:1" -> "Sprint 1"
    for l in labels:
        if l.lower().startswith("sprint:"):
            val = l.split(":", 1)[1].strip()
            if val:
                return f"Sprint {val}"
    # fallback: use milestone title if present and contains "Sprint"
    if milestone and milestone.get("title"):
        title = milestone.get("title").strip()
        if title:
            # if milestone title like "Sprint 1" or "Sprint-1", normalize to "Sprint X"
            if title.lower().startswith("sprint"):
                # return as-is (preserve formatting)
                return title
            return title
    return None

def build_properties(issue: Dict[str, Any]) -> Dict[str, Any]:
    gh_id = issue.get("id")
    repo_text = issue.get("repository_url", "")
    repo_full = "unknown/unknown"
    if repo_text:
        parts = repo_text.rstrip("/").split("/")
        if len(parts) >= 2:
            repo_full = f"{parts[-2]}/{parts[-1]}"
    short_repo = repo_full.split("/",1)[1] if "/" in repo_full else repo_full

    title = issue.get("title","")[:200]
    body = issue.get("body") or ""
    labels = [l.get("name") for l in issue.get("labels",[])] if issue.get("labels") else []
    label_map = parse_label_map(labels)
    gh_assignee = issue.get("assignee", {}).get("login") if issue.get("assignee") else None
    milestone = issue.get("milestone")  # can be None or dict with 'title'
    created = issue.get("created_at")
    updated = issue.get("updated_at")
    gh_state = issue.get("state","open")

    props: Dict[str, Any] = {}
    props[PROP_TITLE] = {"title":[{"text":{"content": title}}]}
    props[PROP_GH_ID] = {"number": gh_id}
    props.update(select_payload(PROP_REPO, short_repo))
    project_val = resolve_project_name_from_repo(short_repo)
    props.update(select_payload(PROP_PROJECT, project_val))
    props[PROP_ISSUE_URL] = {"url": issue.get("html_url")}
    status_val = "Done" if gh_state == "closed" else "Backlog"
    props.update(select_payload(PROP_STATUS, status_val))
    lbl_payload = multi_select_payload(PROP_LABELS, labels)
    if lbl_payload:
        props.update(lbl_payload)
    if "type" in label_map:
        props.update(select_payload(PROP_TYPE, label_map["type"]))
    if "priority" in label_map:
        props.update(select_payload(PROP_PRIORITY, label_map["priority"]))
    # Sprint: label preferred, then milestone title
    sprint_val = resolve_sprint_from_issue(labels, milestone)
    if sprint_val:
        props.update(select_payload(PROP_SPRINT, sprint_val))
    props.update(people_payload(PROP_ASSIGNEE, gh_assignee))
    props[PROP_BODY] = {"rich_text":[{"text":{"content": body}}]}
    if created:
        props[PROP_CREATED] = {"date":{"start": created}}
    if updated:
        props[PROP_UPDATED] = {"date":{"start": updated}}
    # append non-supported label info into notes
    if DB_SCHEMA.get(PROP_LABELS) != "multi_select" and labels:
        existing = props[PROP_BODY]["rich_text"][0]["text"]["content"]
        props[PROP_BODY] = {"rich_text":[{"text":{"content": existing + "\n\nLabels: " + ", ".join(labels)}}]}
    if DB_SCHEMA.get(PROP_ASSIGNEE) == "people" and gh_assignee and NOTION_PEOPLE_MAP.get(gh_assignee) is None:
        existing = props[PROP_BODY]["rich_text"][0]["text"]["content"]
        props[PROP_BODY] = {"rich_text":[{"text":{"content": existing + f"\n\nGitHub assignee: {gh_assignee}"}}]}
    return props

def notion_create_page(properties: Dict[str, Any]) -> bool:
    if DRY_RUN:
        print("DRY_CREATE:", json.dumps(properties, indent=2)[:2500])
        return True
    url = f"{NOTION_BASE}/pages"
    payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties}
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
    if r.status_code not in (200,201):
        print("Notion create error:", r.status_code, r.text)
        return False
    return True

def notion_update_page(page_id: str, properties: Dict[str, Any]) -> bool:
    if DRY_RUN:
        print("DRY_UPDATE:", page_id, json.dumps(properties, indent=2)[:2500])
        return True
    url = f"{NOTION_BASE}/pages/{page_id}"
    r = requests.patch(url, headers=NOTION_HEADERS, json={"properties": properties}, timeout=30)
    if r.status_code not in (200,201):
        print("Notion update error:", r.status_code, r.text)
        return False
    return True

def properties_differ(existing: Dict[str, Any], new_props: Dict[str, Any]) -> bool:
    try:
        props = existing.get("properties", {})
        e_title = "".join(p.get("text",{}).get("content","") for p in props.get(PROP_TITLE,{}).get("title",[]))
        n_title = "".join(p.get("text",{}).get("content","") for p in new_props.get(PROP_TITLE,{}).get("title",[]))
        if e_title.strip() != n_title.strip(): return True
        # project
        if DB_SCHEMA.get(PROP_PROJECT) == "select":
            e_proj = props.get(PROP_PROJECT,{}).get("select",{}).get("name","")
            n_proj = new_props.get(PROP_PROJECT,{}).get("select",{}).get("name","")
            if e_proj != n_proj: return True
        # type
        if DB_SCHEMA.get(PROP_TYPE) == "select":
            e_type = props.get(PROP_TYPE,{}).get("select",{}).get("name","")
            n_type = new_props.get(PROP_TYPE,{}).get("select",{}).get("name","")
            if e_type != n_type: return True
        # priority
        if DB_SCHEMA.get(PROP_PRIORITY) == "select":
            e_pri = props.get(PROP_PRIORITY,{}).get("select",{}).get("name","")
            n_pri = new_props.get(PROP_PRIORITY,{}).get("select",{}).get("name","")
            if e_pri != n_pri: return True
        # sprint
        if DB_SCHEMA.get(PROP_SPRINT) == "select":
            e_s = props.get(PROP_SPRINT,{}).get("select",{}).get("name","")
            n_s = new_props.get(PROP_SPRINT,{}).get("select",{}).get("name","")
            if e_s != n_s: return True
        # labels
        if DB_SCHEMA.get(PROP_LABELS) == "multi_select":
            e_labels = set([m.get("name") for m in props.get(PROP_LABELS,{}).get("multi_select",[])])
            n_labels = set([m.get("name") for m in new_props.get(PROP_LABELS,{}).get("multi_select",[])])
            if e_labels != n_labels: return True
        # body
        e_body = "".join(p.get("text",{}).get("content","") for p in props.get(PROP_BODY,{}).get("rich_text",[]))
        n_body = "".join(p.get("text",{}).get("content","") for p in new_props.get(PROP_BODY,{}).get("rich_text",[]))
        if e_body.strip() != n_body.strip(): return True
        # status: only update when new status is Done and existing not Done
        e_status = props.get(PROP_STATUS,{}).get("select",{}).get("name","")
        n_status = new_props.get(PROP_STATUS,{}).get("select",{}).get("name","")
        if n_status == "Done" and e_status != "Done": return True
        return False
    except Exception:
        return True

def sync_repo(repo_full: str):
    owner, repo = repo_full.split("/",1)
    print(f"Syncing issues from {owner}/{repo} ...")
    issues = gh_get_issues(owner, repo)
    created = updated = 0
    for it in issues:
        try:
            gh_id = it.get("id")
            if not gh_id:
                continue
            existing = notion_query_by_github_id(gh_id)
            new_props = build_properties(it)
            if existing:
                if properties_differ(existing, new_props):
                    update_payload = {}
                    update_payload[PROP_TITLE] = new_props[PROP_TITLE]
                    update_payload[PROP_BODY] = new_props[PROP_BODY]
                    if PROP_PROJECT in new_props and DB_SCHEMA.get(PROP_PROJECT) == "select":
                        update_payload[PROP_PROJECT] = new_props[PROP_PROJECT]
                    if PROP_TYPE in new_props and DB_SCHEMA.get(PROP_TYPE) == "select":
                        update_payload[PROP_TYPE] = new_props[PROP_TYPE]
                    if PROP_PRIORITY in new_props and DB_SCHEMA.get(PROP_PRIORITY) == "select":
                        update_payload[PROP_PRIORITY] = new_props[PROP_PRIORITY]
                    if PROP_SPRINT in new_props and DB_SCHEMA.get(PROP_SPRINT) == "select":
                        update_payload[PROP_SPRINT] = new_props[PROP_SPRINT]
                    if DB_SCHEMA.get(PROP_LABELS) == "multi_select" and PROP_LABELS in new_props:
                        update_payload[PROP_LABELS] = new_props.get(PROP_LABELS, {"multi_select":[]})
                    if new_props.get(PROP_STATUS,{}).get("select",{}).get("name") == "Done":
                        update_payload[PROP_STATUS] = {"select":{"name":"Done"}}
                    if notion_update_page(existing.get("id"), update_payload):
                        updated += 1
            else:
                if notion_create_page(new_props):
                    created += 1
        except Exception as e:
            print("Error processing issue:", it.get("html_url"), e)
            traceback.print_exc()
            continue
    print(f"{repo_full}: created={created} updated={updated}")

def main():
    repo_list = normalize_repo_list(REPOS_RAW)
    if not repo_list:
        print("No valid repos in REPOS env.")
        sys.exit(1)
    for r in repo_list:
        sync_repo(r)

if __name__ == "__main__":
    main()
