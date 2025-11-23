#!/usr/bin/env python3
# Minimal single-issue -> Notion sync tool
from __future__ import annotations
import os, sys, json, time, argparse, traceback
from typing import Dict, Any, List, Optional
import requests

# --- config from env ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
PROJECT_MAP_JSON = os.getenv("PROJECT_MAP_JSON", "")
NOTION_PEOPLE_MAP_JSON = os.getenv("NOTION_PEOPLE_MAP_JSON", "")
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
NOTION_BASE = "https://api.notion.com/v1"
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

if not NOTION_TOKEN or not NOTION_DATABASE_ID:
    print("ERROR: NOTION_TOKEN and NOTION_DATABASE_ID must be set as secrets")
    sys.exit(2)

# load optional maps
try:
    PROJECT_MAP = json.loads(PROJECT_MAP_JSON) if PROJECT_MAP_JSON else {}
except Exception:
    PROJECT_MAP = {}
try:
    NOTION_PEOPLE_MAP = json.loads(NOTION_PEOPLE_MAP_JSON) if NOTION_PEOPLE_MAP_JSON else {}
except Exception:
    NOTION_PEOPLE_MAP = {}

# helper: get database schema types
def get_db_schema() -> Dict[str, str]:
    r = requests.get(f"{NOTION_BASE}/databases/{NOTION_DATABASE_ID}", headers=NOTION_HEADERS, timeout=30)
    r.raise_for_status()
    props = r.json().get("properties", {})
    return {k: v.get("type") for k, v in props.items()}

DB_SCHEMA = get_db_schema()

# property names in your Notion DB - adjust if you renamed them
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

# helpers to format Notion payloads based on property types
def select_payload(prop_name: str, value: str):
    t = DB_SCHEMA.get(prop_name)
    if t == "select":
        return {prop_name: {"select": {"name": value}}}
    if t == "multi_select":
        return {prop_name: {"multi_select": [{"name": value}]}}
    return {prop_name: {"rich_text": [{"text": {"content": value}}]}}

def multi_select_payload(prop_name: str, values: List[str]):
    t = DB_SCHEMA.get(prop_name)
    if not values: 
        return {prop_name: {"multi_select": []}} if t == "multi_select" else None
    if t == "multi_select":
        return {prop_name: {"multi_select": [{"name": v} for v in values]}}
    return None

def people_payload(prop_name: str, gh_login: Optional[str]):
    t = DB_SCHEMA.get(prop_name)
    if t == "people":
        if gh_login and gh_login in NOTION_PEOPLE_MAP:
            return {prop_name: {"people": [{"id": NOTION_PEOPLE_MAP[gh_login]}]}}
        return {prop_name: {"people": []}}
    return {prop_name: {"rich_text": [{"text": {"content": gh_login or ""}}]}}

# search Notion by GitHub id
def notion_query_by_github_id(github_id: int) -> Optional[Dict[str, Any]]:
    url = f"{NOTION_BASE}/databases/{NOTION_DATABASE_ID}/query"
    payload = {"filter": {"property": PROP_GH_ID, "number": {"equals": github_id}}, "page_size": 1}
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    items = r.json().get("results", [])
    return items[0] if items else None

def notion_create_page(props: Dict[str, Any]) -> bool:
    if DRY_RUN:
        print("DRY_RUN create payload:", json.dumps(props, indent=2)[:2000])
        return True
    r = requests.post(f"{NOTION_BASE}/pages", headers=NOTION_HEADERS, json={"parent": {"database_id": NOTION_DATABASE_ID}, "properties": props}, timeout=30)
    if r.status_code not in (200,201):
        print("Notion create failed:", r.status_code, r.text)
        return False
    return True

def notion_update_page(page_id: str, props: Dict[str, Any]) -> bool:
    if DRY_RUN:
        print("DRY_RUN update payload:", page_id, json.dumps(props, indent=2)[:2000])
        return True
    r = requests.patch(f"{NOTION_BASE}/pages/{page_id}", headers=NOTION_HEADERS, json={"properties": props}, timeout=30)
    if r.status_code not in (200,201):
        print("Notion update failed:", r.status_code, r.text)
        return False
    return True

def parse_label_map(labels: List[str]) -> Dict[str, str]:
    m = {}
    for s in labels:
        if ":" in s:
            k, v = s.split(":", 1)
            m[k.lower()] = v
    return m

def resolve_project(repo_full: str) -> str:
    short = repo_full.split("/",1)[1] if "/" in repo_full else repo_full
    if short in PROJECT_MAP: 
        return PROJECT_MAP[short]
    for k,v in PROJECT_MAP.items():
        if k.lower() == short.lower():
            return v
    return "All"

def build_properties(issue: Dict[str, Any], repo_full: str) -> Dict[str, Any]:
    gh_id = issue.get("id")
    short_repo = repo_full.split("/",1)[1] if "/" in repo_full else repo_full
    title = issue.get("title","")[:200]
    body = issue.get("body") or ""
    labels = [l.get("name") for l in issue.get("labels",[])] if issue.get("labels") else []
    label_map = parse_label_map(labels)
    gh_assignee = issue.get("assignee",{}).get("login") if issue.get("assignee") else None
    created = issue.get("created_at")
    updated = issue.get("updated_at")
    state = issue.get("state","open")

    props = {}
    props[PROP_TITLE] = {"title":[{"text":{"content": title}}]}
    props[PROP_GH_ID] = {"number": gh_id}
    props.update(select_payload(PROP_REPO, short_repo))
    props.update(select_payload(PROP_PROJECT, resolve_project(short_repo)))
    props[PROP_ISSUE_URL] = {"url": issue.get("html_url")}
    props.update(select_payload(PROP_STATUS, "Done" if state=="closed" else "Backlog"))
    lbl_payload = multi_select_payload(PROP_LABELS, labels)
    if lbl_payload:
        props.update(lbl_payload)
    if "type" in label_map:
        props.update(select_payload(PROP_TYPE, label_map["type"]))
    if "priority" in label_map:
        props.update(select_payload(PROP_PRIORITY, label_map["priority"]))
    # sprint from label or milestone
    sprint_val = None
    for s in labels:
        if s.lower().startswith("sprint:"):
            sprint_val = f"Sprint {s.split(':',1)[1].strip()}"
            break
    if not sprint_val and issue.get("milestone"):
        mtitle = issue["milestone"].get("title","").strip()
        if mtitle:
            sprint_val = mtitle
    if sprint_val:
        props.update(select_payload(PROP_SPRINT, sprint_val))
    props.update(people_payload(PROP_ASSIGNEE, gh_assignee))
    props[PROP_BODY] = {"rich_text":[{"text":{"content": body}}]}
    if created:
        props[PROP_CREATED] = {"date":{"start": created}}
    if updated:
        props[PROP_UPDATED] = {"date":{"start": updated}}
    # fallback: put labels/assignee into notes if DB can't store them
    if DB_SCHEMA.get(PROP_LABELS) != "multi_select" and labels:
        existing = props[PROP_BODY]["rich_text"][0]["text"]["content"]
        props[PROP_BODY] = {"rich_text":[{"text":{"content": existing + "\n\nLabels: " + ", ".join(labels)}}]}
    if DB_SCHEMA.get(PROP_ASSIGNEE) == "people" and gh_assignee and gh_assignee not in NOTION_PEOPLE_MAP:
        existing = props[PROP_BODY]["rich_text"][0]["text"]["content"]
        props[PROP_BODY] = {"rich_text":[{"text":{"content": existing + f"\n\nGitHub assignee: {gh_assignee}"}}]}
    return props

def properties_differ(existing: Dict[str, Any], new_props: Dict[str, Any]) -> bool:
    try:
        props = existing.get("properties", {})
        e_title = "".join(p.get("text",{}).get("content","") for p in props.get(PROP_TITLE,{}).get("title",[]))
        n_title = "".join(p.get("text",{}).get("content","") for p in new_props.get(PROP_TITLE,{}).get("title",[]))
        if e_title.strip() != n_title.strip(): return True
        # check status -> only update to Done if changed
        e_status = props.get(PROP_STATUS,{}).get("select",{}).get("name","")
        n_status = new_props.get(PROP_STATUS,{}).get("select",{}).get("name","")
        if n_status == "Done" and e_status != "Done": return True
        # check body change
        e_body = "".join(p.get("text",{}).get("content","") for p in props.get(PROP_BODY,{}).get("rich_text",[]))
        n_body = "".join(p.get("text",{}).get("content","") for p in new_props.get(PROP_BODY,{}).get("rich_text",[]))
        if e_body.strip() != n_body.strip(): return True
        # check selects: project/type/priority/sprint if present in schema
        for prop in (PROP_PROJECT, PROP_TYPE, PROP_PRIORITY, PROP_SPRINT):
            if DB_SCHEMA.get(prop) == "select":
                e_v = props.get(prop,{}).get("select",{}).get("name","")
                n_v = new_props.get(prop,{}).get("select",{}).get("name","")
                if e_v != n_v: return True
        # labels multi_select
        if DB_SCHEMA.get(PROP_LABELS) == "multi_select":
            e_labels = set([m.get("name") for m in props.get(PROP_LABELS,{}).get("multi_select",[])])
            n_labels = set([m.get("name") for m in new_props.get(PROP_LABELS,{}).get("multi_select",[])])
            if e_labels != n_labels: return True
        return False
    except Exception:
        return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-path", required=True)
    parser.add_argument("--repo", required=True)
    args = parser.parse_args()

    # read GH event
    with open(args.event_path, "r", encoding="utf-8") as f:
        ev = json.load(f)

    issue = ev.get("issue")
    if not issue:
        print("No issue found in event payload; exiting")
        return

    # repo_full passed from workflow
    repo_full = args.repo
    page = notion_query_by_github_id(issue.get("id"))
    new_props = build_properties(issue, repo_full)

    if page:
        if properties_differ(page, new_props):
            # minimal update payload
            update_payload = {}
            update_payload[PROP_TITLE] = new_props[PROP_TITLE]
            update_payload[PROP_BODY] = new_props[PROP_BODY]
            for p in (PROP_PROJECT, PROP_TYPE, PROP_PRIORITY, PROP_SPRINT):
                if p in new_props and DB_SCHEMA.get(p) == "select":
                    update_payload[p] = new_props[p]
            if DB_SCHEMA.get(PROP_LABELS) == "multi_select" and PROP_LABELS in new_props:
                update_payload[PROP_LABELS] = new_props.get(PROP_LABELS, {"multi_select":[]})
            if new_props.get(PROP_STATUS,{}).get("select",{}).get("name") == "Done":
                update_payload[PROP_STATUS] = {"select": {"name": "Done"}}
            ok = notion_update_page(page.get("id"), update_payload)
            print("Updated existing page:", ok)
        else:
            print("No changes detected; nothing to update")
    else:
        ok = notion_create_page(new_props)
        print("Created new page:", ok)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Fatal error:", e)
        traceback.print_exc()
        sys.exit(2)
