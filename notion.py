import os
import requests

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_BASE = "https://api.notion.com/v1"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28"
}

resp = requests.get(f"{NOTION_BASE}/users", headers=HEADERS)
data = resp.json()

print("\n=== Notion Users in Workspace ===")
for u in data.get("results", []):
    if u.get("type") == "person":
        print(f"Name: {u['name']},  ID: {u['id']},  Email: {u['person']['email']}")
