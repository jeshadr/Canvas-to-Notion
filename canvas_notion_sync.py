import os
import sys
import time
import requests
from datetime import datetime, timedelta
from dateutil import parser as dateparser
from dotenv import load_dotenv
from notion_client import Client as NotionClient
from typing import Dict, Any, List, Optional
import pytz

load_dotenv()

# ---------- Env ----------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
CANVAS_BASE_URL = os.getenv("CANVAS_BASE_URL", "").rstrip("/")
CANVAS_TOKEN = os.getenv("CANVAS_TOKEN")
TIMEZONE = os.getenv("TIMEZONE", "America/Phoenix")
LOOKAHEAD_DAYS = int(os.getenv("LOOKAHEAD_DAYS", "120"))

# Keep only your chosen courses
# Example:
# COURSE_IDS=224756,231655,228303,235490
# COURSE_NAME_OVERRIDES=224756:CSE 485,231655:CSE 412,228303:CSE 463,235490:CSE 471
COURSE_IDS = [int(x.strip()) for x in os.getenv("COURSE_IDS", "").split(",") if x.strip().isdigit()]
OVERRIDES_RAW = os.getenv("COURSE_NAME_OVERRIDES", "")
OVERRIDES: Dict[int, str] = {}
for part in [p.strip() for p in OVERRIDES_RAW.split(",") if p.strip()]:
    if ":" in part:
        cid, name = part.split(":", 1)
        if cid.strip().isdigit():
            OVERRIDES[int(cid.strip())] = name.strip()

if not all([NOTION_TOKEN, NOTION_DATABASE_ID, CANVAS_BASE_URL, CANVAS_TOKEN]):
    print("Missing required env vars. Check NOTION_TOKEN, NOTION_DATABASE_ID, CANVAS_BASE_URL, CANVAS_TOKEN")
    sys.exit(1)
if not COURSE_IDS:
    print("COURSE_IDS is empty. Add a comma separated list of Canvas course IDs to .env")
    sys.exit(1)

notion = NotionClient(auth=NOTION_TOKEN)
tz = pytz.timezone(TIMEZONE)
CANVAS_HEADERS = {"Authorization": f"Bearer {CANVAS_TOKEN}"}

# Caches
_DB_SCHEMA: Dict[str, Any] = {}
_SELECT_PROPERTY_CACHE: Dict[str, bool] = {}
_STATUS_PROPERTY_IS_STATUS_TYPE = True

# ---------- Canvas helpers ----------
def canvas_get(url: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    next_url = url
    local_params = params.copy() if params else {}
    while next_url:
        resp = requests.get(next_url, headers=CANVAS_HEADERS, params=local_params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            items.extend(data)
        else:
            return data
        link = resp.headers.get("Link", "")
        next_url = None
        if link:
            for part in link.split(","):
                seg = part.strip().split(";")
                if len(seg) >= 2:
                    url_part = seg[0].strip()[1:-1]
                    rel = seg[1].strip()
                    if rel == 'rel="next"':
                        next_url = url_part
                        break
        local_params = {}
    return items

def get_courses_by_id(course_ids: List[int]) -> List[Dict[str, Any]]:
    courses: List[Dict[str, Any]] = []
    for cid in course_ids:
        url = f"{CANVAS_BASE_URL}/api/v1/courses/{cid}"
        try:
            resp = requests.get(url, headers=CANVAS_HEADERS, timeout=20)
            if resp.status_code == 200:
                courses.append(resp.json())
            else:
                print(f"Failed to fetch course {cid}: HTTP {resp.status_code}")
        except requests.RequestException as e:
            print(f"Error fetching course {cid}: {e}")
        time.sleep(0.1)
    return courses

def get_course_assignments(course_id: int) -> List[Dict[str, Any]]:
    url = f"{CANVAS_BASE_URL}/api/v1/courses/{course_id}/assignments"
    return canvas_get(url, {"per_page": 100})

def get_course_quizzes(course_id: int) -> List[Dict[str, Any]]:
    url = f"{CANVAS_BASE_URL}/api/v1/courses/{course_id}/quizzes"
    try:
        return canvas_get(url, {"per_page": 100})
    except requests.HTTPError as e:
        print(f"Quizzes not accessible for course {course_id}: {e}")
        return []

def get_submission(course_id: int, assignment_id: int) -> Optional[Dict[str, Any]]:
    url = f"{CANVAS_BASE_URL}/api/v1/courses/{course_id}/assignments/{assignment_id}/submissions/self"
    try:
        resp = requests.get(url, headers=CANVAS_HEADERS, timeout=20)
        if resp.status_code == 200:
            return resp.json()
    except requests.RequestException:
        pass
    return None

# ---------- Time helpers ----------
def parse_canvas_time(iso_str: Optional[str]) -> Optional[datetime]:
    if not iso_str:
        return None
    dt = dateparser.parse(iso_str)
    if not dt.tzinfo:
        dt = pytz.UTC.localize(dt)
    return dt.astimezone(tz)

def time_str(dt: datetime) -> str:
    return dt.strftime("%I:%M %p").lstrip("0")

# ---------- Notion schema helpers ----------
def load_db_schema() -> Dict[str, Any]:
    global _DB_SCHEMA
    if not _DB_SCHEMA:
        _DB_SCHEMA = notion.databases.retrieve(NOTION_DATABASE_ID)
    return _DB_SCHEMA

def db_has_property(name: str) -> bool:
    db = load_db_schema()
    return name in db.get("properties", {})

def ensure_property(name: str, ptype: str, options: Optional[List[str]] = None) -> None:
    db = load_db_schema()
    props = db.get("properties", {})
    if name in props:
        existing_type = props[name].get("type")
        if existing_type != ptype:
            print(f"Warning: property {name} exists as type {existing_type}, expected {ptype}. Adapting at runtime.")
        return
    new_prop: Dict[str, Any] = {}
    if ptype == "title":
        new_prop = {"title": {}}
    elif ptype == "rich_text":
        new_prop = {"rich_text": {}}
    elif ptype == "date":
        new_prop = {"date": {}}
    elif ptype == "select":
        new_prop = {"select": {"options": [{"name": o} for o in (options or [])]}}
    elif ptype == "status":
        new_prop = {"status": {"options": [{"name": o} for o in (options or ["To do", "In Progress", "Complete", "DNF"])]}}
    else:
        raise ValueError(f"Unsupported property type: {ptype}")

    try:
        notion.databases.update(database_id=NOTION_DATABASE_ID, properties={name: new_prop})
        _DB_SCHEMA.clear()
        load_db_schema()
        print(f"Created property {name} ({ptype})")
    except Exception as e:
        print(f"Failed to create property {name}: {e}")

def ensure_schema() -> None:
    # Only the properties you want visible
    ensure_property("Task", "title")
    ensure_property("Class", "select")
    ensure_property("Type", "select", ["Assignment", "Quiz", "Exam"])
    ensure_property("Due", "date")
    ensure_property("Time", "rich_text")
    # Status can be 'status' or 'select'; prefer 'status'
    if db_has_property("Status"):
        t = load_db_schema()["properties"]["Status"]["type"]
        global _STATUS_PROPERTY_IS_STATUS_TYPE
        _STATUS_PROPERTY_IS_STATUS_TYPE = (t == "status")
    else:
        ensure_property("Status", "status", ["To do", "In Progress", "Complete", "DNF"])
        _STATUS_PROPERTY_IS_STATUS_TYPE = True
    # Hidden but critical unique key to prevent duplicates when Due changes
    ensure_property("Key", "rich_text")  # Hide this column in your Notion view

def property_is_select(prop: str) -> bool:
    if prop in _SELECT_PROPERTY_CACHE:
        return _SELECT_PROPERTY_CACHE[prop]
    try:
        db = load_db_schema()
        p = db["properties"].get(prop)
        is_select = p and p.get("type") in ["select", "multi_select"]
        _SELECT_PROPERTY_CACHE[prop] = bool(is_select)
        return bool(is_select)
    except Exception:
        _SELECT_PROPERTY_CACHE[prop] = True
        return True

def status_prop_value(status_name: str) -> Dict[str, Any]:
    if _STATUS_PROPERTY_IS_STATUS_TYPE:
        return {"status": {"name": status_name}}
    return {"select": {"name": status_name}}

# ---------- Upsert logic with stable Key ----------
def key_for(course_id: int, assignment_id: int) -> str:
    return f"{course_id}:{assignment_id}"

def find_by_key(key: str) -> Optional[str]:
    try:
        res = notion.databases.query(
            database_id=NOTION_DATABASE_ID,
            filter={"property": "Key", "rich_text": {"equals": key}},
            page_size=1,
        )
        results = res.get("results", [])
        if results:
            return results[0]["id"]
    except Exception as e:
        print(f"Notion query by Key failed: {e}")
    return None

def find_legacy(task: str, course_name: str, due_dt: Optional[datetime]) -> Optional[str]:
    # Only used once to migrate old rows that were created before Key existed
    filters: Dict[str, Any] = {
        "and": [
            {"property": "Task", "title": {"equals": task}},
            {"property": "Class", ("select" if property_is_select("Class") else "rich_text"): {"equals": course_name}},
        ]
    }
    if due_dt:
        filters["and"].append({"property": "Due", "date": {"equals": due_dt.isoformat()}})
    try:
        res = notion.databases.query(database_id=NOTION_DATABASE_ID, filter=filters, page_size=1)
        results = res.get("results", [])
        if results:
            return results[0]["id"]
    except Exception as e:
        print(f"Notion legacy query failed: {e}")
    return None

def build_props(task: str, course_name: str, type_str: str, due_dt: Optional[datetime],
                status_value: str, key: str) -> Dict[str, Any]:
    props: Dict[str, Any] = {
        "Task": {"title": [{"type": "text", "text": {"content": task[:1000]}}]},
        "Type": {"select": {"name": type_str}},
        "Status": status_prop_value(status_value),
        "Key": {"rich_text": [{"type": "text", "text": {"content": key}}]},
    }
    if property_is_select("Class"):
        props["Class"] = {"select": {"name": course_name}}
    else:
        props["Class"] = {"rich_text": [{"type": "text", "text": {"content": course_name}}]}
    if due_dt:
        props["Due"] = {"date": {"start": due_dt.isoformat()}}
        props["Time"] = {"rich_text": [{"type": "text", "text": {"content": time_str(due_dt)}}]}
    else:
        props["Time"] = {"rich_text": [{"type": "text", "text": {"content": ""}}]}
    return props

def upsert_page(props: Dict[str, Any], page_id: Optional[str]) -> None:
    payload = {"properties": props}
    try:
        if page_id:
            notion.pages.update(page_id=page_id, **payload)
        else:
            notion.pages.create(parent={"database_id": NOTION_DATABASE_ID}, **payload)
    except Exception as e:
        print(f"Notion upsert failed: {e}")

# ---------- Mapping ----------
EXAM_KEYWORDS = ["exam", "midterm", "final"]

def classify_type(assignment: Dict[str, Any]) -> str:
    if assignment.get("quiz_id"):
        return "Quiz"
    name = (assignment.get("name") or "").lower()
    if any(k in name for k in EXAM_KEYWORDS):
        return "Exam"
    return "Assignment"

def decide_status(submission: Optional[Dict[str, Any]], due_dt: Optional[datetime]) -> str:
    if submission:
        state = submission.get("workflow_state")
        submitted_at = submission.get("submitted_at")
        if state in {"graded", "submitted"} or submitted_at:
            return "Complete"
        if state in {"pending_review"}:
            return "In Progress"
    if due_dt and due_dt < datetime.now(tz):
        return "DNF"
    return "To do"

# ---------- Main ----------
def main() -> None:
    ensure_schema()

    horizon_start = datetime.now(tz) - timedelta(days=7)
    horizon_end = datetime.now(tz) + timedelta(days=LOOKAHEAD_DAYS)

    print("Fetching filtered courses...")
    courses = get_courses_by_id(COURSE_IDS)

    for course in courses:
        course_id = course["id"]
        course_name = OVERRIDES.get(course_id) or course.get("course_code") or course.get("name") or f"Course {course_id}"
        print(f"- {course_name} ({course_id})")

       # _ = get_course_quizzes(course_id)  # harmless if 404

        assignments = get_course_assignments(course_id)
        for a in assignments:
            due_at_local = parse_canvas_time(a.get("due_at"))
            if not due_at_local:
                continue
            if due_at_local < horizon_start or due_at_local > horizon_end:
                continue

            type_str = classify_type(a)
            submission = get_submission(course_id, a["id"])
            status_value = decide_status(submission, due_at_local)

            task_name = a.get("name") or f"Assignment {a['id']}"
            stable_key = key_for(course_id, a["id"])

            # 1) Try by stable Key
            page_id = find_by_key(stable_key)

            # 2) If not found, migrate an old row by Task+Class+Due then stamp Key
            if not page_id:
                page_id = find_legacy(task_name, course_name, due_at_local)

            props = build_props(
                task=task_name,
                course_name=course_name,
                type_str=type_str,
                due_dt=due_at_local,
                status_value=status_value,
                key=stable_key,
            )
            upsert_page(props, page_id)
            time.sleep(0.15)

    print("Sync complete.")

if __name__ == "__main__":
    main()
