import os, sys, time, json, pathlib, requests, re
from datetime import datetime, timedelta, timezone
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
    print("Missing env vars. Check NOTION_TOKEN, NOTION_DATABASE_ID, CANVAS_BASE_URL, CANVAS_TOKEN")
    sys.exit(1)
if not COURSE_IDS:
    print("COURSE_IDS is empty. Add your course IDs to .env")
    sys.exit(1)

# ---------- Clients ----------
notion = NotionClient(auth=NOTION_TOKEN)
tz = pytz.timezone(TIMEZONE)
CANVAS_HEADERS = {"Authorization": f"Bearer {CANVAS_TOKEN}"}

# ---------- Local state ----------
STATE_DIR = pathlib.Path(".state")
STATE_DIR.mkdir(exist_ok=True)
LAST_SYNC_FILE = STATE_DIR / "last_sync.txt"

def now_local() -> datetime:
    return datetime.now(tz)

def load_last_sync() -> Optional[datetime]:
    if LAST_SYNC_FILE.exists():
        s = LAST_SYNC_FILE.read_text().strip()
        if s:
            try:
                return dateparser.parse(s).astimezone(tz)
            except Exception:
                return None
    return None

def save_last_sync(dt: datetime) -> None:
    LAST_SYNC_FILE.write_text(dt.astimezone(timezone.utc).isoformat())

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

def get_course(course_id: int) -> Optional[Dict[str, Any]]:
    url = f"{CANVAS_BASE_URL}/api/v1/courses/{course_id}"
    try:
        r = requests.get(url, headers=CANVAS_HEADERS, timeout=20)
        if r.status_code == 200:
            return r.json()
    except requests.RequestException:
        pass
    return None

def get_course_assignments_with_submissions(course_id: int) -> List[Dict[str, Any]]:
    # Quizzes show up here as assignments with quiz_id
    url = f"{CANVAS_BASE_URL}/api/v1/courses/{course_id}/assignments"
    return canvas_get(url, {"per_page": 100, "include[]": ["submission"]})

def get_submissions_changed_since(course_id: int, since_iso: str) -> List[Dict[str, Any]]:
    # Only submissions that changed since last run
    url = f"{CANVAS_BASE_URL}/api/v1/courses/{course_id}/students/submissions"
    params = {
        "student_ids[]": "self",
        "per_page": 100,
        "include[]": ["assignment"],
        "submitted_since": since_iso,
        "graded_since": since_iso,
    }
    return canvas_get(url, params)

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

# ---------- Notion schema ----------
_DB_SCHEMA: Dict[str, Any] = {}
_SELECT_CACHE: Dict[str, bool] = {}
_STATUS_IS_STATUS_TYPE = True

def load_db_schema() -> Dict[str, Any]:
    global _DB_SCHEMA
    if not _DB_SCHEMA:
        _DB_SCHEMA = notion.databases.retrieve(NOTION_DATABASE_ID)
    return _DB_SCHEMA

def db_has_property(name: str) -> bool:
    return name in load_db_schema().get("properties", {})

def ensure_property(name: str, ptype: str, options: Optional[List[str]] = None) -> None:
    db = load_db_schema()
    props = db.get("properties", {})
    if name in props:
        existing_type = props[name].get("type")
        if existing_type != ptype:
            print(f"Warning: property {name} is {existing_type} not {ptype}. Adapting at runtime.")
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
    elif ptype == "url":
        new_prop = {"url": {}}
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
    ensure_property("Task", "title")
    ensure_property("Class", "select")
    ensure_property("Type", "select", ["Assignment", "Quiz", "Exam"])
    ensure_property("Due", "date")
    ensure_property("Time", "rich_text")
    ensure_property("Notes", "rich_text")  # for assignment descriptions
    ensure_property("Submission Link", "url")  # for assignment links
    if db_has_property("Status"):
        t = load_db_schema()["properties"]["Status"]["type"]
        global _STATUS_IS_STATUS_TYPE
        _STATUS_IS_STATUS_TYPE = (t == "status")
    else:
        ensure_property("Status", "status", ["To do", "In Progress", "Complete", "DNF"])
        _STATUS_IS_STATUS_TYPE = True
    ensure_property("Key", "rich_text")  # hidden dedupe key

def property_is_select(prop: str) -> bool:
    if prop in _SELECT_CACHE:
        return _SELECT_CACHE[prop]
    p = load_db_schema()["properties"].get(prop)
    is_select = p and p.get("type") in ["select", "multi_select"]
    _SELECT_CACHE[prop] = bool(is_select)
    return bool(is_select)

def status_prop_value(status_name: str) -> Dict[str, Any]:
    if _STATUS_IS_STATUS_TYPE:
        return {"status": {"name": status_name}}
    return {"select": {"name": status_name}}

# ---------- Upsert helpers ----------
def key_for(course_id: int, assignment_id: int) -> str:
    return f"{course_id}:{assignment_id}"

def find_by_key(key: str) -> Optional[str]:
    # ensure schema before querying by Key
    if not db_has_property("Key"):
        ensure_schema()
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

def upsert_page(props: Dict[str, Any], page_id: Optional[str]) -> None:
    # props is the raw Notion properties dict. Do not wrap again.
    try:
        if page_id:
            notion.pages.update(page_id=page_id, properties=props)
        else:
            notion.pages.create(parent={"database_id": NOTION_DATABASE_ID}, properties=props)
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

def decide_status_from_submission(sub: Optional[Dict[str, Any]], due_dt: Optional[datetime]) -> str:
    if sub:
        state = sub.get("workflow_state")
        submitted_at = sub.get("submitted_at")
        if state in {"graded", "submitted"} or submitted_at:
            return "Complete"
        if state in {"pending_review"}:
            return "In Progress"
    if due_dt and due_dt < now_local():
        return "DNF"
    return "To do"

# ---------- Delta sync for recent submission changes ----------
def sync_status_deltas_since(course_id: int, since_dt: datetime) -> None:
    since_iso = since_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        changed = get_submissions_changed_since(course_id, since_iso)
    except requests.HTTPError as e:
        print(f"Delta fetch failed for {course_id}: {e}")
        return

    for s in changed:
        a = s.get("assignment") or {}
        aid = a.get("id")
        if not aid:
            continue
        key = key_for(course_id, aid)
        page_id = find_by_key(key)
        if not page_id:
            continue  # will be created during full scan
        due_dt = parse_canvas_time(a.get("due_at"))
        status_value = decide_status_from_submission(s, due_dt)
        props: Dict[str, Any] = {
            "Status": status_prop_value(status_value),
        }
        if due_dt:
            props["Due"] = {"date": {"start": due_dt.isoformat()}}
            props["Time"] = {"rich_text": [{"type": "text", "text": {"content": time_str(due_dt)}}]}
        
        # Add Submission Link field with assignment URL
        assignment_url = f"{CANVAS_BASE_URL}/courses/{course_id}/assignments/{aid}"
        props["Submission Link"] = {"url": assignment_url}
        
        upsert_page(props, page_id)

# ---------- Full scan within window ----------
def full_scan(course_id: int, course_name: str, horizon_start: datetime, horizon_end: datetime) -> None:
    assignments = get_course_assignments_with_submissions(course_id)
    for a in assignments:
        due_at_local = parse_canvas_time(a.get("due_at"))
        if not due_at_local:
            continue
        if due_at_local < horizon_start or due_at_local > horizon_end:
            continue

        type_str = classify_type(a)
        submission = a.get("submission")
        status_value = decide_status_from_submission(submission, due_at_local)

        task_name = a.get("name") or f"Assignment {a['id']}"
        key = key_for(course_id, a["id"])
        
        # Extract assignment description
        description = a.get("description", "")
        if description:
            # Clean up HTML tags and convert to plain text for better readability
            description = re.sub(r'<[^>]+>', '', description)  # Remove HTML tags
            description = description.strip()
        
        # build properties
        props: Dict[str, Any] = {
            "Task": {"title": [{"type": "text", "text": {"content": task_name[:1000]}}]},
            "Type": {"select": {"name": type_str}},
            "Status": status_prop_value(status_value),
            "Key": {"rich_text": [{"type": "text", "text": {"content": key}}]},
            "Due": {"date": {"start": due_at_local.isoformat()}},
            "Time": {"rich_text": [{"type": "text", "text": {"content": time_str(due_at_local)}}]},
        }
        
        # Add Notes field with assignment description
        if description:
            props["Notes"] = {"rich_text": [{"type": "text", "text": {"content": description[:2000]}}]}
        
        # Add Submission Link field with assignment URL
        assignment_url = f"{CANVAS_BASE_URL}/courses/{course_id}/assignments/{a['id']}"
        props["Submission Link"] = {"url": assignment_url}
        
        if property_is_select("Class"):
            props["Class"] = {"select": {"name": course_name}}
        else:
            props["Class"] = {"rich_text": [{"type": "text", "text": {"content": course_name}}]}

        page_id = find_by_key(key)
        upsert_page(props, page_id)
        time.sleep(0.1)

# ---------- Main ----------
def main() -> None:
    ensure_schema()

    horizon_start = now_local() - timedelta(days=7)
    horizon_end = now_local() + timedelta(days=LOOKAHEAD_DAYS)
    last_sync = load_last_sync()

    print("Fetching filtered courses...")
    for cid in COURSE_IDS:
        course = get_course(cid)
        if not course:
            print(f"- Could not load course {cid}")
            continue
        course_name = OVERRIDES.get(cid) or course.get("course_code") or course.get("name") or f"Course {cid}"
        print(f"- {course_name} ({cid})")

        if last_sync:
            sync_status_deltas_since(cid, last_sync)

        full_scan(cid, course_name, horizon_start, horizon_end)

    save_last_sync(now_local())
    print("Sync complete.")

if __name__ == "__main__":
    main()
