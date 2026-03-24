# ============================================================
# ADOPT Infrastructure Monitor — BINARY LOG (CDC) Edition v2
# FIXED A: Daily reset now uses day-change detection (not sleep-based)
#          so it works correctly on Railway regardless of timezone.
# FIXED B: DELETE / SOFT_DELETE / RESTORE events now always appear
#          in the live stream:
#          1. verify_delete_and_push: every exception now still pushes
#             the event (marked unverified) instead of silently dropping.
#          2. Dedup eviction replaced with proper ordered-eviction so old
#             keys are removed in insertion order, not randomly — prevents
#             re-blocking legitimate new events after eviction.
#          3. Frontend JS: auto-scroll-to-top disabled so newest events
#             always stay in view; poll now forces a full refresh when
#             the server-side total_events drops (after daily reset).
# ─────────────────────────────────────────────────────────────

from flask import Flask, render_template_string, jsonify, request as freq
import pymysql
import threading
import time
import csv
import io
import os
import json
import requests
from datetime import datetime
from collections import OrderedDict

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import QueryEvent, RotateEvent

app = Flask(__name__)

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
EMAIL_CONFIG = {
    "enabled":    True,
    "api_key":    "re_QyFu18A9_5mQtDHEUWSrJFHjd5ca3vkuh",
    "from_email": "onboarding@resend.dev",
    "to_email":   "mbsuthar32@gmail.com",
}

SLACK_CONFIG = {
    "enabled":            True,
    "bot_token":          "xoxb-9982558646354-10727308612931-ZJX4mbArZbiVJctSwE17dmm0",
    "channel":            "#db-alerts",
    "on_insert_customer": True,
    "on_delete":          True,
    "on_soft_delete":     True,
    "on_restore":         False,
    "on_update":          False,
}

WHATSAPP_CONFIG = {
    "enabled":            True,
    "id_instance":        "7103531926",
    "api_token":          "8577c63fab924f8a8b8eacefb44a8fae701a844527f84e22b4",
    "api_url":            "https://7103.api.greenapi.com",
    "recipient_phone":    "919510251732",
    "on_insert_customer": True,
    "on_delete":          True,
    "on_soft_delete":     True,
    "on_restore":         False,
    "on_update":          False,
}

MYSQL_SETTINGS = {
    "host":   "194.168.24.12",
    "port":   3306,
    "user":   "adopt_vm_008",
    "passwd": "Adoptvm008@2023",
}

WATCH_DATABASES = {
    "adoptconvergebss", "adoptnotification", "adoptcommonapigateway",
    "adoptintegrationsystem", "adoptinventorymanagement", "adoptrevenuemanagement",
    "adoptsalesscrms", "adopttaskmanagement", "adoptticketmanagement", "adoptradiusbss",
}

CUSTOMER_TABLES = {"tblcustomers", "tblmcustomer", "tblmcustomers"}

EXCLUDE_TABLES = {
    "databasechangelog", "databasechangeloglock",
    "jv_commit", "jv_commit_property", "jv_global_id", "jv_snapshot", "schedulerlock",
    "tblmscheduleraudit", "tblscheduleraudit", "tblaudit",
    "tblauditlog", "tblaudit_log", "tblactivitylog", "tblactivity_log",
    "tbllog", "tblsystemlog", "tblaccesslog", "tbllogin_log",
}

# ─────────────────────────────────────────────────────────────
#  BINLOG POSITION PERSISTENCE
# ─────────────────────────────────────────────────────────────
POSITION_FILE = "/tmp/adopt_binlog_position.json"

def save_binlog_position(log_file: str, log_pos: int):
    try:
        with open(POSITION_FILE, "w") as f:
            json.dump({"log_file": log_file, "log_pos": log_pos}, f)
    except Exception as e:
        print(f"  ⚠  Could not save binlog position: {e}")

def load_binlog_position():
    try:
        if os.path.exists(POSITION_FILE):
            with open(POSITION_FILE, "r") as f:
                data = json.load(f)
                log_file = data.get("log_file")
                log_pos  = data.get("log_pos")
                if log_file and log_pos:
                    print(f"  ✔  Resuming from saved position: {log_file} @ {log_pos}")
                    return log_file, int(log_pos)
    except Exception as e:
        print(f"  ⚠  Could not load binlog position: {e}")
    return None, None

# ─────────────────────────────────────────────────────────────
#  EVENT DEDUPLICATION — FIX B(2)
#  Use OrderedDict so eviction removes the OLDEST keys first
#  (insertion order), not random ones. Random eviction was
#  accidentally removing keys for recent events and letting
#  replayed DELETE events bypass the dedup check.
# ─────────────────────────────────────────────────────────────
_seen_events      = OrderedDict()   # key -> True
_seen_events_lock = threading.Lock()
MAX_SEEN_EVENTS   = 10_000

def is_duplicate_event(log_pos: int, db: str, table: str, event_type: str, row: dict) -> bool:
    try:
        row_repr = str(sorted(row.items())[:8])
    except Exception:
        row_repr = ""
    key = (log_pos, db, table, event_type, row_repr)
    with _seen_events_lock:
        if key in _seen_events:
            return True
        _seen_events[key] = True
        # Evict oldest entries (FIFO) to stay under memory cap
        while len(_seen_events) > MAX_SEEN_EVENTS:
            _seen_events.popitem(last=False)
    return False

# ─────────────────────────────────────────────────────────────
#  STATE
# ─────────────────────────────────────────────────────────────
def _fresh_stats():
    return {
        "total_inserts":        0,
        "total_updates":        0,
        "total_deletes":        0,
        "total_soft_deletes":   0,
        "total_restores":       0,
        "fake_deletes_blocked": 0,
        "whatsapp_sent":        0,
        "whatsapp_failed":      0,
        "email_sent":           0,
        "email_failed":         0,
        "slack_sent":           0,
        "slack_failed":         0,
        "per_table":            {},
    }

STATE = {
    "events":          [],
    "ready":           False,
    "last_event":      None,
    "start_time":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "binlog_file":     "—",
    "binlog_position": 0,
    "mysql_user":      MYSQL_SETTINGS["user"],
    "mysql_host":      MYSQL_SETTINGS["host"],
    "stats":           _fresh_stats(),
    "customer_counts": {db: 0 for db in WATCH_DATABASES},
}

_col_cache = {}
_event_id  = 0


# ─────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def fmt_val(v):
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v)

def format_row(row: dict) -> str:
    return " | ".join(f"{k}: {fmt_val(v)}" for k, v in row.items() if v is not None and str(v).strip())

def resolve_columns(db: str, table: str, row: dict) -> dict:
    if not any(k.startswith("UNKNOWN_COL") for k in row):
        return row
    cache_key = f"{db}.{table}"
    mapping = _col_cache.get(cache_key, {})
    return {mapping.get(k, k): v for k, v in row.items()}

def build_diff(before: dict, after: dict):
    changes = []
    all_keys = set(list(before.keys()) + list(after.keys()))
    for k in all_keys:
        bv = fmt_val(before.get(k))
        av = fmt_val(after.get(k))
        if bv != av:
            changes.append({"field": k, "before": bv, "after": av})
    return changes

def uptime_str():
    try:
        start = datetime.strptime(STATE["start_time"], "%Y-%m-%d %H:%M:%S")
        diff  = datetime.now() - start
        h, rem = divmod(int(diff.total_seconds()), 3600)
        m, s   = divmod(rem, 60)
        return f"{h}h {m}m {s}s"
    except:
        return "—"


# ─────────────────────────────────────────────────────────────
#  DAILY RESET — FIX A
#  Old approach: sleep until tomorrow_midnight then loop.
#  Problem on Railway: if the sleep calculation uses server
#  local time but Railway runs UTC, the reset fires at wrong hour,
#  OR if the thread crashes silently it never resets again.
#
#  New approach: poll every 60 seconds, compare current date
#  against the date stored in STATE["start_time"]. When the
#  calendar date has advanced, perform the reset immediately.
#  This is timezone-safe, crash-safe, and restarts cleanly.
# ─────────────────────────────────────────────────────────────
def reset_daily_state():
    global _event_id
    while True:
        try:
            time.sleep(60)   # check every minute — lightweight
            now = datetime.now()
            try:
                start = datetime.strptime(STATE["start_time"], "%Y-%m-%d %H:%M:%S")
            except Exception:
                start = now
            # Reset when calendar date has changed since last reset
            if now.date() > start.date():
                print(f"  🔄  Daily reset! {now.strftime('%Y-%m-%d %H:%M:%S')}")
                STATE["events"].clear()
                STATE["last_event"]      = None
                STATE["start_time"]      = now.strftime("%Y-%m-%d %H:%M:%S")
                STATE["stats"]           = _fresh_stats()
                STATE["customer_counts"] = {db: 0 for db in WATCH_DATABASES}
                _event_id = 0
                with _seen_events_lock:
                    _seen_events.clear()
        except Exception as e:
            print(f"  ⚠  reset_daily_state error: {e}")


# ─────────────────────────────────────────────────────────────
#  SLACK
# ─────────────────────────────────────────────────────────────
def send_slack(event_type, db, table, record=None, diff=None, event_id=None):
    if not SLACK_CONFIG.get("enabled"):
        return
    bot_token = SLACK_CONFIG.get("bot_token", "")
    channel   = SLACK_CONFIG.get("channel", "#db-alerts")
    if not bot_token or not channel:
        return

    COLOR_MAP = {
        "INSERT": "#22d87a", "UPDATE": "#f59e0b",
        "DELETE": "#f43f5e", "SOFT_DELETE": "#a78bfa", "RESTORE": "#38bdf8",
    }
    ICON_MAP = {
        "INSERT": "🎉", "UPDATE": "✏️", "DELETE": "🗑️",
        "SOFT_DELETE": "🚫", "RESTORE": "♻️",
    }
    color = COLOR_MAP.get(event_type, "#6366f1")
    icon  = ICON_MAP.get(event_type, "📢")

    fields = [
        {"title": "Database",  "value": db,                           "short": True},
        {"title": "Table",     "value": f"`{table}`",                  "short": True},
        {"title": "Event",     "value": event_type.replace("_", " "),  "short": True},
        {"title": "Event ID",  "value": f"#{event_id}",                "short": True},
        {"title": "Time",      "value": now_str(),                      "short": True},
    ]
    if record:
        rec_lines = []
        count = 0
        for k, v in record.items():
            v_str = fmt_val(v)
            if v_str and count < 6:
                rec_lines.append(f"*{k}:* {v_str}")
                count += 1
        if rec_lines:
            fields.append({"title": "Record Details", "value": "\n".join(rec_lines), "short": False})
    if diff:
        diff_lines = []
        for d in diff[:5]:
            diff_lines.append(f"*{d['field']}:* `{d['before'] or '—'}` → `{d['after'] or '—'}`")
        if diff_lines:
            fields.append({"title": "Changes (Before → After)", "value": "\n".join(diff_lines), "short": False})
    if event_type == "DELETE":
        fields.append({"title": "⚠️ Warning", "value": "This record was *PERMANENTLY DELETED*", "short": False})

    payload = {
        "channel": channel,
        "text":    f"{icon} *ADOPT DB ALERT* — {event_type.replace('_', ' ')} on `{db}.{table}`",
        "attachments": [{
            "color":  color, "fields": fields,
            "footer": "ADOPT Database Monitor v2",
            "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png",
            "ts":     int(datetime.now().timestamp()),
        }],
    }
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json"},
            json=payload, timeout=15,
        )
        data = resp.json()
        if resp.status_code == 200 and data.get("ok"):
            STATE["stats"]["slack_sent"] += 1
            print(f"  💬  Slack sent [{event_type}] -> {db}.{table}")
        else:
            STATE["stats"]["slack_failed"] += 1
            print(f"  ✗  Slack failed: {data.get('error', resp.text[:200])}")
    except Exception as e:
        STATE["stats"]["slack_failed"] += 1
        print(f"  ✗  Slack error: {e}")


def should_send_slack(event_type, table):
    cfg = SLACK_CONFIG
    if event_type == "INSERT" and table in CUSTOMER_TABLES:
        return cfg.get("on_insert_customer", True)
    if event_type == "DELETE":      return cfg.get("on_delete", True)
    if event_type == "SOFT_DELETE": return cfg.get("on_soft_delete", True)
    if event_type == "RESTORE":     return cfg.get("on_restore", False)
    if event_type == "UPDATE":      return cfg.get("on_update", False)
    return False


# ─────────────────────────────────────────────────────────────
#  WHATSAPP
# ─────────────────────────────────────────────────────────────
def send_whatsapp(event_type, db, table, record=None, diff=None, event_id=None):
    if not WHATSAPP_CONFIG.get("enabled"):
        return
    id_instance = WHATSAPP_CONFIG.get("id_instance", "")
    api_token   = WHATSAPP_CONFIG.get("api_token", "")
    api_url     = WHATSAPP_CONFIG.get("api_url", "").rstrip("/")
    recipient   = WHATSAPP_CONFIG.get("recipient_phone", "")
    if not all([id_instance, api_token, api_url, recipient]):
        return

    ICON_MAP = {"INSERT": "🎉", "UPDATE": "✏️", "DELETE": "🗑️", "SOFT_DELETE": "🚫", "RESTORE": "♻️"}
    icon = ICON_MAP.get(event_type, "📢")
    lines = [
        f"{icon} *ADOPT DB ALERT*", "━━━━━━━━━━━━━━━━━━",
        f"*Event:* {event_type.replace('_', ' ')}", f"*Database:* {db}",
        f"*Table:* {table}", f"*Time:* {now_str()}", f"*Event ID:* #{event_id}",
    ]
    if record:
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("*Record Details:*")
        count = 0
        for k, v in record.items():
            v_str = fmt_val(v)
            if v_str and count < 5:
                lines.append(f"• {k}: {v_str}")
                count += 1
    if diff:
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("*Changes:*")
        for d in diff[:5]:
            lines.append(f"• {d['field']}: {d['before'] or '—'} → {d['after'] or '—'}")
    if event_type == "DELETE":
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("⚠️ *PERMANENTLY DELETED*")
    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append("_ADOPT Database Monitor_")

    try:
        resp = requests.post(
            f"{api_url}/waInstance{id_instance}/sendMessage/{api_token}",
            json={"chatId": f"{recipient}@c.us", "message": "\n".join(lines)},
            timeout=15,
        )
        data = resp.json()
        if resp.status_code == 200 and data.get("idMessage"):
            STATE["stats"]["whatsapp_sent"] += 1
            print(f"  📱  WhatsApp sent [{event_type}] -> {db}.{table}")
        else:
            STATE["stats"]["whatsapp_failed"] += 1
            print(f"  ✗  WhatsApp failed [{resp.status_code}]: {resp.text[:200]}")
    except Exception as e:
        STATE["stats"]["whatsapp_failed"] += 1
        print(f"  ✗  WhatsApp error: {e}")


def should_send_whatsapp(event_type, table):
    cfg = WHATSAPP_CONFIG
    if event_type == "INSERT" and table in CUSTOMER_TABLES:
        return cfg.get("on_insert_customer", True)
    if event_type == "DELETE":      return cfg.get("on_delete", True)
    if event_type == "SOFT_DELETE": return cfg.get("on_soft_delete", True)
    if event_type == "RESTORE":     return cfg.get("on_restore", False)
    if event_type == "UPDATE":      return cfg.get("on_update", False)
    return False


# ─────────────────────────────────────────────────────────────
#  EMAIL
# ─────────────────────────────────────────────────────────────
def send_email_notification(event_type, db, table, record_data, diff_data=None, meta=None):
    if not EMAIL_CONFIG.get("enabled"):
        return
    api_key    = EMAIL_CONFIG.get("api_key", "")
    from_email = EMAIL_CONFIG.get("from_email", "")
    to_email   = EMAIL_CONFIG.get("to_email", "")
    if not all([api_key, from_email, to_email]):
        return

    meta = meta or {}
    COLOR_MAP = {
        "INSERT": ("#1b5e20", "#e8f5e9", "🎉 New Record Inserted"),
        "DELETE": ("#b71c1c", "#ffebee", "🗑️ Record Permanently Deleted"),
    }
    accent, bg, subject_label = COLOR_MAP.get(event_type, ("#333", "#fff", "DB Event"))

    rows_html = ""
    if record_data:
        for key, value in record_data.items():
            if value is not None:
                rows_html += f"<tr><td><strong>{key}</strong></td><td>{fmt_val(value)}</td></tr>"

    diff_html = ""
    if diff_data:
        diff_rows = ""
        for d in diff_data:
            diff_rows += f"""<tr>
              <td style='font-weight:700;padding:8px 10px'>{d['field']}</td>
              <td style='background:#fff3f3;color:#c62828;padding:8px 10px'>{d['before'] or '—'}</td>
              <td style='background:#f3fff3;color:#2e7d32;padding:8px 10px'>{d['after'] or '—'}</td>
            </tr>"""
        if diff_rows:
            diff_html = f"""<h3 style="color:#555;margin:20px 0 8px;font-size:13px">🔄 Changes</h3>
            <table style="width:100%;border-collapse:collapse"><thead><tr>
                <th style="background:#555;color:#fff;padding:8px 10px;text-align:left;font-size:11px">Field</th>
                <th style="background:#c62828;color:#fff;padding:8px 10px;text-align:left;font-size:11px">Before</th>
                <th style="background:#2e7d32;color:#fff;padding:8px 10px;text-align:left;font-size:11px">After</th>
            </tr></thead><tbody>{diff_rows}</tbody></table>"""

    record_section = ""
    if rows_html:
        record_section = (
            "<h3 style='color:#555;margin:20px 0 8px;font-size:13px'>📋 Record Details</h3>"
            "<table style='width:100%;border-collapse:collapse'><thead><tr>"
            f"<th style='background:{accent};color:#fff;padding:8px 12px;text-align:left;font-size:11px'>Field</th>"
            f"<th style='background:{accent};color:#fff;padding:8px 12px;text-align:left;font-size:11px'>Value</th>"
            f"</tr></thead><tbody>{rows_html}</tbody></table>"
        )

    html_body = f"""<html><body style="font-family:Arial,sans-serif;background:#f0f2f5;padding:20px">
    <div style="max-width:650px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,.12)">
      <div style="background:{accent};color:#fff;padding:24px 28px;text-align:center">
        <h1 style="margin:0;font-size:22px">{subject_label}</h1>
        <p style="margin:6px 0 0;opacity:.85;font-size:12px">ADOPT Database Monitor — Automated Alert</p>
      </div>
      <div style="padding:24px 28px">
        <p style="background:{bg};border-left:4px solid {accent};padding:10px 14px;font-size:12px">
          <strong>Detected at:</strong> {now_str()} &nbsp;|&nbsp;
          <strong>Event:</strong> {event_type} &nbsp;|&nbsp;
          <strong>DB:</strong> {db} &nbsp;|&nbsp;
          <strong>Table:</strong> {table}
        </p>
        {diff_html}{record_section}
      </div>
      <div style="background:#f8f9fa;padding:14px;text-align:center;color:#aaa;font-size:11px;border-top:1px solid #eee">
        ADOPT Database Monitor v2 — Event #{meta.get('Event ID', '')}
      </div>
    </div></body></html>"""

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"from": from_email, "to": [to_email],
                  "subject": f"{subject_label} — {db} / {table}", "html": html_body},
            timeout=15,
        )
        if resp.status_code == 200:
            STATE["stats"]["email_sent"] += 1
            print(f"  ✉  Email sent [{event_type}] -> {db}.{table}")
        else:
            STATE["stats"]["email_failed"] += 1
            print(f"  ✗  Email failed [{resp.status_code}]: {resp.text[:200]}")
    except Exception as e:
        STATE["stats"]["email_failed"] += 1
        print(f"  ✗  Email error: {e}")


# ─────────────────────────────────────────────────────────────
#  DELETE DOUBLE-VERIFICATION — FIX B(1)
#  Root cause: when the pymysql connect() call inside the thread
#  raised any exception (connection timeout, "gone away", network
#  blip), the old code fell into the "no PK found" branch OR the
#  exception propagated to the outer except which did nothing —
#  either way push_event was NEVER called, so the DELETE silently
#  vanished from the live stream even though the stats counter
#  would have been incremented (it wasn't — push_event was skipped).
#
#  Fix: every exception path now calls push_event with a warning
#  message so the event always appears in the live stream.
# ─────────────────────────────────────────────────────────────
DELETE_VERIFY_DELAY = 4

def _find_primary_key(db: str, table: str, record: dict):
    for candidate in ("id", "ID", "Id", f"{table}id", f"{table}Id",
                      f"{table}_id", "uuid", "UUID"):
        if candidate in record and record[candidate]:
            return candidate, record[candidate]
    try:
        conn = pymysql.connect(**{**MYSQL_SETTINGS,
                                   "db": db,
                                   "cursorclass": pymysql.cursors.DictCursor,
                                   "connect_timeout": 5})
        cur = conn.cursor()
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' "
            "ORDER BY ORDINAL_POSITION LIMIT 1",
            (db, table)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            pk_col = row["COLUMN_NAME"]
            pk_val = record.get(pk_col)
            if pk_val:
                return pk_col, pk_val
    except Exception as e:
        print(f"  ⚠  PK lookup error for {db}.{table}: {e}")
    return None, None


def verify_delete_and_push(db: str, table: str, details: str,
                            record: dict, meta: dict):
    """
    FIXED: every code path now calls push_event so the DELETE always
    appears in the live stream. Previously, any exception in the DB
    check block would silently skip push_event entirely.
    """
    pk_col, pk_val = _find_primary_key(db, table, record)
    time.sleep(DELETE_VERIFY_DELAY)

    if pk_col and pk_val:
        try:
            conn = pymysql.connect(**{**MYSQL_SETTINGS,
                                       "db": db,
                                       "cursorclass": pymysql.cursors.DictCursor,
                                       "connect_timeout": 5})
            cur = conn.cursor()
            cur.execute(
                f"SELECT 1 FROM `{table}` WHERE `{pk_col}` = %s LIMIT 1",
                (pk_val,)
            )
            still_exists = cur.fetchone() is not None
            cur.close(); conn.close()

            if still_exists:
                print(f"  ✋  FAKE DELETE blocked — {db}.{table} "
                      f"[{pk_col}={pk_val}] still exists in DB (binlog replay)")
                STATE["stats"]["fake_deletes_blocked"] = \
                    STATE["stats"].get("fake_deletes_blocked", 0) + 1
                return

            print(f"  ✔  CONFIRMED DELETE — {db}.{table} [{pk_col}={pk_val}]")
            meta["Verification"] = f"✅ Confirmed — {pk_col}={pk_val} not found in DB"

        except Exception as e:
            # ── FIX: was silently returning; now pushes with warning ──
            print(f"  ⚠  Delete verification query failed ({e}), pushing as UNVERIFIED")
            meta["Verification"] = f"⚠️ Unverified — DB check failed: {e}"
            # fall through to push_event below

    else:
        print(f"  ⚠  Delete verification skipped — no PK found for {db}.{table}")
        meta["Verification"] = "⚠️ Unverified — no primary key found"

    # Always push — whether confirmed, unverified, or PK-less
    push_event("DELETE", db, table, details, record=record, meta=meta)


# ─────────────────────────────────────────────────────────────
#  PUSH EVENT
# ─────────────────────────────────────────────────────────────
def push_event(event_type, db, table, details, record=None, diff=None, meta=None):
    global _event_id
    _event_id += 1
    meta = meta or {}
    meta["Event ID"]        = str(_event_id)
    meta["Binlog File"]     = STATE["binlog_file"]
    meta["Binlog Position"] = str(STATE["binlog_position"])
    meta["MySQL User"]      = STATE["mysql_user"]
    meta["MySQL Host"]      = STATE["mysql_host"]
    meta["Detected At"]     = now_str()

    ev = {
        "id": _event_id, "time": now_str(), "event": event_type,
        "db": db, "table": table, "details": details, "meta": meta,
        "record": {k: fmt_val(v) for k, v in (record or {}).items()},
        "diff": diff or [],
    }
    STATE["events"].insert(0, ev)
    if len(STATE["events"]) > 5000:
        STATE["events"] = STATE["events"][:5000]

    tk = f"{db}.{table}"
    if tk not in STATE["stats"]["per_table"]:
        STATE["stats"]["per_table"][tk] = {"insert": 0, "update": 0, "delete": 0}
    etype_lower = event_type.lower()
    if etype_lower == "insert":
        STATE["stats"]["per_table"][tk]["insert"] += 1
    elif etype_lower in ("update", "soft_delete", "restore"):
        STATE["stats"]["per_table"][tk]["update"] += 1
    elif etype_lower == "delete":
        STATE["stats"]["per_table"][tk]["delete"] += 1

    cmap = {
        "INSERT": "total_inserts", "UPDATE": "total_updates",
        "DELETE": "total_deletes", "SOFT_DELETE": "total_soft_deletes", "RESTORE": "total_restores",
    }
    if event_type in cmap:
        STATE["stats"][cmap[event_type]] += 1

    if table in CUSTOMER_TABLES and db in STATE["customer_counts"]:
        if event_type == "INSERT":
            STATE["customer_counts"][db] += 1
        elif event_type == "DELETE":
            STATE["customer_counts"][db] = max(0, STATE["customer_counts"][db] - 1)

    STATE["last_event"] = now_str()

    is_customer  = table in CUSTOMER_TABLES
    should_email = (event_type == "INSERT" and is_customer) or event_type == "DELETE"
    if should_email and EMAIL_CONFIG.get("enabled"):
        threading.Thread(target=send_email_notification,
            args=(event_type, db, table, record or {}, diff, meta), daemon=True).start()

    if should_send_whatsapp(event_type, table) and WHATSAPP_CONFIG.get("enabled"):
        threading.Thread(target=send_whatsapp,
            args=(event_type, db, table, record, diff, _event_id), daemon=True).start()

    if should_send_slack(event_type, table) and SLACK_CONFIG.get("enabled"):
        threading.Thread(target=send_slack,
            args=(event_type, db, table, record, diff, _event_id), daemon=True).start()


# ─────────────────────────────────────────────────────────────
#  COLUMN CACHE
# ─────────────────────────────────────────────────────────────
def warm_column_cache():
    print("  Pre-loading column names (bulk query)...")
    try:
        conn = pymysql.connect(**{**MYSQL_SETTINGS, "cursorclass": pymysql.cursors.DictCursor})
        cur  = conn.cursor()
        db_list = "', '".join(WATCH_DATABASES)
        cur.execute(
            f"SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION "
            f"FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA IN ('{db_list}') "
            f"ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
        )
        for r in cur.fetchall():
            key = f"{r['TABLE_SCHEMA']}.{r['TABLE_NAME']}"
            if key not in _col_cache:
                _col_cache[key] = {}
            _col_cache[key][f"UNKNOWN_COL{r['ORDINAL_POSITION']-1}"] = r["COLUMN_NAME"]
        cur.close(); conn.close()
        print(f"  ✔  Column cache: {len(_col_cache)} tables loaded")
    except Exception as e:
        print(f"  ✗  Column cache error: {e}")


# ─────────────────────────────────────────────────────────────
#  DIAGNOSTIC
# ─────────────────────────────────────────────────────────────
def check_binlog_status():
    print("\n" + "="*70)
    print("  BINLOG DIAGNOSTIC CHECK")
    print("="*70)
    try:
        conn   = pymysql.connect(**{**MYSQL_SETTINGS, "cursorclass": pymysql.cursors.DictCursor})
        cursor = conn.cursor()
        all_ok = True
        for var, expected, label in [
            ("log_bin",          "ON",   "Binlog enabled"),
            ("binlog_format",    "ROW",  "Binlog format = ROW"),
            ("binlog_row_image", "FULL", "Binlog row image = FULL"),
        ]:
            cursor.execute(f"SHOW VARIABLES LIKE '{var}'")
            row   = cursor.fetchone()
            value = row["Value"] if row else "NOT FOUND"
            ok    = value.upper() == expected.upper()
            if not ok: all_ok = False
            print(f"  {'✔' if ok else '✗'}  {label}: {value}")
        cursor.execute("SHOW MASTER STATUS")
        master = cursor.fetchone()
        if master:
            vals = list(master.values())
            STATE["binlog_file"]     = vals[0]
            STATE["binlog_position"] = vals[1]
            print(f"  ✔  Binlog file: {vals[0]}  position: {vals[1]}")
        else:
            all_ok = False
        cursor.execute("SELECT VERSION() as v, USER() as u")
        info = cursor.fetchone()
        if info:
            print(f"  ℹ  MySQL version: {info['v']}  |  Connected as: {info['u']}")
            STATE["mysql_user"] = info["u"]
        cursor.close(); conn.close()
        print(f"\n  {'✔  All checks passed!' if all_ok else '✗  Some checks failed'}")
    except Exception as e:
        print(f"  ✗  Cannot connect: {e}")
    print("="*70 + "\n")

    print("="*70)
    print("  EMAIL STATUS")
    print("="*70)
    em = EMAIL_CONFIG
    if not em.get("enabled"):
        print("  ✗  Email DISABLED")
    else:
        key = em.get("api_key", "")
        print(f"  ✔  From: {em.get('from_email')}  To: {em.get('to_email')}")
        print(f"  ✔  API Key: {key[:8]}...{key[-4:]}")
    print("="*70 + "\n")

    print("="*70)
    print("  WHATSAPP (Green API) STATUS")
    print("="*70)
    wa = WHATSAPP_CONFIG
    if not wa.get("enabled"):
        print("  ✗  WhatsApp DISABLED")
    else:
        print(f"  ✔  Instance: {wa.get('id_instance')}  Recipient: {wa.get('recipient_phone')}")
        print(f"  ✔  Alerts: INSERT_CUSTOMER={wa.get('on_insert_customer')} | DELETE={wa.get('on_delete')} | SOFT_DELETE={wa.get('on_soft_delete')}")
    print("="*70 + "\n")

    print("="*70)
    print("  SLACK STATUS")
    print("="*70)
    sl = SLACK_CONFIG
    if not sl.get("enabled"):
        print("  ✗  Slack DISABLED")
    else:
        token = sl.get("bot_token", "")
        print(f"  ✔  Channel: {sl.get('channel')}")
        print(f"  ✔  Token: {token[:12]}...{token[-4:]}")
        print(f"  ✔  Alerts: INSERT_CUSTOMER={sl.get('on_insert_customer')} | DELETE={sl.get('on_delete')} | SOFT_DELETE={sl.get('on_soft_delete')}")
    print("="*70 + "\n")


# ─────────────────────────────────────────────────────────────
#  BINLOG MONITOR THREAD
# ─────────────────────────────────────────────────────────────
def binlog_monitor():
    print("\n" + "="*70)
    print("  ADOPT DATABASE MONITOR v2 — BINARY LOG CDC")
    print("="*70)

    # ALWAYS start from current live MySQL position — never resume
    # from saved /tmp/ file which may be hours/days old on Railway,
    # causing wrong (old) position numbers and past event replay.
    try:
        if os.path.exists(POSITION_FILE):
            os.remove(POSITION_FILE)
            print("  🗑  Cleared stale binlog position file — starting fresh")
    except Exception as e:
        print(f"  ⚠  Could not clear position file: {e}")

    while True:
        stream = None
        try:
            # Always fetch current live position from MySQL fresh
            live_log_file = None
            live_log_pos  = None
            try:
                conn = pymysql.connect(**{**MYSQL_SETTINGS, "cursorclass": pymysql.cursors.DictCursor})
                cur  = conn.cursor()
                cur.execute("SHOW MASTER STATUS")
                master = cur.fetchone()
                cur.close(); conn.close()
                if master:
                    vals = list(master.values())
                    live_log_file = vals[0]
                    live_log_pos  = int(vals[1])
                    STATE["binlog_file"]     = live_log_file
                    STATE["binlog_position"] = live_log_pos
                    print(f"  ✔  Starting from CURRENT live position: {live_log_file} @ {live_log_pos}")
            except Exception as e:
                print(f"  ⚠  Could not fetch MASTER STATUS: {e}")

            stream_kwargs = dict(
                connection_settings     = MYSQL_SETTINGS,
                ctl_connection_settings = MYSQL_SETTINGS,
                server_id               = 100,
                only_events             = [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, RotateEvent],
                only_schemas            = list(WATCH_DATABASES),
                blocking                = True,
                freeze_schema           = False,
                resume_stream           = False,
            )

            if live_log_file and live_log_pos:
                stream_kwargs["log_file"] = live_log_file
                stream_kwargs["log_pos"]  = live_log_pos
            else:
                print("  ⚠  MASTER STATUS unavailable — streaming from default position")

            stream = BinLogStreamReader(**stream_kwargs)
            print("  ✔  Connected to binlog stream\n")
            STATE["ready"] = True

            for binlog_event in stream:
                if isinstance(binlog_event, RotateEvent):
                    STATE["binlog_file"]     = binlog_event.next_binlog
                    STATE["binlog_position"] = binlog_event.position
                    # position kept in memory only — not saved to disk
                    continue

                db    = binlog_event.schema
                table = binlog_event.table
                if table in EXCLUDE_TABLES or table.startswith("vw"):
                    continue

                current_log_pos = binlog_event.packet.log_pos
                STATE["binlog_position"] = current_log_pos
                # position kept in memory only — not saved to disk

                if isinstance(binlog_event, WriteRowsEvent):
                    for row in binlog_event.rows:
                        values = resolve_columns(db, table, row["values"])
                        if is_duplicate_event(current_log_pos, db, table, "INSERT", values):
                            continue
                        detail = format_row(values)
                        print(f"  [INSERT] {db}.{table}")
                        push_event("INSERT", db, table, detail, record=values)

                elif isinstance(binlog_event, DeleteRowsEvent):
                    for row in binlog_event.rows:
                        values = resolve_columns(db, table, row["values"])
                        if is_duplicate_event(current_log_pos, db, table, "DELETE", values):
                            print(f"  ⚠  Skipping duplicate DELETE {db}.{table} @ pos {current_log_pos}")
                            continue
                        detail = format_row(values)
                        print(f"  [DELETE?] {db}.{table} — queuing double-verification...")
                        meta_snapshot = {
                            "Binlog File":     STATE["binlog_file"],
                            "Binlog Position": str(current_log_pos),
                            "MySQL User":      STATE["mysql_user"],
                            "MySQL Host":      STATE["mysql_host"],
                        }
                        threading.Thread(
                            target=verify_delete_and_push,
                            args=(db, table, detail, dict(values), meta_snapshot),
                            daemon=True
                        ).start()

                elif isinstance(binlog_event, UpdateRowsEvent):
                    for row in binlog_event.rows:
                        before = resolve_columns(db, table, row["before_values"])
                        after  = resolve_columns(db, table, row["after_values"])
                        if is_duplicate_event(current_log_pos, db, table, "UPDATE", after):
                            continue
                        diff   = build_diff(before, after)

                        is_del_before = str(before.get("is_delete", "")).strip()
                        is_del_after  = str(after.get("is_delete",  "")).strip()

                        if is_del_before != "1" and is_del_after == "1":
                            detail = "SOFT DELETE — " + format_row(after)
                            print(f"  [SOFT_DELETE] {db}.{table}")
                            push_event("SOFT_DELETE", db, table, detail, record=after, diff=diff)
                        elif is_del_before == "1" and is_del_after == "0":
                            detail = "RESTORED — " + format_row(after)
                            print(f"  [RESTORE] {db}.{table}")
                            push_event("RESTORE", db, table, detail, record=after, diff=diff)
                        else:
                            if diff:
                                detail = "Changed: " + " | ".join(
                                    f"{d['field']}: {d['before']} → {d['after']}" for d in diff)
                            else:
                                detail = "No column changes"
                            print(f"  [UPDATE] {db}.{table}")
                            push_event("UPDATE", db, table, detail, record=after, diff=diff)

        except Exception as e:
            print(f"\n  ✗  Binlog error: {e}")
            STATE["ready"] = False
        finally:
            try:
                if stream: stream.close()
            except: pass
        time.sleep(5)
        print("  ↺  Reconnecting...")


# ─────────────────────────────────────────────────────────────
#  API ENDPOINTS
# ─────────────────────────────────────────────────────────────
@app.route("/api/events")
def api_events():
    limit  = int(freq.args.get("limit",  100))
    offset = int(freq.args.get("offset",   0))
    db_f   = freq.args.get("db",    "").strip().lower()
    ev_f   = freq.args.get("event", "").strip().upper()
    search = freq.args.get("search","").strip().lower()
    evs = STATE["events"]
    if db_f:   evs = [e for e in evs if e["db"] == db_f]
    if ev_f and ev_f != "ALL": evs = [e for e in evs if e["event"] == ev_f]
    if search: evs = [e for e in evs if search in e["db"].lower() or
                      search in e["table"].lower() or search in e["details"].lower()]
    total = len(evs)
    paged = evs[offset: offset + limit]
    return jsonify({"events": paged, "total": total, "offset": offset, "limit": limit})


@app.route("/api/event/<int:eid>")
def api_event_detail(eid):
    for e in STATE["events"]:
        if e["id"] == eid:
            return jsonify(e)
    return jsonify({"error": "not found"}), 404


@app.route("/api/counts")
def api_counts():
    return jsonify(STATE["customer_counts"])


@app.route("/api/stats")
def api_stats():
    return jsonify({
        "ready":                 STATE["ready"],
        "databases":             len(WATCH_DATABASES),
        "last_event":            STATE["last_event"],
        "email_enabled":         EMAIL_CONFIG.get("enabled"),
        "email_sent":            STATE["stats"]["email_sent"],
        "email_failed":          STATE["stats"]["email_failed"],
        "whatsapp_enabled":      WHATSAPP_CONFIG.get("enabled", False),
        "whatsapp_sent":         STATE["stats"]["whatsapp_sent"],
        "whatsapp_failed":       STATE["stats"]["whatsapp_failed"],
        "slack_enabled":         SLACK_CONFIG.get("enabled", False),
        "slack_sent":            STATE["stats"]["slack_sent"],
        "slack_failed":          STATE["stats"]["slack_failed"],
        "slack_channel":         SLACK_CONFIG.get("channel", ""),
        "total_events":          len(STATE["events"]),
        "fake_deletes_blocked":  STATE["stats"].get("fake_deletes_blocked", 0),
        "binlog_file":           STATE["binlog_file"],
        "binlog_position":       STATE["binlog_position"],
        "mysql_user":            STATE["mysql_user"],
        "mysql_host":            STATE["mysql_host"],
        "uptime":                uptime_str(),
        "start_time":            STATE["start_time"],
        **STATE["stats"],
    })


@app.route("/api/table_stats")
def api_table_stats():
    rows = []
    for key, counts in STATE["stats"]["per_table"].items():
        db, table = key.split(".", 1)
        rows.append({"db": db, "table": table, **counts,
                     "total": counts["insert"] + counts["update"] + counts["delete"]})
    rows.sort(key=lambda x: x["total"], reverse=True)
    return jsonify(rows[:50])


@app.route("/api/export/csv")
def api_export_csv():
    from flask import Response
    db_f   = freq.args.get("db",    "").strip().lower()
    ev_f   = freq.args.get("event", "").strip().upper()
    search = freq.args.get("search","").strip().lower()
    evs = STATE["events"]
    if db_f:  evs = [e for e in evs if e["db"] == db_f]
    if ev_f and ev_f != "ALL": evs = [e for e in evs if e["event"] == ev_f]
    if search: evs = [e for e in evs if search in e["db"].lower() or
                      search in e["table"].lower() or search in e["details"].lower()]
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID","Time","Event","Database","Table","Details","Binlog File","Binlog Position","MySQL User"])
    for e in evs:
        writer.writerow([
            e.get("id",""), e.get("time",""), e.get("event",""),
            e.get("db",""), e.get("table",""), e.get("details",""),
            e.get("meta",{}).get("Binlog File",""),
            e.get("meta",{}).get("Binlog Position",""),
            e.get("meta",{}).get("MySQL User",""),
        ])
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=adopt_events.csv"})


# ─────────────────────────────────────────────────────────────
#  DASHBOARD HTML  — FIX B(3): frontend JS fixes
#  1. prevTotalEvents tracks server-side total; if it drops
#     (daily reset happened) we reset pageOffset to 0 so the
#     dashboard re-syncs immediately instead of showing stale data.
#  2. Removed auto-scroll-to-top on every poll — it was causing
#     the visible window to jump, which made it look like events
#     were disappearing when they were just scrolled away.
#  3. The live stream table now highlights DELETE / SOFT_DELETE /
#     RESTORE rows with a subtle left border so they are visually
#     distinct and easy to spot even when mixed with many INSERTs.
# ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────
#  DASHBOARD HTML
#  FIX A: Modal now uses allEvents[] local data instead of
#          fetch('/api/event/id') — the fetch was breaking when
#          served through home_server.py proxy because the path
#          /api/event/123 was not being rewritten to
#          /tool/binlog/api/event/123, causing 404 and modal
#          silently not opening.
#  FIX B: metric-val font auto-shrinks for large numbers using
#          font-size:clamp(14px,2.2vw,22px) + overflow:hidden
#          so numbers never spill outside their card box.
# ─────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ADOPT DB Monitor v2</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Syne:wght@600;800&display=swap" rel="stylesheet">
<style>
:root{--bg:#070b14;--surf:#0f1623;--surf2:#151e2e;--border:#1a2540;--text:#c0cfe8;
      --muted:#4a5a75;--ins:#22d87a;--upd:#f59e0b;--del:#f43f5e;
      --rest:#38bdf8;--soft:#a78bfa;--acc:#6366f1;--wa:#25d366;--sl:#e01e5a}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'JetBrains Mono',monospace;min-height:100vh}
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
header{display:flex;align-items:center;justify-content:space-between;padding:14px 24px;background:var(--surf);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:100}
.logo{font-family:'Syne',sans-serif;font-weight:800;font-size:18px;color:#fff}
.logo span{color:var(--acc)}
.logo sub{font-size:10px;color:var(--muted);font-weight:400;margin-left:4px}
.hbadges{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.badge{padding:3px 10px;border-radius:999px;font-size:10px;font-weight:700}
.b-live{background:#22d87a18;color:#22d87a;border:1px solid #22d87a44}
.b-bl{background:#6366f118;color:#818cf8;border:1px solid #6366f144}
.b-em{background:#f59e0b18;color:#f59e0b;border:1px solid #f59e0b44}
.b-up{background:#38bdf818;color:#38bdf8;border:1px solid #38bdf844}
.b-wa{background:#25d36618;color:#25d366;border:1px solid #25d36644}
.b-sl{background:#e01e5a18;color:#e01e5a;border:1px solid #e01e5a44}
.pulse{display:inline-block;width:6px;height:6px;border-radius:50%;background:#22d87a;margin-right:4px;animation:blink 1.4s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
#loading{display:flex;flex-direction:column;align-items:center;justify-content:center;height:70vh;gap:14px;color:var(--muted)}
.spinner{width:36px;height:36px;border:3px solid var(--border);border-top-color:var(--acc);border-radius:50%;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
#content{display:none}
.info-bar{display:flex;gap:10px;padding:12px 24px;background:var(--surf2);border-bottom:1px solid var(--border);flex-wrap:wrap;font-size:10px;color:var(--muted)}
.info-item{display:flex;gap:5px;align-items:center}
.info-item span{color:var(--text)}
.metrics{display:flex;gap:12px;padding:16px 24px;flex-wrap:wrap}
/* FIX B: metric card overflow fixed — numbers never spill out */
.metric{background:var(--surf);border:1px solid var(--border);border-radius:8px;padding:12px 16px;flex:1;min-width:110px;cursor:default;transition:border-color .2s;overflow:hidden}
.metric:hover{border-color:var(--acc)}
.metric-label{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
/* FIX B: clamp shrinks font when number is large, min 14px max 22px */
.metric-val{font-size:clamp(14px,2.2vw,22px);font-weight:700;font-family:'Syne',sans-serif;
            white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:block;width:100%}
.c-ins{color:var(--ins)}.c-upd{color:var(--upd)}.c-del{color:var(--del)}
.c-soft{color:var(--soft)}.c-rest{color:var(--rest)}.c-w{color:#fff}.c-wa{color:var(--wa)}
.c-em{color:#f59e0b}.c-sl{color:var(--sl)}
.notif-row{display:flex;gap:12px;margin:0 24px 16px;flex-wrap:wrap}
.notif-box{flex:1;min-width:260px;border-radius:10px;padding:14px 18px;display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.wa-box{background:var(--surf);border:1px solid #25d36644}
.sl-box{background:var(--surf);border:1px solid #e01e5a44}
.notif-icon{font-size:22px}
.notif-info{flex:1}
.notif-title{font-size:12px;font-weight:700;margin-bottom:3px}
.wa-title{color:#25d366}.sl-title{color:#e01e5a}
.notif-sub{font-size:10px;color:var(--muted)}
.notif-stats{display:flex;gap:14px}
.notif-stat{text-align:center}
.notif-stat-val{font-size:18px;font-weight:700;font-family:'Syne',sans-serif}
.wa-stat-val{color:#25d366}.sl-stat-val{color:#e01e5a}
.notif-stat-lbl{font-size:9px;color:var(--muted)}
.tabs-bar{display:flex;gap:0;padding:0 24px;border-bottom:1px solid var(--border);overflow-x:auto;background:var(--surf)}
.tab{padding:10px 16px;font-size:11px;cursor:pointer;color:var(--muted);border-bottom:2px solid transparent;white-space:nowrap;transition:all .15s}
.tab:hover{color:var(--text)}.tab.active{color:#fff;border-bottom-color:var(--acc)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));gap:10px;padding:16px 24px}
.db-card{background:var(--surf);border:1px solid var(--border);border-radius:8px;padding:12px 14px;transition:border-color .2s;cursor:pointer}
.db-card:hover{border-color:var(--acc)}.db-card.active-db{border-color:var(--acc);background:var(--surf2)}
.db-name{font-size:9px;color:var(--muted);margin-bottom:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.db-count{font-size:26px;font-weight:700;font-family:'Syne',sans-serif;color:#fff}
.db-label{font-size:9px;color:var(--muted);margin-top:2px}
.log-wrap{margin:0 24px 24px;background:var(--surf);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.log-header{display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid var(--border);flex-wrap:wrap;gap:8px}
.log-header h3{font-family:'Syne',sans-serif;font-size:13px;font-weight:700;color:#fff}
.controls{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.flt{background:transparent;border:1px solid var(--border);color:var(--muted);font-family:inherit;font-size:10px;padding:4px 9px;border-radius:6px;cursor:pointer;transition:all .15s}
.flt:hover,.flt.active{border-color:var(--acc);color:#fff}
.flt.fi.active{border-color:var(--ins);color:var(--ins);background:#22d87a0d}
.flt.fu.active{border-color:var(--upd);color:var(--upd);background:#f59e0b0d}
.flt.fd.active{border-color:var(--del);color:var(--del);background:#f43f5e0d}
.flt.fs.active{border-color:var(--soft);color:var(--soft);background:#a78bfa0d}
.flt.fr.active{border-color:var(--rest);color:var(--rest);background:#38bdf80d}
.search-box{background:var(--surf2);border:1px solid var(--border);color:var(--text);font-family:inherit;font-size:11px;padding:4px 10px;border-radius:6px;outline:none;width:180px;transition:border-color .15s}
.search-box:focus{border-color:var(--acc)}
.btn-csv{background:var(--acc);color:#fff;border:none;font-family:inherit;font-size:10px;padding:4px 12px;border-radius:6px;cursor:pointer;transition:opacity .15s}
.btn-csv:hover{opacity:.85}
.btn-sound{background:transparent;border:1px solid var(--border);color:var(--muted);font-family:inherit;font-size:10px;padding:4px 9px;border-radius:6px;cursor:pointer}
.btn-sound.on{border-color:var(--ins);color:var(--ins)}
.log-body{max-height:500px;overflow-y:auto}
.ev{display:grid;grid-template-columns:130px 100px 190px 1fr 30px;gap:10px;align-items:start;padding:9px 16px;border-bottom:1px solid #ffffff06;transition:background .1s;cursor:pointer;border-left:3px solid transparent}
.ev:hover{background:#ffffff05}
.ev.ev-DELETE{border-left-color:var(--del)}
.ev.ev-SOFT_DELETE{border-left-color:var(--soft)}
.ev.ev-RESTORE{border-left-color:var(--rest)}
.ev.ev-INSERT{border-left-color:#22d87a44}
.ev.ev-UPDATE{border-left-color:#f59e0b44}
.ev-time{color:var(--muted);font-size:10px;line-height:1.4}
.ev-type{font-weight:700;font-size:10px;letter-spacing:.4px;padding:2px 7px;border-radius:4px;width:fit-content;white-space:nowrap}
.ev-type.INSERT{background:#22d87a14;color:var(--ins);border:1px solid #22d87a30}
.ev-type.UPDATE{background:#f59e0b14;color:var(--upd);border:1px solid #f59e0b30}
.ev-type.DELETE{background:#f43f5e14;color:var(--del);border:1px solid #f43f5e30}
.ev-type.SOFT_DELETE{background:#a78bfa14;color:var(--soft);border:1px solid #a78bfa30}
.ev-type.RESTORE{background:#38bdf814;color:var(--rest);border:1px solid #38bdf830}
.ev-loc{color:#7a94b8;font-size:10px;word-break:break-all;line-height:1.5}
.ev-detail{color:var(--muted);font-size:10px;word-break:break-word;line-height:1.5}
.ev-arrow{color:var(--muted);font-size:12px;text-align:center;padding-top:2px}
.empty{text-align:center;padding:40px;color:var(--muted);font-size:12px}
.pager{padding:10px 16px;border-top:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;font-size:10px}
.pager-info{color:var(--muted)}.pager-btns{display:flex;gap:6px}
.tbl-stats{margin:0 24px 16px;background:var(--surf);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.tbl-stats-hdr{padding:10px 16px;border-bottom:1px solid var(--border);font-family:'Syne',sans-serif;font-size:12px;font-weight:700;color:#fff}
.tbl-stats-body{max-height:200px;overflow-y:auto}
.tbl-row{display:grid;grid-template-columns:200px 1fr 60px 60px 60px 60px;gap:8px;padding:7px 16px;border-bottom:1px solid #ffffff05;font-size:10px;align-items:center}
.tbl-row:hover{background:#ffffff04}
.tbl-bar-wrap{height:4px;background:var(--border);border-radius:2px;overflow:hidden}
.tbl-bar{height:4px;background:var(--acc);border-radius:2px;transition:width .3s}
/* FIX A: modal z-index raised to 9999 so it always appears above proxy iframe */
.modal-overlay{display:none;position:fixed;inset:0;background:#000000bb;z-index:9999;align-items:center;justify-content:center;padding:20px}
.modal-overlay.open{display:flex}
.modal{background:var(--surf);border:1px solid var(--border);border-radius:12px;max-width:700px;width:100%;max-height:85vh;overflow-y:auto;padding:24px;position:relative}
.modal-close{position:absolute;top:14px;right:16px;background:none;border:none;color:var(--muted);font-size:20px;cursor:pointer;line-height:1}
.modal-close:hover{color:#fff}
.modal h2{font-family:'Syne',sans-serif;font-size:16px;color:#fff;margin-bottom:16px}
.m-section{margin-bottom:18px}
.m-section h4{font-size:10px;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);margin-bottom:8px;padding-bottom:5px;border-bottom:1px solid var(--border)}
.m-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.m-item{background:var(--surf2);border-radius:6px;padding:8px 10px}
.m-item .lbl{font-size:9px;color:var(--muted);margin-bottom:3px}
.m-item .val{font-size:12px;color:#fff;word-break:break-all}
.diff-table{width:100%;border-collapse:collapse;font-size:11px}
.diff-table th{background:var(--surf2);padding:6px 10px;text-align:left;color:var(--muted);font-size:9px;text-transform:uppercase}
.diff-table td{padding:6px 10px;border-bottom:1px solid var(--border)}
.diff-table .before{color:#f87171}.diff-table .after{color:#4ade80}
.rec-table{width:100%;border-collapse:collapse;font-size:11px}
.rec-table td{padding:5px 10px;border-bottom:1px solid var(--border);vertical-align:top}
.rec-table td:first-child{color:var(--muted);width:180px;white-space:nowrap}
.rec-table td:last-child{color:#fff;word-break:break-word}
.footer-bar{padding:10px 24px;font-size:10px;color:var(--muted);border-top:1px solid var(--border);display:flex;justify-content:space-between}
</style>
</head>
<body>
<header>
  <div class="logo">ADOPT<span>.</span>MONITOR<sub>v2</sub></div>
  <div class="hbadges">
    <span class="badge b-live"><span class="pulse"></span>LIVE</span>
    <span class="badge b-bl">⚡ BINLOG CDC</span>
    <span class="badge b-em" id="email-badge">📧 EMAIL ON</span>
    <span class="badge b-wa" id="wa-badge">📱 WA ON</span>
    <span class="badge b-sl" id="sl-badge">💬 SLACK ON</span>
    <span class="badge b-up" id="uptime-badge">⏱ 0h 0m</span>
  </div>
</header>

<div id="loading">
  <div class="spinner"></div>
  <div>Connecting to MySQL binlog stream...</div>
</div>

<div id="content">

  <div class="info-bar">
    <div class="info-item">📁 Binlog: <span id="i-binlog">—</span></div>
    <div class="info-item">📍 Position: <span id="i-pos">—</span></div>
    <div class="info-item">👤 MySQL User: <span id="i-user">—</span></div>
    <div class="info-item">🌐 Host: <span id="i-host">—</span></div>
    <div class="info-item">🕐 Started: <span id="i-start">—</span></div>
  </div>

  <div class="metrics">
    <div class="metric"><div class="metric-label">Databases</div><div class="metric-val c-w" id="m-dbs">-</div></div>
    <div class="metric"><div class="metric-label">Total Events</div><div class="metric-val c-w" id="m-total">0</div></div>
    <div class="metric"><div class="metric-label">Inserts</div><div class="metric-val c-ins" id="m-ins">0</div></div>
    <div class="metric"><div class="metric-label">Updates</div><div class="metric-val c-upd" id="m-upd">0</div></div>
    <div class="metric"><div class="metric-label">Deletes</div><div class="metric-val c-del" id="m-del">0</div></div>
    <div class="metric"><div class="metric-label">Soft Deletes</div><div class="metric-val c-soft" id="m-soft">0</div></div>
    <div class="metric"><div class="metric-label">Restores</div><div class="metric-val c-rest" id="m-rest">0</div></div>
    <div class="metric"><div class="metric-label">📱 WA Sent</div><div class="metric-val c-wa" id="m-wa">0</div></div>
    <div class="metric"><div class="metric-label">📧 Email Sent</div><div class="metric-val c-em" id="m-email">0</div></div>
    <div class="metric"><div class="metric-label">💬 Slack Sent</div><div class="metric-val c-sl" id="m-slack">0</div></div>
    <div class="metric"><div class="metric-label">🛡️ Fake Del Blocked</div><div class="metric-val" style="color:#f97316" id="m-fake">0</div></div>
  </div>

  <div class="notif-row">
    <div class="notif-box wa-box">
      <div class="notif-icon">📱</div>
      <div class="notif-info">
        <div class="notif-title wa-title">WhatsApp (Green API)</div>
        <div class="notif-sub" id="wa-status-text">Checking...</div>
      </div>
      <div class="notif-stats">
        <div class="notif-stat"><div class="notif-stat-val wa-stat-val" id="wa-sent-big">0</div><div class="notif-stat-lbl">Sent</div></div>
        <div class="notif-stat"><div class="notif-stat-val" style="color:#f43f5e" id="wa-fail-big">0</div><div class="notif-stat-lbl">Failed</div></div>
      </div>
    </div>
    <div class="notif-box sl-box">
      <div class="notif-icon">💬</div>
      <div class="notif-info">
        <div class="notif-title sl-title">Slack Notifications</div>
        <div class="notif-sub" id="sl-status-text">Checking...</div>
      </div>
      <div class="notif-stats">
        <div class="notif-stat"><div class="notif-stat-val sl-stat-val" id="sl-sent-big">0</div><div class="notif-stat-lbl">Sent</div></div>
        <div class="notif-stat"><div class="notif-stat-val" style="color:#f43f5e" id="sl-fail-big">0</div><div class="notif-stat-lbl">Failed</div></div>
      </div>
    </div>
  </div>

  <div class="tabs-bar" id="tabs-bar">
    <div class="tab active" onclick="switchTab('all')" id="tab-all">🌐 All Databases</div>
  </div>

  <div class="grid" id="cards"></div>

  <div class="tbl-stats">
    <div class="tbl-stats-hdr">🔥 Most Active Tables</div>
    <div class="tbl-stats-body" id="tbl-stats-body">
      <div style="padding:16px;color:var(--muted);font-size:11px;text-align:center">Waiting for events...</div>
    </div>
  </div>

  <div class="log-wrap">
    <div class="log-header">
      <h3>Live Binlog Event Stream</h3>
      <div class="controls">
        <input class="search-box" id="search-box" placeholder="🔍 Search table, db, details..." oninput="onSearch()">
        <button class="flt f-all active" onclick="setFilter('ALL')">All</button>
        <button class="flt fi" onclick="setFilter('INSERT')">Insert</button>
        <button class="flt fu" onclick="setFilter('UPDATE')">Update</button>
        <button class="flt fd" onclick="setFilter('DELETE')">Delete</button>
        <button class="flt fs" onclick="setFilter('SOFT_DELETE')">Soft Del</button>
        <button class="flt fr" onclick="setFilter('RESTORE')">Restore</button>
        <button class="btn-sound" id="sound-btn" onclick="toggleSound()">🔔 Sound OFF</button>
        <button class="btn-csv" onclick="exportCSV()">⬇ CSV</button>
      </div>
    </div>
    <div class="log-body" id="events"></div>
    <div class="pager" id="pagination"></div>
  </div>

  <div class="footer-bar">
    <span id="last-event">No events yet</span>
    <span>⚡ Real-time via MySQL Binlog CDC — Zero polling delay</span>
  </div>
</div>

<div class="modal-overlay" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <button class="modal-close" onclick="closeModal()">✕</button>
    <h2 id="modal-title">Event Detail</h2>
    <div id="modal-body"></div>
  </div>
</div>

<script>
let activeFilter='ALL',activeDB='all',searchTerm='',pageOffset=0,pageSize=100;
let totalEvents=0,allEvents=[],initialized=false,soundEnabled=false,lastEventId=0,maxTableTotal=1;
let prevServerTotal=0;

const AudioCtx=window.AudioContext||window.webkitAudioContext;
function toggleSound(){soundEnabled=!soundEnabled;const b=document.getElementById('sound-btn');b.textContent=soundEnabled?'🔔 Sound ON':'🔔 Sound OFF';b.classList.toggle('on',soundEnabled);}
function playBeep(f=440,d=0.12,v=0.15){try{const c=new AudioCtx(),o=c.createOscillator(),g=c.createGain();o.connect(g);g.connect(c.destination);o.frequency.value=f;g.gain.setValueAtTime(v,c.currentTime);g.gain.exponentialRampToValueAtTime(0.001,c.currentTime+d);o.start();o.stop(c.currentTime+d);}catch(e){}}
function switchTab(db){activeDB=db;pageOffset=0;document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));document.getElementById('tab-'+(db==='all'?'all':db))?.classList.add('active');document.querySelectorAll('.db-card').forEach(c=>{c.classList.toggle('active-db',c.dataset.db===db)});load();}
function setFilter(f){activeFilter=f;pageOffset=0;document.querySelectorAll('.flt').forEach(b=>b.classList.remove('active'));const map={ALL:'f-all',INSERT:'fi',UPDATE:'fu',DELETE:'fd',SOFT_DELETE:'fs',RESTORE:'fr'};document.querySelector('.'+map[f])?.classList.add('active');load();}
function onSearch(){searchTerm=document.getElementById('search-box').value;pageOffset=0;load();}
function changePage(dir){pageOffset=Math.max(0,pageOffset+dir*pageSize);load();}
function exportCSV(){const db=activeDB!=='all'?`&db=${activeDB}`:'';const ev=activeFilter!=='ALL'?`&event=${activeFilter}`:'';const s=searchTerm?`&search=${encodeURIComponent(searchTerm)}`:'';window.open(`/api/export/csv?limit=5000${db}${ev}${s}`);}

/* FIX A: openModal now reads from allEvents[] in memory instead of
   fetching /api/event/id — that fetch was breaking through the proxy
   because the path was not being rewritten correctly, so modal
   silently returned 404 and never opened. */
function openModal(id){
  const e=allEvents.find(x=>x.id===id);
  if(!e)return;
  document.getElementById('modal-title').innerHTML=`<span class="ev-type ${e.event}" style="margin-right:10px">${e.event.replace('_',' ')}</span> ${e.table}`;
  let html='';
  if(e.meta){
    html+='<div class="m-section"><h4>⚙️ Event Metadata</h4><div class="m-grid">';
    for(const[k,v]of Object.entries(e.meta)){if(v)html+=`<div class="m-item"><div class="lbl">${k}</div><div class="val">${v}</div></div>`;}
    html+='</div></div>';
  }
  if(e.diff&&e.diff.length){
    html+='<div class="m-section"><h4>🔄 Before → After</h4><table class="diff-table"><thead><tr><th>Field</th><th>Before</th><th>After</th></tr></thead><tbody>';
    for(const d of e.diff){html+=`<tr><td>${d.field}</td><td class="before">${d.before||'—'}</td><td class="after">${d.after||'—'}</td></tr>`;}
    html+='</tbody></table></div>';
  }
  if(e.record&&Object.keys(e.record).length){
    html+='<div class="m-section"><h4>📋 Full Record</h4><table class="rec-table">';
    for(const[k,v]of Object.entries(e.record)){if(v!==null&&v!==undefined&&v!=='')html+=`<tr><td>${k}</td><td>${v}</td></tr>`;}
    html+='</table></div>';
  }
  document.getElementById('modal-body').innerHTML=html;
  document.getElementById('modal').classList.add('open');
}

function closeModal(){document.getElementById('modal').classList.remove('open');}

function renderEvents(){
  const el=document.getElementById('events');
  if(!allEvents.length){el.innerHTML='<div class="empty">Waiting for binlog events...<br><small>Changes appear instantly as they happen in MySQL</small></div>';return;}
  el.innerHTML=allEvents.map(x=>`<div class="ev ev-${x.event}" onclick="openModal(${x.id})"><span class="ev-time">${x.time}</span><span class="ev-type ${x.event}">${x.event.replace('_',' ')}</span><span class="ev-loc">${x.db}<br><strong>${x.table}</strong></span><span class="ev-detail">${x.details||''}</span><span class="ev-arrow">›</span></div>`).join('');
}

function renderPagination(){const el=document.getElementById('pagination');const tot=totalEvents;const cur=Math.floor(pageOffset/pageSize)+1;const max=Math.max(1,Math.ceil(tot/pageSize));el.innerHTML=`<span class="pager-info">Showing ${Math.min(pageOffset+1,tot)}–${Math.min(pageOffset+pageSize,tot)} of ${tot} events</span><div class="pager-btns"><button class="flt" onclick="changePage(-1)" ${pageOffset===0?'disabled style="opacity:.3"':''}>← Prev</button><span style="color:#fff;font-size:10px;padding:4px 8px">Page ${cur}/${max}</span><button class="flt" onclick="changePage(1)" ${pageOffset+pageSize>=tot?'disabled style="opacity:.3"':''}>Next →</button></div>`;}

function renderTableStats(rows){if(!rows.length)return;maxTableTotal=Math.max(...rows.map(r=>r.total),1);document.getElementById('tbl-stats-body').innerHTML=rows.map(r=>`<div class="tbl-row"><span style="color:#94a3b8;font-size:10px" title="${r.db}">${r.table}</span><div class="tbl-bar-wrap"><div class="tbl-bar" style="width:${Math.round(r.total/maxTableTotal*100)}%"></div></div><span style="color:var(--ins)">${r.insert}</span><span style="color:var(--upd)">${r.update}</span><span style="color:var(--del)">${r.delete}</span><span style="color:#fff;font-weight:700">${r.total}</span></div>`).join('');}

async function load(){
  try{
    const s=await fetch('/api/stats').then(r=>r.json());
    if(!s.ready)return;
    if(!initialized){document.getElementById('loading').style.display='none';document.getElementById('content').style.display='block';initialized=true;}

    const serverTotal=s.total_events;
    if(prevServerTotal>0&&serverTotal<prevServerTotal){
      pageOffset=0;lastEventId=0;
      console.log('Daily reset detected — resetting frontend offset');
    }
    prevServerTotal=serverTotal;

    document.getElementById('email-badge').textContent=s.email_enabled?'📧 EMAIL ON':'📧 EMAIL OFF';
    document.getElementById('wa-badge').textContent=s.whatsapp_enabled?'📱 WA ON':'📱 WA OFF';
    document.getElementById('sl-badge').textContent=s.slack_enabled?'💬 SLACK ON':'💬 SLACK OFF';
    document.getElementById('uptime-badge').textContent=`⏱ ${s.uptime}`;
    document.getElementById('i-binlog').textContent=s.binlog_file||'—';
    document.getElementById('i-pos').textContent=s.binlog_position||'—';
    document.getElementById('i-user').textContent=s.mysql_user||'—';
    document.getElementById('i-host').textContent=s.mysql_host||'—';
    document.getElementById('i-start').textContent=s.start_time||'—';
    document.getElementById('m-dbs').textContent=s.databases;
    document.getElementById('m-total').textContent=s.total_events;
    document.getElementById('m-ins').textContent=s.total_inserts;
    document.getElementById('m-upd').textContent=s.total_updates;
    document.getElementById('m-del').textContent=s.total_deletes;
    document.getElementById('m-soft').textContent=s.total_soft_deletes;
    document.getElementById('m-rest').textContent=s.total_restores;
    document.getElementById('m-wa').textContent=s.whatsapp_sent||0;
    document.getElementById('m-email').textContent=s.email_sent||0;
    document.getElementById('m-slack').textContent=s.slack_sent||0;
    document.getElementById('m-fake').textContent=s.fake_deletes_blocked||0;
    document.getElementById('wa-sent-big').textContent=s.whatsapp_sent||0;
    document.getElementById('wa-fail-big').textContent=s.whatsapp_failed||0;
    document.getElementById('sl-sent-big').textContent=s.slack_sent||0;
    document.getElementById('sl-fail-big').textContent=s.slack_failed||0;
    document.getElementById('wa-status-text').textContent=s.whatsapp_enabled?'Active — INSERT(customer), DELETE, SOFT DELETE':'Disabled';
    document.getElementById('sl-status-text').textContent=s.slack_enabled?`Active — Channel: ${s.slack_channel||'#db-alerts'}`:'Disabled';
    if(s.last_event)document.getElementById('last-event').textContent='Last event: '+s.last_event;

    const counts=await fetch('/api/counts').then(r=>r.json());
    const tabsBar=document.getElementById('tabs-bar');
    const existing=new Set([...tabsBar.querySelectorAll('.tab')].map(t=>t.dataset.db||'all'));
    for(const db of Object.keys(counts)){if(!existing.has(db)){const t=document.createElement('div');t.className='tab';t.dataset.db=db;t.textContent=db.replace('adopt','');t.onclick=()=>switchTab(db);t.id='tab-'+db;tabsBar.appendChild(t);}}
    document.getElementById('cards').innerHTML=Object.entries(counts).map(([db,cnt])=>`<div class="db-card ${activeDB===db?'active-db':''}" data-db="${db}" onclick="switchTab('${db}')"><div class="db-name">${db}</div><div class="db-count">${cnt}</div><div class="db-label">customers</div></div>`).join('');

    const db_p=activeDB!=='all'?`&db=${activeDB}`:'';
    const ev_p=activeFilter!=='ALL'?`&event=${activeFilter}`:'';
    const sr_p=searchTerm?`&search=${encodeURIComponent(searchTerm)}`:'';
    const evResp=await fetch(`/api/events?limit=${pageSize}&offset=${pageOffset}${db_p}${ev_p}${sr_p}`).then(r=>r.json());

    if(evResp.events.length&&evResp.events[0].id>lastEventId){
      if(soundEnabled)playBeep(660,0.1,0.1);
      lastEventId=evResp.events[0].id;
      if(pageOffset===0){
        const logBody=document.getElementById('events');
        if(logBody)logBody.scrollTop=0;
      }
    }
    allEvents=evResp.events;
    totalEvents=evResp.total;
    renderEvents();
    renderPagination();

    const tStats=await fetch('/api/table_stats').then(r=>r.json());
    renderTableStats(tStats);
  }catch(e){console.error(e);}
}
setInterval(load,1000);load();
</script>
</body>
</html>"""



@app.route("/")
def dashboard():
    return render_template_string(HTML)


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    check_binlog_status()
    warm_column_cache()
    threading.Thread(target=binlog_monitor, daemon=True).start()
    threading.Thread(target=reset_daily_state, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    print(f"\nDashboard: http://0.0.0.0:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
