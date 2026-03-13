from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import sqlite3
import time
import threading
import os

DB_PATH = os.environ.get("AGENT_DB", "agent.db")
WEBHOOK_KEY = os.environ.get("WEBHOOK_KEY", "change-me-long-random-secret")
GRACE_BEFORE_MIN = int(os.environ.get("GRACE_BEFORE_MIN", "10"))
GRACE_AFTER_MIN = int(os.environ.get("GRACE_AFTER_MIN", "10"))

app = FastAPI(title="Microhire Venue Agent")


class BookingIn(BaseModel):
    roomId: str = Field(..., min_length=1)
    active: bool = True
    title: str = ""
    pinRequired: bool = True
    sessionPin: str = ""
    startsAtEpoch: int
    endsAtEpoch: int
    nowEpoch: Optional[int] = None
    bookingId: int
    venueId: int


def db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    con = db()
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS bookings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wp_booking_id INTEGER,
        venue_id INTEGER,
        room_id TEXT NOT NULL,
        start_iso TEXT NOT NULL,
        end_iso TEXT NOT NULL,
        title TEXT,
        pin_required INTEGER,
        session_pin TEXT,
        active INTEGER NOT NULL DEFAULT 1,
        created_at_utc TEXT NOT NULL,
        updated_at_utc TEXT NOT NULL
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bookings_room_start ON bookings(room_id, start_iso)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_bookings_wp_booking_id ON bookings(wp_booking_id)")
    con.commit()
    con.close()


def epoch_to_iso(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat()


def parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def is_active(now_utc: datetime, start: datetime, end: datetime) -> bool:
    start2 = start.timestamp() - GRACE_BEFORE_MIN * 60
    end2 = end.timestamp() + GRACE_AFTER_MIN * 60
    return start2 <= now_utc.timestamp() < end2


_state_cache: Dict[str, Dict[str, Any]] = {}


def recompute_states_loop():
    while True:
        try:
            con = db()
            cur = con.cursor()
            now = datetime.now(timezone.utc)

            rooms = [r["room_id"] for r in cur.execute("SELECT DISTINCT room_id FROM bookings").fetchall()]

            for room in rooms:
                rows = cur.execute(
                    "SELECT * FROM bookings WHERE room_id=? AND active=1 ORDER BY start_iso ASC",
                    (room,)
                ).fetchall()

                active_booking = None
                for row in rows:
                    s = parse_iso(row["start_iso"]).astimezone(timezone.utc)
                    e = parse_iso(row["end_iso"]).astimezone(timezone.utc)
                    if is_active(now, s, e):
                        active_booking = row
                        break

                if active_booking:
                    ends = parse_iso(active_booking["end_iso"]).astimezone(timezone.utc)
                    _state_cache[room] = {
                        "roomId": room,
                        "active": True,
                        "title": active_booking["title"] or "",
                        "pinRequired": bool(active_booking["pin_required"]),
                        "sessionPin": active_booking["session_pin"] or "",
                        "endsAtEpoch": int(ends.timestamp()),
                        "nowEpoch": int(now.timestamp()),
                    }
                else:
                    _state_cache[room] = {
                        "roomId": room,
                        "active": False,
                        "title": "",
                        "pinRequired": False,
                        "sessionPin": "",
                        "endsAtEpoch": 0,
                        "nowEpoch": int(now.timestamp()),
                    }

            con.close()
        except Exception as ex:
            print("State loop error:", ex)

        time.sleep(10)


@app.on_event("startup")
def startup():
    init_db()
    t = threading.Thread(target=recompute_states_loop, daemon=True)
    t.start()


@app.get("/api/v1/health")
def health():
    return {"ok": True, "db": DB_PATH, "roomsCached": list(_state_cache.keys())}


@app.post("/api/v1/bookings")
def create_or_update_booking(
    payload: BookingIn,
    x_microhire_webhook_key: Optional[str] = Header(default=None)
):
    if x_microhire_webhook_key != WEBHOOK_KEY:
        raise HTTPException(status_code=401, detail="Invalid webhook key")

    start_iso = epoch_to_iso(payload.startsAtEpoch)
    end_iso = epoch_to_iso(payload.endsAtEpoch)

    start = parse_iso(start_iso)
    end = parse_iso(end_iso)
    if end <= start:
        raise HTTPException(status_code=400, detail="endsAtEpoch must be after startsAtEpoch")

    con = db()
    cur = con.cursor()
    now_iso = datetime.now(timezone.utc).isoformat()

    existing = cur.execute(
        "SELECT id FROM bookings WHERE wp_booking_id=?",
        (payload.bookingId,)
    ).fetchone()

    if existing:
        cur.execute("""
            UPDATE bookings
            SET venue_id=?,
                room_id=?,
                start_iso=?,
                end_iso=?,
                title=?,
                pin_required=?,
                session_pin=?,
                active=?,
                updated_at_utc=?
            WHERE wp_booking_id=?
        """, (
            payload.venueId,
            payload.roomId,
            start_iso,
            end_iso,
            payload.title,
            1 if payload.pinRequired else 0,
            payload.sessionPin,
            1 if payload.active else 0,
            now_iso,
            payload.bookingId
        ))
        local_id = existing["id"]
    else:
        cur.execute("""
            INSERT INTO bookings (
                wp_booking_id, venue_id, room_id, start_iso, end_iso,
                title, pin_required, session_pin, active, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            payload.bookingId,
            payload.venueId,
            payload.roomId,
            start_iso,
            end_iso,
            payload.title,
            1 if payload.pinRequired else 0,
            payload.sessionPin,
            1 if payload.active else 0,
            now_iso,
            now_iso
        ))
        local_id = cur.lastrowid

    con.commit()
    con.close()

    return {
        "ok": True,
        "bookingId": payload.bookingId,
        "localId": local_id,
        "active": payload.active
    }


@app.get("/api/v1/rooms/{room_id}/state")
def room_state(room_id: str):
    state = _state_cache.get(room_id)
    if not state:
        return {
            "roomId": room_id,
            "active": False,
            "title": "",
            "pinRequired": False,
            "sessionPin": "",
            "endsAtEpoch": 0
        }

    return state
