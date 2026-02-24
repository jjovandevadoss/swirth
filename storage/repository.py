import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schema import SCHEMA_SQL


class MessageRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        db_parent = Path(db_path).parent
        if db_parent and not db_parent.exists():
            db_parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)

    def create_message(self, message_uid: str, protocol: str, source_ip: str, raw_message: str, parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.utcnow().isoformat()
        parsed_json = json.dumps(parsed_data)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO messages (
                    message_uid, protocol, source_ip, raw_message, parsed_data,
                    delivery_status, attempts, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?)
                """,
                (message_uid, protocol, source_ip, raw_message, parsed_json, now, now),
            )
            row = conn.execute("SELECT * FROM messages WHERE message_uid = ?", (message_uid,)).fetchone()
        return self._row_to_dict(row)

    def mark_delivered(self, message_uid: str, api_status: int) -> None:
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE messages
                SET delivery_status = 'delivered',
                    attempts = attempts + 1,
                    api_status = ?,
                    last_error = NULL,
                    delivered_at = ?,
                    updated_at = ?
                WHERE message_uid = ?
                """,
                (api_status, now, now, message_uid),
            )

    def mark_failed_attempt(self, message_uid: str, error: str) -> None:
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE messages
                SET delivery_status = 'failed',
                    attempts = attempts + 1,
                    last_error = ?,
                    updated_at = ?
                WHERE message_uid = ?
                """,
                (error, now, message_uid),
            )

    def get_message(self, message_uid: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM messages WHERE message_uid = ?", (message_uid,)).fetchone()
        return self._row_to_dict(row) if row else None

    def get_latest_by_protocol(self, protocol: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM messages
                WHERE protocol = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (protocol.upper(),),
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_recent_messages(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM messages
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_pending_retries(self, max_attempts: int = 5, limit: int = 50) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM messages
                WHERE delivery_status IN ('pending', 'failed')
                  AND attempts < ?
                ORDER BY updated_at ASC
                LIMIT ?
                """,
                (max_attempts, limit),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        parsed_data = None
        if row["parsed_data"]:
            try:
                parsed_data = json.loads(row["parsed_data"])
            except json.JSONDecodeError:
                parsed_data = None

        return {
            "id": row["id"],
            "message_uid": row["message_uid"],
            "protocol": row["protocol"],
            "source_ip": row["source_ip"],
            "raw_message": row["raw_message"],
            "parsed_data": parsed_data,
            "delivery_status": row["delivery_status"],
            "attempts": row["attempts"],
            "api_status": row["api_status"],
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "delivered_at": row["delivered_at"],
        }
