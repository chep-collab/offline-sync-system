"""
sync_queue.py
-------------
Manages the offline sync queue on the field device.

The queue is the heart of the offline-first system.
Records are written to the queue immediately on collection.
They stay in the queue until the server explicitly acknowledges receipt.

This is not optional — it's what prevents data loss when
connectivity drops mid-sync, which happens constantly in the field.
"""

import sqlite3
import json
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from enum import Enum
import logging

logger = logging.getLogger(__name__)

QUEUE_DB_PATH = "local_queue.db"


class SyncStatus(Enum):
    PENDING = "PENDING"          # Not yet attempted
    IN_PROGRESS = "IN_PROGRESS"  # Sync attempt in progress
    SYNCED = "SYNCED"            # Server acknowledged
    FAILED = "FAILED"            # Failed after max retries
    CONFLICT = "CONFLICT"        # Conflict detected — needs resolution


class SyncQueue:
    """
    SQLite-backed sync queue for offline data collection.

    Records are written immediately to local SQLite on collection.
    Status moves: PENDING → IN_PROGRESS → SYNCED (or FAILED/CONFLICT)

    Critical: records are only removed from PENDING state
    after explicit server acknowledgement.
    """

    def __init__(self, db_path: str = QUEUE_DB_PATH, device_id: str = "UNKNOWN"):
        self.db_path = db_path
        self.device_id = device_id
        self._init_db()

    def _init_db(self):
        """Initialise the queue database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_queue (
                    queue_id        TEXT PRIMARY KEY,
                    record_id       TEXT NOT NULL,
                    record_type     TEXT NOT NULL,
                    payload         TEXT NOT NULL,
                    device_id       TEXT NOT NULL,
                    created_at      TEXT NOT NULL,
                    status          TEXT NOT NULL DEFAULT 'PENDING',
                    attempt_count   INTEGER DEFAULT 0,
                    last_attempt    TEXT,
                    synced_at       TEXT,
                    server_ack_id   TEXT,
                    error_message   TEXT,
                    checksum        TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_audit (
                    audit_id        TEXT PRIMARY KEY,
                    queue_id        TEXT NOT NULL,
                    event           TEXT NOT NULL,
                    timestamp       TEXT NOT NULL,
                    details         TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON sync_queue(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_record_id ON sync_queue(record_id)")
            conn.commit()
        logger.info(f"Queue initialised | Device: {self.device_id} | DB: {self.db_path}")

    def enqueue(self, record_id: str, record_type: str, payload: Dict) -> str:
        """
        Add a record to the sync queue immediately on collection.
        Returns the queue_id for tracking.
        """
        queue_id = str(uuid.uuid4())
        payload_json = json.dumps(payload)
        checksum = self._checksum(payload_json)
        created_at = datetime.utcnow().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO sync_queue
                (queue_id, record_id, record_type, payload, device_id, created_at, status, checksum)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)
            """, (queue_id, record_id, record_type, payload_json, self.device_id, created_at, checksum))
            conn.commit()

        self._audit(queue_id, "ENQUEUED", f"record_type={record_type}, record_id={record_id}")
        logger.debug(f"Enqueued: {queue_id} | Type: {record_type} | Record: {record_id}")
        return queue_id

    def get_pending_batch(self, batch_size: int = 50) -> List[Dict]:
        """
        Get the next batch of PENDING records for sync.
        Ordered by created_at — oldest first.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT * FROM sync_queue
                WHERE status = 'PENDING'
                ORDER BY created_at ASC
                LIMIT ?
            """, (batch_size,)).fetchall()

        records = []
        for row in rows:
            records.append({
                "queue_id": row["queue_id"],
                "record_id": row["record_id"],
                "record_type": row["record_type"],
                "payload": json.loads(row["payload"]),
                "device_id": row["device_id"],
                "created_at": row["created_at"],
                "attempt_count": row["attempt_count"],
                "checksum": row["checksum"],
            })

        return records

    def mark_in_progress(self, queue_ids: List[str]):
        """Mark records as in-progress before sync attempt."""
        now = datetime.utcnow().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany("""
                UPDATE sync_queue
                SET status = 'IN_PROGRESS', last_attempt = ?, attempt_count = attempt_count + 1
                WHERE queue_id = ?
            """, [(now, qid) for qid in queue_ids])
            conn.commit()

    def acknowledge(self, queue_id: str, server_ack_id: str):
        """
        Mark a record as synced after explicit server acknowledgement.

        CRITICAL: Only call this after the server has confirmed receipt.
        Never acknowledge based on the sync request alone —
        connectivity can drop between request and server write.
        """
        now = datetime.utcnow().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE sync_queue
                SET status = 'SYNCED', synced_at = ?, server_ack_id = ?
                WHERE queue_id = ?
            """, (now, server_ack_id, queue_id))
            conn.commit()

        self._audit(queue_id, "ACKNOWLEDGED", f"server_ack_id={server_ack_id}")

    def mark_failed(self, queue_id: str, error: str, max_retries: int = 5):
        """Mark a record as failed. After max_retries, status becomes FAILED."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT attempt_count FROM sync_queue WHERE queue_id = ?", (queue_id,)
            ).fetchone()

            if row and row[0] >= max_retries:
                conn.execute("""
                    UPDATE sync_queue
                    SET status = 'FAILED', error_message = ?
                    WHERE queue_id = ?
                """, (error, queue_id))
                self._audit(queue_id, "FAILED", f"error={error}, attempts={row[0]}")
                logger.warning(f"Record permanently failed after {row[0]} attempts: {queue_id}")
            else:
                # Reset to PENDING for retry
                conn.execute("""
                    UPDATE sync_queue
                    SET status = 'PENDING', error_message = ?
                    WHERE queue_id = ?
                """, (error, queue_id))
                self._audit(queue_id, "RETRY_SCHEDULED", f"error={error}")

            conn.commit()

    def mark_conflict(self, queue_id: str, conflict_details: str):
        """Mark a record as having a conflict — requires human/system resolution."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE sync_queue
                SET status = 'CONFLICT', error_message = ?
                WHERE queue_id = ?
            """, (conflict_details, queue_id))
            conn.commit()
        self._audit(queue_id, "CONFLICT_DETECTED", conflict_details)
        logger.warning(f"Conflict detected for queue_id: {queue_id}")

    def get_queue_stats(self) -> Dict:
        """Return current queue statistics."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("""
                SELECT status, COUNT(*) as count
                FROM sync_queue
                GROUP BY status
            """).fetchall()

        stats = {status.value: 0 for status in SyncStatus}
        for row in rows:
            stats[row[0]] = row[1]

        stats["total"] = sum(stats.values())
        return stats

    def _checksum(self, payload_json: str) -> str:
        """Simple checksum for payload integrity verification."""
        import hashlib
        return hashlib.md5(payload_json.encode()).hexdigest()

    def _audit(self, queue_id: str, event: str, details: str = ""):
        """Write an audit log entry."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO sync_audit (audit_id, queue_id, event, timestamp, details)
                VALUES (?, ?, ?, ?, ?)
            """, (str(uuid.uuid4()), queue_id, event, datetime.utcnow().isoformat(), details))
            conn.commit()
