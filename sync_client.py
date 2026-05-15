"""
sync_client.py
--------------
Device-side sync logic for offline-first CHW data collection.

This is the component that runs on the field device (Android/tablet).
It detects connectivity, pulls pending records from the local queue,
sends them to the server in batches, and only marks them as synced
after explicit server acknowledgement.

Lessons from the field (Zambia, 2020-2022):
- Never trust that a request reached the server just because it was sent
- Connectivity can drop between request and server write
- Batch size matters — too large and a mid-sync dropout wastes everything
- Always log. When anomalies surface weeks later, you need the trail.
"""

import time
import requests
import socket
import logging
from datetime import datetime
from typing import Optional, Dict, List
from sync_queue import SyncQueue, SyncStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Backoff settings — important in low-connectivity environments
# where hammering a weak connection makes things worse
INITIAL_BACKOFF = 2       # seconds
MAX_BACKOFF = 120         # 2 minutes max wait
BACKOFF_MULTIPLIER = 2
MAX_RETRIES = 5
BATCH_SIZE = 50           # records per sync batch


class SyncClient:
    """
    Manages the sync process from field device to central server.

    Design principles:
    1. Only acknowledge after server confirms — not after request sent
    2. Batch processing — never try to sync everything at once
    3. Exponential backoff — bad connections need breathing room
    4. Full audit trail — every sync event logged locally
    5. Graceful degradation — failure should never corrupt local data
    """

    def __init__(
        self,
        queue: SyncQueue,
        server_url: str,
        device_id: str,
        api_key: Optional[str] = None,
        batch_size: int = BATCH_SIZE,
    ):
        self.queue = queue
        self.server_url = server_url.rstrip("/")
        self.device_id = device_id
        self.api_key = api_key
        self.batch_size = batch_size
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        self.session.headers.update({
            "Content-Type": "application/json",
            "X-Device-ID": device_id,
        })

    def is_connected(self, timeout: int = 3) -> bool:
        """
        Check internet connectivity before attempting sync.
        We use a DNS lookup rather than pinging the server directly
        so we don't generate unnecessary server load during connectivity checks.
        """
        try:
            socket.setdefaulttimeout(timeout)
            socket.getaddrinfo("8.8.8.8", 53)
            return True
        except (socket.gaierror, OSError):
            return False

    def sync(self) -> Dict:
        """
        Run a full sync cycle.
        Returns a summary of what happened.
        """
        if not self.is_connected():
            logger.info("No connectivity detected — skipping sync")
            return {"status": "skipped", "reason": "no_connectivity"}

        logger.info(f"Connectivity detected — starting sync | Device: {self.device_id}")

        stats = {
            "started_at": datetime.utcnow().isoformat(),
            "batches_processed": 0,
            "records_synced": 0,
            "records_failed": 0,
            "records_conflict": 0,
        }

        while True:
            batch = self.queue.get_pending_batch(self.batch_size)
            if not batch:
                logger.info("Queue empty — sync complete")
                break

            logger.info(f"Processing batch of {len(batch)} records")
            self.queue.mark_in_progress([r["queue_id"] for r in batch])

            batch_result = self._sync_batch(batch)
            stats["batches_processed"] += 1
            stats["records_synced"] += batch_result["synced"]
            stats["records_failed"] += batch_result["failed"]
            stats["records_conflict"] += batch_result["conflicts"]

            # If connectivity dropped mid-batch, stop and wait
            if batch_result["connectivity_lost"]:
                logger.warning("Connectivity lost mid-sync — stopping. Will retry on next cycle.")
                break

        stats["completed_at"] = datetime.utcnow().isoformat()
        queue_stats = self.queue.get_queue_stats()
        stats["queue_after_sync"] = queue_stats

        logger.info(
            f"Sync complete | Synced: {stats['records_synced']} | "
            f"Failed: {stats['records_failed']} | "
            f"Pending: {queue_stats.get('PENDING', 0)}"
        )
        return stats

    def _sync_batch(self, batch: List[Dict]) -> Dict:
        """
        Sync a single batch of records to the server.
        Processes each record individually so a single failure
        doesn't block the rest of the batch.
        """
        result = {"synced": 0, "failed": 0, "conflicts": 0, "connectivity_lost": False}
        backoff = INITIAL_BACKOFF

        for record in batch:
            if not self.is_connected():
                result["connectivity_lost"] = True
                # Reset in-progress records back to pending
                self.queue.mark_failed(record["queue_id"], "connectivity_lost_mid_sync")
                logger.warning("Connectivity lost during batch processing")
                break

            success, response = self._send_record(record)

            if success:
                ack_id = response.get("ack_id", "")
                self.queue.acknowledge(record["queue_id"], ack_id)
                result["synced"] += 1
                backoff = INITIAL_BACKOFF  # Reset backoff on success

            elif response.get("error_type") == "CONFLICT":
                self.queue.mark_conflict(
                    record["queue_id"],
                    response.get("conflict_details", "Server conflict detected")
                )
                result["conflicts"] += 1

            else:
                error_msg = response.get("error", "unknown_error")
                self.queue.mark_failed(record["queue_id"], error_msg)
                result["failed"] += 1

                # Exponential backoff on failure
                logger.warning(f"Record failed: {record['queue_id']} | Backing off {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)

        return result

    def _send_record(self, record: Dict):
        """
        Send a single record to the server.
        Returns (success: bool, response: dict)
        """
        payload = {
            "queue_id": record["queue_id"],
            "record_id": record["record_id"],
            "record_type": record["record_type"],
            "payload": record["payload"],
            "device_id": record["device_id"],
            "created_at": record["created_at"],
            "checksum": record["checksum"],
        }

        try:
            response = self.session.post(
                f"{self.server_url}/api/sync/ingest",
                json=payload,
                timeout=15  # Short timeout — don't hang waiting in bad connectivity
            )

            if response.status_code == 200:
                return True, response.json()
            elif response.status_code == 409:
                return False, {"error_type": "CONFLICT", "conflict_details": response.text}
            else:
                return False, {"error": f"http_{response.status_code}"}

        except requests.exceptions.Timeout:
            return False, {"error": "timeout"}
        except requests.exceptions.ConnectionError:
            return False, {"error": "connection_error"}
        except Exception as e:
            return False, {"error": str(e)}

    def run_continuous(self, interval_seconds: int = 30):
        """
        Run sync continuously, checking every interval_seconds.
        This is what runs as a background service on the field device.
        """
        logger.info(f"Starting continuous sync | Interval: {interval_seconds}s | Device: {self.device_id}")
        while True:
            try:
                self.sync()
            except Exception as e:
                logger.error(f"Unexpected sync error: {e}")
            time.sleep(interval_seconds)
