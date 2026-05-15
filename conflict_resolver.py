"""
conflict_resolver.py
--------------------
Conflict detection and resolution for offline sync scenarios.

Conflicts happen in CHW systems. This is not a bug to be fixed —
it's a reality to be handled well. The most common scenarios:

1. Two CHWs visit the same household offline and both sync later
2. A record is edited on the device after partial sync
3. Server received the record but acknowledgement never reached device
   so it gets sent again (duplicate submission)

The wrong approach: silently pick one version and discard the other.
The right approach: detect, classify, preserve both, flag for review.

In health data, discarding a record without human review is never acceptable.
"""

import hashlib
import json
import logging
from datetime import datetime
from typing import Dict, Tuple, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class ConflictType(Enum):
    DUPLICATE_SUBMISSION = "DUPLICATE_SUBMISSION"   # Same record sent twice
    CONCURRENT_EDIT = "CONCURRENT_EDIT"             # Same record edited in two places
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"             # Record fields don't match server schema
    STALE_UPDATE = "STALE_UPDATE"                   # Update to a record that has newer version on server


class ResolutionStrategy(Enum):
    KEEP_SERVER = "KEEP_SERVER"         # Server version wins
    KEEP_CLIENT = "KEEP_CLIENT"         # Device version wins
    KEEP_BOTH = "KEEP_BOTH"             # Preserve both, flag for human review
    MERGE = "MERGE"                     # Attempt field-level merge
    HUMAN_REVIEW = "HUMAN_REVIEW"       # Too complex — needs a person


class ConflictResolver:
    """
    Detects and resolves conflicts between device records and server records.

    Default strategy is KEEP_BOTH — we never discard health data
    without explicit human review. This is a hard design principle.
    """

    def __init__(self, default_strategy: ResolutionStrategy = ResolutionStrategy.KEEP_BOTH):
        self.default_strategy = default_strategy
        self.resolution_log = []

    def detect(
        self,
        client_record: Dict,
        server_record: Optional[Dict]
    ) -> Tuple[bool, Optional[ConflictType], str]:
        """
        Check if a conflict exists between client and server versions.

        Returns: (has_conflict: bool, conflict_type: ConflictType, reason: str)
        """
        if server_record is None:
            # No server version — no conflict, this is a new record
            return False, None, "new_record"

        client_checksum = self._checksum(client_record.get("payload", {}))
        server_checksum = self._checksum(server_record.get("payload", {}))

        if client_checksum == server_checksum:
            # Identical content — this is a duplicate submission
            # (device sent the same record twice, e.g. acknowledgement was lost)
            return True, ConflictType.DUPLICATE_SUBMISSION, "Identical record already exists on server"

        # Content differs — check timestamps to understand which is newer
        client_ts = client_record.get("created_at", "")
        server_ts = server_record.get("updated_at", server_record.get("created_at", ""))

        if client_ts and server_ts:
            try:
                client_dt = datetime.fromisoformat(client_ts)
                server_dt = datetime.fromisoformat(server_ts)

                if server_dt > client_dt:
                    return True, ConflictType.STALE_UPDATE, (
                        f"Server version is newer (server: {server_ts}, client: {client_ts})"
                    )
                else:
                    return True, ConflictType.CONCURRENT_EDIT, (
                        f"Records differ and client is newer (client: {client_ts}, server: {server_ts})"
                    )
            except (ValueError, TypeError):
                pass

        # Can't determine from timestamps — treat as concurrent edit
        return True, ConflictType.CONCURRENT_EDIT, "Records differ, timestamps unclear"

    def resolve(
        self,
        client_record: Dict,
        server_record: Optional[Dict],
        conflict_type: ConflictType,
        strategy: Optional[ResolutionStrategy] = None
    ) -> Dict:
        """
        Resolve a detected conflict.

        Returns a resolution dict with:
        - action: what was decided
        - record: the record to use (if applicable)
        - flagged_for_review: whether a human needs to look at this
        - both_records: both versions (for KEEP_BOTH and HUMAN_REVIEW)
        """
        strategy = strategy or self._choose_strategy(conflict_type)
        resolution = {
            "resolved_at": datetime.utcnow().isoformat(),
            "conflict_type": conflict_type.value,
            "strategy_applied": strategy.value,
            "flagged_for_review": False,
            "record": None,
            "both_records": None,
        }

        if conflict_type == ConflictType.DUPLICATE_SUBMISSION:
            # Safe to silently resolve — identical content, server already has it
            resolution["action"] = "acknowledged_as_duplicate"
            resolution["record"] = server_record
            logger.info(f"Duplicate submission resolved silently for record: {client_record.get('record_id')}")

        elif strategy == ResolutionStrategy.KEEP_SERVER:
            resolution["action"] = "kept_server_version"
            resolution["record"] = server_record
            resolution["flagged_for_review"] = True
            logger.info(f"Conflict resolved: kept server version for {client_record.get('record_id')}")

        elif strategy == ResolutionStrategy.KEEP_CLIENT:
            resolution["action"] = "kept_client_version"
            resolution["record"] = client_record
            resolution["flagged_for_review"] = True
            logger.info(f"Conflict resolved: kept client version for {client_record.get('record_id')}")

        elif strategy == ResolutionStrategy.MERGE:
            merged = self._attempt_merge(client_record, server_record)
            if merged["success"]:
                resolution["action"] = "merged"
                resolution["record"] = merged["record"]
                resolution["flagged_for_review"] = True
                logger.info(f"Conflict resolved: merged for {client_record.get('record_id')}")
            else:
                # Merge failed — fall back to KEEP_BOTH
                resolution["action"] = "merge_failed_kept_both"
                resolution["both_records"] = {
                    "client": client_record,
                    "server": server_record
                }
                resolution["flagged_for_review"] = True
                logger.warning(f"Merge failed for {client_record.get('record_id')} — both versions preserved")

        else:
            # KEEP_BOTH or HUMAN_REVIEW — preserve everything
            resolution["action"] = "kept_both_versions"
            resolution["both_records"] = {
                "client": client_record,
                "server": server_record
            }
            resolution["flagged_for_review"] = True
            logger.info(
                f"Conflict flagged for human review: {client_record.get('record_id')} | "
                f"Type: {conflict_type.value}"
            )

        self._log_resolution(client_record.get("record_id"), conflict_type, strategy, resolution["action"])
        return resolution

    def _choose_strategy(self, conflict_type: ConflictType) -> ResolutionStrategy:
        """
        Choose resolution strategy based on conflict type.
        Conservative by default — health data should not be silently discarded.
        """
        strategies = {
            ConflictType.DUPLICATE_SUBMISSION: ResolutionStrategy.KEEP_SERVER,
            ConflictType.STALE_UPDATE: ResolutionStrategy.KEEP_SERVER,
            ConflictType.CONCURRENT_EDIT: ResolutionStrategy.KEEP_BOTH,
            ConflictType.SCHEMA_MISMATCH: ResolutionStrategy.HUMAN_REVIEW,
        }
        return strategies.get(conflict_type, self.default_strategy)

    def _attempt_merge(self, client_record: Dict, server_record: Dict) -> Dict:
        """
        Attempt a field-level merge of two record versions.

        Strategy: for each field, if only one version has a value, use it.
        If both have different values, flag the field as conflicted.
        Only succeeds if there are no field-level conflicts.
        """
        client_payload = client_record.get("payload", {})
        server_payload = server_record.get("payload", {})
        all_keys = set(client_payload.keys()) | set(server_payload.keys())

        merged = {}
        field_conflicts = []

        for key in all_keys:
            client_val = client_payload.get(key)
            server_val = server_payload.get(key)

            if client_val == server_val:
                merged[key] = client_val
            elif client_val is None:
                merged[key] = server_val
            elif server_val is None:
                merged[key] = client_val
            else:
                field_conflicts.append(key)

        if field_conflicts:
            return {"success": False, "field_conflicts": field_conflicts}

        merged_record = {**client_record, "payload": merged}
        return {"success": True, "record": merged_record}

    def _checksum(self, payload: Dict) -> str:
        payload_str = json.dumps(payload, sort_keys=True)
        return hashlib.md5(payload_str.encode()).hexdigest()

    def _log_resolution(self, record_id: str, conflict_type: ConflictType,
                         strategy: ResolutionStrategy, action: str):
        self.resolution_log.append({
            "timestamp": datetime.utcnow().isoformat(),
            "record_id": record_id,
            "conflict_type": conflict_type.value,
            "strategy": strategy.value,
            "action": action,
        })

    def get_summary(self) -> Dict:
        total = len(self.resolution_log)
        flagged = sum(1 for r in self.resolution_log if r["action"] not in
                      ["acknowledged_as_duplicate", "kept_server_version"])
        return {
            "total_conflicts_resolved": total,
            "flagged_for_human_review": flagged,
            "by_type": {
                ct.value: sum(1 for r in self.resolution_log if r["conflict_type"] == ct.value)
                for ct in ConflictType
            }
        }
