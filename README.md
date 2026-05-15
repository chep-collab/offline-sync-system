# Offline-First Health Data Sync System
### Reliable Data Collection in Zero-Connectivity Environments

A production-grade offline-first data synchronisation system for community health worker programmes operating in low or zero-connectivity environments. Built from real-world experience deploying data infrastructure across rural Kenya and Zambia.

---

## The Problem

Community health workers in remote areas need to:
- Collect structured health data on mobile devices
- Work reliably with **no internet connection**
- Sync data back to central systems **when connectivity returns**
- Maintain **data integrity** throughout — no lost records, no silent failures

Most off-the-shelf solutions assume connectivity. This system is built for environments where connectivity is the exception, not the rule.

---

## How It Works

```
[Field Device]                    [Central Server]
     │                                   │
     │  CHW collects data                │
     │  (offline, local storage)         │
     │                                   │
     │  Connectivity detected?           │
     │  ──────────────────────           │
     │  YES → Sync queue                 │
     │        → Conflict detection       │
     │        → Server merge             │  ← Audit log written
     │        → Acknowledgement          │
     │  NO  → Continue offline           │
     │        → Queue grows              │
     │        → Retry on reconnect       │
```

---

## Key Features

- **SQLite local storage** — works with no connectivity whatsoever
- **Queue-based sync** — records queue locally, sync when connection returns
- **Conflict detection** — handles the case where the same record was modified offline on multiple devices
- **Retry logic with exponential backoff** — handles unreliable connections gracefully
- **Sync acknowledgement** — device only clears local queue after server confirms receipt
- **Partial sync support** — large queues sync in batches, connectivity interruptions mid-sync are handled
- **Full sync audit trail** — every sync event logged with timestamp, device ID, and outcome

---

## Real-World Context

This architecture reflects patterns used in a community registry deployment covering **250,000+ households in Zambia** — where field teams operated across areas with no 3G/4G coverage, unreliable power, and shared devices. The key lessons:

1. **Never clear the local queue until the server confirms** — connectivity can drop mid-sync
2. **Conflicts happen** — design for them, don't pretend they won't
3. **The audit trail saves you** — when data anomalies appear weeks later, you need to trace exactly what synced when from which device

---

## Tech Stack

- Python 3.10+
- SQLite (local device storage)
- FastAPI (sync server)
- PostgreSQL (central store)
- Docker / Docker Compose

---

## Project Structure

```
offline-sync-system/
├── src/
│   ├── local_store.py        # SQLite local storage manager
│   ├── sync_queue.py         # Offline queue management
│   ├── sync_client.py        # Device-side sync logic
│   ├── sync_server.py        # Server-side sync endpoint (FastAPI)
│   ├── conflict_resolver.py  # Conflict detection and resolution
│   └── connectivity.py       # Connectivity detection utilities
├── tests/
│   ├── test_sync_queue.py
│   ├── test_conflict_resolver.py
│   └── test_offline_scenario.py
├── docs/
│   ├── ARCHITECTURE.md
│   └── SYNC_PROTOCOL.md
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Quick Start

```bash
git clone https://github.com/mercychepngeno/offline-sync-system
cd offline-sync-system

# Start the sync server
docker-compose up -d

# Simulate offline data collection + sync
python src/sync_client.py --simulate-offline --records 100 --device-id CHW_001
```

---

## Sync Protocol

See `docs/SYNC_PROTOCOL.md` for full protocol documentation including:
- Queue structure and record schema
- Conflict detection logic
- Retry and backoff strategy
- Partial sync handling
- Audit log schema

---

*Built for the reality of community health work — where connectivity is a luxury, not a given.*
