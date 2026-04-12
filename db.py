"""
db.py — MongoDB connection for the Telegram File Sharing Bot.

Database : filebot
Collection: files
"""

import logging
import os

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ─── Module-level singletons ──────────────────────────────────────────────────

_client: MongoClient | None = None
_db: Database | None = None
files_collection: Collection | None = None   # imported by other modules

# ─── Public API ───────────────────────────────────────────────────────────────

def init_db() -> None:
    """
    Connect to MongoDB, initialise the `filebot` database and `files`
    collection, then run a quick smoke-test insert/fetch.

    Call this once at application startup.
    """
    global _client, _db, files_collection

    mongo_uri = os.environ["MONGO_URI"]

    logger.info("Connecting to MongoDB …")
    _client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=5_000,   # fail fast if unreachable
        connectTimeoutMS=5_000,
    )

    # Force a real connection attempt so we surface errors early.
    try:
        _client.admin.command("ping")
    except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
        logger.error("MongoDB connection failed: %s", exc)
        raise

    _db = _client["filebot"]
    files_collection = _db["files"]

    logger.info("MongoDB connected — db=filebot, collection=files ✅")

    _smoke_test()


def get_files_collection() -> Collection:
    """Return the `files` collection (raises if not initialised)."""
    if files_collection is None:
        raise RuntimeError("Database not initialised — call init_db() first.")
    return files_collection


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _smoke_test() -> None:
    """Insert a test document and print it to confirm everything works."""
    col = get_files_collection()

    result = col.insert_one({"test": "ok"})
    doc = col.find_one({"_id": result.inserted_id})

    # Pretty-print so it's visible in the startup log.
    logger.info("MongoDB smoke-test document → %s", doc)
    print(f"\n[DB SMOKE TEST] Inserted document: {doc}\n")
