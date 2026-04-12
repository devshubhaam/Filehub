"""
db.py — MongoDB connection for the Telegram File Sharing Bot.

Database : filebot
Collection: files
"""

import logging
import os
from datetime import datetime, timezone

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
files_collection: Collection | None = None


# ─── Public API ───────────────────────────────────────────────────────────────

def init_db() -> None:
    """
    Connect to MongoDB, initialise the `filebot` database and `files`
    collection, then run a quick smoke-test insert/fetch.
    """
    global _client, _db, files_collection

    mongo_uri = os.environ["MONGO_URI"]

    logger.info("Connecting to MongoDB ...")
    _client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=5_000,
        connectTimeoutMS=5_000,
    )

    try:
        _client.admin.command("ping")
    except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
        logger.error("MongoDB connection failed: %s", exc)
        raise

    _db = _client["filebot"]
    files_collection = _db["files"]

    # sparse=True ignores docs where unique_id is null (e.g. smoke-test docs)
    files_collection.create_index("unique_id", unique=True, sparse=True)

    logger.info("MongoDB connected — db=filebot, collection=files OK")
    _smoke_test()


def get_files_collection() -> Collection:
    """Return the `files` collection (raises if not initialised)."""
    if files_collection is None:
        raise RuntimeError("Database not initialised — call init_db() first.")
    return files_collection


def save_file(unique_id: str, file_id: str) -> str:
    """
    Save or update a file record in MongoDB.

    Returns:
      "updated"  -- existing record, new file_id added
      "inserted" -- brand new record created
      "exists"   -- file_id was already present (no change)
    """
    col = get_files_collection()
    now = datetime.now(tz=timezone.utc)

    existing = col.find_one({"unique_id": unique_id})

    if existing:
        if file_id in existing.get("file_ids", []):
            logger.info("file_id already exists for unique_id=%s -- skipping", unique_id)
            return "exists"

        col.update_one(
            {"unique_id": unique_id},
            {
                "$addToSet": {"file_ids": file_id},
                "$set":      {"updated_at": now},
            },
        )
        logger.info("Updated unique_id=%s with new file_id OK", unique_id)
        return "updated"

    else:
        col.insert_one(
            {
                "unique_id":  unique_id,
                "file_ids":   [file_id],
                "created_at": now,
                "updated_at": now,
            }
        )
        logger.info("Inserted new record unique_id=%s OK", unique_id)
        return "inserted"


def get_file(unique_id: str) -> dict | None:
    """
    Fetch a file document by unique_id.
    Returns the full document or None if not found.
    """
    col = get_files_collection()
    doc = col.find_one({"unique_id": unique_id})
    if doc:
        logger.info(
            "Found unique_id=%s -- %d file_id(s) available",
            unique_id,
            len(doc.get("file_ids", [])),
        )
    else:
        logger.warning("unique_id=%s not found in DB", unique_id)
    return doc


def remove_file_id(unique_id: str, file_id: str) -> None:
    """
    Remove a single dead file_id from the file_ids array in MongoDB.

    Uses $pull to surgically remove just that one entry.
    Also updates updated_at timestamp for audit trail.
    """
    col = get_files_collection()
    now = datetime.now(tz=timezone.utc)

    result = col.update_one(
        {"unique_id": unique_id},
        {
            "$pull": {"file_ids": file_id},
            "$set":  {"updated_at": now},
        },
    )

    if result.modified_count:
        logger.info(
            "[SELF-HEAL] Removed dead file_id from unique_id=%s | file_id=%s",
            unique_id, file_id,
        )
    else:
        logger.warning(
            "[SELF-HEAL] remove_file_id: unique_id=%s not found or file_id not in array",
            unique_id,
        )


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _smoke_test() -> None:
    """Insert a test document and print it to confirm everything works."""
    col = get_files_collection()
    result = col.insert_one({"test": "ok"})
    doc = col.find_one({"_id": result.inserted_id})
    logger.info("MongoDB smoke-test document -> %s", doc)
    print(f"\n[DB SMOKE TEST] Inserted document: {doc}\n")
