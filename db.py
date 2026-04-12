"""
db.py — MongoDB connection for the Telegram File Sharing Bot.

Database    : filebot
Collections : files, users
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
users_collection: Collection | None = None


# ─── Initialisation ───────────────────────────────────────────────────────────

def init_db() -> None:
    """
    Connect to MongoDB, set up `filebot` database with:
      - files  collection (unique_id index, sparse)
      - users  collection (user_id index, unique)
    Runs a smoke-test insert on startup.
    """
    global _client, _db, files_collection, users_collection

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

    # ── files collection ──────────────────────────────────────────────────────
    files_collection = _db["files"]
    # sparse=True: ignores smoke-test docs that have no unique_id field
    files_collection.create_index("unique_id", unique=True, sparse=True)

    # ── users collection ──────────────────────────────────────────────────────
    users_collection = _db["users"]
    users_collection.create_index("user_id", unique=True)

    logger.info("MongoDB connected — db=filebot | collections: files, users OK")
    _smoke_test()


# ─── Collection accessors ─────────────────────────────────────────────────────

def get_files_collection() -> Collection:
    if files_collection is None:
        raise RuntimeError("Database not initialised — call init_db() first.")
    return files_collection


def get_users_collection() -> Collection:
    if users_collection is None:
        raise RuntimeError("Database not initialised — call init_db() first.")
    return users_collection


# ─── Files API ────────────────────────────────────────────────────────────────

def save_file(unique_id: str, file_id: str) -> str:
    """
    Upsert a file record.

    Returns:
      "inserted" — new document created
      "updated"  — file_id added to existing document
      "exists"   — file_id already present, no change
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

    col.insert_one(
        {
            "unique_id":  unique_id,
            "file_ids":   [file_id],
            "views":      0,            # analytics counter
            "created_at": now,
            "updated_at": now,
        }
    )
    logger.info("Inserted new record unique_id=%s OK", unique_id)
    return "inserted"


def get_file(unique_id: str) -> dict | None:
    """Fetch a file document by unique_id. Returns None if not found."""
    col = get_files_collection()
    doc = col.find_one({"unique_id": unique_id})
    if doc:
        logger.info(
            "Found unique_id=%s -- %d file_id(s) | views=%d",
            unique_id,
            len(doc.get("file_ids", [])),
            doc.get("views", 0),
        )
    else:
        logger.warning("unique_id=%s not found in DB", unique_id)
    return doc


def remove_file_id(unique_id: str, file_id: str) -> None:
    """
    Remove a single dead file_id from the file_ids array.
    Uses $pull — does not touch other file_ids.
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
            "[SELF-HEAL] Could not remove file_id — unique_id=%s not found",
            unique_id,
        )


def increment_views(unique_id: str) -> None:
    """
    Increment the views counter for a file by 1.
    Called after every successful file delivery.
    """
    col = get_files_collection()
    result = col.update_one(
        {"unique_id": unique_id},
        {"$inc": {"views": 1}},
    )
    if result.modified_count:
        logger.info("[ANALYTICS] Views incremented for unique_id=%s", unique_id)
    else:
        logger.warning("[ANALYTICS] increment_views: unique_id=%s not found", unique_id)


# ─── Users API ────────────────────────────────────────────────────────────────

def upsert_user(user_id: int, first_name: str = "") -> str:
    """
    Track a user in the users collection.

    - First visit  → insert with first_seen + last_seen
    - Return visit → update last_seen only

    Returns:
      "new"      — user inserted for the first time
      "returning"— user already existed, last_seen updated
    """
    col = get_users_collection()
    now = datetime.now(tz=timezone.utc)

    existing = col.find_one({"user_id": user_id})

    if existing:
        col.update_one(
            {"user_id": user_id},
            {"$set": {"last_seen": now}},
        )
        logger.info("[USER] Returning user user_id=%s | last_seen updated", user_id)
        return "returning"

    col.insert_one(
        {
            "user_id":    user_id,
            "first_name": first_name,
            "first_seen": now,
            "last_seen":  now,
        }
    )
    logger.info("[USER] New user tracked user_id=%s | first_name=%r", user_id, first_name)
    return "new"


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _smoke_test() -> None:
    """Insert a test document at startup to confirm write access."""
    col = get_files_collection()
    result = col.insert_one({"test": "ok"})
    doc = col.find_one({"_id": result.inserted_id})
    logger.info("MongoDB smoke-test document -> %s", doc)
    print(f"\n[DB SMOKE TEST] Inserted document: {doc}\n")
