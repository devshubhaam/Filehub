"""
db.py — MongoDB connection for the Telegram File Sharing Bot.

Database    : filebot
Collections : files, users, clicks, access_tokens
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ─── Module-level singletons ──────────────────────────────────────────────────

_client: MongoClient | None = None
_db: Database | None        = None
files_collection: Collection | None = None
users_collection: Collection | None = None


# ─── Initialisation ───────────────────────────────────────────────────────────

def init_db() -> None:
    global _client, _db, files_collection, users_collection

    mongo_uri = os.environ["MONGO_URI"]
    logger.info("Connecting to MongoDB ...")
    _client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5_000, connectTimeoutMS=5_000)

    try:
        _client.admin.command("ping")
    except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
        logger.error("MongoDB connection failed: %s", exc)
        raise

    _db = _client["filebot"]

    files_collection = _db["files"]
    files_collection.create_index("unique_id", unique=True, sparse=True)

    users_collection = _db["users"]
    users_collection.create_index("user_id", unique=True)

    # access_tokens — pending (15min) + granted (24hr)
    # MongoDB TTL index auto-deletes expired docs
    access_col = _db["access_tokens"]
    access_col.create_index("token",      unique=True)
    access_col.create_index("user_id",    sparse=True)
    access_col.create_index("expires_at", expireAfterSeconds=0)

    logger.info("MongoDB connected — db=filebot | collections: files, users, access_tokens OK")
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

def _get_access_col() -> Collection:
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")
    return _db["access_tokens"]


# ─── Files API ────────────────────────────────────────────────────────────────

def save_file(unique_id: str, file_id: str) -> str:
    col = get_files_collection()
    now = datetime.now(tz=timezone.utc)
    existing = col.find_one({"unique_id": unique_id})

    if existing:
        if file_id in existing.get("file_ids", []):
            return "exists"
        col.update_one(
            {"unique_id": unique_id},
            {"$addToSet": {"file_ids": file_id}, "$set": {"updated_at": now}},
        )
        return "updated"

    col.insert_one({
        "unique_id":  unique_id,
        "file_ids":   [file_id],
        "views":      0,
        "created_at": now,
        "updated_at": now,
    })
    return "inserted"

def get_file(unique_id: str) -> dict | None:
    col = get_files_collection()
    return col.find_one({"unique_id": unique_id})

def remove_file_id(unique_id: str, file_id: str) -> None:
    col = get_files_collection()
    now = datetime.now(tz=timezone.utc)
    col.update_one(
        {"unique_id": unique_id},
        {"$pull": {"file_ids": file_id}, "$set": {"updated_at": now}},
    )
    logger.info("[SELF-HEAL] Removed dead file_id from unique_id=%s", unique_id)

def increment_views(unique_id: str) -> None:
    col = get_files_collection()
    col.update_one({"unique_id": unique_id}, {"$inc": {"views": 1}})
    logger.info("[ANALYTICS] Views incremented for unique_id=%s", unique_id)


# ─── Users API ────────────────────────────────────────────────────────────────

def upsert_user(user_id: int, first_name: str = "") -> str:
    col = get_users_collection()
    now = datetime.now(tz=timezone.utc)
    existing = col.find_one({"user_id": user_id})

    if existing:
        col.update_one({"user_id": user_id}, {"$set": {"last_seen": now}})
        return "returning"

    col.insert_one({
        "user_id":    user_id,
        "first_name": first_name,
        "first_seen": now,
        "last_seen":  now,
    })
    return "new"


# ─── Click Tracking ───────────────────────────────────────────────────────────

def track_click(unique_id: str, ip: str, user_agent: str) -> None:
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")
    _db["clicks"].insert_one({
        "unique_id":  unique_id,
        "ip":         ip,
        "user_agent": user_agent,
        "timestamp":  datetime.now(tz=timezone.utc),
    })
    logger.info("[CLICK] Tracked | unique_id=%s | ip=%s", unique_id, ip)


# ─── Access Token API ─────────────────────────────────────────────────────────
#
#  TYPE "pending" — shortener ke andar jaane se pehle banta hai (15 min TTL)
#  TYPE "granted" — bot pe verify_ se aane ke baad banta hai (24hr TTL)
#
# ─────────────────────────────────────────────────────────────────────────────

def create_pending_token(token: str, ttl_seconds: int = 900) -> None:
    """Short-lived token — shortener URL ke andar embed hota hai."""
    col = _get_access_col()
    now = datetime.now(tz=timezone.utc)
    col.insert_one({
        "token":      token,
        "type":       "pending",
        "used":       False,
        "expires_at": now + timedelta(seconds=ttl_seconds),
        "created_at": now,
    })
    logger.info("[TOKEN] Pending token created | ttl=%ds", ttl_seconds)


def verify_pending_token(token: str) -> bool:
    """
    Token valid hai? → True + mark as used
    Expired/used/invalid? → False
    """
    col = _get_access_col()
    now = datetime.now(tz=timezone.utc)

    doc = col.find_one({
        "token":      token,
        "type":       "pending",
        "used":       False,
        "expires_at": {"$gt": now},
    })

    if not doc:
        logger.warning("[TOKEN] Invalid/expired pending token: %s...", token[:10])
        return False

    col.update_one({"_id": doc["_id"]}, {"$set": {"used": True, "used_at": now}})
    logger.info("[TOKEN] Pending token consumed: %s...", token[:10])
    return True


def grant_user_access(user_id: int, ttl_hours: int = 24) -> None:
    """User ko ttl_hours ka access do. Purana access replace hota hai."""
    col = _get_access_col()
    now = datetime.now(tz=timezone.utc)

    col.delete_many({"user_id": user_id, "type": "granted"})
    col.insert_one({
        "type":       "granted",
        "user_id":    user_id,
        "expires_at": now + timedelta(hours=ttl_hours),
        "created_at": now,
    })
    logger.info("[ACCESS] Granted %dhr access | user_id=%s", ttl_hours, user_id)


def has_valid_access(user_id: int) -> bool:
    """User ka active access check karo."""
    col = _get_access_col()
    now = datetime.now(tz=timezone.utc)
    doc = col.find_one({
        "user_id":    user_id,
        "type":       "granted",
        "expires_at": {"$gt": now},
    })
    if doc:
        hrs = (doc["expires_at"] - now).total_seconds() / 3600
        logger.info("[ACCESS] Valid | user_id=%s | %.1fhr left", user_id, hrs)
        return True
    logger.info("[ACCESS] No valid access | user_id=%s", user_id)
    return False


def get_access_expiry(user_id: int) -> datetime | None:
    """User ke current access ka expiry time return karo."""
    col = _get_access_col()
    now = datetime.now(tz=timezone.utc)
    doc = col.find_one({
        "user_id":    user_id,
        "type":       "granted",
        "expires_at": {"$gt": now},
    })
    return doc["expires_at"] if doc else None


# ─── Internal ─────────────────────────────────────────────────────────────────

def _smoke_test() -> None:
    col = get_files_collection()
    result = col.insert_one({"test": "ok"})
    doc = col.find_one({"_id": result.inserted_id})
    logger.info("MongoDB smoke-test -> %s", doc)
    print(f"\n[DB SMOKE TEST] {doc}\n")
