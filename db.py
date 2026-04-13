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

def track_click(unique_id: str, ip: str, user_agent: str) -> None:
    """
    Record a click event in the `clicks` collection.

    Schema:
      unique_id  — which file was accessed
      ip         — visitor IP address
      user_agent — visitor browser/client string
      timestamp  — UTC datetime of the click
    """
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    clicks_col = _db["clicks"]
    now        = datetime.now(tz=timezone.utc)

    clicks_col.insert_one(
        {
            "unique_id":  unique_id,
            "ip":         ip,
            "user_agent": user_agent,
            "timestamp":  now,
        }
    )
    logger.info(
        "[CLICK] Tracked click | unique_id=%s | ip=%s",
        unique_id, ip,
    )

# ─── Verified Users API ───────────────────────────────────────────────────────

_ACCESS_HOURS = 24  # hours before re-verification required


def verify_user(user_id: int) -> None:
    """
    Store or update verification time for user_id.

    - If user exists → update verified_at to now
    - If not         → insert new record

    Single upsert query — optimised for high traffic.
    """
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    now = datetime.now(tz=timezone.utc)
    _db["verified_users"].update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "verified_at": now}},
        upsert=True,
    )
    logger.info("[ACCESS] User verified | user_id=%s | verified_at=%s", user_id, now.isoformat())


def is_user_verified(user_id: int) -> bool:
    """
    Return True if user verified within the last 24 hours.

    Single DB query. Handles naive/aware datetime from MongoDB safely.
    """
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    doc = _db["verified_users"].find_one(
        {"user_id": user_id},
        {"verified_at": 1},      # project only the field we need
    )

    if not doc:
        return False

    verified_at = doc.get("verified_at")
    if not verified_at:
        return False

    # MongoDB may return naive datetime — make tz-aware
    if verified_at.tzinfo is None:
        verified_at = verified_at.replace(tzinfo=timezone.utc)

    elapsed_hours = (datetime.now(tz=timezone.utc) - verified_at).total_seconds() / 3600
    valid = elapsed_hours < _ACCESS_HOURS

    logger.info(
        "[ACCESS] is_user_verified user_id=%s → %s | elapsed=%.1fh",
        user_id, valid, elapsed_hours,
    )
    return valid


def can_access(user_id: int) -> bool:
    """
    Single source of truth for file access control.

    All file delivery must pass through this function.
    Returns True only if user has a valid 24-hour verification token.
    """
    return is_user_verified(user_id)

# ─── Payments API ─────────────────────────────────────────────────────────────

def submit_payment(user_id: int, utr: str) -> str:
    """
    Store a new payment request.
    Returns "ok" on success, "duplicate" if UTR already exists.
    """
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    existing = _db["payments"].find_one({"utr": utr})
    if existing:
        logger.warning("[PAYMENT] Duplicate UTR=%s from user_id=%s", utr, user_id)
        return "duplicate"

    now = datetime.now(tz=timezone.utc)
    _db["payments"].insert_one({
        "user_id":   user_id,
        "utr":       utr,
        "status":    "pending",
        "timestamp": now,
    })
    logger.info("[PAYMENT] Submitted | user_id=%s | utr=%s", user_id, utr)
    return "ok"


def approve_payment(user_id: int, utr: str) -> None:
    """Mark payment approved and grant 30-day premium to user."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    from datetime import timedelta
    now           = datetime.now(tz=timezone.utc)
    valid_until   = now + timedelta(days=30)

    _db["payments"].update_one(
        {"user_id": user_id, "utr": utr},
        {"$set": {"status": "approved", "approved_at": now}},
    )
    _db["premium_users"].update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "valid_until": valid_until}},
        upsert=True,
    )
    logger.info("[PAYMENT] Approved | user_id=%s | utr=%s | valid_until=%s",
                user_id, utr, valid_until.isoformat())


def reject_payment(user_id: int, utr: str) -> None:
    """Mark a single payment as rejected."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    now = datetime.now(tz=timezone.utc)
    _db["payments"].update_one(
        {"user_id": user_id, "utr": utr},
        {"$set": {"status": "rejected", "rejected_at": now}},
    )
    logger.info("[PAYMENT] Rejected | user_id=%s | utr=%s", user_id, utr)


def reject_all_pending() -> int:
    """
    Reject all payments currently in pending status.
    Returns count of documents updated.
    """
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    now    = datetime.now(tz=timezone.utc)
    result = _db["payments"].update_many(
        {"status": "pending"},
        {"$set": {"status": "rejected", "rejected_at": now}},
    )
    logger.info("[PAYMENT] Reject-all | %d records updated", result.modified_count)
    return result.modified_count


def is_premium(user_id: int) -> bool:
    """Return True if user has active premium (valid_until > now)."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    doc = _db["premium_users"].find_one(
        {"user_id": user_id},
        {"valid_until": 1},
    )
    if not doc:
        return False

    valid_until = doc.get("valid_until")
    if not valid_until:
        return False

    if valid_until.tzinfo is None:
        valid_until = valid_until.replace(tzinfo=timezone.utc)

    result = valid_until > datetime.now(tz=timezone.utc)
    logger.info("[PREMIUM] is_premium user_id=%s → %s", user_id, result)
    return result

# ─── Referral API ─────────────────────────────────────────────────────────────

def use_referral(referrer_id: int, referred_user_id: int) -> bool:
    """
    Track a referral when User 2 joins via User 1's link.

    Rules:
      - A user can only be referred once
      - No self-referral allowed

    NOTE: This only records the referral.
    Reward (24h access) is given to User 1 ONLY AFTER User 2
    completes shortlink verification — see reward_referrer().

    Returns True if recorded, False if duplicate or self-referral.
    """
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    if referrer_id == referred_user_id:
        logger.warning("[REFERRAL] Self-referral blocked | user_id=%s", referrer_id)
        return False

    existing = _db["referrals"].find_one({"referred_user": referred_user_id})
    if existing:
        logger.info("[REFERRAL] Already tracked | referred_user=%s", referred_user_id)
        return False

    now = datetime.now(tz=timezone.utc)
    _db["referrals"].insert_one({
        "referrer_id":   referrer_id,
        "referred_user": referred_user_id,
        "rewarded":      False,       # reward pending until User 2 verifies
        "timestamp":     now,
    })

    logger.info(
        "[REFERRAL] Tracked | referrer=%s → referred=%s | reward pending",
        referrer_id, referred_user_id,
    )
    return True


def reward_referrer(verified_user_id: int) -> int | None:
    """
    Called when a user completes shortlink verification.

    If this user was referred by someone:
      - Give that referrer 24-hour verified access (reward)
      - Mark referral as rewarded so it only fires once

    Returns referrer_id if reward was given, None otherwise.
    """
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    # Find pending referral for this user
    doc = _db["referrals"].find_one({
        "referred_user": verified_user_id,
        "rewarded": False,
    })

    if not doc:
        return None

    referrer_id = doc["referrer_id"]
    now         = datetime.now(tz=timezone.utc)

    # Give referrer 24-hour access
    _db["verified_users"].update_one(
        {"user_id": referrer_id},
        {"$set": {"user_id": referrer_id, "verified_at": now}},
        upsert=True,
    )

    # Mark referral as rewarded (prevent double reward)
    _db["referrals"].update_one(
        {"_id": doc["_id"]},
        {"$set": {"rewarded": True, "rewarded_at": now}},
    )

    logger.info(
        "[REFERRAL] Referrer rewarded | referrer=%s ← referred=%s | 24h access granted",
        referrer_id, verified_user_id,
    )
    return referrer_id
