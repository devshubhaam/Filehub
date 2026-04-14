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

def save_file(unique_id: str, file_id: str, file_type: str = "document") -> str:
    """
    Upsert a file record supporting multiple media files per unique_id.

    Each entry in `media` list:
      { "file_id": str, "file_type": "video" | "photo" | "document" | "audio" }

    Returns:
      "inserted" — new document created
      "updated"  — file added to existing document
      "exists"   — file_id already present, no change
    """
    col = get_files_collection()
    now = datetime.now(tz=timezone.utc)

    existing = col.find_one({"unique_id": unique_id})
    media_entry = {"file_id": file_id, "file_type": file_type}

    if existing:
        # Check duplicate by file_id
        existing_ids = [m.get("file_id") for m in existing.get("media", [])]
        # Backward compat — also check legacy file_ids list
        legacy_ids   = existing.get("file_ids", [])
        if file_id in existing_ids or file_id in legacy_ids:
            logger.info("file_id already exists for unique_id=%s -- skipping", unique_id)
            return "exists"

        col.update_one(
            {"unique_id": unique_id},
            {
                "$push": {"media": media_entry},
                "$set":  {"updated_at": now},
            },
        )
        logger.info("Updated unique_id=%s | added %s file OK", unique_id, file_type)
        return "updated"

    col.insert_one(
        {
            "unique_id":  unique_id,
            "media":      [media_entry],
            "file_ids":   [],            # kept for backward compat
            "views":      0,
            "created_at": now,
            "updated_at": now,
        }
    )
    logger.info("Inserted new record unique_id=%s | type=%s OK", unique_id, file_type)
    return "inserted"


def get_file(unique_id: str) -> dict | None:
    """Fetch a file document by unique_id. Returns None if not found."""
    col = get_files_collection()
    doc = col.find_one({"unique_id": unique_id})
    if doc:
        media_count = len(doc.get("media", doc.get("file_ids", [])))
        logger.info(
            "Found unique_id=%s -- %d media item(s) | views=%d",
            unique_id, media_count, doc.get("views", 0),
        )
    else:
        logger.warning("unique_id=%s not found in DB", unique_id)
    return doc


def remove_file_id(unique_id: str, file_id: str) -> None:
    """
    Remove a dead media entry by file_id.
    Works on both new `media` array and legacy `file_ids` array.
    """
    col = get_files_collection()
    now = datetime.now(tz=timezone.utc)

    # Remove from new media array
    r1 = col.update_one(
        {"unique_id": unique_id},
        {
            "$pull": {"media": {"file_id": file_id}},
            "$set":  {"updated_at": now},
        },
    )
    # Also remove from legacy file_ids if present
    col.update_one(
        {"unique_id": unique_id},
        {"$pull": {"file_ids": file_id}},
    )

    if r1.modified_count:
        logger.info("[SELF-HEAL] Removed dead file_id=%s from unique_id=%s", file_id, unique_id)
    else:
        logger.warning("[SELF-HEAL] file_id=%s not found in unique_id=%s", file_id, unique_id)


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

# ─── Database Cleanup ─────────────────────────────────────────────────────────

def run_cleanup() -> dict:
    """
    Safe database cleanup — runs on bot startup.

    Deletes ONLY data that:
      1. Is expired / no longer useful
      2. Cannot cause any abuse if removed

    NEVER deletes:
      - referrals       (fraud prevention)
      - users           (tracking history)
      - premium_users   (paid access records)
      - files           (actual file data)
      - payments with status=pending (active reviews)

    Returns dict with count of deleted documents per collection.
    """
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    from datetime import timedelta
    now     = datetime.now(tz=timezone.utc)
    results = {}

    # 1. verified_users — delete expired (> 25 hours old, extra 1h buffer)
    cutoff_24h = now - timedelta(hours=25)
    r = _db["verified_users"].delete_many({"verified_at": {"$lt": cutoff_24h}})
    results["verified_users"] = r.deleted_count
    logger.info("[CLEANUP] verified_users: deleted %d expired records", r.deleted_count)

    # 2. clicks — delete older than 7 days
    cutoff_7d = now - timedelta(days=7)
    r = _db["clicks"].delete_many({"timestamp": {"$lt": cutoff_7d}})
    results["clicks"] = r.deleted_count
    logger.info("[CLEANUP] clicks: deleted %d old records", r.deleted_count)

    # 3. files — delete smoke-test docs (no unique_id field)
    r = _db["files"].delete_many({"test": "ok"})
    results["files_test"] = r.deleted_count
    logger.info("[CLEANUP] files: deleted %d smoke-test docs", r.deleted_count)

    # 4. payments — delete approved/rejected older than 90 days
    #    KEEP: pending (active), and all recent records for UTR duplicate check
    cutoff_90d = now - timedelta(days=90)
    r = _db["payments"].delete_many({
        "status":    {"$in": ["approved", "rejected"]},
        "timestamp": {"$lt": cutoff_90d},
    })
    results["payments"] = r.deleted_count
    logger.info("[CLEANUP] payments: deleted %d old approved/rejected records", r.deleted_count)

    total = sum(results.values())
    logger.info("[CLEANUP] Done — total %d documents removed | breakdown: %s", total, results)
    return results

# ─── Admin Stats API ──────────────────────────────────────────────────────────

def get_bot_stats() -> dict:
    """Return key stats for admin /mystats command."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    from datetime import timedelta
    now = datetime.now(tz=timezone.utc)

    total_users     = _db["users"].count_documents({})
    total_premium   = _db["premium_users"].count_documents(
        {"valid_until": {"$gt": now}}
    )
    pending_payments = _db["payments"].count_documents({"status": "pending"})
    total_referrals  = _db["referrals"].count_documents({})
    total_files      = _db["files"].count_documents({"unique_id": {"$exists": True}})
    total_views      = sum(
        d.get("views", 0)
        for d in _db["files"].find({"unique_id": {"$exists": True}}, {"views": 1})
    )

    return {
        "total_users":      total_users,
        "total_premium":    total_premium,
        "pending_payments": pending_payments,
        "total_referrals":  total_referrals,
        "total_files":      total_files,
        "total_views":      total_views,
    }


def get_file_stats(unique_id: str) -> dict | None:
    """Return stats for a specific file."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    doc = _db["files"].find_one({"unique_id": unique_id})
    if not doc:
        return None
    return {
        "unique_id":   unique_id,
        "views":       doc.get("views", 0),
        "media_count": len(doc.get("media", doc.get("file_ids", []))),
        "created_at":  doc.get("created_at"),
        "updated_at":  doc.get("updated_at"),
    }


def get_user_status(user_id: int) -> dict:
    """Return full status for a user — premium, verified, referrals."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    from datetime import timedelta
    now = datetime.now(tz=timezone.utc)

    # Premium
    premium_doc   = _db["premium_users"].find_one({"user_id": user_id})
    premium_active = False
    premium_days_left = 0
    if premium_doc:
        valid_until = premium_doc.get("valid_until")
        if valid_until:
            if valid_until.tzinfo is None:
                valid_until = valid_until.replace(tzinfo=timezone.utc)
            if valid_until > now:
                premium_active    = True
                premium_days_left = (valid_until - now).days

    # 24h verified
    verified_doc = _db["verified_users"].find_one({"user_id": user_id})
    verified_active = False
    verified_hours_left = 0
    if verified_doc:
        verified_at = verified_doc.get("verified_at")
        if verified_at:
            if verified_at.tzinfo is None:
                verified_at = verified_at.replace(tzinfo=timezone.utc)
            elapsed = (now - verified_at).total_seconds() / 3600
            if elapsed < 24:
                verified_active     = True
                verified_hours_left = int(24 - elapsed)

    # Referrals made
    referrals_made = _db["referrals"].count_documents({"referrer_id": user_id})

    return {
        "premium_active":     premium_active,
        "premium_days_left":  premium_days_left,
        "verified_active":    verified_active,
        "verified_hours_left": verified_hours_left,
        "referrals_made":     referrals_made,
    }


# ─── Ban System ───────────────────────────────────────────────────────────────

def ban_user(user_id: int) -> None:
    """Add user to banned_users collection."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")
    _db["banned_users"].update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "banned_at": datetime.now(tz=timezone.utc)}},
        upsert=True,
    )
    logger.info("[BAN] Banned user_id=%s", user_id)


def unban_user(user_id: int) -> None:
    """Remove user from banned_users collection."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")
    _db["banned_users"].delete_one({"user_id": user_id})
    logger.info("[BAN] Unbanned user_id=%s", user_id)


def is_banned(user_id: int) -> bool:
    """Return True if user is banned."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")
    return _db["banned_users"].find_one({"user_id": user_id}) is not None


# ─── Force Join ───────────────────────────────────────────────────────────────

def get_force_join_channel() -> str:
    """Return force-join channel username from DB config."""
    if _db is None:
        return ""
    doc = _db["config"].find_one({"key": "force_join_channel"})
    return doc.get("value", "") if doc else ""


# ─── Broadcast ────────────────────────────────────────────────────────────────

def get_all_user_ids() -> list[int]:
    """Return all user IDs for broadcast."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")
    return [d["user_id"] for d in _db["users"].find({}, {"user_id": 1})]

# ─── Multi-Plan Premium ───────────────────────────────────────────────────────

PREMIUM_PLANS = {
    "7":  {"days": 7,  "amount": 19,  "label": "⚡ 7 Days"},
    "30": {"days": 30, "amount": 49,  "label": "💎 30 Days"},
    "90": {"days": 90, "amount": 99,  "label": "👑 90 Days"},
}


def submit_payment_plan(user_id: int, utr: str, plan: str = "30") -> str:
    """Store payment with plan info. Returns 'ok' or 'duplicate'."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    if _db["payments"].find_one({"utr": utr}):
        return "duplicate"

    now = datetime.now(tz=timezone.utc)
    _db["payments"].insert_one({
        "user_id":   user_id,
        "utr":       utr,
        "plan":      plan,
        "status":    "pending",
        "timestamp": now,
    })
    logger.info("[PAYMENT] Submitted | user_id=%s | utr=%s | plan=%s", user_id, utr, plan)
    return "ok"


def approve_payment_plan(user_id: int, utr: str) -> int:
    """Approve payment, grant premium based on plan. Returns days granted."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    from datetime import timedelta
    now = datetime.now(tz=timezone.utc)

    doc = _db["payments"].find_one({"user_id": user_id, "utr": utr})
    plan = doc.get("plan", "30") if doc else "30"
    days = PREMIUM_PLANS.get(plan, PREMIUM_PLANS["30"])["days"]

    # Extend existing premium if active
    existing = _db["premium_users"].find_one({"user_id": user_id})
    if existing:
        current = existing.get("valid_until", now)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        base = max(current, now)  # extend from current expiry
    else:
        base = now

    valid_until = base + timedelta(days=days)

    _db["payments"].update_one(
        {"user_id": user_id, "utr": utr},
        {"$set": {"status": "approved", "approved_at": now}},
    )
    _db["premium_users"].update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "valid_until": valid_until, "plan": plan}},
        upsert=True,
    )
    logger.info("[PREMIUM] Approved %d days | user_id=%s | until=%s", days, user_id, valid_until.isoformat())
    return days


# ─── Free Trial ───────────────────────────────────────────────────────────────

def use_free_trial(user_id: int) -> bool:
    """
    Give user 2-hour free access. One-time only.
    Returns True if trial applied, False if already used.
    """
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    if _db["trials"].find_one({"user_id": user_id}):
        return False

    now = datetime.now(tz=timezone.utc)
    _db["trials"].insert_one({"user_id": user_id, "used_at": now})

    # Give 2 hours verified access
    trial_until = now
    from datetime import timedelta
    trial_at = now - timedelta(hours=22)  # 24h - 22h = 2h remaining
    _db["verified_users"].update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "verified_at": trial_at}},
        upsert=True,
    )
    logger.info("[TRIAL] Free trial granted | user_id=%s", user_id)
    return True


# ─── Referral Milestone Rewards ──────────────────────────────────────────────

REFERRAL_MILESTONES = {3: 7, 10: 30}  # referrals: premium days


def check_referral_milestone(referrer_id: int) -> int | None:
    """
    Check if referrer hit a milestone (3 or 10 successful referrals).
    Returns days of premium to grant, or None if no milestone hit.
    """
    if _db is None:
        return None

    count = _db["referrals"].count_documents({
        "referrer_id": referrer_id,
        "rewarded": True,
    })

    # Check if this exact count is a milestone (not already rewarded)
    if count in REFERRAL_MILESTONES:
        milestone_key = f"milestone_{count}"
        already = _db["referral_milestones"].find_one({
            "user_id": referrer_id, "milestone": count
        })
        if not already:
            days = REFERRAL_MILESTONES[count]
            from datetime import timedelta
            now = datetime.now(tz=timezone.utc)
            existing = _db["premium_users"].find_one({"user_id": referrer_id})
            if existing:
                current = existing.get("valid_until", now)
                if current.tzinfo is None:
                    current = current.replace(tzinfo=timezone.utc)
                base = max(current, now)
            else:
                base = now
            valid_until = base + timedelta(days=days)
            _db["premium_users"].update_one(
                {"user_id": referrer_id},
                {"$set": {"user_id": referrer_id, "valid_until": valid_until}},
                upsert=True,
            )
            _db["referral_milestones"].insert_one({
                "user_id": referrer_id, "milestone": count, "days": days, "at": now
            })
            logger.info("[REFERRAL] Milestone %d hit | user_id=%s | +%d days premium",
                        count, referrer_id, days)
            return days
    return None


# ─── Viral Share ──────────────────────────────────────────────────────────────

def use_viral_share(sharer_id: int) -> bool:
    """
    Give 24h access when user shares a file link.
    One reward per user per day.
    """
    if _db is None:
        return False

    from datetime import timedelta
    now   = datetime.now(tz=timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    already = _db["viral_shares"].find_one({
        "user_id":  sharer_id,
        "shared_at": {"$gte": today},
    })
    if already:
        return False

    _db["viral_shares"].insert_one({"user_id": sharer_id, "shared_at": now})
    _db["verified_users"].update_one(
        {"user_id": sharer_id},
        {"$set": {"user_id": sharer_id, "verified_at": now}},
        upsert=True,
    )
    logger.info("[VIRAL] Share reward | user_id=%s", sharer_id)
    return True


# ─── Myreferrals ─────────────────────────────────────────────────────────────

def get_referral_stats(user_id: int) -> dict:
    """Return referral stats for a user."""
    if _db is None:
        return {}

    total    = _db["referrals"].count_documents({"referrer_id": user_id})
    rewarded = _db["referrals"].count_documents({"referrer_id": user_id, "rewarded": True})
    pending  = total - rewarded
    next_milestone = None
    for m in sorted(REFERRAL_MILESTONES.keys()):
        if rewarded < m:
            next_milestone = m
            break
    return {
        "total":          total,
        "rewarded":       rewarded,
        "pending":        pending,
        "next_milestone": next_milestone,
        "milestones":     REFERRAL_MILESTONES,
    }


# ─── Cancel Payment ───────────────────────────────────────────────────────────

def cancel_payment(user_id: int) -> int:
    """Cancel all pending payments for a user. Returns count cancelled."""
    if _db is None:
        return 0

    now    = datetime.now(tz=timezone.utc)
    result = _db["payments"].update_many(
        {"user_id": user_id, "status": "pending"},
        {"$set": {"status": "cancelled", "cancelled_at": now}},
    )
    logger.info("[PAYMENT] Cancelled %d payments for user_id=%s", result.modified_count, user_id)
    return result.modified_count


# ─── Top Files ────────────────────────────────────────────────────────────────

def get_top_files(limit: int = 10) -> list:
    """Return top files sorted by views."""
    if _db is None:
        return []

    return list(_db["files"].find(
        {"unique_id": {"$exists": True}},
        {"unique_id": 1, "views": 1, "media": 1, "created_at": 1},
    ).sort("views", -1).limit(limit))


# ─── Daily Report ─────────────────────────────────────────────────────────────

def get_daily_report() -> dict:
    """Stats for the last 24 hours."""
    if _db is None:
        return {}

    from datetime import timedelta
    now      = datetime.now(tz=timezone.utc)
    since    = now - timedelta(hours=24)

    new_users    = _db["users"].count_documents({"first_seen": {"$gte": since}})
    new_premium  = _db["payments"].count_documents({"status": "approved", "approved_at": {"$gte": since}})
    new_payments = _db["payments"].count_documents({"timestamp": {"$gte": since}})
    files_accessed = _db["verified_users"].count_documents({"verified_at": {"$gte": since}})
    active_premium = _db["premium_users"].count_documents({"valid_until": {"$gt": now}})
    total_users    = _db["users"].count_documents({})

    return {
        "new_users":      new_users,
        "new_premium":    new_premium,
        "new_payments":   new_payments,
        "files_accessed": files_accessed,
        "active_premium": active_premium,
        "total_users":    total_users,
        "date":           now.strftime("%d %b %Y"),
    }


# ─── Extend Premium ───────────────────────────────────────────────────────────

def extend_premium(user_id: int, days: int) -> datetime:
    """Extend premium by days from current expiry or now."""
    if _db is None:
        raise RuntimeError("Database not initialised — call init_db() first.")

    from datetime import timedelta
    now      = datetime.now(tz=timezone.utc)
    existing = _db["premium_users"].find_one({"user_id": user_id})

    if existing:
        current = existing.get("valid_until", now)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        base = max(current, now)
    else:
        base = now

    valid_until = base + timedelta(days=days)
    _db["premium_users"].update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "valid_until": valid_until}},
        upsert=True,
    )
    logger.info("[PREMIUM] Extended +%d days | user_id=%s | until=%s", days, user_id, valid_until.isoformat())
    return valid_until

# ═══════════════════════════════════════════════════════════════════════════════
# NEW FEATURES — Subscription Reminders, Coins, Bookmarks, Paid Files,
#                Revenue, Analytics, Scheduled Broadcast, Forward Protection
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Subscription Reminders ───────────────────────────────────────────────────

def get_users_expiring_in(hours_min: int, hours_max: int) -> list[dict]:
    """
    Return premium users expiring between hours_min and hours_max from now.
    Used for reminder system.
    """
    if _db is None:
        return []
    from datetime import timedelta
    now       = datetime.now(tz=timezone.utc)
    from_dt   = now + timedelta(hours=hours_min)
    to_dt     = now + timedelta(hours=hours_max)
    return list(_db["premium_users"].find({
        "valid_until": {"$gte": from_dt, "$lt": to_dt}
    }))


def mark_reminder_sent(user_id: int, reminder_type: str) -> None:
    """Mark that a reminder was sent so we don't send it again."""
    if _db is None:
        return
    _db["reminder_log"].update_one(
        {"user_id": user_id, "type": reminder_type},
        {"$set": {"user_id": user_id, "type": reminder_type,
                  "sent_at": datetime.now(tz=timezone.utc)}},
        upsert=True,
    )


def reminder_already_sent(user_id: int, reminder_type: str) -> bool:
    """Check if reminder was already sent this cycle."""
    if _db is None:
        return False
    from datetime import timedelta
    now   = datetime.now(tz=timezone.utc)
    since = now - timedelta(days=2)
    return _db["reminder_log"].find_one({
        "user_id": user_id,
        "type":    reminder_type,
        "sent_at": {"$gte": since},
    }) is not None


# ─── Paid File Access ─────────────────────────────────────────────────────────

def set_file_price(unique_id: str, price: int) -> None:
    """Set a per-file price in rupees. 0 = free."""
    if _db is None:
        return
    get_files_collection().update_one(
        {"unique_id": unique_id},
        {"$set": {"price": price}},
    )
    logger.info("[PRICE] Set price=%d for unique_id=%s", price, unique_id)


def get_file_price(unique_id: str) -> int:
    """Return file price. 0 = free."""
    if _db is None:
        return 0
    doc = get_files_collection().find_one({"unique_id": unique_id}, {"price": 1})
    return doc.get("price", 0) if doc else 0


def has_paid_for_file(user_id: int, unique_id: str) -> bool:
    """Check if user has already paid for this specific file."""
    if _db is None:
        return False
    return _db["file_purchases"].find_one(
        {"user_id": user_id, "unique_id": unique_id}
    ) is not None


def record_file_purchase(user_id: int, unique_id: str, amount: int) -> None:
    """Record a file purchase after admin approval."""
    if _db is None:
        return
    now = datetime.now(tz=timezone.utc)
    _db["file_purchases"].insert_one({
        "user_id":   user_id,
        "unique_id": unique_id,
        "amount":    amount,
        "paid_at":   now,
    })
    logger.info("[FILE-PURCHASE] user_id=%s bought unique_id=%s for ₹%d",
                user_id, unique_id, amount)


# ─── Coins System ─────────────────────────────────────────────────────────────

COIN_REWARDS = {
    "daily_checkin": 10,
    "referral":      50,
    "share":         20,
    "purchase":      5,
}
COINS_FOR_ACCESS = 100  # coins needed for 24h access


def get_coins(user_id: int) -> int:
    """Return user's coin balance."""
    if _db is None:
        return 0
    doc = _db["coins"].find_one({"user_id": user_id}, {"balance": 1})
    return doc.get("balance", 0) if doc else 0


def add_coins(user_id: int, amount: int, reason: str) -> int:
    """Add coins to user balance. Returns new balance."""
    if _db is None:
        return 0
    now = datetime.now(tz=timezone.utc)
    result = _db["coins"].find_one_and_update(
        {"user_id": user_id},
        {
            "$inc": {"balance": amount},
            "$push": {"transactions": {"amount": amount, "reason": reason, "at": now}},
        },
        upsert=True,
        return_document=True,
    )
    balance = result.get("balance", amount) if result else amount
    logger.info("[COINS] +%d to user_id=%s (%s) | balance=%d", amount, user_id, reason, balance)
    return balance


def spend_coins(user_id: int, amount: int, reason: str) -> bool:
    """
    Spend coins from balance. Returns True if successful, False if insufficient.
    """
    if _db is None:
        return False
    doc = _db["coins"].find_one({"user_id": user_id})
    balance = doc.get("balance", 0) if doc else 0
    if balance < amount:
        return False
    now = datetime.now(tz=timezone.utc)
    _db["coins"].update_one(
        {"user_id": user_id},
        {
            "$inc": {"balance": -amount},
            "$push": {"transactions": {"amount": -amount, "reason": reason, "at": now}},
        },
    )
    logger.info("[COINS] -%d from user_id=%s (%s) | was=%d", amount, user_id, reason, balance)
    return True


def daily_checkin(user_id: int) -> tuple[bool, int]:
    """
    Daily check-in. Returns (applied: bool, coins_earned: int).
    Only once per calendar day.
    """
    if _db is None:
        return False, 0
    from datetime import timedelta
    now   = datetime.now(tz=timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    already = _db["checkins"].find_one({
        "user_id":    user_id,
        "checked_in": {"$gte": today},
    })
    if already:
        return False, 0
    earned = COIN_REWARDS["daily_checkin"]
    _db["checkins"].insert_one({"user_id": user_id, "checked_in": now})
    new_balance = add_coins(user_id, earned, "daily_checkin")
    return True, earned


# ─── Bookmarks ────────────────────────────────────────────────────────────────

def save_bookmark(user_id: int, unique_id: str) -> bool:
    """Save a file bookmark. Returns False if already saved."""
    if _db is None:
        return False
    existing = _db["bookmarks"].find_one(
        {"user_id": user_id, "unique_id": unique_id}
    )
    if existing:
        return False
    _db["bookmarks"].insert_one({
        "user_id":   user_id,
        "unique_id": unique_id,
        "saved_at":  datetime.now(tz=timezone.utc),
    })
    logger.info("[BOOKMARK] user_id=%s saved unique_id=%s", user_id, unique_id)
    return True


def get_bookmarks(user_id: int) -> list[str]:
    """Return list of bookmarked unique_ids for user."""
    if _db is None:
        return []
    docs = list(_db["bookmarks"].find(
        {"user_id": user_id},
        {"unique_id": 1},
    ).sort("saved_at", -1).limit(20))
    return [d["unique_id"] for d in docs]


def remove_bookmark(user_id: int, unique_id: str) -> bool:
    """Remove a bookmark. Returns True if removed."""
    if _db is None:
        return False
    result = _db["bookmarks"].delete_one({"user_id": user_id, "unique_id": unique_id})
    return result.deleted_count > 0


# ─── IP Rate Limiting ─────────────────────────────────────────────────────────

_ip_requests: dict = {}


def check_ip_rate(ip: str, max_per_min: int = 30) -> bool:
    """
    Return True if IP is rate-limited (too many requests).
    Cleans old timestamps automatically.
    """
    import time as _time
    now    = _time.time()
    window = now - 60

    if ip not in _ip_requests:
        _ip_requests[ip] = []

    _ip_requests[ip] = [ts for ts in _ip_requests[ip] if ts > window]

    if len(_ip_requests[ip]) >= max_per_min:
        logger.warning("[IP-LIMIT] IP %s exceeded %d req/min", ip, max_per_min)
        return True

    _ip_requests[ip].append(now)
    return False


# ─── Forward Protection Settings ─────────────────────────────────────────────

def set_forward_protection(enabled: bool) -> None:
    """Global forward protection toggle stored in DB config."""
    if _db is None:
        return
    _db["config"].update_one(
        {"key": "forward_protection"},
        {"$set": {"key": "forward_protection", "value": enabled}},
        upsert=True,
    )
    logger.info("[PROTECT] Forward protection set to %s", enabled)


def get_forward_protection() -> bool:
    """Return True if forward protection is enabled."""
    if _db is None:
        return False
    doc = _db["config"].find_one({"key": "forward_protection"})
    return doc.get("value", False) if doc else False


# ─── Revenue Dashboard ────────────────────────────────────────────────────────

def get_revenue_stats() -> dict:
    """Aggregate revenue from approved payments."""
    if _db is None:
        return {}
    from datetime import timedelta
    now   = datetime.now(tz=timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week  = now - timedelta(days=7)
    month = now - timedelta(days=30)

    def _sum_revenue(query: dict) -> int:
        pipeline = [
            {"$match": {**query, "status": "approved"}},
            {"$lookup": {
                "from":         "payments",
                "localField":   "utr",
                "foreignField": "utr",
                "as":           "pay",
            }},
        ]
        total = 0
        plan_prices = {"7": 19, "30": 49, "90": 99}
        docs = list(_db["payments"].find({**query, "status": "approved"}))
        for d in docs:
            total += plan_prices.get(d.get("plan", "30"), 49)
        return total

    today_rev = _sum_revenue({"approved_at": {"$gte": today}})
    week_rev  = _sum_revenue({"approved_at": {"$gte": week}})
    month_rev = _sum_revenue({"approved_at": {"$gte": month}})
    total_rev = _sum_revenue({})

    # Plan breakdown
    plan_counts = {}
    for plan in ("7", "30", "90"):
        plan_counts[plan] = _db["payments"].count_documents(
            {"status": "approved", "plan": plan}
        )

    pending_count = _db["payments"].count_documents({"status": "pending"})

    return {
        "today":         today_rev,
        "week":          week_rev,
        "month":         month_rev,
        "total":         total_rev,
        "plan_counts":   plan_counts,
        "pending_count": pending_count,
    }


# ─── User Journey / Event Tracking ────────────────────────────────────────────

def log_event(user_id: int, action: str, meta: dict | None = None) -> None:
    """Log a user action event for analytics."""
    if _db is None:
        return
    _db["events"].insert_one({
        "user_id":   user_id,
        "action":    action,
        "meta":      meta or {},
        "timestamp": datetime.now(tz=timezone.utc),
    })


def get_user_journey(user_id: int, limit: int = 20) -> list[dict]:
    """Return recent events for a user."""
    if _db is None:
        return []
    return list(_db["events"].find(
        {"user_id": user_id},
        {"action": 1, "meta": 1, "timestamp": 1},
    ).sort("timestamp", -1).limit(limit))


# ─── Scheduled Broadcast ─────────────────────────────────────────────────────

def schedule_broadcast(message: str, scheduled_at: datetime, admin_id: int) -> str:
    """Schedule a broadcast message. Returns doc ID."""
    if _db is None:
        return ""
    result = _db["scheduled_broadcasts"].insert_one({
        "message":      message,
        "scheduled_at": scheduled_at,
        "admin_id":     admin_id,
        "status":       "pending",
        "created_at":   datetime.now(tz=timezone.utc),
        "sent_count":   0,
        "fail_count":   0,
        "retries":      0,
    })
    logger.info("[SCHEDULE] Broadcast scheduled at %s by admin=%s",
                scheduled_at.isoformat(), admin_id)
    return str(result.inserted_id)


def get_pending_broadcasts() -> list[dict]:
    """Return broadcasts due to be sent now."""
    if _db is None:
        return []
    now = datetime.now(tz=timezone.utc)
    return list(_db["scheduled_broadcasts"].find({
        "status":       "pending",
        "scheduled_at": {"$lte": now},
        "retries":      {"$lt": 3},
    }))


def mark_broadcast_done(doc_id, sent: int, failed: int) -> None:
    """Mark a scheduled broadcast as completed."""
    if _db is None:
        return
    from bson import ObjectId
    _db["scheduled_broadcasts"].update_one(
        {"_id": ObjectId(doc_id)},
        {"$set": {
            "status":     "done",
            "sent_count": sent,
            "fail_count": failed,
            "done_at":    datetime.now(tz=timezone.utc),
        }},
    )


def mark_broadcast_retry(doc_id) -> None:
    """Increment retry count for a failed broadcast."""
    if _db is None:
        return
    from bson import ObjectId
    _db["scheduled_broadcasts"].update_one(
        {"_id": ObjectId(doc_id)},
        {
            "$inc": {"retries": 1},
            "$set": {"last_retry": datetime.now(tz=timezone.utc)},
        },
    )
