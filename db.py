"""
db.py — MongoDB database layer for FilePe Bot
All async-compatible using motor (async pymongo driver)
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_URI, DB_NAME, VERIFICATION_HOURS, CACHE_TTL

logger = logging.getLogger(__name__)

# ─── Client ───────────────────────────────────────────────────────────────────
_client: Optional[AsyncIOMotorClient] = None


def get_db():
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(MONGO_URI)
    return _client[DB_NAME]


def db():
    return get_db()


# ─── Indexes (call on startup) ────────────────────────────────────────────────
async def create_indexes():
    d = db()
    await d.files.create_index("unique_id", unique=True)
    await d.files.create_index("created_at")
    await d.users.create_index("user_id", unique=True)
    await d.verified_users.create_index("user_id", unique=True)
    await d.verified_users.create_index("verified_at")
    await d.premium_users.create_index("user_id", unique=True)
    await d.premium_users.create_index("expires_at")
    await d.payments.create_index("user_id")
    await d.payments.create_index("utr", unique=True)
    await d.payments.create_index("status")
    await d.banned_users.create_index("user_id", unique=True)
    await d.clicks.create_index("unique_id")
    await d.clicks.create_index("user_id")
    await d.clicks.create_index("clicked_at")
    logger.info("✅ MongoDB indexes created")


# ─── Users ────────────────────────────────────────────────────────────────────
async def upsert_user(user_id: int, username: str = None, full_name: str = None):
    await db().users.update_one(
        {"user_id": user_id},
        {"$set": {
            "username": username,
            "full_name": full_name,
            "last_seen": datetime.now(timezone.utc),
        }, "$setOnInsert": {
            "user_id": user_id,
            "joined_at": datetime.now(timezone.utc),
        }},
        upsert=True,
    )


async def get_user(user_id: int) -> Optional[Dict]:
    return await db().users.find_one({"user_id": user_id})


async def get_total_users() -> int:
    return await db().users.count_documents({})


# ─── Files ────────────────────────────────────────────────────────────────────
async def save_file(unique_id: str, media: List[Dict], caption: str = None) -> bool:
    try:
        await db().files.insert_one({
            "unique_id": unique_id,
            "media": media,         # list of {file_id, type, file_name}
            "caption": caption,
            "views": 0,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        })
        return True
    except Exception as e:
        logger.error(f"save_file error: {e}")
        return False


async def get_file(unique_id: str) -> Optional[Dict]:
    return await db().files.find_one({"unique_id": unique_id})


async def increment_views(unique_id: str):
    await db().files.update_one(
        {"unique_id": unique_id},
        {"$inc": {"views": 1}}
    )


async def remove_broken_file_id(unique_id: str, broken_file_id: str):
    """Self-healing: remove a broken file_id from a file's media array."""
    await db().files.update_one(
        {"unique_id": unique_id},
        {"$pull": {"media": {"file_id": broken_file_id}}}
    )
    logger.warning(f"Removed broken file_id {broken_file_id} from {unique_id}")


async def delete_file(unique_id: str) -> bool:
    result = await db().files.delete_one({"unique_id": unique_id})
    return result.deleted_count > 0


async def get_all_files(skip: int = 0, limit: int = 20) -> List[Dict]:
    cursor = db().files.find({}, {"unique_id": 1, "views": 1, "created_at": 1}).sort(
        "created_at", -1).skip(skip).limit(limit)
    return await cursor.to_list(length=limit)


async def get_total_files() -> int:
    return await db().files.count_documents({})


# ─── Verified Users ───────────────────────────────────────────────────────────
async def set_verified(user_id: int):
    now = datetime.now(timezone.utc)
    await db().verified_users.update_one(
        {"user_id": user_id},
        {"$set": {
            "user_id": user_id,
            "verified_at": now,
            "expires_at": now + timedelta(hours=VERIFICATION_HOURS),
        }},
        upsert=True,
    )


async def is_verified(user_id: int) -> bool:
    doc = await db().verified_users.find_one({"user_id": user_id})
    if not doc:
        return False
    expires = doc.get("expires_at")
    if expires and expires.replace(tzinfo=timezone.utc) > datetime.now(timezone.utc):
        return True
    return False


async def cleanup_expired_verifications():
    result = await db().verified_users.delete_many(
        {"expires_at": {"$lt": datetime.now(timezone.utc)}}
    )
    logger.info(f"Cleaned {result.deleted_count} expired verifications")


# ─── Premium Users ────────────────────────────────────────────────────────────
async def grant_premium(user_id: int, days: int):
    now = datetime.now(timezone.utc)
    # Extend existing premium if active
    doc = await db().premium_users.find_one({"user_id": user_id})
    if doc and doc.get("expires_at", now).replace(tzinfo=timezone.utc) > now:
        new_expiry = doc["expires_at"].replace(tzinfo=timezone.utc) + timedelta(days=days)
    else:
        new_expiry = now + timedelta(days=days)

    await db().premium_users.update_one(
        {"user_id": user_id},
        {"$set": {
            "user_id": user_id,
            "expires_at": new_expiry,
            "updated_at": now,
        }},
        upsert=True,
    )
    return new_expiry


async def is_premium(user_id: int) -> bool:
    doc = await db().premium_users.find_one({"user_id": user_id})
    if not doc:
        return False
    expires = doc.get("expires_at")
    if expires and expires.replace(tzinfo=timezone.utc) > datetime.now(timezone.utc):
        return True
    return False


async def get_premium_info(user_id: int) -> Optional[Dict]:
    return await db().premium_users.find_one({"user_id": user_id})


# ─── Payments ─────────────────────────────────────────────────────────────────
async def create_payment(user_id: int, utr: str, plan: str, amount: int) -> bool:
    try:
        await db().payments.insert_one({
            "user_id": user_id,
            "utr": utr,
            "plan": plan,
            "amount": amount,
            "status": "pending",
            "created_at": datetime.now(timezone.utc),
        })
        return True
    except Exception as e:
        logger.error(f"create_payment error: {e}")
        return False


async def get_pending_payments(limit: int = 10) -> List[Dict]:
    cursor = db().payments.find({"status": "pending"}).sort("created_at", 1).limit(limit)
    return await cursor.to_list(length=limit)


async def get_payment_by_id(payment_id: str) -> Optional[Dict]:
    from bson import ObjectId
    return await db().payments.find_one({"_id": ObjectId(payment_id)})


async def approve_payment(payment_id: str) -> Optional[Dict]:
    from bson import ObjectId
    doc = await db().payments.find_one_and_update(
        {"_id": ObjectId(payment_id), "status": "pending"},
        {"$set": {"status": "approved", "approved_at": datetime.now(timezone.utc)}},
        return_document=True,
    )
    return doc


async def reject_payment(payment_id: str) -> Optional[Dict]:
    from bson import ObjectId
    doc = await db().payments.find_one_and_update(
        {"_id": ObjectId(payment_id), "status": "pending"},
        {"$set": {"status": "rejected", "rejected_at": datetime.now(timezone.utc)}},
        return_document=True,
    )
    return doc


async def utr_exists(utr: str) -> bool:
    return bool(await db().payments.find_one({"utr": utr}))


# ─── Banned Users ─────────────────────────────────────────────────────────────
async def ban_user(user_id: int, reason: str = ""):
    await db().banned_users.update_one(
        {"user_id": user_id},
        {"$set": {
            "user_id": user_id,
            "reason": reason,
            "banned_at": datetime.now(timezone.utc),
        }},
        upsert=True,
    )


async def unban_user(user_id: int):
    await db().banned_users.delete_one({"user_id": user_id})


async def is_banned(user_id: int) -> bool:
    return bool(await db().banned_users.find_one({"user_id": user_id}))


# ─── Click Tracking ───────────────────────────────────────────────────────────
async def log_click(unique_id: str, user_id: int, bot_username: str = None):
    await db().clicks.insert_one({
        "unique_id": unique_id,
        "user_id": user_id,
        "bot_username": bot_username,
        "clicked_at": datetime.now(timezone.utc),
    })


async def get_click_count(unique_id: str) -> int:
    return await db().clicks.count_documents({"unique_id": unique_id})


# ─── Stats ────────────────────────────────────────────────────────────────────
async def get_stats() -> Dict:
    total_users = await get_total_users()
    total_files = await get_total_files()
    total_premium = await db().premium_users.count_documents(
        {"expires_at": {"$gt": datetime.now(timezone.utc)}}
    )
    total_pending_payments = await db().payments.count_documents({"status": "pending"})
    total_banned = await db().banned_users.count_documents({})
    return {
        "users": total_users,
        "files": total_files,
        "premium": total_premium,
        "pending_payments": total_pending_payments,
        "banned": total_banned,
    }
