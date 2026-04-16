"""
helpers.py — Utility functions for FilePe Bot
"""

import asyncio
import logging
import random
import string
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

import aiohttp

from config import (
    SHORTLINK_API_URL, SHORTLINK_API_KEY, SHORTLINK_BASE,
    CACHE_TTL, RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW, AUTO_BAN_THRESHOLD,
    AUTO_DELETE_MINUTES, PREMIUM_PLANS, UPI_ID
)

logger = logging.getLogger(__name__)


# ─── In-Memory Cache ──────────────────────────────────────────────────────────
class TTLCache:
    """Simple TTL-based in-memory cache."""

    def __init__(self, ttl: int = CACHE_TTL):
        self._store: Dict[str, tuple] = {}
        self._ttl = ttl

    def get(self, key: str) -> Optional[Any]:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if time.monotonic() > expires_at:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any, ttl: int = None):
        ttl = ttl or self._ttl
        self._store[key] = (value, time.monotonic() + ttl)

    def delete(self, key: str):
        self._store.pop(key, None)

    def clear(self):
        self._store.clear()


# Global caches
verified_cache = TTLCache(ttl=300)   # 5 min
premium_cache = TTLCache(ttl=300)    # 5 min
file_cache = TTLCache(ttl=120)       # 2 min


# ─── Rate Limiter ─────────────────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, max_requests: int = RATE_LIMIT_REQUESTS, window: int = RATE_LIMIT_WINDOW):
        self._counts: Dict[int, List[float]] = defaultdict(list)
        self._max = max_requests
        self._window = window
        self._violations: Dict[int, int] = defaultdict(int)

    def check(self, user_id: int) -> tuple[bool, int]:
        """
        Returns (allowed: bool, violation_count: int)
        """
        now = time.monotonic()
        window_start = now - self._window
        requests = self._counts[user_id]

        # Remove old entries
        self._counts[user_id] = [r for r in requests if r > window_start]

        if len(self._counts[user_id]) >= self._max:
            self._violations[user_id] += 1
            return False, self._violations[user_id]

        self._counts[user_id].append(now)
        return True, 0

    def should_auto_ban(self, user_id: int) -> bool:
        return self._violations.get(user_id, 0) >= AUTO_BAN_THRESHOLD


rate_limiter = RateLimiter()


# ─── Unique ID Generator ──────────────────────────────────────────────────────
def generate_unique_id(length: int = 8) -> str:
    """Generate a random alphanumeric unique ID."""
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


# ─── Shortlink ────────────────────────────────────────────────────────────────
async def create_shortlink(original_url: str) -> Optional[str]:
    """
    Create a shortlink using the configured API.
    Returns the shortlink URL or None on failure.
    """
    if not SHORTLINK_API_URL or not SHORTLINK_API_KEY:
        logger.warning("Shortlink API not configured, returning original URL")
        return original_url

    try:
        params = {
            "api": SHORTLINK_API_KEY,
            "url": original_url,
        }
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(SHORTLINK_API_URL, params=params) as resp:
                data = await resp.json()
                if data.get("status") == "success":
                    return data.get("shortenedUrl", original_url)
                logger.error(f"Shortlink API error: {data}")
                return original_url
    except Exception as e:
        logger.error(f"Shortlink creation failed: {e}")
        return original_url


# ─── Telegram Deep Link ───────────────────────────────────────────────────────
def make_file_link(bot_username: str, unique_id: str) -> str:
    return f"https://t.me/{bot_username}?start=file_{unique_id}"


# ─── File Type Helpers ────────────────────────────────────────────────────────
def get_media_type(message) -> Optional[str]:
    if message.video:
        return "video"
    elif message.photo:
        return "photo"
    elif message.document:
        return "document"
    elif message.audio:
        return "audio"
    elif message.animation:
        return "animation"
    elif message.voice:
        return "voice"
    elif message.video_note:
        return "video_note"
    return None


def extract_media_info(message) -> Optional[Dict]:
    media_type = get_media_type(message)
    if not media_type:
        return None

    media_map = {
        "video": message.video,
        "photo": message.photo[-1] if message.photo else None,
        "document": message.document,
        "audio": message.audio,
        "animation": message.animation,
        "voice": message.voice,
        "video_note": message.video_note,
    }

    media = media_map.get(media_type)
    if not media:
        return None

    info = {
        "type": media_type,
        "file_id": media.file_id,
        "file_unique_id": media.file_unique_id,
    }

    # Optional fields
    if hasattr(media, "file_name"):
        info["file_name"] = media.file_name
    if hasattr(media, "file_size"):
        info["file_size"] = media.file_size

    return info


# ─── Auto Delete ─────────────────────────────────────────────────────────────
async def schedule_delete(bot, chat_id: int, message_ids: List[int], delay_minutes: int = AUTO_DELETE_MINUTES):
    """Schedule deletion of messages after delay."""
    await asyncio.sleep(delay_minutes * 60)
    for msg_id in message_ids:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception as e:
            logger.debug(f"Could not delete message {msg_id}: {e}")


# ─── Premium Plan Keyboard ────────────────────────────────────────────────────
def get_premium_plans_text() -> str:
    lines = ["💎 <b>FilePe Premium Plans</b>\n"]
    for key, plan in PREMIUM_PLANS.items():
        lines.append(f"• <b>{plan['label']}</b> — ₹{plan['price']}")
    lines.append(f"\n💳 UPI ID: <code>{UPI_ID}</code>")
    lines.append("\nPay and send your UTR number using /pay command.")
    return "\n".join(lines)


# ─── Human-Readable Size ──────────────────────────────────────────────────────
def human_size(size_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


# ─── Datetime Formatting ──────────────────────────────────────────────────────
def fmt_datetime(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%d %b %Y, %I:%M %p UTC")


def time_until(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = dt - datetime.now(timezone.utc)
    if delta.total_seconds() < 0:
        return "Expired"
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes = remainder // 60
    if days > 0:
        return f"{days}d {hours}h"
    elif hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"
