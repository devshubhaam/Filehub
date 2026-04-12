"""
helpers.py — Utility functions for the Telegram File Sharing Bot.

Provides:
  - extract_unique_id(caption) → str
  - generate_link(unique_id)   → str
  - generate_shortlink(unique_id) → str
"""

import logging
import os
import random
import re
import string

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

_ID_PATTERN = re.compile(r"ID:\s*([a-zA-Z0-9_-]+)", re.IGNORECASE)
_ID_CHARS   = string.ascii_lowercase + string.digits
_ID_LENGTH  = 9


# ─── Public API ───────────────────────────────────────────────────────────────

def extract_unique_id(caption: str | None) -> str:
    """
    Extract unique_id from a file caption.

    Rules:
      - "ID: <value>" found → return <value>
      - Not found           → return random 9-char alphanumeric ID
    """
    if caption:
        match = _ID_PATTERN.search(caption)
        if match:
            unique_id = match.group(1).strip()
            logger.info("Extracted unique_id from caption: %s", unique_id)
            return unique_id

    unique_id = _random_id()
    logger.info("No ID in caption — generated random unique_id: %s", unique_id)
    return unique_id


def generate_link(unique_id: str, bot_username: str) -> str:
    """
    Generate a permanent Telegram deep-link for a file.
    Format: https://t.me/<bot_username>?start=file_<unique_id>
    """
    link = f"https://t.me/{bot_username}?start=file_{unique_id}"
    logger.info("Generated permanent link: %s", link)
    return link


    try:
        resp = http_requests.get(
            api_url,
            params={"api": api_key, "url": target_url},
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()

        # GPLinks / most providers return shortenedUrl or short_url
        short = (
            data.get("shortenedUrl")
            or data.get("short_url")
            or data.get("shortlink")
            or data.get("result", {}).get("url")
        )

        if short:
            logger.info("[SHORTLINK] Generated: %s → %s", target_url, short)
            return short

        logger.warning("[SHORTLINK] Unexpected API response: %s", data)

    except Exception as exc:
        logger.error("[SHORTLINK] API call failed: %s — using fallback", exc)

    # Fallback: return raw verify URL
    logger.info("[SHORTLINK] Falling back to direct URL: %s", target_url)
    return target_url


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _random_id() -> str:
    """Return a random lowercase alphanumeric string of length _ID_LENGTH."""
    return "".join(random.choices(_ID_CHARS, k=_ID_LENGTH))
