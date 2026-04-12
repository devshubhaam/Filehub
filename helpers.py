"""
helpers.py — Utility functions for the Telegram File Sharing Bot.

Provides:
  - extract_unique_id(caption) → str
  - generate_link(unique_id)   → str
"""

import logging
import random
import re
import string

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

_ID_PATTERN = re.compile(r"ID:\s*([a-zA-Z0-9_-]+)", re.IGNORECASE)
_ID_CHARS   = string.ascii_lowercase + string.digits   # a-z + 0-9
_ID_LENGTH  = 9                                        # 8–10 chars


# ─── Public API ───────────────────────────────────────────────────────────────

def extract_unique_id(caption: str | None) -> str:
    """
    Extract unique_id from a file caption.

    Rules:
      - If caption contains "ID: <value>"  → return <value>
      - Otherwise                          → return random 9-char alphanumeric ID

    Examples:
      "ID: test123"    → "test123"
      "ID:abc"         → "abc"
      "some text"      → "k9mxqr4a2"  (random)
      None             → "k9mxqr4a2"  (random)
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
    Generate a permanent deep-link for a file.

    Format: https://t.me/<bot_username>?start=file_<unique_id>

    Example:
      generate_link("test123", "hubfilerobot")
      → "https://t.me/hubfilerobot?start=file_test123"
    """
    link = f"https://t.me/{bot_username}?start=file_{unique_id}"
    logger.info("Generated permanent link: %s", link)
    return link


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _random_id() -> str:
    """Return a random lowercase alphanumeric string of length _ID_LENGTH."""
    return "".join(random.choices(_ID_CHARS, k=_ID_LENGTH))

