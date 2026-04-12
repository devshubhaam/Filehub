"""
helpers.py — Utility functions for the Telegram File Sharing Bot.

Provides:
  - extract_unique_id(caption) → str
  - generate_link(unique_id, bot_username) → str
  - generate_shortlink(long_url) → str   (Linkshortify)
"""

import logging
import os
import random
import re
import string

import requests as http_requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

_ID_PATTERN = re.compile(r"ID:\s*([a-zA-Z0-9_-]+)", re.IGNORECASE)
_ID_CHARS   = string.ascii_lowercase + string.digits
_ID_LENGTH  = 9


# ─── Public API ───────────────────────────────────────────────────────────────

def extract_unique_id(caption: str | None) -> str:
    """
    Extract unique_id from a file caption.
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


def generate_shortlink(long_url: str) -> str:
    """
    Shorten a URL using Linkshortify API.

    API format:
      GET https://linkshortify.com/api
      Params: api=<key>, url=<target>, format=json

    Success response:
      { "status": "success", "shortenedUrl": "https://lksfy.com/xxxxx" }

    Fallback: returns long_url unchanged if API fails or key missing.
    """
    api_key = os.environ.get("SHORTLINK_API", "")

    if not api_key:
        logger.warning("[SHORTLINK] SHORTLINK_API not set — using direct URL")
        return long_url

    try:
        resp = http_requests.get(
            "https://linkshortify.com/api",
            params={"api": api_key, "url": long_url, "format": "json"},
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "success":
            short = data.get("shortenedUrl", "")
            if short:
                logger.info("[SHORTLINK] %s → %s", long_url, short)
                return short

        logger.warning("[SHORTLINK] Unexpected response: %s", data)

    except Exception as exc:
        logger.error("[SHORTLINK] API error: %s — using fallback", exc)

    logger.info("[SHORTLINK] Fallback — returning original URL")
    return long_url


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _random_id() -> str:
    """Return a random lowercase alphanumeric string of length _ID_LENGTH."""
    return "".join(random.choices(_ID_CHARS, k=_ID_LENGTH))
