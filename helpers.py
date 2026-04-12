"""
helpers.py — Utility functions for the Telegram File Sharing Bot.

Provides:
  - extract_unique_id(caption) → str
  - generate_link(unique_id)   → str
  - shorten_url(url)           → str   ← NEW: GPlink / VPlink / LinkPays / generic
"""

import logging
import os
import random
import re
import string

import requests as http_requests

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

_ID_PATTERN = re.compile(r"ID:\s*([a-zA-Z0-9_-]+)", re.IGNORECASE)
_ID_CHARS   = string.ascii_lowercase + string.digits
_ID_LENGTH  = 9

# ─── Supported Shortener Providers ───────────────────────────────────────────
#
#  Set in .env:
#    SHORTENER_API_URL  = https://gplinks.in/api      (or vplink / linkpays etc.)
#    SHORTENER_API_KEY  = your_api_key_here
#    SHORTENER_PROVIDER = gplinks   (gplinks | vplink | linkpays | generic)
#
# Provider-specific response key mapping:
_PROVIDER_KEYS = {
    "gplinks":  ["shortenedUrl", "short_url", "shortlink"],
    "vplink":   ["shortenedUrl", "short_url", "shortlink"],
    "linkpays": ["short_url", "shortenedUrl", "shortlink"],
    "generic":  ["shortenedUrl", "short_url", "shortlink", "url"],
}


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


def shorten_url(target_url: str) -> str:
    """
    Shorten a URL using the configured link shortener.

    Reads from environment:
      SHORTENER_API_URL  — provider API endpoint
      SHORTENER_API_KEY  — your API key
      SHORTENER_PROVIDER — provider name (gplinks | vplink | linkpays | generic)

    Returns shortened URL on success, original URL as fallback.
    """
    api_url  = os.environ.get("SHORTENER_API_URL", "").strip()
    api_key  = os.environ.get("SHORTENER_API_KEY", "").strip()
    provider = os.environ.get("SHORTENER_PROVIDER", "generic").strip().lower()

    if not api_url or not api_key:
        logger.warning("[SHORTENER] Not configured — SHORTENER_API_URL or SHORTENER_API_KEY missing")
        return target_url

    try:
        resp = http_requests.get(
            api_url,
            params={"api": api_key, "url": target_url},
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()

        keys_to_try = _PROVIDER_KEYS.get(provider, _PROVIDER_KEYS["generic"])
        for key in keys_to_try:
            short = data.get(key)
            if short:
                logger.info("[SHORTENER][%s] %s -> %s", provider, target_url, short)
                return short

        nested = data.get("result") or data.get("data") or {}
        if isinstance(nested, dict):
            for key in keys_to_try:
                short = nested.get(key)
                if short:
                    logger.info("[SHORTENER][%s] (nested) %s -> %s", provider, target_url, short)
                    return short

        logger.warning("[SHORTENER][%s] Unexpected API response: %s", provider, data)

    except Exception as exc:
        logger.error("[SHORTENER] API call failed: %s — using fallback URL", exc)

    logger.info("[SHORTENER] Falling back to direct URL: %s", target_url)
    return target_url


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _random_id() -> str:
    """Return a random lowercase alphanumeric string of length _ID_LENGTH."""
    return "".join(random.choices(_ID_CHARS, k=_ID_LENGTH))
