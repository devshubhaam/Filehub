"""
main.py — Flask webhook server for FilePe multi-bot system.
Each bot runs as a separate webhook path on the same Flask app.
"""

import asyncio
import logging
import sys
import threading

from flask import Flask, request, jsonify, Response
from telegram import Update
from telegram.ext import Application

import db
from bot import build_app
from config import (
    BOT_TOKENS, BOT_USERNAMES,
    FLASK_PORT, WEBHOOK_BASE_URL, FLASK_SECRET,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

flask_app = Flask(__name__)

# ─── Multi-bot registry ───────────────────────────────────────────────────────
# { "bot_token": Application }
applications: dict[str, Application] = {}
# { "bot_username": "bot_token" } for health endpoint
username_to_token: dict[str, str] = {}

# One asyncio event loop for all bots
loop = asyncio.new_event_loop()


def run_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


loop_thread = threading.Thread(target=run_loop, args=(loop,), daemon=True)
loop_thread.start()


def sync_run(coro):
    """Run a coroutine on our background event loop, block until done."""
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=30)


# ─── Webhook Routes ───────────────────────────────────────────────────────────
@flask_app.route(f"/webhook/<token>", methods=["POST"])
def webhook(token: str):
    app = applications.get(token)
    if not app:
        return jsonify({"error": "Unknown bot"}), 404

    secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if FLASK_SECRET and secret != FLASK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(force=True)
    if not data:
        return jsonify({"error": "No data"}), 400

    update = Update.de_json(data, app.bot)

    async def process():
        async with app:
            await app.process_update(update)

    # Fire and forget in background loop
    asyncio.run_coroutine_threadsafe(process(), loop)
    return jsonify({"ok": True})


# ─── Health Endpoint ──────────────────────────────────────────────────────────
@flask_app.route("/health", methods=["GET"])
def health():
    """Global health check for Vercel failover."""
    return jsonify({
        "status": "ok",
        "bots": list(username_to_token.keys()),
    })


@flask_app.route("/health/<username>", methods=["GET"])
def health_bot(username: str):
    """Per-bot health check."""
    if username in username_to_token:
        return jsonify({"status": "ok", "bot": username})
    return jsonify({"status": "not_found"}), 404


@flask_app.route("/", methods=["GET"])
def index():
    return jsonify({
        "service": "FilePe Bot",
        "bots": len(applications),
        "status": "running",
    })


# ─── Startup ──────────────────────────────────────────────────────────────────
async def setup_bot(token: str, username: str, index: int):
    """Initialize one bot application."""
    app = build_app(token)
    await app.initialize()

    webhook_url = f"{WEBHOOK_BASE_URL}/webhook/{token}"
    await app.bot.set_webhook(
        url=webhook_url,
        secret_token=FLASK_SECRET or None,
        allowed_updates=Update.ALL_TYPES,
    )
    logger.info(f"✅ Bot @{username} webhook set → {webhook_url}")
    return app


async def startup():
    # Create DB indexes
    await db.create_indexes()

    if not BOT_TOKENS:
        logger.error("❌ No BOT_TOKENS configured!")
        return

    for i, (token, username) in enumerate(zip(BOT_TOKENS, BOT_USERNAMES)):
        app = await setup_bot(token, username, i)
        applications[token] = app
        username_to_token[username] = token
        logger.info(f"Bot {i+1}: @{username} ready")


def main():
    # Run startup in background loop
    sync_run(startup())
    logger.info(f"🚀 FilePe starting on port {FLASK_PORT}")
    flask_app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, threaded=True)


if __name__ == "__main__":
    main()
