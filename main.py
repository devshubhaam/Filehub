"""
main.py — Telegram File Sharing Bot
Entry point: initializes bot, Flask web server, and webhook.
"""

import asyncio
import logging
import os
import threading
import time

import requests as http_requests
from flask import Flask, Response, jsonify, request
from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

from db import init_db

from dotenv import load_dotenv

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Silence noisy third-party loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ─── Config ───────────────────────────────────────────────────────────────────

BOT_TOKEN    = os.environ["BOT_TOKEN"]
MONGO_URI    = os.environ["MONGO_URI"]
WEBHOOK_URL  = os.environ["WEBHOOK_URL"].rstrip("/")
BOT_USERNAME = os.environ["BOT_USERNAME"]

WEBHOOK_PATH     = "/webhook"
WEBHOOK_FULL_URL = f"{WEBHOOK_URL}{WEBHOOK_PATH}"

# ─── Flask App ────────────────────────────────────────────────────────────────

flask_app = Flask(__name__)

# ─── Bot Application (module-level so Flask routes can reach it) ──────────────

bot_app: Application = (
    ApplicationBuilder()
    .token(BOT_TOKEN)
    .updater(None)          # We handle updates manually via webhook
    .build()
)

# ─── Telegram Command Handlers ────────────────────────────────────────────────

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command."""
    await update.message.reply_text("Bot is running successfully 🚀")
    logger.info("Handled /start from user_id=%s", update.effective_user.id)

# ─── Flask Routes ─────────────────────────────────────────────────────────────

@flask_app.get("/")
def health_check() -> Response:
    """Health-check endpoint."""
    return "Bot is live ✅", 200


@flask_app.post(WEBHOOK_PATH)
def webhook() -> Response:
    """Receive Telegram update and dispatch to the bot application."""
    if not request.is_json:
        logger.warning("Webhook received non-JSON payload")
        return jsonify({"error": "expected JSON"}), 415

    payload = request.get_json(force=True)
    logger.debug("Incoming update: %s", payload)

    update = Update.de_json(payload, bot_app.bot)

    # Schedule async processing from within Flask's sync context
    asyncio.run_coroutine_threadsafe(
        bot_app.process_update(update),
        _event_loop,
    )

    return jsonify({"ok": True}), 200

# ─── Startup Helpers ──────────────────────────────────────────────────────────

async def register_handlers() -> None:
    """Register all command/message handlers with the bot application."""
    bot_app.add_handler(CommandHandler("start", start_handler))
    logger.info("Handlers registered.")


async def set_webhook() -> None:
    """Tell Telegram where to send updates."""
    await bot_app.bot.set_webhook(
        url=WEBHOOK_FULL_URL,
        allowed_updates=Update.ALL_TYPES,
    )
    logger.info("Webhook set → %s", WEBHOOK_FULL_URL)


async def startup() -> None:
    """Full async initialisation: DB → bot → webhook."""
    # 1. Database
    init_db()

    # 2. Bot internals (queue, job-queue, etc.)
    await bot_app.initialize()
    await bot_app.start()

    # 3. Handlers
    await register_handlers()

    # 4. Webhook
    await set_webhook()

    logger.info("Bot is live and ready.")

# ─── Keep-Alive (prevents Render free-tier sleep) ────────────────────────────

def keep_alive() -> None:
    """Ping own health-check endpoint every 10 min to prevent Render cold starts."""
    while True:
        time.sleep(600)  # 10 minutes
        try:
            resp = http_requests.get(f"{WEBHOOK_URL}/", timeout=10)
            logger.info("Keep-alive ping → HTTP %s ✅", resp.status_code)
        except Exception as exc:
            logger.warning("Keep-alive ping failed: %s", exc)

# ─── Entry Point ──────────────────────────────────────────────────────────────

# We need a persistent event loop that both the startup coroutine and the
# Flask route handler (asyncio.run_coroutine_threadsafe) can share.
_event_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_event_loop)


def main() -> None:
    # Run all async setup synchronously before Flask starts accepting requests.
    _event_loop.run_until_complete(startup())

    # Start keep-alive background thread
    threading.Thread(target=keep_alive, daemon=True).start()
    logger.info("Keep-alive thread started ✅")

    # Start Flask in the foreground.
    # On Render / production set host="0.0.0.0" and use the $PORT env var.
    port = int(os.environ.get("PORT", 8080))
    logger.info("Starting Flask on port %d …", port)
    flask_app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
