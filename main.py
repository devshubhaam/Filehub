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

# ─── Bot Application ──────────────────────────────────────────────────────────

bot_app: Application = (
    ApplicationBuilder()
    .token(BOT_TOKEN)
    .updater(None)
    .build()
)

# ─── Shared event loop (runs in its own thread) ───────────────────────────────

_event_loop = asyncio.new_event_loop()


def _start_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Run event loop forever in a background thread."""
    asyncio.set_event_loop(loop)
    loop.run_forever()


_loop_thread = threading.Thread(
    target=_start_event_loop,
    args=(_event_loop,),
    daemon=True,
)

# ─── Telegram Command Handlers ────────────────────────────────────────────────

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command."""
    logger.info("Handling /start from user_id=%s", update.effective_user.id)
    await update.message.reply_text("Bot is running successfully 🚀")
    logger.info("/start reply sent ✅")

# ─── Flask Routes ─────────────────────────────────────────────────────────────

@flask_app.get("/")
def health_check() -> Response:
    return "Bot is live ✅", 200


@flask_app.post(WEBHOOK_PATH)
def webhook() -> Response:
    """Receive Telegram update and dispatch to the bot application."""
    if not request.is_json:
        logger.warning("Webhook received non-JSON payload")
        return jsonify({"error": "expected JSON"}), 415

    payload = request.get_json(force=True)
    update = Update.de_json(payload, bot_app.bot)

    # Submit to the dedicated event loop and wait for result
    future = asyncio.run_coroutine_threadsafe(
        bot_app.process_update(update),
        _event_loop,
    )
    try:
        future.result(timeout=30)
        logger.info("Update processed ✅")
    except Exception as exc:
        logger.error("Failed to process update: %s", exc)

    return jsonify({"ok": True}), 200

# ─── Startup Helpers ──────────────────────────────────────────────────────────

async def register_handlers() -> None:
    bot_app.add_handler(CommandHandler("start", start_handler))
    logger.info("Handlers registered.")


async def set_webhook() -> None:
    await bot_app.bot.set_webhook(
        url=WEBHOOK_FULL_URL,
        allowed_updates=Update.ALL_TYPES,
    )
    logger.info("Webhook set → %s", WEBHOOK_FULL_URL)


async def startup() -> None:
    init_db()
    await bot_app.initialize()
    await bot_app.start()
    await register_handlers()
    await set_webhook()
    logger.info("Bot is live and ready.")

# ─── Keep-Alive ───────────────────────────────────────────────────────────────

def keep_alive() -> None:
    """Ping own server every 10 min to prevent Render sleep."""
    while True:
        time.sleep(600)
        try:
            resp = http_requests.get(f"{WEBHOOK_URL}/", timeout=10)
            logger.info("Keep-alive ping → HTTP %s ✅", resp.status_code)
        except Exception as exc:
            logger.warning("Keep-alive ping failed: %s", exc)

# ─── Entry Point ──────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Start the dedicated event loop in background thread
    _loop_thread.start()
    logger.info("Event loop thread started ✅")

    # 2. Run startup coroutines on that loop
    future = asyncio.run_coroutine_threadsafe(startup(), _event_loop)
    future.result(timeout=60)  # Block until startup is complete

    # 3. Keep-alive thread
    threading.Thread(target=keep_alive, daemon=True).start()
    logger.info("Keep-alive thread started ✅")

    # 4. Start Flask
    port = int(os.environ.get("PORT", 8080))
    logger.info("Starting Flask on port %d …", port)
    flask_app.run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()
