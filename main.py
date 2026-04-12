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
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from db import init_db, save_file, get_file
from helpers import extract_unique_id, generate_link
from dotenv import load_dotenv

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s - %(message)s",
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

# Parse comma-separated admin IDs  e.g. "123456789,987654321"
ADMIN_IDS: set[int] = {
    int(uid.strip())
    for uid in os.environ.get("ADMIN_IDS", "").split(",")
    if uid.strip().isdigit()
}

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

# ─── Dedicated event loop in its own thread ───────────────────────────────────

_event_loop = asyncio.new_event_loop()


def _start_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


_loop_thread = threading.Thread(
    target=_start_event_loop,
    args=(_event_loop,),
    daemon=True,
)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _is_admin(user_id: int) -> bool:
    """Return True if user_id is in the ADMIN_IDS set."""
    return user_id in ADMIN_IDS


async def _handle_file_upload(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    file_id: str,
    caption: str | None,
    source: str = "user",
) -> None:
    """
    Core upload logic — shared by both user uploads and channel posts.

    1. Extract / generate unique_id from caption
    2. Save to MongoDB
    3. Generate permanent link
    4. Reply with link (user) or log (channel)
    """
    unique_id = extract_unique_id(caption)
    result    = save_file(unique_id, file_id)
    link      = generate_link(unique_id, BOT_USERNAME)

    status_map = {
        "inserted": "New file saved",
        "updated":  "New backup added to existing file",
        "exists":   "File already exists (no change)",
    }
    status_msg = status_map.get(result, result)

    logger.info("[%s] unique_id=%s | %s | link=%s", source, unique_id, status_msg, link)

    reply_text = (
        f"File saved successfully!\n\n"
        f"Unique ID : `{unique_id}`\n"
        f"Status    : {status_msg}\n\n"
        f"Permanent Link:\n{link}"
    )

    if update.message:
        await update.message.reply_text(reply_text, parse_mode="Markdown")
    elif update.channel_post:
        # For channel posts just log — replying to channels is not always possible
        logger.info("Channel post processed: unique_id=%s link=%s", unique_id, link)


# ─── Telegram Command Handlers ────────────────────────────────────────────────

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start  — plain greeting
    /start file_<unique_id>  — send the requested file
    """
    args = context.args  # list of words after /start

    # ── Deep-link: /start file_<unique_id> ────────────────────────────────────
    if args and args[0].startswith("file_"):
        unique_id = args[0][len("file_"):]   # strip "file_" prefix
        logger.info("File request: unique_id=%s from user_id=%s",
                    unique_id, update.effective_user.id)

        doc = get_file(unique_id)

        if not doc or not doc.get("file_ids"):
            await update.message.reply_text("File not found. It may have been removed.")
            return

        # Send the first (primary) file_id
        file_id = doc["file_ids"][0]
        try:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=file_id,
            )
            logger.info("Sent file unique_id=%s to user_id=%s",
                        unique_id, update.effective_user.id)
        except Exception as exc:
            logger.error("Failed to send file unique_id=%s: %s", unique_id, exc)
            await update.message.reply_text(
                "Could not send the file. Please try again later."
            )
        return

    # ── Plain /start ──────────────────────────────────────────────────────────
    await update.message.reply_text("Bot is running successfully!")
    logger.info("/start from user_id=%s", update.effective_user.id)


async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle file uploads from admins (video / document / audio).
    Non-admins are silently ignored.
    """
    user = update.effective_user
    if not _is_admin(user.id):
        logger.warning("Unauthorized upload attempt from user_id=%s", user.id)
        await update.message.reply_text("You are not authorized to upload files.")
        return

    msg     = update.message
    caption = msg.caption  # may be None

    # Determine file type and extract file_id
    if msg.document:
        file_id   = msg.document.file_id
        file_type = "document"
    elif msg.video:
        file_id   = msg.video.file_id
        file_type = "video"
    elif msg.audio:
        file_id   = msg.audio.file_id
        file_type = "audio"
    else:
        return  # unsupported type — ignore

    logger.info("Admin upload: type=%s user_id=%s caption=%r", file_type, user.id, caption)
    await _handle_file_upload(update, context, file_id, caption, source="admin")


async def channel_post_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle file uploads posted directly to a channel.
    The bot must be an admin of that channel.
    """
    post    = update.channel_post
    caption = post.caption

    if post.document:
        file_id   = post.document.file_id
        file_type = "document"
    elif post.video:
        file_id   = post.video.file_id
        file_type = "video"
    elif post.audio:
        file_id   = post.audio.file_id
        file_type = "audio"
    else:
        return  # not a file post

    logger.info("Channel post upload: type=%s chat=%s caption=%r",
                file_type, post.chat.id, caption)
    await _handle_file_upload(update, context, file_id, caption, source="channel")


# ─── Flask Routes ─────────────────────────────────────────────────────────────

@flask_app.get("/")
def health_check() -> Response:
    return "Bot is live OK", 200


@flask_app.post(WEBHOOK_PATH)
def webhook() -> Response:
    if not request.is_json:
        logger.warning("Webhook received non-JSON payload")
        return jsonify({"error": "expected JSON"}), 415

    payload = request.get_json(force=True)
    update  = Update.de_json(payload, bot_app.bot)

    future = asyncio.run_coroutine_threadsafe(
        bot_app.process_update(update),
        _event_loop,
    )
    try:
        future.result(timeout=30)
        logger.info("Update processed OK")
    except Exception as exc:
        logger.error("Failed to process update: %s", exc)

    return jsonify({"ok": True}), 200


# ─── Startup Helpers ──────────────────────────────────────────────────────────

async def register_handlers() -> None:
    # /start (with optional deep-link arg)
    bot_app.add_handler(CommandHandler("start", start_handler))

    # Admin uploads — private/group messages
    file_filter = filters.Document.ALL | filters.VIDEO | filters.AUDIO
    bot_app.add_handler(MessageHandler(file_filter, upload_handler))

    # Channel posts
    channel_file_filter = (
        filters.Document.ALL | filters.VIDEO | filters.AUDIO
    ) & filters.ChatType.CHANNEL
    bot_app.add_handler(
        MessageHandler(channel_file_filter, channel_post_handler)
    )

    logger.info("Handlers registered OK")


async def set_webhook() -> None:
    await bot_app.bot.set_webhook(
        url=WEBHOOK_FULL_URL,
        allowed_updates=Update.ALL_TYPES,
    )
    logger.info("Webhook set -> %s", WEBHOOK_FULL_URL)


async def startup() -> None:
    init_db()
    await bot_app.initialize()
    await bot_app.start()
    await register_handlers()
    await set_webhook()
    logger.info("Bot is live and ready.")


# ─── Keep-Alive ───────────────────────────────────────────────────────────────

def keep_alive() -> None:
    while True:
        time.sleep(600)
        try:
            resp = http_requests.get(f"{WEBHOOK_URL}/", timeout=10)
            logger.info("Keep-alive ping -> HTTP %s OK", resp.status_code)
        except Exception as exc:
            logger.warning("Keep-alive ping failed: %s", exc)


# ─── Entry Point ──────────────────────────────────────────────────────────────

def main() -> None:
    _loop_thread.start()
    logger.info("Event loop thread started OK")

    future = asyncio.run_coroutine_threadsafe(startup(), _event_loop)
    future.result(timeout=60)

    threading.Thread(target=keep_alive, daemon=True).start()
    logger.info("Keep-alive thread started OK")

    port = int(os.environ.get("PORT", 8080))
    logger.info("Starting Flask on port %d ...", port)
    flask_app.run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()
