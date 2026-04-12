"""
main.py — Telegram File Sharing Bot
Final production build — Step 5.

Features:
  - File upload + permanent links   (Step 2)
  - Self-healing fallback delivery  (Step 4)
  - Views analytics                 (Step 5)
  - User tracking                   (Step 5)
  - Rate limiting / anti-spam       (Step 5)
"""

import asyncio
import logging
import os
import threading
import time
from collections import defaultdict

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

from db import (
    init_db,
    save_file,
    get_file,
    remove_file_id,
    increment_views,
    upsert_user,
)
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

# Strict int set — only verified numeric IDs allowed
ADMIN_IDS: set[int] = {
    int(uid.strip())
    for uid in os.environ.get("ADMIN_IDS", "").split(",")
    if uid.strip().isdigit()
}

WEBHOOK_PATH     = "/webhook"
WEBHOOK_FULL_URL = f"{WEBHOOK_URL}{WEBHOOK_PATH}"

# ─── Rate Limiter ─────────────────────────────────────────────────────────────

# In-memory store: { user_id: [timestamp, timestamp, ...] }
_user_requests: dict[int, list[float]] = defaultdict(list)

RATE_LIMIT_MAX    = 5    # max requests
RATE_LIMIT_WINDOW = 60   # per N seconds


def _is_rate_limited(user_id: int) -> bool:
    """
    Return True if user has exceeded RATE_LIMIT_MAX requests
    within the last RATE_LIMIT_WINDOW seconds.

    Automatically cleans up old timestamps on every call.
    """
    now    = time.time()
    window = now - RATE_LIMIT_WINDOW

    # Keep only timestamps within the current window
    _user_requests[user_id] = [
        ts for ts in _user_requests[user_id] if ts > window
    ]

    if len(_user_requests[user_id]) >= RATE_LIMIT_MAX:
        logger.warning(
            "[RATE-LIMIT] user_id=%s exceeded %d req/%ds",
            user_id, RATE_LIMIT_MAX, RATE_LIMIT_WINDOW,
        )
        return True

    # Record this request
    _user_requests[user_id].append(now)
    return False


# ─── Flask App ────────────────────────────────────────────────────────────────

flask_app = Flask(__name__)

# ─── Bot Application ──────────────────────────────────────────────────────────

bot_app: Application = (
    ApplicationBuilder()
    .token(BOT_TOKEN)
    .updater(None)
    .build()
)

# ─── Dedicated event loop ─────────────────────────────────────────────────────

_event_loop = asyncio.new_event_loop()


def _start_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


_loop_thread = threading.Thread(
    target=_start_event_loop,
    args=(_event_loop,),
    daemon=True,
)

# ─── Admin Check ──────────────────────────────────────────────────────────────

def _is_admin(user_id: int) -> bool:
    """Strict check — user_id must be in ADMIN_IDS set."""
    return user_id in ADMIN_IDS


# ─── Upload Handler ───────────────────────────────────────────────────────────

async def _handle_file_upload(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    file_id: str,
    caption: str | None,
    source: str = "admin",
) -> None:
    """
    Core upload flow (admin + channel):
      1. Extract/generate unique_id from caption
      2. Save file_id to MongoDB
      3. Generate permanent link
      4. Reply to admin / log for channel
    """
    unique_id  = extract_unique_id(caption)
    result     = save_file(unique_id, file_id)
    link       = generate_link(unique_id, BOT_USERNAME)

    status_map = {
        "inserted": "New file saved",
        "updated":  "New backup added to existing file",
        "exists":   "File already exists (no change)",
    }
    status_msg = status_map.get(result, result)

    logger.info("[UPLOAD][%s] unique_id=%s | %s | link=%s",
                source, unique_id, status_msg, link)

    reply_text = (
        f"File saved successfully!\n\n"
        f"Unique ID : {unique_id}\n"
        f"Status    : {status_msg}\n\n"
        f"Permanent Link:\n{link}"
    )

    if update.message:
        await update.message.reply_text(reply_text)
    elif update.channel_post:
        logger.info("[UPLOAD][channel] unique_id=%s | link=%s", unique_id, link)


# ─── Self-Healing File Sender ─────────────────────────────────────────────────

async def send_file_with_fallback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    unique_id: str,
) -> None:
    """
    Deliver a file to the user using the self-healing fallback system.

    Algorithm:
      For each file_id in DB (in order):
        - Try to send
        - Success → increment views, log, stop
        - Failure → log dead ID, remove from DB, try next
      All failed → notify user
    """
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    # 1. Fetch document from DB
    doc = get_file(unique_id)

    if not doc:
        logger.warning("[DELIVERY] unique_id=%s not found | user_id=%s", unique_id, user_id)
        await update.message.reply_text("File not found.")
        return

    file_ids: list[str] = list(doc.get("file_ids", []))

    if not file_ids:
        logger.warning("[DELIVERY] unique_id=%s has no file_ids | user_id=%s", unique_id, user_id)
        await update.message.reply_text("File unavailable.")
        return

    logger.info(
        "[DELIVERY] Starting delivery: unique_id=%s | %d file_id(s) | user_id=%s",
        unique_id, len(file_ids), user_id,
    )

    # 2. Try each file_id
    sent = False
    for index, file_id in enumerate(file_ids):
        try:
            await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
            )

            # Success
            label = "primary" if index == 0 else f"fallback (index={index})"
            logger.info(
                "[DELIVERY] Sent via %s | unique_id=%s | user_id=%s",
                label, unique_id, user_id,
            )

            # Increment analytics counter
            increment_views(unique_id)

            sent = True
            break

        except Exception as exc:
            logger.warning(
                "[SELF-HEAL] Dead file_id detected (index=%d) | unique_id=%s | error=%s",
                index, unique_id, exc,
            )
            remove_file_id(unique_id, file_id)
            logger.info(
                "[SELF-HEAL] Dead file_id removed from DB | unique_id=%s",
                unique_id,
            )

    # 3. All failed
    if not sent:
        logger.error(
            "[DELIVERY] All file_ids exhausted | unique_id=%s | user_id=%s",
            unique_id, user_id,
        )
        await update.message.reply_text(
            "File is currently unavailable. Please contact admin."
        )


# ─── Telegram Handlers ────────────────────────────────────────────────────────

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start           — greeting + user tracking
    /start file_<id> — rate-limit check → user tracking → file delivery
    """
    user    = update.effective_user
    user_id = user.id
    args    = context.args

    # ── File deep-link ────────────────────────────────────────────────────────
    if args and args[0].startswith("file_"):
        unique_id = args[0][len("file_"):]

        # 1. Rate limit check
        if _is_rate_limited(user_id):
            await update.message.reply_text(
                "Too many requests, slow down."
            )
            return

        # 2. Track user activity
        status = upsert_user(user_id, first_name=user.first_name or "")
        logger.info(
            "[USER] %s user | user_id=%s | unique_id=%s",
            status, user_id, unique_id,
        )

        # 3. Deliver file
        logger.info("[DELIVERY] Request: unique_id=%s | user_id=%s", unique_id, user_id)
        await send_file_with_fallback(update, context, unique_id)
        return

    # ── Plain /start ──────────────────────────────────────────────────────────
    upsert_user(user_id, first_name=user.first_name or "")
    await update.message.reply_text("Bot is running successfully!")
    logger.info("[USER] /start from user_id=%s", user_id)


async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle file uploads from admins only (video / document / audio)."""
    user = update.effective_user

    # Strict admin check
    if not _is_admin(user.id):
        logger.warning("[SECURITY] Unauthorized upload attempt from user_id=%s", user.id)
        await update.message.reply_text("You are not authorized to upload files.")
        return

    msg     = update.message
    caption = msg.caption

    if msg.document:
        file_id, file_type = msg.document.file_id, "document"
    elif msg.video:
        file_id, file_type = msg.video.file_id, "video"
    elif msg.audio:
        file_id, file_type = msg.audio.file_id, "audio"
    else:
        return  # unsupported type

    logger.info("[UPLOAD] type=%s | user_id=%s | caption=%r", file_type, user.id, caption)
    await _handle_file_upload(update, context, file_id, caption, source="admin")


async def channel_post_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle file uploads posted directly to a linked channel."""
    post    = update.channel_post
    caption = post.caption

    if post.document:
        file_id, file_type = post.document.file_id, "document"
    elif post.video:
        file_id, file_type = post.video.file_id, "video"
    elif post.audio:
        file_id, file_type = post.audio.file_id, "audio"
    else:
        return

    logger.info("[UPLOAD][channel] type=%s | chat=%s | caption=%r",
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


# ─── Startup ──────────────────────────────────────────────────────────────────

async def register_handlers() -> None:
    bot_app.add_handler(CommandHandler("start", start_handler))

    file_filter = filters.Document.ALL | filters.VIDEO | filters.AUDIO
    bot_app.add_handler(MessageHandler(file_filter, upload_handler))

    channel_file_filter = (
        filters.Document.ALL | filters.VIDEO | filters.AUDIO
    ) & filters.ChatType.CHANNEL
    bot_app.add_handler(MessageHandler(channel_file_filter, channel_post_handler))

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
    """Ping own health endpoint every 10 min to prevent Render sleep."""
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
