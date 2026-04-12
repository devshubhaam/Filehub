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
from datetime import datetime, timezone
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
    track_click,
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
    # Use domain redirect URL — bot can be swapped by changing BOT_USERNAME in .env
    link       = f"{WEBHOOK_URL}/file/{unique_id}"

    status_map = {
        "inserted": "New file saved",
        "updated":  "New backup added to existing file",
        "exists":   "File already exists (no change)",
    }
    status_msg = status_map.get(result, result)

    logger.info("[UPLOAD][%s] unique_id=%s | %s | link=%s",
                source, unique_id, status_msg, link)

    reply_text = (
        f"✅ *File Saved Successfully*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 *Unique ID:* `{unique_id}`\n"
        f"📊 *Status:* {status_msg}\n\n"
        f"🔗 *Permanent Link:*\n"
        f"{link}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_Share the link above to give access to this file._"
    )

    if update.message:
        await update.message.reply_text(reply_text, parse_mode="Markdown")
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
        await update.message.reply_text(
            "❌ *File Not Found*\n\n"
            "The file you requested does not exist or may have been removed.\n\n"
            "_Please check the link and try again._",
            parse_mode="Markdown",
        )
        return

    file_ids: list[str] = list(doc.get("file_ids", []))

    if not file_ids:
        logger.warning("[DELIVERY] unique_id=%s has no file_ids | user_id=%s", unique_id, user_id)
        await update.message.reply_text(
            "⚠️ *File Unavailable*\n\n"
            "This file currently has no available sources.\n"
            "Please contact the admin for assistance.",
            parse_mode="Markdown",
        )
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
            "❌ *File Unavailable*\n\n"
            "We were unable to deliver this file at the moment.\n"
            "All sources have been exhausted.\n\n"
            "Please contact the admin or try again later.",
            parse_mode="Markdown",
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
                "⚠️ *Too Many Requests*\n\n"
                "You are sending requests too quickly.\n"
                "Please wait a moment and try again.",
                parse_mode="Markdown"
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
    await update.message.reply_text(
        f"👋 Welcome, {user.first_name}!\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 *File Hub Bot*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"I can securely deliver files to you via permanent links.\n\n"
        f"📎 Simply open a file link and I'll send it directly here.\n\n"
        f"_Powered by File Hub_",
        parse_mode="Markdown",
    )
    logger.info("[USER] /start from user_id=%s", user_id)


async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle file uploads from admins only (video / document / audio)."""
    user = update.effective_user

    # Strict admin check
    if not _is_admin(user.id):
        logger.warning("[SECURITY] Unauthorized upload attempt from user_id=%s", user.id)
        await update.message.reply_text(
            "🚫 *Access Denied*\n\n"
            "You are not authorized to upload files.\n"
            "_Only admins can use this feature._",
            parse_mode="Markdown",
        )
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



# ─── Redirect Route ───────────────────────────────────────────────────────────


@flask_app.get("/file/<unique_id>")
def file_redirect(unique_id: str) -> Response:
    """
    Public redirect endpoint: /file/<unique_id>

    Flow:
      1. Anti-bot check (User-Agent required)
      2. Track click in MongoDB
      3. Return countdown HTML page that auto-redirects to Telegram deep-link
    """
    from flask import make_response

    user_agent = request.headers.get("User-Agent")
    ip         = request.remote_addr

    # Anti-bot: block requests with no User-Agent
    if not user_agent:
        logger.warning("[REDIRECT] Blocked no-UA request | unique_id=%s | ip=%s", unique_id, ip)
        return make_response("Access denied", 403)

    tg_link = f"https://t.me/{BOT_USERNAME}?start=file_{unique_id}"

    logger.info(
        "[REDIRECT] Click received | unique_id=%s | ip=%s | bot=%s",
        unique_id, ip, BOT_USERNAME,
    )

    # Track click (non-blocking)
    try:
        track_click(unique_id, ip=ip, user_agent=user_agent)
    except Exception as exc:
        logger.error("[REDIRECT] click tracking failed: %s", exc)

    logger.info("[REDIRECT] Redirecting -> %s", tg_link)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Opening File...</title>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #0f0f1a; color: #e0e0e0;
      display: flex; align-items: center;
      justify-content: center; min-height: 100vh;
    }}
    .card {{
      text-align: center; padding: 40px 32px;
      background: #1a1a2e; border-radius: 16px;
      max-width: 360px; width: 90%;
      box-shadow: 0 8px 32px rgba(0,0,0,0.4);
    }}
    .icon {{ font-size: 48px; margin-bottom: 16px; }}
    h2 {{ font-size: 20px; margin-bottom: 8px; color: #ffffff; }}
    p  {{ font-size: 14px; color: #888; margin-bottom: 24px; }}
    .counter {{ font-size: 40px; font-weight: 700; color: #5b8dee; margin-bottom: 20px; }}
    .bar-wrap {{ background: #2a2a3e; border-radius: 8px; overflow: hidden; height: 6px; margin-bottom: 24px; }}
    .bar {{ height: 100%; background: linear-gradient(90deg, #5b8dee, #a855f7); width: 100%; animation: shrink 5s linear forwards; }}
    @keyframes shrink {{ from {{ width: 100%; }} to {{ width: 0%; }} }}
    a.btn {{ display: inline-block; padding: 12px 28px; background: #5b8dee; color: #fff; border-radius: 8px; text-decoration: none; font-size: 15px; font-weight: 600; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">📂</div>
    <h2>Generating your link...</h2>
    <p>You will be redirected to Telegram automatically.</p>
    <div class="counter" id="count">5</div>
    <div class="bar-wrap"><div class="bar"></div></div>
    <a class="btn" href="{tg_link}">Open Now</a>
  </div>
  <script>
    var count = 5;
    var el = document.getElementById("count");
    var iv = setInterval(function() {{
      count--;
      el.textContent = count;
      if (count <= 0) {{ clearInterval(iv); window.location.href = "{tg_link}"; }}
    }}, 1000);
  </script>
</body>
</html>"""

    return make_response(html, 200)


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
