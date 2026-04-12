"""
main.py — Telegram File Sharing Bot
Final production build — Step 6.

Features:
  - File upload + permanent links        (Step 2)
  - Self-healing fallback delivery       (Step 4)
  - Views analytics + click tracking     (Step 5)
  - User tracking + rate limiting        (Step 5)
  - 24hr Access Token Verification       (Step 6)
    · Shortener link seedha Telegram deeplink pe redirect karta hai
    · Format: t.me/Bot?start=verify_<token>
    · Ek baar verify → 24 ghante tak sab files free
"""

import asyncio
from datetime import datetime, timezone
import logging
import os
import secrets
import threading
import time
from collections import defaultdict

import requests as http_requests
from flask import Flask, Response, jsonify, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
    create_pending_token,
    verify_pending_token,
    grant_user_access,
    has_valid_access,
    get_access_expiry,
)
from helpers import extract_unique_id, generate_link, shorten_url
from dotenv import load_dotenv

load_dotenv()

# ─── IST Timezone ─────────────────────────────────────────────────────────────

from datetime import timedelta as _td
_IST = timezone(_td(hours=5, minutes=30))

def _fmt_ist(dt: datetime) -> str:
    """UTC datetime → Indian Standard Time string."""
    return dt.astimezone(_IST).strftime("%d %b %Y, %I:%M %p IST")

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

# Token verification toggle (TOKEN_VERIFY=true in .env to enable)
TOKEN_VERIFY_ENABLED = os.environ.get("TOKEN_VERIFY", "false").lower() == "true"
ACCESS_TTL_HOURS     = int(os.environ.get("ACCESS_TTL_HOURS", "24"))
PENDING_TTL_SECONDS  = int(os.environ.get("PENDING_TTL", "900"))   # 15 min default

ADMIN_IDS: set[int] = {
    int(uid.strip())
    for uid in os.environ.get("ADMIN_IDS", "").split(",")
    if uid.strip().isdigit()
}

WEBHOOK_PATH     = "/webhook"
WEBHOOK_FULL_URL = f"{WEBHOOK_URL}{WEBHOOK_PATH}"

# ─── Rate Limiter ─────────────────────────────────────────────────────────────

_user_requests: dict[int, list[float]] = defaultdict(list)
RATE_LIMIT_MAX    = 5
RATE_LIMIT_WINDOW = 60


def _is_rate_limited(user_id: int) -> bool:
    now    = time.time()
    window = now - RATE_LIMIT_WINDOW
    _user_requests[user_id] = [ts for ts in _user_requests[user_id] if ts > window]
    if len(_user_requests[user_id]) >= RATE_LIMIT_MAX:
        logger.warning("[RATE-LIMIT] user_id=%s exceeded limit", user_id)
        return True
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

_loop_thread = threading.Thread(target=_start_event_loop, args=(_event_loop,), daemon=True)

# ─── Admin Check ──────────────────────────────────────────────────────────────

def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# ─── Upload Handler ───────────────────────────────────────────────────────────

async def _handle_file_upload(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    file_id: str,
    caption: str | None,
    source: str = "admin",
) -> None:
    unique_id = extract_unique_id(caption)
    result    = save_file(unique_id, file_id)
    link      = f"{WEBHOOK_URL}/file/{unique_id}"

    status_map = {
        "inserted": "New file saved",
        "updated":  "New backup added to existing file",
        "exists":   "File already exists (no change)",
    }
    status_msg = status_map.get(result, result)
    logger.info("[UPLOAD][%s] unique_id=%s | %s", source, unique_id, status_msg)

    reply_text = (
        f"✅ File saved!\n\n"
        f"🆔 ID: `{unique_id}`\n"
        f"📊 {status_msg}\n\n"
        f"🔗 Link:\n{link}"
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
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    doc = get_file(unique_id)
    if not doc:
        await update.message.reply_text(
            "❌ *File Not Found*\n\nThe file you requested does not exist or may have been removed.\n\n_Please check the link and try again._",
            parse_mode="Markdown",
        )
        return

    file_ids: list[str] = list(doc.get("file_ids", []))
    if not file_ids:
        await update.message.reply_text(
            "⚠️ *File Unavailable*\n\nThis file currently has no available sources.\nPlease contact the admin for assistance.",
            parse_mode="Markdown",
        )
        return

    logger.info("[DELIVERY] unique_id=%s | %d file_id(s) | user_id=%s", unique_id, len(file_ids), user_id)

    sent = False
    for index, file_id in enumerate(file_ids):
        try:
            sent_msg = await context.bot.send_document(chat_id=chat_id, document=file_id)

            label = "primary" if index == 0 else f"fallback (index={index})"
            logger.info("[DELIVERY] Sent via %s | unique_id=%s | user_id=%s", label, unique_id, user_id)
            increment_views(unique_id)

            notice_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "⏳ *Auto-Delete Notice*\n\n"
                    "This file will be automatically deleted from this chat "
                    "in *10 minutes* for security purposes.\n\n"
                    "_Save it before the timer runs out!_"
                ),
                parse_mode="Markdown",
            )

            DELETE_AFTER = 600
            async def _delete_messages(bot, cid, fmid, nmid):
                await asyncio.sleep(DELETE_AFTER)
                for mid in (fmid, nmid):
                    try:
                        await bot.delete_message(chat_id=cid, message_id=mid)
                        logger.info("[AUTO-DELETE] Deleted msg_id=%s chat_id=%s", mid, cid)
                    except Exception as e:
                        logger.warning("[AUTO-DELETE] Could not delete msg_id=%s: %s", mid, e)

            asyncio.ensure_future(
                _delete_messages(context.bot, chat_id, sent_msg.message_id, notice_msg.message_id),
                loop=_event_loop,
            )
            logger.info("[AUTO-DELETE] Scheduled %ds | chat_id=%s", DELETE_AFTER, chat_id)
            sent = True
            break

        except Exception as exc:
            logger.warning("[SELF-HEAL] Dead file_id idx=%d | unique_id=%s | %s", index, unique_id, exc)
            remove_file_id(unique_id, file_id)

    if not sent:
        await update.message.reply_text(
            "❌ *File Unavailable*\n\nWe were unable to deliver this file.\nAll sources have been exhausted.\n\nPlease contact the admin or try again later.",
            parse_mode="Markdown",
        )


# ─── Telegram Handlers ────────────────────────────────────────────────────────

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start                  — welcome message
    /start file_<id>        — access check → file delivery ya verify link
    /start verify_<token>   — token consume → 24hr access grant → file delivery
    """
    user    = update.effective_user
    user_id = user.id
    args    = context.args

    # ──────────────────────────────────────────────────────────────────────────
    # CASE 1: verify_<token> — shortener complete karke user wapas aaya
    # Shortener ne directly t.me/Bot?start=verify_<token> pe bheja
    # ──────────────────────────────────────────────────────────────────────────
    if args and args[0].startswith("verify_"):
        token = args[0][len("verify_"):]
        logger.info("[ACCESS] verify_ deeplink | user_id=%s | token=%s...", user_id, token[:10])

        is_valid = verify_pending_token(token)

        if not is_valid:
            await update.message.reply_text(
                "❌ *Verification Failed*\n\n"
                "This link has *expired or already been used*.\n\n"
                "Please open the file link again to get a fresh one.\n"
                "_Links expire in 15 minutes._",
                parse_mode="Markdown",
            )
            return

        # ✅ Token valid — 24hr access grant karo
        upsert_user(user_id, first_name=user.first_name or "")
        grant_user_access(user_id, ttl_hours=ACCESS_TTL_HOURS)

        expiry     = get_access_expiry(user_id)
        expiry_str = _fmt_ist(expiry) if expiry else f"{ACCESS_TTL_HOURS} hours from now"

        logger.info("[ACCESS] Granted %dhr | user_id=%s | expires=%s", ACCESS_TTL_HOURS, user_id, expiry_str)

        await update.message.reply_text(
            f"✅ Verification successful!\n\n"
            f"You now have *{ACCESS_TTL_HOURS}-hour access* to all files.\n"
            f"⏰ Expires: `{expiry_str}`\n\n"
            f"Open any file link — it will be sent instantly!",
            parse_mode="Markdown",
        )
        return

    # ──────────────────────────────────────────────────────────────────────────
    # CASE 2: file_<id> — user file maang raha hai
    # ──────────────────────────────────────────────────────────────────────────
    if args and args[0].startswith("file_"):
        unique_id = args[0][len("file_"):]

        if _is_rate_limited(user_id):
            await update.message.reply_text(
                "⚠️ *Too Many Requests*\n\nPlease wait a moment and try again.",
                parse_mode="Markdown",
            )
            return

        upsert_user(user_id, first_name=user.first_name or "")

        if TOKEN_VERIFY_ENABLED:

            # ✅ Valid 24hr access hai — seedha file bhejo
            if has_valid_access(user_id):
                expiry     = get_access_expiry(user_id)
                expiry_str = _fmt_ist(expiry) if expiry else ""
                logger.info("[ACCESS] Valid access | user_id=%s | unique_id=%s", user_id, unique_id)
                await update.message.reply_text(
                    f"✅ Access verified — sending your file...\n"
                    f"_Expires: {expiry_str}_",
                    parse_mode="Markdown",
                )
                await send_file_with_fallback(update, context, unique_id)
                return

            # ❌ Access nahi — pending token banao
            # Shortener ka target URL seedha Telegram deeplink hai
            pending_token = secrets.token_urlsafe(24)
            create_pending_token(pending_token, ttl_seconds=PENDING_TTL_SECONDS)

            # Telegram deeplink jo shortener ke baad open hoga
            tg_deeplink = f"https://t.me/{BOT_USERNAME}?start=verify_{pending_token}"

            # Shortener se shorten karo (target = tg_deeplink)
            short_url = shorten_url(tg_deeplink)
            minutes   = PENDING_TTL_SECONDS // 60

            logger.info("[ACCESS] Sending verify link | user_id=%s | token=%s...", user_id, pending_token[:10])

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔓 Verify & Get Access", url=short_url)]
            ])

            await update.message.reply_text(
                f"🔐 *Verification Required*\n\n"
                f"Tap the button below to complete a quick one-time verification "
                f"and unlock *{ACCESS_TTL_HOURS} hours* of free access to all files.\n\n"
                f"⏱ Link expires in {minutes} minutes.",
                parse_mode="Markdown",
                reply_markup=keyboard,
            )
            return

        # Token verify disabled — seedha file bhejo
        await send_file_with_fallback(update, context, unique_id)
        return

    # ──────────────────────────────────────────────────────────────────────────
    # CASE 3: Plain /start — welcome message
    # ──────────────────────────────────────────────────────────────────────────
    upsert_user(user_id, first_name=user.first_name or "")

    access_line = ""
    if TOKEN_VERIFY_ENABLED:
        if has_valid_access(user_id):
            expiry     = get_access_expiry(user_id)
            expiry_str = _fmt_ist(expiry) if expiry else ""
            access_line = f"\n\n✅ Access active until `{expiry_str}`"
        else:
            access_line = f"\n\n🔐 Open a file link to verify and get {ACCESS_TTL_HOURS}hr access."

    await update.message.reply_text(
        f"👋 Welcome, {user.first_name}!\n\n"
        f"I can securely deliver files to you via permanent links.\n"
        f"Simply open a file link and I'll send it directly here."
        f"{access_line}",
        parse_mode="Markdown",
    )
    logger.info("[USER] /start | user_id=%s", user_id)


async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not _is_admin(user.id):
        await update.message.reply_text(
            "🚫 *Access Denied*\n\nYou are not authorized to upload files.",
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
        return

    logger.info("[UPLOAD] type=%s | user_id=%s", file_type, user.id)
    await _handle_file_upload(update, context, file_id, caption, source="admin")


async def channel_post_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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

    logger.info("[UPLOAD][channel] type=%s | chat=%s", file_type, post.chat.id)
    await _handle_file_upload(update, context, file_id, caption, source="channel")


# ─── Flask Routes ─────────────────────────────────────────────────────────────

@flask_app.get("/")
def health_check() -> Response:
    return "Bot is live OK", 200


@flask_app.post(WEBHOOK_PATH)
def webhook() -> Response:
    if not request.is_json:
        return jsonify({"error": "expected JSON"}), 415

    payload = request.get_json(force=True)
    update  = Update.de_json(payload, bot_app.bot)

    future = asyncio.run_coroutine_threadsafe(bot_app.process_update(update), _event_loop)
    try:
        future.result(timeout=30)
    except Exception as exc:
        logger.error("Failed to process update: %s", exc)

    return jsonify({"ok": True}), 200


@flask_app.get("/file/<unique_id>")
def file_redirect(unique_id: str) -> Response:
    """
    /file/<unique_id> — countdown page, phir Telegram deeplink pe redirect
    """
    from flask import make_response

    user_agent = request.headers.get("User-Agent")
    ip         = request.remote_addr

    if not user_agent:
        return make_response("Access denied", 403)

    tg_link = f"https://t.me/{BOT_USERNAME}?start=file_{unique_id}"

    try:
        track_click(unique_id, ip=ip, user_agent=user_agent)
    except Exception as exc:
        logger.error("[REDIRECT] click tracking failed: %s", exc)

    logger.info("[REDIRECT] unique_id=%s -> %s", unique_id, tg_link)

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
      display: flex; align-items: center; justify-content: center; min-height: 100vh;
    }}
    .card {{
      text-align: center; padding: 40px 32px; background: #1a1a2e;
      border-radius: 16px; max-width: 360px; width: 90%;
      box-shadow: 0 8px 32px rgba(0,0,0,0.4);
    }}
    .icon {{ font-size: 48px; margin-bottom: 16px; }}
    h2 {{ font-size: 20px; margin-bottom: 8px; color: #fff; }}
    p  {{ font-size: 14px; color: #888; margin-bottom: 24px; }}
    .counter {{ font-size: 40px; font-weight: 700; color: #5b8dee; margin-bottom: 20px; }}
    .bar-wrap {{ background: #2a2a3e; border-radius: 8px; overflow: hidden; height: 6px; margin-bottom: 24px; }}
    .bar {{ height: 100%; background: linear-gradient(90deg, #5b8dee, #a855f7); animation: shrink 5s linear forwards; }}
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
    var c=5, el=document.getElementById("count");
    var iv=setInterval(function(){{
      c--; el.textContent=c;
      if(c<=0){{ clearInterval(iv); window.location.href="{tg_link}"; }}
    }},1000);
  </script>
</body>
</html>"""
    return make_response(html, 200)


# ─── Startup ──────────────────────────────────────────────────────────────────

async def register_handlers() -> None:
    bot_app.add_handler(CommandHandler("start", start_handler))
    file_filter = filters.Document.ALL | filters.VIDEO | filters.AUDIO
    bot_app.add_handler(MessageHandler(file_filter, upload_handler))
    channel_file_filter = (filters.Document.ALL | filters.VIDEO | filters.AUDIO) & filters.ChatType.CHANNEL
    bot_app.add_handler(MessageHandler(channel_file_filter, channel_post_handler))
    logger.info("Handlers registered OK")

async def set_webhook() -> None:
    await bot_app.bot.set_webhook(url=WEBHOOK_FULL_URL, allowed_updates=Update.ALL_TYPES)
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
            logger.info("Keep-alive ping -> HTTP %s", resp.status_code)
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
