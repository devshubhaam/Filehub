"""
main.py — Telegram File Sharing Bot
Final production build — Step 6.

Features:
  - File upload + permanent links        (Step 2)
  - Self-healing fallback delivery       (Step 4)
  - Views analytics                      (Step 5)
  - User tracking                        (Step 5)
  - Rate limiting / anti-spam            (Step 5)
  - 24hr Access Token Verification       (Step 6)  ← NEW
    · User ek baar shortener complete kare
    · 24 ghante tak freely sab files access kare
    · Link format: t.me/Bot?start=verify_<token>
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
    create_pending_token,
    verify_pending_token,
    grant_user_access,
    has_valid_access,
    get_access_expiry,
)
from helpers import extract_unique_id, generate_link, shorten_url
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

# 24hr access verification (set TOKEN_VERIFY=true in .env to enable)
TOKEN_VERIFY_ENABLED = os.environ.get("TOKEN_VERIFY", "false").lower() == "true"
ACCESS_TTL_HOURS     = int(os.environ.get("ACCESS_TTL_HOURS", "24"))   # default 24hr
PENDING_TTL_SECONDS  = int(os.environ.get("PENDING_TTL", "900"))       # shortener link TTL: 15min

# Strict int set — only verified numeric IDs allowed
ADMIN_IDS: set[int] = {
    int(uid.strip())
    for uid in os.environ.get("ADMIN_IDS", "").split(",")
    if uid.strip().isdigit()
}

WEBHOOK_PATH     = "/webhook"
WEBHOOK_FULL_URL = f"{WEBHOOK_URL}{WEBHOOK_PATH}"

# ─── Rate Limiter ─────────────────────────────────────────────────────────────

_user_requests: dict[int, list[float]] = defaultdict(list)

RATE_LIMIT_MAX    = 5    # max requests
RATE_LIMIT_WINDOW = 60   # per N seconds


def _is_rate_limited(user_id: int) -> bool:
    """
    Return True if user has exceeded RATE_LIMIT_MAX requests
    within the last RATE_LIMIT_WINDOW seconds.
    """
    now    = time.time()
    window = now - RATE_LIMIT_WINDOW

    _user_requests[user_id] = [
        ts for ts in _user_requests[user_id] if ts > window
    ]

    if len(_user_requests[user_id]) >= RATE_LIMIT_MAX:
        logger.warning(
            "[RATE-LIMIT] user_id=%s exceeded %d req/%ds",
            user_id, RATE_LIMIT_MAX, RATE_LIMIT_WINDOW,
        )
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

    sent = False
    for index, file_id in enumerate(file_ids):
        try:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
            )

            label = "primary" if index == 0 else f"fallback (index={index})"
            logger.info(
                "[DELIVERY] Sent via %s | unique_id=%s | user_id=%s",
                label, unique_id, user_id,
            )

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
            async def _delete_messages(
                bot,
                cid: int,
                file_message_id: int,
                notice_message_id: int,
            ) -> None:
                await asyncio.sleep(DELETE_AFTER)
                for mid in (file_message_id, notice_message_id):
                    try:
                        await bot.delete_message(chat_id=cid, message_id=mid)
                        logger.info(
                            "[AUTO-DELETE] Deleted message_id=%s | chat_id=%s",
                            mid, cid,
                        )
                    except Exception as del_exc:
                        logger.warning(
                            "[AUTO-DELETE] Could not delete message_id=%s: %s",
                            mid, del_exc,
                        )

            asyncio.ensure_future(
                _delete_messages(
                    context.bot,
                    chat_id,
                    sent_msg.message_id,
                    notice_msg.message_id,
                ),
                loop=_event_loop,
            )
            logger.info(
                "[AUTO-DELETE] Scheduled deletion in %ds | chat_id=%s",
                DELETE_AFTER, chat_id,
            )

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
    /start                    — greeting + user tracking
    /start file_<id>          — access check → send file (or ask to verify)
    /start verify_<token>     — token check → grant 24hr access → pending file deliver
    """
    user    = update.effective_user
    user_id = user.id
    args    = context.args

    # ── verify_<token> — user shortener complete karke wapas aaya ─────────────
    if args and args[0].startswith("verify_"):
        token = args[0][len("verify_"):]

        logger.info("[ACCESS] verify_ deeplink | user_id=%s | token=%s...", user_id, token[:10])

        # Pending token validate karo
        is_valid = verify_pending_token(token)

        if not is_valid:
            logger.warning("[ACCESS] Invalid/expired token | user_id=%s", user_id)
            await update.message.reply_text(
                "❌ *Verification Failed*\n\n"
                "This verification link has *expired or already been used*.\n\n"
                "Please open the file link again to get a fresh verification link.\n\n"
                "_Links expire in 15 minutes._",
                parse_mode="Markdown",
            )
            return

        # ── Token valid: grant user 24hr access ──────────────────────────────
        upsert_user(user_id, first_name=user.first_name or "")
        grant_user_access(user_id, token, ttl_hours=ACCESS_TTL_HOURS)

        expiry     = get_access_expiry(user_id)
        expiry_str = expiry.strftime("%d %b %Y, %I:%M %p UTC") if expiry else "24 hours"

        logger.info("[ACCESS] Access granted | user_id=%s | expires=%s", user_id, expiry_str)

        await update.message.reply_text(
            f"✅ *Verification Successful!*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🎉 You now have *{ACCESS_TTL_HOURS}-hour access* to all files.\n\n"
            f"⏰ *Access expires:* `{expiry_str}`\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"_Now open any file link and it will be sent instantly!_",
            parse_mode="Markdown",
        )
        return

    # ── file_<id> — user file maang raha hai ──────────────────────────────────
    if args and args[0].startswith("file_"):
        unique_id = args[0][len("file_"):]

        # Rate limit check
        if _is_rate_limited(user_id):
            await update.message.reply_text(
                "⚠️ *Too Many Requests*\n\n"
                "You are sending requests too quickly.\n"
                "Please wait a moment and try again.",
                parse_mode="Markdown",
            )
            return

        upsert_user(user_id, first_name=user.first_name or "")

        # ── Token verification enabled: check 24hr access ────────────────────
        if TOKEN_VERIFY_ENABLED:

            if has_valid_access(user_id):
                # ✅ Access hai — seedha file bhejo
                expiry     = get_access_expiry(user_id)
                expiry_str = expiry.strftime("%d %b, %I:%M %p UTC") if expiry else ""
                logger.info(
                    "[ACCESS] User has valid access | user_id=%s | unique_id=%s",
                    user_id, unique_id,
                )
                await update.message.reply_text(
                    f"✅ *Access Verified* — sending your file...\n"
                    f"_Access valid until: {expiry_str}_",
                    parse_mode="Markdown",
                )
                await send_file_with_fallback(update, context, unique_id)
                return

            # ❌ Access nahi — pending token banao, shortener link bhejo
            pending_token = secrets.token_urlsafe(24)
            create_pending_token(pending_token, ttl_seconds=PENDING_TTL_SECONDS)

            # Verify URL → shortener se shorten karo
            verify_url = f"{WEBHOOK_URL}/verify/{pending_token}"
            short_url  = shorten_url(verify_url)
            minutes    = PENDING_TTL_SECONDS // 60

            logger.info(
                "[ACCESS] No access — sending verify link | user_id=%s | unique_id=%s",
                user_id, unique_id,
            )

            await update.message.reply_text(
                f"🔐 *Verification Required*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Complete a quick one-time verification to access all files "
                f"for *{ACCESS_TTL_HOURS} hours*.\n\n"
                f"👇 *Tap the link below and complete it:*\n"
                f"{short_url}\n\n"
                f"⏱ *Link expires in {minutes} minutes.*\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"_After verification, open this file link again — "
                f"it will work instantly for {ACCESS_TTL_HOURS} hours!_",
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            return

        # Token verify disabled — seedha file bhejo
        logger.info("[DELIVERY] Request: unique_id=%s | user_id=%s", unique_id, user_id)
        await send_file_with_fallback(update, context, unique_id)
        return

    # ── Plain /start ──────────────────────────────────────────────────────────
    upsert_user(user_id, first_name=user.first_name or "")

    # Access status check for welcome message
    access_line = ""
    if TOKEN_VERIFY_ENABLED:
        if has_valid_access(user_id):
            expiry     = get_access_expiry(user_id)
            expiry_str = expiry.strftime("%d %b, %I:%M %p UTC") if expiry else ""
            access_line = f"\n\n✅ *Access active* — valid until `{expiry_str}`"
        else:
            access_line = "\n\n🔐 _Open a file link to start verification._"

    await update.message.reply_text(
        f"👋 Welcome, {user.first_name}!\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 *File Hub Bot*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"I can securely deliver files to you via permanent links.\n\n"
        f"📎 Simply open a file link and I'll send it directly here."
        f"{access_line}\n\n"
        f"_Powered by File Hub_",
        parse_mode="Markdown",
    )
    logger.info("[USER] /start from user_id=%s", user_id)


async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle file uploads from admins only (video / document / audio)."""
    user = update.effective_user

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
        return

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


# ─── /file/<id> Redirect Route ────────────────────────────────────────────────

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

    if not user_agent:
        logger.warning("[REDIRECT] Blocked no-UA request | unique_id=%s | ip=%s", unique_id, ip)
        return make_response("Access denied", 403)

    tg_link = f"https://t.me/{BOT_USERNAME}?start=file_{unique_id}"

    logger.info(
        "[REDIRECT] Click received | unique_id=%s | ip=%s | bot=%s",
        unique_id, ip, BOT_USERNAME,
    )

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


# ─── /verify/<token> Route ────────────────────────────────────────────────────

@flask_app.get("/verify/<token>")
def verify_route(token: str) -> Response:
    """
    Token verification endpoint: /verify/<token>

    This is the URL that the link shortener redirects to after completion.

    Flow:
      1. Validate pending token (not used, not expired)
      2. If valid   → show success page + redirect to bot with verify_<token>
      3. If invalid → show error page
    """
    from flask import make_response

    ip = request.remote_addr
    logger.info("[VERIFY] Token check | token=%s... | ip=%s", token[:10], ip)

    # Validate: token genuine hai ya nahi check (exists + not expired)
    # Note: hum yahan consume nahi karte — bot pe jaake consume hoga
    # Isliye hum sirf check karte hain ki token DB mein hai ya nahi
    from db import _get_access_col
    from datetime import datetime, timezone as tz

    col = _get_access_col()
    now = datetime.now(tz=tz.utc)

    doc = col.find_one({
        "token":      token,
        "type":       "pending",
        "used":       False,
        "expires_at": {"$gt": now},
    })

    if not doc:
        logger.warning("[VERIFY] Invalid/expired token | token=%s... | ip=%s", token[:10], ip)
        html = _render_verify_page(success=False, token=token)
        return make_response(html, 403)

    # Valid — redirect to bot with verify_<token> deeplink
    tg_link = f"https://t.me/{BOT_USERNAME}?start=verify_{token}"
    logger.info("[VERIFY] Token valid — redirecting to bot | token=%s...", token[:10])

    html = _render_verify_page(success=True, token=token, tg_link=tg_link)
    return make_response(html, 200)


def _render_verify_page(
    success: bool,
    token: str,
    tg_link: str = "",
) -> str:
    """Render HTML page shown after shortener completion."""

    if success:
        icon     = "✅"
        heading  = "Verification Complete!"
        message  = f"You now have <strong>{ACCESS_TTL_HOURS}-hour access</strong> to all files. Tap below to open Telegram and receive your file."
        btn_cls  = "btn-success"
        btn_txt  = f"Get {ACCESS_TTL_HOURS}hr Access →"
        btn_href = tg_link
        auto_js  = f'setTimeout(function(){{ window.location.href="{tg_link}"; }}, 2000);'
    else:
        icon     = "❌"
        heading  = "Link Expired"
        message  = "This verification link has expired or has already been used. Please open the file link again to get a new one."
        btn_cls  = "btn-error"
        btn_txt  = "Go to Bot"
        btn_href = f"https://t.me/{BOT_USERNAME}"
        auto_js  = ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Verification — File Hub</title>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #0f0f1a; color: #e0e0e0;
      display: flex; align-items: center;
      justify-content: center; min-height: 100vh;
    }}
    .card {{
      text-align: center; padding: 44px 36px;
      background: #1a1a2e; border-radius: 18px;
      max-width: 400px; width: 90%;
      box-shadow: 0 8px 40px rgba(0,0,0,0.5);
    }}
    .icon    {{ font-size: 56px; margin-bottom: 18px; }}
    h2       {{ font-size: 22px; margin-bottom: 14px; color: #ffffff; }}
    p        {{ font-size: 14px; color: #999; margin-bottom: 30px; line-height: 1.7; }}
    p strong {{ color: #e0e0e0; }}
    a.btn    {{
      display: inline-block; padding: 14px 36px;
      border-radius: 10px; text-decoration: none;
      font-size: 15px; font-weight: 600; color: #fff;
    }}
    .btn-success {{ background: linear-gradient(135deg, #5b8dee, #a855f7); }}
    .btn-error   {{ background: #e05252; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{icon}</div>
    <h2>{heading}</h2>
    <p>{message}</p>
    <a class="btn {btn_cls}" href="{btn_href}">{btn_txt}</a>
  </div>
  <script>{auto_js}</script>
</body>
</html>"""


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
