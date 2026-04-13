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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
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
    verify_user,
    can_access,
    submit_payment,
    approve_payment,
    reject_payment,
    reject_all_pending,
    is_premium,
    use_referral,
    reward_referrer,
    run_cleanup,
)
from helpers import extract_unique_id, generate_link, generate_shortlink
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

SHORTLINK_API    = os.environ.get("SHORTLINK_API", "")
UPI_ID           = os.environ.get("UPI_ID", "yourname@upi")
PREMIUM_AMOUNT   = os.environ.get("PREMIUM_AMOUNT", "49")
UPI_QR_FILE      = os.environ.get("UPI_QR_FILE", "")

# ── Separate image URL for each message type ──────────────────────────────────
IMG_WELCOME      = os.environ.get("IMG_WELCOME",      "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")
IMG_ACCESS       = os.environ.get("IMG_ACCESS",       "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")
IMG_VERIFY       = os.environ.get("IMG_VERIFY",       "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")
IMG_PREMIUM      = os.environ.get("IMG_PREMIUM",      "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")
IMG_REFERRAL     = os.environ.get("IMG_REFERRAL",     "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")
UPI_QR_URL       = os.environ.get("UPI_QR_URL",       "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")

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



# ─── Image Send Helper ────────────────────────────────────────────────────────

async def _send_image_msg(
    target,          # update.message or context.bot
    chat_id: int,
    img_url: str,
    text: str,
    parse_mode: str = "Markdown",
    reply_markup=None,
    is_bot: bool = False,
) -> None:
    """Send a photo+caption if img_url set, else send text. Fallback to text on error."""
    kwargs = dict(caption=text, parse_mode=parse_mode)
    if reply_markup:
        kwargs["reply_markup"] = reply_markup
    if img_url:
        try:
            if is_bot:
                await target.send_photo(chat_id=chat_id, photo=img_url, **kwargs)
            else:
                await target.reply_photo(photo=img_url, **kwargs)
            return
        except Exception as exc:
            logger.warning("[IMG] Photo send failed: %s — falling back to text", exc)
    # Fallback
    txt_kwargs = dict(text=text, parse_mode=parse_mode)
    if reply_markup:
        txt_kwargs["reply_markup"] = reply_markup
    if is_bot:
        await target.send_message(chat_id=chat_id, **txt_kwargs)
    else:
        await target.reply_text(**txt_kwargs)

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
        f"✅ *File Saved!*\n\n"
        f"🆔 *Unique ID*\n"
        f"`{unique_id}`\n\n"
        f"📊 *Status:* _{status_msg}_\n\n"
        f"🔗 *Permanent Link*\n"
        f"{link}\n\n"
        f"💡 _Share this link to give access to the file._"
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
            "*This file does not exist or has been removed.*\n\n"
            "_Double-check the link and try again._",
            parse_mode="Markdown",
        )
        return

    file_ids: list[str] = list(doc.get("file_ids", []))

    if not file_ids:
        logger.warning("[DELIVERY] unique_id=%s has no file_ids | user_id=%s", unique_id, user_id)
        await update.message.reply_text(
            "⚠️ *File Unavailable*\n\n"
            "*No sources are currently available for this file.*\n\n"
            "_Please contact the admin for assistance._",
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
            sent_msg = await context.bot.send_document(
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

            # Send auto-delete notice
            notice_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "⏳ *Auto-Delete Active*\n\n"
                    "*This file will be deleted in 10 minutes.*\n\n"
                    "_Save it now before the timer runs out!_"
                ),
                parse_mode="Markdown",
            )

            # Schedule deletion of both file + notice after 600 seconds
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

    # 3. All failed
    if not sent:
        logger.error(
            "[DELIVERY] All file_ids exhausted | unique_id=%s | user_id=%s",
            unique_id, user_id,
        )
        await update.message.reply_text(
            "❌ *Delivery Failed*\n\n"
            "*Unable to deliver this file right now.*\n\n"
            "_All backup sources have been exhausted._\n"
            "_Please contact the admin or try again later._",
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

    # ── Referral flow: /start ref_<referrer_id> ─────────────────────────────
    if args and args[0].startswith("ref_"):
        try:
            referrer_id = int(args[0][len("ref_"):])
        except ValueError:
            referrer_id = None

        if referrer_id:
            applied = use_referral(referrer_id, user_id)
            if applied:
                logger.info("[REFERRAL] New user via referral | referrer=%s | user=%s",
                            referrer_id, user_id)
                ref_text = (
                    "🎁 *Welcome!*\n\n"
                    "> You joined via a referral link\n\n"
                    "✅ *24-hour free access activated!*\n\n"
                    "_You can now access all files for the next 24 hours_"
                )
                await _send_image_msg(update.message, user_id, IMG_REFERRAL, ref_text, parse_mode="Markdown")
            else:
                await update.message.reply_text(
                    "👋 *Welcome back!*\n\n"
                    "_You have already used a referral before._",
                    parse_mode="Markdown",
                )

        upsert_user(user_id, first_name=user.first_name or "")
        return

    # ── Verification flow: /start verify_access_<unique_id> ─────────────────
    if args and args[0].startswith("verify_access_"):
        unique_id = args[0][len("verify_access_"):]

        # Give User 2 (this user) their 24h access
        verify_user(user_id)
        upsert_user(user_id, first_name=user.first_name or "")
        logger.info("[ACCESS] Verification complete | user_id=%s", user_id)

        verify_text = (
            "✅ *Verification Successful!*\n\n"
            "> You now have 24-hour access to ALL files\n\n"
            "_Sending your file now..._"
        )
        await _send_image_msg(update.message, user_id, IMG_VERIFY, verify_text, parse_mode="Markdown")

        # Check if User 2 was referred — if so reward User 1 (referrer)
        referrer_id = reward_referrer(user_id)
        if referrer_id:
            logger.info("[REFERRAL] Rewarding referrer=%s after user=%s verified",
                        referrer_id, user_id)
            try:
                await context.bot.send_message(
                    chat_id=referrer_id,
                    text=(
                        "🎉 *Referral Reward!*\n\n"
                        "Someone you referred just verified their account.\n\n"
                        "✅ *You have been granted 24-hour free access!*\n\n"
                        "_Enjoy unlimited file access for the next 24 hours._"
                    ),
                    parse_mode="Markdown",
                )
            except Exception as exc:
                logger.warning("[REFERRAL] Could not notify referrer=%s: %s", referrer_id, exc)

        # Deliver the file
        logger.info("[DELIVERY] Post-verify delivery | unique_id=%s | user_id=%s", unique_id, user_id)
        await send_file_with_fallback(update, context, unique_id)
        return

    # ── File deep-link: /start file_<unique_id> ───────────────────────────────
    if args and args[0].startswith("file_"):
        unique_id = args[0][len("file_"):]

        # 1. Rate limit check
        if _is_rate_limited(user_id):
            await update.message.reply_text(
                "⚠️ *Slow Down!*\n\n"
                "*You are sending requests too quickly.*\n\n"
                "_Please wait a moment and try again._",
                parse_mode="Markdown"
            )
            return

        # 2. Track user activity
        upsert_user(user_id, first_name=user.first_name or "")

        # 3. Priority 1 — Premium user → direct access
        if is_premium(user_id):
            logger.info("[ACCESS] Premium granted | user_id=%s | unique_id=%s", user_id, unique_id)
            await send_file_with_fallback(update, context, unique_id)
            return

        # 4. Priority 2 — Shortlink-verified (24h token) → direct access
        if can_access(user_id):
            logger.info("[ACCESS] Verified (24h) granted | user_id=%s | unique_id=%s", user_id, unique_id)
            await send_file_with_fallback(update, context, unique_id)
            return

        # 5. Priority 3 — Not verified → send shortlink button
        logger.info("[ACCESS] Blocked | user_id=%s | unique_id=%s", user_id, unique_id)

        verify_url = f"https://t.me/{BOT_USERNAME}?start=verify_access_{unique_id}"
        short_url  = generate_shortlink(verify_url)
        logger.info("[ACCESS] Shortlink generated | user_id=%s | short=%s", user_id, short_url)

        buy_url = f"https://t.me/{BOT_USERNAME}?start=buy"
        ref_url = f"https://t.me/{BOT_USERNAME}?start=referral"

        # Only show Buy button if user is NOT already premium
        buttons = [[InlineKeyboardButton("✅ Verify Now (Free)", url=short_url)]]
        if not is_premium(user_id):
            buttons.append([InlineKeyboardButton("💎 Get Premium Access", url=buy_url)])
        buttons.append([InlineKeyboardButton("🎁 Use Referral (Free 24h)", url=ref_url)])

        keyboard = InlineKeyboardMarkup(buttons)

        access_text = (
            "🔐 *Access Required*\n\n"
            "> Choose how you want to access this file\n\n"
            "✅ *Verify Now* \— Free, valid 24 hours\n"
            + ("" if is_premium(user_id) else "💎 *Premium* \— Pay once, 30 days access\n")
            + "🎁 *Referral* \— Friend\'s referral for free 24h"
        )

        await _send_image_msg(
            update.message, user_id, IMG_ACCESS,
            access_text, parse_mode="Markdown",
            reply_markup=keyboard,
        )
        return

    # ── Deep-link: /start buy → show /buy ───────────────────────────────────
    if args and args[0] == "buy":
        await buy_handler(update, context)
        return

    # ── Deep-link: /start referral → show /referral ───────────────────────────
    if args and args[0] == "referral":
        await referral_handler(update, context)
        return

    # ── Plain /start ──────────────────────────────────────────────────────────
    upsert_user(user_id, first_name=user.first_name or "")

    welcome_text = (
        f"👋 *Hey {user.first_name}!*\n\n"
        f"*File Hub Bot*\n"
        f"> 📂 Securely store and deliver files via permanent links\n"
        f"> 📎 Open any file link and I'll send it directly here\n"
        f"> 🔒 Files auto-delete after 10 minutes\n\n"
        f"*What you can do:*\n"
        f"💎 /buy — Get 30-day Premium access\n"
        f"🎁 /referral — Share your referral link\n"
        f"💰 /paid <UTR> — Submit payment after buying"
    )

    await _send_image_msg(update.message, user_id, IMG_WELCOME, welcome_text, parse_mode="Markdown")
    logger.info("[USER] /start from user_id=%s", user_id)


async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle file uploads from admins only (video / document / audio)."""
    user = update.effective_user

    # Strict admin check
    if not _is_admin(user.id):
        logger.warning("[SECURITY] Unauthorized upload attempt from user_id=%s", user.id)
        await update.message.reply_text(
            "🚫 *Access Denied*\n\n"
            "*You are not authorized to upload files.*\n\n"
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




# ─── Buy + Referral Handlers ──────────────────────────────────────────────────

async def buy_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /buy — Show UPI payment details with QR code.
    If already premium → show premium status instead.
    """
    user = update.effective_user

    # Already premium — no need to buy again
    if is_premium(user.id):
        already_text = (
            "💎 *You Are Already Premium!*\n\n"
            "> Your premium access is currently active\n\n"
            "_Enjoy unlimited access to all files for the remainder of your 30-day plan._\n\n"
            "You do not need to purchase again."
        )
        await _send_image_msg(update.message, user.id, IMG_PREMIUM, already_text, parse_mode="Markdown")
        return

    caption = (
        f"💎 *Get Premium Access*\n\n"
        f"> 👤 For: {user.first_name}\n\n"
        f"💰 *Amount:* ₹{PREMIUM_AMOUNT}\n"
        f"📲 *UPI ID:* `{UPI_ID}`\n\n"
        f"📋 *Steps:*\n"
        f"1️⃣ Pay ₹{PREMIUM_AMOUNT} to the UPI ID above\n"
        f"2️⃣ Note your UTR / Transaction ID\n"
        f"3️⃣ Send /paid <UTR> to confirm\n\n"
        f"⏳ *Access:* 30 days after approval\n\n"
        f"_Example: /paid 123456789012_"
    )

    await _send_image_msg(update.message, user.id, UPI_QR_URL or UPI_QR_FILE, caption)
    logger.info("[BUY] /buy sent to user_id=%s", user.id)


async def referral_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /referral — Generate and share a personal referral link.
    """
    user    = update.effective_user
    ref_url = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"

    await update.message.reply_text(
        f"🔗 *Your Referral Link*\n\n"
        f"`{ref_url}`\n\n"
        f"📢 *Share this link with friends!*\n\n"
        f"✅ Every person who joins via your link gets *24-hour free access*.\n\n"
        f"_Tap the link above to copy it._",
        parse_mode="Markdown",
    )
    logger.info("[REFERRAL] Link generated for user_id=%s", user.id)

# ─── Payment Handlers ─────────────────────────────────────────────────────────

async def paid_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /paid <UTR>
    User submits a payment UTR for admin review.
    """
    user    = update.effective_user
    user_id = user.id

    if not context.args:
        await update.message.reply_text(
            "⚠️ *Usage:* `/paid <UTR number>`\n\n"
            "_Example: /paid 123456789012_",
            parse_mode="Markdown",
        )
        return

    utr    = context.args[0].strip()
    result = submit_payment(user_id, utr)

    if result == "duplicate":
        await update.message.reply_text(
            "⚠️ *Duplicate UTR*\n\n"
            "*This UTR has already been submitted.*\n\n"
            "_Please check and try again._",
            parse_mode="Markdown",
        )
        return

    # Acknowledge user
    await update.message.reply_text(
        "✅ *Payment Submitted!*\n\n"
        f"*UTR:* `{utr}`\n\n"
        "_Your payment is under review. You will be notified once approved._",
        parse_mode="Markdown",
    )
    logger.info("[PAYMENT] Submitted | user_id=%s | utr=%s", user_id, utr)

    # Notify all admins with inline buttons
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"approve_{user_id}_{utr}"),
            InlineKeyboardButton("❌ Reject",  callback_data=f"reject_{user_id}_{utr}"),
        ],
        [
            InlineKeyboardButton("🚫 Reject All Pending", callback_data="reject_all"),
        ],
    ])

    admin_msg = (
        f"💰 *New Payment Request*\n\n"
        f"👤 *User ID:* `{user_id}`\n"
        f"🏷 *Name:* {user.first_name or 'Unknown'}\n"
        f"🔢 *UTR:* `{utr}`\n"
        f"📌 *Status:* PENDING"
    )

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=admin_msg,
                parse_mode="Markdown",
                reply_markup=keyboard,
            )
        except Exception as exc:
            logger.error("[PAYMENT] Failed to notify admin_id=%s: %s", admin_id, exc)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle inline button callbacks for payment approval system.
    Only ADMIN_IDS can trigger these actions.
    """
    query   = update.callback_query
    admin   = query.from_user

    # Security — only admins
    if admin.id not in ADMIN_IDS:
        await query.answer("🚫 You are not authorized.", show_alert=True)
        logger.warning("[SECURITY] Unauthorized callback from user_id=%s", admin.id)
        return

    await query.answer()  # dismiss loading spinner
    data = query.data

    # ── Approve ───────────────────────────────────────────────────────────────
    if data.startswith("approve_"):
        parts   = data.split("_", 2)      # ["approve", user_id, utr]
        user_id = int(parts[1])
        utr     = parts[2]

        approve_payment(user_id, utr)
        logger.info("[PAYMENT] Approved by admin=%s | user_id=%s | utr=%s",
                    admin.id, user_id, utr)

        # Notify user
        try:
            approved_text = (
                "🎉 *Payment Approved!*\n\n"
                "> Your Premium access has been activated\n\n"
                "⏳ *Valid for 30 days*\n\n"
                "_You now have unlimited access to all files_"
            )
            await _send_image_msg(
                context.bot, user_id, IMG_PREMIUM,
                approved_text, parse_mode="Markdown", is_bot=True,
            )
        except Exception as exc:
            logger.warning("[PAYMENT] Could not notify user_id=%s: %s", user_id, exc)

        # Update admin message
        await query.edit_message_text(
            f"✅ *Payment Approved*\n\n"
            f"👤 User ID: `{user_id}`\n"
            f"🔢 UTR: `{utr}`\n"
            f"👮 Approved by: {admin.first_name}",
            parse_mode="Markdown",
        )

    # ── Reject ────────────────────────────────────────────────────────────────
    elif data.startswith("reject_") and data != "reject_all":
        parts   = data.split("_", 2)      # ["reject", user_id, utr]
        user_id = int(parts[1])
        utr     = parts[2]

        reject_payment(user_id, utr)
        logger.info("[PAYMENT] Rejected by admin=%s | user_id=%s | utr=%s",
                    admin.id, user_id, utr)

        # Notify user
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    "❌ *Payment Rejected*\n\n"
                    f"*UTR:* `{utr}`\n\n"
                    "_Your payment could not be verified. "
                    "Please contact support if you believe this is an error._"
                ),
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("[PAYMENT] Could not notify user_id=%s: %s", user_id, exc)

        await query.edit_message_text(
            f"❌ *Payment Rejected*\n\n"
            f"👤 User ID: `{user_id}`\n"
            f"🔢 UTR: `{utr}`\n"
            f"👮 Rejected by: {admin.first_name}",
            parse_mode="Markdown",
        )

    # ── Reject All ────────────────────────────────────────────────────────────
    elif data == "reject_all":
        count = reject_all_pending()
        logger.info("[PAYMENT] Reject-all by admin=%s | %d records", admin.id, count)

        await query.edit_message_text(
            f"🚫 *All Pending Payments Rejected*\n\n"
            f"📊 *{count}* payment(s) have been rejected.\n\n"
            f"👮 Action by: {admin.first_name}",
            parse_mode="Markdown",
        )

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
    bot_app.add_handler(CommandHandler("buy", buy_handler))
    bot_app.add_handler(CommandHandler("referral", referral_handler))
    bot_app.add_handler(CommandHandler("paid", paid_handler))
    bot_app.add_handler(CallbackQueryHandler(callback_handler))

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

    # Run cleanup on startup
    try:
        results = run_cleanup()
        logger.info("[CLEANUP] Startup cleanup done: %s", results)
    except Exception as exc:
        logger.warning("[CLEANUP] Startup cleanup failed: %s", exc)

    await bot_app.initialize()
    await bot_app.start()
    await register_handlers()
    await set_webhook()
    logger.info("Bot is live and ready.")


# ─── Daily Cleanup Thread ────────────────────────────────────────────────────

def daily_cleanup() -> None:
    """Run cleanup once every 24 hours in background thread."""
    while True:
        time.sleep(86400)  # wait 24 hours
        try:
            results = run_cleanup()
            logger.info("[CLEANUP] Daily cleanup done: %s", results)
        except Exception as exc:
            logger.warning("[CLEANUP] Daily cleanup failed: %s", exc)


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

    threading.Thread(target=daily_cleanup, daemon=True).start()
    logger.info("Daily cleanup thread started OK")

    port = int(os.environ.get("PORT", 8080))
    logger.info("Starting Flask on port %d ...", port)
    flask_app.run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()
