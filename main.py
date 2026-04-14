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
    get_bot_stats,
    get_file_stats,
    get_user_status,
    ban_user,
    unban_user,
    is_banned,
    get_force_join_channel,
    get_all_user_ids,
    PREMIUM_PLANS,
    submit_payment_plan,
    approve_payment_plan,
    use_free_trial,
    check_referral_milestone,
    use_viral_share,
    get_referral_stats,
    cancel_payment,
    get_top_files,
    get_daily_report,
    extend_premium,
    get_users_expiring_in,
    mark_reminder_sent,
    reminder_already_sent,
    set_file_price,
    get_file_price,
    has_paid_for_file,
    record_file_purchase,
    get_coins,
    add_coins,
    spend_coins,
    daily_checkin,
    COIN_REWARDS,
    COINS_FOR_ACCESS,
    save_bookmark,
    get_bookmarks,
    remove_bookmark,
    check_ip_rate,
    set_forward_protection,
    get_forward_protection,
    get_revenue_stats,
    log_event,
    get_user_journey,
    schedule_broadcast,
    get_pending_broadcasts,
    mark_broadcast_done,
    mark_broadcast_retry,
    track_shortlink_click,
    get_shortlink_stats,
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
SHORTLINK_CPM    = os.environ.get("SHORTLINK_CPM", "40")  # ₹ per 1000 clicks
UPI_ID           = os.environ.get("UPI_ID", "yourname@upi")
PREMIUM_AMOUNT   = os.environ.get("PREMIUM_AMOUNT", "49")  # legacy
PLAN_7_AMOUNT    = os.environ.get("PLAN_7_AMOUNT",  "19")
PLAN_30_AMOUNT   = os.environ.get("PLAN_30_AMOUNT", "49")
PLAN_90_AMOUNT   = os.environ.get("PLAN_90_AMOUNT", "99")
UPI_QR_FILE      = os.environ.get("UPI_QR_FILE", "")

# ── Separate image URL for each message type ──────────────────────────────────
IMG_WELCOME      = os.environ.get("IMG_WELCOME",      "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")
IMG_ACCESS       = os.environ.get("IMG_ACCESS",       "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")
IMG_VERIFY       = os.environ.get("IMG_VERIFY",       "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")
IMG_PREMIUM      = os.environ.get("IMG_PREMIUM",      "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")
IMG_REFERRAL     = os.environ.get("IMG_REFERRAL",     "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")
UPI_QR_URL       = os.environ.get("UPI_QR_URL",       "https://i.ibb.co/rRG680k1/Account-QRCode-AIRP-5423-DARK-THEME.png")

FORCE_JOIN_CHANNEL = os.environ.get("FORCE_JOIN_CHANNEL", "")

WEBHOOK_PATH     = "/webhook"
WEBHOOK_FULL_URL = f"{WEBHOOK_URL}{WEBHOOK_PATH}"

# ─── Rate Limiter ─────────────────────────────────────────────────────────────

# In-memory store: { user_id: [timestamp, timestamp, ...] }
_user_requests: dict[int, list[float]] = defaultdict(list)

RATE_LIMIT_MAX    = 5    # max requests before warning
RATE_LIMIT_WINDOW = 60   # per N seconds
RATE_LIMIT_BAN    = 20   # auto temp-ban threshold per minute


def _is_rate_limited(user_id: int) -> bool:
    """
    Rate limit + auto temp-ban on flood.
    - >5 req/min  → warning
    - >20 req/min → auto ban for 1 hour
    """
    now    = time.time()
    window = now - RATE_LIMIT_WINDOW

    _user_requests[user_id] = [
        ts for ts in _user_requests[user_id] if ts > window
    ]

    count = len(_user_requests[user_id])

    # Auto temp-ban on flood
    if count >= RATE_LIMIT_BAN:
        ban_user(user_id)
        logger.warning("[FLOOD] Auto-banned user_id=%s | %d req/min", user_id, count)
        return True

    if count >= RATE_LIMIT_MAX:
        logger.warning("[RATE-LIMIT] user_id=%s exceeded %d req/%ds", user_id, RATE_LIMIT_MAX, RATE_LIMIT_WINDOW)
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
    source_file_type: str = "document",
) -> None:
    """
    Core upload flow (admin + channel):
      1. Extract/generate unique_id from caption
      2. Save file_id + type to MongoDB
      3. Generate permanent link
      4. Reply to admin / log for channel
    """
    unique_id  = extract_unique_id(caption)
    result     = save_file(unique_id, file_id, file_type=source_file_type)
    # Use domain redirect URL — bot can be swapped by changing BOT_USERNAME in .env
    link       = f"{WEBHOOK_URL}/file/{unique_id}"

    status_map = {
        "inserted": "New file saved",
        "updated":  "Media added to album",
        "exists":   "File already exists (no change)",
    }
    status_msg = status_map.get(result, result)

    logger.info("[UPLOAD][%s] unique_id=%s | %s | link=%s",
                source, unique_id, status_msg, link)

    reply_text = (
        f"✅ *File Saved!*\n\n"
        f"🆔 *Unique ID*\n"
        f"`{unique_id}`\n\n"
        f"📊 *Status:* _{status_msg}_\n"
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
    Deliver ALL media files for a unique_id as an album (Option A).

    - Fetches all media entries from DB
    - Sends them together as a Telegram media group
    - Dead file_ids are removed on failure
    - Auto-deletes all sent messages after 10 minutes
    """
    from telegram import InputMediaVideo, InputMediaPhoto, InputMediaDocument, InputMediaAudio

    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    # 1. Fetch document
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

    # Support both new `media` list and legacy `file_ids`
    raw_media = doc.get("media", [])
    if not raw_media:
        # Backward compat: legacy file_ids treated as documents
        raw_media = [
            {"file_id": fid, "file_type": "document"}
            for fid in doc.get("file_ids", [])
        ]

    if not raw_media:
        logger.warning("[DELIVERY] unique_id=%s has no media | user_id=%s", unique_id, user_id)
        await update.message.reply_text(
            "⚠️ *File Unavailable*\n\n"
            "*No media available for this file.*\n\n"
            "_Please contact the admin for assistance._",
            parse_mode="Markdown",
        )
        return

    logger.info(
        "[DELIVERY] Starting album delivery: unique_id=%s | %d item(s) | user_id=%s",
        unique_id, len(raw_media), user_id,
    )

    # 2. Build InputMedia list — remove dead entries as we go
    def _make_input(entry: dict):
        fid   = entry["file_id"]
        ftype = entry.get("file_type", "document")
        if ftype == "video":
            return InputMediaVideo(media=fid)
        elif ftype == "photo":
            return InputMediaPhoto(media=fid)
        elif ftype == "audio":
            return InputMediaAudio(media=fid)
        else:
            return InputMediaDocument(media=fid)

    # Try to send in batches of 10 (Telegram album limit)
    DELETE_AFTER   = 600
    sent_message_ids = []
    total_sent     = 0
    dead_ids       = []

    # First pass — validate all file_ids
    valid_media = []
    for entry in raw_media:
        try:
            im = _make_input(entry)
            valid_media.append((entry, im))
        except Exception as exc:
            logger.warning("[SELF-HEAL] Bad media entry: %s | %s", entry, exc)
            dead_ids.append(entry["file_id"])

    if not valid_media:
        await update.message.reply_text(
            "❌ *Delivery Failed*\n\n"
            "_All media entries are invalid. Please contact admin._",
            parse_mode="Markdown",
        )
        return

    # Send in chunks of 10
    batch_size = 10
    for i in range(0, len(valid_media), batch_size):
        batch = valid_media[i:i + batch_size]
        input_media = [im for _, im in batch]

        try:
            if len(input_media) == 1:
                # Single file — use appropriate send method
                entry, _ = batch[0]
                fid   = entry["file_id"]
                ftype = entry.get("file_type", "document")
                protect = get_forward_protection()
                if ftype == "video":
                    msgs = [await context.bot.send_video(chat_id=chat_id, video=fid, protect_content=protect)]
                elif ftype == "photo":
                    msgs = [await context.bot.send_photo(chat_id=chat_id, photo=fid, protect_content=protect)]
                elif ftype == "audio":
                    msgs = [await context.bot.send_audio(chat_id=chat_id, audio=fid, protect_content=protect)]
                else:
                    msgs = [await context.bot.send_document(chat_id=chat_id, document=fid, protect_content=protect)]
            else:
                protect = get_forward_protection()
                msgs = await context.bot.send_media_group(
                    chat_id=chat_id,
                    media=input_media,
                    protect_content=protect,
                )

            sent_message_ids.extend([m.message_id for m in msgs])
            total_sent += len(msgs)
            logger.info(
                "[DELIVERY] Album batch %d sent | %d items | unique_id=%s | user_id=%s",
                i // batch_size + 1, len(msgs), unique_id, user_id,
            )

        except Exception as exc:
            logger.warning(
                "[SELF-HEAL] Album batch failed | unique_id=%s | error=%s",
                unique_id, exc,
            )
            # Mark all in this batch as dead
            for entry, _ in batch:
                dead_ids.append(entry["file_id"])

    # Remove dead file_ids
    for dead_id in dead_ids:
        remove_file_id(unique_id, dead_id)
        logger.info("[SELF-HEAL] Removed dead file_id=%s | unique_id=%s", dead_id, unique_id)

    if total_sent == 0:
        await update.message.reply_text(
            "❌ *Delivery Failed*\n\n"
            "*Unable to deliver the files right now.*\n\n"
            "_Please contact the admin or try again later._",
            parse_mode="Markdown",
        )
        return

    # 3. Increment views
    increment_views(unique_id)

    # 4. Send auto-delete notice with share button
    share_url  = f"{WEBHOOK_URL}/file/{unique_id}"
    share_btn  = f"https://t.me/{BOT_USERNAME}?start=file_{unique_id}"
    notice_msg = await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"⏳ *Auto-Delete Active*\n\n"
            f"*{total_sent} file(s) will be deleted in 10 minutes.*\n\n"
            "_Save them now before the timer runs out!_\n\n"
            "🔗 *Share & earn 24h free access:*"
        ),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔗 Share & Get 24h Free", url=f"https://t.me/share/url?url={share_btn}&text=Get+this+file+for+free!")
        ]])
    )
    sent_message_ids.append(notice_msg.message_id)

    # 5. Schedule deletion of all sent messages
    async def _delete_all(bot, cid: int, mids: list[int]) -> None:
        await asyncio.sleep(DELETE_AFTER)
        for mid in mids:
            try:
                await bot.delete_message(chat_id=cid, message_id=mid)
                logger.info("[AUTO-DELETE] Deleted message_id=%s | chat_id=%s", mid, cid)
            except Exception as del_exc:
                logger.warning("[AUTO-DELETE] Could not delete message_id=%s: %s", mid, del_exc)

    asyncio.ensure_future(
        _delete_all(context.bot, chat_id, sent_message_ids),
        loop=_event_loop,
    )
    logger.info(
        "[AUTO-DELETE] Scheduled deletion of %d messages in %ds | chat_id=%s",
        len(sent_message_ids), DELETE_AFTER, chat_id,
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

    # ── Ban check — silently ignore banned users ──────────────────────────────
    if is_banned(user_id):
        logger.info("[BAN] Blocked request from banned user_id=%s", user_id)
        return

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
        log_event(user_id, "verified")
        track_shortlink_click(user_id, unique_id)  # track for earnings report
        logger.info("[ACCESS] Verification complete | user_id=%s", user_id)

        verify_text = (
            "✅ *Verification Successful!*\n\n"
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

            # Check milestone reward
            milestone_days = check_referral_milestone(referrer_id)
            if milestone_days:
                try:
                    await context.bot.send_message(
                        chat_id=referrer_id,
                        text=(
                            f"🏆 *Referral Milestone Reached!*\n\n"
                            f"*You have earned {milestone_days} days of FREE Premium!*\n\n"
                            "_Keep referring to unlock the next milestone!_"
                        ),
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass

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

        # 3. Force join check
        force_ch = FORCE_JOIN_CHANNEL or get_force_join_channel()
        if force_ch:
            try:
                member = await context.bot.get_chat_member(
                    chat_id=f"@{force_ch.lstrip('@')}",
                    user_id=user_id,
                )
                if member.status in ("left", "kicked"):
                    ch_url = f"https://t.me/{force_ch.lstrip('@')}"
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("📢 Join Channel", url=ch_url),
                        InlineKeyboardButton("✅ I Joined", url=f"https://t.me/{BOT_USERNAME}?start=file_{unique_id}"),
                    ]])
                    await update.message.reply_text(
                        "📢 *Join Required*\n\n"
                        "*You must join our channel to access files.*\n\n"
                        "_Click Join Channel, then tap I Joined._",
                        parse_mode="Markdown",
                        reply_markup=keyboard,
                    )
                    return
            except Exception as exc:
                logger.warning("[FORCE_JOIN] Check failed: %s", exc)

        # Check if file has a price and user hasn't paid
        file_price = get_file_price(unique_id)
        if file_price > 0 and not is_premium(user_id) and not has_paid_for_file(user_id, unique_id):
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(f"💳 Unlock for ₹{file_price}", callback_data=f"buynow_{unique_id}_{file_price}")
            ]])
            await update.message.reply_text(
                f"🔒 *Paid Content*\n\n"
                f"*This file costs ₹{file_price} to access.*\n\n"
                f"📲 Pay to UPI: `{UPI_ID}`\n"
                f"Then send: `/paid <UTR> file_{unique_id}`\n\n"
                f"_Or get Premium for unlimited access: /buy_",
                parse_mode="Markdown",
                reply_markup=keyboard,
            )
            log_event(user_id, "paid_file_blocked", {"unique_id": unique_id, "price": file_price})
            return

        # Priority 1 — Premium user → direct access
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
            "✅ *Verify Now* — Free, valid 24 hours\n"
            + ("" if is_premium(user_id) else "💎 *Premium* — Pay once, 30 days access\n")
            + "🎁 *Referral* — Friend\'s referral for free 24h"
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
    elif msg.photo:
        file_id, file_type = msg.photo[-1].file_id, "photo"
    else:
        return  # unsupported type

    logger.info("[UPLOAD] type=%s | user_id=%s | caption=%r", file_type, user.id, caption)
    await _handle_file_upload(update, context, file_id, caption, source="admin", source_file_type=file_type)


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
    elif post.photo:
        file_id, file_type = post.photo[-1].file_id, "photo"
    else:
        return

    logger.info("[UPLOAD][channel] type=%s | chat=%s | caption=%r",
                file_type, post.chat.id, caption)
    await _handle_file_upload(update, context, file_id, caption, source="channel", source_file_type=file_type)




# ─── Buy + Referral Handlers ──────────────────────────────────────────────────

async def buy_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /buy — Show multi-plan premium options.
    Already premium → show status + extend option.
    """
    user    = update.effective_user
    user_id = user.id

    if is_premium(user_id):
        s = get_user_status(user_id)
        already_text = (
            "💎 *You Are Already Premium!*\n\n"
            f"⏳ *{s['premium_days_left']} days remaining*\n\n"
            "_Want to extend? Pay and submit UTR — your days will be added to current plan._\n\n"
            f"📲 *UPI ID:* `{UPI_ID}`"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"⚡ Extend 7 Days — ₹{PLAN_7_AMOUNT}",  callback_data="buy_plan_7")],
            [InlineKeyboardButton(f"💎 Extend 30 Days — ₹{PLAN_30_AMOUNT}", callback_data="buy_plan_30")],
            [InlineKeyboardButton(f"👑 Extend 90 Days — ₹{PLAN_90_AMOUNT}", callback_data="buy_plan_90")],
        ])
        await _send_image_msg(update.message, user_id, IMG_PREMIUM, already_text,
                              parse_mode="Markdown", reply_markup=keyboard)
        return

    caption = (
        f"💎 *Get Premium Access*\n\n"
        f"📲 *UPI ID:* `{UPI_ID}`\n\n"
        f"Choose your plan:\n"
        f"⚡ *7 Days* — ₹{PLAN_7_AMOUNT}\n"
        f"💎 *30 Days* — ₹{PLAN_30_AMOUNT}\n"
        f"👑 *90 Days* — ₹{PLAN_90_AMOUNT}\n\n"
        f"📋 *Steps:*\n"
        f"1️⃣ Choose plan, pay UPI\n"
        f"2️⃣ Note UTR / Transaction ID\n"
        f"3️⃣ Send: /paid <UTR> <plan>\n\n"
        f"_Example: /paid 123456789012 30_\n"
        f"_(Default plan: 30 days if not specified)_"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⚡ 7 Days — ₹{PLAN_7_AMOUNT}",  callback_data="buy_plan_7")],
        [InlineKeyboardButton(f"💎 30 Days — ₹{PLAN_30_AMOUNT}", callback_data="buy_plan_30")],
        [InlineKeyboardButton(f"👑 90 Days — ₹{PLAN_90_AMOUNT}", callback_data="buy_plan_90")],
    ])
    await _send_image_msg(update.message, user_id, IMG_PREMIUM, caption,
                          parse_mode="Markdown", reply_markup=keyboard)
    logger.info("[BUY] /buy sent to user_id=%s", user_id)


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
    plan   = context.args[1].strip() if len(context.args) > 1 else "30"
    if plan not in ("7", "30", "90"):
        plan = "30"
    result = submit_payment_plan(user_id, utr, plan)

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

        days = approve_payment_plan(user_id, utr)
        logger.info("[PAYMENT] Approved %d days by admin=%s | user_id=%s | utr=%s",
                    days, admin.id, user_id, utr)

        # Notify user
        try:
            approved_text = (
                "🎉 *Payment Approved!*\n\n"
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

    # ── Paid File Purchase ────────────────────────────────────────────────────
    elif data.startswith("buynow_"):
        parts    = data.split("_", 2)
        uid      = parts[1]
        price    = parts[2] if len(parts) > 2 else "?"
        await query.answer(
            f"Pay ₹{price} to {UPI_ID}\nThen send: /paid <UTR> file_{uid}",
            show_alert=True,
        )

    # ── Buy Plan Info (show UPI for chosen plan) ─────────────────────────────
    elif data.startswith("buy_plan_"):
        plan = data.replace("buy_plan_", "")
        plan_info = PREMIUM_PLANS.get(plan, PREMIUM_PLANS["30"])
        await query.answer(
            f"Pay ₹{plan_info['amount']} to {UPI_ID}\nThen send: /paid <UTR> {plan}",
            show_alert=True,
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

    # IP rate limiting
    if check_ip_rate(ip, max_per_min=30):
        return make_response("Too many requests", 429)

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




# ─── Status + Help + Admin Commands ──────────────────────────────────────────

async def status_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/status — Show user's current access status."""
    user    = update.effective_user
    user_id = user.id

    if is_banned(user_id):
        return

    s = get_user_status(user_id)

    premium_line = (
        f"💎 *Premium:* ✅ Active ({s['premium_days_left']} days left)"
        if s["premium_active"]
        else "💎 *Premium:* ❌ Inactive"
    )
    verified_line = (
        f"🔓 *24h Access:* ✅ Active ({s['verified_hours_left']}h left)"
        if s["verified_active"]
        else "🔓 *24h Access:* ❌ Expired"
    )

    await update.message.reply_text(
        f"📊 *Your Status*\n\n"
        f"👤 *Name:* {user.first_name}\n"
        f"🆔 *ID:* `{user_id}`\n\n"
        f"{premium_line}\n"
        f"{verified_line}\n"
        f"👥 *Referrals Made:* {s['referrals_made']}",
        parse_mode="Markdown",
    )


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/help — Show all available commands."""
    user_id = update.effective_user.id

    if is_banned(user_id):
        return

    admin_section = ""
    if _is_admin(user_id):
        admin_section = (
            "\n\n👮 *Admin Commands*\n"
            "/mystats — Bot statistics\n"
            "/broadcast <msg> — Message all users\n"
            "/ban <user_id> — Ban a user\n"
            "/unban <user_id> — Unban a user\n"
            "/filestats <id> — File statistics\n"
            "/approve <user_id> — Approve payment\n"
            "/reject <user_id> — Reject payment"
        )

    await update.message.reply_text(
        "📖 *Available Commands*\n\n"
        "🔹 /start — Welcome message\n"
        "🔹 /buy — Get Premium access\n"
        "🔹 /paid <UTR> <plan> — Submit payment\n"
        "🔹 /referral — Get your referral link\n"
        "🔹 /myreferrals — Your referral stats\n"
        "🔹 /trial — 2-hour free trial (once)\n"
        "🔹 /share <id> — Share file, get 24h free\n"
        "🔹 /cancel — Cancel pending payment\n"
        "🔹 /checkin — Daily coins reward\n"
        "🔹 /coins — Your coin balance\n"
        "🔹 /redeem — Spend coins for 24h access\n"
        "🔹 /save <id> — Bookmark a file\n"
        "🔹 /mysaved — View bookmarks\n"
        "🔹 /unsave <id> — Remove bookmark\n"
        "🔹 /status — Check your access status\n"
        "🔹 /help — Show this message"
        + admin_section,
        parse_mode="Markdown",
    )


async def mystats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/mystats — Admin only bot statistics."""
    if not _is_admin(update.effective_user.id):
        return

    s = get_bot_stats()
    await update.message.reply_text(
        "📊 *Bot Statistics*\n\n"
        f"👥 *Total Users:* {s['total_users']}\n"
        f"💎 *Active Premium:* {s['total_premium']}\n"
        f"⏳ *Pending Payments:* {s['pending_payments']}\n"
        f"🔗 *Total Referrals:* {s['total_referrals']}\n"
        f"📁 *Total Files:* {s['total_files']}\n"
        f"👁 *Total Views:* {s['total_views']}",
        parse_mode="Markdown",
    )


async def broadcast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/broadcast <message> — Send message to all users."""
    if not _is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text(
            "⚠️ *Usage:* `/broadcast Your message here`",
            parse_mode="Markdown",
        )
        return

    msg_text  = " ".join(context.args)
    user_ids  = get_all_user_ids()
    sent = failed = 0

    status_msg = await update.message.reply_text(
        f"📤 *Broadcasting to {len(user_ids)} users...*",
        parse_mode="Markdown",
    )

    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=msg_text)
            sent += 1
        except Exception:
            failed += 1

    await status_msg.edit_text(
        f"✅ *Broadcast Complete*\n\n"
        f"📤 *Sent:* {sent}\n"
        f"❌ *Failed:* {failed}",
        parse_mode="Markdown",
    )
    logger.info("[BROADCAST] Sent=%d Failed=%d by admin=%s", sent, failed, update.effective_user.id)


async def ban_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/ban <user_id> — Admin only."""
    if not _is_admin(update.effective_user.id):
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("⚠️ *Usage:* `/ban <user_id>`", parse_mode="Markdown")
        return

    target_id = int(context.args[0])
    ban_user(target_id)
    await update.message.reply_text(
        f"🚫 *User Banned*\n\n`{target_id}` has been banned.",
        parse_mode="Markdown",
    )


async def unban_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/unban <user_id> — Admin only."""
    if not _is_admin(update.effective_user.id):
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("⚠️ *Usage:* `/unban <user_id>`", parse_mode="Markdown")
        return

    target_id = int(context.args[0])
    unban_user(target_id)
    await update.message.reply_text(
        f"✅ *User Unbanned*\n\n`{target_id}` can now use the bot again.",
        parse_mode="Markdown",
    )


async def filestats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/filestats <unique_id> — Admin only file stats."""
    if not _is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text("⚠️ *Usage:* `/filestats <unique_id>`", parse_mode="Markdown")
        return

    stats = get_file_stats(context.args[0])
    if not stats:
        await update.message.reply_text("❌ File not found.", parse_mode="Markdown")
        return

    created = stats["created_at"].strftime("%d %b %Y") if stats["created_at"] else "Unknown"
    await update.message.reply_text(
        f"📁 *File Stats*\n\n"
        f"🆔 *ID:* `{stats['unique_id']}`\n"
        f"👁 *Views:* {stats['views']}\n"
        f"💾 *Backups:* {stats['file_ids']}\n"
        f"📅 *Created:* {created}",
        parse_mode="Markdown",
    )



# ─── New Feature Handlers ─────────────────────────────────────────────────────

async def trial_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/trial — 2-hour free access, one time only."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        return

    applied = use_free_trial(user_id)
    if applied:
        await update.message.reply_text(
            "🎁 *Free Trial Activated!*\n\n"
            "*You have 2 hours of free access.*\n\n"
            "_Open any file link to access it now._\n\n"
            "💎 _Enjoying it? Use /buy to get full premium access!_",
            parse_mode="Markdown",
        )
        logger.info("[TRIAL] Trial used by user_id=%s", user_id)
    else:
        await update.message.reply_text(
            "⚠️ *Trial Already Used*\n\n"
            "_You have already used your free trial._\n\n"
            "💎 Use /buy to get Premium access.",
            parse_mode="Markdown",
        )


async def myreferrals_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/myreferrals — Show referral stats."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        return

    s       = get_referral_stats(user_id)
    ref_url = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"

    next_info = ""
    if s.get("next_milestone"):
        m    = s["next_milestone"]
        days = s["milestones"][m]
        need = m - s["rewarded"]
        next_info = f"\n🎯 *Next Milestone:* {need} more referral(s) = *{days} days free premium!*"

    await update.message.reply_text(
        f"👥 *Your Referral Stats*\n\n"
        f"🔗 *Total Referred:* {s['total']}\n"
        f"✅ *Verified (Rewarded):* {s['rewarded']}\n"
        f"⏳ *Pending Verification:* {s['pending']}"
        + next_info +
        f"\n\n*Your Referral Link:*\n`{ref_url}`",
        parse_mode="Markdown",
    )


async def cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/cancel — Cancel pending payments."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        return

    count = cancel_payment(user_id)
    if count:
        await update.message.reply_text(
            f"✅ *{count} pending payment(s) cancelled.*\n\n"
            "_You can submit a new payment anytime with /paid._",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(
            "ℹ️ *No pending payments found.*",
            parse_mode="Markdown",
        )


async def topfiles_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/topfiles — Admin: top 10 most viewed files."""
    if not _is_admin(update.effective_user.id):
        return

    files = get_top_files(10)
    if not files:
        await update.message.reply_text("No files found.", parse_mode="Markdown")
        return

    lines = ["📊 *Top 10 Files*\n"]
    for i, f in enumerate(files, 1):
        uid    = f.get("unique_id", "?")
        views  = f.get("views", 0)
        media  = len(f.get("media", []))
        lines.append(f"{i}. `{uid}` — 👁 {views} views | 📎 {media} files")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def extend_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/extend — Admin: manually extend premium. Usage: /extend <user_id> <days>"""
    if not _is_admin(update.effective_user.id):
        return

    if len(context.args) < 2 or not context.args[0].isdigit() or not context.args[1].isdigit():
        await update.message.reply_text(
            "⚠️ *Usage:* `/extend <user_id> <days>`",
            parse_mode="Markdown",
        )
        return

    target_id = int(context.args[0])
    days      = int(context.args[1])
    until     = extend_premium(target_id, days)

    await update.message.reply_text(
        f"✅ *Premium Extended*\n\n"
        f"👤 User: `{target_id}`\n"
        f"➕ Added: {days} days\n"
        f"📅 Valid until: {until.strftime('%d %b %Y')}",
        parse_mode="Markdown",
    )
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"🎁 *Premium Extended!*\n\n"
                f"*+{days} days have been added to your Premium.*\n\n"
                f"📅 *Valid until:* {until.strftime('%d %b %Y')}"
            ),
            parse_mode="Markdown",
        )
    except Exception:
        pass


async def viral_share_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/share <unique_id> — Get 24h access by sharing a file."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        return

    if not context.args:
        await update.message.reply_text(
            "⚠️ *Usage:* `/share <file_id>`\n\n"
            "_Share a file and get 24h free access!_",
            parse_mode="Markdown",
        )
        return

    applied = use_viral_share(user_id)
    uid     = context.args[0]
    share_url = f"https://t.me/{BOT_USERNAME}?start=file_{uid}"

    if applied:
        await update.message.reply_text(
            "✅ *Share Reward Activated!*\n\n"
            "*You got 24-hour free access for sharing!*\n\n"
            f"🔗 Share this link:\n`{share_url}`\n\n"
            "_Tap to copy and share with friends._",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(
            "ℹ️ *Already shared today.*\n\n"
            f"🔗 Your link:\n`{share_url}`\n\n"
            "_You can share again tomorrow for another reward._",
            parse_mode="Markdown",
        )



# ─── Coins + Checkin + Bookmark + Admin Tools ─────────────────────────────────

async def checkin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/checkin — Daily coin reward."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        return

    applied, earned = daily_checkin(user_id)
    balance = get_coins(user_id)

    if applied:
        log_event(user_id, "daily_checkin", {"earned": earned})
        await update.message.reply_text(
            f"✅ *Daily Check-in!*\n\n"
            f"🪙 *+{earned} coins earned!*\n"
            f"💰 *Balance:* {balance} coins\n\n"
            f"_Come back tomorrow for more!_\n"
            f"_{COINS_FOR_ACCESS} coins = 24h free access (/redeem)_",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(
            f"⏰ *Already checked in today!*\n\n"
            f"💰 *Balance:* {balance} coins\n\n"
            f"_Come back tomorrow!_",
            parse_mode="Markdown",
        )


async def coins_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/coins — Show coin balance."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        return
    balance = get_coins(user_id)
    await update.message.reply_text(
        f"🪙 *Your Coins*\n\n"
        f"💰 *Balance:* {balance} coins\n\n"
        f"*Earn coins:*\n"
        f"📅 /checkin — +{COIN_REWARDS['daily_checkin']} daily\n"
        f"👥 Referral verified — +{COIN_REWARDS['referral']}\n"
        f"🔗 /share — +{COIN_REWARDS['share']}\n\n"
        f"*Spend:*\n"
        f"🔓 /redeem — {COINS_FOR_ACCESS} coins = 24h access",
        parse_mode="Markdown",
    )


async def redeem_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/redeem — Spend coins for 24h access."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        return

    balance = get_coins(user_id)
    if balance < COINS_FOR_ACCESS:
        await update.message.reply_text(
            f"❌ *Not Enough Coins*\n\n"
            f"💰 *Your balance:* {balance} coins\n"
            f"🎯 *Required:* {COINS_FOR_ACCESS} coins\n\n"
            f"_Earn more with /checkin and /share!_",
            parse_mode="Markdown",
        )
        return

    spent = spend_coins(user_id, COINS_FOR_ACCESS, "24h_access")
    if spent:
        verify_user(user_id)
        log_event(user_id, "redeem_coins", {"spent": COINS_FOR_ACCESS})
        await update.message.reply_text(
            f"🎉 *Access Unlocked!*\n\n"
            f"✅ *{COINS_FOR_ACCESS} coins spent*\n"
            f"🔓 *24-hour access activated!*\n\n"
            f"💰 *Remaining:* {balance - COINS_FOR_ACCESS} coins",
            parse_mode="Markdown",
        )


async def save_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/save <unique_id> — Bookmark a file."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        return

    if not context.args:
        await update.message.reply_text(
            "⚠️ *Usage:* `/save <unique_id>`", parse_mode="Markdown")
        return

    uid     = context.args[0]
    applied = save_bookmark(user_id, uid)
    if applied:
        await update.message.reply_text(
            f"🔖 *Saved!*\n\n`{uid}` added to your bookmarks.\n"
            f"_View with /mysaved_",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(
            f"ℹ️ `{uid}` is already in your bookmarks.",
            parse_mode="Markdown",
        )


async def mysaved_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/mysaved — Show bookmarked files."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        return

    bookmarks = get_bookmarks(user_id)
    if not bookmarks:
        await update.message.reply_text(
            "📂 *No bookmarks yet.*\n\n_Use /save <id> to bookmark files._",
            parse_mode="Markdown",
        )
        return

    lines = ["🔖 *Your Saved Files*\n"]
    for uid in bookmarks:
        link = f"https://t.me/{BOT_USERNAME}?start=file_{uid}"
        lines.append(f"• `{uid}` — [Open]({link})")

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )


async def unsave_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/unsave <unique_id> — Remove bookmark."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        return

    if not context.args:
        await update.message.reply_text(
            "⚠️ *Usage:* `/unsave <unique_id>`", parse_mode="Markdown")
        return

    uid     = context.args[0]
    removed = remove_bookmark(user_id, uid)
    if removed:
        await update.message.reply_text(
            f"✅ `{uid}` removed from bookmarks.", parse_mode="Markdown")
    else:
        await update.message.reply_text(
            f"❌ `{uid}` not found in your bookmarks.", parse_mode="Markdown")


async def revenue_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/revenue — Admin revenue dashboard."""
    if not _is_admin(update.effective_user.id):
        return

    r = get_revenue_stats()
    plan_lines = ""
    for plan, count in r.get("plan_counts", {}).items():
        prices = {"7": 19, "30": 49, "90": 99}
        plan_lines += f"  • {plan}-day plan: {count} sales\n"

    await update.message.reply_text(
        f"💰 *Revenue Dashboard*\n\n"
        f"📅 *Today:* ₹{r.get('today', 0)}\n"
        f"📆 *This Week:* ₹{r.get('week', 0)}\n"
        f"🗓 *This Month:* ₹{r.get('month', 0)}\n"
        f"💎 *All Time:* ₹{r.get('total', 0)}\n\n"
        f"📊 *Plan Breakdown:*\n{plan_lines}\n"
        f"⏳ *Pending Payments:* {r.get('pending_count', 0)}",
        parse_mode="Markdown",
    )


async def journey_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/journey <user_id> — Admin: view user journey."""
    if not _is_admin(update.effective_user.id):
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(
            "⚠️ *Usage:* `/journey <user_id>`", parse_mode="Markdown")
        return

    uid    = int(context.args[0])
    events = get_user_journey(uid)
    if not events:
        await update.message.reply_text("No events found.", parse_mode="Markdown")
        return

    lines = [f"🗺 *User Journey — {uid}*\n"]
    for e in events:
        ts     = e.get("timestamp")
        time_s = ts.strftime("%d/%m %H:%M") if ts else "?"
        action = e.get("action", "?")
        meta   = e.get("meta", {})
        detail = f" ({', '.join(f'{k}={v}' for k, v in meta.items())})" if meta else ""
        lines.append(f"`{time_s}` — {action}{detail}")

    await update.message.reply_text(
        "\n".join(lines[:25]), parse_mode="Markdown")


async def setprice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/setprice <unique_id> <price> — Admin: set file price."""
    if not _is_admin(update.effective_user.id):
        return

    if len(context.args) < 2 or not context.args[1].isdigit():
        await update.message.reply_text(
            "⚠️ *Usage:* `/setprice <unique_id> <price>`\n"
            "_Use price 0 to make it free again._",
            parse_mode="Markdown",
        )
        return

    uid   = context.args[0]
    price = int(context.args[1])
    set_file_price(uid, price)
    label = f"₹{price}" if price > 0 else "Free"
    await update.message.reply_text(
        f"✅ *Price Set*\n\n`{uid}` → *{label}*",
        parse_mode="Markdown",
    )


async def protect_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/protect on|off — Admin: toggle forward protection."""
    if not _is_admin(update.effective_user.id):
        return

    if not context.args or context.args[0].lower() not in ("on", "off"):
        await update.message.reply_text(
            "⚠️ *Usage:* `/protect on` or `/protect off`",
            parse_mode="Markdown",
        )
        return

    enabled = context.args[0].lower() == "on"
    set_forward_protection(enabled)
    status = "✅ Enabled" if enabled else "❌ Disabled"
    await update.message.reply_text(
        f"🛡 *Forward Protection {status}*\n\n"
        f"_All new file deliveries will {'prevent' if enabled else 'allow'} forwarding._",
        parse_mode="Markdown",
    )


async def schedule_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/schedule <DD/MM/YYYY HH:MM> <message> — Schedule a broadcast."""
    if not _is_admin(update.effective_user.id):
        return

    if len(context.args) < 3:
        await update.message.reply_text(
            "⚠️ *Usage:* `/schedule DD/MM/YYYY HH:MM Your message here`\n\n"
            "_Example: /schedule 25/12/2024 10:00 Merry Christmas!_",
            parse_mode="Markdown",
        )
        return

    date_str = context.args[0]
    time_str = context.args[1]
    message  = " ".join(context.args[2:])

    try:
        from datetime import timedelta
        dt_naive = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
        dt_utc   = dt_naive.replace(tzinfo=timezone.utc)
        if dt_utc <= datetime.now(tz=timezone.utc):
            await update.message.reply_text(
                "❌ Scheduled time is in the past!", parse_mode="Markdown")
            return
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid date format. Use DD/MM/YYYY HH:MM",
            parse_mode="Markdown",
        )
        return

    doc_id = schedule_broadcast(message, dt_utc, update.effective_user.id)
    await update.message.reply_text(
        f"✅ *Broadcast Scheduled*\n\n"
        f"📅 *Time:* {date_str} {time_str} UTC\n"
        f"💬 *Message:* {message[:50]}{'...' if len(message) > 50 else ''}\n\n"
        f"_ID: `{doc_id}`_",
        parse_mode="Markdown",
    )


async def shortlinkstats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/shortlinkstats [days] — Admin: shortlink click + estimated earnings report."""
    if not _is_admin(update.effective_user.id):
        return

    days = 7
    if context.args and context.args[0].isdigit():
        days = min(int(context.args[0]), 30)

    s = get_shortlink_stats(days=days)

    daily_lines = ""
    for d in s["daily"]:
        daily_lines += f"  `{d['date']}` — {d['clicks']} clicks — \u20b9{d['earnings']}\n"

    msg = (
        f"\U0001f517 *Shortlink Stats \u2014 Last {days} Day(s)*\n\n"
        f"\U0001f4c5 *Today:*\n"
        f"  \U0001f446 Clicks: *{s['today_clicks']}*\n"
        f"  \U0001f465 Unique Users: *{s['today_unique']}*\n"
        f"  \U0001f4b0 Est. Earning: *\u20b9{s['today_estimated']}*\n\n"
        f"\U0001f4ca *{days}\-Day Total:*\n"
        f"  \U0001f446 Total Clicks: *{s['total_clicks']}*\n"
        f"  \U0001f4b0 Est. Total: *\u20b9{s['total_estimated']}*\n\n"
        f"\U0001f4c8 *Daily Breakdown:*\n{daily_lines}\n"
        f"\u2139\ufe0f _CPM rate: \u20b9{s['cpm']} per 1000 clicks_\n"
        f"_Update SHORTLINK\_CPM in .env to match your actual rate_"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

# ─── Startup ──────────────────────────────────────────────────────────────────

async def register_handlers() -> None:
    bot_app.add_handler(CommandHandler("start", start_handler))
    bot_app.add_handler(CommandHandler("buy", buy_handler))
    bot_app.add_handler(CommandHandler("referral", referral_handler))
    bot_app.add_handler(CommandHandler("paid", paid_handler))
    bot_app.add_handler(CommandHandler("status", status_handler))
    bot_app.add_handler(CommandHandler("help", help_handler))
    bot_app.add_handler(CommandHandler("mystats", mystats_handler))
    bot_app.add_handler(CommandHandler("broadcast", broadcast_handler))
    bot_app.add_handler(CommandHandler("ban", ban_handler))
    bot_app.add_handler(CommandHandler("unban", unban_handler))
    bot_app.add_handler(CommandHandler("filestats", filestats_handler))
    bot_app.add_handler(CommandHandler("trial", trial_handler))
    bot_app.add_handler(CommandHandler("myreferrals", myreferrals_handler))
    bot_app.add_handler(CommandHandler("cancel", cancel_handler))
    bot_app.add_handler(CommandHandler("topfiles", topfiles_handler))
    bot_app.add_handler(CommandHandler("extend", extend_handler))
    bot_app.add_handler(CommandHandler("share", viral_share_handler))
    bot_app.add_handler(CommandHandler("checkin", checkin_handler))
    bot_app.add_handler(CommandHandler("coins", coins_handler))
    bot_app.add_handler(CommandHandler("redeem", redeem_handler))
    bot_app.add_handler(CommandHandler("save", save_handler))
    bot_app.add_handler(CommandHandler("mysaved", mysaved_handler))
    bot_app.add_handler(CommandHandler("unsave", unsave_handler))
    bot_app.add_handler(CommandHandler("revenue", revenue_handler))
    bot_app.add_handler(CommandHandler("journey", journey_handler))
    bot_app.add_handler(CommandHandler("setprice", setprice_handler))
    bot_app.add_handler(CommandHandler("protect", protect_handler))
    bot_app.add_handler(CommandHandler("schedule", schedule_handler))
    bot_app.add_handler(CommandHandler("shortlinkstats", shortlinkstats_handler))
    bot_app.add_handler(CallbackQueryHandler(callback_handler))

    file_filter = filters.Document.ALL | filters.VIDEO | filters.AUDIO | filters.PHOTO
    bot_app.add_handler(MessageHandler(file_filter, upload_handler))

    channel_file_filter = (
        filters.Document.ALL | filters.VIDEO | filters.AUDIO | filters.PHOTO
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


# ─── Daily Report Thread ─────────────────────────────────────────────────────

def daily_report_thread() -> None:
    """Send daily stats report to all admins at midnight."""
    while True:
        # Sleep until next midnight
        now      = datetime.now(timezone.utc)
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        from datetime import timedelta
        next_midnight = midnight + timedelta(days=1)
        sleep_secs = (next_midnight - now).total_seconds()
        time.sleep(sleep_secs)

        try:
            r = get_daily_report()
            report_text = (
                f"📊 *Daily Report — {r['date']}*\n\n"
                f"👥 *New Users:* {r['new_users']}\n"
                f"💎 *New Premium:* {r['new_premium']}\n"
                f"💰 *New Payments:* {r['new_payments']}\n"
                f"📁 *Files Accessed:* {r['files_accessed']}\n"
                f"🔐 *Active Premium:* {r['active_premium']}\n"
                f"👤 *Total Users:* {r['total_users']}"
            )
            for admin_id in ADMIN_IDS:
                fut = asyncio.run_coroutine_threadsafe(
                    bot_app.bot.send_message(
                        chat_id=admin_id,
                        text=report_text,
                        parse_mode="Markdown",
                    ),
                    _event_loop,
                )
                try:
                    fut.result(timeout=10)
                except Exception:
                    pass
            logger.info("[REPORT] Daily report sent to %d admins", len(ADMIN_IDS))
        except Exception as exc:
            logger.warning("[REPORT] Daily report failed: %s", exc)


# ─── Premium Expiry Warning Thread ──────────────────────────────────────────

def premium_expiry_warning_thread() -> None:
    """Check daily and warn users whose premium expires in 3 days."""
    import asyncio as _asyncio
    while True:
        time.sleep(86400)  # check every 24 hours
        try:
            from datetime import timedelta
            from db import _db
            if _db is None:
                continue
            now        = datetime.now(timezone.utc)
            warn_until = now + timedelta(days=3)
            docs = list(_db["premium_users"].find({
                "valid_until": {"$gt": now, "$lt": warn_until}
            }))
            for doc in docs:
                uid = doc.get("user_id")
                valid_until = doc.get("valid_until")
                if not uid or not valid_until:
                    continue
                if valid_until.tzinfo is None:
                    valid_until = valid_until.replace(tzinfo=timezone.utc)
                days_left = (valid_until - now).days
                fut = asyncio.run_coroutine_threadsafe(
                    bot_app.bot.send_message(
                        chat_id=uid,
                        text=(
                            f"⚠️ *Premium Expiring Soon!*\n\n"
                            f"*Your Premium access expires in {days_left} day(s).*\n\n"
                            f"_Use /buy to renew and keep uninterrupted access._"
                        ),
                        parse_mode="Markdown",
                    ),
                    _event_loop,
                )
                try:
                    fut.result(timeout=10)
                    logger.info("[PREMIUM] Expiry warning sent to user_id=%s", uid)
                except Exception as exc:
                    logger.warning("[PREMIUM] Warning failed for user_id=%s: %s", uid, exc)
        except Exception as exc:
            logger.warning("[PREMIUM] Expiry warning thread error: %s", exc)


# ─── Subscription Reminder Thread ───────────────────────────────────────────

def subscription_reminder_thread() -> None:
    """Check hourly and send premium expiry reminders."""
    while True:
        time.sleep(3600)  # every hour
        try:
            # 7-day warning
            for doc in get_users_expiring_in(hours_min=0, hours_max=168):  # 0-7 days
                uid  = doc.get("user_id")
                vunt = doc.get("valid_until")
                if not uid or not vunt:
                    continue
                if vunt.tzinfo is None:
                    vunt = vunt.replace(tzinfo=timezone.utc)
                from datetime import timedelta
                now       = datetime.now(timezone.utc)
                days_left = (vunt - now).days

                if days_left <= 1 and not reminder_already_sent(uid, "1d"):
                    msg = ("⚠️ *Premium Expiring Tomorrow!*\n\n"
                           "*Your premium expires in less than 24 hours.*\n\n"
                           "_Use /buy now to renew!_")
                    rtype = "1d"
                elif days_left <= 3 and not reminder_already_sent(uid, "3d"):
                    msg = (f"⏰ *Premium Expiring in {days_left} Days*\n\n"
                           "_Renew with /buy to avoid interruption!_")
                    rtype = "3d"
                elif days_left <= 7 and not reminder_already_sent(uid, "7d"):
                    msg = (f"💡 *Premium Expiring in {days_left} Days*\n\n"
                           "_Plan ahead and renew with /buy!_")
                    rtype = "7d"
                else:
                    continue

                fut = asyncio.run_coroutine_threadsafe(
                    bot_app.bot.send_message(chat_id=uid, text=msg, parse_mode="Markdown"),
                    _event_loop,
                )
                try:
                    fut.result(timeout=10)
                    mark_reminder_sent(uid, rtype)
                    logger.info("[REMINDER] Sent %s reminder to user_id=%s", rtype, uid)
                except Exception as exc:
                    logger.warning("[REMINDER] Failed for user_id=%s: %s", uid, exc)
        except Exception as exc:
            logger.warning("[REMINDER] Thread error: %s", exc)


# ─── Scheduled Broadcast Thread ───────────────────────────────────────────────

def scheduled_broadcast_thread() -> None:
    """Check every minute for pending broadcasts and send them with smart retry."""
    while True:
        time.sleep(60)
        try:
            pending = get_pending_broadcasts()
            for doc in pending:
                doc_id  = str(doc["_id"])
                message = doc["message"]
                user_ids = get_all_user_ids()
                sent = failed = 0

                for uid in user_ids:
                    fut = asyncio.run_coroutine_threadsafe(
                        bot_app.bot.send_message(chat_id=uid, text=message),
                        _event_loop,
                    )
                    try:
                        fut.result(timeout=8)
                        sent += 1
                    except Exception:
                        failed += 1

                if failed > sent * 0.5 and doc.get("retries", 0) < 2:
                    # More than 50% failed — retry later
                    mark_broadcast_retry(doc_id)
                    logger.warning("[SCHEDULE] Broadcast %s had high failures (%d/%d) — will retry",
                                   doc_id, failed, sent + failed)
                else:
                    mark_broadcast_done(doc_id, sent, failed)
                    logger.info("[SCHEDULE] Broadcast %s done | sent=%d failed=%d", doc_id, sent, failed)

                    # Notify admin
                    for admin_id in ADMIN_IDS:
                        try:
                            asyncio.run_coroutine_threadsafe(
                                bot_app.bot.send_message(
                                    chat_id=admin_id,
                                    text=(f"📤 *Scheduled Broadcast Sent*\n\n"
                                          f"✅ Sent: {sent}\n❌ Failed: {failed}"),
                                    parse_mode="Markdown",
                                ),
                                _event_loop,
                            ).result(timeout=10)
                        except Exception:
                            pass
        except Exception as exc:
            logger.warning("[SCHEDULE] Thread error: %s", exc)


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

    threading.Thread(target=premium_expiry_warning_thread, daemon=True).start()
    logger.info("Premium expiry warning thread started OK")

    threading.Thread(target=daily_report_thread, daemon=True).start()
    logger.info("Daily report thread started OK")

    threading.Thread(target=subscription_reminder_thread, daemon=True).start()
    logger.info("Subscription reminder thread started OK")

    threading.Thread(target=scheduled_broadcast_thread, daemon=True).start()
    logger.info("Scheduled broadcast thread started OK")

    port = int(os.environ.get("PORT", 8080))
    logger.info("Starting Flask on port %d ...", port)
    flask_app.run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()
