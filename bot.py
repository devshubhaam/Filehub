"""
bot.py — Main FilePe Telegram Bot
Handles all commands, file delivery, verification, premium, admin features.
Uses python-telegram-bot v20+ with full async support.
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument,
    InputMediaAudio, InputMediaAnimation,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from telegram.error import BadRequest, Forbidden, TelegramError

import db
import helpers
from config import (
    ADMIN_IDS, BOT_TOKENS, BOT_USERNAMES, AUTO_DELETE_MINUTES,
    PREMIUM_PLANS, UPI_ID, UPI_NAME, VERIFICATION_HOURS,
    FLASK_PORT, WEBHOOK_BASE_URL, FLASK_SECRET,
    START_MSG, FORCE_VERIFY_MSG, VERIFIED_MSG,
    FILE_AUTO_DELETE_MSG, PREMIUM_MSG,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ─── Decorators / Guards ──────────────────────────────────────────────────────
def admin_only(func):
    """Restrict handler to admins only."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("⛔ Admin only.")
            return
        return await func(update, context)
    wrapper.__name__ = func.__name__
    return wrapper


async def guard_user(update: Update) -> bool:
    """
    Returns True if user is allowed to proceed.
    Handles: banned check, rate limit, auto-ban.
    """
    user_id = update.effective_user.id

    if await db.is_banned(user_id):
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return False

    allowed, violations = helpers.rate_limiter.check(user_id)
    if not allowed:
        if helpers.rate_limiter.should_auto_ban(user_id):
            await db.ban_user(user_id, reason="Auto-banned for rate limit abuse")
            await update.message.reply_text("🚫 You've been banned for spamming.")
        else:
            await update.message.reply_text("⚠️ Too many requests. Please slow down.")
        return False

    return True


# ─── /start ───────────────────────────────────────────────────────────────────
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await db.upsert_user(user.id, user.username, user.full_name)

    if not await guard_user(update):
        return

    args = context.args
    if args and args[0].startswith("file_"):
        unique_id = args[0][5:]  # strip "file_"
        await deliver_file(update, context, unique_id)
        return

    # Plain /start — show welcome
    keyboard = [[
        InlineKeyboardButton("💎 Premium", callback_data="show_premium"),
        InlineKeyboardButton("ℹ️ Help", callback_data="show_help"),
    ]]
    await update.message.reply_text(
        START_MSG,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ─── File Delivery ────────────────────────────────────────────────────────────
async def deliver_file(update: Update, context: ContextTypes.DEFAULT_TYPE, unique_id: str):
    user = update.effective_user
    bot_username = context.bot.username

    # Log click
    asyncio.create_task(db.log_click(unique_id, user.id, bot_username))

    # Check premium (cache-first)
    _premium = helpers.premium_cache.get(f"premium_{user.id}")
    if _premium is None:
        _premium = await db.is_premium(user.id)
        helpers.premium_cache.set(f"premium_{user.id}", _premium)

    # Check verification (cache-first) — skip if premium
    if not _premium:
        _verified = helpers.verified_cache.get(f"verified_{user.id}")
        if _verified is None:
            _verified = await db.is_verified(user.id)
            helpers.verified_cache.set(f"verified_{user.id}", _verified)

        if not _verified:
            await send_verification_prompt(update, context, unique_id)
            return

    # Fetch file (cache-first)
    file_doc = helpers.file_cache.get(f"file_{unique_id}")
    if file_doc is None:
        file_doc = await db.get_file(unique_id)
        if file_doc:
            helpers.file_cache.set(f"file_{unique_id}", file_doc)

    if not file_doc:
        await update.message.reply_text("❌ File not found or has been deleted.")
        return

    await db.increment_views(unique_id)

    sent_ids = []

    try:
        media_list = file_doc.get("media", [])
        caption = file_doc.get("caption", "")

        if len(media_list) == 1:
            # Single file
            msg = await send_single_media(update, media_list[0], caption, unique_id)
            if msg:
                sent_ids.append(msg.message_id)

        elif len(media_list) > 1:
            # Album
            msgs = await send_album(update, media_list, caption, unique_id)
            sent_ids.extend([m.message_id for m in msgs])

        if sent_ids:
            # Auto-delete notice
            notice = await update.message.reply_text(
                FILE_AUTO_DELETE_MSG.format(minutes=AUTO_DELETE_MINUTES),
                parse_mode=ParseMode.HTML,
            )
            sent_ids.append(notice.message_id)

            # Schedule deletion in background
            asyncio.create_task(
                helpers.schedule_delete(context.bot, update.effective_chat.id, sent_ids)
            )

    except Exception as e:
        logger.error(f"File delivery error for {unique_id}: {e}")
        await update.message.reply_text("❌ An error occurred while sending the file.")


async def send_single_media(update: Update, media: dict, caption: str, unique_id: str):
    """Send a single media file, with self-healing on BadRequest."""
    file_id = media["file_id"]
    media_type = media["type"]

    send_map = {
        "video": update.message.reply_video,
        "photo": update.message.reply_photo,
        "document": update.message.reply_document,
        "audio": update.message.reply_audio,
        "animation": update.message.reply_animation,
        "voice": update.message.reply_voice,
        "video_note": update.message.reply_video_note,
    }

    sender = send_map.get(media_type)
    if not sender:
        return None

    try:
        kwargs = {"caption": caption, "parse_mode": ParseMode.HTML} if media_type != "video_note" else {}
        return await sender(file_id, **kwargs)
    except BadRequest as e:
        logger.warning(f"BadRequest for {file_id}: {e} — removing from DB")
        await db.remove_broken_file_id(unique_id, file_id)
        helpers.file_cache.delete(f"file_{unique_id}")
        await update.message.reply_text("⚠️ One file appears to be broken and has been removed.")
        return None


async def send_album(update: Update, media_list: list, caption: str, unique_id: str):
    """Send a media group (album), with self-healing."""
    input_media = []
    broken_ids = []

    for i, media in enumerate(media_list):
        file_id = media["file_id"]
        media_type = media["type"]
        cap = caption if i == 0 else None

        type_map = {
            "photo": InputMediaPhoto,
            "video": InputMediaVideo,
            "document": InputMediaDocument,
            "audio": InputMediaAudio,
            "animation": InputMediaAnimation,
        }

        cls = type_map.get(media_type)
        if not cls:
            continue

        try:
            input_media.append(cls(media=file_id, caption=cap, parse_mode=ParseMode.HTML))
        except Exception as e:
            logger.warning(f"Could not add {file_id} to album: {e}")
            broken_ids.append(file_id)

    if not input_media:
        return []

    try:
        msgs = await update.message.reply_media_group(input_media)
        # Remove any broken file_ids from DB
        for broken in broken_ids:
            await db.remove_broken_file_id(unique_id, broken)
            helpers.file_cache.delete(f"file_{unique_id}")
        return msgs
    except BadRequest as e:
        logger.error(f"Album send failed: {e}")
        await update.message.reply_text("❌ Failed to send album. Some files may be broken.")
        return []


# ─── Verification Flow ────────────────────────────────────────────────────────
async def send_verification_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, unique_id: str):
    """Send verification shortlink to user."""
    user = update.effective_user
    bot_username = context.bot.username

    # The URL user needs to verify through
    callback_url = f"https://t.me/{bot_username}?start=verify_{user.id}"
    short_url = await helpers.create_shortlink(callback_url)

    keyboard = [[
        InlineKeyboardButton("🔗 Verify Now", url=short_url),
        InlineKeyboardButton("✅ I've Verified", callback_data=f"check_verify_{unique_id}"),
    ]]

    await update.message.reply_text(
        FORCE_VERIFY_MSG,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def verify_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start verify_<user_id> — mark user as verified."""
    args = context.args
    if args and args[0].startswith("verify_"):
        target_user_id = int(args[0][7:])
        await db.set_verified(target_user_id)
        helpers.verified_cache.set(f"verified_{target_user_id}", True)
        await update.message.reply_text(
            "✅ <b>Verification successful!</b>\n\nYou can now access the file. Go back and click 'I've Verified'.",
            parse_mode=ParseMode.HTML,
        )


# ─── Callbacks ────────────────────────────────────────────────────────────────
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user

    if data.startswith("check_verify_"):
        unique_id = data[len("check_verify_"):]
        _verified = await db.is_verified(user.id)
        helpers.verified_cache.set(f"verified_{user.id}", _verified)

        if _verified:
            await query.message.reply_text(VERIFIED_MSG, parse_mode=ParseMode.HTML)
            # Deliver file inline
            class FakeUpdate:
                effective_user = user
                effective_chat = query.message.chat
                message = query.message

            await deliver_file(FakeUpdate(), context, unique_id)
        else:
            await query.answer("❌ Not verified yet. Please complete the shortlink first.", show_alert=True)

    elif data == "show_premium":
        text = helpers.get_premium_plans_text()
        keyboard = [[InlineKeyboardButton("💳 Buy Premium", callback_data="buy_premium")]]
        await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data == "buy_premium":
        await premium_handler(update, context, from_callback=True)

    elif data == "show_help":
        await query.message.reply_text(
            "📖 <b>How to use FilePe</b>\n\n"
            "1. Get a file link from our channel\n"
            "2. Click the link to open this bot\n"
            "3. Verify once (valid for 24h)\n"
            "4. Get your file instantly!\n\n"
            "💎 <b>Premium users</b> skip verification entirely.",
            parse_mode=ParseMode.HTML,
        )

    elif data.startswith("approve_payment_"):
        payment_id = data[len("approve_payment_"):]
        await admin_approve_payment(query, context, payment_id)

    elif data.startswith("reject_payment_"):
        payment_id = data[len("reject_payment_"):]
        await admin_reject_payment(query, context, payment_id)


# ─── /premium ─────────────────────────────────────────────────────────────────
async def premium_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, from_callback=False):
    user_id = (update.callback_query or update).from_user.id if from_callback else update.effective_user.id
    reply = update.callback_query.message.reply_text if from_callback else update.message.reply_text

    # Show current premium status
    prem_info = await db.get_premium_info(user_id)
    status_text = ""
    if prem_info:
        expires = prem_info.get("expires_at")
        if expires:
            if expires.replace(tzinfo=timezone.utc) > datetime.now(timezone.utc):
                status_text = f"\n\n✅ <b>Your Premium expires:</b> {helpers.time_until(expires)}"
            else:
                status_text = "\n\n❌ Your premium has expired."

    text = PREMIUM_MSG.format(upi_id=UPI_ID) + status_text

    keyboard = [[
        InlineKeyboardButton(f"7 Days — ₹49", callback_data="plan_7"),
        InlineKeyboardButton(f"30 Days — ₹149", callback_data="plan_30"),
    ], [
        InlineKeyboardButton(f"90 Days — ₹399", callback_data="plan_90"),
    ]]

    await reply(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))


async def premium_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await premium_handler(update, context)


# ─── /pay <utr> <plan> ────────────────────────────────────────────────────────
async def pay_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard_user(update):
        return

    user = update.effective_user
    args = context.args

    if len(args) < 2:
        await update.message.reply_text(
            "💳 <b>Submit Payment</b>\n\n"
            "Usage: <code>/pay &lt;UTR&gt; &lt;plan&gt;</code>\n"
            "Plans: 7, 30, 90\n\n"
            f"Example: <code>/pay 123456789012 30</code>\n\n"
            f"UPI ID: <code>{UPI_ID}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    utr = args[0].strip()
    plan_key = args[1].strip()

    if plan_key not in PREMIUM_PLANS:
        await update.message.reply_text("❌ Invalid plan. Choose: 7, 30, or 90")
        return

    if len(utr) < 10 or len(utr) > 20 or not utr.isdigit():
        await update.message.reply_text("❌ Invalid UTR number. Should be 10-20 digits.")
        return

    if await db.utr_exists(utr):
        await update.message.reply_text("❌ This UTR has already been submitted.")
        return

    plan = PREMIUM_PLANS[plan_key]
    success = await db.create_payment(user.id, utr, plan_key, plan["price"])

    if not success:
        await update.message.reply_text("❌ Failed to submit payment. Please try again.")
        return

    await update.message.reply_text(
        f"✅ <b>Payment submitted!</b>\n\n"
        f"Plan: {plan['label']}\n"
        f"Amount: ₹{plan['price']}\n"
        f"UTR: <code>{utr}</code>\n\n"
        "⏳ Admin will verify within 24 hours.",
        parse_mode=ParseMode.HTML,
    )

    # Notify admins
    payment_doc = await db.db().payments.find_one({"utr": utr})
    if payment_doc:
        payment_id = str(payment_doc["_id"])
        keyboard = [[
            InlineKeyboardButton("✅ Approve", callback_data=f"approve_payment_{payment_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"reject_payment_{payment_id}"),
        ]]
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"💳 <b>New Payment Request</b>\n\n"
                    f"User: {user.full_name} (<code>{user.id}</code>)\n"
                    f"Plan: {plan['label']}\n"
                    f"Amount: ₹{plan['price']}\n"
                    f"UTR: <code>{utr}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )
            except Exception as e:
                logger.warning(f"Could not notify admin {admin_id}: {e}")


# ─── Admin Payment Approval ───────────────────────────────────────────────────
async def admin_approve_payment(query, context, payment_id: str):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔ Admins only.", show_alert=True)
        return

    payment = await db.approve_payment(payment_id)
    if not payment:
        await query.answer("❌ Payment not found or already processed.", show_alert=True)
        return

    plan_key = payment["plan"]
    plan = PREMIUM_PLANS.get(plan_key, {})
    days = plan.get("days", 30)
    user_id = payment["user_id"]

    expiry = await db.grant_premium(user_id, days)
    helpers.premium_cache.set(f"premium_{user_id}", True)

    await query.edit_message_text(
        f"✅ <b>Payment Approved</b>\n\nUser: <code>{user_id}</code>\nPlan: {plan.get('label')}\nExpiry: {helpers.fmt_datetime(expiry)}",
        parse_mode=ParseMode.HTML,
    )

    # Notify user
    try:
        await context.bot.send_message(
            user_id,
            f"🎉 <b>Premium Activated!</b>\n\n"
            f"Plan: {plan.get('label', '')}\n"
            f"Expires: {helpers.fmt_datetime(expiry)}\n\n"
            "You can now access all files without verification!",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning(f"Could not notify user {user_id}: {e}")


async def admin_reject_payment(query, context, payment_id: str):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔ Admins only.", show_alert=True)
        return

    payment = await db.reject_payment(payment_id)
    if not payment:
        await query.answer("❌ Payment not found or already processed.", show_alert=True)
        return

    await query.edit_message_text(
        f"❌ <b>Payment Rejected</b>\n\nUTR: <code>{payment['utr']}</code>",
        parse_mode=ParseMode.HTML,
    )

    try:
        await context.bot.send_message(
            payment["user_id"],
            "❌ <b>Payment Rejected</b>\n\nYour payment could not be verified. "
            "Please check the UTR number or contact support.",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


# ─── /upload (Admin) ──────────────────────────────────────────────────────────
@admin_only
async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin uploads file(s). Reply to a media message with /upload [caption]"""
    message = update.message
    reply = message.reply_to_message

    if not reply:
        await message.reply_text(
            "📤 <b>How to upload:</b>\n\n"
            "1. Send your file(s) to the bot\n"
            "2. Reply to the file with <code>/upload [optional caption]</code>\n\n"
            "For albums: forward all files, then reply to first one with /upload",
            parse_mode=ParseMode.HTML,
        )
        return

    caption = " ".join(context.args) if context.args else reply.caption or ""
    media_info = helpers.extract_media_info(reply)

    if not media_info:
        await message.reply_text("❌ No supported media found in the replied message.")
        return

    unique_id = helpers.generate_unique_id()
    # Ensure unique
    while await db.get_file(unique_id):
        unique_id = helpers.generate_unique_id()

    success = await db.save_file(unique_id, [media_info], caption)
    if not success:
        await message.reply_text("❌ Failed to save file. Please try again.")
        return

    # Generate link using first bot username
    bot_username = context.bot.username
    link = helpers.make_file_link(bot_username, unique_id)

    await message.reply_text(
        f"✅ <b>File Uploaded!</b>\n\n"
        f"ID: <code>{unique_id}</code>\n"
        f"Type: {media_info['type']}\n"
        f"Caption: {caption or 'None'}\n\n"
        f"🔗 <b>Link:</b>\n<code>{link}</code>",
        parse_mode=ParseMode.HTML,
    )


# ─── /batch (Admin) — Upload album ───────────────────────────────────────────
# Store pending batch state per admin
_batch_state: dict = {}


@admin_only
async def batch_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_id = update.effective_user.id
    _batch_state[admin_id] = {"media": [], "caption": " ".join(context.args) if context.args else ""}
    await update.message.reply_text(
        "📦 <b>Batch mode started!</b>\n\nSend your files one by one. When done, send /batch_done",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def batch_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_id = update.effective_user.id
    state = _batch_state.get(admin_id)

    if not state or not state["media"]:
        await update.message.reply_text("❌ No files in batch. Use /batch to start.")
        return

    unique_id = helpers.generate_unique_id()
    while await db.get_file(unique_id):
        unique_id = helpers.generate_unique_id()

    success = await db.save_file(unique_id, state["media"], state["caption"])
    del _batch_state[admin_id]

    if not success:
        await update.message.reply_text("❌ Failed to save batch.")
        return

    link = helpers.make_file_link(context.bot.username, unique_id)
    await update.message.reply_text(
        f"✅ <b>Batch Uploaded!</b>\n\n"
        f"ID: <code>{unique_id}</code>\n"
        f"Files: {len(state['media'])}\n\n"
        f"🔗 <b>Link:</b>\n<code>{link}</code>",
        parse_mode=ParseMode.HTML,
    )


async def batch_collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Collect media files during batch mode."""
    admin_id = update.effective_user.id
    if admin_id not in ADMIN_IDS or admin_id not in _batch_state:
        return

    media_info = helpers.extract_media_info(update.message)
    if media_info:
        _batch_state[admin_id]["media"].append(media_info)
        count = len(_batch_state[admin_id]["media"])
        await update.message.reply_text(f"✅ File {count} added to batch.")


# ─── /stats (Admin) ───────────────────────────────────────────────────────────
@admin_only
async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = await db.get_stats()
    await update.message.reply_text(
        f"📊 <b>FilePe Stats</b>\n\n"
        f"👤 Users: {stats['users']}\n"
        f"📁 Files: {stats['files']}\n"
        f"💎 Premium: {stats['premium']}\n"
        f"💳 Pending Payments: {stats['pending_payments']}\n"
        f"🚫 Banned: {stats['banned']}",
        parse_mode=ParseMode.HTML,
    )


# ─── /ban & /unban (Admin) ────────────────────────────────────────────────────
@admin_only
async def ban_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /ban <user_id> [reason]")
        return
    user_id = int(context.args[0])
    reason = " ".join(context.args[1:]) if len(context.args) > 1 else "Admin ban"
    await db.ban_user(user_id, reason)
    await update.message.reply_text(f"🚫 User <code>{user_id}</code> banned.", parse_mode=ParseMode.HTML)


@admin_only
async def unban_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return
    user_id = int(context.args[0])
    await db.unban_user(user_id)
    await update.message.reply_text(f"✅ User <code>{user_id}</code> unbanned.", parse_mode=ParseMode.HTML)


# ─── /broadcast (Admin) ───────────────────────────────────────────────────────
@admin_only
async def broadcast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply = update.message.reply_to_message
    if not reply:
        await update.message.reply_text("Reply to a message with /broadcast to send it to all users.")
        return

    await update.message.reply_text("📢 Starting broadcast...")
    cursor = db.db().users.find({}, {"user_id": 1})
    success = failed = 0

    async for user in cursor:
        try:
            await reply.forward(chat_id=user["user_id"])
            success += 1
            await asyncio.sleep(0.05)  # Avoid flood limits
        except (Forbidden, BadRequest):
            failed += 1
        except TelegramError as e:
            logger.warning(f"Broadcast error for {user['user_id']}: {e}")
            failed += 1

    await update.message.reply_text(
        f"📢 <b>Broadcast Complete</b>\n\n✅ Sent: {success}\n❌ Failed: {failed}",
        parse_mode=ParseMode.HTML,
    )


# ─── /grant (Admin) — Manually grant premium ─────────────────────────────────
@admin_only
async def grant_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /grant <user_id> <days>")
        return
    user_id = int(context.args[0])
    days = int(context.args[1])
    expiry = await db.grant_premium(user_id, days)
    helpers.premium_cache.set(f"premium_{user_id}", True)
    await update.message.reply_text(
        f"✅ Granted {days} days premium to <code>{user_id}</code>\nExpires: {helpers.fmt_datetime(expiry)}",
        parse_mode=ParseMode.HTML,
    )


# ─── /delete (Admin) — Delete file ───────────────────────────────────────────
@admin_only
async def delete_file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /delete <unique_id>")
        return
    unique_id = context.args[0]
    success = await db.delete_file(unique_id)
    helpers.file_cache.delete(f"file_{unique_id}")
    if success:
        await update.message.reply_text(f"✅ File <code>{unique_id}</code> deleted.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("❌ File not found.")


# ─── /pending (Admin) — List pending payments ─────────────────────────────────
@admin_only
async def pending_payments_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    payments = await db.get_pending_payments(limit=5)
    if not payments:
        await update.message.reply_text("No pending payments.")
        return

    for p in payments:
        pid = str(p["_id"])
        plan = PREMIUM_PLANS.get(p["plan"], {})
        keyboard = [[
            InlineKeyboardButton("✅ Approve", callback_data=f"approve_payment_{pid}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"reject_payment_{pid}"),
        ]]
        await update.message.reply_text(
            f"💳 <b>Pending Payment</b>\n\n"
            f"User: <code>{p['user_id']}</code>\n"
            f"Plan: {plan.get('label', p['plan'])}\n"
            f"Amount: ₹{p['amount']}\n"
            f"UTR: <code>{p['utr']}</code>\n"
            f"Time: {helpers.fmt_datetime(p['created_at'])}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )


# ─── /status ─────────────────────────────────────────────────────────────────
async def status_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    is_prem = await db.is_premium(user_id)
    is_ver = await db.is_verified(user_id)

    prem_info = await db.get_premium_info(user_id) if is_prem else None
    prem_text = ""
    if prem_info and prem_info.get("expires_at"):
        prem_text = f"⏳ Expires in: {helpers.time_until(prem_info['expires_at'])}"

    await update.message.reply_text(
        f"👤 <b>Your Status</b>\n\n"
        f"💎 Premium: {'✅ Yes' if is_prem else '❌ No'}\n"
        f"{prem_text}\n"
        f"🔐 Verified: {'✅ Yes (24h)' if is_ver else '❌ No'}",
        parse_mode=ParseMode.HTML,
    )


# ─── Cleanup Task ─────────────────────────────────────────────────────────────
async def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    """Periodic cleanup — runs every hour."""
    await db.cleanup_expired_verifications()
    logger.info("Periodic cleanup completed")


# ─── Application Builder ──────────────────────────────────────────────────────
def build_app(token: str) -> Application:
    app = Application.builder().token(token).build()

    # Commands
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("premium", premium_command))
    app.add_handler(CommandHandler("pay", pay_command))
    app.add_handler(CommandHandler("status", status_handler))

    # Admin commands
    app.add_handler(CommandHandler("upload", upload_handler))
    app.add_handler(CommandHandler("batch", batch_start))
    app.add_handler(CommandHandler("batch_done", batch_done))
    app.add_handler(CommandHandler("stats", stats_handler))
    app.add_handler(CommandHandler("ban", ban_handler))
    app.add_handler(CommandHandler("unban", unban_handler))
    app.add_handler(CommandHandler("broadcast", broadcast_handler))
    app.add_handler(CommandHandler("grant", grant_handler))
    app.add_handler(CommandHandler("delete", delete_file_handler))
    app.add_handler(CommandHandler("pending", pending_payments_handler))

    # Callback queries
    app.add_handler(CallbackQueryHandler(callback_handler))

    # Batch media collection
    app.add_handler(MessageHandler(
        filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.ANIMATION,
        batch_collect,
    ))

    # Job: hourly cleanup
    app.job_queue.run_repeating(cleanup_task, interval=3600, first=60)

    return app
