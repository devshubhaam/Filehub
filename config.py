import os
from dotenv import load_dotenv

load_dotenv()

# ─── Bot Tokens ───────────────────────────────────────────────────────────────
BOT_TOKENS = [t.strip() for t in os.getenv("BOT_TOKENS", "").split(",") if t.strip()]
BOT_USERNAMES = [u.strip() for u in os.getenv("BOT_USERNAMES", "").split(",") if u.strip()]

# ─── Admin ────────────────────────────────────────────────────────────────────
ADMIN_IDS = [int(i) for i in os.getenv("ADMIN_IDS", "").split(",") if i.strip()]

# ─── MongoDB ──────────────────────────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "filepe")

# ─── Verification ─────────────────────────────────────────────────────────────
SHORTLINK_API_URL = os.getenv("SHORTLINK_API_URL", "")
SHORTLINK_API_KEY = os.getenv("SHORTLINK_API_KEY", "")
SHORTLINK_BASE = os.getenv("SHORTLINK_BASE", "https://shortlink.example.com")
VERIFICATION_HOURS = int(os.getenv("VERIFICATION_HOURS", "24"))

# ─── Premium Plans ────────────────────────────────────────────────────────────
PREMIUM_PLANS = {
    "7":  {"days": 7,  "price": 49,  "label": "7 Days"},
    "30": {"days": 30, "price": 149, "label": "30 Days"},
    "90": {"days": 90, "price": 399, "label": "90 Days"},
}

# ─── UPI ──────────────────────────────────────────────────────────────────────
UPI_ID = os.getenv("UPI_ID", "your@upi")
UPI_NAME = os.getenv("UPI_NAME", "FilePe Bot")

# ─── Auto-Delete ──────────────────────────────────────────────────────────────
AUTO_DELETE_MINUTES = int(os.getenv("AUTO_DELETE_MINUTES", "10"))

# ─── Rate Limiting ────────────────────────────────────────────────────────────
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "10"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))  # seconds
AUTO_BAN_THRESHOLD = int(os.getenv("AUTO_BAN_THRESHOLD", "30"))

# ─── Cache TTL ────────────────────────────────────────────────────────────────
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))  # 5 minutes

# ─── Flask ────────────────────────────────────────────────────────────────────
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "https://your-app.onrender.com")
FLASK_PORT = int(os.getenv("PORT", "8080"))
FLASK_SECRET = os.getenv("FLASK_SECRET", "change-this-secret-key")

# ─── Vercel Frontend ──────────────────────────────────────────────────────────
VERCEL_URL = os.getenv("VERCEL_URL", "https://your-app.vercel.app")

# ─── Messages ─────────────────────────────────────────────────────────────────
START_MSG = """
👋 <b>Welcome to FilePe!</b>

I can share files securely and instantly.

🔗 Use a file link to get started.
💎 Upgrade to Premium to skip ads.

Use /premium to see plans.
"""

FORCE_VERIFY_MSG = """
🔐 <b>Verification Required</b>

To access this file, please verify yourself first.

👇 Click the button below to verify (takes ~30 seconds):
"""

VERIFIED_MSG = "✅ <b>Verified!</b> Sending your file now..."

FILE_AUTO_DELETE_MSG = "⏳ This file will be <b>auto-deleted in {minutes} minutes</b> to save storage."

PREMIUM_MSG = """
💎 <b>FilePe Premium</b>

Skip ads, instant access, priority support!

<b>Plans:</b>
• 7 Days — ₹49
• 30 Days — ₹149  
• 90 Days — ₹399

Pay via UPI and submit your UTR number.
UPI ID: <code>{upi_id}</code>
"""
