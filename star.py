

import asyncio, logging, os, random, string, sqlite3
from datetime import datetime, date, timedelta
from typing import Optional

import aiosqlite
from dotenv import load_dotenv
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, BotCommand, ChatMember
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

load_dotenv()
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN    = os.getenv("BOT_TOKEN", "8758203475:AAFQs5kgTLQFVCQAo5BxmovMaJ5079r5ntI")
_ADMIN_RAW   = os.getenv("ADMIN_IDS", "8373846582")
ADMIN_IDS    = [int(x.strip()) for x in _ADMIN_RAW.split(",") if x.strip().isdigit()]
DB           = "bot_data.db"

# ──────────────────────────────────────────────────────────────────────────────
#  DATABASE
# ──────────────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id         INTEGER PRIMARY KEY,
    username        TEXT    DEFAULT '',
    full_name       TEXT    DEFAULT 'User',
    stars           INTEGER DEFAULT 0,
    total_earned    INTEGER DEFAULT 0,
    referred_by     INTEGER,
    referral_code   TEXT    UNIQUE,
    join_date       TEXT    DEFAULT (date('now')),
    last_daily      TEXT,
    daily_streak    INTEGER DEFAULT 0,
    tasks_completed INTEGER DEFAULT 0,
    is_banned       INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tasks (
    task_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT    NOT NULL,
    description  TEXT    DEFAULT '',
    task_type    TEXT    DEFAULT 'link',
    link         TEXT    DEFAULT '',
    channel_id   TEXT    DEFAULT '',
    stars_reward INTEGER DEFAULT 10,
    is_active    INTEGER DEFAULT 1,
    created_at   TEXT    DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS user_tasks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER,
    task_id      INTEGER,
    completed_at TEXT DEFAULT (datetime('now')),
    UNIQUE(user_id, task_id)
);
CREATE TABLE IF NOT EXISTS referrals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    referrer_id INTEGER,
    referee_id  INTEGER,
    completed   INTEGER DEFAULT 0,
    tasks_done  INTEGER DEFAULT 0,
    created_at  TEXT    DEFAULT (datetime('now')),
    UNIQUE(referee_id)
);
CREATE TABLE IF NOT EXISTS withdrawals (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER,
    stars        INTEGER,
    unique_code  TEXT    UNIQUE,
    status       TEXT    DEFAULT 'pending',
    requested_at TEXT    DEFAULT (datetime('now')),
    processed_at TEXT
);
CREATE TABLE IF NOT EXISTS transactions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id    INTEGER,
    amount     INTEGER,
    type       TEXT,
    note       TEXT    DEFAULT '',
    created_at TEXT    DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS announcements (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    message    TEXT,
    sent_by    INTEGER,
    sent_count INTEGER DEFAULT 0,
    created_at TEXT    DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

DEFAULT_SETTINGS = {
    "bot_name":           "🎁 Stars Gift Bot",
    "welcome_message":    "Welcome! Complete missions to earn stars!",
    "daily_stars":        "5",
    "daily_streak_bonus": "2",
    "ref_required_tasks": "5",
    "ref_bonus_stars":    "20",
    "min_withdrawal":     "100",
    "max_withdrawal":     "5000",
    "withdrawal_agent":   "",
    "maintenance_mode":   "0",
    "support_username":   "",
    "channel_username":   "",
}

async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.executescript(SCHEMA)
        for k, v in DEFAULT_SETTINGS.items():
            await db.execute("INSERT OR IGNORE INTO settings (key,value) VALUES (?,?)", (k, v))
        await db.commit()
    logger.info("DB ready.")


async def gset(key: str, default: str = "") -> str:
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT value FROM settings WHERE key=?", (key,)) as c:
            row = await c.fetchone()
            return row[0] if row else default

async def sset(key: str, value: str):
    async with aiosqlite.connect(DB) as db:
        await db.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, value))
        await db.commit()


def rnd_code(n=12) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))

def rnd_ref() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=10))


# ──────────────────────────────────────────────────────────────────────────────
#  USER HELPERS
# ──────────────────────────────────────────────────────────────────────────────

async def ensure_user(uid: int, username: str, full_name: str, referred_by: int = None):
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT user_id FROM users WHERE user_id=?", (uid,)) as c:
            exists = await c.fetchone()
        if not exists:
            code = rnd_ref()
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id,username,full_name,referral_code,referred_by) VALUES (?,?,?,?,?)",
                (uid, username or "", full_name or "User", code, referred_by)
            )
            if referred_by:
                await db.execute(
                    "INSERT OR IGNORE INTO referrals (referrer_id,referee_id) VALUES (?,?)",
                    (referred_by, uid)
                )
        else:
            await db.execute(
                "UPDATE users SET username=?,full_name=? WHERE user_id=?",
                (username or "", full_name or "User", uid)
            )
        await db.commit()


async def get_user(uid: int) -> Optional[dict]:
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id=?", (uid,)) as c:
            row = await c.fetchone()
            return dict(row) if row else None


async def add_stars(uid: int, amt: int, tx_type="earned", note=""):
    async with aiosqlite.connect(DB) as db:
        if amt > 0:
            await db.execute(
                "UPDATE users SET stars=stars+?,total_earned=total_earned+? WHERE user_id=?",
                (amt, amt, uid)
            )
        await db.execute(
            "INSERT INTO transactions (user_id,amount,type,note) VALUES (?,?,?,?)",
            (uid, amt, tx_type, note)
        )
        await db.commit()


async def sub_stars(uid: int, amt: int, tx_type="deducted", note=""):
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE users SET stars=MAX(0,stars-?) WHERE user_id=?", (amt, uid))
        await db.execute(
            "INSERT INTO transactions (user_id,amount,type,note) VALUES (?,?,?,?)",
            (uid, -amt, tx_type, note)
        )
        await db.commit()


async def set_stars(uid: int, amt: int, note="admin set"):
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE users SET stars=? WHERE user_id=?", (amt, uid))
        await db.execute(
            "INSERT INTO transactions (user_id,amount,type,note) VALUES (?,?,?,?)",
            (uid, amt, "admin_set", note)
        )
        await db.commit()


async def completed_ids(uid: int) -> list:
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT task_id FROM user_tasks WHERE user_id=?", (uid,)) as c:
            return [r[0] for r in await c.fetchall()]


async def mark_task_done(uid: int, task_id: int, stars: int) -> bool:
    async with aiosqlite.connect(DB) as db:
        try:
            await db.execute("INSERT INTO user_tasks (user_id,task_id) VALUES (?,?)", (uid, task_id))
            await db.execute(
                "UPDATE users SET stars=stars+?,total_earned=total_earned+?,tasks_completed=tasks_completed+1 WHERE user_id=?",
                (stars, stars, uid)
            )
            await db.execute(
                "INSERT INTO transactions (user_id,amount,type,note) VALUES (?,?,?,?)",
                (uid, stars, "task", f"Task #{task_id}")
            )
            await db.commit()
            return True
        except sqlite3.IntegrityError:
            return False


async def get_tasks(active_only=True) -> list:
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        q = "SELECT * FROM tasks WHERE is_active=1 ORDER BY task_id" if active_only else "SELECT * FROM tasks ORDER BY task_id DESC"
        async with db.execute(q) as c:
            return [dict(r) for r in await c.fetchall()]


async def get_task(tid: int) -> Optional[dict]:
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tasks WHERE task_id=?", (tid,)) as c:
            row = await c.fetchone()
            return dict(row) if row else None


async def get_leaderboard(limit=10) -> list:
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id,full_name,username,stars FROM users WHERE is_banned=0 ORDER BY stars DESC LIMIT ?",
            (limit,)
        ) as c:
            return [dict(r) for r in await c.fetchall()]


async def user_rank(uid: int) -> int:
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT COUNT(*)+1 FROM users WHERE stars>(SELECT stars FROM users WHERE user_id=?) AND is_banned=0",
            (uid,)
        ) as c:
            row = await c.fetchone()
            return row[0] if row else 0


async def all_user_ids() -> list:
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT user_id FROM users WHERE is_banned=0") as c:
            return [r[0] for r in await c.fetchall()]


async def get_txs(uid: int, limit=12) -> list:
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM transactions WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (uid, limit)
        ) as c:
            return [dict(r) for r in await c.fetchall()]


async def get_stats() -> dict:
    async with aiosqlite.connect(DB) as db:
        async def cnt(q, *p):
            async with db.execute(q, p) as c:
                return (await c.fetchone())[0]

        today = date.today().isoformat()
        return dict(
            total_users   = await cnt("SELECT COUNT(*) FROM users"),
            active_users  = await cnt("SELECT COUNT(*) FROM users WHERE is_banned=0"),
            banned_users  = await cnt("SELECT COUNT(*) FROM users WHERE is_banned=1"),
            new_today     = await cnt("SELECT COUNT(*) FROM users WHERE join_date=?", today),
            active_tasks  = await cnt("SELECT COUNT(*) FROM tasks WHERE is_active=1"),
            total_tasks   = await cnt("SELECT COUNT(*) FROM tasks"),
            completions   = await cnt("SELECT COUNT(*) FROM user_tasks"),
            total_stars   = await cnt("SELECT COALESCE(SUM(stars),0) FROM users"),
            all_earned    = await cnt("SELECT COALESCE(SUM(total_earned),0) FROM users"),
            total_refs    = await cnt("SELECT COUNT(*) FROM referrals"),
            done_refs     = await cnt("SELECT COUNT(*) FROM referrals WHERE completed=1"),
            pending_wds   = await cnt("SELECT COUNT(*) FROM withdrawals WHERE status='pending'"),
        )


# ──────────────────────────────────────────────────────────────────────────────
#  CHANNEL VERIFICATION
# ──────────────────────────────────────────────────────────────────────────────

async def is_member(bot, channel_id: str, uid: int) -> bool:
    try:
        ch = channel_id.strip()
        if not ch.startswith("@") and not ch.startswith("-"):
            ch = "@" + ch
        m = await bot.get_chat_member(ch, uid)
        return m.status in (ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER)
    except Exception:
        return False


def channel_url(channel_id: str, custom_link: str = "") -> str:
    if custom_link.startswith("http"):
        return custom_link
    ch = channel_id.strip().lstrip("@")
    if ch.startswith("-100") or ch.lstrip("-").isdigit():
        return ""   # numeric ID — no public link, admin must set link manually
    return f"https://t.me/{ch}"


async def periodic_channel_check(context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT t.task_id,t.channel_id,t.stars_reward,ut.user_id "
            "FROM tasks t JOIN user_tasks ut ON t.task_id=ut.task_id "
            "WHERE t.task_type='channel' AND t.channel_id!='' AND t.is_active=1"
        ) as c:
            rows = [dict(r) for r in await c.fetchall()]

    for row in rows:
        if not row["channel_id"]:
            continue
        ok = await is_member(context.bot, row["channel_id"], row["user_id"])
        if not ok:
            async with aiosqlite.connect(DB) as db:
                await db.execute(
                    "DELETE FROM user_tasks WHERE user_id=? AND task_id=?",
                    (row["user_id"], row["task_id"])
                )
                await db.execute(
                    "UPDATE users SET stars=MAX(0,stars-?),tasks_completed=MAX(0,tasks_completed-1) WHERE user_id=?",
                    (row["stars_reward"], row["user_id"])
                )
                await db.execute(
                    "INSERT INTO transactions (user_id,amount,type,note) VALUES (?,?,?,?)",
                    (row["user_id"], -row["stars_reward"], "deducted", f"Left channel task#{row['task_id']}")
                )
                await db.commit()
            try:
                t = await get_task(row["task_id"])
                tname = t["name"] if t else "task"
                await context.bot.send_message(
                    row["user_id"],
                    f"⚠️ *Stars Deducted!*\n\n"
                    f"You left the channel for: *{tname}*\n"
                    f"─ *{row['stars_reward']}⭐* removed from your account.\n\n"
                    f"Rejoin and complete the task again to earn them back!",
                    parse_mode="Markdown"
                )
            except Exception:
                pass


async def check_ref_progress(uid: int, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT referrer_id FROM referrals WHERE referee_id=? AND completed=0", (uid,)
        ) as c:
            row = await c.fetchone()
        if not row:
            return
        referrer = row[0]
        req = int(await gset("ref_required_tasks", "5"))
        async with db.execute("SELECT tasks_completed FROM users WHERE user_id=?", (uid,)) as c:
            tc = await c.fetchone()
        if not tc or tc[0] < req:
            return
        bonus = int(await gset("ref_bonus_stars", "20"))
        await db.execute("UPDATE referrals SET completed=1,tasks_done=? WHERE referee_id=?", (tc[0], uid))
        await db.execute(
            "UPDATE users SET stars=stars+?,total_earned=total_earned+? WHERE user_id=?",
            (bonus, bonus, referrer)
        )
        await db.execute(
            "INSERT INTO transactions (user_id,amount,type,note) VALUES (?,?,?,?)",
            (referrer, bonus, "referral", f"Ref #{uid} done {req} tasks")
        )
        await db.commit()
    try:
        u = await get_user(uid)
        name = (u.get("full_name") or "Someone") if u else "Someone"
        await context.bot.send_message(
            referrer,
            f"🎉 *Referral Bonus!*\n\n"
            f"Your friend *{name}* completed {req} missions!\n"
            f"⭐ You earned *{bonus} stars* bonus!",
            parse_mode="Markdown"
        )
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
#  KEYBOARDS
# ──────────────────────────────────────────────────────────────────────────────

def user_kbd():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🎯 Missions"),     KeyboardButton("👤 Profile")],
        [KeyboardButton("📅 Daily Reward"), KeyboardButton("🏆 Leaderboard")],
        [KeyboardButton("👥 Referral"),     KeyboardButton("💎 Withdraw")],
        [KeyboardButton("📊 My Stats"),     KeyboardButton("ℹ️ Help")],
    ], resize_keyboard=True, input_field_placeholder="Choose an option...")


def admin_kbd():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📊 Stats"),         KeyboardButton("👥 Users")],
        [KeyboardButton("🎯 Tasks"),         KeyboardButton("➕ Add Task")],
        [KeyboardButton("⚙️ Settings"),      KeyboardButton("📢 Broadcast")],
        [KeyboardButton("💎 Withdrawals"),   KeyboardButton("🔍 Find User")],
        [KeyboardButton("💸 Give Stars"),    KeyboardButton("✂️ Take Stars")],
        [KeyboardButton("🔧 Set Stars"),     KeyboardButton("🚫 Ban User")],
        [KeyboardButton("📋 Transactions"),  KeyboardButton("🔄 Reset User")],
        [KeyboardButton("🏠 Exit Admin")],
    ], resize_keyboard=True, input_field_placeholder="Admin panel...")


def task_icon(t: str) -> str:
    return {"link": "🔗", "channel": "📢", "manual": "📝", "youtube": "▶️", "twitter": "🐦"}.get(t, "▫️")


# ──────────────────────────────────────────────────────────────────────────────
#  STATE HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def st_set(ctx, state: str, **kw):
    ctx.user_data["state"] = state
    ctx.user_data.update(kw)

def st_clear(ctx):
    for k in list(ctx.user_data):
        if k != "is_admin_mode":
            del ctx.user_data[k]

def st_get(ctx) -> str:
    return ctx.user_data.get("state", "")


# ──────────────────────────────────────────────────────────────────────────────
#  /start
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    args = ctx.args or []

    referred_by = None
    if args:
        arg = args[0]
        if arg.startswith("ref_"):
            code = arg[4:]
            async with aiosqlite.connect(DB) as db:
                async with db.execute("SELECT user_id FROM users WHERE referral_code=?", (code,)) as c:
                    row = await c.fetchone()
                    if row and row[0] != u.id:
                        referred_by = row[0]

    await ensure_user(u.id, u.username or "", u.full_name or "User", referred_by)
    user = await get_user(u.id)

    if user and user.get("is_banned"):
        await update.message.reply_text("🚫 You have been banned from this bot.")
        return

    if await gset("maintenance_mode") == "1" and u.id not in ADMIN_IDS:
        await update.message.reply_text("🔧 Bot is under maintenance. Please try again later.")
        return

    # Force user mode on /start (even for admins — use /admin to switch)
    ctx.user_data.clear()

    bot_name = await gset("bot_name", "🎁 Stars Gift Bot")
    welcome  = await gset("welcome_message", "Complete missions, earn stars, claim gifts!")

    stars = user.get("stars", 0) if user else 0
    text = (
        f"✨ *{bot_name}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{welcome}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⭐ Your balance: *{stars} stars*\n\n"
        f"🎯 Complete missions to earn stars\n"
        f"📅 Claim daily bonus every day\n"
        f"👥 Invite friends for extra stars\n"
        f"💎 Withdraw your stars as gifts"
    )
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=user_kbd())

    if referred_by:
        await update.message.reply_text(
            "🎉 You joined via a referral! Complete missions to unlock your friend's bonus."
        )


# ──────────────────────────────────────────────────────────────────────────────
#  USER SCREENS
# ──────────────────────────────────────────────────────────────────────────────

async def screen_profile(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user = await get_user(uid)
    if not user:
        return
    done = await completed_ids(uid)
    rank = await user_rank(uid)
    streak = user.get("daily_streak", 0)

    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT COUNT(*),COALESCE(SUM(stars),0) FROM withdrawals WHERE user_id=? AND status='approved'", (uid,)) as c:
            wd_row = await c.fetchone()
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (uid,)) as c:
            refs_total = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=? AND completed=1", (uid,)) as c:
            refs_done = (await c.fetchone())[0]

    wd_count, wd_total = wd_row if wd_row else (0, 0)
    sf = "🔥 " + str(streak) + " day streak" if streak > 1 else ("🔥 1 day streak" if streak == 1 else "No streak yet")

    text = (
        f"👤 *My Profile*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔  `{uid}`\n"
        f"👤  *{user.get('full_name','?')}*"
        + (f"  @{user.get('username')}" if user.get('username') else "") + "\n\n"
        f"⭐  Balance: *{user.get('stars',0)} stars*\n"
        f"💎  Total earned: *{user.get('total_earned',0)}*\n"
        f"🏆  Global rank: *#{rank}*\n\n"
        f"✅  Tasks done: *{len(done)}*\n"
        f"👥  Referrals: *{refs_total}* ({refs_done} rewarded)\n"
        f"💸  Withdrawals: *{wd_count}* (total {wd_total}⭐)\n"
        f"🔥  Streak: {sf}\n"
        f"📆  Joined: {user.get('join_date','—')}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Transaction History", callback_data="my_txs"),
         InlineKeyboardButton("🔗 Referral Link",       callback_data="ref_link")]
    ])
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def screen_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    user = await get_user(uid)
    if not user:
        return
    done  = await completed_ids(uid)
    rank  = await user_rank(uid)
    tasks = await get_tasks()
    rem   = len(tasks) - len([t for t in tasks if t["task_id"] in done])

    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT COUNT(*),COALESCE(SUM(stars),0) FROM withdrawals WHERE user_id=? AND status='approved'", (uid,)
        ) as c:
            wd_row = await c.fetchone()

    wd_count, wd_total = wd_row if wd_row else (0, 0)
    prog = "▓" * len(done) + "░" * rem if (len(done) + rem) <= 20 else f"{len(done)}/{len(tasks)}"

    text = (
        f"📊 *My Statistics*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⭐  Stars: *{user.get('stars',0)}*\n"
        f"💎  Total earned: *{user.get('total_earned',0)}*\n"
        f"💸  Withdrawn: *{wd_total}* ({wd_count} times)\n\n"
        f"🏆  Rank: *#{rank}*\n"
        f"✅  Tasks done: *{len(done)}/{len(tasks)}*\n"
        f"📋  Remaining: *{rem}*\n"
        f"🔥  Streak: *{user.get('daily_streak',0)} days*\n\n"
        f"Progress: `{prog}`"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def screen_missions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    tasks = await get_tasks()
    done  = await completed_ids(uid)

    if not tasks:
        await update.message.reply_text(
            "📭 *No missions available yet.*\nCheck back soon — new missions are added regularly!",
            parse_mode="Markdown"
        )
        return

    pending = [t for t in tasks if t["task_id"] not in done]
    earned  = sum(t["stars_reward"] for t in pending)

    text = (
        f"🎯 *Missions*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Done: *{len(done)}/{len(tasks)}* tasks\n"
        f"💰 Earnable: *{earned}⭐* remaining\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    buttons = []
    for t in tasks:
        tid   = t["task_id"]
        is_dn = tid in done
        icon  = "✅" if is_dn else task_icon(t.get("task_type", "link"))
        label = f"{'✅' if is_dn else icon} {t['name']}  •  {t['stars_reward']}⭐"
        if is_dn:
            label = f"✅ {t['name']}  (Done)"
        text += f"{'✅' if is_dn else icon} *{t['name']}* — {t['stars_reward']}⭐\n"
        if t.get("description"):
            text += f"    _{t['description']}_\n"
        text += "\n"
        buttons.append([InlineKeyboardButton(label, callback_data=f"tview_{tid}")])

    if pending:
        first_id = pending[0]["task_id"]
        buttons.insert(0, [InlineKeyboardButton("▶️ Start All Missions", callback_data=f"tview_{first_id}")])

    await update.message.reply_text(
        text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None
    )


async def screen_daily(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    user = await get_user(uid)
    if not user:
        return

    today = date.today().isoformat()
    last  = user.get("last_daily")
    streak = user.get("daily_streak", 0)
    base   = int(await gset("daily_stars", "5"))
    bonus_per = int(await gset("daily_streak_bonus", "2"))

    if last == today:
        nxt = (date.today() + timedelta(days=1)).strftime("%d %b")
        await update.message.reply_text(
            f"⏰ *Daily Reward*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Already claimed today!\n\n"
            f"🔥 Streak: *{streak} days*\n"
            f"⏳ Next reward: *{nxt}*\n\n"
            f"Keep your streak for bonus stars!",
            parse_mode="Markdown"
        )
        return

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    new_streak = (streak + 1) if last == yesterday else 1
    bonus      = (new_streak - 1) * bonus_per
    total      = base + bonus

    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "UPDATE users SET stars=stars+?,total_earned=total_earned+?,last_daily=?,daily_streak=? WHERE user_id=?",
            (total, total, today, new_streak, uid)
        )
        await db.execute(
            "INSERT INTO transactions (user_id,amount,type,note) VALUES (?,?,?,?)",
            (uid, total, "daily", f"Day-{new_streak} streak")
        )
        await db.commit()

    u2 = await get_user(uid)
    streak_txt = f"\n🔥 *{new_streak} day streak!* +{bonus}⭐ bonus" if new_streak > 1 else ""

    await update.message.reply_text(
        f"🎁 *Daily Reward Claimed!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⭐ Base reward: *{base} stars*"
        f"{streak_txt}\n"
        f"🎉 Total received: *{total} stars*\n\n"
        f"💰 New balance: *{u2.get('stars',0) if u2 else total}⭐*\n"
        f"🔥 Streak: *{new_streak} day{'s' if new_streak != 1 else ''}*",
        parse_mode="Markdown"
    )


async def screen_leaderboard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid     = update.effective_user.id
    leaders = await get_leaderboard(10)
    rank    = await user_rank(uid)
    user    = await get_user(uid)
    medals  = {0: "🥇", 1: "🥈", 2: "🥉"}

    text = "🏆 *Top 10 Leaderboard*\n━━━━━━━━━━━━━━━━━━━━━\n\n"
    for i, lu in enumerate(leaders):
        m    = medals.get(i, f"{i+1}.")
        name = (lu.get("full_name") or "User")[:20]
        you  = " 👈 *You*" if lu["user_id"] == uid else ""
        text += f"{m} *{name}* — {lu['stars']}⭐{you}\n"

    if not leaders:
        text += "_Nobody yet — be the first!_"

    my_stars = user.get("stars", 0) if user else 0
    text += f"\n━━━━━━━━━━━━━━━━━━━━━\n📍 Your position: *#{rank}* • *{my_stars}⭐*"
    await update.message.reply_text(text, parse_mode="Markdown")


async def screen_referral(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    user = await get_user(uid)
    if not user:
        return
    bi    = await ctx.bot.get_me()
    code  = user.get("referral_code", "")
    link  = f"https://t.me/{bi.username}?start=ref_{code}"
    req   = int(await gset("ref_required_tasks", "5"))
    bonus = int(await gset("ref_bonus_stars", "20"))

    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (uid,)) as c:
            total_r = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=? AND completed=1", (uid,)) as c:
            done_r = (await c.fetchone())[0]
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT r.tasks_done,u.full_name FROM referrals r "
            "JOIN users u ON r.referee_id=u.user_id "
            "WHERE r.referrer_id=? AND r.completed=0 LIMIT 5", (uid,)
        ) as c:
            pending = [dict(r) for r in await c.fetchall()]

    p_text = ""
    if pending:
        p_text = "\n⏳ *Pending:*\n"
        for p in pending:
            td   = p.get("tasks_done", 0)
            name = (p.get("full_name") or "User")[:14]
            bar  = "▓" * td + "░" * (req - td)
            p_text += f"  • {name}: `{bar}` {td}/{req}\n"

    text = (
        f"👥 *Referral Program*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎁 Reward: *{bonus}⭐* per referral\n"
        f"📋 Requirement: friend completes *{req} tasks*\n\n"
        f"📊 *Your Referrals:*\n"
        f"  Total: *{total_r}*  |  Rewarded: ✅ *{done_r}*\n"
        f"  Earned: *{done_r * bonus}⭐ total*\n"
        f"{p_text}\n"
        f"🔗 *Your Link:*\n`{link}`"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "📤 Share Link",
            url=f"https://t.me/share/url?url={link}&text=Join+me+and+earn+stars!"
        )
    ]])
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def screen_withdraw(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    user  = await get_user(uid)
    if not user:
        return
    bal   = user.get("stars", 0)
    min_w = int(await gset("min_withdrawal", "100"))
    max_w = int(await gset("max_withdrawal", "5000"))
    agent = await gset("withdrawal_agent", "")

    if not agent:
        await update.message.reply_text(
            "⚠️ *Withdrawals Unavailable*\n\nNot configured yet. Contact support.",
            parse_mode="Markdown"
        )
        return

    if bal < min_w:
        await update.message.reply_text(
            f"💎 *Withdraw Stars*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"⭐ Balance: *{bal}*\n"
            f"📉 Minimum: *{min_w}⭐*\n\n"
            f"❌ You need *{min_w - bal} more stars*!\n"
            f"Complete missions and refer friends to earn faster.",
            parse_mode="Markdown"
        )
        return

    clamp = min(bal, max_w)
    tiers = [a for a in [100, 250, 500, 1000, 2500, 5000] if min_w <= a <= clamp]
    if not tiers:
        tiers = [clamp]

    buttons = [[InlineKeyboardButton(f"💎 {a}⭐", callback_data=f"wd_{a}")] for a in tiers]

    text = (
        f"💎 *Withdraw Stars*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⭐ Balance: *{bal}*\n"
        f"📉 Min: *{min_w}⭐*  •  📈 Max: *{max_w}⭐*\n"
        f"📤 Agent: @{agent}\n\n"
        f"Select amount to withdraw:\n"
        f"_(A unique code will be generated)_"
    )
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons))


async def screen_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    name    = await gset("bot_name", "Stars Gift Bot")
    support = await gset("support_username", "")
    channel = await gset("channel_username", "")

    text = (
        f"ℹ️ *{name} — Help*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"*🎯 Missions*\n"
        f"Complete tasks to earn stars. Browse them with Next/Skip/Complete buttons.\n\n"
        f"*📅 Daily Reward*\n"
        f"Claim free stars every day. Build a streak for bonus stars!\n\n"
        f"*👥 Referrals*\n"
        f"Share your referral link. Earn bonus stars when friends complete tasks.\n\n"
        f"*💎 Withdraw*\n"
        f"Reach the minimum balance. A unique code is generated — send it to our agent.\n\n"
        f"*⚠️ Channel Tasks*\n"
        f"If you leave a channel, stars are automatically deducted. Stay to keep them!\n\n"
        f"*🏆 Leaderboard*\n"
        f"Top 10 players by star count."
    )
    if support or channel:
        text += "\n━━━━━━━━━━━━━━━━━━━━━\n"
        if support:
            text += f"📞 Support: @{support}\n"
        if channel:
            text += f"📢 Updates: @{channel}\n"
    await update.message.reply_text(text, parse_mode="Markdown")


# ──────────────────────────────────────────────────────────────────────────────
#  TASK CAROUSEL (multi-task flow)
# ──────────────────────────────────────────────────────────────────────────────

async def show_task_card(target, ctx, task_id: int, uid: int):
    """Render a task card. `target` is a Message or CallbackQuery."""
    task  = await get_task(task_id)
    if not task or not task["is_active"]:
        txt = "❌ This task is no longer available."
        if hasattr(target, "edit_message_text"):
            await target.edit_message_text(txt)
        else:
            await target.reply_text(txt)
        return

    tasks = await get_tasks()
    done  = await completed_ids(uid)
    ids   = [t["task_id"] for t in tasks]
    idx   = ids.index(task_id) if task_id in ids else 0
    total = len(ids)
    pending_ids = [t["task_id"] for t in tasks if t["task_id"] not in done]

    is_done  = task_id in done
    t_type   = task.get("task_type", "link")
    link     = (task.get("link") or "").strip()
    ch_id    = (task.get("channel_id") or "").strip()
    stars    = task["stars_reward"]
    name     = task["name"]
    desc     = task.get("description", "")

    # Compose card text
    status_line = "✅ *Already Completed!*\n\n" if is_done else ""
    text = (
        f"{status_line}"
        f"{task_icon(t_type)} *{name}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏷  Type: {t_type.capitalize()}\n"
        f"⭐  Reward: *{stars} stars*\n"
    )
    if desc:
        text += f"📝  {desc}\n"
    text += f"\n📌  Task {idx+1} of {total}"

    # Build buttons
    rows = []

    # Action button (if applicable)
    if t_type == "channel" and ch_id:
        url = channel_url(ch_id, link)
        if url:
            rows.append([InlineKeyboardButton(f"📢 Join Channel", url=url)])
    elif link and link.startswith("http"):
        icon = task_icon(t_type)
        rows.append([InlineKeyboardButton(f"{icon} Open Link", url=link)])

    # Complete / Skip / Next
    nav_row = []
    if not is_done:
        nav_row.append(InlineKeyboardButton("✅ Complete", callback_data=f"tverify_{task_id}"))

    # Next incomplete task (skip this one)
    next_pid = None
    cur_idx_in_pending = pending_ids.index(task_id) if task_id in pending_ids else -1
    if cur_idx_in_pending >= 0 and cur_idx_in_pending + 1 < len(pending_ids):
        next_pid = pending_ids[cur_idx_in_pending + 1]
    elif pending_ids and task_id not in pending_ids:
        next_pid = pending_ids[0] if pending_ids else None

    if next_pid:
        if not is_done:
            nav_row.append(InlineKeyboardButton("⏭ Skip", callback_data=f"tview_{next_pid}"))
        else:
            nav_row.append(InlineKeyboardButton("⏩ Next Task", callback_data=f"tview_{next_pid}"))
    elif not is_done and not next_pid:
        # No more pending tasks
        nav_row.append(InlineKeyboardButton("🎊 Last Task!", callback_data="missions_done"))

    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton("⬅️ All Missions", callback_data="missions_list")])

    kb = InlineKeyboardMarkup(rows)

    if hasattr(target, "edit_message_text"):
        try:
            await target.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
        except Exception:
            await target.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await target.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def handle_task_verify(query, ctx, task_id: int):
    uid  = query.from_user.id
    task = await get_task(task_id)
    if not task:
        await query.answer("❌ Task not found.", show_alert=True)
        return

    done = await completed_ids(uid)
    if task_id in done:
        await query.answer("✅ Already completed!", show_alert=True)
        return

    # Channel check
    if task.get("task_type") == "channel" and task.get("channel_id"):
        ok = await is_member(ctx.bot, task["channel_id"], uid)
        if not ok:
            await query.answer("❌ Please join the channel first!", show_alert=True)
            return

    ok = await mark_task_done(uid, task_id, task["stars_reward"])
    if not ok:
        await query.answer("Already done!", show_alert=True)
        return

    await check_ref_progress(uid, ctx)
    await query.answer(f"🎉 +{task['stars_reward']}⭐ earned!", show_alert=True)

    # Auto-advance to next task
    tasks    = await get_tasks()
    new_done = await completed_ids(uid)
    pending  = [t for t in tasks if t["task_id"] not in new_done]

    if pending:
        await show_task_card(query, ctx, pending[0]["task_id"], uid)
    else:
        user = await get_user(uid)
        await query.edit_message_text(
            f"🏆 *All Missions Complete!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎊 You've finished every available mission!\n\n"
            f"💰 Total stars: *{user.get('stars',0) if user else '?'}⭐*\n\n"
            f"Check back later for new missions!",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💎 Withdraw Stars", callback_data="go_withdraw")]
            ])
        )


# ──────────────────────────────────────────────────────────────────────────────
#  WITHDRAWAL FLOW
# ──────────────────────────────────────────────────────────────────────────────

async def handle_wd_request(query, ctx, stars: int):
    uid  = query.from_user.id
    user = await get_user(uid)
    if not user:
        return

    bal   = user.get("stars", 0)
    min_w = int(await gset("min_withdrawal", "100"))
    max_w = int(await gset("max_withdrawal", "5000"))
    agent = await gset("withdrawal_agent", "")

    if bal < stars or stars < min_w or stars > max_w:
        await query.answer("❌ Invalid amount.", show_alert=True)
        return
    if not agent:
        await query.answer("❌ Withdrawals not configured.", show_alert=True)
        return

    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT id FROM withdrawals WHERE user_id=? AND status='pending'", (uid,)
        ) as c:
            if await c.fetchone():
                await query.answer("⚠️ You already have a pending withdrawal!", show_alert=True)
                return

    code = rnd_code(12)
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT INTO withdrawals (user_id,stars,unique_code) VALUES (?,?,?)",
            (uid, stars, code)
        )
        await db.execute("UPDATE users SET stars=stars-? WHERE user_id=?", (stars, uid))
        await db.execute(
            "INSERT INTO transactions (user_id,amount,type,note) VALUES (?,?,?,?)",
            (uid, -stars, "withdrawal", f"WD-{code}")
        )
        await db.commit()

    text = (
        f"💎 *Withdrawal Request Created!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⭐ Amount: *{stars} stars*\n"
        f"📤 Agent: @{agent}\n\n"
        f"🔑 *Your Unique Code:*\n"
        f"`{code}`\n\n"
        f"📋 *Steps to Claim:*\n"
        f"1️⃣  Copy the code above\n"
        f"2️⃣  Open Telegram → message @{agent}\n"
        f"3️⃣  Send this code to the agent\n"
        f"4️⃣  Agent will verify and deliver your gift\n\n"
        f"⏳ Status: *Pending Review*\n"
        f"⚠️ This code is unique to you. Only share with @{agent}!"
    )
    await query.edit_message_text(text, parse_mode="Markdown")

    # Notify admins
    for aid in ADMIN_IDS:
        try:
            await ctx.bot.send_message(
                aid,
                f"💎 *New Withdrawal Request*\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 *{user.get('full_name','?')}* (`{uid}`)"
                + (f"\n🏷  @{user.get('username')}" if user.get("username") else "") +
                f"\n⭐ Stars: *{stars}*\n"
                f"🔑 Code: `{code}`\n"
                f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Approve", callback_data=f"wdok_{code}"),
                    InlineKeyboardButton("❌ Reject",  callback_data=f"wdno_{code}")
                ]])
            )
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────────────────────
#  USER CALLBACK ROUTER
# ──────────────────────────────────────────────────────────────────────────────

async def user_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = q.from_user.id
    data = q.data
    await q.answer()

    if data == "my_txs":
        txs = await get_txs(uid)
        if not txs:
            await q.edit_message_text("📋 No transactions yet.")
            return
        text = "📋 *Recent Transactions*\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        for tx in txs:
            a  = tx["amount"]
            sg = "+" if a > 0 else ""
            ic = "⭐" if a > 0 else "💸"
            text += f"{ic} *{sg}{a}⭐*  —  _{tx['type']}_\n"
            if tx.get("note"):
                text += f"    `{tx['note'][:25]}`\n"
            text += f"    📅 {tx['created_at'][:16]}\n\n"
        await q.edit_message_text(text, parse_mode="Markdown")
        return

    if data == "ref_link":
        user = await get_user(uid)
        if not user:
            return
        bi    = await ctx.bot.get_me()
        code  = user.get("referral_code", "")
        link  = f"https://t.me/{bi.username}?start=ref_{code}"
        bonus = await gset("ref_bonus_stars", "20")
        req   = await gset("ref_required_tasks", "5")
        await q.edit_message_text(
            f"🔗 *Your Referral Link*\n\n`{link}`\n\n"
            f"Earn *{bonus}⭐* when a friend completes *{req} tasks*!",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📤 Share", url=f"https://t.me/share/url?url={link}&text=Join+and+earn+stars!")
            ]])
        )
        return

    if data.startswith("tview_"):
        tid = int(data.split("_")[1])
        await show_task_card(q, ctx, tid, uid)
        return

    if data.startswith("tverify_"):
        tid = int(data.split("_")[1])
        await handle_task_verify(q, ctx, tid)
        return

    if data == "missions_list":
        tasks = await get_tasks()
        done  = await completed_ids(uid)
        buttons = []
        for t in tasks:
            is_dn = t["task_id"] in done
            icon  = "✅" if is_dn else task_icon(t.get("task_type", "link"))
            label = f"{'✅' if is_dn else icon} {t['name']}  •  {t['stars_reward']}⭐"
            buttons.append([InlineKeyboardButton(label, callback_data=f"tview_{t['task_id']}")])
        text = f"🎯 *Missions*\n━━━━━━━━━━━━━━━━━━━━━\n✅ Done: {len(done)}/{len(tasks)}"
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)
        return

    if data == "missions_done":
        await q.answer("🎊 All missions completed!", show_alert=True)
        return

    if data.startswith("wd_"):
        amt = int(data.split("_")[1])
        await handle_wd_request(q, ctx, amt)
        return

    if data == "go_withdraw":
        # Redirect to withdraw in chat (can't use reply_text from inline)
        user = await get_user(uid)
        if user:
            bal   = user.get("stars", 0)
            min_w = int(await gset("min_withdrawal", "100"))
            max_w = int(await gset("max_withdrawal", "5000"))
            agent = await gset("withdrawal_agent", "")
            clamp = min(bal, max_w)
            tiers = [a for a in [100, 250, 500, 1000, 2500, 5000] if min_w <= a <= clamp]
            if not tiers:
                tiers = [clamp] if clamp >= min_w else []
            buttons = [[InlineKeyboardButton(f"💎 {a}⭐", callback_data=f"wd_{a}")] for a in tiers]
            text = (
                f"💎 *Withdraw Stars*\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"⭐ Balance: *{bal}*  |  Min: *{min_w}⭐*  Max: *{max_w}⭐*\n"
                f"📤 Agent: @{agent if agent else 'N/A'}"
            )
            await q.edit_message_text(text, parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)
        return


# ──────────────────────────────────────────────────────────────────────────────
#  ADMIN PANEL
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("❌ Access denied.")
        return
    ctx.user_data.clear()
    ctx.user_data["is_admin_mode"] = True
    await update.message.reply_text(
        "🔧 *Admin Panel*\nUse the menu below:",
        parse_mode="Markdown",
        reply_markup=admin_kbd()
    )


async def handle_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    if uid not in ADMIN_IDS:
        return
    text  = update.message.text
    state = st_get(ctx)

    # ── Top-level admin menu buttons ──────────────────────────────────────────
    if text == "🏠 Exit Admin":
        ctx.user_data.clear()
        await update.message.reply_text("✅ Back to user mode.", reply_markup=user_kbd())
        return

    if text == "📊 Stats" and not state:
        s = await get_stats()
        await update.message.reply_text(
            f"📊 *Bot Statistics*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 Total users:   *{s['total_users']}*\n"
            f"✅ Active:         *{s['active_users']}*\n"
            f"🚫 Banned:         *{s['banned_users']}*\n"
            f"🆕 New today:      *{s['new_today']}*\n\n"
            f"🎯 Active tasks:   *{s['active_tasks']}/{s['total_tasks']}*\n"
            f"📋 Completions:    *{s['completions']}*\n\n"
            f"⭐ Stars in circ:  *{s['total_stars']}*\n"
            f"💎 Ever earned:    *{s['all_earned']}*\n\n"
            f"👥 Referrals:      *{s['total_refs']}* ({s['done_refs']} done)\n"
            f"💸 Pending WDs:    *{s['pending_wds']}*",
            parse_mode="Markdown"
        )
        return

    if text == "👥 Users" and not state:
        await admin_list_users(update, ctx, page=0)
        return

    if text == "🎯 Tasks" and not state:
        tasks = await get_tasks(active_only=False)
        if not tasks:
            await update.message.reply_text("No tasks yet. Use ➕ Add Task.")
            return
        btns = []
        txt  = "🎯 *All Tasks*\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        for t in tasks:
            s  = "✅" if t["is_active"] else "❌"
            ic = task_icon(t.get("task_type","link"))
            txt += f"{s} [{t['task_id']}] {ic} *{t['name']}* — {t['stars_reward']}⭐\n"
            btns.append([
                InlineKeyboardButton(f"{s} {t['name'][:16]}", callback_data=f"at_view_{t['task_id']}"),
                InlineKeyboardButton("🔄",  callback_data=f"at_tog_{t['task_id']}"),
                InlineKeyboardButton("🗑",  callback_data=f"at_del_{t['task_id']}"),
            ])
        await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(btns))
        return

    if text == "➕ Add Task" and not state:
        st_set(ctx, "atask_name")
        await update.message.reply_text(
            "➕ *New Task — Step 1/5*\n\nEnter task *name*:\n_(or /cancel to stop)_",
            parse_mode="Markdown"
        )
        return

    if text == "⚙️ Settings" and not state:
        await admin_settings(update, ctx)
        return

    if text == "📢 Broadcast" and not state:
        st_set(ctx, "broadcast")
        await update.message.reply_text(
            "📢 *Broadcast*\n\nType your message. Supports *bold*, _italic_, `code`.\n\n_(or /cancel)_",
            parse_mode="Markdown"
        )
        return

    if text == "💎 Withdrawals" and not state:
        await admin_list_wds(update, ctx)
        return

    if text == "🔍 Find User" and not state:
        st_set(ctx, "find_user")
        await update.message.reply_text("🔍 Enter user ID or @username:")
        return

    if text == "💸 Give Stars" and not state:
        st_set(ctx, "give_uid")
        await update.message.reply_text("💸 *Give Stars — Step 1/2*\n\nEnter *User ID*:", parse_mode="Markdown")
        return

    if text == "✂️ Take Stars" and not state:
        st_set(ctx, "take_uid")
        await update.message.reply_text("✂️ *Remove Stars — Step 1/2*\n\nEnter *User ID*:", parse_mode="Markdown")
        return

    if text == "🔧 Set Stars" and not state:
        st_set(ctx, "sset_uid")
        await update.message.reply_text("🔧 *Set Stars — Step 1/2*\n\nEnter *User ID*:", parse_mode="Markdown")
        return

    if text == "🚫 Ban User" and not state:
        st_set(ctx, "ban_uid")
        await update.message.reply_text("🚫 Enter *User ID* to ban/unban:", parse_mode="Markdown")
        return

    if text == "📋 Transactions" and not state:
        st_set(ctx, "tx_uid")
        await update.message.reply_text("📋 Enter *User ID* to view transactions:", parse_mode="Markdown")
        return

    if text == "🔄 Reset User" and not state:
        st_set(ctx, "reset_uid")
        await update.message.reply_text("🔄 Enter *User ID* to reset (clears stars, tasks, streak):", parse_mode="Markdown")
        return

    # ── State-based input ──────────────────────────────────────────────────────

    # Add Task flow
    if state == "atask_name":
        ctx.user_data["t_name"] = text
        st_set(ctx, "atask_desc")
        await update.message.reply_text("Step 2/5 — Enter task *description* (or `skip`):", parse_mode="Markdown")
        return

    if state == "atask_desc":
        ctx.user_data["t_desc"] = "" if text.lower() == "skip" else text
        st_set(ctx, "atask_type")
        await update.message.reply_text(
            "Step 3/5 — Select task *type*:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔗 Link",    callback_data="aty_link"),
                 InlineKeyboardButton("📢 Channel", callback_data="aty_channel")],
                [InlineKeyboardButton("📝 Manual",  callback_data="aty_manual"),
                 InlineKeyboardButton("▶️ YouTube", callback_data="aty_youtube")],
            ])
        )
        return

    if state == "atask_link":
        ctx.user_data["t_link"] = text
        if ctx.user_data.get("t_type") == "channel":
            ctx.user_data["t_channel"] = text.lstrip("@").replace("https://t.me/", "")
        st_set(ctx, "atask_stars")
        await update.message.reply_text("Step 5/5 — How many ⭐ stars reward?")
        return

    if state == "atask_stars":
        if text.isdigit() and int(text) > 0:
            await save_new_task(update, ctx, int(text))
        else:
            await update.message.reply_text("❌ Enter a valid positive number:")
        return

    # Broadcast
    if state == "broadcast":
        await do_broadcast(update, ctx, text)
        return

    # Find user
    if state == "find_user":
        await find_user_cmd(update, ctx, text)
        st_clear(ctx)
        ctx.user_data["is_admin_mode"] = True
        return

    # Give stars
    if state == "give_uid":
        if text.lstrip("-").isdigit():
            ctx.user_data["t_uid"] = int(text)
            u = await get_user(int(text))
            name = u.get("full_name","?") if u else "Not found"
            st_set(ctx, "give_amt")
            await update.message.reply_text(
                f"User: *{name}* (`{text}`)\nStep 2/2 — Stars to give:", parse_mode="Markdown"
            )
        else:
            await update.message.reply_text("❌ Enter numeric user ID:")
        return

    if state == "give_amt":
        if text.isdigit() and int(text) > 0:
            t = ctx.user_data.get("t_uid")
            a = int(text)
            await add_stars(t, a, "admin_gift", f"Gift from admin")
            await update.message.reply_text(f"✅ Gave *{a}⭐* to `{t}`", parse_mode="Markdown")
            try:
                await ctx.bot.send_message(t, f"🎁 *Gift!*\n\nAn admin gave you *{a}⭐*!", parse_mode="Markdown")
            except Exception: pass
            st_clear(ctx); ctx.user_data["is_admin_mode"] = True
        else:
            await update.message.reply_text("❌ Enter a positive number:")
        return

    # Take stars
    if state == "take_uid":
        if text.lstrip("-").isdigit():
            ctx.user_data["t_uid"] = int(text)
            u = await get_user(int(text))
            name = u.get("full_name","?") if u else "Not found"
            st_set(ctx, "take_amt")
            await update.message.reply_text(
                f"User: *{name}* (`{text}`)\nStep 2/2 — Stars to remove:", parse_mode="Markdown"
            )
        else:
            await update.message.reply_text("❌ Enter numeric user ID:")
        return

    if state == "take_amt":
        if text.isdigit() and int(text) > 0:
            t = ctx.user_data.get("t_uid")
            a = int(text)
            await sub_stars(t, a, "admin_deduct", f"Deducted by admin")
            await update.message.reply_text(f"✅ Removed *{a}⭐* from `{t}`", parse_mode="Markdown")
            try:
                await ctx.bot.send_message(t, f"⚠️ *Stars Removed*\n\nAn admin deducted *{a}⭐* from your balance.", parse_mode="Markdown")
            except Exception: pass
            st_clear(ctx); ctx.user_data["is_admin_mode"] = True
        else:
            await update.message.reply_text("❌ Enter a positive number:")
        return

    # Set stars directly
    if state == "sset_uid":
        if text.lstrip("-").isdigit():
            ctx.user_data["t_uid"] = int(text)
            u = await get_user(int(text))
            name = u.get("full_name","?") if u else "Not found"
            st_set(ctx, "sset_amt")
            await update.message.reply_text(
                f"User: *{name}* (`{text}`)\nEnter the *exact* star balance to set:", parse_mode="Markdown"
            )
        else:
            await update.message.reply_text("❌ Enter numeric user ID:")
        return

    if state == "sset_amt":
        if text.isdigit():
            t = ctx.user_data.get("t_uid")
            a = int(text)
            await set_stars(t, a, f"Set by admin")
            await update.message.reply_text(f"✅ Set `{t}` balance to *{a}⭐*", parse_mode="Markdown")
            st_clear(ctx); ctx.user_data["is_admin_mode"] = True
        else:
            await update.message.reply_text("❌ Enter a non-negative number:")
        return

    # Ban user
    if state == "ban_uid":
        if text.lstrip("-").isdigit():
            t  = int(text)
            u  = await get_user(t)
            if u:
                nb = 0 if u.get("is_banned") else 1
                async with aiosqlite.connect(DB) as db:
                    await db.execute("UPDATE users SET is_banned=? WHERE user_id=?", (nb, t))
                    await db.commit()
                act = "🚫 Banned" if nb else "✅ Unbanned"
                await update.message.reply_text(f"{act} *{u.get('full_name','?')}* (`{t}`)", parse_mode="Markdown")
                if nb:
                    try: await ctx.bot.send_message(t, "🚫 You have been banned.")
                    except Exception: pass
            else:
                await update.message.reply_text("❌ User not found.")
            st_clear(ctx); ctx.user_data["is_admin_mode"] = True
        else:
            await update.message.reply_text("❌ Enter numeric user ID:")
        return

    # Reset user
    if state == "reset_uid":
        if text.lstrip("-").isdigit():
            t = int(text)
            u = await get_user(t)
            if u:
                async with aiosqlite.connect(DB) as db:
                    await db.execute("DELETE FROM user_tasks WHERE user_id=?", (t,))
                    await db.execute("UPDATE users SET stars=0,total_earned=0,tasks_completed=0,daily_streak=0 WHERE user_id=?", (t,))
                    await db.commit()
                await update.message.reply_text(
                    f"🔄 Reset *{u.get('full_name','?')}* — stars, tasks and streak cleared.",
                    parse_mode="Markdown"
                )
            else:
                await update.message.reply_text("❌ User not found.")
            st_clear(ctx); ctx.user_data["is_admin_mode"] = True
        else:
            await update.message.reply_text("❌ Enter numeric user ID:")
        return

    # Transactions
    if state == "tx_uid":
        if text.lstrip("-").isdigit():
            t   = int(text)
            txs = await get_txs(t, 15)
            u   = await get_user(t)
            name = u.get("full_name","?") if u else str(t)
            if txs:
                msg = f"📋 *Transactions — {name}*\n━━━━━━━━━━━━━━━━━━━━━\n\n"
                for tx in txs:
                    a  = tx["amount"]
                    sg = "+" if a > 0 else ""
                    msg += f"{sg}{a}⭐  {tx['type']}  {tx.get('note','')[:20]}\n"
                    msg += f"  📅 {tx['created_at'][:16]}\n"
            else:
                msg = f"No transactions for `{t}`"
            await update.message.reply_text(msg, parse_mode="Markdown")
            st_clear(ctx); ctx.user_data["is_admin_mode"] = True
        else:
            await update.message.reply_text("❌ Enter numeric user ID:")
        return

    # Settings states
    setting_states = {
        "s_botname":  ("bot_name",           "✅ Bot name updated!"),
        "s_welcome":  ("welcome_message",     "✅ Welcome message updated!"),
        "s_daily":    ("daily_stars",         "✅ Daily stars updated!"),
        "s_streak":   ("daily_streak_bonus",  "✅ Streak bonus updated!"),
        "s_reftasks": ("ref_required_tasks",  "✅ Referral task requirement updated!"),
        "s_refbonus": ("ref_bonus_stars",     "✅ Referral bonus updated!"),
        "s_minwd":    ("min_withdrawal",      "✅ Min withdrawal updated!"),
        "s_maxwd":    ("max_withdrawal",      "✅ Max withdrawal updated!"),
        "s_agent":    ("withdrawal_agent",    "✅ Withdrawal agent updated!"),
        "s_support":  ("support_username",    "✅ Support username updated!"),
        "s_channel":  ("channel_username",    "✅ Channel updated!"),
    }
    if state in setting_states:
        key, msg = setting_states[state]
        val = text.lstrip("@") if state in ("s_agent","s_support","s_channel") else text
        await sset(key, val)
        await update.message.reply_text(msg)
        st_clear(ctx); ctx.user_data["is_admin_mode"] = True
        return

    # Unhandled in admin mode — show hint
    if not state:
        await update.message.reply_text("Use the admin menu buttons below 👇", reply_markup=admin_kbd())


async def admin_list_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE, page=0):
    per = 8
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id,full_name,stars,is_banned FROM users ORDER BY stars DESC LIMIT ? OFFSET ?",
            (per, page * per)
        ) as c:
            users = [dict(r) for r in await c.fetchall()]
        async with db.execute("SELECT COUNT(*) FROM users") as c:
            total = (await c.fetchone())[0]

    pages = max(1, (total - 1) // per + 1)
    txt = f"👥 *Users* — Page {page+1}/{pages}\n━━━━━━━━━━━━━━━━━━━━━\n\n"
    btns = []
    for u in users:
        ic = "🚫" if u["is_banned"] else "👤"
        txt += f"{ic} `{u['user_id']}` *{u['full_name'][:16]}* {u['stars']}⭐\n"
        btns.append([InlineKeyboardButton(
            f"{ic} {u['full_name'][:18]} ({u['stars']}⭐)",
            callback_data=f"au_view_{u['user_id']}"
        )])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"au_pg_{page-1}"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"au_pg_{page+1}"))
    if nav:
        btns.append(nav)
    await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(btns) if btns else None)


async def admin_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    v = {k: await gset(k) for k in DEFAULT_SETTINGS}
    text = (
        f"⚙️ *Settings*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Bot name:       *{v['bot_name']}*\n"
        f"📅 Daily stars:    *{v['daily_stars']}*\n"
        f"🔥 Streak bonus:   *+{v['daily_streak_bonus']}⭐/day*\n"
        f"👥 Ref tasks req:  *{v['ref_required_tasks']}*\n"
        f"🎁 Ref bonus:      *{v['ref_bonus_stars']}⭐*\n"
        f"📉 Min withdrawal: *{v['min_withdrawal']}⭐*\n"
        f"📈 Max withdrawal: *{v['max_withdrawal']}⭐*\n"
        f"📤 WD agent:       *{'@'+v['withdrawal_agent'] if v['withdrawal_agent'] else 'Not set'}*\n"
        f"📞 Support:        *{'@'+v['support_username'] if v['support_username'] else 'Not set'}*\n"
        f"📢 Channel:        *{'@'+v['channel_username'] if v['channel_username'] else 'Not set'}*\n"
        f"🔧 Maintenance:    *{'ON ⚠️' if v['maintenance_mode']=='1' else 'OFF ✅'}*"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 Bot Name",     callback_data="as_botname"),
         InlineKeyboardButton("💬 Welcome Msg",  callback_data="as_welcome")],
        [InlineKeyboardButton("📅 Daily Stars",  callback_data="as_daily"),
         InlineKeyboardButton("🔥 Streak Bonus", callback_data="as_streak")],
        [InlineKeyboardButton("👥 Ref Tasks",    callback_data="as_reftasks"),
         InlineKeyboardButton("🎁 Ref Bonus",    callback_data="as_refbonus")],
        [InlineKeyboardButton("📉 Min WD",       callback_data="as_minwd"),
         InlineKeyboardButton("📈 Max WD",       callback_data="as_maxwd")],
        [InlineKeyboardButton("📤 WD Agent",     callback_data="as_agent"),
         InlineKeyboardButton("📞 Support",      callback_data="as_support")],
        [InlineKeyboardButton("📢 Channel",      callback_data="as_channel"),
         InlineKeyboardButton("🔧 Maintenance",  callback_data="as_maint")],
    ])
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def admin_list_wds(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT w.*,u.full_name,u.username FROM withdrawals w "
            "JOIN users u ON w.user_id=u.user_id WHERE w.status='pending' ORDER BY w.requested_at LIMIT 10"
        ) as c:
            wds = [dict(r) for r in await c.fetchall()]

    if not wds:
        await update.message.reply_text("💎 No pending withdrawals.")
        return

    for wd in wds:
        name   = wd.get("full_name","?")
        uname  = f"  @{wd['username']}" if wd.get("username") else ""
        text = (
            f"💎 *Withdrawal Request*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 *{name}*{uname}\n"
            f"🆔  `{wd['user_id']}`\n"
            f"⭐  Stars: *{wd['stars']}*\n"
            f"🔑  Code: `{wd['unique_code']}`\n"
            f"📅  {wd['requested_at'][:16]}"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Approve", callback_data=f"wdok_{wd['unique_code']}"),
            InlineKeyboardButton("❌ Reject",  callback_data=f"wdno_{wd['unique_code']}")
        ]])
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def do_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    users  = await all_user_ids()
    sent, failed = 0, 0
    msg = await update.message.reply_text(f"📢 Sending to {len(users)} users...")
    for batch_start in range(0, len(users), 50):
        batch = users[batch_start:batch_start+50]
        for uid in batch:
            try:
                await ctx.bot.send_message(uid, text, parse_mode="Markdown")
                sent += 1
            except Exception:
                failed += 1
        await asyncio.sleep(1)
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT INTO announcements (message,sent_by,sent_count) VALUES (?,?,?)",
            (text, update.effective_user.id, sent)
        )
        await db.commit()
    await msg.edit_text(f"📢 *Broadcast Done!*\n\n✅ Sent: {sent}\n❌ Failed: {failed}", parse_mode="Markdown")
    st_clear(ctx)
    ctx.user_data["is_admin_mode"] = True


async def find_user_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE, query_str: str):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        q = query_str.lstrip("@")
        if q.lstrip("-").isdigit():
            async with db.execute("SELECT * FROM users WHERE user_id=?", (int(q),)) as c:
                row = await c.fetchone()
        else:
            async with db.execute("SELECT * FROM users WHERE username LIKE ?", (f"%{q}%",)) as c:
                row = await c.fetchone()

    if not row:
        await update.message.reply_text("❌ User not found.")
        return

    u    = dict(row)
    done = await completed_ids(u["user_id"])
    rank = await user_rank(u["user_id"])
    ban  = "🚫 Banned" if u.get("is_banned") else "✅ Active"

    text = (
        f"👤 *User Details*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔  `{u['user_id']}`\n"
        f"👤  *{u.get('full_name','?')}*"
        + (f"  @{u.get('username')}" if u.get("username") else "") + "\n"
        f"⭐  Stars: *{u.get('stars',0)}* | Earned: *{u.get('total_earned',0)}*\n"
        f"🏆  Rank: *#{rank}* | Tasks: *{len(done)}*\n"
        f"🔥  Streak: *{u.get('daily_streak',0)}* | Joined: {u.get('join_date','?')}\n"
        f"Status: {ban}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚫 Ban/Unban", callback_data=f"au_ban_{u['user_id']}"),
         InlineKeyboardButton("🔄 Reset",     callback_data=f"au_reset_{u['user_id']}")],
        [InlineKeyboardButton("💸 Give",      callback_data=f"au_give_{u['user_id']}"),
         InlineKeyboardButton("✂️ Take",      callback_data=f"au_take_{u['user_id']}")]
    ])
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def save_new_task(update: Update, ctx: ContextTypes.DEFAULT_TYPE, stars: int):
    name    = ctx.user_data.get("t_name", "Task")
    desc    = ctx.user_data.get("t_desc", "")
    t_type  = ctx.user_data.get("t_type", "link")
    link    = ctx.user_data.get("t_link", "")
    channel = ctx.user_data.get("t_channel", "")

    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT INTO tasks (name,description,task_type,link,channel_id,stars_reward) VALUES (?,?,?,?,?,?)",
            (name, desc, t_type, link, channel, stars)
        )
        await db.commit()

    st_clear(ctx)
    ctx.user_data["is_admin_mode"] = True
    await update.message.reply_text(
        f"✅ *Task Created!*\n\n"
        f"📝 Name: *{name}*\n"
        f"🏷  Type: {task_icon(t_type)} {t_type}\n"
        f"⭐  Reward: *{stars}⭐*\n\n"
        f"Task is now live!",
        parse_mode="Markdown"
    )


# ──────────────────────────────────────────────────────────────────────────────
#  ADMIN CALLBACK ROUTER
# ──────────────────────────────────────────────────────────────────────────────

async def admin_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = q.from_user.id
    data = q.data
    await q.answer()

    # Task type selection
    if data.startswith("aty_"):
        t_type = data[4:]
        ctx.user_data["t_type"] = t_type
        if t_type == "manual":
            ctx.user_data["t_link"]    = ""
            ctx.user_data["t_channel"] = ""
            st_set(ctx, "atask_stars")
            await q.edit_message_text("Step 5/5 — How many ⭐ stars reward?")
        else:
            st_set(ctx, "atask_link")
            if t_type == "channel":
                await q.edit_message_text("Step 4/5 — Enter *channel username* (e.g. @mychannel) or invite link:", parse_mode="Markdown")
            elif t_type == "youtube":
                await q.edit_message_text("Step 4/5 — Enter the *YouTube video URL*:", parse_mode="Markdown")
            else:
                await q.edit_message_text("Step 4/5 — Enter the *URL* for this task:", parse_mode="Markdown")
        return

    # Settings prompts
    setting_prompts = {
        "as_botname":  ("s_botname",  "🤖 Enter new bot name:"),
        "as_welcome":  ("s_welcome",  "💬 Enter new welcome message:"),
        "as_daily":    ("s_daily",    "📅 Enter daily stars amount:"),
        "as_streak":   ("s_streak",   "🔥 Enter streak bonus (stars/day):"),
        "as_reftasks": ("s_reftasks", "👥 Enter tasks required for referral:"),
        "as_refbonus": ("s_refbonus", "🎁 Enter referral bonus stars:"),
        "as_minwd":    ("s_minwd",    "📉 Enter minimum withdrawal (stars):"),
        "as_maxwd":    ("s_maxwd",    "📈 Enter maximum withdrawal (stars):"),
        "as_agent":    ("s_agent",    "📤 Enter withdrawal agent username (no @):"),
        "as_support":  ("s_support",  "📞 Enter support username (no @):"),
        "as_channel":  ("s_channel",  "📢 Enter channel username (no @):"),
    }
    if data == "as_maint":
        cur = await gset("maintenance_mode", "0")
        nv  = "0" if cur == "1" else "1"
        await sset("maintenance_mode", nv)
        s = "ON ⚠️" if nv == "1" else "OFF ✅"
        await q.edit_message_text(f"🔧 Maintenance mode: *{s}*", parse_mode="Markdown")
        return
    if data in setting_prompts:
        state_key, prompt = setting_prompts[data]
        st_set(ctx, state_key)
        await q.edit_message_text(prompt)
        return

    # User management from inline buttons
    if data.startswith("au_view_"):
        target = int(data.split("_")[2])
        u = await get_user(target)
        if not u:
            await q.edit_message_text("❌ User not found.")
            return
        done = await completed_ids(target)
        rank = await user_rank(target)
        ban  = "🚫 Banned" if u.get("is_banned") else "✅ Active"
        text = (
            f"👤 *User Details*\n━━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔  `{target}`\n"
            f"👤  *{u.get('full_name','?')}*"
            + (f"  @{u.get('username')}" if u.get("username") else "") + "\n"
            f"⭐  Stars: *{u.get('stars',0)}* | Earned: *{u.get('total_earned',0)}*\n"
            f"🏆  Rank: #{rank} | Tasks: {len(done)}\n"
            f"🔥  Streak: {u.get('daily_streak',0)} | Joined: {u.get('join_date','')}\n"
            f"Status: {ban}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚫 Ban/Unban", callback_data=f"au_ban_{target}"),
             InlineKeyboardButton("🔄 Reset",     callback_data=f"au_reset_{target}")],
            [InlineKeyboardButton("💸 Give",      callback_data=f"au_give_{target}"),
             InlineKeyboardButton("✂️ Take",      callback_data=f"au_take_{target}")]
        ])
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
        return

    if data.startswith("au_ban_"):
        target = int(data.split("_")[2])
        u = await get_user(target)
        if u:
            nb = 0 if u.get("is_banned") else 1
            async with aiosqlite.connect(DB) as db:
                await db.execute("UPDATE users SET is_banned=? WHERE user_id=?", (nb, target))
                await db.commit()
            act = "🚫 Banned" if nb else "✅ Unbanned"
            await q.answer(f"{act}!", show_alert=True)
            if nb:
                try: await ctx.bot.send_message(target, "🚫 You have been banned.")
                except Exception: pass
        return

    if data.startswith("au_reset_"):
        target = int(data.split("_")[2])
        async with aiosqlite.connect(DB) as db:
            await db.execute("DELETE FROM user_tasks WHERE user_id=?", (target,))
            await db.execute("UPDATE users SET stars=0,total_earned=0,tasks_completed=0,daily_streak=0 WHERE user_id=?", (target,))
            await db.commit()
        await q.answer("🔄 User reset!", show_alert=True)
        return

    if data.startswith("au_give_"):
        target = int(data.split("_")[2])
        ctx.user_data["t_uid"] = target
        st_set(ctx, "give_amt")
        await q.edit_message_text(f"💸 Enter stars to give to `{target}`:", parse_mode="Markdown")
        return

    if data.startswith("au_take_"):
        target = int(data.split("_")[2])
        ctx.user_data["t_uid"] = target
        st_set(ctx, "take_amt")
        await q.edit_message_text(f"✂️ Enter stars to remove from `{target}`:", parse_mode="Markdown")
        return

    if data.startswith("au_pg_"):
        page = int(data.split("_")[2])
        per  = 8
        async with aiosqlite.connect(DB) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT user_id,full_name,stars,is_banned FROM users ORDER BY stars DESC LIMIT ? OFFSET ?",
                (per, page * per)
            ) as c:
                users = [dict(r) for r in await c.fetchall()]
            async with db.execute("SELECT COUNT(*) FROM users") as c:
                total = (await c.fetchone())[0]
        pages = max(1, (total-1)//per+1)
        txt   = f"👥 *Users* — Page {page+1}/{pages}\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        btns  = []
        for u in users:
            ic = "🚫" if u["is_banned"] else "👤"
            txt += f"{ic} `{u['user_id']}` *{u['full_name'][:16]}* {u['stars']}⭐\n"
            btns.append([InlineKeyboardButton(
                f"{ic} {u['full_name'][:18]} ({u['stars']}⭐)",
                callback_data=f"au_view_{u['user_id']}"
            )])
        nav = []
        if page > 0:      nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"au_pg_{page-1}"))
        if page+1 < pages: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"au_pg_{page+1}"))
        if nav:
            btns.append(nav)
        await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(btns) if btns else None)
        return

    # Task management
    if data.startswith("at_view_"):
        tid  = int(data.split("_")[2])
        task = await get_task(tid)
        if not task:
            await q.edit_message_text("❌ Task not found.")
            return
        async with aiosqlite.connect(DB) as db:
            async with db.execute("SELECT COUNT(*) FROM user_tasks WHERE task_id=?", (tid,)) as c:
                comps = (await c.fetchone())[0]
        s   = "✅ Active" if task["is_active"] else "❌ Inactive"
        ic  = task_icon(task.get("task_type","link"))
        txt = (
            f"{ic} *Task #{tid}*\n━━━━━━━━━━━━━━━━━━━━━\n"
            f"📝  Name: *{task['name']}*\n"
            f"📄  Desc: {task.get('description','—')}\n"
            f"🏷   Type: {task.get('task_type','link')}\n"
            f"🔗  Link: {task.get('link','—')}\n"
            f"📢  Channel: {task.get('channel_id','—')}\n"
            f"⭐  Reward: *{task['stars_reward']}*\n"
            f"Status: {s}\n"
            f"👥  Completions: *{comps}*"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Toggle", callback_data=f"at_tog_{tid}"),
            InlineKeyboardButton("🗑 Delete", callback_data=f"at_del_{tid}")
        ]])
        await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb)
        return

    if data.startswith("at_tog_"):
        tid = int(data.split("_")[2])
        async with aiosqlite.connect(DB) as db:
            async with db.execute("SELECT is_active FROM tasks WHERE task_id=?", (tid,)) as c:
                row = await c.fetchone()
            if row:
                ns = 0 if row[0] else 1
                await db.execute("UPDATE tasks SET is_active=? WHERE task_id=?", (ns, tid))
                await db.commit()
                await q.answer(f"Task {'activated ✅' if ns else 'deactivated ❌'}", show_alert=True)
        return

    if data.startswith("at_del_"):
        tid = int(data.split("_")[2])
        async with aiosqlite.connect(DB) as db:
            await db.execute("DELETE FROM tasks WHERE task_id=?", (tid,))
            await db.execute("DELETE FROM user_tasks WHERE task_id=?", (tid,))
            await db.commit()
        await q.answer("🗑 Deleted!", show_alert=True)
        await q.edit_message_text("🗑 Task deleted.")
        return

    # Withdrawal approve/reject
    if data.startswith("wdok_") or data.startswith("wdno_"):
        action = "approve" if data.startswith("wdok_") else "reject"
        code   = data[5:]   # strips "wdok_" or "wdno_"
        async with aiosqlite.connect(DB) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM withdrawals WHERE unique_code=?", (code,)) as c:
                wd = await c.fetchone()
            if not wd:
                await q.answer("Not found.", show_alert=True)
                return
            wd  = dict(wd)
            now = datetime.now().isoformat()
            new_status = "approved" if action == "approve" else "rejected"
            await db.execute(
                "UPDATE withdrawals SET status=?,processed_at=? WHERE unique_code=?",
                (new_status, now, code)
            )
            if action == "reject":
                await db.execute("UPDATE users SET stars=stars+? WHERE user_id=?", (wd["stars"], wd["user_id"]))
                await db.execute(
                    "INSERT INTO transactions (user_id,amount,type,note) VALUES (?,?,?,?)",
                    (wd["user_id"], wd["stars"], "refund", f"Rejected WD {code}")
                )
            await db.commit()

        label = "approved ✅" if action == "approve" else "rejected ❌"
        await q.edit_message_text(f"💎 Withdrawal `{code}` — *{label}*", parse_mode="Markdown")

        try:
            if action == "approve":
                msg = (
                    f"✅ *Withdrawal Approved!*\n\n"
                    f"⭐ Stars: *{wd['stars']}*\n"
                    f"🔑 Code: `{code}`\n\n"
                    f"Your gift is being processed by the agent!"
                )
            else:
                msg = (
                    f"❌ *Withdrawal Rejected*\n\n"
                    f"⭐ *{wd['stars']} stars* returned to your balance.\n"
                    f"Contact support if you need help."
                )
            await ctx.bot.send_message(wd["user_id"], msg, parse_mode="Markdown")
        except Exception:
            pass
        return


# ──────────────────────────────────────────────────────────────────────────────
#  MAIN MESSAGE ROUTER
# ──────────────────────────────────────────────────────────────────────────────

async def router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    uid  = update.effective_user.id
    text = update.message.text

    # Admin mode: ALL messages go to admin handler (never user panel)
    if uid in ADMIN_IDS and (ctx.user_data.get("is_admin_mode") or st_get(ctx)):
        await handle_admin(update, ctx)
        return

    # Maintenance gate
    if await gset("maintenance_mode") == "1" and uid not in ADMIN_IDS:
        await update.message.reply_text("🔧 Bot is under maintenance. Try again later.")
        return

    user = await get_user(uid)
    if not user:
        await cmd_start(update, ctx)
        return
    if user.get("is_banned"):
        await update.message.reply_text("🚫 You are banned from this bot.")
        return

    # User menu routing
    menu = {
        "🎯 Missions":     screen_missions,
        "👤 Profile":      screen_profile,
        "📅 Daily Reward": screen_daily,
        "🏆 Leaderboard":  screen_leaderboard,
        "👥 Referral":     screen_referral,
        "💎 Withdraw":     screen_withdraw,
        "📊 My Stats":     screen_stats,
        "ℹ️ Help":         screen_help,
    }
    if text in menu:
        await menu[text](update, ctx)
    else:
        await update.message.reply_text(
            "Use the menu below to navigate 👇",
            reply_markup=user_kbd()
        )


# ──────────────────────────────────────────────────────────────────────────────
#  GLOBAL CALLBACK ROUTER
# ──────────────────────────────────────────────────────────────────────────────

async def cb_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = q.from_user.id
    data = q.data

    is_admin_cb = any(data.startswith(p) for p in (
        "au_", "at_", "aty_", "as_", "wdok_", "wdno_"
    ))

    if uid in ADMIN_IDS and is_admin_cb:
        await admin_callback(update, ctx)
    else:
        await user_callback(update, ctx)


# ──────────────────────────────────────────────────────────────────────────────
#  COMMANDS
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    is_adm = ctx.user_data.get("is_admin_mode")
    st_clear(ctx)
    if is_adm:
        ctx.user_data["is_admin_mode"] = True
    await update.message.reply_text(
        "❌ Cancelled.",
        reply_markup=admin_kbd() if is_adm else user_kbd()
    )


async def cmd_give(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    a = ctx.args
    if len(a) < 2 or not a[0].lstrip("-").isdigit() or not a[1].isdigit():
        await update.message.reply_text("Usage: /give <user_id> <stars>"); return
    t, amt = int(a[0]), int(a[1])
    await add_stars(t, amt, "admin_gift", "Given by admin")
    await update.message.reply_text(f"✅ Gave *{amt}⭐* to `{t}`", parse_mode="Markdown")
    try: await ctx.bot.send_message(t, f"🎁 An admin gave you *{amt}⭐*!", parse_mode="Markdown")
    except Exception: pass


async def cmd_take(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    a = ctx.args
    if len(a) < 2 or not a[0].lstrip("-").isdigit() or not a[1].isdigit():
        await update.message.reply_text("Usage: /take <user_id> <stars>"); return
    t, amt = int(a[0]), int(a[1])
    await sub_stars(t, amt, "admin_deduct", "Deducted by admin")
    await update.message.reply_text(f"✅ Removed *{amt}⭐* from `{t}`", parse_mode="Markdown")


async def cmd_setstars(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    a = ctx.args
    if len(a) < 2 or not a[0].lstrip("-").isdigit() or not a[1].isdigit():
        await update.message.reply_text("Usage: /setstars <user_id> <amount>"); return
    await set_stars(int(a[0]), int(a[1]))
    await update.message.reply_text(f"✅ Set `{a[0]}` to *{a[1]}⭐*", parse_mode="Markdown")


async def cmd_ban(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    a = ctx.args
    if not a or not a[0].lstrip("-").isdigit():
        await update.message.reply_text("Usage: /ban <user_id>"); return
    t = int(a[0])
    u = await get_user(t)
    if u:
        nb = 0 if u.get("is_banned") else 1
        async with aiosqlite.connect(DB) as db:
            await db.execute("UPDATE users SET is_banned=? WHERE user_id=?", (nb, t))
            await db.commit()
        await update.message.reply_text(f"User {t} {'banned 🚫' if nb else 'unbanned ✅'}")


async def cmd_stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    s = await get_stats()
    await update.message.reply_text(
        f"Users:{s['total_users']} Active:{s['active_users']} Stars:{s['total_stars']} Tasks:{s['completions']}"
    )


async def error_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {ctx.error}", exc_info=ctx.error)


# ──────────────────────────────────────────────────────────────────────────────
#  STARTUP
# ──────────────────────────────────────────────────────────────────────────────

async def post_init(app: Application):
    await init_db()
    await app.bot.set_my_commands([
        BotCommand("start",    "Start / main menu"),
        BotCommand("admin",    "Open admin panel"),
        BotCommand("cancel",   "Cancel current action"),
        BotCommand("give",     "Give stars  (admin)"),
        BotCommand("take",     "Take stars  (admin)"),
        BotCommand("setstars", "Set stars   (admin)"),
        BotCommand("ban",      "Ban/unban   (admin)"),
        BotCommand("stats",    "Quick stats (admin)"),
    ])
    logger.info("🚀 Bot ready!")


def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN not set!")
    if not ADMIN_IDS:
        logger.warning("No ADMIN_IDS set!")

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("admin",    cmd_admin))
    app.add_handler(CommandHandler("cancel",   cmd_cancel))
    app.add_handler(CommandHandler("give",     cmd_give))
    app.add_handler(CommandHandler("take",     cmd_take))
    app.add_handler(CommandHandler("setstars", cmd_setstars))
    app.add_handler(CommandHandler("ban",      cmd_ban))
    app.add_handler(CommandHandler("stats",    cmd_stats_cmd))
    app.add_handler(CallbackQueryHandler(cb_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, router))
    app.add_error_handler(error_handler)

    app.job_queue.run_repeating(
        periodic_channel_check,
        interval=1800,
        first=120,
        name="ch_check"
    )

    logger.info("Polling started...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
