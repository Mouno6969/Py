# Py
import logging
import sqlite3
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ChatMemberHandler,
    ContextTypes, ConversationHandler, MessageHandler, filters,
    CallbackQueryHandler
)

# --- CONFIG - fill these in ----------------------------------------------------
BOT_TOKEN           = "8620572891:AAF9pvdoECUiT0ZpKCWTSTajy4sPKA_mmJ0"   # From @BotFather
CHANNEL_ID          = -1003823753781         # Channel numeric ID (negative)
GROUP_ID            = -1003929907374          # Group numeric ID (negative)
ADMIN_ID            = 8218984587               # Your personal Telegram user ID
DB_PATH             = "referrals.db"

REWARD_PER_REFERRAL    = 0.01                 # $ per successful referral
MILESTONE_EVERY        = 10                   # Bonus every N referrals
MILESTONE_BONUS        = 0.05                 # $ bonus at each milestone
MIN_WITHDRAWAL         = 0.01                 # Minimum withdrawal in $
# -------------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

WAITING_WALLET  = 1
WAITING_SUPPORT = 2


# --- DATABASE ------------------------------------------------------------------

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id          INTEGER PRIMARY KEY,
                username         TEXT,
                first_name       TEXT,
                invite_link      TEXT NOT NULL,
                balance          REAL DEFAULT 0.0,
                total_earned     REAL DEFAULT 0.0,
                total_withdrawn  REAL DEFAULT 0.0,
                wallet           TEXT,
                joined_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS referral_joins (
                referred_id      INTEGER PRIMARY KEY,
                referred_name    TEXT,
                referred_username TEXT,
                referrer_id      INTEGER NOT NULL,
                joined_channel   INTEGER DEFAULT 0,
                joined_group     INTEGER DEFAULT 0,
                verified_in_group INTEGER DEFAULT 0,
                completed        INTEGER DEFAULT 0,
                completed_at     TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS withdrawals (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id       INTEGER NOT NULL,
                amount        REAL NOT NULL,
                wallet        TEXT NOT NULL,
                status        TEXT DEFAULT 'pending',
                requested_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved_at   TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        # Tracks which milestones have already been rewarded per user
        conn.execute("""
            CREATE TABLE IF NOT EXISTS milestones (
                user_id    INTEGER NOT NULL,
                milestone  INTEGER NOT NULL,
                awarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, milestone)
            )
        """)
        conn.commit()

        # --- Migrations: add new columns if upgrading an existing DB ---
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(referral_joins)").fetchall()}
        if "verified_in_group" not in existing_cols:
            conn.execute("ALTER TABLE referral_joins ADD COLUMN verified_in_group INTEGER DEFAULT 0")
        if "referred_username" not in existing_cols:
            conn.execute("ALTER TABLE referral_joins ADD COLUMN referred_username TEXT")
        conn.commit()


# --- USER QUERIES --------------------------------------------------------------

def get_user(user_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """SELECT user_id, username, first_name, invite_link,
                      balance, total_earned, total_withdrawn, wallet
               FROM users WHERE user_id = ?""",
            (user_id,)
        ).fetchone()
    if row:
        return {
            "user_id": row[0], "username": row[1], "first_name": row[2],
            "invite_link": row[3], "balance": row[4],
            "total_earned": row[5], "total_withdrawn": row[6], "wallet": row[7]
        }
    return None


def save_user(user_id: int, username, first_name: str, invite_link: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO users (user_id, username, first_name, invite_link)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username    = excluded.username,
                first_name  = excluded.first_name,
                invite_link = excluded.invite_link
        """, (user_id, username or "", first_name, invite_link))
        conn.commit()


def save_wallet(user_id: int, wallet: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE users SET wallet = ? WHERE user_id = ?", (wallet, user_id))
        conn.commit()


def credit_balance(user_id: int, amount: float):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            UPDATE users SET
                balance      = balance + ?,
                total_earned = total_earned + ?
            WHERE user_id = ?
        """, (amount, amount, user_id))
        conn.commit()


def deduct_balance(user_id: int, amount: float):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            UPDATE users SET
                balance         = balance - ?,
                total_withdrawn = total_withdrawn + ?
            WHERE user_id = ?
        """, (amount, amount, user_id))
        conn.commit()


def get_referrer_by_link(invite_link: str):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT user_id FROM users WHERE invite_link = ?", (invite_link,)
        ).fetchone()
    return row[0] if row else None


def upsert_referral_join(referred_id: int, referred_name: str, referrer_id: int, referred_username: str = None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT OR IGNORE INTO referral_joins (referred_id, referred_name, referred_username, referrer_id)
            VALUES (?, ?, ?, ?)
        """, (referred_id, referred_name, referred_username, referrer_id))
        conn.commit()


def mark_joined_channel(referred_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE referral_joins SET joined_channel = 1 WHERE referred_id = ?",
            (referred_id,)
        )
        conn.commit()


def mark_joined_group(referred_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE referral_joins SET joined_group = 1 WHERE referred_id = ?",
            (referred_id,)
        )
        conn.commit()


def mark_verified_in_group(referred_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE referral_joins SET verified_in_group = 1 WHERE referred_id = ?",
            (referred_id,)
        )
        conn.commit()


def get_withdrawal_history(user_id: int, limit: int = 10):
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """SELECT amount, wallet, status, requested_at, resolved_at
               FROM withdrawals WHERE user_id = ?
               ORDER BY requested_at DESC LIMIT ?""",
            (user_id, limit)
        ).fetchall()
    return rows


def check_and_complete_referral(referred_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("""
            SELECT referrer_id, joined_channel, joined_group, verified_in_group, completed
            FROM referral_joins WHERE referred_id = ?
        """, (referred_id,)).fetchone()
        if not row:
            return None
        referrer_id, joined_channel, joined_group, verified_in_group, completed = row
        if completed:
            return None
        # All three steps required: joined channel, joined group, AND verified
        if joined_channel and joined_group and verified_in_group:
            conn.execute("""
                UPDATE referral_joins
                SET completed = 1, completed_at = CURRENT_TIMESTAMP
                WHERE referred_id = ?
            """, (referred_id,))
            conn.commit()
            return referrer_id
    return None


def get_referral_count(user_id: int) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM referral_joins WHERE referrer_id = ? AND completed = 1",
            (user_id,)
        ).fetchone()
    return row[0] if row else 0


# --- MILESTONE LOGIC -----------------------------------------------------------

def check_milestone(user_id: int, referral_count: int) -> int | None:
    """
    Check if referral_count has crossed a new milestone that hasn't been awarded yet.
    Returns the milestone number (e.g. 10, 20, 30...) if a new one was just hit, else None.
    Marks it as awarded atomically so it only fires once.
    """
    if referral_count == 0 or referral_count % MILESTONE_EVERY != 0:
        return None

    milestone = referral_count  # e.g. 10, 20, 30

    with sqlite3.connect(DB_PATH) as conn:
        # Try to insert - will fail silently if already awarded (PRIMARY KEY constraint)
        try:
            conn.execute(
                "INSERT INTO milestones (user_id, milestone) VALUES (?, ?)",
                (user_id, milestone)
            )
            conn.commit()
            return milestone
        except sqlite3.IntegrityError:
            return None  # Already awarded this milestone


def get_milestones_hit(user_id: int) -> list:
    """Return all milestones a user has been awarded."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT milestone FROM milestones WHERE user_id = ? ORDER BY milestone ASC",
            (user_id,)
        ).fetchall()
    return [r[0] for r in rows]


# --- LEADERBOARD QUERIES -------------------------------------------------------

def get_leaderboard_by_referrals():
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("""
            SELECT u.first_name, u.username,
                   COUNT(rj.referred_id) AS total_refs,
                   u.total_earned
            FROM referral_joins rj
            JOIN users u ON u.user_id = rj.referrer_id
            WHERE rj.completed = 1
            GROUP BY rj.referrer_id
            ORDER BY total_refs DESC
            LIMIT 10
        """).fetchall()


def get_leaderboard_by_earnings():
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("""
            SELECT u.first_name, u.username,
                   u.total_earned,
                   COUNT(rj.referred_id) AS total_refs
            FROM users u
            LEFT JOIN referral_joins rj
                ON rj.referrer_id = u.user_id AND rj.completed = 1
            WHERE u.total_earned > 0
            GROUP BY u.user_id
            ORDER BY u.total_earned DESC
            LIMIT 10
        """).fetchall()


def get_earner_rank(user_id: int) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("""
            SELECT COUNT(*) + 1 FROM users
            WHERE total_earned > (SELECT total_earned FROM users WHERE user_id = ?)
        """, (user_id,)).fetchone()
    return row[0] if row else 0


# --- WITHDRAWAL QUERIES --------------------------------------------------------

def create_withdrawal(user_id: int, amount: float, wallet: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT INTO withdrawals (user_id, amount, wallet) VALUES (?, ?, ?)",
            (user_id, amount, wallet)
        )
        conn.commit()
        return cur.lastrowid


def get_withdrawal(withdrawal_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT id, user_id, amount, wallet, status FROM withdrawals WHERE id = ?",
            (withdrawal_id,)
        ).fetchone()
    if row:
        return {"id": row[0], "user_id": row[1], "amount": row[2], "wallet": row[3], "status": row[4]}
    return None


def resolve_withdrawal(withdrawal_id: int, status: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            UPDATE withdrawals SET status = ?, resolved_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (status, withdrawal_id))
        conn.commit()


def get_pending_withdrawals():
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("""
            SELECT w.id, w.user_id, w.amount, w.wallet,
                   u.first_name, u.username, w.requested_at
            FROM withdrawals w
            JOIN users u ON u.user_id = w.user_id
            WHERE w.status = 'pending'
            ORDER BY w.requested_at ASC
        """).fetchall()


def has_pending_withdrawal(user_id: int) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM withdrawals WHERE user_id = ? AND status = 'pending'",
            (user_id,)
        ).fetchone()
    return row[0] > 0


# --- HELPERS -------------------------------------------------------------------

def display_name(first_name: str, username) -> str:
    return f"{first_name} (@{username})" if username else first_name


def fmt(amount: float) -> str:
    return f"${amount:.4f}"


def rank_badge(i: int) -> str:
    return ["ðŸ¥‡", "ðŸ¥ˆ", "ðŸ¥‰"][i] if i < 3 else f"{i + 1}."


def next_milestone(count: int) -> int:
    """Return the next milestone referral count from current count."""
    return ((count // MILESTONE_EVERY) + 1) * MILESTONE_EVERY


# --- COMMAND HANDLERS ----------------------------------------------------------

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    existing = get_user(user.id)

    if existing:
        invite_link = existing["invite_link"]
        intro = "ðŸ‘‹ Welcome back!"
    else:
        try:
            link_obj = await context.bot.create_chat_invite_link(
                chat_id=CHANNEL_ID,
                name=f"ref_{user.id}",
                expire_date=None,
                member_limit=None,
            )
            invite_link = link_obj.invite_link
        except Exception as e:
            logger.error(f"Could not create invite link for {user.id}: {e}")
            await update.message.reply_text(
                "âŒ Failed to generate your referral link. "
                "Make sure the bot is an admin of the channel and try again."
            )
            return
        save_user(user.id, user.username, user.first_name, invite_link)
        intro = "ðŸŽ‰ Welcome! Your referral account has been created."

    await update.message.reply_text(
        f"{intro}\n\n"
        f"ðŸ“¢ *Your referral link:*\n`{invite_link}`\n\n"
        f"ðŸ’° *Earnings:*\n"
        f"- {fmt(REWARD_PER_REFERRAL)} per successful referral\n"
        f"- {fmt(MILESTONE_BONUS)} bonus every {MILESTONE_EVERY} referrals ðŸŽ¯\n\n"
        f"ðŸ“‹ *How it works:*\n"
        f"1ï¸âƒ£ Share your link with friends\n"
        f"2ï¸âƒ£ They join the channel via your link\n"
        f"3ï¸âƒ£ They also join the group\n"
        f"âœ… Both steps = referral counted + rewards credited!\n\n"
        f"ðŸ“Œ Commands:\n"
        f"/balance - earnings & wallet\n"
        f"/stats - your referral stats\n"
        f"/topearners - top earners leaderboard\n"
        f"/withdraw - request a payout\n"
        f"/history - withdrawal history\n"
        f"/setwallet - save your Binance wallet\n"
        f"/support - contact admin for help\n"
        f"/help - show all commands",
        parse_mode="Markdown"
    )


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    if not user:
        await update.message.reply_text("Send /start first to create your account.")
        return

    count = get_referral_count(user["user_id"])
    next_ms = next_milestone(count)
    refs_needed = next_ms - count
    milestones_hit = get_milestones_hit(user["user_id"])
    milestone_total = len(milestones_hit) * MILESTONE_BONUS

    await update.message.reply_text(
        f"ðŸ’° *Your Earnings*\n\n"
        f"âœ… Successful referrals: *{count}*\n"
        f"ðŸ’µ Current balance: *{fmt(user['balance'])}*\n"
        f"ðŸ“ˆ Total earned: *{fmt(user['total_earned'])}*\n"
        f"ðŸŽ¯ Milestone bonuses earned: *{fmt(milestone_total)}* "
        f"({len(milestones_hit)} bonus{'es' if len(milestones_hit) != 1 else ''})\n"
        f"ðŸ“¤ Total withdrawn: *{fmt(user['total_withdrawn'])}*\n\n"
        f"ðŸ’³ Wallet: `{user['wallet'] or 'Not set - use /setwallet'}`\n\n"
        f"ðŸŽ¯ *Next milestone:* {next_ms} referrals "
        f"({refs_needed} more to go -> +{fmt(MILESTONE_BONUS)} bonus!)\n"
        f"Minimum withdrawal: *{fmt(MIN_WITHDRAWAL)}*",
        parse_mode="Markdown"
    )


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not get_user(user.id):
        await update.message.reply_text("Send /start first to create your account.")
        return

    count = get_referral_count(user.id)
    next_ms = next_milestone(count)
    leaderboard = get_leaderboard_by_referrals()

    lb_lines = []
    for i, (fname, uname, total_refs, total_earned) in enumerate(leaderboard):
        name = display_name(fname, uname)
        lb_lines.append(
            f"{rank_badge(i)} {name} - {total_refs} referral{'s' if total_refs != 1 else ''} ({fmt(total_earned)})"
        )

    lb_text = "\n".join(lb_lines) if lb_lines else "No referrals yet. Be the first! ðŸš€"

    await update.message.reply_text(
        f"ðŸ“Š *Your Referral Stats*\n\n"
        f"âœ… Successful referrals: *{count}*\n"
        f"ðŸ’µ Earned from referrals: *{fmt(count * REWARD_PER_REFERRAL)}*\n"
        f"ðŸŽ¯ Next milestone bonus at: *{next_ms} referrals* "
        f"({next_ms - count} to go!)\n\n"
        f"ðŸ† *Top 10 - Most Referrals*\n{lb_text}\n\n"
        f"Use /topearners to see who earned the most ðŸ’¸",
        parse_mode="Markdown"
    )


async def topearners_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not get_user(update.effective_user.id):
        await update.message.reply_text("Send /start first to create your account.")
        return

    earners = get_leaderboard_by_earnings()
    if not earners:
        await update.message.reply_text(
            "ðŸ’¸ No earnings recorded yet. Start referring to be the first on the board! ðŸš€"
        )
        return

    lines = []
    for i, (fname, uname, total_earned, total_refs) in enumerate(earners):
        name = display_name(fname, uname)
        lines.append(
            f"{rank_badge(i)} *{name}*\n"
            f"   ðŸ’µ {fmt(total_earned)} earned | âœ… {total_refs} referral{'s' if total_refs != 1 else ''}"
        )

    user_id = update.effective_user.id
    user_data = get_user(user_id)
    rank = get_earner_rank(user_id)

    await update.message.reply_text(
        f"ðŸ’¸ *Top Earners Leaderboard*\n\n"
        f"{chr(10).join(lines)}\n\n"
        f"-----------------\n"
        f"ðŸŽ¯ *Your rank:* #{rank}\n"
        f"ðŸ’µ *Your total earned:* {fmt(user_data['total_earned'] if user_data else 0.0)}\n\n"
        f"Keep referring to climb the board! ðŸš€",
        parse_mode="Markdown"
    )


# --- WALLET SETUP --------------------------------------------------------------

async def setwallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not get_user(update.effective_user.id):
        await update.message.reply_text("Send /start first to create your account.")
        return ConversationHandler.END

    await update.message.reply_text(
        "ðŸ’³ Please send your *Binance wallet address* (USDT TRC-20 or BEP-20):\n\n"
        "Type /cancel to abort.",
        parse_mode="Markdown"
    )
    return WAITING_WALLET


async def receive_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wallet = update.message.text.strip()
    if len(wallet) < 10:
        await update.message.reply_text(
            "âŒ That doesn't look like a valid wallet address. Please try again or type /cancel."
        )
        return WAITING_WALLET

    save_wallet(update.effective_user.id, wallet)
    await update.message.reply_text(
        f"âœ… Wallet saved!\n\n`{wallet}`\n\nUse /withdraw to request a payout.",
        parse_mode="Markdown"
    )
    return ConversationHandler.END


async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("âŒ Cancelled.")
    return ConversationHandler.END


# --- WITHDRAWAL ----------------------------------------------------------------

async def withdraw_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    if not user:
        await update.message.reply_text("Send /start first to create your account.")
        return

    if not user["wallet"]:
        await update.message.reply_text(
            "âŒ No wallet address saved.\nUse /setwallet to add your Binance address first."
        )
        return

    if user["balance"] < MIN_WITHDRAWAL:
        count = get_referral_count(user["user_id"])
        next_ms = next_milestone(count)
        await update.message.reply_text(
            f"âŒ Your balance is *{fmt(user['balance'])}*.\n\n"
            f"Minimum withdrawal is *{fmt(MIN_WITHDRAWAL)}*.\n"
            f"ðŸŽ¯ Next milestone bonus at *{next_ms} referrals* "
            f"({next_ms - count} more to go -> +{fmt(MILESTONE_BONUS)})! ðŸ’ª",
            parse_mode="Markdown"
        )
        return

    if has_pending_withdrawal(user["user_id"]):
        await update.message.reply_text(
            "â³ You already have a pending withdrawal.\n"
            "Please wait for it to be processed before submitting another."
        )
        return

    amount = user["balance"]
    withdrawal_id = create_withdrawal(user["user_id"], amount, user["wallet"])
    deduct_balance(user["user_id"], amount)

    await update.message.reply_text(
        f"âœ… *Withdrawal request submitted!*\n\n"
        f"ðŸ’µ Amount: *{fmt(amount)}*\n"
        f"ðŸ’³ Wallet: `{user['wallet']}`\n"
        f"ðŸ”– Request ID: `#{withdrawal_id}`\n\n"
        f"â³ Pending admin approval. You'll be notified once processed.",
        parse_mode="Markdown"
    )

    user_display = display_name(user["first_name"], user["username"])
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("âœ… Approve", callback_data=f"approve_{withdrawal_id}"),
            InlineKeyboardButton("âŒ Reject",  callback_data=f"reject_{withdrawal_id}")
        ]
    ])
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=(
            f"ðŸ’¸ *New Withdrawal Request*\n\n"
            f"ðŸ”– Request ID: `#{withdrawal_id}`\n"
            f"ðŸ‘¤ User: {user_display} (`{user['user_id']}`)\n"
            f"ðŸ’µ Amount: *{fmt(amount)}*\n"
            f"ðŸ’³ Wallet: `{user['wallet']}`"
        ),
        parse_mode="Markdown",
        reply_markup=keyboard
    )


# --- ADMIN APPROVAL ------------------------------------------------------------

async def handle_admin_decision(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.from_user.id != ADMIN_ID:
        await query.answer("â›” Not authorized.", show_alert=True)
        return

    action, withdrawal_id = query.data.split("_", 1)
    withdrawal_id = int(withdrawal_id)
    withdrawal = get_withdrawal(withdrawal_id)

    if not withdrawal:
        await query.edit_message_text("âŒ Withdrawal not found.")
        return

    if withdrawal["status"] != "pending":
        await query.edit_message_text(
            f"â„¹ï¸ Request `#{withdrawal_id}` was already *{withdrawal['status']}*.",
            parse_mode="Markdown"
        )
        return

    user = get_user(withdrawal["user_id"])
    user_display = display_name(user["first_name"], user["username"]) if user else f"User {withdrawal['user_id']}"

    if action == "approve":
        resolve_withdrawal(withdrawal_id, "approved")
        try:
            await context.bot.send_message(
                chat_id=withdrawal["user_id"],
                text=(
                    f"ðŸŽ‰ *Withdrawal Approved!*\n\n"
                    f"ðŸ’µ Amount: *{fmt(withdrawal['amount'])}*\n"
                    f"ðŸ’³ Sent to: `{withdrawal['wallet']}`\n"
                    f"ðŸ”– Request ID: `#{withdrawal_id}`\n\n"
                    f"Thank you! Keep referring to earn more. ðŸš€"
                ),
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"Could not notify user: {e}")

        await query.edit_message_text(
            f"âœ… *Approved* `#{withdrawal_id}` - {user_display} | {fmt(withdrawal['amount'])}",
            parse_mode="Markdown"
        )

    elif action == "reject":
        resolve_withdrawal(withdrawal_id, "rejected")
        credit_balance(withdrawal["user_id"], withdrawal["amount"])
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE users SET total_withdrawn = total_withdrawn - ? WHERE user_id = ?",
                (withdrawal["amount"], withdrawal["user_id"])
            )
            conn.commit()

        try:
            await context.bot.send_message(
                chat_id=withdrawal["user_id"],
                text=(
                    f"âŒ *Withdrawal Rejected*\n\n"
                    f"Your request of *{fmt(withdrawal['amount'])}* (ID `#{withdrawal_id}`) was rejected.\n\n"
                    f"ðŸ’µ Your balance has been *refunded*.\n"
                    f"Please update your wallet with /setwallet and try again."
                ),
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"Could not notify user: {e}")

        await query.edit_message_text(
            f"âŒ *Rejected* `#{withdrawal_id}` - {user_display} | {fmt(withdrawal['amount'])} refunded.",
            parse_mode="Markdown"
        )


async def pending_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    rows = get_pending_withdrawals()
    if not rows:
        await update.message.reply_text("âœ… No pending withdrawal requests.")
        return

    lines = []
    for row in rows:
        wid, uid, amount, wallet, fname, uname, requested_at = row
        name = display_name(fname, uname)
        lines.append(
            f"ðŸ”– `#{wid}` | {name} | *{fmt(amount)}*\n"
            f"   ðŸ’³ `{wallet}`\n"
            f"   ðŸ“… {requested_at[:16]}"
        )

    await update.message.reply_text(
        f"â³ *Pending Withdrawals ({len(rows)})*\n\n" + "\n\n".join(lines),
        parse_mode="Markdown"
    )


# --- CHAT MEMBER TRACKING ------------------------------------------------------

async def track_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = update.chat_member
    if not result:
        return

    chat_id    = result.chat.id
    new_member = result.new_chat_member
    old_member = result.old_chat_member
    joined_user    = new_member.user
    joined_user_id = joined_user.id
    joined_name    = joined_user.first_name
    joined_username = joined_user.username  # may be None

    if chat_id == CHANNEL_ID:
        # User must transition from outside (left/kicked) to active
        became_active = new_member.status in ("member", "administrator", "creator")
        was_outside   = old_member.status in ("left", "kicked")
        if not (became_active and was_outside):
            return

        invite_link_obj = result.invite_link
        if not invite_link_obj:
            return
        link_url = invite_link_obj.invite_link
        referrer_id = get_referrer_by_link(link_url)
        if not referrer_id or referrer_id == joined_user_id:
            return

        upsert_referral_join(joined_user_id, joined_name, referrer_id, joined_username)
        mark_joined_channel(joined_user_id)
        logger.info(f"[CHANNEL] {joined_user_id} (@{joined_username}) joined via ref of {referrer_id}")

    elif chat_id == GROUP_ID:
        # Ignore if user is leaving or being banned
        if new_member.status in ("left", "kicked"):
            return

        # Only record that the referred user joined the group.
        # Referral completion is handled exclusively by track_first_message_in_group.
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT referrer_id, joined_group FROM referral_joins WHERE referred_id = ?",
                (joined_user_id,)
            ).fetchone()
        if not row:
            return  # Not a referred user

        referrer_id, already_joined_group = row

        # Update username in referral_joins if we have it now
        if joined_username:
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    "UPDATE referral_joins SET referred_username = ? WHERE referred_id = ?",
                    (joined_username, joined_user_id)
                )
                conn.commit()

        if not already_joined_group:
            # Do NOT call mark_joined_group here.
            # joined_group is only marked when the user actually sends a message (track_first_message_in_group).
            # This ensures the referral is never completed by a join alone.
            logger.info(f"[GROUP] {joined_user_id} (@{joined_username}) joined (referred by {referrer_id}) â€” waiting for first message before marking joined_group")


async def track_first_message_in_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    When a referred user PERSONALLY sends a message in the group, mark them as
    joined + verified and complete the referral.
    Only fires once per user (ignored after referral is already completed).

    Guards:
    - Ignores messages sent by bots (e.g. welcome bots that mention the new user)
    - Ignores Telegram service messages (new_chat_members, left_chat_member, etc.)
    - Only counts a message where from_user is the referred user themselves
    """
    message = update.effective_message
    if not message or not message.from_user:
        return

    # Skip any message sent by a bot (welcome bots, captcha bots, etc.)
    if message.from_user.is_bot:
        return

    # Skip Telegram service/system messages (join/leave notifications)
    if (message.new_chat_members or message.left_chat_member or
            message.new_chat_title or message.new_chat_photo or
            message.delete_chat_photo or message.group_chat_created or
            message.supergroup_chat_created):
        return

    sender_id = message.from_user.id

    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT referrer_id, referred_name, joined_channel, completed FROM referral_joins WHERE referred_id = ?",
            (sender_id,)
        ).fetchone()

    if not row:
        return  # Not a referred user

    referrer_id, referred_name, joined_channel, completed = row

    if completed:
        return  # Already counted, nothing to do

    if not joined_channel:
        logger.info(f"[FIRST-MSG] {sender_id} messaged in group but hasn't joined channel yet â€” skipping")
        return

    # Mark group join + verified in one go
    mark_joined_group(sender_id)
    mark_verified_in_group(sender_id)
    logger.info(f"[FIRST-MSG] {sender_id} sent first message in group â€” completing referral")

    completed_referrer = check_and_complete_referral(sender_id)
    if completed_referrer:
        await _handle_completed_referral(context, completed_referrer, sender_id, referred_name or str(sender_id))


async def _handle_completed_referral(
    context: ContextTypes.DEFAULT_TYPE,
    referrer_id: int,
    referred_id: int,
    referred_name: str
):
    # Credit base reward
    credit_balance(referrer_id, REWARD_PER_REFERRAL)
    count = get_referral_count(referrer_id)
    referrer = get_user(referrer_id)
    referrer_display = display_name(referrer["first_name"], referrer["username"]) if referrer else f"User {referrer_id}"
    new_balance = referrer["balance"] + REWARD_PER_REFERRAL if referrer else REWARD_PER_REFERRAL
    rank = get_earner_rank(referrer_id)
    next_ms = next_milestone(count)

    # Check and award milestone bonus
    milestone_hit = check_milestone(referrer_id, count)
    bonus_line = ""
    admin_bonus_line = ""
    if milestone_hit:
        credit_balance(referrer_id, MILESTONE_BONUS)
        new_balance += MILESTONE_BONUS
        bonus_line = (
            f"\n\nðŸŽ¯ *MILESTONE BONUS!* You reached *{milestone_hit} referrals!*\n"
            f"ðŸŽ Extra *{fmt(MILESTONE_BONUS)}* has been added to your balance!"
        )
        admin_bonus_line = f"\nðŸŽ¯ Milestone hit: *{milestone_hit} referrals* -> +{fmt(MILESTONE_BONUS)} bonus awarded!"

    # Notify referrer
    try:
        await context.bot.send_message(
            chat_id=referrer_id,
            text=(
                f"ðŸŽ‰ *New successful referral!*\n\n"
                f"*{referred_name}* joined the channel, joined the group, and passed verification.\n\n"
                f"ðŸ’° Base reward: *{fmt(REWARD_PER_REFERRAL)}*\n"
                f"ðŸ’µ New balance: *{fmt(new_balance)}*\n"
                f"âœ… Total referrals: *{count}*\n"
                f"ðŸ† Earner rank: *#{rank}*\n"
                f"ðŸŽ¯ Next milestone: *{next_ms} referrals* "
                f"({next_ms - count} to go -> +{fmt(MILESTONE_BONUS)}!)"
                f"{bonus_line}"
            ),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"Could not notify referrer {referrer_id}: {e}")

    # Notify referred user
    try:
        await context.bot.send_message(
            chat_id=referred_id,
            text=(
                f"ðŸ‘‹ Welcome, *{referred_name}*!\n\n"
                f"You've successfully joined both the channel and the group.\n"
                f"Start the bot to get your own referral link and start earning!"
            ),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"Could not notify referred user {referred_id}: {e}")

    # Notify admin
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"âœ… *Referral completed!*\n\n"
                f"ðŸ‘¤ Referrer: {referrer_display}\n"
                f"âž• New member: {referred_name} (`{referred_id}`)\n"
                f"ðŸ’° Credited: *{fmt(REWARD_PER_REFERRAL)}*\n"
                f"ðŸ“Š Total referrals: *{count}* | Earner rank: #{rank}"
                f"{admin_bonus_line}"
            ),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"Could not notify admin: {e}")


# --- HISTORY -------------------------------------------------------------------

async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not get_user(user.id):
        await update.message.reply_text("Send /start first to create your account.")
        return

    rows = get_withdrawal_history(user.id, limit=10)

    if not rows:
        await update.message.reply_text(
            "ðŸ“­ *No Withdrawal History*\n\n"
            "You haven't made any withdrawal requests yet.\n"
            "Use /withdraw to request a payout.",
            parse_mode="Markdown"
        )
        return

    lines = []
    for i, (amount, wallet, status, requested_at, resolved_at) in enumerate(rows, 1):
        date = requested_at[:10] if requested_at else "?"
        if status == "approved":
            status_icon = "âœ…"
        elif status == "rejected":
            status_icon = "âŒ"
        else:
            status_icon = "â³"
        short_wallet = wallet[:6] + "..." + wallet[-4:] if wallet and len(wallet) > 12 else (wallet or "?")
        lines.append(
            f"{i}. {status_icon} *{fmt(amount)}* | {date} | `{short_wallet}`"
        )

    await update.message.reply_text(
        f"ðŸ“‹ *Your Withdrawal History* (last {len(rows)})\n\n" + "\n".join(lines),
        parse_mode="Markdown"
    )


# --- HELP ----------------------------------------------------------------------

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    is_admin = user.id == ADMIN_ID
    text = (
        "ðŸ“– *Available Commands*\n\n"
        "ðŸ‘¤ *Account*\n"
        "/start - Home menu & your referral link\n"
        "/balance - Check your earnings & wallet\n"
        "/stats - Your referral stats & leaderboard rank\n"
        "/history - Your withdrawal history\n"
        "/setwallet - Save your Binance wallet address\n\n"
        "ðŸ† *Referrals*\n"
        "/topearners - Top 10 referrers leaderboard\n\n"
        "ðŸ’¸ *Withdrawals*\n"
        "/withdraw - Request a payout\n\n"
        "ðŸŽ§ *Support*\n"
        "/support - Send a message to admin\n"
        "/cancel - Cancel current action\n"
    )
    if is_admin:
        text += (
            "\nðŸ”‘ *Admin Only*\n"
            "/pending - View pending withdrawal requests\n"
            "/reply <user\\_id> <msg> - Reply to a user's support message\n"
        )
    await update.message.reply_text(text, parse_mode="Markdown")


# --- SUPPORT -------------------------------------------------------------------

async def support_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not get_user(user.id):
        await update.message.reply_text("Send /start first to create your account.")
        return ConversationHandler.END

    await update.message.reply_text(
        "ðŸŽ§ *Support*\n\n"
        "Please describe your issue or question and send it as a message.\n"
        "Our admin will get back to you as soon as possible.\n\n"
        "Type /cancel to abort.",
        parse_mode="Markdown"
    )
    return WAITING_SUPPORT


async def receive_support_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text.strip() if update.message.text else None

    if not text:
        await update.message.reply_text(
            "âŒ Please send a text message. Type /cancel to abort."
        )
        return WAITING_SUPPORT

    user_data = get_user(user.id)
    user_display = display_name(user_data["first_name"], user_data["username"]) if user_data else user.first_name

    # Forward message to admin
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"ðŸ“© *New Support Message*\n\n"
                f"ðŸ‘¤ From: {user_display}\n"
                f"ðŸ†” User ID: `{user.id}`\n"
                f"{'@' + user.username if user.username else ''}\n\n"
                f"ðŸ’¬ *Message:*\n{text}\n\n"
                f"_To reply, use:_ `/reply {user.id} <your message>`"
            ),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"Could not forward support message to admin: {e}")
        await update.message.reply_text(
            "âŒ Failed to send your message. Please try again later."
        )
        return ConversationHandler.END

    await update.message.reply_text(
        "âœ… *Your message has been sent to our support team!*\n\n"
        "We will get back to you as soon as possible. Thank you for reaching out.",
        parse_mode="Markdown"
    )
    return ConversationHandler.END


async def reply_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: /reply <user_id> <message>"""
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: `/reply <user_id> <your message>`",
            parse_mode="Markdown"
        )
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("âŒ Invalid user ID.")
        return

    reply_text = " ".join(context.args[1:])
    target_user = get_user(target_id)
    target_display = display_name(target_user["first_name"], target_user["username"]) if target_user else f"User {target_id}"

    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"ðŸ“¬ *Reply from Support*\n\n"
                f"{reply_text}"
            ),
            parse_mode="Markdown"
        )
        await update.message.reply_text(
            f"âœ… Reply sent to {target_display} (`{target_id}`).",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"Could not send reply to {target_id}: {e}")
        await update.message.reply_text(
            f"âŒ Failed to send reply to `{target_id}`. The user may have blocked the bot.",
            parse_mode="Markdown"
        )


# --- MAIN ----------------------------------------------------------------------

def main():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    wallet_conv = ConversationHandler(
        entry_points=[CommandHandler("setwallet", setwallet_command)],
        states={
            WAITING_WALLET: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_wallet)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)]
    )

    support_conv = ConversationHandler(
        entry_points=[CommandHandler("support", support_command)],
        states={
            WAITING_SUPPORT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_support_message)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)]
    )

    app.add_handler(CommandHandler("start",       start_command))
    app.add_handler(CommandHandler("balance",     balance_command))
    app.add_handler(CommandHandler("stats",       stats_command))
    app.add_handler(CommandHandler("topearners",  topearners_command))
    app.add_handler(CommandHandler("withdraw",    withdraw_command))
    app.add_handler(CommandHandler("history",     history_command))
    app.add_handler(CommandHandler("help",        help_command))
    app.add_handler(CommandHandler("pending",     pending_command))   # Admin only
    app.add_handler(CommandHandler("reply",       reply_command))     # Admin only
    app.add_handler(wallet_conv)
    app.add_handler(support_conv)
    app.add_handler(CallbackQueryHandler(handle_admin_decision, pattern=r"^(approve|reject)_\d+$"))
    app.add_handler(ChatMemberHandler(track_chat_member, ChatMemberHandler.ANY_CHAT_MEMBER))

    # Fires when a real (non-bot) user sends a message in the group.
    # Excludes commands and bot-sent messages (welcome bots, captcha bots, etc.)
    app.add_handler(
        MessageHandler(
            filters.Chat(GROUP_ID) & ~filters.COMMAND & ~filters.VIA_BOT & ~filters.UpdateType.EDITED_MESSAGE,
            track_first_message_in_group
        ),
        group=-1
    )

    # Register commands so they appear in Telegram's "/" menu
    from telegram import BotCommand
    commands = [
        BotCommand("start",      "Home menu & referral link"),
        BotCommand("balance",    "Check your earnings & wallet"),
        BotCommand("stats",      "Your referral stats"),
        BotCommand("history",    "Your withdrawal history"),
        BotCommand("topearners", "Top 10 referrers leaderboard"),
        BotCommand("withdraw",   "Request a payout"),
        BotCommand("setwallet",  "Save your Binance wallet"),
        BotCommand("support",    "Contact admin for help"),
        BotCommand("help",       "Show all commands"),
        BotCommand("cancel",     "Cancel current action"),
    ]
    import asyncio
    async def set_commands():
        await app.bot.set_my_commands(commands)
    asyncio.get_event_loop().run_until_complete(set_commands())

    logger.info("Bot is running...")
    app.run_polling(allowed_updates=["message", "chat_member", "callback_query"])


if __name__ == "__main__":
    main()
