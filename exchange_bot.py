#!/usr/bin/env python3
"""
Exchange Bot (based on CashLait architecture).
"""

import logging
import os
import sqlite3
import threading
import time
import json
from datetime import datetime
from typing import Any, Dict, Optional, List
from html import escape
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

# =================================================================================
# CONFIGURATION
# =================================================================================
BOT_TOKEN = os.getenv("EXCHANGE_BOT_TOKEN")
DB_PATH = os.getenv("EXCHANGE_DB", "exchange.db")
# Ссылка на вашего конструктора (используется и для подписи, и для кнопки «Хочу такого же бота»)
# И username конструктора можно переопределить прямо здесь или через переменную окружения CREATOR_USERNAME.
CREATOR_USERNAME_DEFAULT = "@YourCreatorBot"
CREATOR_DEFAULT_LINK = f"https://t.me/{CREATOR_USERNAME_DEFAULT.lstrip('@')}"
# Parse ADMIN_IDS from env
raw_admins = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = set()
if raw_admins:
    for x in raw_admins.replace(";", ",").split(","):
        if x.strip().isdigit():
            ADMIN_IDS.add(int(x.strip()))

# Default Texts
DEFAULT_WELCOME = """Здравствуйте! 👋

Для того, чтобы подать заявку на обмен, просто в свободной форме напишите и отправьте сообщение в бот!

Пример 1:
FKwallet на карту РФ (ВТБ), сумма 1000р.

Пример 2:
C карты РФ (Альфа-банк) на FKwallet, сумма 2000р.

🚨 Минимальная сумма обмена: 500р."""

DEFAULT_HOW_TO = """Как совершить обмен:
1. Напишите направление обмена и сумму в бот.
2. Дождитесь ответа оператора.
3. Следуйте инструкциям."""

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("exchange_bot.log", encoding="utf-8"),
    ]
)
logger = logging.getLogger("exchange-bot")

if not BOT_TOKEN:
    logger.error("No BOT_TOKEN provided. Exiting.")
    exit(1)

bot = telebot.TeleBot(BOT_TOKEN)
BOT_SELF = bot.get_me()
BOT_SELF_ID = BOT_SELF.id

# =================================================================================
# STORAGE
# =================================================================================
class Storage:
    """Thread-safe SQLite helper."""

    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn:
            # Users table
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    join_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Settings table
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            # Bans table
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS bans (
                    user_id INTEGER PRIMARY KEY,
                    reason TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Message mapping for anonymous replies
            # admin_msg_id: ID of the message in the ADMIN CHAT (the forwarded copy)
            # user_id: The original sender ID
            # original_msg_id: The original message ID in user's chat (optional, for context)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS msg_map (
                    admin_msg_id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    original_msg_id INTEGER
                )
            """)

    def get_setting(self, key: str, default: str = "") -> str:
        with self._lock:
            cur = self._conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cur.fetchone()
            return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))

    def add_user(self, user: types.User) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
                (user.id, user.username, user.first_name)
            )
            # Update info if exists
            self._conn.execute(
                "UPDATE users SET username = ?, first_name = ? WHERE user_id = ?",
                (user.username, user.first_name, user.id)
            )

    def get_stats(self) -> dict:
        with self._lock:
            total = self._conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            today = self._conn.execute("SELECT COUNT(*) FROM users WHERE date(join_date) = date('now')").fetchone()[0]
            return {"total": total, "today": today}
            
    def save_msg_map(self, admin_msg_id: int, user_id: int, original_msg_id: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO msg_map (admin_msg_id, user_id, original_msg_id) VALUES (?, ?, ?)",
                (admin_msg_id, user_id, original_msg_id)
            )
            
    def get_msg_map(self, admin_msg_id: int) -> Optional[Dict]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM msg_map WHERE admin_msg_id = ?", (admin_msg_id,)).fetchone()
            return dict(row) if row else None

    def get_all_users(self) -> List[int]:
        with self._lock:
            rows = self._conn.execute("SELECT user_id FROM users").fetchall()
            return [r["user_id"] for r in rows]

db = Storage(DB_PATH)

# Initialize welcome text from env if provided (and not set in DB yet, or force update?)
# Usually Creator bot updates DB on startup? No, Creator passes env var.
# So if env var is present, we should use it as default or override.
# Let's update DB if env var differs.
env_welcome = os.getenv("EXCHANGE_WELCOME_TEXT")
if env_welcome:
    current_db_welcome = db.get_setting('welcome_text')
    # If DB is empty or we want to sync from Creator, update it.
    # Creator is the source of truth if edited there.
    if env_welcome != current_db_welcome:
        db.set_setting('welcome_text', env_welcome)

# =================================================================================
# HELPERS
# =================================================================================

def get_ban_record(user_id: int) -> Optional[Dict[str, str]]:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT user_id, reason, created_at FROM bans WHERE user_id = ?", (user_id,)).fetchone()
        return dict(zip(["user_id", "reason", "created_at"], row)) if row else None

def ban_user(target_id: int, reason: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "REPLACE INTO bans (user_id, reason, created_at) VALUES (?, ?, ?)",
            (target_id, reason, datetime.utcnow().isoformat())
        )

def unban_user(target_id: int) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("DELETE FROM bans WHERE user_id = ?", (target_id,))
        return cur.rowcount > 0

def list_banned_users() -> List[Dict[str, str]]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT user_id, reason, created_at FROM bans ORDER BY created_at DESC LIMIT 50").fetchall()
        return [dict(zip(["user_id", "reason", "created_at"], row)) for row in rows]

def get_op_channels() -> List[Dict[str, str]]:
    raw = db.get_setting('op_channels') or "[]"
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError:
        pass
    return []

def save_op_channels(channels: List[Dict[str, str]]) -> None:
    db.set_setting('op_channels', json.dumps(channels, ensure_ascii=False))

def format_channel_identifier(identifier: str) -> Dict[str, str]:
    identifier = identifier.strip()
    chat_id = identifier
    link = identifier
    if identifier.startswith("@"):
        chat_id = identifier
        link = f"https://t.me/{identifier.lstrip('@')}"
    elif identifier.startswith("https://t.me/"):
        link = identifier
        chat_id = "@" + identifier.split("https://t.me/", 1)[1]
    elif identifier.lstrip("-").isdigit():
        chat_id = int(identifier)
        link = ""
    else:
        link = f"https://t.me/{identifier}"
        chat_id = f"@{identifier}"
    return {"chat_id": chat_id, "link": link}


def _normalize_creator_link(value: Optional[str]) -> str:
    if not value:
        return ""
    trimmed = value.strip()
    if not trimmed:
        return ""
    if trimmed.startswith("@"):
        username = trimmed.lstrip("@")
        return f"https://t.me/{username}"
    return trimmed


def _derive_creator_label(raw_label: Optional[str], normalized_link: str) -> str:
    candidate = (raw_label or "").strip()
    if candidate:
        return candidate
    if normalized_link.startswith("https://t.me/"):
        username = normalized_link.split("https://t.me/", 1)[1].strip("/")
        if username:
            return f"@{username}"
    return normalized_link or ""


def _creator_label_html(label: str, normalized_link: str) -> str:
    safe_label = escape(label or "")
    safe_link = escape(normalized_link or "")
    if safe_label and safe_link:
        return f"<a href=\"{safe_link}\">{safe_label}</a>"
    return safe_label or safe_link


def _normalize_creator_username(value: Optional[str]) -> str:
    if not value:
        return ""
    trimmed = value.strip()
    if not trimmed:
        return ""
    if trimmed.startswith("https://t.me/"):
        trimmed = trimmed.split("https://t.me/", 1)[1]
    trimmed = trimmed.lstrip("@").strip()
    if not trimmed:
        return ""
    return f"@{trimmed}"


_TRUE_VALUES = {"1", "true", "yes", "on", "enable", "enabled", "y"}


def _env_flag(*names: str, default: bool = False) -> bool:
    for name in names:
        value = os.getenv(name)
        if value is None:
            continue
        if value.strip().lower() in _TRUE_VALUES:
            return True
    return default


CREATOR_BRANDING_ENABLED = _env_flag("CREATOR_BRANDING")
CREATOR_USERNAME = _normalize_creator_username(os.getenv("CREATOR_USERNAME") or CREATOR_USERNAME_DEFAULT)
creator_link_env = os.getenv("CREATOR_CONTACT_URL")
if creator_link_env:
    _creator_contact_url_source = creator_link_env
elif CREATOR_USERNAME:
    _creator_contact_url_source = f"https://t.me/{CREATOR_USERNAME.lstrip('@')}"
else:
    _creator_contact_url_source = CREATOR_DEFAULT_LINK

CREATOR_CONTACT_URL = _normalize_creator_link(_creator_contact_url_source)
CREATOR_CONTACT_LABEL = _derive_creator_label(os.getenv("CREATOR_CONTACT_LABEL", CREATOR_USERNAME), CREATOR_CONTACT_URL)
CREATOR_CONTACT_BUTTON_LABEL = (
    os.getenv("CREATOR_CONTACT_BUTTON_LABEL", "🤖 Хочу такого же бота").strip() or "🤖 Хочу такого же бота"
)
CREATOR_BRANDING_MESSAGE_TEMPLATE = (
    os.getenv("CREATOR_BRANDING_MESSAGE", "🤖 Бот создан с помощью {label_html}").strip()
)
VIP_BRANDING_DISABLED = _env_flag(
    "EXCHANGE_VIP_ACTIVE",
    "VIP_ACTIVE",
    "VIP_MODE",
    "VIP_BRANDING_DISABLED",
    "CREATOR_VIP_ACTIVE",
)


def is_creator_branding_active() -> bool:
    if VIP_BRANDING_DISABLED:
        return False
    return CREATOR_BRANDING_ENABLED and bool(CREATOR_CONTACT_URL or CREATOR_CONTACT_LABEL)


def render_creator_branding_text() -> Optional[str]:
    if not is_creator_branding_active():
        return None
    template = CREATOR_BRANDING_MESSAGE_TEMPLATE
    if not template:
        return None
    label = CREATOR_CONTACT_LABEL or CREATOR_CONTACT_URL
    label_html = _creator_label_html(label, CREATOR_CONTACT_URL)
    context = {
        "label": label or "",
        "label_html": label_html or "",
        "link": CREATOR_CONTACT_URL or "",
    }
    try:
        return template.format(**context)
    except KeyError:
        return (
            template.replace("{label_html}", context["label_html"])
            .replace("{label}", context["label"])
            .replace("{link}", context["link"])
        )


def send_creator_branding_banner(chat_id: int) -> None:
    text = render_creator_branding_text()
    if not text:
        return
    try:
        bot.send_message(chat_id, text, parse_mode="HTML", disable_web_page_preview=True)
    except ApiTelegramException:
        pass


def build_creator_button() -> Optional[types.InlineKeyboardButton]:
    if not is_creator_branding_active():
        return None
    if not CREATOR_CONTACT_URL:
        return None
    return types.InlineKeyboardButton(CREATOR_CONTACT_BUTTON_LABEL, url=CREATOR_CONTACT_URL)

def normalize_chat_username(value: str) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if value.startswith("https://t.me/"):
        value = value.split("https://t.me/", 1)[1]
    value = value.lstrip('@').strip()
    if not value:
        return None
    # Telegram usernames: letters, numbers, underscore; we allow + for invite? require typical username
    if not value.replace('_', '').isalnum():
        return None
    return f"@{value}"


_ADMIN_CHAT_CACHE = {"raw_id": None, "raw_username": None, "info": None}


def _build_admin_chat_info(raw_id: str, raw_username: str) -> Dict[str, Any]:
    numeric_id: Optional[int] = None
    username_label: Optional[str] = None
    username_lower: Optional[str] = None

    def consider(candidate: Optional[str]) -> None:
        nonlocal numeric_id, username_label, username_lower
        if not candidate:
            return
        cleaned = str(candidate).strip()
        if not cleaned:
            return
        if numeric_id is None and cleaned.lstrip("-").isdigit():
            try:
                numeric_id = int(cleaned)
                return
            except ValueError:
                pass
        if username_label is None:
            normalized = normalize_chat_username(cleaned)
            if normalized:
                username_label = normalized
                username_lower = normalized.lower()

    consider(raw_id)
    consider(raw_username)

    if username_label is None and raw_username:
        normalized = normalize_chat_username(raw_username)
        if normalized:
            username_label = normalized
            username_lower = normalized.lower()

    target: Optional[Any] = None
    if numeric_id is not None:
        target = numeric_id
    elif username_label:
        target = username_label

    display = "Не установлен"
    if username_label:
        display = username_label
    elif numeric_id is not None:
        display = str(numeric_id)
    elif raw_id:
        display = raw_id
    elif raw_username:
        display = raw_username

    return {
        "raw_id": raw_id,
        "raw_username": raw_username,
        "numeric_id": numeric_id,
        "username_label": username_label,
        "username_lower": username_lower,
        "target": target,
        "display": display,
        "is_configured": target is not None,
    }


def invalidate_admin_chat_cache() -> None:
    global _ADMIN_CHAT_CACHE
    _ADMIN_CHAT_CACHE = {"raw_id": None, "raw_username": None, "info": None}


def get_admin_chat_info(force_refresh: bool = False) -> Dict[str, Any]:
    raw_id = db.get_setting('admin_chat_id') or ""
    raw_username = db.get_setting('admin_chat_username') or ""
    global _ADMIN_CHAT_CACHE
    if (
        not force_refresh
        and _ADMIN_CHAT_CACHE["info"] is not None
        and _ADMIN_CHAT_CACHE["raw_id"] == raw_id
        and _ADMIN_CHAT_CACHE["raw_username"] == raw_username
    ):
        return _ADMIN_CHAT_CACHE["info"]
    info = _build_admin_chat_info(raw_id, raw_username)
    _ADMIN_CHAT_CACHE = {"raw_id": raw_id, "raw_username": raw_username, "info": info}
    return info


def is_message_from_admin_chat(chat: types.Chat, admin_info: Dict[str, Any]) -> bool:
    if not admin_info or not admin_info.get("is_configured"):
        return False
    numeric_id = admin_info.get("numeric_id")
    if numeric_id is not None and int(chat.id) == int(numeric_id):
        return True
    username_lower = admin_info.get("username_lower")
    chat_username = getattr(chat, "username", None)
    if username_lower and chat_username:
        if f"@{chat_username}".lower() == username_lower:
            return True
    return False

def check_required_channels(user_id: int) -> List[Dict[str, str]]:
    channels = get_op_channels()
    missing = []
    for ch in channels:
        target = ch.get('chat_id') or ch.get('link')
        if not target:
            continue
        try:
            member = bot.get_chat_member(target, user_id)
            if member.status in ('left', 'kicked'):
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return missing

def prompt_subscription(user_id: int, channels: Optional[List[Dict[str, str]]] = None) -> None:
    channels = channels or get_op_channels()
    if not channels:
        return
    text_lines = [
        "📡 <b>Обязательная подписка</b>",
        "",
        "Для продолжения подпишитесь на каналы ниже, затем нажмите «✅ Проверить подписку».",
    ]
    markup = types.InlineKeyboardMarkup(row_width=1)
    for ch in channels:
        title = ch.get('title') or ch.get('chat_id') or ch.get('link')
        link = ch.get('link') or (f"https://t.me/{str(ch.get('chat_id')).lstrip('@')}" if ch.get('chat_id') else "")
        if link:
            markup.add(types.InlineKeyboardButton(f"🔗 {title}", url=link))
    markup.add(types.InlineKeyboardButton("✅ Проверить подписку", callback_data="check_subs"))
    bot.send_message(user_id, "\n".join(text_lines), parse_mode="HTML", reply_markup=markup)

def ensure_subscription(user_id: int) -> bool:
    missing = check_required_channels(user_id)
    if missing:
        prompt_subscription(user_id, missing)
        return False
    return True

def render_ban_menu(chat_id: int, message_id: Optional[int] = None):
    text = (
        "🚫 <b>Управление банами</b>\n\n"
        "• «➕ Забанить» — формат: <code>ID|Причина</code>\n"
        "• «♻️ Разбанить» — отправьте ID пользователя\n"
        "• «📋 Список банов» — показать последние записи"
    )
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("➕ Забанить", callback_data="ban_add"))
    markup.add(types.InlineKeyboardButton("♻️ Разбанить", callback_data="ban_remove"))
    markup.add(types.InlineKeyboardButton("📋 Список банов", callback_data="ban_list"))
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="HTML", reply_markup=markup)
            return
        except Exception:
            pass
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)

def render_op_menu(chat_id: int, message_id: Optional[int] = None):
    channels = get_op_channels()
    if channels:
        lines = ["📡 <b>Каналы обязательной подписки</b>\n"]
        for idx, ch in enumerate(channels, start=1):
            title = ch.get('title') or ch.get('chat_id') or ch.get('link')
            link = ch.get('link') or ''
            lines.append(f"{idx}. {title} — {link or 'без ссылки'}")
        text = "\n".join(lines)
    else:
        text = "📡 <b>Обязательная подписка</b>\n\nСписок каналов пуст."
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("➕ Добавить канал", callback_data="op_add"))
    for idx, ch in enumerate(channels):
        title = ch.get('title') or ch.get('chat_id') or "Канал"
        markup.add(types.InlineKeyboardButton(f"❌ Удалить «{title}»", callback_data=f"op_remove_{idx}"))
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="HTML", reply_markup=markup)
            return
        except Exception:
            pass
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)

# =================================================================================
# HANDLERS
# =================================================================================

def main_menu(is_admin: bool = False):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("🔄 Как совершить обмен"), types.KeyboardButton("ℹ️ О боте"))
    if is_admin:
        markup.add(types.KeyboardButton("⚙️ Админка"))
    return markup

@bot.message_handler(commands=['start'])
def start_handler(message):
    db.add_user(message.from_user)
    # Re-fetch in case it changed
    welcome = db.get_setting('welcome_text') or DEFAULT_WELCOME
    ban_info = get_ban_record(message.from_user.id)
    if ban_info:
        bot.send_message(
            message.chat.id,
            f"🚫 Вы заблокированы.\nПричина: {ban_info.get('reason') or 'не указана'}",
            parse_mode="HTML"
        )
        return
    is_admin = message.from_user.id in ADMIN_IDS
    if not is_admin and not ensure_subscription(message.from_user.id):
        return
    try:
        bot.send_message(message.chat.id, welcome, reply_markup=main_menu(is_admin), parse_mode="HTML")
        if is_creator_branding_active():
            send_creator_branding_banner(message.chat.id)
    except Exception as e:
        logger.error(f"Error sending welcome: {e}")

@bot.message_handler(func=lambda m: m.text == "🔄 Как совершить обмен")
def howto_handler(message):
    text = db.get_setting('how_to_text') or DEFAULT_HOW_TO
    bot.send_message(message.chat.id, text, parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "ℹ️ О боте")
def about_handler(message):
    stats = db.get_stats()
    text = f"📊 Статистика:\n\n👤 Всего пользователей: {stats['total']}\n🆕 Новых сегодня: {stats['today']}"
    button = build_creator_button()
    markup = types.InlineKeyboardMarkup().add(button) if button else None
    bot.send_message(message.chat.id, text, reply_markup=markup)

# ADMIN PANEL
state = {} # Simple in-memory state for admin actions

@bot.message_handler(commands=['admin', 'settings'])
def admin_handler(message):
    if message.from_user.id not in ADMIN_IDS:
        return

    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("📝 Изменить приветствие", callback_data="set_welcome"))
    markup.add(types.InlineKeyboardButton("📄 Изменить 'Как обмен'", callback_data="set_howto"))
    markup.add(types.InlineKeyboardButton("🎯 Установить чат заявок", callback_data="set_chat"))
    markup.add(types.InlineKeyboardButton("📣 Рассылка", callback_data="broadcast"))
    markup.add(types.InlineKeyboardButton("🚫 Бан / Разбан", callback_data="ban_menu"))
    markup.add(types.InlineKeyboardButton("📡 Обязательная подписка", callback_data="op_menu"))
    markup.add(types.InlineKeyboardButton("ℹ️ Помощь по настройке", callback_data="admin_help"))
    
    admin_chat_info = get_admin_chat_info()
    current_chat = admin_chat_info.get("display") or 'Не установлен'
    text = f"⚙️ Админ панель\n\nТекущий чат для заявок: {current_chat}"
    bot.send_message(message.chat.id, text, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    user_id = call.from_user.id
    if call.data == "check_subs":
        missing = check_required_channels(user_id)
        if missing:
            bot.answer_callback_query(call.id, "❌ Подписка не найдена. Подпишитесь и попробуйте ещё раз.", show_alert=True)
            prompt_subscription(user_id, missing)
        else:
            bot.answer_callback_query(call.id, "✅ Подписка подтверждена!", show_alert=True)
            bot.send_message(user_id, "Спасибо! Подписка подтверждена, продолжайте работу.", parse_mode="HTML")
        return
    if user_id not in ADMIN_IDS:
        return
    
    if call.data == "set_chat":
        state[user_id] = 'waiting_for_chat_id'
        bot.send_message(
            call.message.chat.id,
            "Отправьте ссылку на чат/канал в формате <code>https://t.me/username</code> или @username, куда будут падать заявки.\n"
            "⚠️ Добавьте бота администратором в этот чат, иначе пересылка обращений не будет работать.",
            parse_mode="HTML"
        )
    elif call.data == "set_welcome":
        state[user_id] = 'waiting_for_welcome'
        bot.send_message(call.message.chat.id, "Отправьте новый текст приветствия.")
    elif call.data == "set_howto":
        state[user_id] = 'waiting_for_howto'
        bot.send_message(call.message.chat.id, "Отправьте новый текст 'Как совершить обмен'.")
    elif call.data == "broadcast":
        state[user_id] = 'waiting_for_broadcast'
        bot.send_message(call.message.chat.id, "Отправьте сообщение для рассылки (текст/фото).")
    elif call.data == "ban_menu":
        bot.answer_callback_query(call.id)
        render_ban_menu(call.message.chat.id, call.message.message_id)
        return
    elif call.data == "ban_add":
        bot.answer_callback_query(call.id)
        state[user_id] = 'waiting_for_ban'
        bot.send_message(call.message.chat.id, "Введите ID пользователя и причину через вертикальную черту. Пример: <code>12345|спам</code>", parse_mode="HTML")
        return
    elif call.data == "ban_remove":
        bot.answer_callback_query(call.id)
        state[user_id] = 'waiting_for_unban'
        bot.send_message(call.message.chat.id, "Введите ID пользователя для разбана:")
        return
    elif call.data == "ban_list":
        bot.answer_callback_query(call.id)
        banned = list_banned_users()
        if not banned:
            bot.send_message(call.message.chat.id, "Список банов пуст.")
        else:
            lines = ["📋 <b>Заблокированные пользователи:</b>"]
            for item in banned:
                lines.append(f"• <code>{item['user_id']}</code> — {item.get('reason') or 'без причины'}")
            bot.send_message(call.message.chat.id, "\n".join(lines), parse_mode="HTML")
        return
    elif call.data == "op_menu":
        bot.answer_callback_query(call.id)
        render_op_menu(call.message.chat.id, call.message.message_id)
        return
    elif call.data == "op_add":
        bot.answer_callback_query(call.id)
        state[user_id] = 'waiting_for_op_channel'
        bot.send_message(
            call.message.chat.id,
            "Отправьте канал в формате <code>Название | @username</code> или ссылку.",
            parse_mode="HTML"
        )
        return
    elif call.data.startswith("op_remove_"):
        bot.answer_callback_query(call.id)
        try:
            idx = int(call.data.split('_')[2])
            channels = get_op_channels()
            if 0 <= idx < len(channels):
                removed = channels.pop(idx)
                save_op_channels(channels)
                bot.send_message(call.message.chat.id, f"✅ Канал «{removed.get('title') or removed.get('chat_id')}» удалён.")
            else:
                bot.send_message(call.message.chat.id, "❌ Канал не найден.")
        except Exception as e:
            bot.send_message(call.message.chat.id, f"❌ Ошибка: {e}")
        render_op_menu(call.message.chat.id, call.message.message_id)
        return
    elif call.data == "admin_help":
        bot.answer_callback_query(call.id)
        help_text = (
            "ℹ️ <b>Помощь по настройке бота-обменника</b>\n\n"
            "1. Получите токен бота у @BotFather и укажите его в конфигурации.\n"
            "2. В разделе «🎯 Установить чат заявок» задайте чат, куда будут пересылаться все обращения пользователей. "
            "Обязательно добавьте бота администратором в этот чат — без прав администратора пересылка работать не будет.\n"
            "3. (Опционально) в «📡 Обязательная подписка» добавьте каналы, на которые пользователь должен подписаться.\n"
            "4. Используйте «🚫 Бан / Разбан», чтобы ограничивать доступ нежелательных пользователей.\n"
            "5. При необходимости делайте рассылки через «📣 Рассылка» — бот разошлёт сообщение всем пользователям.\n\n"
            "После смены токена или приветствия перезапустите бота через конфигурацию в конструкторе."
        )
        bot.send_message(call.message.chat.id, help_text, parse_mode="HTML")
        return
        
    bot.answer_callback_query(call.id)

# UNIFIED MESSAGE PROCESSING
@bot.message_handler(content_types=['text', 'photo', 'video', 'document', 'voice', 'sticker', 'animation', 'audio'])
def message_processor(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    admin_chat_info = get_admin_chat_info()
    admin_chat_target = admin_chat_info.get("target")
    admin_chat_configured = admin_chat_info.get("is_configured")
    in_admin_chat = admin_chat_configured and is_message_from_admin_chat(message.chat, admin_chat_info)
    is_admin = user_id in ADMIN_IDS
    
    if not is_admin and not in_admin_chat:
        ban_info = get_ban_record(user_id)
        if ban_info:
            bot.send_message(chat_id, f"🚫 Вы заблокированы.\nПричина: {ban_info.get('reason') or 'не указана'}", parse_mode="HTML")
            return
        if not ensure_subscription(user_id):
            return
    else:
        if message.text == "⚙️ Админка":
            admin_handler(message)
            return
    
    # 1. ADMIN ACTIONS (Setting values)
    if is_admin and user_id in state:
        action = state[user_id]
        if action == 'waiting_for_chat_id':
            raw = (message.text or "").strip()
            normalized = normalize_chat_username(raw)
            if not normalized:
                bot.send_message(chat_id, "❌ Укажите @username канала/чата или ссылку вида https://t.me/username.")
                return
            try:
                member = bot.get_chat_member(normalized, BOT_SELF_ID)
                status = getattr(member, 'status', '')
                if status not in ('administrator', 'creator'):
                    bot.send_message(chat_id, "❌ Добавьте бота администратором в этот чат и попробуйте снова.")
                    return
            except ApiTelegramException as e:
                bot.send_message(chat_id, f"❌ Не удалось проверить чат: {e}")
                return

            resolved_chat_id = None
            resolved_username = None
            try:
                chat_obj = bot.get_chat(normalized)
                resolved_chat_id = getattr(chat_obj, "id", None)
                resolved_username = getattr(chat_obj, "username", None)
            except ApiTelegramException:
                pass

            if resolved_chat_id is not None:
                db.set_setting('admin_chat_id', str(resolved_chat_id))
            else:
                db.set_setting('admin_chat_id', normalized)

            if resolved_username:
                db.set_setting('admin_chat_username', f"@{resolved_username}")
            elif normalized.startswith("@"):
                db.set_setting('admin_chat_username', normalized)
            else:
                db.set_setting('admin_chat_username', "")

            invalidate_admin_chat_cache()
            new_admin_chat_info = get_admin_chat_info()
            bot.send_message(
                chat_id,
                f"✅ Чат заявок установлен: {new_admin_chat_info.get('display', normalized)}\nБот подтверждён как администратор.",
            )
            del state[user_id]
            return
        elif action == 'waiting_for_welcome':
            db.set_setting('welcome_text', message.text or "")
            bot.send_message(chat_id, "✅ Приветствие обновлено.")
            del state[user_id]
            return
        elif action == 'waiting_for_howto':
            db.set_setting('how_to_text', message.text or "")
            bot.send_message(chat_id, "✅ Текст обновлен.")
            del state[user_id]
            return
        elif action == 'waiting_for_broadcast':
            del state[user_id]
            bot.send_message(chat_id, "🚀 Рассылка запущена...")
            users = db.get_all_users()
            count = 0
            for uid in users:
                try:
                    bot.copy_message(uid, chat_id, message.message_id)
                    count += 1
                    time.sleep(0.05)
                except Exception:
                    pass
            bot.send_message(chat_id, f"✅ Рассылка завершена. Доставлено: {count}")
            return
        elif action == 'waiting_for_ban':
            try:
                parts = message.text.split('|', 1)
                target_id = int(parts[0].strip())
                reason = parts[1].strip() if len(parts) > 1 else "Не указана"
                ban_user(target_id, reason)
                bot.send_message(chat_id, f"✅ Пользователь <code>{target_id}</code> заблокирован.\nПричина: {reason}", parse_mode="HTML")
            except Exception:
                bot.send_message(chat_id, "❌ Ошибка. Используйте формат <code>ID|Причина</code>.", parse_mode="HTML")
                return
            del state[user_id]
            render_ban_menu(chat_id)
            try: bot.delete_message(chat_id, message.message_id)
            except: pass
            return
        elif action == 'waiting_for_unban':
            try:
                target_id = int(message.text.strip())
            except ValueError:
                bot.send_message(chat_id, "❌ Ошибка. Введите корректный ID пользователя.")
                return
            if unban_user(target_id):
                bot.send_message(chat_id, f"♻️ Пользователь <code>{target_id}</code> разблокирован.", parse_mode="HTML")
            else:
                bot.send_message(chat_id, "ℹ️ Пользователь не найден в списке банов.")
            del state[user_id]
            render_ban_menu(chat_id)
            try: bot.delete_message(chat_id, message.message_id)
            except: pass
            return
        elif action == 'waiting_for_op_channel':
            raw = (message.text or "").strip()
            if not raw:
                bot.send_message(chat_id, "❌ Пустое значение. Повторите ввод.")
                return
            if '|' in raw:
                title, identifier = [x.strip() for x in raw.split('|', 1)]
            else:
                title, identifier = raw, raw
            info = format_channel_identifier(identifier)
            if not info.get('link') and not str(info.get('chat_id', '')).startswith('@'):
                bot.send_message(chat_id, "❌ Укажите @username или ссылку на канал.")
                return
            info['title'] = title or info.get('chat_id') or info.get('link')
            channels = get_op_channels()
            channels.append(info)
            save_op_channels(channels)
            bot.send_message(chat_id, f"✅ Канал «{info['title']}» добавлен.")
            del state[user_id]
            render_op_menu(chat_id)
            try: bot.delete_message(chat_id, message.message_id)
            except: pass
            return

    # 2. OPERATOR REPLY LOGIC (In Admin Chat)
    if admin_chat_configured and in_admin_chat:
        # This is a message in the admin chat
        if message.reply_to_message:
            # Check if we have a mapping for the message being replied to
            mapping = db.get_msg_map(message.reply_to_message.message_id)
            if mapping:
                target_user = mapping['user_id']
                try:
                    bot.copy_message(target_user, chat_id, message.message_id)
                    # Optional: React or silent success
                except Exception as e:
                    logger.error(f"Failed to copy reply to user {target_user}: {e}")
                    bot.reply_to(message, f"❌ Ошибка отправки: {e}")
            else:
                # Reply to unmapped message (maybe system msg or old)
                pass
        return

    # 3. USER MESSAGE -> FORWARD TO ADMIN CHAT
    # If not in admin state and not in admin chat (already handled above if chat_id == admin_chat_id)
    if admin_chat_target and not in_admin_chat:
        # Ignore commands
        if message.text and message.text.startswith('/'):
            return

        try:
            # Forward the message to admin chat
            fwd_msg = bot.forward_message(admin_chat_target, chat_id, message.message_id)
            # Save mapping so we know who sent it
            db.save_msg_map(fwd_msg.message_id, user_id, message.message_id)
        except Exception as e:
            logger.error(f"Failed to forward message from {user_id}: {e}")
            # Don't tell user about error to keep it clean, or maybe a generic "Operator offline" if critical

if __name__ == "__main__":
    logger.info("Starting Exchange Bot...")
    while True:
        try:
            bot.polling(non_stop=True, interval=1, timeout=20)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(5)
