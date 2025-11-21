#!/usr/bin/env python3
"""
CashLait task bot.

Features implemented:
- Main reply keyboard: Личный кабинет, Задания, Продвижение, Рефералы, Инфо
- Flyer API integration for tasks
- Custom OP tasks stored in SQLite
- Referral system with 2 levels
- Withdrawal via Crypto Pay API checks
- Admin panel with settings, broadcasts, OP management, reserve tools
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html import escape
from io import BytesIO
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import telebot
from telebot import types
from telebot.apihelper import ApiException


# ⚠️ ВСТАВЬТЕ ВАШ ТОКЕН БОТА ОТ @BotFather:
BOT_TOKEN = os.getenv("CASHLAIT_BOT_TOKEN", "8400644706:AAFjCQDxS73hvhizY4f3v94-vlXLkvqGHdQ")  # Например: "1234567890:ABCdefGHIjklMNOpqrsTUVwxyz"
CONSTRUCTOR_BOT_USERNAME = os.getenv("CONSTRUCTOR_BOT_USERNAME", "MinxoCreate_bot").strip("@ ")
CONSTRUCTOR_BOT_LINK = os.getenv("CONSTRUCTOR_BOT_LINK")
if not CONSTRUCTOR_BOT_LINK:
    if CONSTRUCTOR_BOT_USERNAME:
        CONSTRUCTOR_BOT_LINK = f"https://t.me/{CONSTRUCTOR_BOT_USERNAME}"
    else:
        CONSTRUCTOR_BOT_LINK = ""
CREATOR_USERNAME_DEFAULT = f"@{CONSTRUCTOR_BOT_USERNAME}" if CONSTRUCTOR_BOT_USERNAME else "@MinxoCreate_bot"
CREATOR_CONTACT_LABEL_DEFAULT = os.getenv("CONSTRUCTOR_BOT_LABEL", "🤖 Хочу такого же бота") or "🤖 Хочу такого же бота"
ADMIN_IDS = {
    int(token)
    for token in os.getenv("ADMIN_IDS", "6745031200,7585735331").replace(";", ",").split(",")
    if token.strip().isdigit()
}
DATABASE_PATH = os.getenv(
    "CASHLAIT_DB",
    os.path.join(os.path.dirname(__file__), "cashlait.db"),
)
LOG_FILE_PATH = os.getenv(
    "CASHLAIT_LOG",
    os.path.join(os.path.dirname(__file__), "cashlait_bot.log"),
)


DEFAULT_SETTINGS: Dict[str, str] = {
    "currency_symbol": "USDT",
    "task_reward": "1.0",
    "task_price_per_completion": "1.0",
    "min_withdraw": "3.0",
    "flyer_api_key": "",
    "flyer_task_limit": "5",
    "crypto_pay_token": "",
    "crypto_pay_asset": "USDT",
    # "asset_rate" убран - курс получается автоматически через Crypto Pay API
    "ref_percent_level1": "15.0",
    "ref_percent_level2": "5.0",
    "payout_notify_channel": "",
    "reserve_invoice_asset": "USDT",
    "reserve_invoice_description": "Пополнение резерва",
    "welcome_text": "Добро пожаловать! Здесь вы сможете зарабатывать на подписках с автовыводом средств. Используйте меню ниже, чтобы начать.",
    "menu_btn_cabinet": "📱 Кабинет",
    "menu_btn_tasks": "📝 Задания",
    "menu_btn_promo": "📣 Продвижение",
    "menu_btn_referrals": "👥 Рефералы",
    "menu_btn_info": "📚 Инфо",
    "menu_btn_admin": "⚙️ Админка",
    "info_help_url": "",
    "info_news_url": "",
    "info_chat_url": "",
    # Настройки брендинга конструктора
    "creator_contact_url": CONSTRUCTOR_BOT_LINK,
    "creator_contact_label": CREATOR_CONTACT_LABEL_DEFAULT,
    "creator_contact_button_label": "🤖 Хочу такого же бота",
    "creator_branding_message": "🤖 Бот создан с помощью {label_html}",
    "creator_branding_enabled": "true",
    "vip_branding_disabled": "false",
}

ADMIN_SETTING_FIELDS: Dict[str, Tuple[str, str]] = {
    "task_reward": ("Награда исполнителю (USDT)", "decimal"),
    "task_price_per_completion": ("Стоимость для рекламодателя (USDT)", "decimal"),
    "min_withdraw": ("Минимальный вывод (USDT)", "decimal"),
    "currency_symbol": ("Символ валюты", "text"),
    "ref_percent_level1": ("Процент 1 уровня (%)", "decimal"),
    "ref_percent_level2": ("Процент 2 уровня (%)", "decimal"),
    # "asset_rate" убран - курс получается автоматически через Crypto Pay API
}

FLYER_SETTING_FIELDS: Dict[str, Tuple[str, str]] = {
    "flyer_api_key": ("API ключ Flyer", "text"),
    "flyer_task_limit": ("Лимит выдачи заданий", "int"),
}

BUTTON_SETTING_FIELDS: Dict[str, Tuple[str, str]] = {
    "menu_btn_cabinet": ("Кнопка «Кабинет»", "text"),
    "menu_btn_tasks": ("Кнопка «Задания»", "text"),
    "menu_btn_promo": ("Кнопка «Продвижение»", "text"),
    "menu_btn_referrals": ("Кнопка «Рефералы»", "text"),
    "menu_btn_info": ("Кнопка «Инфо»", "text"),
    "menu_btn_admin": ("Кнопка «Админка»", "text"),
}

INFO_LINK_FIELDS: Dict[str, Tuple[str, str]] = {
    "info_help_url": ("Ссылка «Помощь»", "text"),
    "info_news_url": ("Ссылка «Новости»", "text"),
    "info_chat_url": ("Ссылка «Чат»", "text"),
    # Кнопка «Хочу такого же бота» теперь настраивается отдельными параметрами брендинга
}

RESERVE_SETTING_FIELDS: Dict[str, Tuple[str, str]] = {
    "crypto_pay_token": ("Crypto Pay токен", "text"),
    "crypto_pay_asset": ("Актив выплат (например, USDT)", "text"),
    "reserve_invoice_asset": ("Актив пополнения (например, USDT)", "text"),
    "reserve_invoice_description": ("Описание счёта", "text"),
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE_PATH, encoding="utf-8"),
    ],
    force=True,
)
logger = logging.getLogger("cashlait-bot")


MONEY_QUANT = Decimal("0.001")
ASSET_QUANT = Decimal("0.00000001")
FLYER_FAIL_STATUSES = {"incomplete", "abort"}
FLYER_PENALTY_STATUSES = {"unsubscribe", "unsubscribed", "left", "removed", "abort"}
DECIMAL_INPUT_QUANT = Decimal("0.0001")


def now_utc() -> datetime:
    return datetime.now(UTC)


def dec(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError):
        return Decimal(default)


def format_amount(amount: Decimal, symbol: str) -> str:
    return f"{amount.quantize(Decimal('0.000'), rounding=ROUND_HALF_UP)} {symbol}"


def format_duration(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    if total_seconds <= 0:
        return "менее минуты"
    hours, remainder = divmod(total_seconds, 3600)
    minutes = remainder // 60
    parts: List[str] = []
    if hours:
        parts.append(f"{hours} ч")
    if minutes or not parts:
        parts.append(f"{minutes} мин")
    return " ".join(parts)


def parse_chat_identifier(value: str) -> Optional[int | str]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if value.lstrip("-").isdigit():
        try:
            return int(value)
        except ValueError:
            pass
    return value


def row_get(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, sqlite3.Row):
        if key in row.keys():
            value = row[key]
            return default if value is None else value
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    return getattr(row, key, default)


def row_to_dict(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return row
    if isinstance(row, sqlite3.Row):
        return {col: row[col] for col in row.keys()}
    if hasattr(row, "__dict__"):
        return dict(vars(row))
    return {"value": row}


def mask_setting_value(value: str) -> str:
    if not value:
        return "не задано"
    value = value.strip()
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def setting_display(key: str, value: str) -> str:
    if key.endswith("token"):
        return mask_setting_value(value)
    if not value:
        return "не задано"
    return value


def parse_decimal_input(text: str, quant: Decimal = DECIMAL_INPUT_QUANT) -> Decimal:
    value = Decimal(text.replace(",", "."))
    return value.quantize(quant, rounding=ROUND_HALF_UP)


_TRUE_VALUES = {"1", "true", "yes", "on", "enable", "enabled", "y"}
_VIP_ENV_FLAGS = (
    "CASHLAIT_VIP_ACTIVE",
    "EXCHANGE_VIP_ACTIVE",
    "VIP_ACTIVE",
    "VIP_MODE",
    "VIP_BRANDING_DISABLED",
    "CREATOR_VIP_ACTIVE",
)


def _normalize_creator_link(value: Optional[str]) -> str:
    if not value:
        return ""
    trimmed = str(value).strip()
    if not trimmed:
        return ""
    if trimmed.startswith("@"):
        username = trimmed.lstrip("@")
        return f"https://t.me/{username}"
    return trimmed


def _derive_creator_label(raw_label: Optional[str], normalized_link: str) -> str:
    label = (raw_label or "").strip()
    if label:
        return label
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


def _str_to_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in _TRUE_VALUES


def _env_flag(*names: str) -> Optional[bool]:
    for name in names:
        value = os.getenv(name)
        if value is None:
            continue
        return _str_to_bool(value, False)
    return None


def get_creator_contact_url() -> str:
    env_value = os.getenv("CREATOR_CONTACT_URL")
    if env_value:
        return _normalize_creator_link(env_value)
    setting_value = db.get_setting("creator_contact_url", CONSTRUCTOR_BOT_LINK or "")
    if setting_value:
        return _normalize_creator_link(setting_value)
    return _normalize_creator_link(CONSTRUCTOR_BOT_LINK)


def get_creator_contact_label() -> str:
    env_value = os.getenv("CREATOR_CONTACT_LABEL")
    if env_value:
        return env_value.strip()
    setting_value = db.get_setting("creator_contact_label", CREATOR_CONTACT_LABEL_DEFAULT)
    if setting_value:
        return setting_value.strip()
    return _derive_creator_label("", get_creator_contact_url())


def get_creator_button_label() -> str:
    env_value = os.getenv("CREATOR_CONTACT_BUTTON_LABEL")
    if env_value:
        return env_value.strip()
    setting_value = db.get_setting("creator_contact_button_label", "🤖 Хочу такого же бота")
    if setting_value:
        return setting_value.strip()
    return "🤖 Хочу такого же бота"


def is_vip_branding_disabled() -> bool:
    env_value = _env_flag(*_VIP_ENV_FLAGS)
    if env_value:
        return True
    setting_value = db.get_setting("vip_branding_disabled", "false")
    return _str_to_bool(setting_value, False)


def is_creator_branding_active() -> bool:
    if is_vip_branding_disabled():
        return False
    env_flag = _env_flag("CREATOR_BRANDING_ENABLED", "CREATOR_BRANDING")
    if env_flag is not None:
        enabled = env_flag
    else:
        enabled = _str_to_bool(db.get_setting("creator_branding_enabled", "true"), True)
    if not enabled:
        return False
    return bool(get_creator_contact_url() or get_creator_contact_label())


def render_creator_branding_text() -> Optional[str]:
    if not is_creator_branding_active():
        return None
    template = os.getenv("CREATOR_BRANDING_MESSAGE")
    if template is None:
        template = db.get_setting("creator_branding_message", "🤖 Бот создан с помощью {label_html}")
    template = template.strip()
    if not template:
        return None
    link = get_creator_contact_url()
    label = get_creator_contact_label() or link
    label_html = _creator_label_html(label, link)
    context = {
        "label": label or "",
        "label_html": label_html or "",
        "link": link or "",
    }
    try:
        return template.format(**context)
    except KeyError:
        return (
            template.replace("{label_html}", context["label_html"])
            .replace("{label}", context["label"])
            .replace("{link}", context["link"])
        )


def build_creator_branding_button() -> Optional[types.InlineKeyboardButton]:
    if not is_creator_branding_active():
        return None
    link = get_creator_contact_url()
    if not link:
        return None
    text = get_creator_button_label() or get_creator_contact_label()
    if not text:
        text = "🤖 Хочу такого же бота"
    return types.InlineKeyboardButton(text, url=link)


def build_creator_branding_markup() -> Optional[types.InlineKeyboardMarkup]:
    button = build_creator_branding_button()
    if not button:
        return None
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(button)
    return markup


def send_creator_branding_banner(chat_id: int) -> None:
    if not is_creator_branding_active():
        return
    text = render_creator_branding_text()
    markup = build_creator_branding_markup()
    if not text and not markup:
        return
    try:
        bot.send_message(chat_id, text or "", reply_markup=markup, disable_web_page_preview=True)
    except ApiException:
        pass


def convert_admin_value(value_type: str, raw_text: str) -> Tuple[bool, Optional[str], str]:
    cleaned = (raw_text or "").strip()
    if not cleaned:
        return False, None, "Значение не может быть пустым."
    try:
        if value_type == "decimal":
            decimal_value = parse_decimal_input(cleaned)
            return True, f"{decimal_value.normalize():f}", ""
        if value_type == "int":
            return True, str(int(cleaned)), ""
        return True, cleaned, ""
    except (InvalidOperation, ValueError):
        return False, None, "Введите корректное число."


class Storage:
    """Thread-safe SQLite helper."""

    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    language_code TEXT,
                    balance REAL NOT NULL DEFAULT 0,
                    withdrawn_total REAL NOT NULL DEFAULT 0,
                    completed_tasks INTEGER NOT NULL DEFAULT 0,
                    referrer_id INTEGER,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_faucet_claim TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS required_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    invite_link TEXT,
                    category TEXT NOT NULL DEFAULT 'global'
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS custom_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    placement TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    button_text TEXT NOT NULL,
                    url TEXT NOT NULL,
                    channel_id TEXT,
                    reward REAL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    signature TEXT,
                    source TEXT NOT NULL,
                    context TEXT NOT NULL,
                    reward REAL NOT NULL,
                    completed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    context TEXT NOT NULL,
                    signature TEXT NOT NULL,
                    source TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, context, signature)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    creator_id INTEGER NOT NULL,
                    signature TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    description TEXT,
                    url TEXT NOT NULL,
                    button_text TEXT NOT NULL,
                    completions INTEGER NOT NULL,
                    cost_per_completion REAL NOT NULL,
                    total_cost REAL NOT NULL,
                    channel_id TEXT,
                    channel_username TEXT,
                    channel_link TEXT,
                    completed_count INTEGER NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._ensure_column("promo_tasks", "channel_id TEXT")
            self._ensure_column("promo_tasks", "channel_username TEXT")
            self._ensure_column("promo_tasks", "channel_link TEXT")
            self._ensure_column("promo_tasks", "completed_count INTEGER NOT NULL DEFAULT 0")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS subscription_watchlist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    signature TEXT NOT NULL,
                    source TEXT NOT NULL,
                    reward REAL NOT NULL,
                    expires_at TEXT NOT NULL,
                    last_checked TEXT,
                    completed INTEGER NOT NULL DEFAULT 0,
                    penalty_applied INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS withdraw_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT NOT NULL,
                    check_id TEXT,
                    check_url TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS deposit_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    asset_amount REAL NOT NULL,
                    invoice_id TEXT NOT NULL UNIQUE,
                    invoice_url TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS referral_earnings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER NOT NULL,
                    referred_id INTEGER NOT NULL,
                    level INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        self._bootstrap_settings()
        self._migrate_schema()
        self._migrate_settings()
        self._normalize_initial_balances()

    def _migrate_settings(self) -> None:
        with self._lock, self._conn:
            cur = self._conn.execute("SELECT value FROM settings WHERE key = 'currency_symbol'")
            row = cur.fetchone()
            current = (row["value"] or "").strip() if row else ""
            normalized = current.replace(".", "").replace(" ", "")
            normalized_upper = normalized.upper()
            legacy_symbols = {"₽"}
            legacy_codes = {"RUB", "RUBLE", "RUBLES", "РУБ", "РУБЛЬ", "РУБЛЕЙ"}
            is_legacy = (
                not normalized
                or current in legacy_symbols
                or normalized_upper in legacy_codes
            )
            if is_legacy:
                if row:
                    self._conn.execute(
                        "UPDATE settings SET value = ? WHERE key = 'currency_symbol'",
                        ("USDT",),
                    )
                else:
                    self._conn.execute(
                        "INSERT INTO settings (key, value) VALUES (?, ?)",
                        ("currency_symbol", "USDT"),
                    )

    def _normalize_initial_balances(self) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE users
                SET balance = 0
                WHERE COALESCE(balance, 0) != 0
                  AND COALESCE(withdrawn_total, 0) = 0
                  AND COALESCE(completed_tasks, 0) = 0
                  AND user_id NOT IN (SELECT DISTINCT user_id FROM task_logs)
                  AND user_id NOT IN (
                      SELECT DISTINCT user_id FROM deposit_requests WHERE status = 'paid'
                  )
                """
            )

    def _migrate_schema(self) -> None:
        with self._lock, self._conn:
            self._ensure_column("users", "language_code TEXT")
            self._ensure_column("users", "frozen_balance REAL NOT NULL DEFAULT 0")
            self._ensure_column("users", "promo_balance REAL NOT NULL DEFAULT 0")

    def _ensure_column(self, table: str, column_def: str) -> None:
        column_name = column_def.split()[0]
        cur = self._conn.execute(f"PRAGMA table_info({table})")
        existing = {row["name"] for row in cur.fetchall()}
        if column_name not in existing:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")

    def _bootstrap_settings(self) -> None:
        with self._lock, self._conn:
            for key, value in DEFAULT_SETTINGS.items():
                self._conn.execute(
                    "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                    (key, value),
                )

    def ensure_user(self, tg_user: telebot.types.User, referrer_id: Optional[int] = None) -> sqlite3.Row:
        language_code = getattr(tg_user, "language_code", None)
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO users (user_id, username, first_name, language_code, referrer_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (tg_user.id, tg_user.username, tg_user.first_name, language_code, referrer_id),
            )
            self._conn.execute(
                """
                UPDATE users
                SET username = ?,
                    first_name = ?,
                    language_code = COALESCE(?, language_code),
                    last_seen = ?
                WHERE user_id = ?
                """,
                (
                    tg_user.username,
                    tg_user.first_name,
                    language_code,
                    now_utc().isoformat(timespec="seconds"),
                    tg_user.id,
                ),
            )
        return self.get_user(tg_user.id)

    def get_user(self, user_id: int) -> sqlite3.Row:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
        if row is None:
            raise ValueError(f"user {user_id} not found")
        return row

    def set_referrer_if_empty(self, user_id: int, referrer_id: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE users
                SET referrer_id = ?
                WHERE user_id = ? AND (referrer_id IS NULL OR referrer_id = 0)
                """,
                (referrer_id, user_id),
            )

    def update_user_balance(
        self,
        user_id: int,
        *,
        delta_balance: Decimal = Decimal("0"),
        delta_withdrawn: Decimal = Decimal("0"),
        delta_promo_balance: Decimal = Decimal("0"),
        delta_frozen_balance: Decimal = Decimal("0"),
        inc_completed: int = 0,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE users
                SET balance = balance + ?,
                    withdrawn_total = withdrawn_total + ?,
                    promo_balance = COALESCE(promo_balance, 0) + ?,
                    frozen_balance = COALESCE(frozen_balance, 0) + ?,
                    completed_tasks = completed_tasks + ?
                WHERE user_id = ?
                """,
                (
                    float(delta_balance),
                    float(delta_withdrawn),
                    float(delta_promo_balance),
                    float(delta_frozen_balance),
                    inc_completed,
                    user_id,
                ),
            )

    def add_task_log(
        self,
        user_id: int,
        signature: str,
        source: str,
        context: str,
        reward: Decimal,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO task_logs (user_id, signature, source, context, reward, completed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    signature,
                    source,
                    context,
                    float(reward),
                    now_utc().isoformat(timespec="seconds"),
                ),
            )

    def save_tasks(self, user_id: int, context: str, tasks: List[Dict[str, Any]]) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "DELETE FROM pending_tasks WHERE user_id = ? AND context = ?",
                (user_id, context),
            )
            for task in tasks:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO pending_tasks (user_id, context, signature, source, payload)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        user_id,
                        context,
                        task["signature"],
                        task.get("source", "flyer"),
                        json.dumps(task, ensure_ascii=False),
                    ),
                )

    def load_tasks(self, user_id: int, context: str) -> List[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT payload FROM pending_tasks WHERE user_id = ? AND context = ?",
                (user_id, context),
            )
            payloads = [json.loads(row["payload"]) for row in cur.fetchall()]
        return payloads

    def list_pending_tasks(self, user_id: int, context: str) -> List[Tuple[int, Dict[str, Any]]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT id, payload FROM pending_tasks WHERE user_id = ? AND context = ? ORDER BY id",
                (user_id, context),
            )
            result: List[Tuple[int, Dict[str, Any]]] = []
            for row in cur.fetchall():
                result.append((row["id"], json.loads(row["payload"])))
            return result

    def get_pending_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT user_id, context, payload FROM pending_tasks WHERE id = ?",
                (task_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        data = json.loads(row["payload"])
        data["_user_id"] = row["user_id"]
        data["_context"] = row["context"]
        return data

    def delete_pending_task(self, task_id: int) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM pending_tasks WHERE id = ?", (task_id,))

    def add_subscription_watch(
        self,
        *,
        user_id: int,
        signature: str,
        source: str,
        reward: Decimal,
        expires_at: datetime,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO subscription_watchlist (
                    user_id, signature, source, reward, expires_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    signature,
                    source,
                    float(reward),
                    expires_at.isoformat(timespec="seconds"),
                    now_utc().isoformat(timespec="seconds"),
                ),
            )

    def get_active_subscription_watches(
        self,
        *,
        user_id: Optional[int] = None,
        limit: int = 20,
    ) -> List[sqlite3.Row]:
        query = """
            SELECT *
            FROM subscription_watchlist
            WHERE completed = 0
            ORDER BY created_at
            LIMIT ?
        """
        params: List[Any] = [limit]
        if user_id:
            query = """
                SELECT *
                FROM subscription_watchlist
                WHERE completed = 0 AND user_id = ?
                ORDER BY created_at
            """
            params = [user_id]
        with self._lock:
            cur = self._conn.execute(query, params)
            return cur.fetchall()

    def mark_watch_completed(self, watch_id: int, *, penalized: bool = False) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE subscription_watchlist
                SET completed = 1,
                    penalty_applied = CASE WHEN ? THEN 1 ELSE penalty_applied END
                WHERE id = ?
                """,
                (1 if penalized else 0, watch_id),
            )

    def update_watch_last_checked(self, watch_id: int, when: datetime) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE subscription_watchlist SET last_checked = ? WHERE id = ?",
                (when.isoformat(timespec="seconds"), watch_id),
            )

    def get_setting(self, key: str, default: Optional[str] = None) -> str:
        with self._lock:
            cur = self._conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cur.fetchone()
        if row:
            return row["value"]
        return DEFAULT_SETTINGS.get(key, default or "")

    def set_setting(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def all_user_ids(self) -> List[int]:
        with self._lock:
            cur = self._conn.execute("SELECT user_id FROM users")
            return [row["user_id"] for row in cur.fetchall()]

    def count_users(self) -> int:
        with self._lock:
            cur = self._conn.execute("SELECT COUNT(*) as c FROM users")
            return cur.fetchone()["c"]

    def count_new_users(self, since: datetime) -> int:
        with self._lock:
            cur = self._conn.execute(
                "SELECT COUNT(*) AS c FROM users WHERE datetime(created_at) >= ?",
                (since.isoformat(timespec="seconds"),),
            )
            return cur.fetchone()["c"]

    def total_earned(self) -> Decimal:
        with self._lock:
            cur = self._conn.execute("SELECT COALESCE(SUM(reward),0) as total FROM task_logs")
            return dec(cur.fetchone()["total"], "0")

    def total_completed_tasks(self) -> int:
        with self._lock:
            cur = self._conn.execute("SELECT COUNT(*) as c FROM task_logs")
            return cur.fetchone()["c"]

    def total_withdrawn_amount(self) -> Decimal:
        with self._lock:
            cur = self._conn.execute("SELECT COALESCE(SUM(withdrawn_total), 0) as total FROM users")
            return dec(cur.fetchone()["total"], "0")

    def withdrawn_amount_since(self, since: datetime) -> Decimal:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT COALESCE(SUM(amount), 0) as total
                FROM withdraw_requests
                WHERE datetime(created_at) >= datetime(?)
                """,
                (since.isoformat(timespec="seconds"),),
            )
            return dec(cur.fetchone()["total"], "0")

    def total_topups(self) -> Decimal:
        with self._lock:
            cur = self._conn.execute(
                "SELECT COALESCE(SUM(balance + withdrawn_total + COALESCE(frozen_balance,0) + COALESCE(promo_balance,0)), 0) as total FROM users"
            )
            return dec(cur.fetchone()["total"], "0")

    def create_withdraw_request(
        self,
        user_id: int,
        amount: Decimal,
        *,
        check_id: Optional[str],
        check_url: Optional[str],
        status: str = "created",
    ) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                """
                INSERT INTO withdraw_requests (user_id, amount, status, check_id, check_url, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    float(amount),
                    status,
                    check_id,
                    check_url,
                    now_utc().isoformat(timespec="seconds"),
                ),
            )
            return int(cur.lastrowid)

    def create_deposit_request(
        self,
        user_id: int,
        amount: Decimal,
        asset_amount: Decimal,
        *,
        invoice_id: str,
        invoice_url: Optional[str],
        status: str = "pending",
    ) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                """
                INSERT INTO deposit_requests (user_id, amount, asset_amount, invoice_id, invoice_url, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    float(amount),
                    float(asset_amount),
                    invoice_id,
                    invoice_url,
                    status,
                    now_utc().isoformat(timespec="seconds"),
                ),
            )
            return int(cur.lastrowid)

    def get_deposit_request(self, invoice_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM deposit_requests WHERE invoice_id = ?",
                (str(invoice_id),),
            )
            return cur.fetchone()

    def update_deposit_status(self, invoice_id: str, status: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE deposit_requests SET status = ? WHERE invoice_id = ?",
                (status, str(invoice_id)),
            )

    def has_task_completion(self, user_id: int, signature: str, context: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT 1 FROM task_logs
                WHERE user_id = ? AND signature = ? AND context = ?
                LIMIT 1
                """,
                (user_id, signature, context),
            )
            return cur.fetchone() is not None

    def referral_counts(self, user_id: int) -> Tuple[int, int]:
        with self._lock:
            cur = self._conn.execute("SELECT COUNT(*) as c FROM users WHERE referrer_id = ?", (user_id,))
            lvl1 = cur.fetchone()["c"]
            cur = self._conn.execute(
                """
                SELECT COUNT(*) as c
                FROM users
                WHERE referrer_id IN (SELECT user_id FROM users WHERE referrer_id = ?)
                """,
                (user_id,),
            )
            lvl2 = cur.fetchone()["c"]
        return lvl1, lvl2

    def add_referral_bonus(self, referrer_id: int, referred_id: int, level: int, amount: Decimal) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO referral_earnings (referrer_id, referred_id, level, amount, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    referrer_id,
                    referred_id,
                    level,
                    float(amount),
                    now_utc().isoformat(timespec="seconds"),
                ),
            )
            self._conn.execute(
                "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                (float(amount), referrer_id),
            )

    def get_required_channels(self, category: str) -> List[sqlite3.Row]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM required_channels WHERE category = ? ORDER BY id",
                (category,),
            )
            return cur.fetchall()

    def add_required_channel(self, title: str, channel_id: str, invite_link: str, category: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO required_channels (title, channel_id, invite_link, category)
                VALUES (?, ?, ?, ?)
                """,
                (title, channel_id, invite_link, category),
            )

    def remove_required_channel(self, record_id: int) -> bool:
        with self._lock, self._conn:
            cur = self._conn.execute("DELETE FROM required_channels WHERE id = ?", (record_id,))
            return cur.rowcount > 0

    def list_custom_tasks(self, placement: str) -> List[sqlite3.Row]:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT * FROM custom_tasks
                WHERE placement = ? AND is_active = 1
                ORDER BY id
                """,
                (placement,),
            )
            return cur.fetchall()

    def add_custom_task(
        self,
        *,
        placement: str,
        title: str,
        description: str,
        button_text: str,
        url: str,
        channel_id: Optional[str],
        reward: Decimal,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO custom_tasks (placement, title, description, button_text, url, channel_id, reward, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    placement,
                    title,
                    description,
                    button_text,
                    url,
                    channel_id,
                    float(reward),
                ),
            )
    
    def add_promo_task(
        self,
        *,
        creator_id: int,
        signature: str,
        title: str,
        description: str,
        url: str,
        button_text: str,
        completions: int,
        cost_per_completion: Decimal,
        total_cost: Decimal,
        channel_id: int,
        channel_username: Optional[str],
        channel_link: str,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO promo_tasks (
                    creator_id, signature, title, description, url, button_text,
                    completions, cost_per_completion, total_cost, is_active,
                    channel_id, channel_username, channel_link, completed_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, 0)
                """,
                (
                    creator_id,
                    signature,
                    title,
                    description,
                    url,
                    button_text,
                    completions,
                    float(cost_per_completion),
                    float(total_cost),
                    str(channel_id),
                    channel_username or "",
                    channel_link,
                ),
            )
    
    def list_promo_tasks(self) -> List[sqlite3.Row]:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT * FROM promo_tasks
                WHERE is_active = 1
                  AND COALESCE(completed_count, 0) < completions
                ORDER BY created_at DESC
                """
            )
            return cur.fetchall()

    def increment_promo_completion(self, signature: str) -> Tuple[int, int, bool]:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT completions, completed_count FROM promo_tasks WHERE signature = ?",
                (signature,),
            ).fetchone()
            if not row:
                return 0, 0, False
            total = row["completions"]
            current = row["completed_count"] or 0
            new_count = current + 1
            finished = new_count >= total
            self._conn.execute(
                "UPDATE promo_tasks SET completed_count = ?, is_active = CASE WHEN ? >= completions THEN 0 ELSE is_active END WHERE signature = ?",
                (new_count, new_count, signature),
            )
            return new_count, total, finished

    def remove_pending_tasks_by_signature(self, signature: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM pending_tasks WHERE signature = ?", (signature,))

    def get_user_active_promo_tasks(self, creator_id: int) -> List[sqlite3.Row]:
        """Получить активные промо-задания пользователя (не выполненные)"""
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT * FROM promo_tasks
                WHERE creator_id = ? 
                  AND is_active = 1
                  AND COALESCE(completed_count, 0) < completions
                ORDER BY created_at DESC
                """
            , (creator_id,))
            return cur.fetchall()

    def get_user_finished_promo_tasks(self, creator_id: int) -> List[sqlite3.Row]:
        """Получить завершенные промо-задания пользователя"""
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT * FROM promo_tasks
                WHERE creator_id = ?
                  AND (is_active = 0 OR COALESCE(completed_count, 0) >= completions)
                ORDER BY created_at DESC
                """
            , (creator_id,))
            return cur.fetchall()

    def deactivate_promo_task(self, task_id: int, creator_id: int) -> bool:
        """Деактивировать промо-задание (средства не возвращаются)"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE promo_tasks SET is_active = 0 WHERE id = ? AND creator_id = ?",
                (task_id, creator_id),
            )
            if cur.rowcount > 0:
                # Удаляем задание из pending_tasks всех пользователей
                task_row = self._conn.execute(
                    "SELECT signature FROM promo_tasks WHERE id = ?", (task_id,)
                ).fetchone()
                if task_row:
                    self._conn.execute(
                        "DELETE FROM pending_tasks WHERE signature = ?",
                        (task_row["signature"],)
                    )
            return cur.rowcount > 0

    def deactivate_custom_task(self, task_id: int) -> bool:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE custom_tasks SET is_active = 0 WHERE id = ?",
                (task_id,),
            )
            return cur.rowcount > 0


db = Storage(DATABASE_PATH)

def apply_env_overrides() -> None:
    overrides = {
        "flyer_api_key": os.getenv("CASHLAIT_FLYER_API_KEY"),
        "crypto_pay_token": os.getenv("CASHLAIT_CRYPTO_PAY_TOKEN"),
        "currency_symbol": os.getenv("CASHLAIT_CURRENCY_SYMBOL"),
        "flyer_task_limit": os.getenv("CASHLAIT_FLYER_TASK_LIMIT"),
        "welcome_text": os.getenv("CASHLAIT_WELCOME_TEXT"),
        "creator_contact_url": os.getenv("CREATOR_CONTACT_URL"),
        "creator_contact_label": os.getenv("CREATOR_CONTACT_LABEL"),
        "creator_contact_button_label": os.getenv("CREATOR_CONTACT_BUTTON_LABEL"),
        "creator_branding_message": os.getenv("CREATOR_BRANDING_MESSAGE"),
        "creator_branding_enabled": os.getenv("CREATOR_BRANDING_ENABLED"),
        "vip_branding_disabled": os.getenv("VIP_BRANDING_DISABLED"),
    }
    for key, value in overrides.items():
        if value is None:
            continue
        cleaned = value.strip()
        if not cleaned:
            continue
        db.set_setting(key, cleaned)

apply_env_overrides()

if BOT_TOKEN in {"", "PASTE_YOUR_TOKEN", "ВАШ_ТОКЕН_ОТ_BOTFATHER_ЗДЕСЬ"}:
    raise RuntimeError("⚠️ УКАЖИТЕ ТОКЕН БОТА! Откройте cashlait_bot.py и замените BOT_TOKEN на ваш токен от @BotFather")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
try:
    bot_info = bot.get_me()
    BOT_USERNAME = bot_info.username or "CashLait_Bot"
    BOT_ID = bot_info.id
except ApiException as exc:
    raise RuntimeError(f"Не удалось получить информацию о боте: {exc}") from exc


class FlyerAPI:
    BASE_URL = "https://api.flyerservice.io"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key.strip()
        self.session = requests.Session()

    def enabled(self) -> bool:
        return bool(self.api_key)

    def get_tasks(self, *, user_id: int, language_code: Optional[str], limit: int) -> List[Dict[str, Any]]:
        if not self.enabled():
            return []
        payload = {
            "key": self.api_key,
            "user_id": user_id,
            "limit": limit,
        }
        if language_code:
            payload["language_code"] = language_code
        log_payload = dict(payload)
        log_payload["key"] = mask_setting_value(self.api_key)
        logger.info("Flyer get_tasks request=%s", log_payload)
        response = self.session.post(
            f"{self.BASE_URL}/get_tasks",
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        raw_text = response.text
        logger.info("Flyer get_tasks response status=%s body=%s", response.status_code, raw_text)
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Flyer invalid JSON: {raw_text}") from exc
        if data.get("error"):
            raise RuntimeError(f"Flyer error: {data['error']}")
        result = data.get("result") or []
        if not isinstance(result, list):
            return []
        normalized = []
        for entry in result:
            if not isinstance(entry, dict):
                continue
            signature = entry.get("signature")
            if not signature:
                continue
            normalized.append(
                {
                    "signature": signature,
                    "task": entry.get("task") or entry.get("title") or "Задание",
                    "description": entry.get("description") or "",
                    "links": entry.get("links") or ([] if not entry.get("link") else [entry["link"]]),
                    "button_text": entry.get("button_text") or "Открыть",
                    "reward": entry.get("reward"),
                    "source": "flyer",
                }
            )
        return normalized

    def check_task(self, signature: str) -> str:
        if not self.enabled():
            raise RuntimeError("Flyer API key не задан")
        payload = {
            "key": self.api_key,
            "signature": signature,
        }
        log_payload = dict(payload)
        log_payload["key"] = mask_setting_value(self.api_key)
        logger.info("Flyer check_task request=%s", log_payload)
        response = self.session.post(
            f"{self.BASE_URL}/check_task",
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        raw_text = response.text
        logger.info("Flyer check_task response status=%s body=%s", response.status_code, raw_text)
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Flyer invalid JSON: {raw_text}") from exc
        if data.get("error"):
            raise RuntimeError(f"Flyer error: {data['error']}")
        return str(data.get("result") or "")


class CryptoPayClient:
    BASE_URL = os.getenv("CRYPTOPAY_API_URL", "https://pay.crypt.bot/api")

    def __init__(self, token: str) -> None:
        self.token = token.strip()
        if not self.token:
            raise ValueError("Crypto Pay token is empty")
        self.session = requests.Session()
        self.session.headers["Crypto-Pay-API-Token"] = self.token

    def call(self, method: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.BASE_URL}/{method}"
        response = self.session.post(url, json=payload or {}, timeout=15)
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("error", "unknown Crypto Pay error"))
        return data.get("result")

    def create_check(self, *, asset: str, amount: Decimal, pin_to_user_id: Optional[int] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "asset": asset,
            "amount": f"{amount.normalize():f}",
        }
        if pin_to_user_id:
            payload["pin_to_user_id"] = pin_to_user_id
        return self.call("createCheck", payload)

    def get_balance(self) -> List[Dict[str, Any]]:
        return self.call("getBalance")
    
    def get_exchange_rates(self) -> List[Dict[str, Any]]:
        """Получает текущие курсы обмена валют"""
        return self.call("getExchangeRates")

    def create_invoice(
        self,
        *,
        asset: str,
        amount: Decimal,
        description: str,
        currency_type: str = "crypto",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "amount": f"{amount.normalize():f}",
            "description": description,
            "currency_type": currency_type,
        }
        if currency_type == "crypto":
            payload["asset"] = asset
        else:
            payload["fiat"] = asset
        return self.call("createInvoice", payload)

    def get_invoice(self, invoice_id: str | int) -> Optional[Dict[str, Any]]:
        payload = {"invoice_ids": [int(invoice_id)]}
        result = self.call("getInvoices", payload)
        if isinstance(result, list):
            for entry in result:
                if int(entry.get("invoice_id", 0)) == int(invoice_id):
                    return entry
        return None


def get_flyer_client() -> Optional[FlyerAPI]:
    key = db.get_setting("flyer_api_key", "")
    if not key:
        return None
    return FlyerAPI(key)


def get_crypto_client() -> Optional[CryptoPayClient]:
    token = db.get_setting("crypto_pay_token", "")
    if not token:
        return None
    try:
        return CryptoPayClient(token)
    except ValueError:
        return None


def currency_symbol() -> str:
    value = db.get_setting("currency_symbol", "USDT")
    return value or "USDT"


def get_effective_asset_rate(asset: str) -> Decimal:
    """
    Получает курс актива к USDT через Crypto Pay API.
    Если не удается получить курс, возвращает 1.0 (как fallback).
    
    Args:
        asset: Код актива (USDT, TON, BTC, и т.д.)
    
    Returns:
        Decimal: Курс актива к USDT (сколько USDT стоит 1 единица актива)
    """
    # Если актив сам USDT, курс = 1
    if asset == "USDT":
        return Decimal("1.0")
    
    crypto = get_crypto_client()
    if not crypto:
        logger.warning("Crypto Pay клиент не настроен, используется курс 1.0")
        return Decimal("1.0")
    
    try:
        rates = crypto.get_exchange_rates()
        # Ищем курс актива к USD
        for rate_item in rates:
            if rate_item.get("source") == asset and rate_item.get("target") == "USD":
                rate_value = rate_item.get("rate")
                if rate_value and rate_item.get("is_valid"):
                    rate_decimal = dec(rate_value, "1.0")
                    logger.info(f"Получен курс {asset}/USD: {rate_decimal}")
                    return rate_decimal
        
        # Если не нашли прямой курс, пробуем обратный (USD к активу)
        for rate_item in rates:
            if rate_item.get("source") == "USD" and rate_item.get("target") == asset:
                rate_value = rate_item.get("rate")
                if rate_value and rate_item.get("is_valid"):
                    rate_decimal = dec(rate_value, "1.0")
                    if rate_decimal > 0:
                        inverse_rate = Decimal("1.0") / rate_decimal
                        logger.info(f"Получен обратный курс USD/{asset}: {rate_decimal}, инвертирован в {inverse_rate}")
                        return inverse_rate
        
        logger.warning(f"Курс для {asset} не найден в API, используется fallback 1.0")
        return Decimal("1.0")
        
    except Exception as exc:
        logger.error(f"Ошибка получения курса через Crypto Pay API: {exc}")
        return Decimal("1.0")


def get_menu_button_text(key: str) -> str:
    return db.get_setting(key, DEFAULT_SETTINGS.get(key, ""))


def get_task_reward_amount() -> Decimal:
    """
    Возвращает актуальную награду за выполнение одного задания.
    Предпочтительно берёт значение из task_reward, но сохраняет обратную совместимость
    с устаревшим ключом cashlait_task_price.
    """
    value = db.get_setting("task_reward", DEFAULT_SETTINGS.get("task_reward", "1.0"))
    if not value:
        value = db.get_setting("cashlait_task_price", DEFAULT_SETTINGS.get("task_reward", "1.0"))
    return dec(value or DEFAULT_SETTINGS.get("task_reward", "1.0"), DEFAULT_SETTINGS.get("task_reward", "1.0"))


def get_task_price_amount() -> Decimal:
    """
    Возвращает стоимость для рекламодателя за одно выполнение.
    Основной источник — task_price_per_completion, с fallback к устаревшему ключу
    cashlait_task_price и, при необходимости, к текущей награде исполнителю.
    """
    default_price = DEFAULT_SETTINGS.get("task_price_per_completion", DEFAULT_SETTINGS.get("task_reward", "1.0"))
    value = db.get_setting("task_price_per_completion", default_price)
    if not value:
        value = db.get_setting("cashlait_task_price", default_price)
    if not value:
        value = db.get_setting("task_reward", default_price)
    return dec(value or default_price, DEFAULT_SETTINGS.get("task_reward", "1.0"))


def build_main_keyboard(user_id: Optional[int] = None) -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.row(types.KeyboardButton(get_menu_button_text("menu_btn_cabinet")))
    kb.row(types.KeyboardButton(get_menu_button_text("menu_btn_tasks")))
    kb.row(
        types.KeyboardButton(get_menu_button_text("menu_btn_promo")),
        types.KeyboardButton(get_menu_button_text("menu_btn_referrals")),
    )
    kb.row(types.KeyboardButton(get_menu_button_text("menu_btn_info")))
    if user_id and user_id in ADMIN_IDS:
        kb.row(types.KeyboardButton(get_menu_button_text("menu_btn_admin")))
    return kb


def normalize_button_text(value: str) -> str:
    return (value or "").strip().lower()


MENU_BUTTON_SYNONYMS: Dict[str, List[str]] = {
    "menu_btn_cabinet": ["кабинет", "личный кабинет"],
    "menu_btn_tasks": ["задания", "задание", "tasks"],
    "menu_btn_promo": ["продвижение", "реклама"],
    "menu_btn_referrals": ["рефералы", "рефералки", "referrals"],
    "menu_btn_info": ["инфо", "о боте", "информация"],
    "menu_btn_admin": ["админ", "админка", "/admin"],
}


def resolve_menu_button_key(text: str) -> Optional[str]:
    normalized = normalize_button_text(text)
    if not normalized:
        return None
    for key in BUTTON_SETTING_FIELDS:
        candidates = {normalize_button_text(get_menu_button_text(key))}
        for synonym in MENU_BUTTON_SYNONYMS.get(key, []):
            candidates.add(normalize_button_text(synonym))
        if normalized in candidates:
            return key
    return None


def build_subscription_markup(channels: List[sqlite3.Row], category: str) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup(row_width=1)
    for channel in channels:
        title = channel["title"]
        invite = channel["invite_link"] or f"https://t.me/{channel['channel_id'].lstrip('@')}"
        markup.add(
            types.InlineKeyboardButton(
                f"📢 {title}",
                url=invite,
            )
        )
    markup.add(
        types.InlineKeyboardButton(
            "✅ Проверить",
            callback_data=f"check_sub:{category}",
        )
    )
    return markup


def check_subscription(
    *,
    user_id: int,
    chat_id: int,
    category: str,
    notify: bool = True,
) -> bool:
    if category == "global":
        channels = db.get_required_channels("global")
    else:
        channels = db.get_required_channels(category)
    if not channels:
        return True
    missing: List[sqlite3.Row] = []
    for channel in channels:
        channel_id = channel["channel_id"]
        try:
            member = bot.get_chat_member(channel_id, user_id)
            if member.status in ("left", "kicked"):
                missing.append(channel)
        except ApiException as exc:
            logger.warning("Cannot verify subscription %s for user %s: %s", channel_id, user_id, exc)
            missing.append(channel)
    if missing and notify:
        text_lines = [
            "📢 <b>Обязательная подписка</b>",
            "",
            "Для доступа к разделу необходимо подписаться на каналы:",
        ]
        for channel in missing:
            text_lines.append(f"• {channel['title']}")
        text_lines.append("")
        text_lines.append("После подписки нажмите «✅ Проверить».")
        markup = build_subscription_markup(missing, category)
        bot.send_message(chat_id, "\n".join(text_lines), reply_markup=markup)
        return False
    return not missing


user_states: Dict[int, Dict[str, Any]] = {}


def parse_start_payload(text: str) -> Optional[int]:
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return None
    payload = parts[1].strip()
    if payload.startswith("start="):
        payload = payload.split("=", 1)[1]
    payload = payload.strip()
    if payload.isdigit():
        return int(payload)
    return None


def ensure_member(message: types.Message, referrer_id: Optional[int] = None) -> sqlite3.Row:
    ref = None
    if referrer_id and referrer_id != message.from_user.id:
        ref = referrer_id
    user = db.ensure_user(message.from_user, ref)
    if ref:
        db.set_referrer_if_empty(message.from_user.id, ref)
    row = db.get_user(message.from_user.id)
    process_subscription_watchlist(row["user_id"])
    return row


def ensure_user_row(tg_user: telebot.types.User) -> sqlite3.Row:
    db.ensure_user(tg_user)
    row = db.get_user(tg_user.id)
    process_subscription_watchlist(row["user_id"])
    return row


def get_or_refresh_tasks(user: sqlite3.Row, context: str, *, force: bool = False) -> List[Tuple[int, Dict[str, Any]]]:
    normalized_context = "tasks"
    user_id = int(user["user_id"])
    cached = db.list_pending_tasks(user_id, normalized_context)
    if cached and not force:
        return cached

    tasks: List[Dict[str, Any]] = []
    reward_per_task = get_task_reward_amount()
    limit = max(1, int(db.get_setting("flyer_task_limit", "5") or 5))

    language_code = None
    if isinstance(user, sqlite3.Row):
        if "language_code" in user.keys():
            language_code = user["language_code"]
    def is_already_completed(signature: Optional[str]) -> bool:
        if not signature:
            return False
        return db.has_task_completion(user_id, signature, normalized_context)
    flyer = get_flyer_client()
    if flyer and flyer.enabled():
        try:
            flyer_tasks = flyer.get_tasks(
                user_id=user_id,
                language_code=language_code,
                limit=limit,
            )
            for entry in flyer_tasks:
                links = entry.get("links") or []
                url = links[0] if links else entry.get("url")
                if not url:
                    continue
                signature = entry.get("signature")
                if not signature or is_already_completed(signature):
                    continue
                tasks.append(
                    {
                        "signature": signature,
                        "title": entry.get("task") or "Задание",
                        "description": entry.get("description") or "",
                        "url": url,
                        "button_text": entry.get("button_text") or "Открыть",
                        "payout": str(reward_per_task),
                        "source": "flyer",
                    }
                )
        except Exception as exc:
            logger.warning("Flyer get_tasks failed for user %s: %s", user_id, exc)

    placement = "tasks"
    for row in db.list_custom_tasks(placement):
        custom_signature = f"custom:{placement}:{row['id']}"
        if is_already_completed(custom_signature):
            continue
        custom_reward = dec(row["reward"], f"{reward_per_task}")
        tasks.append(
            {
                "signature": custom_signature,
                "title": row["title"],
                "description": row["description"] or "",
                "url": row["url"],
                "button_text": row["button_text"],
                "channel_id": row["channel_id"],
                "payout": str(custom_reward),
                "source": "custom",
            }
        )
    
    # Добавляем промо-задания как Flyer задания
    for row in db.list_promo_tasks():
        channel_id = row_get(row, "channel_id")
        if channel_id is not None:
            try:
                channel_id = int(channel_id)
            except (TypeError, ValueError):
                pass
        promo_signature = row["signature"]
        if is_already_completed(promo_signature):
            continue
        tasks.append(
            {
                "signature": promo_signature,
                "promo_signature": promo_signature,
                "title": row["title"],
                "description": row["description"] or "",
                "url": row_get(row, "channel_link", row["url"]),
                "button_text": row["button_text"] or "Перейти",
                "payout": str(dec(str(row["cost_per_completion"]), "0.1")),
                "channel_id": channel_id,
                "source": "promo",
            }
        )

    db.save_tasks(user_id, normalized_context, tasks)
    return db.list_pending_tasks(user_id, normalized_context)


def build_tasks_summary(
    user: sqlite3.Row,
    context: str,
    *,
    force: bool = False,
) -> Tuple[str, types.InlineKeyboardMarkup]:
    rows = get_or_refresh_tasks(user, context, force=force)
    sym = currency_symbol()
    context_key = "tasks"
    if not rows:
        lines = [
            "🧑 Нет доступных заданий — вы уже подписаны на все продвигаемые каналы.",
            "",
            "Попробуйте позже: новые задания появляются автоматически.",
        ]
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("🔄 Обновить", callback_data=f"tasks:refresh_summary:{context_key}"))
        return "\n".join(lines), markup

    total_reward = sum(dec(task.get("payout"), "0") for _, task in rows)
    lines = [
        f"📝 Доступных заданий: {len(rows)}",
        "________________",
        "",
        f"🪙 Можно заработать: {format_amount(total_reward, sym)}",
    ]
    first_url = rows[0][1].get("url") if rows else None
    markup = types.InlineKeyboardMarkup(row_width=1)
    if first_url:
        markup.add(types.InlineKeyboardButton("➡️ Перейти", url=first_url))
    markup.add(types.InlineKeyboardButton("🔎 Проверить все задания", callback_data=f"tasks:details:{context_key}"))
    markup.add(types.InlineKeyboardButton("🔄 Обновить", callback_data=f"tasks:refresh_summary:{context_key}"))
    return "\n".join(lines), markup


def build_task_details_message(
    user: sqlite3.Row,
    context: str,
    *,
    with_refresh: bool = True,
    force: bool = False,
) -> Tuple[str, types.InlineKeyboardMarkup]:
    rows = get_or_refresh_tasks(user, context, force=force)
    if not rows:
        return build_tasks_summary(user, context, force=False)

    sym = currency_symbol()
    lines: List[str] = []
    lines.append("📋 Все задания")
    lines.append("────────────────")
    lines.append("")

    for idx, (_, task) in enumerate(rows, start=1):
        payout = format_amount(dec(task.get("payout"), "0"), sym)
        lines.append(f"{idx}. {task.get('title', 'Задание')} — {payout}")

    lines.append("")
    lines.append("После выполнения вернитесь и нажмите кнопку проверки напротив задания.")

    markup = types.InlineKeyboardMarkup(row_width=1)
    for idx, (row_id, task) in enumerate(rows, start=1):
        if task.get("url"):
            markup.add(
                types.InlineKeyboardButton(
                    f"➡️ Перейти №{idx}",
                    url=task.get("url"),
                )
            )
        markup.add(
            types.InlineKeyboardButton(
                f"✅ Проверить №{idx}",
                callback_data=f"taskcheck:{context}:{row_id}",
            )
        )
        markup.add(
            types.InlineKeyboardButton(
                f"⏭ Пропустить №{idx}",
                callback_data=f"tasks:skip:{context}:{row_id}",
            )
        )
        # Добавляем кнопку "Следующее задание" если есть еще задания (только для первого задания)
        if idx == 1 and len(rows) > 1:
            next_row_id = rows[1][0]
            markup.add(
                types.InlineKeyboardButton(
                    f"⏭ Следующее задание",
                    callback_data=f"tasks:next:{context}:{next_row_id}",
                )
            )

    if with_refresh:
        markup.add(types.InlineKeyboardButton("🔄 Обновить", callback_data=f"tasks:refresh:{context}"))
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"tasks:summary:{context}"))
    return "\n".join(lines), markup


def process_subscription_watchlist(user_id: Optional[int] = None) -> None:
    """Проверка подписок каждые 10 минут для заданий от Flyer API"""
    flyer = get_flyer_client()
    if not flyer:
        return
    entries = db.get_active_subscription_watches(user_id=user_id)
    if not entries:
        return
    now = now_utc()
    for entry in entries:
        watch_id = entry["id"]
        try:
            expires_at = datetime.fromisoformat(entry["expires_at"])
            created_at = datetime.fromisoformat(entry["created_at"])
        except ValueError:
            expires_at = now
            created_at = now
        
        # Проверяем, прошло ли 3 дня с момента создания
        days_passed = (now - created_at).days
        
        # Если прошло 3 дня и статус успешный - переводим с frozen_balance на основной баланс
        if days_passed >= 3:
            try:
                status = str(flyer.check_task(entry["signature"]) or "").lower()
                # Если статус успешный (не в списке неудачных и не отписался)
                if status not in FLYER_FAIL_STATUSES and not any(token in status for token in FLYER_PENALTY_STATUSES):
                    reward = dec(entry["reward"], "0")
                    if reward > 0:
                        # Переводим с frozen_balance на основной баланс
                        db.update_user_balance(entry["user_id"], delta_frozen_balance=-reward, delta_balance=reward)
                        db.add_task_log(entry["user_id"], entry["signature"], entry["source"], "frozen_to_balance", reward)
                        db.mark_watch_completed(watch_id)
                        try:
                            bot.send_message(
                                entry["user_id"],
                                f"✅ Средства за задание переведены на основной баланс ({format_amount(reward, currency_symbol())}).",
                            )
                        except ApiException as exc:
                            logger.debug("Не удалось отправить уведомление о переводе: %s", exc)
                    continue
            except Exception as exc:
                logger.debug("Не удалось проверить статус задания %s: %s", entry["signature"], exc)
        
        # Если срок истек - завершаем проверку
        if now >= expires_at:
            db.mark_watch_completed(watch_id)
            continue
        
        # Проверяем не чаще чем раз в 10 минут
        last_checked = entry["last_checked"]
        if last_checked:
            try:
                last_dt = datetime.fromisoformat(last_checked)
            except ValueError:
                last_dt = now - timedelta(days=1)
            if now - last_dt < timedelta(minutes=10):
                continue
        
        # Проверяем статус задания
        try:
            status = str(flyer.check_task(entry["signature"]) or "").lower()
        except Exception as exc:
            logger.debug("Не удалось проверить подписку %s: %s", entry["signature"], exc)
            continue
        
        db.update_watch_last_checked(watch_id, now)
        
        # Если отписался - списываем с frozen_balance (удаляем средства)
        if any(token in status for token in FLYER_PENALTY_STATUSES):
            reward = dec(entry["reward"], "0")
            if reward > 0:
                # Списываем с frozen_balance (удаляем средства)
                db.update_user_balance(entry["user_id"], delta_frozen_balance=-reward)
                db.add_task_log(entry["user_id"], entry["signature"], entry["source"], "penalty", -reward)
            db.mark_watch_completed(watch_id, penalized=True)
            try:
                bot.send_message(
                    entry["user_id"],
                    "⚠️ Вы отписались. Средства за задание списаны с удержания.",
                )
            except ApiException as exc:
                logger.debug("Не удалось отправить уведомление о штрафе: %s", exc)


def send_main_screen(chat_id: int, user_id: Optional[int] = None) -> None:
    try:
        text = db.get_setting("welcome_text", DEFAULT_SETTINGS["welcome_text"])
        bot.send_message(chat_id, text, reply_markup=build_main_keyboard(user_id))
        send_creator_branding_banner(chat_id)
        logger.debug(f"Главный экран отправлен в чат {chat_id}")
    except Exception as e:
        logger.error(f"Ошибка при отправке главного экрана в чат {chat_id}: {e}", exc_info=True)
        try:
            bot.send_message(chat_id, "Добро пожаловать! Используйте меню ниже.", reply_markup=build_main_keyboard(user_id))
            send_creator_branding_banner(chat_id)
        except:
            pass


def send_flyer_logs(chat_id: int) -> None:
    try:
        with open(LOG_FILE_PATH, "r", encoding="utf-8") as log_file:
            flyer_lines = [line for line in log_file if "Flyer" in line]
    except FileNotFoundError:
        bot.send_message(chat_id, "Файл логов не найден.")
        return
    if not flyer_lines:
        bot.send_message(chat_id, "Логи Flyer отсутствуют.")
        return
    tail = flyer_lines[-500:]
    buffer = BytesIO("".join(tail).encode("utf-8"))
    buffer.name = "flyer_logs.txt"
    bot.send_document(chat_id, buffer, caption="Ответы Flyer (последние записи)")


def admin_menu_markup() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("💵 Награда исполнителю", callback_data="admin:set:task_reward"),
        types.InlineKeyboardButton("💰 Цена для рекламодателя", callback_data="admin:set:task_price_per_completion"),
    )
    kb.add(
        types.InlineKeyboardButton("⚙️ Настройки", callback_data="admin:settings"),
        types.InlineKeyboardButton("✈️ Flyer", callback_data="admin:flyer"),
    )
    kb.add(types.InlineKeyboardButton("📝 ОП Задания", callback_data="admin:custom:tasks"))
    kb.add(
        types.InlineKeyboardButton("📢 Рассылка", callback_data="admin:broadcast"),
        types.InlineKeyboardButton("💸 Резерв", callback_data="admin:reserve"),
    )
    kb.add(types.InlineKeyboardButton("💰 Балансы пользователей", callback_data="admin:balances"))
    kb.add(types.InlineKeyboardButton("🎛 Кнопки меню", callback_data="admin:buttons"))
    kb.add(types.InlineKeyboardButton("🔗 Ссылки инфо", callback_data="admin:links"))
    kb.add(
        types.InlineKeyboardButton("📣 Обязательные подписки", callback_data="admin:required"),
        types.InlineKeyboardButton("📡 Канал выплат", callback_data="admin:payout_channel"),
    )
    kb.add(types.InlineKeyboardButton("🪁 Логи Flyer", callback_data="admin:flyerlogs"))
    return kb


def send_admin_menu(chat_id: int) -> None:
    bot.send_message(chat_id, "🔐 Админ-панель", reply_markup=admin_menu_markup())


def admin_update_message(call: types.CallbackQuery, text: str, markup: Optional[types.InlineKeyboardMarkup] = None) -> None:
    try:
        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )
    except ApiException as exc:
        logger.debug("Не удалось обновить админское сообщение: %s", exc)
        bot.send_message(call.message.chat.id, text, reply_markup=markup)


def show_admin_settings(call: types.CallbackQuery) -> None:
    lines = ["⚙️ Общие настройки", ""]
    for key, (label, _) in ADMIN_SETTING_FIELDS.items():
        value = db.get_setting(key, DEFAULT_SETTINGS.get(key, ""))
        lines.append(f"{label}: <code>{setting_display(key, value)}</code>")
    lines.append("")
    lines.append("Выберите значение для изменения.")
    kb = types.InlineKeyboardMarkup(row_width=2)
    for key, (label, _) in ADMIN_SETTING_FIELDS.items():
        kb.add(types.InlineKeyboardButton(label.split(" (")[0], callback_data=f"admin:set:{key}"))
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu"))
    admin_update_message(call, "\n".join(lines), kb)
    bot.answer_callback_query(call.id)


def show_balance_menu(call: types.CallbackQuery) -> None:
    lines = [
        "💰 Управление балансами пользователей",
        "",
        "Выберите действие:",
        "• ➕ начисляет средства",
        "• ➖ списывает средства",
        "",
        "После выбора укажите ID пользователя и сумму через пробел.",
    ]
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("➕ Основной", callback_data="admin:balance:add:main"),
        types.InlineKeyboardButton("➖ Основной", callback_data="admin:balance:deduct:main"),
    )
    kb.add(
        types.InlineKeyboardButton("➕ Рекламный", callback_data="admin:balance:add:promo"),
        types.InlineKeyboardButton("➖ Рекламный", callback_data="admin:balance:deduct:promo"),
    )
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu"))
    admin_update_message(call, "\n".join(lines), kb)
    bot.answer_callback_query(call.id)


def start_balance_adjust(call: types.CallbackQuery, operation: str, balance_type: str) -> None:
    operation_names = {
        "add": "Начисление",
        "deduct": "Списание",
    }
    balance_names = {
        "main": "основного",
        "promo": "рекламного",
    }
    if operation not in operation_names or balance_type not in balance_names:
        bot.answer_callback_query(call.id, "Неверное действие", show_alert=True)
        return
    user_states[call.from_user.id] = {
        "mode": "admin_balance_adjust",
        "operation": operation,
        "balance_type": balance_type,
    }
    bot.answer_callback_query(call.id)
    bot.send_message(
        call.message.chat.id,
        f"{operation_names[operation]} {balance_names[balance_type]} баланса.\n"
        "Отправьте ID пользователя и сумму через пробел (например: <code>123456789 10</code>).",
        parse_mode="HTML",
        reply_markup=admin_cancel_markup(),
    )


def show_flyer_settings(call: types.CallbackQuery) -> None:
    lines = ["✈️ Flyer настройки", ""]
    for key, (label, _) in FLYER_SETTING_FIELDS.items():
        value = db.get_setting(key, DEFAULT_SETTINGS.get(key, ""))
        lines.append(f"{label}: <code>{setting_display(key, value)}</code>")
    kb = types.InlineKeyboardMarkup(row_width=1)
    for key, (label, _) in FLYER_SETTING_FIELDS.items():
        kb.add(types.InlineKeyboardButton(label, callback_data=f"admin:flyerset:{key}"))
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu"))
    admin_update_message(call, "\n".join(lines), kb)
    bot.answer_callback_query(call.id)


def show_custom_tasks_menu(call: types.CallbackQuery, placement: str) -> None:
    if placement != "tasks":
        placement = "tasks"
    rows = db.list_custom_tasks(placement)
    title = "📝 ОП Задания"
    lines = [title, ""]
    if not rows:
        lines.append("Пока нет собственных заданий.")
    else:
        default_reward = get_task_reward_amount()
        sym = currency_symbol()
        for row in rows:
            raw_reward = row_get(row, "reward")
            reward_value = default_reward if raw_reward in (None, "") else dec(raw_reward, f"{default_reward}")
            lines.append(f"#{row['id']} — {row['title']} ({format_amount(reward_value, sym)})")
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("➕ Добавить", callback_data=f"admin:customadd:{placement}"),
        types.InlineKeyboardButton("🗑 Удалить", callback_data=f"admin:customdel:{placement}"),
    )
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu"))
    admin_update_message(call, "\n".join(lines), kb)
    bot.answer_callback_query(call.id)


def show_required_channels_menu(call: types.CallbackQuery) -> None:
    categories = {
        "global": "Старт",
        "tasks": "ОП Задания",
    }
    lines = ["📣 Обязательные подписки", ""]
    for key, label in categories.items():
        count = len(db.get_required_channels(key))
        lines.append(f"{label}: {count}")
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("➕ Старт", callback_data="admin:requiredadd:global"),
        types.InlineKeyboardButton("➕ Задания", callback_data="admin:requiredadd:tasks"),
    )
    kb.add(types.InlineKeyboardButton("📋 Список", callback_data="admin:requiredlist"))
    kb.add(types.InlineKeyboardButton("🗑 Удалить", callback_data="admin:requireddel"))
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu"))
    admin_update_message(call, "\n".join(lines), kb)
    bot.answer_callback_query(call.id)


def show_required_channels_list(call: types.CallbackQuery) -> None:
    rows = db.get_required_channels("global") + db.get_required_channels("tasks")
    if not rows:
        text = "Список пуст."
    else:
        text_lines = ["📋 Список каналов", ""]
        for row in rows:
            text_lines.append(
                f"#{row['id']} [{row['category']}] {row['title']} — {row['channel_id']}"
            )
        text = "\n".join(text_lines)
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, text)


def show_link_settings(call: types.CallbackQuery) -> None:
    lines = ["🔗 Ссылки раздела «Инфо»", ""]
    for key, (label, _) in INFO_LINK_FIELDS.items():
        value = db.get_setting(key, DEFAULT_SETTINGS.get(key, ""))
        lines.append(f"{label}: <code>{setting_display(key, value)}</code>")
    kb = types.InlineKeyboardMarkup(row_width=1)
    for key, (label, _) in INFO_LINK_FIELDS.items():
        kb.add(types.InlineKeyboardButton(label, callback_data=f"admin:linkset:{key}"))
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu"))
    admin_update_message(call, "\n".join(lines), kb)
    bot.answer_callback_query(call.id)

def show_button_settings(call: types.CallbackQuery) -> None:
    lines = ["🎛 Текст кнопок меню", ""]
    for key, (label, _) in BUTTON_SETTING_FIELDS.items():
        value = db.get_setting(key, DEFAULT_SETTINGS.get(key, ""))
        lines.append(f"{label}: <code>{setting_display(key, value)}</code>")
    kb = types.InlineKeyboardMarkup(row_width=1)
    for key, (label, _) in BUTTON_SETTING_FIELDS.items():
        kb.add(types.InlineKeyboardButton(label, callback_data=f"admin:buttonset:{key}"))
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu"))
    admin_update_message(call, "\n".join(lines), kb)
    bot.answer_callback_query(call.id)


def send_personal_cabinet(user: sqlite3.Row, chat_id: int) -> None:
    sym = currency_symbol()
    balance = dec(user["balance"], "0")
    withdrawn = dec(user["withdrawn_total"], "0")
    completed = int(user["completed_tasks"] or 0)
    frozen = dec(row_get(user, "frozen_balance", "0"), "0")
    promo_balance = dec(row_get(user, "promo_balance", "0"), "0")
    username = user["username"] or ""
    username_display = f"@{username}" if username else "—"
    text = "\n".join(
        [
            "📱 Ваш кабинет:",
            "━━━━━━━━━━━━━━━━",
            "",
            f"👤 Пользователь: {username_display}",
            f"📋 Выполнено заданий: {completed}",
            "────────────────",
            "",
            f"💳 Баланс для вывода: {format_amount(balance, sym)}",
            f"❄️ Замороженный баланс: {format_amount(frozen, sym)}",
            f"📢 Рекламный баланс: {format_amount(promo_balance, sym)}",
            "",
            f"💲 Всего выведено: {format_amount(withdrawn, sym)}",
            "────────────────",
        ]
    )
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("💳 Пополнить", callback_data="cabinet:deposit"),
        types.InlineKeyboardButton("💸 Вывести", callback_data="withdraw:start"),
    )
    markup.add(types.InlineKeyboardButton("♻️ Конвертировать", callback_data="cabinet:convert"))
    bot.send_message(chat_id, text, reply_markup=markup)


def send_tasks_section(message: types.Message, user: sqlite3.Row) -> None:
    if not check_subscription(user_id=user["user_id"], chat_id=message.chat.id, category="global"):
        return
    if not check_subscription(user_id=user["user_id"], chat_id=message.chat.id, category="tasks", notify=False):
        # show tasks-specific subscription as tasks disguised
        check_subscription(user_id=user["user_id"], chat_id=message.chat.id, category="tasks", notify=True)
        return
    text, markup = build_tasks_summary(user, "tasks")
    bot.send_message(message.chat.id, text, reply_markup=markup)


def send_referrals_section(user: sqlite3.Row, chat_id: int) -> None:
    lvl1, lvl2 = db.referral_counts(user["user_id"])
    ref_link = f"https://t.me/{BOT_USERNAME}?start={user['user_id']}"
    lvl1_percent = db.get_setting("ref_percent_level1", "15")
    lvl2_percent = db.get_setting("ref_percent_level2", "5")
    text = "\n".join(
        [
            "🎯 Реферальная система",
            "",
            "━━━━━━━━━━━━━━━━━",
            "",
            f"👥 Ваших рефералов 1 уровня - {lvl1}",
            f"👥 Ваших рефералов 2 уровня - {lvl2}",
            "",
            "────────────────",
            "",
            "🎁 Бонусы:",
            f"╰• 1 ур. — {lvl1_percent}% с их выводов",
            f"╰• 2 ур. — {lvl2_percent}% с их выводов",
            "",
            "────────────────",
            "",
            "🔗 Ссылка для приглашения:",
            ref_link,
        ]
    )
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("📤 Поделиться", url=f"https://t.me/share/url?url={ref_link}")
    )
    bot.send_message(chat_id, text, reply_markup=markup, disable_web_page_preview=True)


def send_promotion_section(user: sqlite3.Row, chat_id: int) -> None:
    promo_balance = dec(row_get(user, "promo_balance", "0"), "0")
    task_price = get_task_price_amount()
    min_completions = int(db.get_setting("cashlait_min_completions", "10") or 10)
    
    # Рассчитываем, на сколько выполнений хватит баланса
    completions_available = int(promo_balance / task_price) if task_price > 0 else 0
    
    # Получаем реальные значения активных и завершенных заданий
    active_tasks = db.get_user_active_promo_tasks(user["user_id"])
    finished_tasks = db.get_user_finished_promo_tasks(user["user_id"])
    active_count = len(active_tasks)
    finished_count = len(finished_tasks)
    
    text = "\n".join(
        [
            "📣 Продвижение каналов",
            "",
            "Наш бот предлагает вам возможность создать задание на подписку вашего Telegram-канала реальными людьми.",
            f"💵 1 выполнение — {format_amount(task_price, currency_symbol())}",
            f"📊 Минимальное количество выполнений: {min_completions}",
            f"💼 Рекламный баланс — {format_amount(promo_balance, currency_symbol())}",
            f"ℹ️ Его хватит на {completions_available} выполнений.",
            "",
            f"🕒 Активных заказов: {active_count}",
            f"✅ Завершённых заказов: {finished_count}",
            "",
            "❗️ Наш бот должен быть администратором продвигаемого объекта!",
        ]
    )
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("➕ Создать задание", callback_data="promo:create"),
        types.InlineKeyboardButton("📈 Активные", callback_data="promo:active"),
    )
    markup.add(
        types.InlineKeyboardButton("✅ Завершённые", callback_data="promo:finished"),
        types.InlineKeyboardButton("⚙️ Управление", callback_data="promo:manage"),
    )
    bot.send_message(chat_id, text, reply_markup=markup)


def send_about_section(chat_id: int) -> None:
    total_users = db.count_users()
    new_users = db.count_new_users(now_utc() - timedelta(hours=24))
    total_tasks = db.total_completed_tasks()
    sym = currency_symbol()
    total_withdrawn = db.total_withdrawn_amount()
    withdrawn_today = db.withdrawn_amount_since(now_utc() - timedelta(hours=24))
    total_topups = db.total_topups()
    text = "\n".join(
        [
            "📚 Информация о нашем боте:",
            "────────────────",
            f"👥 Пользователей всего: {total_users}",
            f"👥 За сегодня: {new_users}",
            "────────────────",
            f"📋 Выполнено заданий: {total_tasks}",
            "────────────────",
            f"💸 Выведено всего: {format_amount(total_withdrawn, sym)}",
            f"💸 За сегодня: {format_amount(withdrawn_today, sym)}",
            "────────────────",
            f"📢 Пополнено средств: {format_amount(total_topups, sym)}",
            "────────────────",
            "📈 Статистика обновляется в реальном времени.",
        ]
    )
    markup = types.InlineKeyboardMarkup(row_width=2)

    def add_info_button(label: str, setting_key: str, fallback: str) -> None:
        url = db.get_setting(setting_key, DEFAULT_SETTINGS.get(setting_key, ""))
        if url:
            markup.add(types.InlineKeyboardButton(label, url=url))
        else:
            markup.add(types.InlineKeyboardButton(label, callback_data=f"info:{fallback}"))

    add_info_button("❓ Помощь", "info_help_url", "help")
    add_info_button("📣 Новости", "info_news_url", "news")
    add_info_button("💬 Чат", "info_chat_url", "chat")
    brand_button = build_creator_branding_button()
    if brand_button:
        markup.add(brand_button)
    bot.send_message(chat_id, text, reply_markup=markup)


def apply_referral_bonuses(user: sqlite3.Row, withdraw_amount: Decimal) -> None:
    level1_id = user["referrer_id"]
    if not level1_id:
        return
    percent1 = dec(db.get_setting("ref_percent_level1", "15")) / Decimal("100")
    percent2 = dec(db.get_setting("ref_percent_level2", "5")) / Decimal("100")
    bonus1 = (withdraw_amount * percent1).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
    if bonus1 > 0:
        db.add_referral_bonus(level1_id, user["user_id"], 1, bonus1)
    try:
        level1_user = db.get_user(level1_id)
        level2_id = level1_user["referrer_id"]
    except ValueError:
        level2_id = None
    if level2_id:
        bonus2 = (withdraw_amount * percent2).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
        if bonus2 > 0:
            db.add_referral_bonus(level2_id, user["user_id"], 2, bonus2)


def verify_custom_task(task: Dict[str, Any], user_id: int) -> Tuple[bool, str]:
    channel_id = task.get("channel_id")
    if channel_id:
        try:
            channel_id = int(channel_id)
        except (TypeError, ValueError):
            pass
        try:
            member = bot.get_chat_member(channel_id, user_id)
            if member.status in ("left", "kicked"):
                return False, "Подпишитесь на канал и попробуйте снова."
        except ApiException as exc:
            logger.warning("Не удалось проверить подписку на %s: %s", channel_id, exc)
            return False, "Не удалось проверить подписку. Попробуйте позже."
    return True, ""


def notify_withdrawal(user: sqlite3.Row, amount: Decimal, check_url: str) -> None:
    channel_raw = db.get_setting("payout_notify_channel", "")
    if not channel_raw:
        return
    channel = parse_chat_identifier(channel_raw)
    if not channel:
        return
    text = "\n".join(
        [
            "💸 <b>Новая выплата</b>",
            f"Сумма: {format_amount(amount, currency_symbol())}",
            f"Пользователь: <code>{user['user_id']}</code>",
            f"Юзернейм: @{user['username']}" if user["username"] else "Юзернейм: —",
            "",
            f"Чек: {check_url}",
        ]
    )
    try:
        bot.send_message(channel, text)
    except ApiException as exc:
        logger.error("Не удалось отправить уведомление о выплате: %s", exc)


def start_withdrawal(call: types.CallbackQuery, user: sqlite3.Row) -> None:
    min_withdraw = dec(db.get_setting("min_withdraw", "3"))
    balance = dec(user["balance"], "0")
    if balance < min_withdraw:
        bot.answer_callback_query(
            call.id,
            f"Минимальная сумма {format_amount(min_withdraw, currency_symbol())}",
            show_alert=True,
        )
        return
    user_states[user["user_id"]] = {"mode": "withdraw_amount"}
    bot.answer_callback_query(call.id)
    bot.send_message(
        call.message.chat.id,
        f"Введите сумму вывода (мин. {format_amount(min_withdraw, currency_symbol())}).",
    )


def process_withdraw_amount(message: types.Message, user: sqlite3.Row) -> None:
    text = (message.text or "").replace(",", ".").strip()
    try:
        amount = Decimal(text)
    except InvalidOperation:
        bot.reply_to(message, "Введите корректное число.")
        return
    min_withdraw = dec(db.get_setting("min_withdraw", "3"))
    balance = dec(user["balance"], "0")
    if amount < min_withdraw:
        bot.reply_to(message, f"Минимальная сумма вывода {format_amount(min_withdraw, currency_symbol())}.")
        return
    if amount > balance:
        bot.reply_to(message, "Недостаточно средств на балансе.")
        return
    amount = amount.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
    crypto = get_crypto_client()
    if not crypto:
        bot.reply_to(message, "Платёжная система временно недоступна. Попробуйте позже.")
        return
    asset = db.get_setting("crypto_pay_asset", "USDT") or "USDT"
    asset_rate = get_effective_asset_rate(asset)
    asset_amount = (amount / asset_rate).quantize(ASSET_QUANT, rounding=ROUND_HALF_UP)
    if asset_amount <= 0:
        asset_amount = ASSET_QUANT
    try:
        check = crypto.create_check(
            asset=asset,
            amount=asset_amount,
            pin_to_user_id=user["user_id"],
        )
    except Exception as exc:
        logger.error("Crypto Pay create_check failed: %s", exc)
        bot.reply_to(message, "Не удалось создать чек. Попробуйте позже.")
        return
    db.update_user_balance(user["user_id"], delta_balance=-amount, delta_withdrawn=amount)
    db.create_withdraw_request(
        user["user_id"],
        amount,
        check_id=str(check.get("check_id")),
        check_url=check.get("bot_check_url"),
    )
    apply_referral_bonuses(user, amount)
    notify_withdrawal(user, amount, check.get("bot_check_url", ""))
    bot.send_message(
        message.chat.id,
        "\n".join(
            [
                "✅ Заявка на вывод создана!",
                f"Сумма: {format_amount(amount, currency_symbol())}",
                f"Чек: {check.get('bot_check_url')}",
            ]
        ),
        disable_web_page_preview=True,
    )


def process_promo_create_task(message: types.Message, user: sqlite3.Row) -> None:
    """Многошаговое создание задания на продвижение"""
    user_id = user["user_id"]
    state = user_states.get(user_id)
    if not state or state.get("mode") != "promo_create_task":
        return
    step = state.get("step", "completions")
    text = (message.text or "").strip()
    chat_id = state.get("chat_id", message.chat.id)
    prompt_message_id = state.get("prompt_message_id")
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="promo:back"))

    def update_prompt(text_to_show: str) -> None:
        nonlocal prompt_message_id
        target_message_id = prompt_message_id or message.message_id
        try:
            bot.edit_message_text(
                text_to_show,
                chat_id,
                target_message_id,
                reply_markup=markup,
            )
        except ApiException:
            sent = bot.send_message(chat_id, text_to_show, reply_markup=markup)
            prompt_message_id = sent.message_id
            if user_states.get(user_id) is state:
                state["prompt_message_id"] = prompt_message_id
                user_states[user_id] = state

    if text.lower() in ("отмена", "cancel", "отменить"):
        user_states.pop(user_id, None)
        update_prompt("❌ Создание задания отменено.")
        return

    task_price = get_task_price_amount()
    min_completions = int(db.get_setting("cashlait_min_completions", "10") or 10)

    if step == "completions":
        try:
            completions = int(text)
        except ValueError:
            update_prompt("❌ Введите корректное число.\n\nУкажите количество выполнений:")
            return
        if completions < min_completions:
            update_prompt(
                f"❌ Минимальное количество выполнений: {min_completions}\n\n"
                f"Введите количество выполнений (минимум {min_completions}):"
            )
            return

        total_cost = (task_price * Decimal(completions)).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
        updated = db.get_user(user_id)
        if not updated:
            user_states.pop(user_id, None)
            update_prompt("❌ Ошибка: пользователь не найден.")
            return
        promo_balance = dec(row_get(updated, "promo_balance", "0"), "0")
        if promo_balance < total_cost:
            needed = total_cost - promo_balance
            update_prompt(
                f"❌ Недостаточно средств на рекламном балансе.\n\n"
                f"💼 Текущий баланс: {format_amount(promo_balance, currency_symbol())}\n"
                f"💰 Требуется: {format_amount(total_cost, currency_symbol())}\n"
                f"💵 Пополните рекламный баланс на: {format_amount(needed, currency_symbol())}\n\n"
                f"Введите новое количество выполнений (минимум {min_completions}):"
            )
            return

        state.update(
            {
                "step": "channel",
                "completions": completions,
                "total_cost": str(total_cost),
                "task_price": str(task_price),
                "promo_balance": str(promo_balance),
            }
        )
        update_prompt(
            "🔗 Отправьте ссылку или @username канала, который нужно продвигать.\n\n"
            "Важно:\n"
            "• Бот должен быть добавлен администратором в этот канал.\n"
            "• Можно указать числовой ID (например, -1001234567890).\n"
            "• Для приватных каналов используйте @username или ID."
        )
        return

    if step == "channel":
        channel_input = text
        identifier, user_link, error = normalize_channel_input(channel_input)
        if error:
            update_prompt(error)
            return
        parsed_identifier = parse_chat_identifier(identifier)
        try:
            chat = bot.get_chat(parsed_identifier)
        except ApiException as exc:
            update_prompt(f"❌ Не удалось получить канал: {exc}. Убедитесь, что бот добавлен администратором и повторите попытку.")
            return
        try:
            member = bot.get_chat_member(chat.id, BOT_ID)
            if member.status not in ("administrator", "creator"):
                update_prompt("❌ Добавьте бота администратором в канал и повторите попытку.")
                return
        except ApiException as exc:
            update_prompt(f"❌ Не удалось проверить права бота: {exc}")
            return

        completions = int(state.get("completions", 0))
        total_cost = dec(state.get("total_cost"), "0")
        fresh_user = db.get_user(user_id)
        promo_balance = dec(row_get(fresh_user, "promo_balance", "0"), "0")
        if promo_balance < total_cost:
            state["step"] = "completions"
            user_states[user_id] = state
            update_prompt(
                "❌ На рекламном балансе недостаточно средств для этого заказа.\n"
                "Введите новое количество выполнений:"
            )
            return
        db.update_user_balance(user_id, delta_promo_balance=-total_cost)
        new_balance = promo_balance - total_cost

        signature = f"promo:{user_id}:{int(time.time())}"
        channel_link = user_link or channel_input
        channel_username = chat.username or (identifier if identifier.startswith("@") else "")
        db.add_promo_task(
            creator_id=user_id,
            signature=signature,
            title=f"Задание на продвижение ({completions} выполнений)",
            description=f"Подписаться на канал {channel_username or channel_link}",
            url=channel_link,
            button_text="Перейти",
            completions=completions,
            cost_per_completion=dec(state.get("task_price"), "0.1"),
            total_cost=total_cost,
            channel_id=chat.id,
            channel_username=channel_username,
            channel_link=channel_link,
        )

        user_states.pop(user_id, None)
        update_prompt(
            "✅ Задание создано!\n\n"
            f"📊 Количество выполнений: {completions}\n"
            f"💵 Стоимость: {format_amount(total_cost, currency_symbol())}\n"
            f"💼 Остаток на рекламном балансе: {format_amount(new_balance, currency_symbol())}\n\n"
            "Задание добавлено в раздел 'Задания'. Нажмите «⬅️ Назад», чтобы вернуться."
        )


def normalize_channel_input(raw_value: str) -> Tuple[str, str, Optional[str]]:
    value = (raw_value or "").strip()
    if not value:
        return "", "", "❌ Укажите ссылку вида https://t.me/канал или @username."
    link = value
    identifier = value
    lowered = value.lower()
    if lowered.startswith("http://t.me/"):
        value = "https://" + value.split("://", 1)[1]
        lowered = value.lower()
    if lowered.startswith("https://t.me/") or lowered.startswith("t.me/"):
        tail = value.split("t.me/", 1)[1]
        tail = tail.split("?")[0].strip("/")
        if not tail:
            return "", "", "❌ Укажите корректную ссылку на канал."
        if tail.startswith("+"):
            return "", "", "❌ Для приватных каналов задайте @username или предоставьте открытую ссылку t.me/…"
        identifier = f"@{tail.lstrip('@')}"
        link = f"https://t.me/{tail}"
    elif value.startswith("@"):
        identifier = value
        link = f"https://t.me/{value.lstrip('@')}"
    else:
        return "", "", "❌ Поддерживаются только ссылки t.me или @username."
    return identifier, link, None


def process_admin_balance_adjust(message: types.Message, admin_user: sqlite3.Row, state: Dict[str, Any]) -> None:
    text = (message.text or "").replace(",", ".").strip()
    parts = text.split()
    if len(parts) != 2:
        admin_reply(message, "❌ Введите данные в формате: <code>ID СУММА</code>")
        return
    try:
        target_id = int(parts[0])
        amount = Decimal(parts[1])
    except (ValueError, InvalidOperation):
        admin_reply(message, "❌ Некорректные данные. Пример: <code>123456789 10.5</code>")
        return
    if amount <= 0:
        admin_reply(message, "❌ Сумма должна быть больше 0.")
        return
    amount = amount.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
    target_user = db.get_user(target_id)
    if not target_user:
        admin_reply(message, "❌ Пользователь не найден.")
        return

    operation = state.get("operation", "add")
    balance_type = state.get("balance_type", "main")
    delta = amount if operation == "add" else -amount

    if balance_type == "main":
        current = dec(row_get(target_user, "balance", "0"), "0")
    else:
        current = dec(row_get(target_user, "promo_balance", "0"), "0")

    if delta < 0 and current + delta < 0:
        admin_reply(
            message,
            f"❌ Недостаточно средств. Текущий баланс: {format_amount(current, currency_symbol())}",
        )
        return

    if balance_type == "main":
        db.update_user_balance(target_id, delta_balance=delta)
        balance_label = "основном"
    else:
        db.update_user_balance(target_id, delta_promo_balance=delta)
        balance_label = "рекламном"

    new_balance = current + delta
    user_states.pop(admin_user["user_id"], None)
    action_text = "начислено" if delta > 0 else "списано"
    bot.reply_to(
        message,
        f"✅ У пользователя <code>{target_id}</code> {action_text} {format_amount(amount, currency_symbol())} "
        f"на {balance_label} балансе.\nНовый баланс: {format_amount(new_balance, currency_symbol())}",
        parse_mode="HTML",
    )

    if delta > 0:
        notice = (
            f"🎁 Вам начислено {format_amount(amount, currency_symbol())} "
            f"на {balance_label} балансе от администратора."
        )
    else:
        notice = (
            f"⚠️ С вашего {balance_label} баланса списано {format_amount(abs(delta), currency_symbol())} "
            f"администратором."
        )
    try:
        bot.send_message(target_id, notice)
    except ApiException:
        pass

def build_deposit_invoice_markup(invoice_id: str, invoice_url: str) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("💳 Оплатить", url=invoice_url))
    markup.add(types.InlineKeyboardButton("✅ Проверить оплату", callback_data=f"deposit:check:{invoice_id}"))
    markup.add(types.InlineKeyboardButton("❌ Отменить", callback_data=f"deposit:cancel:{invoice_id}"))
    return markup


ADMIN_CANCEL_CALLBACK = "admin:cancel_state"


def admin_cancel_markup() -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("❌ Отменить", callback_data=ADMIN_CANCEL_CALLBACK))
    return markup


def admin_reply(message: types.Message, text: str) -> None:
    bot.reply_to(message, text, reply_markup=admin_cancel_markup())


def start_deposit_flow(call: types.CallbackQuery, user: sqlite3.Row) -> None:
    if not get_crypto_client():
        bot.answer_callback_query(call.id, "Crypto Pay не настроен.", show_alert=True)
        return
    user_states[user["user_id"]] = {"mode": "deposit_amount"}
    bot.answer_callback_query(call.id)
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("❌ Отменить", callback_data="deposit:cancel_input"))
    bot.send_message(
        call.message.chat.id,
        "Введите сумму пополнения в USDT.",
        reply_markup=markup,
    )


def start_convert_flow(call: types.CallbackQuery, user: sqlite3.Row) -> None:
    balance = dec(user["balance"], "0")
    if balance <= 0:
        bot.answer_callback_query(call.id, "Недостаточно средств для конвертации.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("❌ Отменить", callback_data="cabinet:convert_cancel"))
    msg = bot.send_message(
        call.message.chat.id,
        f"♻️ Введите сумму для перевода на рекламный баланс.\n\n"
        f"Доступно: {format_amount(balance, currency_symbol())}",
        reply_markup=markup,
    )
    user_states[user["user_id"]] = {
        "mode": "convert_to_promo",
        "chat_id": call.message.chat.id,
        "message_id": msg.message_id,
    }


def process_convert_to_promo(message: types.Message, user: sqlite3.Row) -> None:
    state = user_states.get(user["user_id"])
    if not state or state.get("mode") != "convert_to_promo":
        return
    text = (message.text or "").replace(",", ".").strip()
    try:
        amount = Decimal(text)
    except InvalidOperation:
        bot.reply_to(message, "❌ Введите корректную сумму.")
        return
    if amount <= 0:
        bot.reply_to(message, "❌ Сумма должна быть больше 0.")
        return
    balance = dec(user["balance"], "0")
    if amount > balance:
        bot.reply_to(
            message,
            f"❌ Недостаточно средств. Доступно {format_amount(balance, currency_symbol())}.",
        )
        return
    amount = amount.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
    db.update_user_balance(user["user_id"], delta_balance=-amount, delta_promo_balance=amount)
    user_states.pop(user["user_id"], None)
    bot.reply_to(
        message,
        f"✅ Переведено {format_amount(amount, currency_symbol())} на рекламный баланс.",
    )


def process_deposit_amount(message: types.Message, user: sqlite3.Row) -> None:
    text = (message.text or "").replace(",", ".").strip()
    try:
        amount = parse_decimal_input(text, MONEY_QUANT)
    except (InvalidOperation, ValueError):
        bot.reply_to(message, "Введите корректную сумму.")
        return
    if amount <= 0:
        bot.reply_to(message, "Сумма должна быть больше 0.")
        return
    crypto = get_crypto_client()
    if not crypto:
        bot.reply_to(message, "Crypto Pay не настроен.")
        return
    asset = db.get_setting("crypto_pay_asset", "USDT") or "USDT"
    asset_rate = get_effective_asset_rate(asset)
    asset_amount = (amount / asset_rate).quantize(ASSET_QUANT, rounding=ROUND_HALF_UP)
    if asset_amount <= 0:
        asset_amount = ASSET_QUANT
    description = f"Пополнение баланса пользователя {user['user_id']}"
    try:
        invoice = crypto.create_invoice(asset=asset, amount=asset_amount, description=description)
    except Exception as exc:
        logger.error("Не удалось создать счёт пополнения: %s", exc)
        bot.reply_to(message, f"Ошибка создания счёта: {exc}")
        return
    invoice_id = str(invoice.get("invoice_id"))
    invoice_url = invoice.get("bot_invoice_url") or invoice.get("pay_url") or ""
    if not invoice_id or not invoice_url:
        bot.reply_to(message, "Платёжная система вернула некорректные данные. Попробуйте позже.")
        return
    db.create_deposit_request(
        user["user_id"],
        amount,
        asset_amount,
        invoice_id=invoice_id,
        invoice_url=invoice_url,
    )
    user_states.pop(user["user_id"], None)
    bot.send_message(
        message.chat.id,
        "\n".join(
            [
                "💳 Пополнение баланса",
                "",
                f"Сумма: {format_amount(amount, currency_symbol())}",
                "Оплатите счёт и нажмите «Проверить оплату».",
            ]
        ),
        reply_markup=build_deposit_invoice_markup(invoice_id, invoice_url),
        disable_web_page_preview=True,
    )


def verify_deposit_invoice(record: sqlite3.Row) -> Tuple[str, bool]:
    amount = dec(record["amount"], "0")
    status = (record["status"] or "").lower()
    if status == "paid":
        return f"✅ Пополнение уже зачислено: {format_amount(amount, currency_symbol())}", True
    crypto = get_crypto_client()
    if not crypto:
        return "Crypto Pay не настроен.", False
    try:
        invoice = crypto.get_invoice(record["invoice_id"])
    except Exception as exc:
        logger.error("Не удалось получить статус счёта %s: %s", record["invoice_id"], exc)
        return f"Ошибка проверки: {exc}", False
    if not invoice:
        return "Счёт не найден. Создайте новый запрос.", False
    invoice_status = str(invoice.get("status") or "").lower()
    if invoice_status == "paid":
        db.update_user_balance(record["user_id"], delta_balance=amount)
        db.update_deposit_status(record["invoice_id"], "paid")
        return f"✅ Зачислено {format_amount(amount, currency_symbol())}", True
    if invoice_status == "expired":
        db.update_deposit_status(record["invoice_id"], "expired")
        return "Счёт просрочен. Создайте новый запрос на пополнение.", False
    return "Платёж ещё не оплачен. Попробуйте позже.", False


@bot.callback_query_handler(func=lambda call: call.data.startswith("deposit:"))
def callback_deposit_actions(call: types.CallbackQuery) -> None:
    parts = call.data.split(":")
    if len(parts) == 2 and parts[1] == "cancel_input":
        state = user_states.get(call.from_user.id)
        if state and state.get("mode") == "deposit_amount":
            user_states.pop(call.from_user.id, None)
        bot.answer_callback_query(call.id, "Пополнение отменено.", show_alert=True)
        bot.send_message(call.message.chat.id, "❌ Пополнение отменено. Вы можете вернуться к кабинету.")
        return
    if len(parts) != 3:
        bot.answer_callback_query(call.id)
        return
    _, action, invoice_id = parts
    record = db.get_deposit_request(invoice_id)
    if not record or record["user_id"] != call.from_user.id:
        bot.answer_callback_query(call.id, "Счёт не найден.", show_alert=True)
        return
    if action == "check":
        text, success = verify_deposit_invoice(record)
        bot.answer_callback_query(call.id, text, show_alert=True)
        if success:
            bot.send_message(call.message.chat.id, text)
    elif action == "cancel":
        current_status = (record["status"] or "").lower()
        if current_status == "paid":
            bot.answer_callback_query(call.id, "Оплата уже получена — отмена невозможна.", show_alert=True)
            return
        if current_status in {"cancelled", "expired"}:
            bot.answer_callback_query(call.id, "Счёт уже закрыт.", show_alert=True)
            return
        db.update_deposit_status(invoice_id, "cancelled")
        bot.answer_callback_query(call.id, "Счёт отменён.", show_alert=True)
        bot.send_message(call.message.chat.id, "❌ Пополнение отменено. Вы можете создать новый счёт.")
    else:
        bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data == ADMIN_CANCEL_CALLBACK)
def callback_admin_cancel(call: types.CallbackQuery) -> None:
    state = user_states.pop(call.from_user.id, None)
    if state:
        bot.answer_callback_query(call.id, "Действие отменено.", show_alert=True)
        bot.send_message(call.message.chat.id, "❌ Действие отменено.")
    else:
        bot.answer_callback_query(call.id, "Активных действий нет.", show_alert=True)


@bot.callback_query_handler(func=lambda call: call.data == "withdraw:start")
def callback_withdraw_start(call: types.CallbackQuery) -> None:
    user = ensure_user_row(call.from_user)
    start_withdrawal(call, user)


@bot.callback_query_handler(func=lambda call: call.data == "cabinet:deposit")
def callback_cabinet_deposit(call: types.CallbackQuery) -> None:
    user = ensure_user_row(call.from_user)
    start_deposit_flow(call, user)


@bot.callback_query_handler(func=lambda call: call.data == "cabinet:convert")
def callback_cabinet_convert(call: types.CallbackQuery) -> None:
    user = ensure_user_row(call.from_user)
    start_convert_flow(call, user)


@bot.callback_query_handler(func=lambda call: call.data == "cabinet:convert_cancel")
def callback_convert_cancel(call: types.CallbackQuery) -> None:
    state = user_states.pop(call.from_user.id, None)
    if state and state.get("mode") == "convert_to_promo":
        bot.answer_callback_query(call.id, "Конвертация отменена.", show_alert=True)
        bot.send_message(call.message.chat.id, "❌ Конвертация отменена. Возвращайтесь в личный кабинет.")
    else:
        bot.answer_callback_query(call.id, "Нет активной конвертации.", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("taskcheck:"))
def callback_task_check(call: types.CallbackQuery) -> None:
    try:
        _, requested_context, task_id_str = call.data.split(":")
        task_id = int(task_id_str)
    except (ValueError, AttributeError):
        bot.answer_callback_query(call.id, "Ошибка данных", show_alert=True)
        return
    task = db.get_pending_task(task_id)
    if not task or task.get("_user_id") != call.from_user.id:
        bot.answer_callback_query(call.id, "Задание устарело.", show_alert=True)
        return
    context = "tasks"
    signature = task.get("signature")
    if not signature:
        bot.answer_callback_query(call.id, "Некорректное задание.", show_alert=True)
        return
    user = ensure_user_row(call.from_user)
    if db.has_task_completion(user["user_id"], signature, context):
        bot.answer_callback_query(call.id, "Задание уже выполнено.", show_alert=True)
        return
    source = task.get("source", "flyer")
    success = False
    error_message = ""
    if source == "flyer":
        flyer = get_flyer_client()
        if not flyer:
            bot.answer_callback_query(call.id, "Flyer API не настроен.", show_alert=True)
            return
        try:
            status = str(flyer.check_task(signature) or "").lower()
        except Exception as exc:
            bot.answer_callback_query(call.id, f"Ошибка проверки: {exc}", show_alert=True)
            return
        if status in FLYER_FAIL_STATUSES:
            bot.answer_callback_query(call.id, "Задание ещё не выполнено.", show_alert=True)
            return
        success = True
    elif source == "promo":
        success, error_message = verify_custom_task(task, call.from_user.id)
        if not success:
            bot.answer_callback_query(call.id, error_message, show_alert=True)
            return
    else:
        success, error_message = verify_custom_task(task, call.from_user.id)
        if not success:
            bot.answer_callback_query(call.id, error_message, show_alert=True)
            return

    payout = dec(task.get("payout"), "0")
    db.add_task_log(user["user_id"], signature, source, context, payout)
    
    # Если задание от Flyer - проверяем тип задания
    # Если это канал - на frozen_balance (удержание), если бот - на основной баланс
    if source == "flyer" and payout > 0:
        task_url = task.get("url", "")
        # Проверяем, является ли задание ботом по URL
        # Боты обычно имеют параметр ?start= в URL
        is_bot_task = False
        if task_url:
            url_lower = task_url.lower()
            # Если есть параметр start - это точно бот
            if "?start=" in url_lower or "/start" in url_lower:
                is_bot_task = True
        
        if is_bot_task:
            # Для ботов - на основной баланс сразу
            db.update_user_balance(user["user_id"], delta_balance=payout, inc_completed=1)
        else:
            # Для каналов - на frozen_balance (удержание)
            db.update_user_balance(user["user_id"], delta_frozen_balance=payout, inc_completed=1)
            db.add_subscription_watch(
                user_id=user["user_id"],
                signature=signature,
                source=source,
                reward=payout,
                expires_at=now_utc() + timedelta(days=3),  # Проверяем 3 дня
            )
    else:
        # Для не-Flyer заданий - на основной баланс
        db.update_user_balance(user["user_id"], delta_balance=payout, inc_completed=1)
    
    db.delete_pending_task(task_id)
    if source == "promo":
        promo_signature = task.get("promo_signature") or signature
        if promo_signature:
            _, _, finished = db.increment_promo_completion(promo_signature)
            if finished:
                db.remove_pending_tasks_by_signature(promo_signature)
    
    # Получаем оставшиеся задания для перехода к следующему
    remaining_tasks = db.list_pending_tasks(user["user_id"], context)
    
    bot.answer_callback_query(
        call.id,
        f"Начислено {format_amount(payout, currency_symbol())}",
        show_alert=True,
    )
    
    # Если есть еще задания, показываем следующее
    if remaining_tasks:
        next_task_id, next_task = remaining_tasks[0]
        sym = currency_symbol()
        next_payout = format_amount(dec(next_task.get("payout"), "0"), sym)
        next_title = next_task.get("title", "Задание")
        
        text = f"✅ Задание выполнено! Начислено {format_amount(payout, sym)}\n\n📋 Следующее задание:\n\n{next_title} — {next_payout}\n\nПосле выполнения вернитесь и нажмите кнопку проверки."
        markup = types.InlineKeyboardMarkup(row_width=1)
        if next_task.get("url"):
            markup.add(
                types.InlineKeyboardButton("➡️ Перейти", url=next_task.get("url"))
            )
        markup.add(
            types.InlineKeyboardButton("✅ Проверить", callback_data=f"taskcheck:{context}:{next_task_id}")
        )
        markup.add(
            types.InlineKeyboardButton("⏭ Пропустить", callback_data=f"tasks:skip:{context}:{next_task_id}")
        )
        # Если есть еще задания после этого, добавляем кнопку "Следующее"
        if len(remaining_tasks) > 1:
            next_next_task_id = remaining_tasks[1][0]
            markup.add(
                types.InlineKeyboardButton("⏭ Следующее задание", callback_data=f"tasks:next:{context}:{next_next_task_id}")
            )
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"tasks:summary:{context}"))
        
        try:
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
            )
        except ApiException:
            bot.send_message(call.message.chat.id, text, reply_markup=markup)
    else:
        # Если заданий больше нет, показываем сообщение
        text = f"✅ Задание выполнено! Начислено {format_amount(payout, currency_symbol())}\n\n✅ Все задания выполнены! Задания обновляются автоматически."
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔄 Обновить", callback_data=f"tasks:refresh:{context}"))
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"tasks:summary:{context}"))
        try:
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
            )
        except ApiException:
            bot.send_message(call.message.chat.id, text, reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("tasks:next:"))
def callback_tasks_next(call: types.CallbackQuery) -> None:
    """Обработчик кнопки 'Следующее задание'"""
    try:
        _, _, context, task_id_str = call.data.split(":")
        task_id = int(task_id_str)
    except (ValueError, AttributeError):
        bot.answer_callback_query(call.id, "Ошибка данных", show_alert=True)
        return
    
    user = ensure_user_row(call.from_user)
    task = db.get_pending_task(task_id)
    if not task or task.get("_user_id") != call.from_user.id:
        bot.answer_callback_query(call.id, "Задание не найдено", show_alert=True)
        # Обновляем список заданий
        text, markup = build_task_details_message(user, context, with_refresh=False)
        try:
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
            )
        except ApiException:
            bot.send_message(call.message.chat.id, text, reply_markup=markup)
        return
    
    # Показываем следующее задание - находим его индекс и показываем только его
    all_tasks = db.list_pending_tasks(user["user_id"], context)
    current_idx = None
    for idx, (tid, _) in enumerate(all_tasks):
        if tid == task_id:
            current_idx = idx
            break
    
    if current_idx is None:
        bot.answer_callback_query(call.id, "Задание не найдено", show_alert=True)
        return
    
    # Показываем следующее задание (если есть)
    if current_idx < len(all_tasks) - 1:
        next_task_id, next_task = all_tasks[current_idx + 1]
        sym = currency_symbol()
        payout = format_amount(dec(next_task.get("payout"), "0"), sym)
        title = next_task.get("title", "Задание")
        
        text = f"📋 Задание\n\n{title} — {payout}\n\nПосле выполнения вернитесь и нажмите кнопку проверки."
        markup = types.InlineKeyboardMarkup(row_width=1)
        if next_task.get("url"):
            markup.add(
                types.InlineKeyboardButton("➡️ Перейти", url=next_task.get("url"))
            )
        markup.add(
            types.InlineKeyboardButton("✅ Проверить", callback_data=f"taskcheck:{context}:{next_task_id}")
        )
        markup.add(
            types.InlineKeyboardButton("⏭ Пропустить", callback_data=f"tasks:skip:{context}:{next_task_id}")
        )
        # Если есть еще задания после этого, добавляем кнопку "Следующее"
        if current_idx + 1 < len(all_tasks) - 1:
            next_next_task_id = all_tasks[current_idx + 2][0]
            markup.add(
                types.InlineKeyboardButton("⏭ Следующее задание", callback_data=f"tasks:next:{context}:{next_next_task_id}")
            )
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"tasks:summary:{context}"))
        
        try:
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
        except ApiException:
            bot.send_message(call.message.chat.id, text, reply_markup=markup)
    else:
        # Это последнее задание
        bot.answer_callback_query(call.id, "Это последнее задание", show_alert=True)
    
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("tasks:skip:"))
def callback_tasks_skip(call: types.CallbackQuery) -> None:
    """Обработчик кнопки 'Пропустить задание'"""
    try:
        _, _, context, task_id_str = call.data.split(":")
        task_id = int(task_id_str)
    except (ValueError, AttributeError):
        bot.answer_callback_query(call.id, "Ошибка данных", show_alert=True)
        return
    
    user = ensure_user_row(call.from_user)
    task = db.get_pending_task(task_id)
    if not task or task.get("_user_id") != call.from_user.id:
        bot.answer_callback_query(call.id, "Задание не найдено", show_alert=True)
        return
    
    # Удаляем задание из pending_tasks для пользователя
    db.delete_pending_task(task_id)
    
    # Получаем оставшиеся задания
    remaining_tasks = db.list_pending_tasks(user["user_id"], context)
    
    bot.answer_callback_query(call.id, "Задание пропущено", show_alert=True)
    
    # Если есть еще задания, показываем следующее
    if remaining_tasks:
        next_task_id, next_task = remaining_tasks[0]
        sym = currency_symbol()
        next_payout = format_amount(dec(next_task.get("payout"), "0"), sym)
        next_title = next_task.get("title", "Задание")
        
        text = f"⏭ Задание пропущено\n\n📋 Следующее задание:\n\n{next_title} — {next_payout}\n\nПосле выполнения вернитесь и нажмите кнопку проверки."
        markup = types.InlineKeyboardMarkup(row_width=1)
        if next_task.get("url"):
            markup.add(
                types.InlineKeyboardButton("➡️ Перейти", url=next_task.get("url"))
            )
        markup.add(
            types.InlineKeyboardButton("✅ Проверить", callback_data=f"taskcheck:{context}:{next_task_id}")
        )
        markup.add(
            types.InlineKeyboardButton("⏭ Пропустить", callback_data=f"tasks:skip:{context}:{next_task_id}")
        )
        # Если есть еще задания после этого, добавляем кнопку "Следующее"
        if len(remaining_tasks) > 1:
            next_next_task_id = remaining_tasks[1][0]
            markup.add(
                types.InlineKeyboardButton("⏭ Следующее задание", callback_data=f"tasks:next:{context}:{next_next_task_id}")
            )
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"tasks:summary:{context}"))
        
        try:
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
            )
        except ApiException:
            bot.send_message(call.message.chat.id, text, reply_markup=markup)
    else:
        # Если заданий больше нет, показываем сообщение
        text = "⏭ Задание пропущено\n\n✅ Все задания выполнены или пропущены! Задания обновляются автоматически."
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔄 Обновить", callback_data=f"tasks:refresh:{context}"))
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"tasks:summary:{context}"))
        try:
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
            )
        except ApiException:
            bot.send_message(call.message.chat.id, text, reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("tasks:refresh:"))
def callback_tasks_refresh(call: types.CallbackQuery) -> None:
    try:
        _, _, context = call.data.split(":")
    except ValueError:
        bot.answer_callback_query(call.id)
        return
    context = "tasks"
    user = ensure_user_row(call.from_user)
    text, markup = build_task_details_message(user, context, force=True)
    try:
        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )
    except ApiException as exc:
        logger.debug("Не удалось отредактировать сообщение: %s", exc)
        bot.send_message(call.message.chat.id, text, reply_markup=markup)
    bot.answer_callback_query(call.id, "Список обновлён")


@bot.callback_query_handler(func=lambda call: call.data.startswith("tasks:refresh_summary:"))
def callback_tasks_refresh_summary(call: types.CallbackQuery) -> None:
    try:
        _, _, context = call.data.split(":")
    except ValueError:
        bot.answer_callback_query(call.id)
        return
    context = "tasks"
    user = ensure_user_row(call.from_user)
    text, markup = build_tasks_summary(user, context, force=True)
    try:
        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )
    except ApiException as exc:
        logger.debug("Не удалось отредактировать сообщение: %s", exc)
        bot.send_message(call.message.chat.id, text, reply_markup=markup)
    bot.answer_callback_query(call.id, "Обновлено")


@bot.callback_query_handler(func=lambda call: call.data.startswith("tasks:details:"))
def callback_tasks_details(call: types.CallbackQuery) -> None:
    try:
        _, _, context = call.data.split(":")
    except ValueError:
        bot.answer_callback_query(call.id)
        return
    context = "tasks"
    user = ensure_user_row(call.from_user)
    text, markup = build_task_details_message(user, context)
    try:
        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )
    except ApiException:
        bot.send_message(call.message.chat.id, text, reply_markup=markup)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("tasks:summary:"))
def callback_tasks_summary(call: types.CallbackQuery) -> None:
    try:
        _, _, context = call.data.split(":")
    except ValueError:
        bot.answer_callback_query(call.id)
        return
    context = "tasks"
    user = ensure_user_row(call.from_user)
    text, markup = build_tasks_summary(user, context)
    try:
        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )
    except ApiException:
        bot.send_message(call.message.chat.id, text, reply_markup=markup)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("promo:"))
def callback_promo_actions(call: types.CallbackQuery) -> None:
    logger.info("Обработка promo callback: %s от пользователя %s", call.data, call.from_user.id)
    try:
        parts = call.data.split(":")
        if len(parts) < 2:
            logger.warning("Неверный формат callback promo: %s", call.data)
            bot.answer_callback_query(call.id, "Ошибка данных", show_alert=True)
            return
        action = parts[1]
        logger.info("Action: %s", action)
    except Exception as exc:
        logger.error("Ошибка парсинга callback promo: %s", exc, exc_info=True)
        bot.answer_callback_query(call.id, "Ошибка", show_alert=True)
        return
    
    try:
        user = ensure_user_row(call.from_user)
        logger.debug("Пользователь получен: %s", user["user_id"])
    except Exception as exc:
        logger.error("Ошибка получения пользователя: %s", exc, exc_info=True)
        bot.answer_callback_query(call.id, "Ошибка получения данных", show_alert=True)
        return
    
    # Отвечаем на callback только после успешного получения пользователя
    try:
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.warning("Не удалось ответить на callback: %s", e)
    
    if action == "create":
        # Получаем настройки
        task_price = get_task_price_amount()
        min_completions = int(db.get_setting("cashlait_min_completions", "10") or 10)
        
        text = (
            f"📣 Создание задания на продвижение\n\n"
            f"💵 Цена за 1 выполнение: {format_amount(task_price, currency_symbol())}\n"
            f"📊 Минимальное количество выполнений: {min_completions}\n\n"
            f"Введите количество выполнений (минимум {min_completions}):"
        )
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="promo:back"))
        try:
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
            )
            prompt_message_id = call.message.message_id
        except ApiException:
            sent = bot.send_message(call.message.chat.id, text, reply_markup=markup)
            prompt_message_id = sent.message_id
        user_states[user["user_id"]] = {
            "mode": "promo_create_task",
            "step": "completions",
            "prompt_message_id": prompt_message_id,
            "chat_id": call.message.chat.id,
        }
        return
    
    if action == "back":
        send_promotion_section(user, call.message.chat.id)
        if user["user_id"] in user_states:
            user_states.pop(user["user_id"], None)
        return
    
    if action == "active":
        logger.debug("Обработка promo:active для пользователя %s", user["user_id"])
        try:
            raw_tasks = db.get_user_active_promo_tasks(user["user_id"])
            tasks = [row_to_dict(task) for task in raw_tasks]
            logger.debug("Найдено активных заданий: %s", len(tasks))
            if not tasks:
                text = "📈 У вас нет активных заданий на продвижение."
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="promo:back"))
                try:
                    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
                except ApiException as e:
                    logger.debug("Не удалось отредактировать сообщение: %s", e)
                    bot.send_message(call.message.chat.id, text, reply_markup=markup)
                return
            
            lines = ["📈 Активные задания на продвижение", ""]
            for task in tasks:
                completed = row_get(task, "completed_count", 0) or 0
                total = row_get(task, "completions", 0)
                cost = dec(row_get(task, "total_cost", "0"), "0")
                title = row_get(task, "title", "Задание")
                lines.append(
                    f"• {title}\n"
                    f"  Выполнено: {completed}/{total}\n"
                    f"  Стоимость: {format_amount(cost, currency_symbol())}"
                )
            
            text_to_send = "\n".join(lines)
            # Ограничиваем длину сообщения (максимум 4096 символов)
            if len(text_to_send) > 4000:
                text_to_send = text_to_send[:4000] + "\n\n... (список обрезан)"
            
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="promo:back"))
            try:
                bot.edit_message_text(text_to_send, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
            except ApiException as e:
                logger.debug("Не удалось отредактировать сообщение: %s", e)
                bot.send_message(call.message.chat.id, text_to_send, reply_markup=markup, parse_mode="HTML")
        except Exception as exc:
            logger.error("Ошибка в promo:active: %s", exc, exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка при загрузке заданий", show_alert=True)
            except:
                pass
        return
    
    if action == "finished":
        raw_tasks = db.get_user_finished_promo_tasks(user["user_id"])
        tasks = [row_to_dict(task) for task in raw_tasks]
        if not tasks:
            text = "✅ У вас нет завершенных заданий на продвижение."
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="promo:back"))
            try:
                bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
            except ApiException:
                bot.send_message(call.message.chat.id, text, reply_markup=markup)
            return
        
        lines = ["✅ Завершенные задания на продвижение", ""]
        for task in tasks:
            completed = row_get(task, "completed_count", 0) or 0
            total = row_get(task, "completions", 0)
            cost = dec(row_get(task, "total_cost", "0"), "0")
            title = row_get(task, "title", "Задание")
            lines.append(
                f"• {title}\n"
                f"  Выполнено: {completed}/{total}\n"
                f"  Стоимость: {format_amount(cost, currency_symbol())}"
            )
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="promo:back"))
        try:
            bot.edit_message_text("\n".join(lines), call.message.chat.id, call.message.message_id, reply_markup=markup)
        except ApiException:
            bot.send_message(call.message.chat.id, "\n".join(lines), reply_markup=markup)
        return
    
    if action == "manage":
        try:
            raw_tasks = db.get_user_active_promo_tasks(user["user_id"])
            tasks = [row_to_dict(task) for task in raw_tasks]
            if not tasks:
                text = "⚙️ У вас нет активных заданий для управления."
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="promo:back"))
                try:
                    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
                except ApiException:
                    bot.send_message(call.message.chat.id, text, reply_markup=markup)
                return
            
            lines = ["⚙️ Управление заданиями", ""]
            lines.append("Выберите задание для удаления (средства не возвращаются):")
            lines.append("")
            
            markup = types.InlineKeyboardMarkup(row_width=1)
            for task in tasks:
                completed = row_get(task, "completed_count", 0) or 0
                total = row_get(task, "completions", 0)
                title = row_get(task, "title", "Задание")
                task_id = row_get(task, "id")
                if task_id:
                    # Ограничиваем длину текста кнопки
                    button_text = f"🗑 {title[:30]}" if len(title) > 30 else f"🗑 {title}"
                    button_text += f" ({completed}/{total})"
                    markup.add(
                        types.InlineKeyboardButton(
                            button_text,
                            callback_data=f"promo:delete:{task_id}"
                        )
                    )
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="promo:back"))
            
            try:
                bot.edit_message_text("\n".join(lines), call.message.chat.id, call.message.message_id, reply_markup=markup)
            except ApiException:
                bot.send_message(call.message.chat.id, "\n".join(lines), reply_markup=markup)
        except Exception as exc:
            logger.error("Ошибка в promo:manage: %s", exc, exc_info=True)
            bot.answer_callback_query(call.id, "Ошибка при загрузке заданий", show_alert=True)
        return
    
    if action == "delete":
        try:
            if len(parts) >= 3:
                task_id = int(parts[2])
            else:
                bot.answer_callback_query(call.id, "Ошибка данных", show_alert=True)
                return
        except (ValueError, IndexError) as exc:
            logger.error("Ошибка парсинга task_id: %s", exc)
            bot.answer_callback_query(call.id, "Ошибка данных", show_alert=True)
            return
        
        try:
            if db.deactivate_promo_task(task_id, user["user_id"]):
                bot.answer_callback_query(call.id, "✅ Задание удалено", show_alert=True)
                # Возвращаемся к управлению
                call.data = "promo:manage"
                callback_promo_actions(call)
            else:
                bot.answer_callback_query(call.id, "❌ Задание не найдено", show_alert=True)
        except Exception as exc:
            logger.error("Ошибка при удалении задания: %s", exc, exc_info=True)
            bot.answer_callback_query(call.id, "Ошибка при удалении", show_alert=True)
        return
    
    bot.send_message(call.message.chat.id, "Функция временно недоступна.")


@bot.callback_query_handler(func=lambda call: call.data.startswith("info:"))
def callback_info_links(call: types.CallbackQuery) -> None:
    slug = call.data.split(":")[1]
    fallback_messages = {
        "help": "❓ Ссылка на помощь не настроена. Добавьте её в админке (🔗 Ссылки инфо).",
        "news": "📣 Ссылка на новости ещё не добавлена.",
        "chat": "💬 Ссылка на чат отсутствует. Укажите её в настройках.",
        "copy": "🤖 Ссылка «Хочу такого же бота» не настроена.",
    }
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, fallback_messages.get(slug, "Ссылка не найдена."))


@bot.callback_query_handler(func=lambda call: call.data.startswith("check_sub:"))
def callback_check_subscription(call: types.CallbackQuery) -> None:
    try:
        _, category = call.data.split(":")
    except ValueError:
        bot.answer_callback_query(call.id)
        return
    user = ensure_user_row(call.from_user)
    if check_subscription(user_id=user["user_id"], chat_id=call.message.chat.id, category=category, notify=False):
        bot.answer_callback_query(call.id, "Подписка подтверждена.", show_alert=True)
        bot.send_message(
            call.message.chat.id,
            "Спасибо! Доступ открыт.",
            reply_markup=build_main_keyboard(user["user_id"]),
        )
    else:
        bot.answer_callback_query(call.id, "Подписка не найдена.", show_alert=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith("admin:"))
def callback_admin_router(call: types.CallbackQuery) -> None:
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
        return
    parts = call.data.split(":")
    if len(parts) < 2:
        bot.answer_callback_query(call.id)
        return
    action = parts[1]
    if action == "menu":
        admin_update_message(call, "🔐 Админ-панель", admin_menu_markup())
        bot.answer_callback_query(call.id)
    elif action == "settings":
        show_admin_settings(call)
    elif action == "flyer":
        show_flyer_settings(call)
    elif action == "custom" and len(parts) >= 3:
        placement = parts[2]
        show_custom_tasks_menu(call, placement)
    elif action == "broadcast":
        start_broadcast_flow(call)
    elif action == "reserve":
        show_reserve_panel(call)
    elif action == "buttons":
        show_button_settings(call)
    elif action == "links":
        show_link_settings(call)
    elif action == "balances":
        show_balance_menu(call)
    elif action == "flyerlogs":
        send_flyer_logs(call.message.chat.id)
        bot.answer_callback_query(call.id)
    elif action == "balance" and len(parts) >= 4:
        operation = parts[2]
        balance_type = parts[3]
        start_balance_adjust(call, operation, balance_type)
    elif action == "cancel_state":
        if call.from_user.id in user_states:
            user_states.pop(call.from_user.id, None)
        bot.answer_callback_query(call.id, "Действие отменено", show_alert=True)
        admin_update_message(call, "🔐 Админ-панель", admin_menu_markup())
    elif action == "reservesettings":
        show_reserve_settings(call)
    elif action == "required":
        show_required_channels_menu(call)
    elif action == "requiredlist":
        show_required_channels_list(call)
    elif action == "payout_channel":
        prompt_payout_channel(call)
    elif action == "set" and len(parts) >= 3:
        key = parts[2]
        prompt_setting_value(call, key, ADMIN_SETTING_FIELDS, context="settings")
    elif action == "flyerset" and len(parts) >= 3:
        key = parts[2]
        prompt_setting_value(call, key, FLYER_SETTING_FIELDS, context="flyer")
    elif action == "reserveset" and len(parts) >= 3:
        key = parts[2]
        prompt_setting_value(call, key, RESERVE_SETTING_FIELDS, context="reserve")
    elif action == "buttonset" and len(parts) >= 3:
        key = parts[2]
        prompt_setting_value(call, key, BUTTON_SETTING_FIELDS, context="buttons")
    elif action == "linkset" and len(parts) >= 3:
        key = parts[2]
        prompt_setting_value(call, key, INFO_LINK_FIELDS, context="links")
    elif action == "customadd" and len(parts) >= 3:
        placement = parts[2]
        start_custom_task_creation(call, placement)
    elif action == "customdel" and len(parts) >= 3:
        placement = parts[2]
        start_custom_task_removal(call, placement)
    elif action == "requiredadd" and len(parts) >= 3:
        category = parts[2]
        start_required_channel_add(call, category)
    elif action == "requireddel":
        start_required_channel_remove(call)
    elif action == "reserveinvoice":
        start_reserve_invoice(call)
    elif action == "reservecashout":
        start_reserve_cashout(call)
    else:
        bot.answer_callback_query(call.id)


def prompt_setting_value(
    call: types.CallbackQuery,
    key: str,
    mapping: Dict[str, Tuple[str, str]],
    context: str,
) -> None:
    meta = mapping.get(key)
    if not meta:
        bot.answer_callback_query(call.id)
        return
    label, value_type = meta
    user_states[call.from_user.id] = {
        "mode": "admin_set_setting",
        "key": key,
        "value_type": value_type,
        "context": context,
    }
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, f"Введите значение для «{label}».", reply_markup=admin_cancel_markup())


def start_custom_task_creation(call: types.CallbackQuery, placement: str) -> None:
    if placement != "tasks":
        placement = "tasks"
    user_states[call.from_user.id] = {
        "mode": "admin_add_custom_task",
        "placement": placement,
        "step": "title",
        "data": {},
    }
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, "Введите заголовок задания.", reply_markup=admin_cancel_markup())


def start_custom_task_removal(call: types.CallbackQuery, placement: str) -> None:
    user_states[call.from_user.id] = {
        "mode": "admin_remove_custom_task",
        "placement": placement,
    }
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, "Введите ID задания для удаления.", reply_markup=admin_cancel_markup())


def start_required_channel_add(call: types.CallbackQuery, category: str) -> None:
    user_states[call.from_user.id] = {
        "mode": "admin_add_channel",
        "category": category,
        "step": "title",
        "data": {},
    }
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, "Введите название канала.", reply_markup=admin_cancel_markup())


def start_required_channel_remove(call: types.CallbackQuery) -> None:
    user_states[call.from_user.id] = {"mode": "admin_remove_channel"}
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, "Введите ID канала для удаления.", reply_markup=admin_cancel_markup())


def prompt_payout_channel(call: types.CallbackQuery) -> None:
    user_states[call.from_user.id] = {"mode": "admin_set_payout_channel"}
    bot.answer_callback_query(call.id)
    bot.send_message(
        call.message.chat.id,
        "Отправьте @username или ID канала для уведомлений о выводах.",
        reply_markup=admin_cancel_markup(),
    )


def start_broadcast_flow(call: types.CallbackQuery) -> None:
    user_states[call.from_user.id] = {"mode": "admin_broadcast"}
    bot.answer_callback_query(call.id)
    bot.send_message(
        call.message.chat.id,
        "Отправьте текст для рассылки всем пользователям.",
        reply_markup=admin_cancel_markup(),
    )


def run_broadcast(text: str) -> Tuple[int, int]:
    success = 0
    failed = 0
    for user_id in db.all_user_ids():
        try:
            bot.send_message(user_id, text, disable_web_page_preview=True)
            success += 1
        except ApiException as exc:
            logger.debug("Не удалось отправить сообщение %s: %s", user_id, exc)
            failed += 1
    return success, failed


def show_reserve_panel(call: types.CallbackQuery) -> None:
    """Показывает панель управления резервом Crypto Pay"""
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("⚙️ Настройки", callback_data="admin:reservesettings"))
    
    crypto = get_crypto_client()
    if not crypto:
        # Даже без токена показываем панель с настройками
        lines = [
            "💸 Резерв Crypto Pay",
            "",
            "⚠️ <b>Crypto Pay API не настроен</b>",
            "",
            "Для работы с резервом необходимо:",
            "1. Получить токен в @CryptoBot → Crypto Pay → Создать приложение",
            "2. Указать токен в настройках ниже",
            "3. Настроить актив для выплат (USDT, TON, и т.д.)",
            "",
            "После настройки здесь будет отображаться баланс резерва."
        ]
        kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu"))
        admin_update_message(call, "\n".join(lines), kb)
        bot.answer_callback_query(call.id)
        return
    
    # Токен есть - пытаемся получить баланс
    try:
        balances = crypto.get_balance()
        lines = ["💸 Резерв Crypto Pay", ""]
        
        if isinstance(balances, list) and len(balances) > 0:
            for item in balances:
                if isinstance(item, dict):
                    # Пытаемся найти код валюты в разных полях
                    asset_name = item.get('asset') or item.get('currency_code') or 'Unknown'
                    
                    available = dec(item.get('available', '0'))
                    onhold = dec(item.get('onhold', '0'))
                    
                    # Показываем только если есть баланс или он был в движении
                    if available > 0 or onhold > 0:
                        lines.append(f"<b>{asset_name}</b>: доступно {available} / удержано {onhold}")
                    
                    # Если Unknown и есть баланс - выводим ключи прямо в сообщение
                    if asset_name == 'Unknown' and (available > 0 or onhold > 0):
                        keys_str = str(list(item.keys()))
                        lines.append(f"⚠️ Неизвестная структура: <code>{keys_str}</code>")
                        # Также попробуем вывести весь item если он небольшой
                        if len(str(item)) < 100:
                             lines.append(f"Item: <code>{item}</code>")
        else:
            lines.append("Балансы пусты")
        
        # Добавляем кнопки пополнения и вывода только если токен работает
        kb.add(
            types.InlineKeyboardButton("➕ Пополнить", callback_data="admin:reserveinvoice"),
            types.InlineKeyboardButton("➖ Вывести", callback_data="admin:reservecashout"),
        )
    except Exception as exc:
        logger.error(f"Ошибка получения баланса Crypto Pay: {exc}", exc_info=True)
        lines = [
            "💸 Резерв Crypto Pay",
            "",
            f"⚠️ <b>Ошибка при получении данных:</b>",
            f"<code>{exc}</code>",
            "",
            "Проверьте токен и настройки."
        ]
    
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu"))
    admin_update_message(call, "\n".join(lines), kb)
    bot.answer_callback_query(call.id)


def show_reserve_settings(call: types.CallbackQuery) -> None:
    lines = ["💳 Crypto Pay настройки", ""]
    for key, (label, _) in RESERVE_SETTING_FIELDS.items():
        value = db.get_setting(key, DEFAULT_SETTINGS.get(key, ""))
        lines.append(f"{label}: <code>{setting_display(key, value)}</code>")
    kb = types.InlineKeyboardMarkup(row_width=1)
    for key, (label, _) in RESERVE_SETTING_FIELDS.items():
        kb.add(types.InlineKeyboardButton(label.split(" (")[0], callback_data=f"admin:reserveset:{key}"))
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin:reserve"))
    admin_update_message(call, "\n".join(lines), kb)
    bot.answer_callback_query(call.id)


def start_reserve_invoice(call: types.CallbackQuery) -> None:
    user_states[call.from_user.id] = {"mode": "admin_reserve_invoice"}
    asset = db.get_setting("reserve_invoice_asset", "USDT") or "USDT"
    bot.answer_callback_query(call.id)
    bot.send_message(
        call.message.chat.id,
        f"Введите сумму для счёта пополнения в <b>{asset}</b>.",
        parse_mode="HTML",
        reply_markup=admin_cancel_markup(),
    )


def start_reserve_cashout(call: types.CallbackQuery) -> None:
    user_states[call.from_user.id] = {"mode": "admin_reserve_cashout"}
    asset = db.get_setting("crypto_pay_asset", "USDT") or "USDT"
    bot.answer_callback_query(call.id)
    bot.send_message(
        call.message.chat.id,
        f"Введите сумму для вывода в <b>{asset}</b>.",
        parse_mode="HTML",
        reply_markup=admin_cancel_markup(),
    )


@bot.message_handler(commands=["start"])
def command_start(message: types.Message) -> None:
    try:
        logger.info(f"Получена команда /start от пользователя {message.from_user.id}")
        ref_id = parse_start_payload(message.text or "")
        user = ensure_member(message, ref_id)
        logger.info(f"Пользователь {user['user_id']} зарегистрирован")
        if not check_subscription(user_id=user["user_id"], chat_id=message.chat.id, category="global"):
            logger.info(f"Пользователь {user['user_id']} не прошел проверку подписки")
            return
        logger.info(f"Отправка главного экрана пользователю {user['user_id']}")
        send_main_screen(message.chat.id, user_id=user["user_id"])
        logger.info(f"Главный экран отправлен пользователю {user['user_id']}")
    except Exception as e:
        logger.error(f"Ошибка в command_start для пользователя {message.from_user.id}: {e}", exc_info=True)
        try:
            bot.reply_to(message, f"❌ Произошла ошибка: {e}")
        except:
            pass


@bot.message_handler(commands=["menu"])
def command_menu(message: types.Message) -> None:
    user = ensure_member(message)
    if not check_subscription(user_id=user["user_id"], chat_id=message.chat.id, category="global"):
        return
    send_main_screen(message.chat.id, user_id=user["user_id"])


@bot.message_handler(commands=["admin"])
def command_admin(message: types.Message) -> None:
    open_admin_panel(message)


def handle_state_message(message: types.Message, user: sqlite3.Row, state: Dict[str, Any]) -> bool:
    mode = state.get("mode")
    if mode == "deposit_amount":
        process_deposit_amount(message, user)
        return True
    if mode == "withdraw_amount":
        process_withdraw_amount(message, user)
        user_states.pop(user["user_id"], None)
        return True
    if mode == "promo_create_task":
        process_promo_create_task(message, user)
        return True
    if mode == "convert_to_promo":
        process_convert_to_promo(message, user)
        return True
    if mode == "admin_balance_adjust":
        process_admin_balance_adjust(message, user, state)
        return True
    if mode == "admin_set_setting":
        key = state.get("key")
        value_type = state.get("value_type", "text")
        context = state.get("context", "settings")
        success, normalized, error = convert_admin_value(value_type, message.text or "")
        if not success or normalized is None:
            admin_reply(message, error)
            return True
        db.set_setting(key, normalized)
        user_states.pop(user["user_id"], None)
        bot.reply_to(message, f"Настройка обновлена: {key} = {normalized}")
        # Optionally refresh related panels
        return True
    if mode == "admin_add_custom_task":
        placement = state.get("placement", "tasks")
        step = state.get("step", "title")
        data = state.setdefault("data", {})
        text = (message.text or "").strip()
        if step == "title":
            if not text:
                admin_reply(message, "Название не может быть пустым.")
                return True
            data["title"] = text
            state["step"] = "description"
            admin_reply(message, "Введите описание (или «нет»).")
            return True
        if step == "description":
            data["description"] = "" if text.lower() in {"нет", "-"} else text
            state["step"] = "url"
            admin_reply(message, "Отправьте ссылку для задания.")
            return True
        if step == "url":
            if not text.startswith("http"):
                admin_reply(message, "Ссылка должна начинаться с http(s).")
                return True
            data["url"] = text
            state["step"] = "button"
            admin_reply(message, "Укажите текст кнопки.")
            return True
        if step == "button":
            if not text:
                admin_reply(message, "Текст кнопки не может быть пустым.")
                return True
            data["button_text"] = text
            state["step"] = "reward"
            admin_reply(message, "Укажите вознаграждение (USDT).")
            return True
        if step == "reward":
            try:
                reward = parse_decimal_input(text, MONEY_QUANT)
            except (InvalidOperation, ValueError):
                admin_reply(message, "Введите корректную сумму.")
                return True
            data["reward"] = reward
            state["step"] = "channel"
            admin_reply(message, "Укажите @канал для проверки (или «нет»).")
            return True
        if step == "channel":
            channel_id = None
            if text.lower() not in {"нет", "-"}:
                channel_id = text
            db.add_custom_task(
                placement=placement,
                title=data.get("title", "Задание"),
                description=data.get("description", ""),
                button_text=data.get("button_text", "Открыть"),
                url=data.get("url", ""),
                channel_id=channel_id,
                reward=data.get("reward", Decimal("0")),
            )
            bot.reply_to(message, "Задание добавлено.")
            user_states.pop(user["user_id"], None)
            return True
    if mode == "admin_remove_custom_task":
        try:
            task_id = int((message.text or "").strip())
        except ValueError:
            admin_reply(message, "Введите числовой ID.")
            return True
        if db.deactivate_custom_task(task_id):
            bot.reply_to(message, f"Задание #{task_id} удалено.")
        else:
            admin_reply(message, "Задание не найдено.")
        user_states.pop(user["user_id"], None)
        return True
    if mode == "admin_add_channel":
        step = state.get("step", "title")
        data = state.setdefault("data", {})
        text = (message.text or "").strip()
        if step == "title":
            if not text:
                admin_reply(message, "Название не может быть пустым.")
                return True
            data["title"] = text
            state["step"] = "channel"
            admin_reply(message, "Введите @username или ID канала.")
            return True
        if step == "channel":
            if not text:
                admin_reply(message, "ID не может быть пустым.")
                return True
            data["channel_id"] = text
            state["step"] = "link"
            admin_reply(message, "Отправьте ссылку-приглашение (или «нет»).")
            return True
        if step == "link":
            link = text if text.lower() not in {"нет", "-"} else ""
            if not link:
                channel_alias = data.get("channel_id", "").lstrip("@")
                if channel_alias:
                    link = f"https://t.me/{channel_alias}"
            category = state.get("category", "global")
            try:
                db.add_required_channel(
                    data.get("title", "Канал"),
                    data.get("channel_id", ""),
                    link,
                    category,
                )
                bot.reply_to(message, "Канал добавлен.")
            except sqlite3.Error as exc:
                admin_reply(message, f"Ошибка базы данных: {exc}")
            user_states.pop(user["user_id"], None)
            return True
    if mode == "admin_remove_channel":
        try:
            record_id = int((message.text or "").strip())
        except ValueError:
            admin_reply(message, "Введите числовой ID.")
            return True
        if db.remove_required_channel(record_id):
            bot.reply_to(message, "Канал удалён.")
        else:
            admin_reply(message, "Канал не найден.")
        user_states.pop(user["user_id"], None)
        return True
    if mode == "admin_set_payout_channel":
        identifier = parse_chat_identifier(message.text or "")
        if not identifier:
            admin_reply(message, "Введите корректный канал.")
            return True
        db.set_setting("payout_notify_channel", str(identifier))
        user_states.pop(user["user_id"], None)
        bot.reply_to(message, "Канал уведомлений сохранён.")
        return True
    if mode == "admin_broadcast":
        text = (message.text or "").strip()
        if not text:
            admin_reply(message, "Текст не может быть пустым.")
            return True
        success, failed = run_broadcast(text)
        bot.reply_to(message, f"Рассылка завершена. Успешно: {success}, ошибок: {failed}.")
        user_states.pop(user["user_id"], None)
        return True
    if mode == "admin_reserve_invoice":
        # Обработка пополнения резерва
        try:
            amount = parse_decimal_input(message.text or "", ASSET_QUANT)
        except (InvalidOperation, ValueError):
            admin_reply(message, "❌ Введите корректную сумму.")
            return True
        
        if amount <= 0:
            admin_reply(message, "❌ Сумма должна быть больше нуля.")
            return True
        
        crypto = get_crypto_client()
        if not crypto:
            admin_reply(message, "❌ Crypto Pay не настроен. Укажите токен в настройках резерва.")
            return True
        
        asset = db.get_setting("reserve_invoice_asset", "USDT") or "USDT"
        description = db.get_setting("reserve_invoice_description", "Пополнение резерва")
        
        try:
            invoice = crypto.create_invoice(asset=asset, amount=amount, description=description)
            invoice_url = invoice.get('bot_invoice_url') or invoice.get('pay_url') or ""
            invoice_id = invoice.get('invoice_id', 'N/A')
            
            if not invoice_url:
                admin_reply(message, "❌ Не удалось получить ссылку на счёт.")
                return True
            
            user_states.pop(user["user_id"], None)
            
            response_text = (
                f"✅ <b>Счёт на пополнение резерва создан!</b>\n\n"
                f"💰 Сумма: <code>{amount}</code> {asset}\n"
                f"🔢 ID счёта: <code>{invoice_id}</code>\n"
                f"📝 Описание: {description}\n\n"
                f"Оплатите счёт по ссылке:\n{invoice_url}\n\n"
                f"После оплаты средства поступят на баланс резерва бота."
            )
            
            bot.reply_to(
                message,
                response_text,
                disable_web_page_preview=True,
            )
            
            logger.info(f"Создан счёт пополнения резерва: {invoice_id}, сумма: {amount} {asset}")
            
        except Exception as exc:
            logger.error(f"Ошибка создания счёта пополнения резерва: {exc}")
            admin_reply(message, f"❌ Ошибка создания счёта:\n<code>{exc}</code>")
        
        return True
    if mode == "admin_reserve_cashout":
        # Обработка вывода средств из резерва
        try:
            amount = parse_decimal_input(message.text or "", ASSET_QUANT)
        except (InvalidOperation, ValueError):
            admin_reply(message, "❌ Введите корректную сумму.")
            return True
        
        if amount <= 0:
            admin_reply(message, "❌ Сумма должна быть больше нуля.")
            return True
        
        crypto = get_crypto_client()
        if not crypto:
            admin_reply(message, "❌ Crypto Pay не настроен. Укажите токен в настройках резерва.")
            return True
        
        asset = db.get_setting("crypto_pay_asset", "USDT") or "USDT"
        
        try:
            # Проверяем баланс перед выводом
            balances = crypto.get_balance()
            available_balance = Decimal("0")
            for balance_item in balances:
                if balance_item.get("asset") == asset:
                    available_balance = dec(balance_item.get("available", "0"), "0")
                    break
            
            if available_balance < amount:
                admin_reply(
                    message, 
                    f"❌ Недостаточно средств в резерве!\n\n"
                    f"Доступно: <code>{available_balance}</code> {asset}\n"
                    f"Запрошено: <code>{amount}</code> {asset}"
                )
                return True
            
            check = crypto.create_check(asset=asset, amount=amount)
            check_url = check.get('bot_check_url', '')
            check_id = check.get('check_id', 'N/A')
            
            if not check_url:
                admin_reply(message, "❌ Не удалось получить ссылку на чек.")
                return True
            
            user_states.pop(user["user_id"], None)
            
            response_text = (
                f"✅ <b>Чек на вывод создан!</b>\n\n"
                f"💰 Сумма: <code>{amount}</code> {asset}\n"
                f"🔢 ID чека: <code>{check_id}</code>\n\n"
                f"Активируйте чек по ссылке:\n{check_url}\n\n"
                f"⚠️ Чек может активировать любой пользователь, кто первым перейдет по ссылке!"
            )
            
            bot.reply_to(
                message,
                response_text,
                disable_web_page_preview=True,
            )
            
            logger.info(f"Создан чек вывода из резерва: {check_id}, сумма: {amount} {asset}")
            
        except Exception as exc:
            logger.error(f"Ошибка создания чека вывода из резерва: {exc}")
            admin_reply(message, f"❌ Ошибка создания чека:\n<code>{exc}</code>")
        
        return True
    return False


@bot.message_handler(content_types=["text"])
def handle_text(message: types.Message) -> None:
    if message.chat.type != "private":
        return
    user = ensure_member(message)
    state = user_states.get(user["user_id"])
    if state and handle_state_message(message, user, state):
        return
    text = (message.text or "").strip()
    button_key = resolve_menu_button_key(text)
    if button_key == "menu_btn_cabinet":
        send_personal_cabinet(user, message.chat.id)
    elif button_key == "menu_btn_tasks":
        send_tasks_section(message, user)
    elif button_key == "menu_btn_promo":
        send_promotion_section(user, message.chat.id)
    elif button_key == "menu_btn_referrals":
        send_referrals_section(user, message.chat.id)
    elif button_key == "menu_btn_info":
        send_about_section(message.chat.id)
    elif button_key == "menu_btn_admin":
        open_admin_panel(message)
    else:
        send_main_screen(message.chat.id, user_id=user["user_id"])


def open_admin_panel(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "Недостаточно прав.")
        return
    send_admin_menu(message.chat.id)


def check_flyer_tasks_periodically():
    """Проверка новых заданий от Flyer каждые 10 минут для каждого пользователя"""
    while True:
        try:
            time.sleep(600)  # 10 минут
            flyer = get_flyer_client()
            if not flyer or not flyer.enabled():
                continue
            
            # Получаем всех пользователей
            with db._lock:
                cur = db._conn.execute("SELECT user_id, language_code FROM users")
                users = cur.fetchall()
            
            for user_row in users:
                try:
                    user_id = user_row["user_id"]
                    language_code = user_row.get("language_code")
                    
                    # Получаем текущие задания пользователя
                    user = db.get_user(user_id)
                    if not user:
                        continue
                    
                    old_tasks = db.load_tasks(user_id, "tasks")
                    old_signatures = {task.get("signature") for task in old_tasks if task.get("signature")}
                    
                    # Получаем новые задания от Flyer
                    limit = max(1, int(db.get_setting("flyer_task_limit", "5") or 5))
                    try:
                        flyer_tasks = flyer.get_tasks(
                            user_id=user_id,
                            language_code=language_code,
                            limit=limit,
                        )
                    except Exception as exc:
                        logger.warning(f"Flyer get_tasks failed for user {user_id}: {exc}")
                        continue
                    
                    # Проверяем, есть ли новые задания
                    new_tasks = []
                    for entry in flyer_tasks:
                        signature = entry.get("signature")
                        if signature and signature not in old_signatures:
                            new_tasks.append(entry)
                    
                    # Если есть новые задания, отправляем уведомление
                    if new_tasks:
                        try:
                            bot.send_message(
                                user_id,
                                f"🎉 Вам доступно новое задание в разделе 'Задания'!"
                            )
                            # Обновляем кэш заданий
                            get_or_refresh_tasks(user, "tasks", force=True)
                        except Exception as exc:
                            logger.warning(f"Failed to notify user {user_id} about new tasks: {exc}")
                
                except Exception as exc:
                    logger.warning(f"Error checking Flyer tasks for user {user_row.get('user_id')}: {exc}")
                    continue
        
        except Exception as exc:
            logger.error(f"Error in check_flyer_tasks_periodically: {exc}", exc_info=True)
            time.sleep(60)  # При ошибке ждем минуту перед повтором


if __name__ == "__main__":
    try:
        logger.info("CashLait bot запущен.")
        logger.info(f"Токен бота: {BOT_TOKEN[:10]}... (первые 10 символов)")
        logger.info(f"Имя бота: {BOT_USERNAME}")
        
        # Запускаем фоновую проверку Flyer заданий
        flyer_check_thread = threading.Thread(target=check_flyer_tasks_periodically, daemon=True)
        flyer_check_thread.start()
        logger.info("Фоновая проверка Flyer заданий запущена (каждые 10 минут)")
        
        # Запускаем фоновую проверку подписок каждые 10 минут
        def check_subscriptions_periodically():
            """Проверка подписок каждые 10 минут"""
            while True:
                try:
                    time.sleep(600)  # 10 минут
                    process_subscription_watchlist()
                except Exception as exc:
                    logger.error(f"Ошибка в проверке подписок: {exc}", exc_info=True)
                    time.sleep(60)  # При ошибке ждем минуту перед повтором
        
        subscription_check_thread = threading.Thread(target=check_subscriptions_periodically, daemon=True)
        subscription_check_thread.start()
        logger.info("Фоновая проверка подписок запущена (каждые 10 минут)")
        
        logger.info("Начинаю polling...")
        bot.infinity_polling(none_stop=True, interval=0, timeout=20)
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем.")
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {e}", exc_info=True)
        raise