import threading
import time
import os
import psutil
import telebot
from telebot import types
import sqlite3
import subprocess
import os
import signal
import time
import traceback
import json
from html import escape
import psutil
import sys
import threading
import logging
from datetime import datetime
from aiocryptopay import AioCryptoPay, Networks
import asyncio
import re
try:
    from flyerapi import Flyer, APIError as FlyerAPIError
    FLYER_IMPORTED_FOR_CHECKER = True
except ImportError:
    FLYER_IMPORTED_FOR_CHECKER = False

# =================================================================================
# --------------------------- ОСНОВНЫЕ НАСТРОЙКИ ----------------------------------
# =================================================================================
CREATOR_BOT_TOKEN = '8400644706:AAFjCQDxS73hvhizY4f3v94-vlXLkvqGHdQ'  # 👈 Вставь свой токен конструктора
CRYPTO_PAY_TOKEN = '467156:AA0Mvgp0h5oKZaETFQZqdnCWUZSoPVpAT0W' # 👈 Получи его в @CryptoBot -> Crypto Pay -> Создать приложение

ADMIN_ID = 7585735331 # 👈 ТВОЙ ID АДМИНИНИСТРАТОРА
# Поддержка нескольких глобальных админов (пример):
# Добавьте сюда ID через запятую, чтобы дать доступ к админ-меню
ADMIN_IDS = [
    ADMIN_ID,
]
MAX_BOTS_PER_USER = 5
REF_BOT_SCRIPT_NAME = 'ref_bot.py'
STARS_BOT_SCRIPT_NAME = 'stars_bot.py'
CLICKER_BOT_SCRIPT_NAME = 'clicker_bot.py'
ANONCHAT_BOT_SCRIPT_NAME = 'anonchatik.py'
CASHLAIT_BOT_SCRIPT_NAME = 'cashlait_bot.py'
DICELITE_BOT_SCRIPT_NAME = 'dicelite_bot.py'
EXCHANGE_BOT_SCRIPT_NAME = 'exchange_bot.py'
CLICKER_UNLOCK_CODE = '62927'
ANONCHAT_UNLOCK_CODE = '67576'
CASHLAIT_UNLOCK_CODE = '480034'
DICELITE_UNLOCK_CODE = '915627'
EXCHANGE_UNLOCK_CODE = '568754'
CLICKER_GLOBAL_SETTING_KEY = 'clicker_global_unlocked'
ANONCHAT_GLOBAL_SETTING_KEY = 'anonchat_global_unlocked'
CASHLAIT_GLOBAL_SETTING_KEY = 'cashlait_global_unlocked'
DICELITE_GLOBAL_SETTING_KEY = 'dicelite_global_unlocked'
EXCHANGE_GLOBAL_SETTING_KEY = 'exchange_global_unlocked'
CUSTOMIZATION_UNLOCK_CODE = '73839'
CUSTOMIZATION_SETTING_KEY = 'customization_unlocked'
CUSTOM_BUTTON_SETTING_PREFIX = 'custom_button_text_'
CUSTOM_TEXT_SETTING_PREFIX = 'custom_text_'

CUSTOM_TEXTS_META = {
    'creator_welcome': {
        'setting_key': 'creator_welcome',
        'default': "👋 Добро пожаловать! Выберите действие:",
        'description': "Стартовое приветствие конструктора",
        'max_length': 4000,
        'supports_html': True,
        'single_line': False,
    },
    'create_bot_prompt': {
        'default': "Выберите тип бота для создания 🧰:",
        'description': "Сообщение при выборе типа бота",
        'max_length': 512,
        'supports_html': True,
        'single_line': False,
    },
    'no_bots_placeholder': {
        'default': "У вас пока нет ботов...",
        'description': "Надпись в списке ботов, когда они отсутствуют",
        'max_length': 64,
        'supports_html': False,
        'single_line': True,
    },
    'admin_menu_heading': {
        'default': "👑 Админ-меню",
        'description': "Заголовок при открытии админ-меню",
        'max_length': 128,
        'supports_html': False,
        'single_line': True,
    },
    'my_bots_intro': {
        'default': "Ниже представлен список ваших ботов:",
        'description': "Сообщение перед списком личных ботов",
        'max_length': 512,
        'supports_html': True,
        'single_line': False,
    },
    'constructor_bot_link': {
        'default': "https://t.me/MinxoCreate_bot",
        'description': "Ссылка в кнопке 'Хочу такого же бота'",
        'max_length': 128,
        'supports_html': False,
        'single_line': True,
    },
    'constructor_bot_link_text': {
        'default': "🤖 Хочу такого же бота",
        'description': "Текст кнопки 'Хочу такого же бота'",
        'max_length': 64,
        'supports_html': False,
        'single_line': True,
    },
    'anonchat_constructor_bot_link': {
        'setting_key': 'anonchat_constructor_bot_link',
        'default': 'https://t.me/GrillCreate_bot',
        'description': 'Ссылка на креатора для Анонимного чата',
        'max_length': 128,
        'supports_html': False,
        'single_line': True,
    },
    'anonchat_constructor_bot_link_text': {
        'setting_key': 'anonchat_constructor_bot_link_text',
        'default': '🤖 Хочу такого же бота',
        'description': 'Текст кнопки для Анонимного чата',
        'max_length': 64,
        'supports_html': False,
        'single_line': True,
    },
}

def normalize_creator_link_value(value):
    if not value:
        return ""
    trimmed = str(value).strip()
    if not trimmed:
        return ""
    if trimmed.startswith("@"):
        trimmed = trimmed[1:]
    if not trimmed:
        return ""
    if trimmed.startswith("https://t.me/"):
        return trimmed
    if trimmed.startswith("t.me/"):
        return f"https://{trimmed}"
    return f"https://t.me/{trimmed}"


def derive_creator_label_from_link(link_value):
    if not link_value:
        return ""
    if link_value.startswith("https://t.me/"):
        username = link_value.split("https://t.me/", 1)[1].strip("/")
        if username:
            return f"@{username}"
    return link_value

DEFAULT_BUTTON_TEXTS = {
    'main_create': "➕ Создать бота",
    'main_my_bots': "🤖 Мои боты",
    'main_lists': "📋 Списки ботов",
    'main_wallet': "💰 Личный кабинет",
    'main_about': "ℹ️ О боте",
    'main_admin': "👑 Админ-панель",
    'create_ref': "💸 Реферальный",
    'create_stars': "⭐ Заработок Звёзд",
    'create_clicker': "🖱 Кликер",
    'create_anonchat': "💬 Анонимный чат",
    'create_cashlait': "🚀 PR Бот",
    'create_dicelite': "🎲 DiceLite",
    'create_exchange': "💱 Бот Обменник",
}

BUTTON_KEY_DESCRIPTIONS = {
    'main_create': 'Кнопка "Создать бота"',
    'main_my_bots': 'Кнопка "Мои боты"',
    'main_lists': 'Кнопка "Списки ботов"',
    'main_wallet': 'Кнопка "Личный кабинет"',
    'main_about': 'Кнопка "О боте"',
    'main_admin': 'Кнопка "Админ-панель"',
    'create_ref': 'Кнопка выбора типа "Реферальный"',
    'create_stars': 'Кнопка выбора типа "Заработок Звёзд"',
    'create_clicker': 'Кнопка выбора типа "Кликер"',
    'create_anonchat': 'Кнопка выбора типа "Анонимный чат"',
    'create_cashlait': 'Кнопка выбора типа "CashLait"',
    'create_dicelite': 'Кнопка выбора типа "DiceLite"',
    'create_exchange': 'Кнопка выбора типа "Бот Обменник"',
}

MAIN_MENU_BUTTON_KEYS = [
    'main_create',
    'main_my_bots',
    'main_lists',
    'main_wallet',
    'main_about',
    'main_admin',
]

BOT_CREATION_BUTTON_KEYS = [
    'create_ref',
    'create_stars',
    'create_clicker',
    'create_anonchat',
    'create_cashlait',
    'create_dicelite',
    'create_exchange',
]
CUSTOMIZATION_RESET_COMMANDS = {'сброс', 'reset', 'default', 'стандарт'}
DB_NAME = 'creator_data2.db'
MIN_CREATOR_WITHDRAWAL = 50.0
TTL_STATES_SECONDS = 1800
# =================================================================================

# =================================================================================
# --------------------------- НАСТРОЙКИ КНОПОК "О БОТЕ" ---------------------------
# =================================================================================
PROJECT_START_DATE = "10.06.2025"
ADMIN_CHAT_LINK = "https://t.me/MinxoAdminChat"
CHANNEL_LINK = "https://t.me/MinxoNews"
# =================================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if not os.path.exists('logs'): os.makedirs('logs')
if not os.path.exists('dbs'): os.makedirs('dbs')

db_lock = threading.Lock()
conn = sqlite3.connect(DB_NAME, check_same_thread=False)
conn.row_factory = sqlite3.Row
bot = telebot.TeleBot(CREATOR_BOT_TOKEN)
user_states = {}
bots_broadcast_running = False

async_loop = asyncio.new_event_loop()

def run_async_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

loop_thread = threading.Thread(target=run_async_loop, args=(async_loop,), daemon=True)

def run_async_task(coro):
    future = asyncio.run_coroutine_threadsafe(coro, async_loop)
    return future.result()

def row_to_dict(record):
    if record is None:
        return None
    if isinstance(record, dict):
        return record
    if isinstance(record, sqlite3.Row):
        return {key: record[key] for key in record.keys()}
    return record

def is_admin(user_id: int) -> bool:
    try:
        return int(user_id) in set(int(x) for x in ADMIN_IDS)
    except Exception:
        return user_id == ADMIN_ID

if "YOUR_CRYPTO_PAY_API_TOKEN" not in CRYPTO_PAY_TOKEN:
    crypto_pay = AioCryptoPay(token=CRYPTO_PAY_TOKEN, network=Networks.MAIN_NET)
else:
    crypto_pay = None
    logging.warning("ВНИМАНИЕ: Не указан токен для Crypto Pay. Оплата VIP будет недоступна.")

# --- Helpers for Crypto Pay safe usage ---
def is_crypto_token_configured() -> bool:
    """Return True if Crypto Pay token seems configured (non-empty and not placeholder)."""
    token = None
    try:
        # Prefer dynamic setting when available
        token = get_setting('crypto_pay_token')
    except Exception:
        token = None
    if not token:
        token = CRYPTO_PAY_TOKEN
    if not token:
        return False
    token_str = str(token).strip()
    if not token_str or token_str in ('—', 'YOUR_CRYPTO_PAY_API_TOKEN'):
        return False
    return True

def get_crypto_client():
    """Lazily initialize and return Crypto Pay client based on the current token.
    Returns None if token is not configured or initialization failed.
    """
    global crypto_pay, CRYPTO_PAY_TOKEN
    if not is_crypto_token_configured():
        return None
    # Ensure CRYPTO_PAY_TOKEN reflects the latest saved setting
    try:
        saved = get_setting('crypto_pay_token')
        if saved:
            CRYPTO_PAY_TOKEN = saved
    except Exception:
        pass
    if crypto_pay is None:
        try:
            crypto_pay = AioCryptoPay(token=CRYPTO_PAY_TOKEN, network=Networks.MAIN_NET)
        except Exception as e:
            logging.error(f"Ошибка инициализации Crypto Pay (lazy): {e}")
            crypto_pay = None
            return None
    return crypto_pay

# =================================================================================
# --------------------------- РАБОТА С БАЗОЙ ДАННЫХ -------------------------------
# =================================================================================

def init_db():
    with db_lock:
        cursor = conn.cursor()
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, balance REAL DEFAULT 0.0,
            frozen_balance REAL DEFAULT 0.0,
            clicker_unlocked BOOLEAN DEFAULT FALSE,
            anonchat_unlocked BOOLEAN DEFAULT FALSE
        )''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS admin_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            resource_link TEXT NOT NULL,
            reward REAL DEFAULT 0.1,
            is_active BOOLEAN DEFAULT TRUE
        )''')

        cursor.execute('''CREATE TABLE IF NOT EXISTS user_completed_admin_tasks (
            user_id INTEGER NOT NULL,
            task_id INTEGER NOT NULL,
            PRIMARY KEY (user_id, task_id)
        )''')

        user_columns = [desc[1] for desc in cursor.execute("PRAGMA table_info(users)").fetchall()]
        if 'frozen_balance' not in user_columns:
            cursor.execute("ALTER TABLE users ADD COLUMN frozen_balance REAL DEFAULT 0.0")
            logging.info("Колонка 'frozen_balance' добавлена в таблицу 'users'.")
        if 'clicker_unlocked' not in user_columns:
            cursor.execute("ALTER TABLE users ADD COLUMN clicker_unlocked BOOLEAN DEFAULT FALSE")
            logging.info("Колонка 'clicker_unlocked' добавлена в таблицу 'users'.")
        if 'anonchat_unlocked' not in user_columns:
            cursor.execute("ALTER TABLE users ADD COLUMN anonchat_unlocked BOOLEAN DEFAULT FALSE")
            logging.info("Колонка 'anonchat_unlocked' добавлена в таблицу 'users'.")
        if 'cashlait_unlocked' not in user_columns:
            cursor.execute("ALTER TABLE users ADD COLUMN cashlait_unlocked BOOLEAN DEFAULT FALSE")
            logging.info("Колонка 'cashlait_unlocked' добавлена в таблицу 'users'.")
        if 'dicelite_unlocked' not in user_columns:
            cursor.execute("ALTER TABLE users ADD COLUMN dicelite_unlocked BOOLEAN DEFAULT FALSE")
            logging.info("Колонка 'dicelite_unlocked' добавлена в таблицу 'users'.")
        if 'exchange_unlocked' not in user_columns:
            cursor.execute("ALTER TABLE users ADD COLUMN exchange_unlocked BOOLEAN DEFAULT FALSE")
            logging.info("Колонка 'exchange_unlocked' добавлена в таблицу 'users'.")

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS bots (
            id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL, bot_username TEXT, bot_token TEXT,
            status TEXT DEFAULT 'unconfigured', pid INTEGER, start_time INTEGER,
            bot_type TEXT DEFAULT 'ref',
            admins TEXT DEFAULT '[]', welcome_message TEXT DEFAULT '👋 Добро пожаловать!',
            
            ref_reward_1 REAL DEFAULT 1.0, ref_reward_2 REAL DEFAULT 0.1, withdrawal_limit REAL DEFAULT 100.0,
            withdrawal_method_text TEXT DEFAULT 'Payeer-кошелек', payout_channel TEXT,
            chat_link TEXT, regulations_text TEXT, vip_status BOOLEAN DEFAULT FALSE,
            flyer_op_enabled BOOLEAN DEFAULT FALSE, flyer_api_key TEXT, flyer_limit INTEGER DEFAULT 5 NOT NULL,

            stars_payments_channel TEXT, stars_support_chat TEXT, stars_flyer_api_key TEXT,
            stars_op_enabled BOOLEAN DEFAULT FALSE, 
            stars_welcome_bonus REAL DEFAULT 2.0,
            stars_daily_bonus REAL DEFAULT 1.0, 
            stars_daily_cooldown INTEGER DEFAULT 24,
            stars_ref_bonus_referrer REAL DEFAULT 15.0, 
            stars_ref_bonus_new_user REAL DEFAULT 10.0
        )''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS creator_withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL,
            details TEXT, status TEXT DEFAULT 'pending', created_at DATETIME
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY, value TEXT
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS crypto_payments (
            invoice_id INTEGER PRIMARY KEY,
            bot_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            status TEXT DEFAULT 'pending' 
        )''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS pending_flyer_rewards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER NOT NULL,
            bot_id INTEGER NOT NULL,
            task_signature TEXT NOT NULL UNIQUE,
            amount REAL NOT NULL,
            check_after_timestamp DATETIME NOT NULL
        )''')
        
        try:
            flyer_rewards_columns = [desc[1] for desc in cursor.execute("PRAGMA table_info(pending_flyer_rewards)").fetchall()]
            if 'task_signature' not in flyer_rewards_columns:
                cursor.execute("DROP TABLE pending_flyer_rewards")
                cursor.execute('''CREATE TABLE pending_flyer_rewards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_id INTEGER NOT NULL,
                    bot_id INTEGER NOT NULL,
                    task_signature TEXT NOT NULL UNIQUE,
                    amount REAL NOT NULL,
                    check_after_timestamp DATETIME NOT NULL
                )''')
                logging.info("Таблица 'pending_flyer_rewards' была пересоздана с правильной структурой.")
        except sqlite3.OperationalError:
             pass

        bot_columns = [desc[1] for desc in cursor.execute("PRAGMA table_info(bots)").fetchall()]
        new_columns = {
            'vip_status': "BOOLEAN DEFAULT FALSE",
            'stars_welcome_bonus': "REAL DEFAULT 2.0",
            'stars_daily_bonus': "REAL DEFAULT 1.0",
            'stars_daily_cooldown': "INTEGER DEFAULT 24",
            'stars_ref_bonus_referrer': "REAL DEFAULT 15.0",
            'stars_ref_bonus_new_user': "REAL DEFAULT 10.0",
            # Clicker-specific settings
            'click_reward_min': "REAL DEFAULT 0.001",
            'click_reward_max': "REAL DEFAULT 0.005",
            'energy_max': "INTEGER DEFAULT 1000",
            'energy_regen_rate': "INTEGER DEFAULT 2",
            'welcome_bonus_clicker': "REAL DEFAULT 1.0",
            'daily_bonus_clicker': "REAL DEFAULT 0.5",
            'daily_bonus_cooldown_clicker': "INTEGER DEFAULT 12",
            'ref_bonus_referrer_clicker': "REAL DEFAULT 0.2",
            'ref_bonus_new_user_clicker': "REAL DEFAULT 0.1",
            'withdrawal_min_clicker': "REAL DEFAULT 10.0",
            'withdrawal_method_text_clicker': "TEXT DEFAULT 'Payeer-кошелек'",
            'payments_channel_clicker': "TEXT",
            'support_chat_clicker': "TEXT",
            'clicker_flyer_api_key': "TEXT",
            'clicker_op_enabled': "BOOLEAN DEFAULT FALSE",
            # Anonchat-specific settings
            'anonchat_channel_id': "TEXT",
            'anonchat_vip_price': "REAL DEFAULT 45.0",
            'anonchat_welcome_message': "TEXT DEFAULT 'Добро пожаловать! Начните общение 🐣.'",
            'anonchat_crypto_api_token': "TEXT",
            'anonchat_flyer_api_key': "TEXT",
            'anonchat_flyer_tasks_limit': "INTEGER DEFAULT 5",
            # CashLait-specific settings
            'cashlait_flyer_api_key': "TEXT",
            'cashlait_crypto_pay_token': "TEXT",
            'cashlait_currency_symbol': "TEXT",
            'cashlait_welcome_text': "TEXT",
            # DiceLite-specific settings
            'dicelite_crypto_pay_token': "TEXT",
            'dicelite_welcome_text': "TEXT",
            # Exchange-specific settings
            'exchange_welcome_text': "TEXT",
        }
        
        for col, col_type in new_columns.items():
            if col not in bot_columns:
                try:
                    cursor.execute(f"ALTER TABLE bots ADD COLUMN {col} {col_type}")
                    logging.info(f"Колонка '{col}' добавлена в таблицу 'bots'.")
                except sqlite3.OperationalError as e:
                    logging.error(f"Не удалось добавить колонку {col}: {e}")

        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('op_reward', '1.0')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('stars_sub_reward', '1.0')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('vip_price', '120.0')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('creator_price', '500.0')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('cashlait_price', '1.0')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('cashlait_task_price', '0.1')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('cashlait_min_completions', '10')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('dicelite_price', '1.0')")
        # Watermark toggle for creator welcome message (enabled by default)
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('creator_watermark_enabled', '1')")
        # Global toggle for '📋 Списки ботов' feature (enabled by default)
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('bots_list_feature_enabled', '1')")
        # Удалено: настройка цены 'creator_price' не используется в этой версии
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('bots_list_min_users', '30')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('bots_list_pinned', '[]')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('bots_list_manual', '[]')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('bots_list_hidden', '[]')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (CLICKER_GLOBAL_SETTING_KEY, '0'))
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (ANONCHAT_GLOBAL_SETTING_KEY, '0'))
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (CASHLAIT_GLOBAL_SETTING_KEY, '0'))
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (DICELITE_GLOBAL_SETTING_KEY, '0'))
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, '0')", (EXCHANGE_GLOBAL_SETTING_KEY,))
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, '0')", (CUSTOMIZATION_SETTING_KEY,))

        # Синхронизируем глобальный доступ к типу 'Кликер' с данными пользователей
        cursor.execute("SELECT value FROM settings WHERE key = ?", (CLICKER_GLOBAL_SETTING_KEY,))
        clicker_setting_row = cursor.fetchone()
        clicker_global_enabled = False
        if clicker_setting_row and clicker_setting_row[0] is not None:
            clicker_global_enabled = str(clicker_setting_row[0]).strip().lower() in ('1', 'true', 'yes', 'on', 'enabled')
        cursor.execute("SELECT 1 FROM users WHERE clicker_unlocked = 1 LIMIT 1")
        clicker_unlocked_exists = cursor.fetchone() is not None
        if clicker_unlocked_exists and not clicker_global_enabled:
            cursor.execute("REPLACE INTO settings (key, value) VALUES (?, '1')", (CLICKER_GLOBAL_SETTING_KEY,))
            clicker_global_enabled = True
            logging.info("Глобальный доступ к типу 'Кликер' восстановлен автоматически на основании существующих пользователей.")
        if clicker_global_enabled:
            cursor.execute("UPDATE users SET clicker_unlocked = 1 WHERE clicker_unlocked IS NULL OR clicker_unlocked = 0")
        
        # Синхронизируем глобальный доступ к типу 'Анонимный чат' с данными пользователей
        cursor.execute("SELECT value FROM settings WHERE key = ?", (ANONCHAT_GLOBAL_SETTING_KEY,))
        anonchat_setting_row = cursor.fetchone()
        anonchat_global_enabled = False
        if anonchat_setting_row and anonchat_setting_row[0] is not None:
            anonchat_global_enabled = str(anonchat_setting_row[0]).strip().lower() in ('1', 'true', 'yes', 'on', 'enabled')
        cursor.execute("SELECT 1 FROM users WHERE anonchat_unlocked = 1 LIMIT 1")
        anonchat_unlocked_exists = cursor.fetchone() is not None
        if anonchat_unlocked_exists and not anonchat_global_enabled:
            cursor.execute("REPLACE INTO settings (key, value) VALUES (?, '1')", (ANONCHAT_GLOBAL_SETTING_KEY,))
            anonchat_global_enabled = True
            logging.info("Глобальный доступ к типу 'Анонимный чат' восстановлен автоматически на основании существующих пользователей.")
        if anonchat_global_enabled:
            cursor.execute("UPDATE users SET anonchat_unlocked = 1 WHERE anonchat_unlocked IS NULL OR anonchat_unlocked = 0")
        
        # Синхронизируем глобальный доступ к типу 'CashLait' с данными пользователей
        cursor.execute("SELECT value FROM settings WHERE key = ?", (CASHLAIT_GLOBAL_SETTING_KEY,))
        cashlait_setting_row = cursor.fetchone()
        cashlait_global_enabled = False
        if cashlait_setting_row and cashlait_setting_row[0] is not None:
            cashlait_global_enabled = str(cashlait_setting_row[0]).strip().lower() in ('1', 'true', 'yes', 'on', 'enabled')
        cursor.execute("SELECT 1 FROM users WHERE cashlait_unlocked = 1 LIMIT 1")
        cashlait_unlocked_exists = cursor.fetchone() is not None
        if cashlait_unlocked_exists and not cashlait_global_enabled:
            cursor.execute("REPLACE INTO settings (key, value) VALUES (?, '1')", (CASHLAIT_GLOBAL_SETTING_KEY,))
            cashlait_global_enabled = True
            logging.info("Глобальный доступ к типу 'CashLait' восстановлен автоматически на основании существующих пользователей.")
        if cashlait_global_enabled:
            cursor.execute("UPDATE users SET cashlait_unlocked = 1 WHERE cashlait_unlocked IS NULL OR cashlait_unlocked = 0")

        # Синхронизируем глобальный доступ к типу 'DiceLite' с данными пользователей
        cursor.execute("SELECT value FROM settings WHERE key = ?", (DICELITE_GLOBAL_SETTING_KEY,))
        dicelite_setting_row = cursor.fetchone()
        dicelite_global_enabled = False
        if dicelite_setting_row and dicelite_setting_row[0] is not None:
            dicelite_global_enabled = str(dicelite_setting_row[0]).strip().lower() in ('1', 'true', 'yes', 'on', 'enabled')
        cursor.execute("SELECT 1 FROM users WHERE dicelite_unlocked = 1 LIMIT 1")
        dicelite_unlocked_exists = cursor.fetchone() is not None
        if dicelite_unlocked_exists and not dicelite_global_enabled:
            cursor.execute("REPLACE INTO settings (key, value) VALUES (?, '1')", (DICELITE_GLOBAL_SETTING_KEY,))
            dicelite_global_enabled = True
            logging.info("Глобальный доступ к типу 'DiceLite' восстановлен автоматически на основании существующих пользователей.")
        if dicelite_global_enabled:
            cursor.execute("UPDATE users SET dicelite_unlocked = 1 WHERE dicelite_unlocked IS NULL OR dicelite_unlocked = 0")

        # Синхронизируем глобальный доступ к типу 'Бот Обменник' с данными пользователей
        cursor.execute("SELECT value FROM settings WHERE key = ?", (EXCHANGE_GLOBAL_SETTING_KEY,))
        exchange_setting_row = cursor.fetchone()
        exchange_global_enabled = False
        if exchange_setting_row and exchange_setting_row[0] is not None:
            exchange_global_enabled = str(exchange_setting_row[0]).strip().lower() in ('1', 'true', 'yes', 'on', 'enabled')
        cursor.execute("SELECT 1 FROM users WHERE exchange_unlocked = 1 LIMIT 1")
        exchange_unlocked_exists = cursor.fetchone() is not None
        if exchange_unlocked_exists and not exchange_global_enabled:
            cursor.execute("REPLACE INTO settings (key, value) VALUES (?, '1')", (EXCHANGE_GLOBAL_SETTING_KEY,))
            exchange_global_enabled = True
            logging.info("Глобальный доступ к типу 'Бот Обменник' восстановлен автоматически на основании существующих пользователей.")
        if exchange_global_enabled:
            cursor.execute("UPDATE users SET exchange_unlocked = 1 WHERE exchange_unlocked IS NULL OR exchange_unlocked = 0")

        conn.commit()
        logging.info("База данных успешно инициализирована/обновлена.")

def db_execute(query, params=(), commit=False, fetchone=False, fetchall=False):
    with db_lock:
        cursor = conn.cursor()
        cursor.execute(query, params)
        if commit:
            conn.commit()
            return cursor.lastrowid
        if fetchone:
            return cursor.fetchone()
        if fetchall:
            return cursor.fetchall()
        return cursor

def get_child_bot_user_count(bot_id, bot_type):
    db_filename_map = {
        'ref': f"dbs/bot_{bot_id}_data.db",
        'stars': f"dbs/bot_{bot_id}_stars_data.db",
        'clicker': f"dbs/bot_{bot_id}_clicker_data.db",
        'anonchat': f"dbs/bot_{bot_id}_anonchat.db",
        'cashlait': f"dbs/bot_{bot_id}_cashlait.db",
        'dicelite': f"dbs/bot_{bot_id}_dicelite.db",
    }
    db_filename = db_filename_map.get(bot_type, f"dbs/bot_{bot_id}_data.db")
    try:
        if not os.path.exists(db_filename):
            return 0
        child_conn = sqlite3.connect(f'file:{db_filename}?mode=ro', uri=True)
        count = child_conn.cursor().execute("SELECT COUNT(*) FROM users").fetchone()[0]
        child_conn.close()
        return count
    except Exception:
        return 0

def update_bot_setting(bot_id, setting_name, new_value):
    allowed_settings = [
        'bot_token', 'bot_username', 'ref_reward_1', 'ref_reward_2', 'withdrawal_limit', 'status',
        'withdrawal_method_text', 'payout_channel', 'chat_link', 'regulations_text', 'vip_status', 
        'admins', 'owner_id', 'welcome_message', 'flyer_op_enabled', 'flyer_api_key', 'flyer_limit',
        'stars_payments_channel', 'stars_support_chat', 'stars_flyer_api_key', 'stars_welcome_bonus', 'stars_op_enabled',
        'stars_daily_bonus', 'stars_daily_cooldown', 'stars_ref_bonus_referrer', 'stars_ref_bonus_new_user',
        # Clicker-specific settings
        'click_reward_min', 'click_reward_max', 'energy_max', 'energy_regen_rate', 'welcome_bonus_clicker',
        'daily_bonus_clicker', 'daily_bonus_cooldown_clicker', 'ref_bonus_referrer_clicker', 'ref_bonus_new_user_clicker',
        'withdrawal_min_clicker', 'withdrawal_method_text_clicker', 'payments_channel_clicker', 'support_chat_clicker',
        'clicker_flyer_api_key', 'clicker_op_enabled',
        # Anonchat-specific settings
        'anonchat_channel_id', 'anonchat_vip_price', 'anonchat_welcome_message', 'anonchat_crypto_api_token',
        'anonchat_flyer_api_key', 'anonchat_flyer_tasks_limit',
        # CashLait-specific settings
        'cashlait_flyer_api_key', 'cashlait_crypto_pay_token', 'cashlait_currency_symbol',
        # DiceLite-specific settings
        'dicelite_crypto_pay_token', 'dicelite_welcome_text',
    ]
    if setting_name in allowed_settings:
        db_execute(f"UPDATE bots SET {setting_name} = ? WHERE id = ?", (new_value, bot_id), commit=True)

def build_public_bots_list(min_users: int):
    """Строит список ботов для раздела '📋 Списки ботов' с учетом закрепов/ручного/скрытых.
    Возвращает список кортежей: (bot_id, username_or_None, bot_type, users_count, link)
    Показаны только активные (запущенные) боты.
    """
    try:
        pinned = json.loads(get_setting('bots_list_pinned') or '[]')
        manual = json.loads(get_setting('bots_list_manual') or '[]')
        hidden = set(json.loads(get_setting('bots_list_hidden') or '[]'))
    except Exception:
        pinned, manual, hidden = [], [], set()

    bots = db_execute("SELECT id, bot_username, bot_type, status FROM bots ORDER BY id DESC", fetchall=True) or []
    listed: List[tuple] = []

    # Добавляем закрепленные сверху, независимо от порога, если не скрыты
    for bid in pinned:
        b = next((x for x in bots if x['id'] == bid), None)
        if not b or bid in hidden:
            continue
        if b['status'] != 'running':
            continue
        cnt = get_child_bot_user_count(b['id'], b['bot_type'])
        link = f"https://t.me/{b['bot_username']}" if b['bot_username'] else "—"
        listed.append((b['id'], b['bot_username'] or 'Без имени', b['bot_type'], cnt, link))

    # Остальные по порогу, исключая скрытых и уже добавленных
    added_ids = {x[0] for x in listed}
    for b in bots:
        if b['id'] in added_ids or b['id'] in hidden:
            continue
        if b['status'] != 'running':
            continue
        cnt = get_child_bot_user_count(b['id'], b['bot_type'])
        if cnt >= min_users:
            link = f"https://t.me/{b['bot_username']}" if b['bot_username'] else "—"
            listed.append((b['id'], b['bot_username'] or 'Без имени', b['bot_type'], cnt, link))

    # Добавляем вручную добавленные, если не скрыты и еще не в списке
    for bid in manual:
        if bid in hidden or bid in added_ids:
            continue
        b = next((x for x in bots if x['id'] == bid), None)
        if not b:
            continue
        if b['status'] != 'running':
            continue
        cnt = get_child_bot_user_count(b['id'], b['bot_type'])
        link = f"https://t.me/{b['bot_username']}" if b['bot_username'] else "—"
        listed.append((b['id'], b['bot_username'] or 'Без имени', b['bot_type'], cnt, link))

    return listed

def get_setting(key):
    result = db_execute("SELECT value FROM settings WHERE key = ?", (key,), fetchone=True)
    return result[0] if result else None

def set_setting(key, value):
    db_execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value), commit=True)

def delete_setting(key):
    db_execute("DELETE FROM settings WHERE key = ?", (key,), commit=True)

def is_customization_unlocked():
    try:
        value = get_setting(CUSTOMIZATION_SETTING_KEY)
    except Exception:
        value = None
    if value is None:
        return False
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on', 'enabled')

def get_custom_button_text(key):
    default_text = DEFAULT_BUTTON_TEXTS.get(key, "")
    try:
        stored = get_setting(f"{CUSTOM_BUTTON_SETTING_PREFIX}{key}")
    except Exception:
        stored = None
    if stored is None:
        return default_text
    text_value = str(stored)
    if not text_value.strip():
        return default_text
    return text_value

def set_custom_button_text(key, text):
    set_setting(f"{CUSTOM_BUTTON_SETTING_PREFIX}{key}", text)

def reset_custom_button_text(key):
    delete_setting(f"{CUSTOM_BUTTON_SETTING_PREFIX}{key}")

def _resolve_custom_text_setting_key(key):
    meta = CUSTOM_TEXTS_META.get(key, {})
    return meta.get('setting_key') or f"{CUSTOM_TEXT_SETTING_PREFIX}{key}"

def get_custom_text(key):
    meta = CUSTOM_TEXTS_META.get(key)
    if not meta:
        return ""
    setting_key = _resolve_custom_text_setting_key(key)
    try:
        stored = get_setting(setting_key)
    except Exception:
        stored = None
    if stored is None:
        return meta.get('default', "")
    text_value = str(stored)
    if not text_value.strip():
        return meta.get('default', "")
    return text_value

def set_custom_text(key, text):
    if key not in CUSTOM_TEXTS_META:
        return
    setting_key = _resolve_custom_text_setting_key(key)
    set_setting(setting_key, text)

def reset_custom_text(key):
    if key not in CUSTOM_TEXTS_META:
        return
    setting_key = _resolve_custom_text_setting_key(key)
    delete_setting(setting_key)

def get_main_menu_button_texts():
    return {
        'create': get_custom_button_text('main_create'),
        'my_bots': get_custom_button_text('main_my_bots'),
        'lists': get_custom_button_text('main_lists'),
        'wallet': get_custom_button_text('main_wallet'),
        'about': get_custom_button_text('main_about'),
        'admin': get_custom_button_text('main_admin'),
    }

def get_bot_creation_button_texts():
    return {
        'ref': get_custom_button_text('create_ref'),
        'stars': get_custom_button_text('create_stars'),
        'clicker': get_custom_button_text('create_clicker'),
        'anonchat': get_custom_button_text('create_anonchat'),
        'cashlait': get_custom_button_text('create_cashlait'),
        'dicelite': get_custom_button_text('create_dicelite'),
        'exchange': get_custom_button_text('create_exchange'),
    }

def is_clicker_unlocked_globally():
    try:
        value = get_setting(CLICKER_GLOBAL_SETTING_KEY)
    except Exception:
        value = None
    if value is None:
        return False
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on', 'enabled')

def unlock_clicker_globally():
    try:
        set_setting(CLICKER_GLOBAL_SETTING_KEY, '1')
        db_execute(
            "UPDATE users SET clicker_unlocked = 1 WHERE clicker_unlocked IS NULL OR clicker_unlocked = 0",
            commit=True
        )
    except Exception as exc:
        logging.error(f"Не удалось установить глобальный доступ к кликеру: {exc}")
        raise

def is_anonchat_unlocked_globally():
    try:
        value = get_setting(ANONCHAT_GLOBAL_SETTING_KEY)
    except Exception:
        value = None
    if value is None:
        return False
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on', 'enabled')

def unlock_anonchat_globally():
    try:
        set_setting(ANONCHAT_GLOBAL_SETTING_KEY, '1')
        db_execute(
            "UPDATE users SET anonchat_unlocked = 1 WHERE anonchat_unlocked IS NULL OR anonchat_unlocked = 0",
            commit=True
        )
    except Exception as exc:
        logging.error(f"Не удалось установить глобальный доступ к анонимному чату: {exc}")
        raise

def is_cashlait_unlocked_globally():
    try:
        value = get_setting(CASHLAIT_GLOBAL_SETTING_KEY)
    except Exception:
        value = None
    if value is None:
        return False
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on', 'enabled')

def unlock_cashlait_globally():
    try:
        set_setting(CASHLAIT_GLOBAL_SETTING_KEY, '1')
        db_execute(
            "UPDATE users SET cashlait_unlocked = 1 WHERE cashlait_unlocked IS NULL OR cashlait_unlocked = 0",
            commit=True
        )
    except Exception as exc:
        logging.error(f"Не удалось установить глобальный доступ к CashLait: {exc}")
        raise

def is_dicelite_unlocked_globally():
    try:
        value = get_setting(DICELITE_GLOBAL_SETTING_KEY)
    except Exception:
        value = None
    if value is None:
        return False
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on', 'enabled')

def unlock_dicelite_globally():
    try:
        set_setting(DICELITE_GLOBAL_SETTING_KEY, '1')
        db_execute(
            "UPDATE users SET dicelite_unlocked = 1 WHERE dicelite_unlocked IS NULL OR dicelite_unlocked = 0",
            commit=True
        )
    except Exception as exc:
        logging.error(f"Не удалось установить глобальный доступ к DiceLite: {exc}")
        raise

def is_exchange_unlocked_globally():
    try:
        value = get_setting(EXCHANGE_GLOBAL_SETTING_KEY)
    except Exception:
        value = None
    if value is None:
        return False
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on', 'enabled')

def unlock_exchange_globally():
    try:
        set_setting(EXCHANGE_GLOBAL_SETTING_KEY, '1')
        db_execute(
            "UPDATE users SET exchange_unlocked = 1 WHERE exchange_unlocked IS NULL OR exchange_unlocked = 0",
            commit=True
        )
    except Exception as exc:
        logging.error(f"Не удалось установить глобальный доступ к Exchange Bot: {exc}")
        raise

def get_user(user_id, username=None):
    user = row_to_dict(db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True))
    if user is None: 
        db_execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", (user_id, username), commit=True)
        user = row_to_dict(db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True))
    if user is not None:
        try:
            needs_sync_clicker = (
                hasattr(user, 'keys')
                and 'clicker_unlocked' in user.keys()
                and user['clicker_unlocked'] in (None, 0, '0', False)
            )
            if needs_sync_clicker and is_clicker_unlocked_globally():
                db_execute(
                    "UPDATE users SET clicker_unlocked = 1 WHERE user_id = ?",
                    (user_id,),
                    commit=True
                )
                user = row_to_dict(db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True))
            
            needs_sync_anonchat = (
                hasattr(user, 'keys')
                and 'anonchat_unlocked' in user.keys()
                and user['anonchat_unlocked'] in (None, 0, '0', False)
            )
            if needs_sync_anonchat and is_anonchat_unlocked_globally():
                db_execute(
                    "UPDATE users SET anonchat_unlocked = 1 WHERE user_id = ?",
                    (user_id,),
                    commit=True
                )
                user = row_to_dict(db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True))
            
            needs_sync_cashlait = (
                hasattr(user, 'keys')
                and 'cashlait_unlocked' in user.keys()
                and user['cashlait_unlocked'] in (None, 0, '0', False)
            )
            if needs_sync_cashlait and is_cashlait_unlocked_globally():
                db_execute(
                    "UPDATE users SET cashlait_unlocked = 1 WHERE user_id = ?",
                    (user_id,),
                    commit=True
                )
                user = row_to_dict(db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True))

            needs_sync_dicelite = (
                hasattr(user, 'keys')
                and 'dicelite_unlocked' in user.keys()
                and user['dicelite_unlocked'] in (None, 0, '0', False)
            )
            if needs_sync_dicelite and is_dicelite_unlocked_globally():
                db_execute(
                    "UPDATE users SET dicelite_unlocked = 1 WHERE user_id = ?",
                    (user_id,),
                    commit=True
                )
                user = row_to_dict(db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True))

            needs_sync_exchange = (
                hasattr(user, 'keys')
                and 'exchange_unlocked' in user.keys()
                and user['exchange_unlocked'] in (None, 0, '0', False)
            )
            if needs_sync_exchange and is_exchange_unlocked_globally():
                db_execute(
                    "UPDATE users SET exchange_unlocked = 1 WHERE user_id = ?",
                    (user_id,),
                    commit=True
                )
                user = row_to_dict(db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True))
        except Exception as exc:
            logging.error(f"Не удалось синхронизировать флаги разблокировки для пользователя {user_id}: {exc}")
    return user

def get_user_bots_count(user_id):
    return db_execute("SELECT COUNT(*) FROM bots WHERE owner_id = ?", (user_id,), fetchone=True)[0]

def get_user_bots(user_id):
    return db_execute("SELECT * FROM bots WHERE owner_id = ? ORDER BY id DESC", (user_id,), fetchall=True)

def get_bot_by_id(bot_id):
    return row_to_dict(db_execute("SELECT * FROM bots WHERE id = ?", (bot_id,), fetchone=True))

def create_bot_in_db(owner_id, bot_type):
    return db_execute("INSERT INTO bots (owner_id, admins, bot_type) VALUES (?, ?, ?)", 
                      (owner_id, json.dumps([owner_id]), bot_type), commit=True)

def update_bot_process_info(bot_id, status, pid, start_time=None):
    db_execute("UPDATE bots SET status = ?, pid = ?, start_time = ? WHERE id = ?", (status, pid, start_time, bot_id), commit=True)

def delete_bot_from_db(bot_id):
    bot_info = get_bot_by_id(bot_id)
    if not bot_info: return
    stop_bot_process(bot_id)
    try: os.remove(f"logs/bot_{bot_id}.log")
    except FileNotFoundError: pass
    db_filename_map = {
        'ref': f"dbs/bot_{bot_id}_data.db",
        'stars': f"dbs/bot_{bot_id}_stars_data.db",
        'clicker': f"dbs/bot_{bot_id}_clicker_data.db",
        'cashlait': f"dbs/bot_{bot_id}_cashlait.db",
        'dicelite': f"dbs/bot_{bot_id}_dicelite.db",
        'exchange': f"dbs/bot_{bot_id}_exchange.db",
    }
    db_filename = db_filename_map.get(bot_info['bot_type'])
    try:
        if db_filename:
            os.remove(db_filename)
    except FileNotFoundError: pass
    db_execute("DELETE FROM bots WHERE id = ?", (bot_id,), commit=True)

def start_bot_process(bot_id):
    try:
        with open("start_debug.log", "a", encoding="utf-8") as f:
            f.write(f"[START] Attempting to launch bot {bot_id}\n")
    except:
        pass

    bot_info = get_bot_by_id(bot_id)
    if not bot_info or not bot_info.get('bot_token'):
        return False, "Сначала установите токен!"
    if bot_info['status'] == 'running':
        return False, "Бот уже запущен."
    try:
        env = os.environ.copy()
        admin_ids_env = []
        try:
            admins_raw = bot_info['admins']
        except (KeyError, IndexError, TypeError):
            admins_raw = None
        if admins_raw:
            try:
                parsed_admins = json.loads(admins_raw)
                if isinstance(parsed_admins, list):
                    for admin_candidate in parsed_admins:
                        try:
                            admin_id = int(admin_candidate)
                        except (TypeError, ValueError):
                            continue
                        if admin_id not in admin_ids_env:
                            admin_ids_env.append(admin_id)
            except (ValueError, TypeError, json.JSONDecodeError):
                logging.warning(f"Не удалось разобрать список админов для бота #{bot_id}")
        try:
            owner_id = bot_info['owner_id']
            if owner_id is not None:
                owner_id_int = int(owner_id)
                if owner_id_int not in admin_ids_env:
                    admin_ids_env.insert(0, owner_id_int)
        except (TypeError, ValueError, KeyError, IndexError):
            pass
        if admin_ids_env:
            env['ADMIN_IDS'] = ",".join(str(admin_id) for admin_id in admin_ids_env)
        script_name = ""
        
        if bot_info['bot_type'] == 'ref':
            script_name = REF_BOT_SCRIPT_NAME
            if bot_info['flyer_api_key']:
                env['FLYER_API_KEY'] = bot_info['flyer_api_key']
        elif bot_info['bot_type'] == 'stars':
            script_name = STARS_BOT_SCRIPT_NAME
            if bot_info['stars_flyer_api_key']:
                 env['FLYER_API_KEY'] = bot_info['stars_flyer_api_key']
        elif bot_info['bot_type'] == 'clicker':
            script_name = CLICKER_BOT_SCRIPT_NAME
            if bot_info['clicker_flyer_api_key']:
                 env['FLYER_API_KEY'] = bot_info['clicker_flyer_api_key']
        elif bot_info['bot_type'] == 'anonchat':
            script_name = ANONCHAT_BOT_SCRIPT_NAME
            env['ANONCHAT_DB'] = os.path.abspath(f"dbs/bot_{bot_id}_anonchat.db")
            if bot_info['bot_token']:
                 env['ANONCHAT_BOT_TOKEN'] = bot_info['bot_token']
            if bot_info['anonchat_flyer_api_key']:
                 env['ANONCHAT_FLYER_API_KEY'] = bot_info['anonchat_flyer_api_key']
            if bot_info['anonchat_flyer_tasks_limit']:
                 env['ANONCHAT_FLYER_TASKS_LIMIT'] = str(bot_info['anonchat_flyer_tasks_limit'])
            # Branding settings
            link_text = get_custom_text('anonchat_constructor_bot_link')
            link_url = get_custom_text('anonchat_constructor_bot_link_text')
            if link_text: env['CREATOR_CONTACT_URL'] = link_text
            if link_url: env['CREATOR_CONTACT_LABEL'] = link_url
            env['CREATOR_BRANDING_ENABLED'] = 'true'
        elif bot_info['bot_type'] == 'cashlait':
            script_name = CASHLAIT_BOT_SCRIPT_NAME
            env['CASHLAIT_DB'] = os.path.abspath(f"dbs/bot_{bot_id}_cashlait.db")
            env['CASHLAIT_FLYER_TASK_LIMIT'] = str(bot_info['flyer_limit'] or 5)
            if bot_info['bot_token']:
                env['CASHLAIT_BOT_TOKEN'] = bot_info['bot_token']
            if bot_info['cashlait_flyer_api_key']:
                env['CASHLAIT_FLYER_API_KEY'] = bot_info['cashlait_flyer_api_key']
            if bot_info['cashlait_crypto_pay_token']:
                env['CASHLAIT_CRYPTO_PAY_TOKEN'] = bot_info['cashlait_crypto_pay_token']
            if bot_info['cashlait_currency_symbol']:
                env['CASHLAIT_CURRENCY_SYMBOL'] = bot_info['cashlait_currency_symbol']
            if bot_info['cashlait_welcome_text']:
                env['CASHLAIT_WELCOME_TEXT'] = bot_info['cashlait_welcome_text']
            
            # Pass custom branding info
            link_text = get_custom_text('constructor_bot_link_text')
            link_url = get_custom_text('constructor_bot_link')
            if link_text: env['CONSTRUCTOR_LINK_TEXT'] = link_text
            if link_url: env['CONSTRUCTOR_LINK_URL'] = link_url
        elif bot_info['bot_type'] == 'dicelite':
            script_name = DICELITE_BOT_SCRIPT_NAME
            env['DICELITE_DB'] = os.path.abspath(f"dbs/bot_{bot_id}_dicelite.db")
            if bot_info['bot_token']:
                env['DICELITE_BOT_TOKEN'] = bot_info['bot_token']
            if bot_info.get('dicelite_crypto_pay_token'):
                env['DICELITE_CRYPTO_PAY_TOKEN'] = bot_info['dicelite_crypto_pay_token']
            if bot_info.get('dicelite_welcome_text'):
                env['DICELITE_WELCOME_TEXT'] = bot_info['dicelite_welcome_text']
            # Creator branding for DiceLite
            link_raw = get_custom_text('constructor_bot_link')
            link_url = normalize_creator_link_value(link_raw)
            link_text = get_custom_text('constructor_bot_link_text')
            label_value = derive_creator_label_from_link(link_url)
            if link_url:
                env['CREATOR_CONTACT_URL'] = link_url
            if label_value:
                env['CREATOR_CONTACT_LABEL'] = label_value
            if link_text:
                env['CREATOR_CONTACT_BUTTON_LABEL'] = link_text
            env['CREATOR_BRANDING'] = 'true' if (link_url and not bot_info['vip_status']) else 'false'
        elif bot_info['bot_type'] == 'exchange':
            script_name = EXCHANGE_BOT_SCRIPT_NAME
            env['EXCHANGE_DB'] = os.path.abspath(f"dbs/bot_{bot_id}_exchange.db")
            if bot_info['bot_token']:
                env['EXCHANGE_BOT_TOKEN'] = bot_info['bot_token']
            link_raw = get_custom_text('constructor_bot_link')
            link_url = normalize_creator_link_value(link_raw)
            link_text = get_custom_text('constructor_bot_link_text')
            label_value = derive_creator_label_from_link(link_url)
            if link_url:
                env['CREATOR_CONTACT_URL'] = link_url
            if label_value:
                env['CREATOR_CONTACT_LABEL'] = label_value
            if link_text:
                env['CREATOR_CONTACT_BUTTON_LABEL'] = link_text
            env['CREATOR_BRANDING'] = 'true' if (link_url and not bot_info['vip_status']) else 'false'
            if bot_info.get('exchange_welcome_text'):
                env['EXCHANGE_WELCOME_TEXT'] = bot_info['exchange_welcome_text']
        else:
            return False, "Неизвестный тип бота."
        
        if bot_info['vip_status']:
            env['VIP_BRANDING_DISABLED'] = 'true'
        else:
            env.setdefault('CREATOR_BRANDING', 'true')
        
        # Ensure logs directory exists
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        log_file = open(f"logs/bot_{bot_id}.log", "a", encoding='utf-8')
        script_path = os.path.join(os.path.dirname(__file__), script_name)
        
        # Verify script exists before launching
        if not os.path.exists(script_path):
            return False, f"Скрипт не найден: {script_name}"
        process = subprocess.Popen(
            [sys.executable, script_path, str(bot_id)],
            stdout=log_file, stderr=log_file, env=env
        )
        log_file.close()
        update_bot_process_info(bot_id, 'running', process.pid, int(time.time()))
        try:
            with open("start_debug.log", "a", encoding="utf-8") as f:
                f.write(f"[SUCCESS] Bot {bot_id} launched with PID {process.pid}\n")
        except:
            pass
        return True, "Бот успешно запущен."
    except Exception as e:
        try:
            with open("start_debug.log", "a", encoding="utf-8") as f:
                f.write(f"[ERROR] Bot {bot_id} failed: {e}\n")
        except:
            pass
        return False, f"Ошибка запуска: {e}"

def stop_bot_process(bot_id):
    bot_info = get_bot_by_id(bot_id)
    if not bot_info or not bot_info['pid']:
        update_bot_process_info(bot_id, 'stopped', None, None)
        return False, "Процесс не найден."
    if bot_info['status'] != 'running': return False, "Бот уже остановлен."
    try:
        p = psutil.Process(bot_info['pid'])
        p.kill()
        update_bot_process_info(bot_id, 'stopped', None, None)
        return True, "Бот успешно остановлен."
    except psutil.NoSuchProcess:
        update_bot_process_info(bot_id, 'stopped', None, None)
        return False, "Процесс не найден, статус сброшен."
    except Exception as e:
        update_bot_process_info(bot_id, 'stopped', None, None)
        return False, f"Ошибка остановки: {e}"

def get_process_resources(pid):
    try:
        process = psutil.Process(pid)
        ram_usage = process.memory_info().rss / (1024 * 1024)
        cpu_usage = process.cpu_percent(interval=0.1)
        return {"ram": ram_usage, "cpu": cpu_usage}
    except psutil.NoSuchProcess:
        return {"ram": 0, "cpu": 0}

def format_uptime(seconds):
    if not seconds: return "не запущен"
    days, rem = divmod(seconds, 86400); hours, rem = divmod(rem, 3600); minutes, seconds = divmod(rem, 60)
    parts = []
    if days: parts.append(f"{int(days)} д.")
    if hours: parts.append(f"{int(hours)} ч.")
    if minutes: parts.append(f"{int(minutes)} мин.")
    if seconds: parts.append(f"{int(seconds)} сек.")
    return " ".join(parts) if parts else "0 сек."

def create_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    main_buttons = get_main_menu_button_texts()
    markup.add(types.KeyboardButton(main_buttons['create']), types.KeyboardButton(main_buttons['my_bots']))
    try:
        bl_enabled_raw = get_setting('bots_list_feature_enabled')
        bl_enabled = str(bl_enabled_raw).strip() in ('1', 'true', 'True') if bl_enabled_raw is not None else True
    except Exception:
        bl_enabled = True
    if bl_enabled:
        markup.add(types.KeyboardButton(main_buttons['lists']))
    markup.add(types.KeyboardButton(main_buttons['wallet']), types.KeyboardButton(main_buttons['about']))
    if is_admin(user_id):
        markup.add(types.KeyboardButton(main_buttons['admin']))
    return markup
    
def create_admin_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    try:
        bl_enabled_raw = get_setting('bots_list_feature_enabled')
        bl_enabled = str(bl_enabled_raw).strip() in ('1', 'true', 'True') if bl_enabled_raw is not None else True
    except Exception:
        bl_enabled = True
    if bl_enabled:
        markup.add(types.InlineKeyboardButton("📣 Рассылка", callback_data="admin_broadcast_start"),
                   types.InlineKeyboardButton("📂 Списки", callback_data="admin_lists_menu"))
    else:
        markup.add(types.InlineKeyboardButton("📣 Рассылка", callback_data="admin_broadcast_start"))

    # New: Bot broadcast entrypoint
    markup.add(types.InlineKeyboardButton("📣 Рассылка в ботах", callback_data="admin_broadcast_bots_menu"))
    markup.add(types.InlineKeyboardButton("📬 Заявки на вывод", callback_data="admin_wd_list"),
               types.InlineKeyboardButton("⚙️ Настройки дохода", callback_data="admin_op_manage"))
    markup.add(types.InlineKeyboardButton("🤖 Все боты", callback_data="admin_bots_all"),
               types.InlineKeyboardButton("₽ Настройки VIP", callback_data="admin_vip_manage"))
    markup.add(types.InlineKeyboardButton("💼 CashLait", callback_data="admin_cashlait_manage"))
    markup.add(types.InlineKeyboardButton("🎲 DiceLite", callback_data="admin_dicelite_manage"))
    markup.add(types.InlineKeyboardButton("💬 Анонимный чат", callback_data="admin_anonchat_manage"))
    markup.add(types.InlineKeyboardButton("💸 Выдать баланс", callback_data="admin_balance_add_start"),
               types.InlineKeyboardButton("🔁 Перевод с удержания", callback_data="admin_hold_transfer_start"))
    markup.add(types.InlineKeyboardButton("🔄 Перезапуск по фильтру", callback_data="admin_restart_filter_start"))
    # New: Mass start by filter
    markup.add(types.InlineKeyboardButton("▶️ Запуск по фильтру", callback_data="admin_start_filter_start"))
    markup.add(types.InlineKeyboardButton("📄 Получить логи бота", callback_data="admin_get_logs_start"),
               types.InlineKeyboardButton("✨ Мои ОП", callback_data="admin_my_op_menu"))
    # Quick settings
    markup.add(
        types.InlineKeyboardButton("🧩 Crypto Pay", callback_data="admin_crypto_pay_manage"),
        types.InlineKeyboardButton("💬 Чат админов", callback_data="admin_set_chat_link")
    )
    markup.add(
        types.InlineKeyboardButton("📢 Ссылка на канал", callback_data="admin_set_channel_link")
    )
    # Top-level button to edit creator welcome text
    markup.add(types.InlineKeyboardButton("✏️ Изменить приветствие креатора", callback_data="admin_edit_creator_welcome"))
    if is_customization_unlocked():
        markup.add(types.InlineKeyboardButton("Кастомизация", callback_data="admin_customization_menu"))
    return markup
    
def create_bot_type_menu(user_id=None):
    markup = types.InlineKeyboardMarkup(row_width=1)
    creation_buttons = get_bot_creation_button_texts()
    markup.add(types.InlineKeyboardButton(creation_buttons['ref'], callback_data="create_bot_ref"))
    markup.add(types.InlineKeyboardButton(creation_buttons['stars'], callback_data="create_bot_stars"))
    clicker_available = is_clicker_unlocked_globally()
    if not clicker_available and user_id is not None:
        try:
            user = get_user(user_id)
            clicker_available = bool(user['clicker_unlocked']) if user and 'clicker_unlocked' in user.keys() else False
        except Exception:
            clicker_available = False
    if clicker_available:
        markup.add(types.InlineKeyboardButton(creation_buttons['clicker'], callback_data="create_bot_clicker"))
    
    anonchat_available = is_anonchat_unlocked_globally()
    if not anonchat_available and user_id is not None:
        try:
            user = get_user(user_id)
            anonchat_available = bool(user['anonchat_unlocked']) if user and 'anonchat_unlocked' in user.keys() else False
        except Exception:
            anonchat_available = False
    if anonchat_available:
        markup.add(types.InlineKeyboardButton(creation_buttons.get('anonchat', "💬 Анонимный чат"), callback_data="create_bot_anonchat"))
    
    cashlait_available = is_cashlait_unlocked_globally()
    if not cashlait_available and user_id is not None:
        try:
            user = get_user(user_id)
            cashlait_available = bool(user.get('cashlait_unlocked', False)) if user else False
        except Exception:
            cashlait_available = False
    if cashlait_available:
        markup.add(types.InlineKeyboardButton(creation_buttons.get('cashlait', "💼 CashLait"), callback_data="create_bot_cashlait"))
    
    dicelite_available = is_dicelite_unlocked_globally()
    if not dicelite_available and user_id is not None:
        try:
            user = get_user(user_id)
            dicelite_available = bool(user.get('dicelite_unlocked', False)) if user else False
        except Exception:
            dicelite_available = False
    if dicelite_available:
        markup.add(types.InlineKeyboardButton(creation_buttons.get('dicelite', "🎲 DiceLite"), callback_data="create_bot_dicelite"))

    exchange_available = is_exchange_unlocked_globally()
    if not exchange_available and user_id is not None:
        try:
            user = get_user(user_id)
            exchange_available = bool(user.get('exchange_unlocked', False)) if user else False
        except Exception:
            exchange_available = False
    if exchange_available:
        markup.add(types.InlineKeyboardButton(creation_buttons.get('exchange', "💱 Бот Обменник"), callback_data="create_bot_exchange"))

    return markup

def create_my_bots_menu(user_id):
    user_bots = get_user_bots(user_id)
    markup = types.InlineKeyboardMarkup(row_width=1)
    if not user_bots:
        markup.add(types.InlineKeyboardButton(get_custom_text('no_bots_placeholder'), callback_data="dummy"))
    else:
        for bot_item in user_bots:
            icons = {'unconfigured': '⚠️', 'stopped': '🔴', 'running': '🟢'}
            status_icon = icons.get(bot_item['status'], '❓')
            type_icons = {
                'ref': '💸',
                'stars': '⭐',
                'clicker': '🖱',
                'anonchat': '💬',
                'cashlait': '💼',
                'dicelite': '🎲',
                'exchange': '💱',
            }
            bot_type_icon = type_icons.get(bot_item['bot_type'], '🎨')
            vip_icon = "⭐" if bot_item['vip_status'] else ""
            name = f"@{bot_item['bot_username']}" if bot_item['bot_username'] else f"Бот #{bot_item['id']} (без имени)"
            markup.add(types.InlineKeyboardButton(f"{status_icon} {bot_type_icon} {name} {vip_icon}", callback_data=f"actions_{bot_item['id']}"))
    return markup

def create_bot_actions_menu(bot_id):
    bot_info = get_bot_by_id(bot_id)
    if not bot_info: return None
    markup = types.InlineKeyboardMarkup(row_width=2)
    if bot_info['status'] == 'running': status_text = "🟢 Запущен"
    elif bot_info['status'] == 'stopped': status_text = "🔴 Остановлен"
    else: status_text = "⚠️ Не настроен"
    
    markup.add(types.InlineKeyboardButton(status_text, callback_data="dummy"))
    
    if bot_info['bot_type'] in ['ref', 'stars', 'clicker', 'anonchat', 'cashlait', 'exchange']:
        markup.add(types.InlineKeyboardButton("⚙️ Конфигурация", callback_data=f"config_{bot_id}"),
                   types.InlineKeyboardButton("💰 Доп. заработок (Flyer)", callback_data=f"dop_zarabotok_{bot_id}"))
    elif bot_info['bot_type'] == 'dicelite':
        markup.add(types.InlineKeyboardButton("⚙️ Конфигурация", callback_data=f"config_{bot_id}"))
    else:
        markup.add(types.InlineKeyboardButton("⚙️ Конфигурация", callback_data=f"config_{bot_id}"))

    markup.add(types.InlineKeyboardButton("📲 Передать бота", callback_data=f"transfer_{bot_id}_start"),
               types.InlineKeyboardButton("📄 Логи", callback_data=f"logs_{bot_id}_get"))
    markup.add(types.InlineKeyboardButton("🗑️ Удалить бота", callback_data=f"delete_{bot_id}_confirm"),
               types.InlineKeyboardButton("📁 Экспорт пользователей", callback_data=f"users_{bot_id}_export"))
    
    vip_status_text = "⭐ VIP-статус (Вкл)" if bot_info['vip_status'] else "⭐ VIP-статус (Выкл)"
    markup.add(types.InlineKeyboardButton(vip_status_text, callback_data=f"vip_{bot_id}_toggle"))
    
    # Удалены все взаимодействия с типом 'creator'
    
    markup.add(types.InlineKeyboardButton("⬅️ К списку ботов", callback_data="back_to_bots_list"))
    return markup

def create_ref_bot_config_menu(bot_id):
    bot_info = get_bot_by_id(bot_id)
    if not bot_info: return None
    markup = types.InlineKeyboardMarkup(row_width=2)
    if bot_info['status'] == 'running':
        markup.add(types.InlineKeyboardButton("⏹️ Остановить", callback_data=f"control_{bot_id}_stop"),
                   types.InlineKeyboardButton("🔄 Перезапустить", callback_data=f"control_{bot_id}_restart"))
    else:
        markup.add(types.InlineKeyboardButton("▶️ Запустить", callback_data=f"control_{bot_id}_start"))
    markup.add(types.InlineKeyboardButton("🔑 Токен", callback_data=f"edit_{bot_id}_bot_token"),
               types.InlineKeyboardButton("👋 Приветствие", callback_data=f"edit_{bot_id}_welcome_message"))
    markup.add(types.InlineKeyboardButton(f"L1: {bot_info['ref_reward_1']}₽", callback_data=f"edit_{bot_id}_ref_reward_1"),
               types.InlineKeyboardButton(f"L2: {bot_info['ref_reward_2']}₽", callback_data=f"edit_{bot_id}_ref_reward_2"))
    markup.add(types.InlineKeyboardButton(f"Мин. вывод: {bot_info['withdrawal_limit']}₽", callback_data=f"edit_{bot_id}_withdrawal_limit"),
               types.InlineKeyboardButton("🏧 Способ вывода", callback_data=f"edit_{bot_id}_withdrawal_method_text"))
    markup.add(types.InlineKeyboardButton(f"📈 Лимит ОП ({bot_info['flyer_limit']})", callback_data=f"edit_{bot_id}_flyer_limit"))
    markup.add(types.InlineKeyboardButton("🔗 Чат поддержки", callback_data=f"edit_{bot_id}_chat_link"),
               types.InlineKeyboardButton("📜 Регламент", callback_data=f"edit_{bot_id}_regulations_text"))
    markup.add(types.InlineKeyboardButton("📢 Канал выплат", callback_data=f"edit_{bot_id}_payout_channel"),
               types.InlineKeyboardButton("👥 Админы", callback_data=f"admins_{bot_id}_manage"))
    markup.add(types.InlineKeyboardButton("⬅️ Главное меню бота", callback_data=f"actions_{bot_id}"))
    return markup

def create_stars_bot_config_menu(bot_id):
    bot_info = get_bot_by_id(bot_id)
    if not bot_info: return None
    markup = types.InlineKeyboardMarkup(row_width=2)
    if bot_info['status'] == 'running':
        markup.add(types.InlineKeyboardButton("⏹️ Остановить", callback_data=f"control_{bot_id}_stop"),
                   types.InlineKeyboardButton("🔄 Перезапустить", callback_data=f"control_{bot_id}_restart"))
    else:
        markup.add(types.InlineKeyboardButton("▶️ Запустить", callback_data=f"control_{bot_id}_start"))
    markup.add(types.InlineKeyboardButton("🔑 Токен", callback_data=f"edit_{bot_id}_bot_token"),
               types.InlineKeyboardButton("👋 Приветствие", callback_data=f"edit_{bot_id}_welcome_message"))
    markup.add(types.InlineKeyboardButton(f"🎁 Награда за подарок: {bot_info['stars_daily_bonus']}⭐", callback_data=f"edit_{bot_id}_stars_daily_bonus"),
               types.InlineKeyboardButton(f"⏱️ КД подарка: {bot_info['stars_daily_cooldown']} ч.", callback_data=f"edit_{bot_id}_stars_daily_cooldown"))
    markup.add(types.InlineKeyboardButton(f"🤝 Бонус рефереру: {bot_info['stars_ref_bonus_referrer']}⭐", callback_data=f"edit_{bot_id}_stars_ref_bonus_referrer"),
               types.InlineKeyboardButton(f"👤 Бонус рефералу: {bot_info['stars_ref_bonus_new_user']}⭐", callback_data=f"edit_{bot_id}_stars_ref_bonus_new_user"))
    markup.add(types.InlineKeyboardButton(f"🎁 Бонус за старт: {bot_info['stars_welcome_bonus']}⭐", callback_data=f"edit_{bot_id}_stars_welcome_bonus"))
    markup.add(types.InlineKeyboardButton("📢 Канал выплат", callback_data=f"edit_{bot_id}_stars_payments_channel"),
               types.InlineKeyboardButton("💬 Чат поддержки", callback_data=f"edit_{bot_id}_stars_support_chat"))
    markup.add(types.InlineKeyboardButton("👥 Админы", callback_data=f"admins_{bot_id}_manage"))
    markup.add(types.InlineKeyboardButton("⬅️ Главное меню бота", callback_data=f"actions_{bot_id}"))
    return markup

def create_clicker_bot_config_menu(bot_id):
    bot_info = get_bot_by_id(bot_id)
    if not bot_info:
        return None
    markup = types.InlineKeyboardMarkup(row_width=2)
    if bot_info['status'] == 'running':
        markup.add(types.InlineKeyboardButton("⏹️ Остановить", callback_data=f"control_{bot_id}_stop"),
                   types.InlineKeyboardButton("🔄 Перезапустить", callback_data=f"control_{bot_id}_restart"))
    else:
        markup.add(types.InlineKeyboardButton("▶️ Запустить", callback_data=f"control_{bot_id}_start"))

    markup.add(types.InlineKeyboardButton("🔑 Токен", callback_data=f"edit_{bot_id}_bot_token"),
               types.InlineKeyboardButton("👋 Приветствие", callback_data=f"edit_{bot_id}_welcome_message"))

    markup.add(types.InlineKeyboardButton(f"Клик: {(bot_info['click_reward_min'] or 0.001)}-{(bot_info['click_reward_max'] or 0.005)}₽", callback_data=f"edit_{bot_id}_click_reward_min"))

    markup.add(types.InlineKeyboardButton(f"Реген: {(bot_info['energy_regen_rate'] or 2)}/сек", callback_data=f"edit_{bot_id}_energy_regen_rate"),
               types.InlineKeyboardButton(f"Мин. вывод: {(bot_info['withdrawal_min_clicker'] or 10.0)}₽", callback_data=f"edit_{bot_id}_withdrawal_min_clicker"))

    markup.add(types.InlineKeyboardButton(f"Бонус за старт: {(bot_info['welcome_bonus_clicker'] or 1.0)}₽", callback_data=f"edit_{bot_id}_welcome_bonus_clicker"),
               types.InlineKeyboardButton(f"Ежедн. бонус: {(bot_info['daily_bonus_clicker'] or 0.5)}₽", callback_data=f"edit_{bot_id}_daily_bonus_clicker"))

    markup.add(types.InlineKeyboardButton(f"КД подарка: {(bot_info['daily_bonus_cooldown_clicker'] or 12)} ч.", callback_data=f"edit_{bot_id}_daily_bonus_cooldown_clicker"),
               types.InlineKeyboardButton("🏧 Способ вывода", callback_data=f"edit_{bot_id}_withdrawal_method_text_clicker"))

    markup.add(types.InlineKeyboardButton(f"Бонус рефереру: {(bot_info['ref_bonus_referrer_clicker'] or 0.2)}₽", callback_data=f"edit_{bot_id}_ref_bonus_referrer_clicker"),
               types.InlineKeyboardButton(f"Бонус рефералу: {(bot_info['ref_bonus_new_user_clicker'] or 0.1)}₽", callback_data=f"edit_{bot_id}_ref_bonus_new_user_clicker"))

    markup.add(types.InlineKeyboardButton("📢 Канал выплат", callback_data=f"edit_{bot_id}_payments_channel_clicker"),
               types.InlineKeyboardButton("💬 Чат поддержки", callback_data=f"edit_{bot_id}_support_chat_clicker"))

    markup.add(types.InlineKeyboardButton("👥 Админы", callback_data=f"admins_{bot_id}_manage"))
    markup.add(types.InlineKeyboardButton("⬅️ Главное меню бота", callback_data=f"actions_{bot_id}"))
    return markup

def create_anonchat_bot_config_menu(bot_id):
    bot_info = get_bot_by_id(bot_id)
    if not bot_info:
        return None
    markup = types.InlineKeyboardMarkup(row_width=2)
    if bot_info['status'] == 'running':
        markup.add(types.InlineKeyboardButton("⏹️ Остановить", callback_data=f"control_{bot_id}_stop"),
                   types.InlineKeyboardButton("🔄 Перезапустить", callback_data=f"control_{bot_id}_restart"))
    else:
        markup.add(types.InlineKeyboardButton("▶️ Запустить", callback_data=f"control_{bot_id}_start"))

    markup.add(types.InlineKeyboardButton("🔑 Токен бота", callback_data=f"edit_{bot_id}_bot_token"),
               types.InlineKeyboardButton("👥 Админы", callback_data=f"edit_{bot_id}_admin_ids"))

    markup.add(types.InlineKeyboardButton("👋 Приветствие", callback_data=f"edit_{bot_id}_anonchat_welcome_message"),
               types.InlineKeyboardButton("📢 ID канала для ОП", callback_data=f"edit_{bot_id}_anonchat_channel_id"))

    markup.add(types.InlineKeyboardButton(f"💎 Цена VIP: {(bot_info['anonchat_vip_price'] or 45.0)}₽", callback_data=f"edit_{bot_id}_anonchat_vip_price"),
               types.InlineKeyboardButton("🔐 Токен Crypto Pay", callback_data=f"edit_{bot_id}_anonchat_crypto_api_token"))

    markup.add(types.InlineKeyboardButton("🪁 Ключ Flyer API", callback_data=f"edit_{bot_id}_anonchat_flyer_api_key"),
               types.InlineKeyboardButton(f"📦 Лимит заданий: {(bot_info['anonchat_flyer_tasks_limit'] or 5)}", callback_data=f"edit_{bot_id}_anonchat_flyer_tasks_limit"))

    markup.add(types.InlineKeyboardButton("👥 Админы", callback_data=f"admins_{bot_id}_manage"))
    markup.add(types.InlineKeyboardButton("⬅️ Главное меню бота", callback_data=f"actions_{bot_id}"))
    return markup

def create_cashlait_bot_config_menu(bot_id):
    bot_info = get_bot_by_id(bot_id)
    if not bot_info:
        return None
    markup = types.InlineKeyboardMarkup(row_width=2)
    if bot_info['status'] == 'running':
        markup.add(
            types.InlineKeyboardButton("⏹️ Остановить", callback_data=f"control_{bot_id}_stop"),
            types.InlineKeyboardButton("🔄 Перезапустить", callback_data=f"control_{bot_id}_restart"),
        )
    else:
        markup.add(types.InlineKeyboardButton("▶️ Запустить", callback_data=f"control_{bot_id}_start"))

    markup.add(
        types.InlineKeyboardButton("🔑 Токен бота", callback_data=f"edit_{bot_id}_bot_token"),
        types.InlineKeyboardButton("🪁 Flyer API ключ", callback_data=f"edit_{bot_id}_cashlait_flyer_api_key"),
    )
    markup.add(
        types.InlineKeyboardButton("💳 Crypto Pay токен", callback_data=f"edit_{bot_id}_cashlait_crypto_pay_token"),
        types.InlineKeyboardButton("💱 Символ валюты", callback_data=f"edit_{bot_id}_cashlait_currency_symbol"),
    )
    markup.add(types.InlineKeyboardButton("👋 Приветствие", callback_data=f"edit_{bot_id}_cashlait_welcome_text"))
    flyer_limit = bot_info['flyer_limit'] or 5
    markup.add(
        types.InlineKeyboardButton(f"📈 Лимит Flyer: {flyer_limit}", callback_data=f"edit_{bot_id}_flyer_limit"),
        types.InlineKeyboardButton("📢 Канал выплат", callback_data=f"edit_{bot_id}_payout_channel"),
    )
    markup.add(
        types.InlineKeyboardButton("🔗 Чат поддержки", callback_data=f"edit_{bot_id}_chat_link"),
        types.InlineKeyboardButton("👥 Админы", callback_data=f"admins_{bot_id}_manage"),
    )
    markup.add(types.InlineKeyboardButton("⬅️ Главное меню бота", callback_data=f"actions_{bot_id}"))
    return markup

def create_dicelite_bot_config_menu(bot_id):
    bot_info = get_bot_by_id(bot_id)
    if not bot_info:
        return None
    markup = types.InlineKeyboardMarkup(row_width=2)
    if bot_info['status'] == 'running':
        markup.add(
            types.InlineKeyboardButton("⏹️ Остановить", callback_data=f"control_{bot_id}_stop"),
            types.InlineKeyboardButton("🔄 Перезапустить", callback_data=f"control_{bot_id}_restart"),
        )
    else:
        markup.add(types.InlineKeyboardButton("▶️ Запустить", callback_data=f"control_{bot_id}_start"))

    markup.add(
        types.InlineKeyboardButton("🔑 Токен бота", callback_data=f"edit_{bot_id}_bot_token"),
        types.InlineKeyboardButton("💳 Crypto Pay токен", callback_data=f"edit_{bot_id}_dicelite_crypto_pay_token"),
    )
    markup.add(
        types.InlineKeyboardButton("👋 Приветствие", callback_data=f"edit_{bot_id}_dicelite_welcome_text"),
        types.InlineKeyboardButton("👥 Админы", callback_data=f"admins_{bot_id}_manage"),
    )
    markup.add(types.InlineKeyboardButton("📄 Логи", callback_data=f"logs_{bot_id}_get"))
    markup.add(types.InlineKeyboardButton("⬅️ Главное меню бота", callback_data=f"actions_{bot_id}"))
    return markup

def create_exchange_bot_config_menu(bot_id):
    bot_info = get_bot_by_id(bot_id)
    if not bot_info:
        return None
    markup = types.InlineKeyboardMarkup(row_width=2)
    if bot_info['status'] == 'running':
        markup.add(
            types.InlineKeyboardButton("⏹️ Остановить", callback_data=f"control_{bot_id}_stop"),
            types.InlineKeyboardButton("🔄 Перезапустить", callback_data=f"control_{bot_id}_restart"),
        )
    else:
        markup.add(types.InlineKeyboardButton("▶️ Запустить", callback_data=f"control_{bot_id}_start"))

    markup.add(
        types.InlineKeyboardButton("🔑 Токен бота", callback_data=f"edit_{bot_id}_bot_token"),
        types.InlineKeyboardButton("👋 Приветствие", callback_data=f"edit_{bot_id}_exchange_welcome_text"),
    )
    markup.add(
        types.InlineKeyboardButton("👥 Админы", callback_data=f"admins_{bot_id}_manage"),
    )
    markup.add(types.InlineKeyboardButton("⬅️ Главное меню бота", callback_data=f"actions_{bot_id}"))
    return markup

def create_dop_zarabotok_menu(bot_id):
    bot_info = get_bot_by_id(bot_id)
    is_enabled = False
    text = ""
    
    if bot_info['bot_type'] == 'ref':
        is_enabled = bot_info['flyer_op_enabled']
        op_reward = get_setting('op_reward') or "1.0"
        text = (f"💰 *Дополнительный заработок*\n\n"
                f"Подключите систему обязательной подписки (ОП) к вашему боту и получайте *{op_reward} ₽* за каждого уникального, настоящего русского пользователя, который её пройдет!\n\n"
                f"Это отличный способ монетизировать аудиторию вашего бота, не требующий от вас никаких усилий. 💸")
    elif bot_info['bot_type'] == 'cashlait':
        is_enabled = bool(bot_info['cashlait_flyer_api_key'])
        op_reward = get_setting('op_reward') or "1.0"
        text = (f"💰 *CashLait + Flyer*\n\n"
                f"Активируйте обязательную подписку, чтобы получать *{op_reward} ₽* за каждого пользователя, который пройдет задания перед входом в бот.\n\n"
                f"Flyer автоматически монетизирует трафик, а вы просто наблюдаете за профитом.")
    elif bot_info['bot_type'] in ['stars', 'clicker']:
        is_enabled = bot_info['stars_op_enabled'] if bot_info['bot_type'] == 'stars' else bot_info['clicker_op_enabled']
        sub_reward = get_setting('stars_sub_reward') or "1.0"
        text = (f"💰 *Дополнительный заработок*\n\n"
                f"1. Вы получаете *{sub_reward} ₽* за каждого нового пользователя, который прошел обязательную подписку в вашем боте (работает только при активном Flyer).\n\n"
                f"2. Подключите систему ОП от Flyer, чтобы пользователи выполняли задания от рекламодателей перед использованием бота.")
    elif bot_info['bot_type'] == 'anonchat':
        is_enabled = bool(bot_info['anonchat_flyer_api_key'])
        sub_reward = get_setting('stars_sub_reward') or "1.0"
        limit = bot_info['anonchat_flyer_tasks_limit'] or 5
        text = (f"💬 *Анонимный чат + Flyer*\n\n"
                f"Перед началом диалога собеседникам предлагаются задания Flyer (до {limit} каналов/чеков).\n"
                f"За каждую успешную проверку вы получаете *{sub_reward} ₽* автоматически.\n\n"
                f"Подайте заявку, и администратор подключит систему.")
    else:
        return "Этот тип бота не поддерживает доп. заработок.", types.InlineKeyboardMarkup()

    status_icon = "🟢" if is_enabled else "🔴"
    status_text = "Подключена" if is_enabled else "Отключена"
    text += f"\n\nСтатус системы Flyer: {status_icon} *{status_text}*"
            
    markup = types.InlineKeyboardMarkup()
    if not is_enabled:
        markup.add(types.InlineKeyboardButton("🚀 Подать заявку на подключение Flyer", callback_data=f"flyer_op_apply_{bot_id}"))
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"actions_{bot_id}"))
    return text, markup

def create_cancel_markup():
    return types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add("❌ Отмена")

def render_customization_menu(chat_id, message_id=None, flash=None):
    if not is_customization_unlocked():
        return
    text = (
        "🎨 <b>Кастомизация интерфейса</b>\n\n"
        "Выберите раздел, где хотите изменить подписи кнопок или тексты сообщений.\n\n"
        "Допустимая длина зависит от выбранного элемента.\n"
        "Чтобы вернуть стандартное значение, отправьте <code>СБРОС</code>."
    )
    if flash:
        text = f"<b>{escape(flash)}</b>\n\n{text}"
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("🧭 Главное меню", callback_data="admin_customization_main"))
    markup.add(types.InlineKeyboardButton("🛠 Создание бота", callback_data="admin_customization_create"))
    markup.add(types.InlineKeyboardButton("📝 Общие тексты", callback_data="admin_customization_texts"))
    markup.add(types.InlineKeyboardButton("⬅️ Назад в админку", callback_data="admin_back"))
    if message_id is not None:
        try:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="HTML", reply_markup=markup)
            return
        except telebot.apihelper.ApiTelegramException:
            pass
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)

def render_customization_section(chat_id, section, message_id=None, flash=None):
    if not is_customization_unlocked():
        return
    if section == 'main':
        keys = MAIN_MENU_BUTTON_KEYS
        title = "Главное меню"
    elif section == 'create':
        keys = BOT_CREATION_BUTTON_KEYS
        title = "Меню создания бота"
    else:
        return

    lines = []
    for key in keys:
        description = BUTTON_KEY_DESCRIPTIONS.get(key, key)
        current_text = escape(get_custom_button_text(key))
        lines.append(f"• {escape(description)}: <code>{current_text}</code>")

    body = "\n".join(lines)
    header = f"🎛 <b>{title}</b>"
    hint = "Нажмите на кнопку ниже, чтобы изменить текст. Чтобы вернуть стандартное значение, отправьте <code>СБРОС</code>."
    prefix = f"<b>{escape(flash)}</b>\n\n" if flash else ""
    text = f"{prefix}{header}\n\n{body}\n\n{hint}"

    markup = types.InlineKeyboardMarkup(row_width=1)
    for key in keys:
        markup.add(
            types.InlineKeyboardButton(
                f"✏️ {BUTTON_KEY_DESCRIPTIONS.get(key, key)}",
                callback_data=f"admin_customization_edit_{key}"
            )
        )
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_customization_menu"))

    if message_id is not None:
        try:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="HTML", reply_markup=markup)
            return
        except telebot.apihelper.ApiTelegramException:
            pass
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)

def render_customization_texts(chat_id, message_id=None, flash=None):
    if not is_customization_unlocked():
        return
    lines = []
    for key, meta in CUSTOM_TEXTS_META.items():
        description = escape(meta.get('description', key))
        current_value = escape(get_custom_text(key)) or "—"
        lines.append(f"• <b>{description}</b>:\n<code>{current_value}</code>")

    body = "\n\n".join(lines) if lines else "Нет доступных текстов."
    hint = "Нажмите на элемент ниже, чтобы изменить текст. Чтобы вернуть стандартное значение, отправьте <code>СБРОС</code>."
    prefix = f"<b>{escape(flash)}</b>\n\n" if flash else ""
    text = f"{prefix}📝 <b>Общие тексты</b>\n\n{body}\n\n{hint}"

    markup = types.InlineKeyboardMarkup(row_width=1)
    for key, meta in CUSTOM_TEXTS_META.items():
        markup.add(
            types.InlineKeyboardButton(
                f"✏️ {meta.get('description', key)}",
                callback_data=f"admin_customization_edit_text_{key}"
            )
        )
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_customization_menu"))

    if message_id is not None:
        try:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="HTML", reply_markup=markup)
            return
        except telebot.apihelper.ApiTelegramException:
            pass
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)

def prompt_custom_text_edit(user_id, text_key, message_id=None, back_callback="admin_customization_texts"):
    # Приветствие креатора доступно всем админам без разблокировки кастомизации
    if text_key != 'creator_welcome' and not is_customization_unlocked():
        return
    meta = CUSTOM_TEXTS_META.get(text_key)
    if not meta:
        return

    description = meta.get('description', text_key)
    max_length = meta.get('max_length', 1024)
    supports_html = meta.get('supports_html', False)
    single_line = meta.get('single_line', False)

    hint_parts = [
        f"✏️ <b>{escape(description)}</b>",
        "",
        f"Отправьте новый текст (макс. {max_length} символов).",
        "Чтобы вернуть стандартное значение, отправьте <code>СБРОС</code>."
    ]
    if supports_html:
        hint_parts.append("Поддерживается HTML-разметка.")
    if single_line:
        hint_parts.append("Используйте одну строку без переносов.")
    prompt_text = "\n".join(hint_parts)

    markup = types.InlineKeyboardMarkup().add(
        types.InlineKeyboardButton("⬅️ Назад", callback_data=back_callback)
    )

    try:
        msg = bot.edit_message_text(prompt_text, user_id, message_id, parse_mode="HTML", reply_markup=markup)
    except telebot.apihelper.ApiTelegramException:
        msg = bot.send_message(user_id, prompt_text, parse_mode="HTML", reply_markup=markup)

    set_user_state(user_id, {
        'action': 'admin_set_custom_text',
        'text_key': text_key,
        'message_id': msg.message_id,
        'back_callback': back_callback
    })

def handle_admin_customization(call):
    user_id = call.from_user.id
    if not is_customization_unlocked():
        bot.answer_callback_query(call.id, "Функция недоступна.", show_alert=True)
        return

    message_id = getattr(call.message, 'message_id', None)
    data = call.data

    if data in ("admin_customization", "admin_customization_menu"):
        bot.answer_callback_query(call.id)
        render_customization_menu(user_id, message_id)
        return

    if data == "admin_customization_main":
        bot.answer_callback_query(call.id)
        render_customization_section(user_id, 'main', message_id)
        return

    if data == "admin_customization_create":
        bot.answer_callback_query(call.id)
        render_customization_section(user_id, 'create', message_id)
        return

    if data == "admin_customization_texts":
        bot.answer_callback_query(call.id)
        render_customization_texts(user_id, message_id)
        return

    if data.startswith("admin_customization_edit_text_"):
        text_key = data[len("admin_customization_edit_text_"):]
        if text_key not in CUSTOM_TEXTS_META:
            bot.answer_callback_query(call.id)
            return

        prompt_custom_text_edit(user_id, text_key, message_id, back_callback="admin_customization_texts")
        bot.answer_callback_query(call.id)
        return

    if data.startswith("admin_customization_edit_"):
        button_key = data[len("admin_customization_edit_"):]
        if button_key not in BUTTON_KEY_DESCRIPTIONS:
            bot.answer_callback_query(call.id)
            return

        category = 'main' if button_key.startswith('main_') else 'create'
        description = BUTTON_KEY_DESCRIPTIONS[button_key]
        prompt_text = (
            f"✏️ <b>{escape(description)}</b>\n\n"
            "Отправьте новый текст для этой кнопки (макс. 64 символа).\n"
            "Чтобы вернуть стандартное значение, отправьте <code>СБРОС</code>."
        )
        back_callback = f"admin_customization_{category}"
        markup = types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("⬅️ Назад", callback_data=back_callback)
        )

        try:
            msg = bot.edit_message_text(prompt_text, user_id, message_id, parse_mode="HTML", reply_markup=markup)
        except telebot.apihelper.ApiTelegramException:
            msg = bot.send_message(user_id, prompt_text, parse_mode="HTML", reply_markup=markup)

        set_user_state(user_id, {
            'action': 'admin_set_custom_button_text',
            'button_key': button_key,
            'category': category,
            'message_id': msg.message_id,
            'back_callback': back_callback
        })

        bot.answer_callback_query(call.id)
        return

    bot.answer_callback_query(call.id)

def cleanup_stale_states():
    while True:
        try:
            now = time.time()
            stale_users = [user_id for user_id, state in list(user_states.items())
                           if now - state.get('timestamp', now) > TTL_STATES_SECONDS]
            
            if stale_users:
                for user_id in stale_users:
                    if user_id in user_states:
                        del user_states[user_id]
                logging.info(f"Очищено {len(stale_users)} зависших состояний пользователей.")
        except Exception as e:
            logging.error(f"Ошибка в cleanup_stale_states: {e}")
        
        time.sleep(600)

def set_user_state(user_id, state_data):
    state_data['timestamp'] = time.time()
    user_states[user_id] = state_data

# =================================================================================
# -------------------- ОБРАБОТЧИКИ СООБЩЕНИЙ И КОЛБЕКОВ ---------------------------
# =================================================================================

def process_state_input(message):
    user_id = message.from_user.id
    if user_id not in user_states:
        return

    state = user_states[user_id]
    action = state.get('action')
    if not action:
        return
    
    if hasattr(message, 'text') and message.text == '❌ Отмена':
        try: bot.delete_message(user_id, state['message_id'])
        except: pass
        if user_id in user_states: del user_states[user_id]
        bot.send_message(user_id, "Действие отменено.", reply_markup=create_main_menu(user_id))
        try: bot.delete_message(user_id, message.message_id)
        except: pass
        return

    if action == 'admin_set_custom_button_text':
        button_key = state.get('button_key')
        category = state.get('category', 'main')
        prompt_message_id = state.get('message_id')
        if not button_key or button_key not in BUTTON_KEY_DESCRIPTIONS:
            if user_id in user_states:
                del user_states[user_id]
            return

        incoming_text = (getattr(message, 'text', '') or '').strip()
        lowered = incoming_text.lower()
        is_reset = lowered in CUSTOMIZATION_RESET_COMMANDS if incoming_text else False

        if not incoming_text and not is_reset:
            bot.send_message(user_id, "❌ Текст не может быть пустым. Введите значение или отправьте 'СБРОС' для возврата к стандарту.")
            return

        result_message = ""
        if is_reset:
            reset_custom_button_text(button_key)
            result_message = "Текст сброшен к стандартному значению."
        else:
            if len(incoming_text) > 64:
                bot.send_message(user_id, f"❌ Текст слишком длинный ({len(incoming_text)} символов). Максимум 64. Попробуйте снова или отправьте 'СБРОС'.")
                return
            set_custom_button_text(button_key, incoming_text)
            result_message = "Текст обновлён."

        try:
            bot.delete_message(user_id, message.message_id)
        except Exception:
            pass

        flash_text = f"✅ {result_message}"
        if prompt_message_id is not None:
            render_customization_section(user_id, category, message_id=prompt_message_id, flash=flash_text)
        else:
            render_customization_section(user_id, category, flash=flash_text)

        if user_id in user_states:
            del user_states[user_id]
        return

    if action == 'admin_set_custom_text':
        text_key = state.get('text_key')
        prompt_message_id = state.get('message_id')
        back_callback = state.get('back_callback', 'admin_customization_texts')
        meta = CUSTOM_TEXTS_META.get(text_key)
        if not meta:
            if user_id in user_states:
                del user_states[user_id]
            return

        incoming_raw = getattr(message, 'text', '') or ''
        incoming_text = incoming_raw
        lowered = incoming_text.lower()
        is_reset = lowered in CUSTOMIZATION_RESET_COMMANDS if incoming_text else False

        if not incoming_text and not is_reset:
            bot.send_message(user_id, "❌ Текст не может быть пустым. Введите значение или отправьте 'СБРОС' для возврата к стандарту.")
            return

        result_message = ""
        if is_reset:
            reset_custom_text(text_key)
            result_message = "Текст сброшен к стандартному значению."
        else:
            max_length = meta.get('max_length', 4096)
            single_line = meta.get('single_line', False)
            cleaned_text = incoming_text.strip() if single_line else incoming_text

            if single_line and ("\n" in cleaned_text or "\r" in cleaned_text):
                bot.send_message(user_id, "❌ Для этого элемента допускается только одна строка без переносов.")
                return

            if len(cleaned_text) > max_length:
                bot.send_message(user_id, f"❌ Текст слишком длинный ({len(cleaned_text)} символов). Максимум {max_length}. Попробуйте снова или отправьте 'СБРОС'.")
                return

            if not cleaned_text.strip():
                bot.send_message(user_id, "❌ Текст не может быть пустым. Введите значение или отправьте 'СБРОС' для возврата к стандарту.")
                return

            set_custom_text(text_key, cleaned_text)
            result_message = "Текст обновлён."

        try:
            bot.delete_message(user_id, message.message_id)
        except Exception:
            pass

        flash_text = f"✅ {result_message}"
        if back_callback == 'admin_back':
            heading = get_custom_text('admin_menu_heading')
            try:
                if prompt_message_id is not None:
                    bot.edit_message_text(heading, user_id, prompt_message_id, reply_markup=create_admin_menu())
                else:
                    bot.send_message(user_id, heading, reply_markup=create_admin_menu())
            except telebot.apihelper.ApiTelegramException:
                bot.send_message(user_id, heading, reply_markup=create_admin_menu())
            bot.send_message(user_id, flash_text)
        elif back_callback == 'admin_op_manage':
            op_reward = get_setting('op_reward') or "1.0"
            stars_reward = get_setting('stars_sub_reward') or "1.0"
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton(f"💸 Награда за Flyer ОП (реф. бот): {op_reward} ₽", callback_data="admin_op_set_reward"))
            markup.add(types.InlineKeyboardButton(f"⭐ Награда за подписку (звёзды): {stars_reward} ₽", callback_data="admin_op_set_stars_reward"))
            markup.add(types.InlineKeyboardButton("✏️ Изменить приветствие креатора", callback_data="admin_edit_creator_welcome"))
            markup.add(types.InlineKeyboardButton("📊 Лимит бесплатных ботов", callback_data="admin_set_max_bots"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
            menu_text = "⚙️ Управление настройками конструктора и доходом Flyer:"
            try:
                if prompt_message_id is not None:
                    bot.edit_message_text(menu_text, user_id, prompt_message_id, reply_markup=markup)
                else:
                    bot.send_message(user_id, menu_text, reply_markup=markup)
            except telebot.apihelper.ApiTelegramException:
                bot.send_message(user_id, menu_text, reply_markup=markup)
            bot.send_message(user_id, flash_text)
        else:
            if prompt_message_id is not None:
                render_customization_texts(user_id, message_id=prompt_message_id, flash=flash_text)
            else:
                render_customization_texts(user_id, flash=flash_text)

        if user_id in user_states:
            del user_states[user_id]
        return

    if action == 'admin_my_op_add_title':
        title = message.text.strip()
        bot.delete_message(user_id, state['message_id'])
        bot.delete_message(user_id, message.message_id)
        msg = bot.send_message(user_id, "<b>Шаг 2/3:</b> Отправьте полную ссылку на ресурс (например, https://t.me/durov):", 
                               parse_mode="HTML", reply_markup=create_cancel_markup())
        set_user_state(user_id, {'action': 'admin_my_op_add_link', 'title': title, 'message_id': msg.message_id})
        return

    # --- Bot broadcast: single bot ID input ---
    if action == 'admin_broadcast_bot_single_id':
        # Parse integer robustly and avoid conflating other exceptions with input errors
        text_input = (getattr(message, 'text', '') or '').strip()
        match = re.search(r"-?\d+", text_input)
        if not match:
            bot.send_message(message.from_user.id, "❌ Ошибка. Введите корректный числовой ID бота.")
            return
        try:
            bot_id = int(match.group(0))
        except Exception:
            bot.send_message(message.from_user.id, "❌ Ошибка. Введите корректный числовой ID бота.")
            return

        bot_info = get_bot_by_id(bot_id)
        if not bot_info:
            try:
                bot.edit_message_text("❌ Бот с таким ID не найден. Введите корректный ID:", message.chat.id, state['message_id'], reply_markup=create_cancel_markup())
            except Exception:
                bot.send_message(message.from_user.id, "❌ Бот с таким ID не найден. Введите корректный ID:")
            return

        # Precheck: warn if bot is not active (single-bot mode only)
        try:
            status = bot_info.get('status') if isinstance(bot_info, dict) else bot_info['status']
        except Exception:
            status = None
        if status != 'running':
            try:
                bot.edit_message_text(
                    "❌ Этот бот не активен. Запустите бота перед рассылкой.",
                    message.chat.id,
                    state['message_id'],
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_broadcast_bots_menu")
                    )
                )
            except Exception:
                try:
                    bot.send_message(
                        message.from_user.id,
                        "❌ Этот бот не активен. Запустите бота перед рассылкой.",
                        reply_markup=types.InlineKeyboardMarkup().add(
                            types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_broadcast_bots_menu")
                        )
                    )
                except Exception:
                    pass
            if message.from_user.id in user_states:
                del user_states[message.from_user.id]
            return

        # Cleanup last input message (best-effort)
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass

        # Prompt next step; if edit fails, send a fresh message and use its id
        prompt_message_id = None
        try:
            prompt = bot.edit_message_text(
                "Отправьте контент для рассылки в ЭТОМ боте (текст/медиа):",
                message.chat.id,
                state['message_id'],
                reply_markup=create_cancel_markup()
            )
            prompt_message_id = prompt.message_id
        except Exception:
            prompt = bot.send_message(
                message.chat.id,
                "Отправьте контент для рассылки в ЭТОМ боте (текст/медиа):",
                reply_markup=create_cancel_markup()
            )
            prompt_message_id = prompt.message_id

        set_user_state(message.from_user.id, {
            'action': 'admin_broadcast_content_for_bot',
            'message_id': prompt_message_id,
            'target_bot_ids': [bot_id]
        })
        return

    # --- Bot broadcast: multi count input ---
    if action == 'admin_broadcast_bot_multi_count':
        # Parse integer robustly; treat non-positive as "all running"
        text_input = (getattr(message, 'text', '') or '').strip()
        match = re.search(r"-?\d+", text_input)
        if not match:
            bot.send_message(message.from_user.id, "❌ Ошибка. Введите число (кол-во ботов).")
            return
        try:
            count = int(match.group(0))
        except Exception:
            bot.send_message(message.from_user.id, "❌ Ошибка. Введите число (кол-во ботов).")
            return

        try:
            if count <= 0:
                # select all running bots
                bots = db_execute("SELECT id FROM bots WHERE status='running' ORDER BY id DESC", fetchall=True) or []
            else:
                bots = db_execute("SELECT id FROM bots WHERE status='running' ORDER BY id DESC LIMIT ?", (count,), fetchall=True) or []
        except Exception:
            bots = []

        target_ids = [b['id'] for b in bots]
        if not target_ids:
            try:
                bot.edit_message_text("❌ Активных ботов не найдено.", message.chat.id, state['message_id'])
            except Exception:
                bot.send_message(message.from_user.id, "❌ Активных ботов не найдено.")
            if message.from_user.id in user_states:
                del user_states[message.from_user.id]
            return

        # Cleanup last input message (best-effort)
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass

        # Prompt next step; if edit fails, send a fresh message and use its id
        prompt_message_id = None
        try:
            prompt = bot.edit_message_text(
                "Отправьте контент для рассылки по выбранным ботам (очередью):",
                message.chat.id,
                state['message_id'],
                reply_markup=create_cancel_markup()
            )
            prompt_message_id = prompt.message_id
        except Exception:
            prompt = bot.send_message(
                message.chat.id,
                "Отправьте контент для рассылки по выбранным ботам (очередью):",
                reply_markup=create_cancel_markup()
            )
            prompt_message_id = prompt.message_id

        set_user_state(message.from_user.id, {
            'action': 'admin_broadcast_content_for_bot',
            'message_id': prompt_message_id,
            'target_bot_ids': target_ids
        })
        return

    # --- Bot broadcast: get content ---
    if action == 'admin_broadcast_content_for_bot':
        target_bot_ids = state.get('target_bot_ids') or []
        if not target_bot_ids:
            if message.from_user.id in user_states: del user_states[message.from_user.id]
            bot.send_message(message.from_user.id, "❌ Не выбраны боты для рассылки.")
            return
        # build preview (copy the submitted content into admin chat)
        try:
            preview = bot.copy_message(message.chat.id, message.chat.id, message.message_id)
            preview_markup = preview.reply_markup
            preview_id = preview.message_id
            try:
                bot.delete_message(message.chat.id, message.message_id)
            except Exception:
                pass
        except Exception:
            preview_markup = None
            preview_id = message.message_id
        try:
            bot.edit_message_text("Проверка контента перед рассылкой:", message.chat.id, state['message_id'])
        except Exception:
            pass
        # Ask for optional inline button (text | URL) before confirming
        try:
            prompt = bot.send_message(
                message.chat.id,
                "<b>Шаг 2/2:</b> Добавить инлайн-кнопку?\nОтправьте в формате <code>Текст | URL</code> или <code>-</code> для пропуска.",
                parse_mode="HTML",
                reply_markup=create_cancel_markup()
            )
            prompt_message_id = prompt.message_id
        except Exception:
            prompt_message_id = state.get('message_id')
        set_user_state(message.from_user.id, {
            'action': 'admin_broadcast_bots_get_button',
            'message_id': prompt_message_id,
            'target_bot_ids': target_bot_ids,
            'preview_message_id': preview_id,
            'reply_markup': preview_markup
        })
        return

    if action == 'admin_broadcast_bots_get_button':
        # Parse optional inline button and finalize preview + confirmation
        target_bot_ids = state.get('target_bot_ids') or []
        preview_id = state.get('preview_message_id')
        button_markup = state.get('reply_markup')  # preserve existing markup if any

        raw_text = (getattr(message, 'text', '') or '').strip()
        if raw_text and raw_text != '-':
            try:
                parts = [p.strip() for p in raw_text.split('|', 1)]
                if len(parts) != 2 or not parts[0] or not parts[1]:
                    raise ValueError("Invalid format")
                btn_text, btn_url = parts[0], parts[1]
                tmp_markup = types.InlineKeyboardMarkup()
                tmp_markup.add(types.InlineKeyboardButton(text=btn_text, url=btn_url))
                button_markup = tmp_markup
            except Exception:
                # Inform admin and abort this broadcast flow
                try:
                    bot.send_message(message.from_user.id, "❌ Неверный формат кнопки. Начните рассылку заново.", reply_markup=create_main_menu(message.from_user.id))
                except Exception:
                    pass
                if message.from_user.id in user_states:
                    del user_states[message.from_user.id]
                return

        # Try to apply the selected markup to the preview message
        try:
            bot.edit_message_reply_markup(message.chat.id, preview_id, reply_markup=button_markup)
        except Exception:
            pass

        # Show final confirmation
        confirm = types.InlineKeyboardMarkup(row_width=1)
        confirm.add(types.InlineKeyboardButton("✅ Начать рассылку в ботах", callback_data=f"admin_broadcast_bots_confirm"))
        confirm.add(types.InlineKeyboardButton("❌ Отмена", callback_data="admin_back"))
        try:
            bot.send_message(message.chat.id, f"Будет отправлено в боты: {', '.join(map(str, target_bot_ids))}", reply_markup=confirm)
        except Exception:
            pass
        set_user_state(message.from_user.id, {
            'action': 'admin_broadcast_ready_for_bots',
            'message_id': state.get('message_id'),
            'target_bot_ids': target_bot_ids,
            'preview_message_id': preview_id,
            'reply_markup': button_markup
        })
        # Clean up prompt and input message if possible
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        try:
            if state.get('message_id'):
                bot.delete_message(message.chat.id, state['message_id'])
        except Exception:
            pass
        return

    if action == 'admin_broadcast_ready_for_bots':
        # shouldn't get here by message; handled via callback confirm
        return

    if action == 'admin_set_crypto_pay_token':
        try:
            new_token = message.text.strip()
            # Persist to settings and update globals
            set_setting('crypto_pay_token', new_token)
            global CRYPTO_PAY_TOKEN, crypto_pay
            CRYPTO_PAY_TOKEN = new_token
            try:
                crypto_pay = AioCryptoPay(token=CRYPTO_PAY_TOKEN, network=Networks.MAIN_NET)
            except Exception as e:
                logging.error(f"Ошибка инициализации Crypto Pay: {e}")
                crypto_pay = None

            # Ensure background payment checker is running (start/restart)
            try:
                threading.Thread(target=run_payment_checker, daemon=True).start()
            except Exception as e:
                logging.warning(f"Не удалось перезапустить проверку платежей: {e}")

            # Inform admin whether token works
            if is_crypto_token_configured() and get_crypto_client() is not None:
                try:
                    bot.answer_callback_query(state.get('call_id', ''), "✅ Crypto Pay токен сохранен и инициализирован.", show_alert=True)
                except Exception:
                    pass
            else:
                try:
                    bot.answer_callback_query(state.get('call_id', ''), "⚠️ Токен сохранен, но инициализация не удалась.", show_alert=True)
                except Exception:
                    pass

            # Cleanup state and confirm
            try: bot.delete_message(message.from_user.id, message.message_id)
            except Exception: pass
            try: bot.delete_message(message.from_user.id, state['message_id'])
            except Exception: pass
            if message.from_user.id in user_states: del user_states[message.from_user.id]
            bot.send_message(message.from_user.id, "✅ Токен Crypto Pay сохранен.", reply_markup=create_main_menu(message.from_user.id))
        except Exception as e:
            bot.send_message(message.from_user.id, f"❌ Не удалось сохранить токен: {e}")
        return

    if action == 'admin_set_chat_link':
        link = message.text.strip()
        set_setting('admin_chat_link', link)
        global ADMIN_CHAT_LINK
        ADMIN_CHAT_LINK = link
        try: bot.delete_message(message.from_user.id, state['message_id'])
        except Exception: pass
        try: bot.delete_message(message.from_user.id, message.message_id)
        except Exception: pass
        if message.from_user.id in user_states: del user_states[message.from_user.id]
        bot.send_message(message.from_user.id, "✅ Ссылка на чат администраторов сохранена.", reply_markup=create_main_menu(message.from_user.id))
        return

    if action == 'admin_set_channel_link':
        link = message.text.strip()
        set_setting('channel_link', link)
        global CHANNEL_LINK
        CHANNEL_LINK = link
        try: bot.delete_message(message.from_user.id, state['message_id'])
        except Exception: pass
        try: bot.delete_message(message.from_user.id, message.message_id)
        except Exception: pass
        if message.from_user.id in user_states: del user_states[message.from_user.id]
        bot.send_message(message.from_user.id, "✅ Ссылка на канал сохранена.", reply_markup=create_main_menu(message.from_user.id))
        return

    if action == 'admin_my_op_add_link':
        link = message.text.strip()
        title = state['title']
        bot.delete_message(user_id, state['message_id'])
        bot.delete_message(user_id, message.message_id)
        msg = bot.send_message(user_id, "<b>Шаг 3/3:</b> Введите награду владельцу бота за одно выполнение (например, 0.5):",
                               parse_mode="HTML", reply_markup=create_cancel_markup())
        set_user_state(user_id, {'action': 'admin_my_op_add_reward', 'title': title, 'link': link, 'message_id': msg.message_id})
        return

    if action == 'admin_my_op_add_reward':
        try:
            reward = float(message.text.strip().replace(',', '.'))
            if reward < 0: raise ValueError
            
            title = state['title']
            link = state['link']

            db_execute("INSERT INTO admin_tasks (title, resource_link, reward) VALUES (?, ?, ?)",
                       (title, link, reward), commit=True)
            
            bot.delete_message(user_id, state['message_id'])
            bot.delete_message(user_id, message.message_id)
            if user_id in user_states: del user_states[user_id]
            
            bot.send_message(user_id, "✅ Новое задание для ОП успешно добавлено!", reply_markup=create_main_menu(user_id))
            
            call_imitation = types.CallbackQuery(id='fake_call', from_user=message.from_user, data="admin_my_op_menu", chat_instance="private", json_string="")
            
            fake_message = types.Message(message_id=state['message_id'], from_user=None, date=None, chat=message.chat, content_type='text', options={}, json_string="")
            call_imitation.message = fake_message
            
            handle_admin_callbacks(call_imitation)

        except ValueError:
            bot.send_message(user_id, "❌ Ошибка. Введите корректное положительное число.")
        return

    if action == 'admin_my_op_edit_reward':
        try:
            new_reward = float(message.text.strip().replace(',', '.'))
            if new_reward < 0: raise ValueError
            task_id = state['task_id']
            db_execute("UPDATE admin_tasks SET reward = ? WHERE id = ?", (new_reward, task_id), commit=True)
            
            bot.delete_message(user_id, state['message_id'])
            bot.delete_message(user_id, message.message_id)
            if user_id in user_states: del user_states[user_id]

            bot.answer_callback_query(state['call_id'], "✅ Награда обновлена!")
            call_imitation = types.CallbackQuery(id=state['call_id'], from_user=message.from_user, data=f"admin_my_op_manage_{task_id}", chat_instance="private", json_string="")
            call_imitation.message = state['message'] 
            handle_admin_callbacks(call_imitation)
        except ValueError:
            bot.send_message(user_id, "❌ Ошибка. Введите корректное положительное число.")
        return

    if action == 'awaiting_restart_filter_count':
        try:
            filter_count = int(message.text.strip())
        except ValueError:
            bot.send_message(user_id, "❌ Ошибка. Введите целое число (например, 100) или `-` для всех.")
            return

        bot.delete_message(user_id, state['message_id'])
        bot.delete_message(user_id, message.message_id)
        if user_id in user_states: del user_states[user_id]
        
        confirmation_text = f"всех ботов" if filter_count <= 0 else f"ботов, у которых {filter_count} или более пользователей"
        markup = types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("✅ Да, перезапустить", callback_data=f"admin_restart_filter_confirm_{filter_count}"),
            types.InlineKeyboardButton("❌ Отмена", callback_data="admin_back")
        )
        bot.send_message(user_id, f"Вы уверены, что хотите перезапустить {confirmation_text}?", reply_markup=markup)
        return

    if action == 'awaiting_start_filter_count':
        try:
            filter_count = int(message.text.strip())
        except ValueError:
            bot.send_message(user_id, "❌ Ошибка. Введите целое число (например, 100) или `-` для всех.")
            return

        bot.delete_message(user_id, state['message_id'])
        bot.delete_message(user_id, message.message_id)
        if user_id in user_states: del user_states[user_id]

        confirmation_text = f"все НЕзапущенные боты" if filter_count <= 0 else f"НЕзапущенные боты, у которых {filter_count}+ пользователей"
        markup = types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("✅ Да, запустить", callback_data=f"admin_start_filter_confirm_{filter_count}"),
            types.InlineKeyboardButton("❌ Отмена", callback_data="admin_back")
        )
        bot.send_message(user_id, f"Вы уверены, что хотите запустить {confirmation_text}?", reply_markup=markup)
        return

    if action == 'awaiting_balance_user_id':
        try:
            target_user_id = int(message.text.strip())
            target_user = get_user(target_user_id)
            if not target_user:
                bot.send_message(user_id, "❌ Пользователь с таким ID не найден в базе конструктора.")
                return
            
            bot.delete_message(user_id, state['message_id'])
            bot.delete_message(user_id, message.message_id)

            msg = bot.send_message(user_id, f"Введите сумму для начисления пользователю <code>{target_user_id}</code> (@{target_user['username'] or 'N/A'}):",
                                   parse_mode="HTML", reply_markup=create_cancel_markup())
            set_user_state(user_id, {'action': 'awaiting_balance_amount', 'target_user_id': target_user_id, 'message_id': msg.message_id})
        except ValueError:
            bot.send_message(user_id, "❌ Ошибка. Введите корректный числовой User ID.")
        return

    # HOLD TRANSFER: step 1 -> got user id, show balances and ask for amounts
    if action == 'awaiting_hold_transfer_user_id':
        try:
            target_user_id = int(message.text.strip())
            target_user = get_user(target_user_id)
            if not target_user:
                bot.send_message(user_id, "❌ Пользователь с таким ID не найден в базе конструктора.")
                return

            bot.delete_message(user_id, state['message_id'])
            bot.delete_message(user_id, message.message_id)

            balance = float(target_user['balance'] or 0.0)
            frozen = float(target_user['frozen_balance'] or 0.0)
            info = (
                f"👤 Пользователь: <code>{target_user_id}</code> (@{target_user['username'] or 'N/A'})\n"
                f"💸 Баланс: <b>{balance:.2f} ₽</b>\n"
                f"⏳ На удержании: <b>{frozen:.2f} ₽</b>\n\n"
                f"Введите суммы в формате <code>Х|Y</code> (пример: <code>70|50</code>)\n"
                f"— <b>Х</b>: списать с удержания\n— <b>Y</b>: зачислить на баланс"
            )
            msg = bot.send_message(user_id, info, parse_mode="HTML", reply_markup=create_cancel_markup())
            set_user_state(user_id, {
                'action': 'awaiting_hold_transfer_amounts',
                'target_user_id': target_user_id,
                'message_id': msg.message_id
            })
        except ValueError:
            bot.send_message(user_id, "❌ Ошибка. Введите корректный числовой User ID.")
        return

    # HOLD TRANSFER: step 2 -> parse X|Y and do atomic update
    if action == 'awaiting_hold_transfer_amounts':
        raw = (getattr(message, 'text', '') or '').strip()
        try:
            if '|' not in raw:
                raise ValueError
            left, right = [x.strip() for x in raw.split('|', 1)]
            amount_from_frozen = float(left.replace(',', '.'))
            amount_to_balance = float(right.replace(',', '.'))
            if amount_from_frozen < 0 or amount_to_balance < 0:
                raise ValueError
        except ValueError:
            bot.send_message(user_id, "❌ Формат неверный. Пример: <code>70|50</code>", parse_mode="HTML")
            return

        target_user_id = state['target_user_id']
        # Validate available frozen
        target = get_user(target_user_id)
        frozen_now = float(target['frozen_balance'] or 0.0)
        if amount_from_frozen > frozen_now:
            bot.send_message(user_id, f"❌ Недостаточно средств на удержании. Сейчас: {frozen_now:.2f} ₽")
            return

        # Apply update
        try:
            db_execute(
                "UPDATE users SET frozen_balance = frozen_balance - ?, balance = balance + ? WHERE user_id = ?",
                (amount_from_frozen, amount_to_balance, target_user_id),
                commit=True
            )
        except Exception as e:
            bot.send_message(user_id, f"❌ Ошибка при переводе: {e}")
            return

        # Cleanup and notify
        try: bot.delete_message(user_id, state['message_id'])
        except: pass
        try: bot.delete_message(user_id, message.message_id)
        except: pass
        if user_id in user_states: del user_states[user_id]

        bot.send_message(
            user_id,
            (
                f"✅ Перевод выполнен для <code>{target_user_id}</code>.\n"
                f"⏳ −{amount_from_frozen:.2f} ₽ с удержания, 💸 +{amount_to_balance:.2f} ₽ на баланс."
            ),
            parse_mode="HTML",
            reply_markup=create_main_menu(user_id)
        )
        try:
            bot.send_message(
                target_user_id,
                (
                    f"🔁 Администратор выполнил перевод: ⏳ −{amount_from_frozen:.2f} ₽ с удержания, "
                    f"💸 +{amount_to_balance:.2f} ₽ на баланс."
                )
            )
        except Exception:
            pass
        return

    if action == 'awaiting_balance_amount':
        try:
            amount = float(message.text.strip().replace(',', '.'))
            if amount <= 0:
                raise ValueError("Amount must be positive")
            target_user_id = state['target_user_id']
            
            db_execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, target_user_id), commit=True)

            bot.delete_message(user_id, state['message_id'])
            bot.delete_message(user_id, message.message_id)
            if user_id in user_states: del user_states[user_id]

            bot.send_message(user_id, f"✅ Баланс пользователя <code>{target_user_id}</code> успешно пополнен на <code>{amount:.2f}</code> ₽.", parse_mode="HTML", reply_markup=create_main_menu(user_id))
            try:
                bot.send_message(target_user_id, f"💸 Ваш баланс в конструкторе пополнен администратором на <b>{amount:.2f} ₽</b>!", parse_mode="HTML")
            except Exception as e:
                logging.warning(f"Не удалось уведомить пользователя {target_user_id} о пополнении: {e}")
        except ValueError:
            bot.send_message(user_id, "❌ Ошибка. Введите корректное положительное число (например, 150.5).")
        return

    if action == 'awaiting_bot_id_for_logs':
        try:
            bot_id_to_get_logs = int(message.text)
            log_path = f"logs/bot_{bot_id_to_get_logs}.log"
            
            with open(log_path, "rb") as log_file:
                bot.send_document(user_id, log_file, caption=f"📄 Логи для бота #{bot_id_to_get_logs}")
            
            bot.delete_message(user_id, state['message_id'])
            bot.delete_message(user_id, message.message_id)
            if user_id in user_states: del user_states[user_id]
            bot.send_message(user_id, get_custom_text('admin_menu_heading'), reply_markup=create_admin_menu())

        except (ValueError, TypeError):
            bot.send_message(user_id, "❌ Ошибка. Введите корректный числовой ID бота.")
            bot.delete_message(user_id, message.message_id)
        except FileNotFoundError:
            bot.send_message(user_id, f"❌ Лог-файл для бота #{message.text} не найден. Возможно, бот еще не запускался или ID неверный.")
            bot.delete_message(user_id, message.message_id)
        except Exception as e:
            bot.send_message(user_id, f"Произошла ошибка: {e}")
            if user_id in user_states: del user_states[user_id]
        return

    if action == 'admin_grant_vip':
        try:
            bot_id_to_grant = int(message.text)
            bot_info = get_bot_by_id(bot_id_to_grant)
            if not bot_info:
                bot.send_message(user_id, "❌ Бот с таким ID не найден. Попробуйте снова.")
                try: bot.delete_message(user_id, message.message_id)
                except: pass
                return 

            update_bot_setting(bot_id_to_grant, 'vip_status', True)
            
            try: bot.delete_message(user_id, state['message_id'])
            except: pass
            try: bot.delete_message(user_id, message.message_id)
            except: pass
            
            if user_id in user_states: del user_states[user_id]
            
            bot.send_message(user_id, f"✅ VIP-статус успешно выдан боту #{bot_id_to_grant}.")
            bot.send_message(user_id, get_custom_text('admin_menu_heading'), reply_markup=create_admin_menu())
            
            try:
                bot.send_message(bot_info['owner_id'], f"🎉 Поздравляем! Администратор выдал вашему боту #{bot_id_to_grant} VIP-статус!")
            except Exception as e:
                logging.warning(f"Не удалось уведомить владельца {bot_info['owner_id']} о выдаче VIP: {e}")

        except (ValueError, TypeError):
            bot.send_message(user_id, "❌ Ошибка. Введите корректный числовой ID бота.")
            try: bot.delete_message(user_id, message.message_id)
            except: pass
        return

    if action == 'admin_grant_cashlait':
        try:
            target_user_id = int(message.text)
            bot_id = create_bot_in_db(target_user_id, 'cashlait')
            
            try: bot.delete_message(user_id, state['message_id'])
            except: pass
            try: bot.delete_message(user_id, message.message_id)
            except: pass
            
            if user_id in user_states: del user_states[user_id]
            
            bot.send_message(user_id, f"✅ CashLait бот #{bot_id} успешно выдан пользователю {target_user_id}.")
            bot.send_message(user_id, get_custom_text('admin_menu_heading'), reply_markup=create_admin_menu())
            
            try:
                bot.send_message(target_user_id, f"🎉 Поздравляем! Администратор выдал вам CashLait бота #{bot_id}!")
            except Exception as e:
                logging.warning(f"Не удалось уведомить пользователя {target_user_id} о выдаче CashLait бота: {e}")

        except (ValueError, TypeError):
            bot.send_message(user_id, "❌ Ошибка. Введите корректный числовой ID пользователя.")
            try: bot.delete_message(user_id, message.message_id)
            except: pass
        return

    if action == 'admin_grant_dicelite':
        try:
            target_user_id = int(message.text)
            bot_id = create_bot_in_db(target_user_id, 'dicelite')
            try: bot.delete_message(user_id, state['message_id'])
            except: pass
            try: bot.delete_message(user_id, message.message_id)
            except: pass
            if user_id in user_states:
                del user_states[user_id]
            bot.send_message(user_id, f"✅ DiceLite бот #{bot_id} успешно выдан пользователю {target_user_id}.")
            bot.send_message(user_id, get_custom_text('admin_menu_heading'), reply_markup=create_admin_menu())
            try:
                bot.send_message(target_user_id, f"🎉 Поздравляем! Администратор выдал вам DiceLite бота #{bot_id}!")
            except Exception as e:
                logging.warning(f"Не удалось уведомить пользователя {target_user_id} о выдаче DiceLite бота: {e}")
        except (ValueError, TypeError):
            bot.send_message(user_id, "❌ Ошибка. Введите корректный числовой ID пользователя.")
            try: bot.delete_message(user_id, message.message_id)
            except: pass
        return

    if action == 'editing_setting':
        setting = state['setting']
        bot_id = state['bot_id']
        message_id = state['message_id']
        new_value_raw = message.text.strip()
        
        try: bot.delete_message(user_id, message.message_id)
        except: pass

        if setting == 'flyer_limit':
            try:
                new_limit = int(new_value_raw)
                if not 1 <= new_limit <= 10:
                    raise ValueError("Лимит должен быть от 1 до 10")
                
                bot.delete_message(user_id, state['message_id'])
                if user_id in user_states: del user_states[user_id]

                bot_info = get_bot_by_id(bot_id)
                current_limit = bot_info['flyer_limit']

                bot.send_message(user_id, f"✅ Ваш запрос на изменение лимита с {current_limit} на {new_limit} отправлен на рассмотрение.", reply_markup=create_main_menu(user_id))
                
                owner_info = message.from_user
                text = (f"🚨 <b>Запрос на изменение лимита Flyer!</b>\n\n"
                        f"<b>Бот:</b> @{bot_info['bot_username']} (ID: <code>{bot_id}</code>)\n"
                        f"<b>Владелец:</b> <code>{owner_info.id}</code> (@{owner_info.username or 'N/A'})\n\n"
                        f"Старый лимит: <b>{current_limit}</b>\n"
                        f"Новый лимит: <b>{new_limit}</b>")
                
                markup = types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("✅ Одобрить и перезапустить", callback_data=f"admin_limit_approve_{bot_id}_{new_limit}"),
                    types.InlineKeyboardButton("❌ Отклонить", callback_data=f"admin_limit_decline_{bot_id}_{user_id}")
                )
                bot.send_message(ADMIN_ID, text, reply_markup=markup, parse_mode="HTML")

            except (ValueError, TypeError):
                bot.send_message(user_id, "❌ Ошибка. Введите целое число от 1 до 10.")
            return

        if user_id in user_states: del user_states[user_id]
        error_text = None
        bot_info = get_bot_by_id(bot_id)

        if setting == 'click_reward_min':
            try:
                min_val_str, max_val_str = new_value_raw.replace(',', '.').split('|')
                min_val = float(min_val_str)
                max_val = float(max_val_str)
                if min_val < 0 or max_val < 0 or min_val > max_val:
                    raise ValueError
                update_bot_setting(bot_id, 'click_reward_min', min_val)
                update_bot_setting(bot_id, 'click_reward_max', max_val)
                new_value_raw = None 
            except (ValueError, IndexError):
                error_text = "<b>❌ Ошибка!</b> Введите два положительных числа через |, например <code>0.001|0.005</code>"
                new_value_raw = None

        if new_value_raw is None:
            pass
        elif setting == 'bot_token':
            try:
                test_bot = telebot.TeleBot(new_value_raw, threaded=False)
                test_bot_info = test_bot.get_me()
                update_bot_setting(bot_id, 'bot_username', test_bot_info.username)
                update_bot_setting(bot_id, 'bot_token', new_value_raw)
                if bot_info['status'] == 'unconfigured': update_bot_setting(bot_id, 'status', 'stopped')
            except Exception: error_text = "<b>❌ Ошибка!</b> Токен недействителен."
        
        elif setting in ['ref_reward_1', 'ref_reward_2', 'withdrawal_limit', 
                         'stars_welcome_bonus', 'stars_daily_bonus', 'stars_ref_bonus_referrer', 'stars_ref_bonus_new_user',
                         'click_reward_max', 'welcome_bonus_clicker', 'daily_bonus_clicker', 'ref_bonus_referrer_clicker', 'ref_bonus_new_user_clicker', 'withdrawal_min_clicker',
                         'anonchat_vip_price',]:
            try:
                value = float(new_value_raw.replace(',', '.'))
                if value < 0: raise ValueError
                update_bot_setting(bot_id, setting, value)
            except ValueError: 
                error_text = "<b>❌ Ошибка!</b> Введите положительное числовое значение. Например: <code>1.5</code> или <code>100</code>"
        
        elif setting in ['stars_daily_cooldown', 'energy_max', 'energy_regen_rate', 'daily_bonus_cooldown_clicker', 'anonchat_flyer_tasks_limit',]:
            try:
                value = int(new_value_raw)
                if value < 0: raise ValueError
                update_bot_setting(bot_id, setting, value)
            except ValueError: 
                error_text = "<b>❌ Ошибка!</b> Введите целое положительное число."
        
        elif setting == 'anonchat_channel_id':
            cleaned = new_value_raw.strip()
            if cleaned == '-' or not cleaned:
                cleaned = ''
            else:
                if cleaned.startswith('https://t.me/'):
                    cleaned = '@' + cleaned.split('https://t.me/', 1)[1]
                if not cleaned.startswith('@'):
                    cleaned = '@' + cleaned
            update_bot_setting(bot_id, setting, cleaned)
        
        elif setting in ['anonchat_crypto_api_token', 'anonchat_flyer_api_key']:
            cleaned = '' if new_value_raw.strip() == '-' else new_value_raw.strip()
            update_bot_setting(bot_id, setting, cleaned)
        elif setting in ['cashlait_flyer_api_key', 'cashlait_crypto_pay_token', 'cashlait_currency_symbol', 'dicelite_crypto_pay_token', 'dicelite_welcome_text', 'exchange_welcome_text']:
            cleaned = '' if new_value_raw.strip() == '-' else new_value_raw.strip()
            update_bot_setting(bot_id, setting, cleaned)
        
        else:
            update_bot_setting(bot_id, setting, new_value_raw)

        bot.delete_message(user_id, message_id)
        bot_info = get_bot_by_id(bot_id)
        name = f"@{bot_info['bot_username']}" if bot_info['bot_username'] else f"Бот #{bot_id}"
        
        config_menu = None
        if bot_info['bot_type'] == 'ref':
            config_menu = create_ref_bot_config_menu(bot_id)
        elif bot_info['bot_type'] == 'stars':
            config_menu = create_stars_bot_config_menu(bot_id)
        elif bot_info['bot_type'] == 'clicker':
            config_menu = create_clicker_bot_config_menu(bot_id)
        elif bot_info['bot_type'] == 'anonchat':
            config_menu = create_anonchat_bot_config_menu(bot_id)
        elif bot_info['bot_type'] == 'cashlait':
            config_menu = create_cashlait_bot_config_menu(bot_id)
        elif bot_info['bot_type'] == 'dicelite':
            config_menu = create_dicelite_bot_config_menu(bot_id)
        elif bot_info['bot_type'] == 'exchange':
            config_menu = create_exchange_bot_config_menu(bot_id)
        bot.send_message(user_id, f"⚙️ Меню конфигурации бота {name}", reply_markup=config_menu)
        if error_text: bot.send_message(user_id, error_text, parse_mode="HTML")
        return

    if action == 'admin_broadcast_get_content':
        bot.delete_message(user_id, state['message_id'])
        set_user_state(user_id, {'action': 'admin_broadcast_get_button', 'content_message_id': message.message_id})
        
        msg = bot.send_message(user_id, 
                               "<b>Шаг 2/3:</b> Теперь отправьте текст для инлайн-кнопки и ссылку в формате:\n"
                               "<code>Текст кнопки | https://t.me/link</code>\n\n"
                               "Если кнопка не нужна, отправьте <code>-</code> (минус).",
                               parse_mode="HTML",
                               reply_markup=create_cancel_markup())
        user_states[user_id]['message_id'] = msg.message_id
        return

    if action == 'admin_broadcast_get_button':
        bot.delete_message(user_id, state['message_id'])
        try: bot.delete_message(user_id, message.message_id)
        except: pass
        
        content_msg_id = state['content_message_id']
        button_markup = types.InlineKeyboardMarkup()
        
        if message.text != '-':
            try:
                parts = message.text.split('|', 1)
                if len(parts) != 2: raise ValueError("Invalid format")
                button_text, button_url = parts[0].strip(), parts[1].strip()
                button_markup.add(types.InlineKeyboardButton(text=button_text, url=button_url))
            except Exception:
                bot.send_message(user_id, "❌ Неверный формат. Попробуйте начать рассылку заново.", reply_markup=create_main_menu(user_id))
                if user_id in user_states: del user_states[user_id]
                return
        else:
            button_markup = None
            
        try:
            bot.send_message(user_id, "<b>Шаг 3/3: Предпросмотр</b>\nВот так будет выглядеть сообщение. Начинаем рассылку?", parse_mode="HTML")
            preview_msg = bot.copy_message(user_id, user_id, content_msg_id, reply_markup=button_markup)
            confirm_markup = types.InlineKeyboardMarkup().row(
                types.InlineKeyboardButton("✅ Начать рассылку", callback_data=f"admin_broadcast_confirm_{preview_msg.message_id}"),
                types.InlineKeyboardButton("❌ Отмена", callback_data="admin_broadcast_cancel")
            )
            bot.send_message(user_id, "Подтвердите действие:", reply_markup=confirm_markup)
            if user_id in user_states: del user_states[user_id]
        except Exception as e:
            bot.send_message(user_id, f"❌ Не удалось создать предпросмотр. Ошибка: {e}\n\nПопробуйте снова.", reply_markup=create_main_menu(user_id))
            if user_id in user_states: del user_states[user_id]
        return
        
    if action == 'admin_view_bot_by_id':
        try:
            bot_id_to_view = int(message.text)
            bot_info = get_bot_by_id(bot_id_to_view)
            if not bot_info:
                raise ValueError("Bot not found")
            bot.delete_message(user_id, message.message_id)
            if user_id in user_states: del user_states[user_id]
            show_admin_bot_info(user_id, state['message_id'], bot_id_to_view)
        except (ValueError, TypeError):
            bot.delete_message(user_id, message.message_id)
            bot.edit_message_text("<b>❌ Ошибка! Бот с таким ID не найден.</b>\n\n🔎 Введите ID бота для поиска:",
                                  user_id, state['message_id'], parse_mode="HTML")
        return
        
    if action == 'admin_set_new_op_key_admin':
        bot_id = state['bot_id']
        new_key = message.text.strip()
        bot_info = get_bot_by_id(bot_id)
        if bot_info['bot_type'] == 'ref':
            update_bot_setting(bot_id, 'flyer_api_key', new_key)
            update_bot_setting(bot_id, 'flyer_op_enabled', True if new_key else False)
        elif bot_info['bot_type'] == 'stars':
            update_bot_setting(bot_id, 'stars_flyer_api_key', new_key)
            update_bot_setting(bot_id, 'stars_op_enabled', True if new_key else False)
        elif bot_info['bot_type'] == 'cashlait':
            update_bot_setting(bot_id, 'cashlait_flyer_api_key', new_key)
            update_bot_setting(bot_id, 'flyer_op_enabled', True if new_key else False)
        

        bot.delete_message(user_id, message.message_id)
        if user_id in user_states: del user_states[user_id]
        
        bot.answer_callback_query(state['call_id'], "✅ Ключ Flyer успешно изменен! Перезапустите бота, чтобы применить.", show_alert=True)
        show_admin_bot_info(user_id, state['message_id'], bot_id)
        return
    
    if action == 'admin_reply_text':
        target_user_id = state['target_user_id']
        bot_id = state.get('bot_id')
        try:
            reply_text = f"Сообщение от администратора:\n\n{message.text}"
            if bot_id:
                reply_text = f"Сообщение от администратора по поводу заявки на Flyer для бота #{bot_id}:\n\n{message.text}"
            bot.send_message(target_user_id, reply_text)
            bot.answer_callback_query(state['call_id'], "✅ Сообщение отправлено!", show_alert=True)
        except Exception as e:
            bot.answer_callback_query(state['call_id'], f"❌ Ошибка отправки: {e}", show_alert=True)
        if user_id in user_states: del user_states[user_id]
        return
        
    if action == 'admin_set_flyer_key':
        bot_id = state['bot_id']
        target_user_id = state['target_user_id']
        api_key = message.text.strip()
        bot_info = get_bot_by_id(bot_id)

        if bot_info['bot_type'] == 'ref':
            update_bot_setting(bot_id, 'flyer_op_enabled', True)
            update_bot_setting(bot_id, 'flyer_api_key', api_key)
        elif bot_info['bot_type'] == 'stars':
            update_bot_setting(bot_id, 'stars_op_enabled', True)
            update_bot_setting(bot_id, 'stars_flyer_api_key', api_key)
        elif bot_info['bot_type'] == 'clicker':
            update_bot_setting(bot_id, 'clicker_op_enabled', True)
            update_bot_setting(bot_id, 'clicker_flyer_api_key', api_key)
        elif bot_info['bot_type'] == 'anonchat':
            update_bot_setting(bot_id, 'anonchat_flyer_api_key', api_key)
        elif bot_info['bot_type'] == 'cashlait':
            update_bot_setting(bot_id, 'cashlait_flyer_api_key', api_key)
            update_bot_setting(bot_id, 'flyer_op_enabled', True)
        

        stop_bot_process(bot_id)
        time.sleep(1)
        start_bot_process(bot_id)
        
        bot.send_message(target_user_id, f"✅ Ваша заявка на подключение Flyer для бота #{bot_id} была *одобрена*! Система активирована, бот перезапущен.")
        bot.edit_message_text(state['original_text'] + "\n\n<b>Статус: ✅ ОДОБРЕНО И КЛЮЧ УСТАНОВЛЕН</b>", message.from_user.id, state['message_id'], parse_mode="HTML")
        if user_id in user_states: del user_states[user_id]
        bot.send_message(user_id, "Ключ успешно установлен, бот перезапущен.", reply_markup=create_main_menu(user_id))
        return
        
    if action == 'admin_change_setting':
        setting_key = state['setting_key']
        try:
            text_value = message.text.strip()
            if setting_key == 'MAX_BOTS_PER_USER':
                new_limit = int(float(text_value.replace(',', '.')))
                if new_limit <= 0:
                    raise ValueError
                global MAX_BOTS_PER_USER
                MAX_BOTS_PER_USER = new_limit
                set_setting('MAX_BOTS_PER_USER', str(new_limit))
            else:
                new_value = float(text_value.replace(",", "."))
                if new_value < 0: raise ValueError
                # Проверка минимального значения для cashlait_price
                if setting_key == 'cashlait_price' and 'min_value' in state:
                    min_val = state['min_value']
                    if new_value < min_val:
                        bot.send_message(user_id, f"❌ Ошибка! Минимальная цена для CashLait бота: {min_val} $")
                        try: bot.delete_message(user_id, message.message_id)
                        except: pass
                        return
                set_setting(setting_key, str(new_value))
            if user_id in user_states: del user_states[user_id]
            
            bot.delete_message(user_id, state['message_id'])
            bot.delete_message(user_id, message.message_id)

            bot.answer_callback_query(state['call_id'], "✅ Настройка обновлена!")
            
            callback_to_return = "admin_op_manage"
            if setting_key == 'vip_price':
                callback_to_return = "admin_vip_manage"
            elif setting_key in ('cashlait_price', 'cashlait_task_price', 'cashlait_min_completions'):
                callback_to_return = "admin_cashlait_manage"
            elif setting_key == 'dicelite_price':
                callback_to_return = "admin_dicelite_manage"
            elif setting_key == 'bots_list_min_users':
                callback_to_return = "admin_lists_menu"

            call_imitation = types.CallbackQuery(id=state['call_id'], from_user=message.from_user, data=callback_to_return, chat_instance="private", json_string="")
            
            fake_message = types.Message(message_id=state['message_id'], from_user=None, date=None, chat=message.chat, content_type='text', options={}, json_string="")
            call_imitation.message = fake_message

            handle_admin_callbacks(call_imitation)
        except ValueError:
            bot.send_message(user_id, "❌ Ошибка! Введите положительное числовое значение.")
        return

    if action == 'admin_lists_by_id_input':
        # Ввод ID бота для управления закрепом/скрытием/ручным добавлением
        text_input = (getattr(message, 'text', '') or '').strip()
        match = re.search(r"-?\d+", text_input)
        if not match:
            try:
                bot.edit_message_text(
                    "❌ Бот с таким ID не найден. Введите корректный ID:",
                    user_id,
                    state['message_id'],
                    reply_markup=create_cancel_markup()
                )
            except Exception:
                bot.send_message(user_id, "❌ Ошибка. Введите корректный числовой ID бота.")
            return
        try:
            bid = int(match.group(0))
        except Exception:
            bot.send_message(user_id, "❌ Ошибка. Введите корректный числовой ID бота.")
            return

        bot_info = get_bot_by_id(bid)
        if not bot_info:
            try:
                bot.edit_message_text(
                    "❌ Бот с таким ID не найден. Введите корректный ID:",
                    user_id,
                    state['message_id'],
                    reply_markup=create_cancel_markup()
                )
            except Exception:
                bot.send_message(user_id, "❌ Бот с таким ID не найден. Введите корректный ID:")
            return

        # Очистим пользовательское сообщение с ID
        try:
            bot.delete_message(user_id, message.message_id)
        except Exception:
            pass

        # Сбрасываем состояние и переходим в просмотр через колбек, чтобы переиспользовать логику
        if user_id in user_states:
            del user_states[user_id]

        call_imitation = types.CallbackQuery(
            id=state.get('call_id', 'fake'),
            from_user=message.from_user,
            data=f"admin_list_view_{bid}",
            chat_instance="private",
            json_string=""
        )
        fake_message = types.Message(
            message_id=state['message_id'],
            from_user=None,
            date=None,
            chat=message.chat,
            content_type='text',
            options={},
            json_string=""
        )
        call_imitation.message = fake_message
        handle_admin_callbacks(call_imitation)
        return

    if action == 'admin_lists_add_manual':
        # Добавление бота в ручной список отображения
        try:
            bid = int(message.text.strip())
            manual = set(json.loads(get_setting('bots_list_manual') or '[]'))
            manual.add(bid)
            set_setting('bots_list_manual', json.dumps(sorted(manual)))
            try: bot.delete_message(user_id, state['message_id'])
            except: pass
            try: bot.delete_message(user_id, message.message_id)
            except: pass
            if user_id in user_states: del user_states[user_id]
            bot.answer_callback_query(state.get('call_id', ''), "Бот добавлен", show_alert=False)
            # Вернуться к просмотру управления по ID
            call_imitation = types.CallbackQuery(id=state.get('call_id', 'fake'), from_user=message.from_user, data=f'admin_list_view_{bid}', chat_instance="private", json_string="")
            fake_message = types.Message(message_id=state['message_id'], from_user=None, date=None, chat=message.chat, content_type='text', options={}, json_string="")
            call_imitation.message = fake_message
            handle_admin_callbacks(call_imitation)
        except Exception:
            bot.send_message(user_id, "❌ Ошибка! Введите корректный ID бота.")
        return

    if action == 'creator_withdrawal_details':
        amount = state['amount']
        details = message.text.strip()
        
        db_execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, user_id), commit=True)
        withdrawal_id = db_execute("INSERT INTO creator_withdrawals (user_id, amount, details, created_at) VALUES (?, ?, ?, ?)", 
                                   (user_id, amount, details, datetime.now()), commit=True)
        
        bot.delete_message(user_id, state['message_id'])
        bot.send_message(user_id, "✅ Заявка на вывод создана и отправлена администратору!", reply_markup=create_main_menu(user_id))
        if user_id in user_states: del user_states[user_id]

        user_info = get_user(user_id)
        admin_text = (f"📬 <b>Новая заявка на вывод №{withdrawal_id}</b>\n\n"
                      f"👤 Пользователь: <code>{user_id}</code> (@{escape(user_info['username'] or 'N/A')})\n"
                      f"💰 Сумма: <code>{amount:.2f} ₽</code>\n"
                      f"💳 Реквизиты: <code>{escape(details)}</code>")
        admin_markup = types.InlineKeyboardMarkup(row_width=2).add(
            types.InlineKeyboardButton("📄 Посмотреть в списке", callback_data=f"admin_wd_view_{withdrawal_id}")
        )
        bot.send_message(ADMIN_ID, admin_text, parse_mode="HTML", reply_markup=admin_markup)
        return

    if action == 'transfer_bot':
        bot_id = state.get('bot_id')
        message_id = state['message_id']
        new_value_raw = message.text.strip()
        if user_id in user_states: del user_states[user_id]

        try:
            new_owner_id = int(new_value_raw)
            markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("✅ Да, передать", callback_data=f"transfer_{bot_id}_confirm_{new_owner_id}"),
                                                      types.InlineKeyboardButton("❌ Нет, отмена", callback_data=f"actions_{bot_id}"))
            bot.edit_message_text(f"Вы уверены, что хотите передать бота пользователю с ID <code>{new_owner_id}</code>? Это действие необратимо.", user_id, message_id, reply_markup=markup, parse_mode="HTML")
        except ValueError:
            bot.edit_message_text(f"<b>❌ Ошибка!</b> Введите корректный числовой ID.\n\nГлавное меню бота:", user_id, message_id, reply_markup=create_bot_actions_menu(bot_id), parse_mode="HTML")
        return

    if action == 'add_admin':
        bot_id = state.get('bot_id')
        message_id = state['message_id']
        new_value_raw = message.text.strip()
        if user_id in user_states: del user_states[user_id]

        try:
            admin_id = int(new_value_raw)
            bot_info = get_bot_by_id(bot_id)
            admins = json.loads(bot_info['admins'])
            if admin_id not in admins:
                admins.append(admin_id)
                update_bot_setting(bot_id, 'admins', json.dumps(admins))
            text = f"✅ Администратор <code>{admin_id}</code> добавлен.\n\nТекущие админы: <code>{', '.join(map(str, admins))}</code>"
            config_menu = None
            if bot_info['bot_type'] == 'ref':
                config_menu = create_ref_bot_config_menu(bot_id)
            elif bot_info['bot_type'] == 'stars':
                config_menu = create_stars_bot_config_menu(bot_id)
            elif bot_info['bot_type'] == 'clicker':
                config_menu = create_clicker_bot_config_menu(bot_id)
            elif bot_info['bot_type'] == 'anonchat':
                config_menu = create_anonchat_bot_config_menu(bot_id)
            elif bot_info['bot_type'] == 'cashlait':
                config_menu = create_cashlait_bot_config_menu(bot_id)
            elif bot_info['bot_type'] == 'dicelite':
                config_menu = create_dicelite_bot_config_menu(bot_id)
            elif bot_info['bot_type'] == 'exchange':
                config_menu = create_exchange_bot_config_menu(bot_id)
            
            bot.edit_message_text(text, user_id, message_id, reply_markup=config_menu, parse_mode="HTML")
        except ValueError:
            bot.edit_message_text("<b>❌ Ошибка!</b> Введите корректный числовой ID.", user_id, message_id, reply_markup=create_bot_actions_menu(bot_id), parse_mode="HTML")
        return
        
def get_total_earned_by_user(user_id):
    approved_sum = db_execute("SELECT SUM(amount) FROM creator_withdrawals WHERE user_id = ? AND status = 'approved'", (user_id,), fetchone=True)[0]
    return approved_sum or 0.0

def handle_personal_cabinet(message_or_call):
    is_call = isinstance(message_or_call, telebot.types.CallbackQuery)
    user_id = message_or_call.from_user.id
    chat_id = message_or_call.message.chat.id if is_call else message_or_call.chat.id
    message_id = message_or_call.message.message_id if is_call else None
    user_info = get_user(user_id)
    balance = user_info['balance'] if user_info else 0.0
    frozen_balance = user_info['frozen_balance'] if user_info else 0.0
    total_earned = get_total_earned_by_user(user_id)
    text = (f"💰 *Личный кабинет*\n\n"
            f"Здесь отображается ваш доход от системы дополнительного заработка.\n\n"
            f"💸 *Ваш баланс:* `{balance:.2f} ₽`\n"
            f"⏳ *На удержании (24ч):* `{frozen_balance:.2f} ₽`\n"
            f"📈 *Всего заработано:* `{total_earned:.2f} ₽`")
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📤 Вывести средства", callback_data="creator_withdraw_start"))
    markup.add(types.InlineKeyboardButton("📜 История выводов", callback_data="creator_withdraw_history"))
    if is_call:
        try: bot.edit_message_text(text, chat_id, message_id, parse_mode="Markdown", reply_markup=markup)
        except: pass
    else: bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)

def handle_admin_callbacks(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id if call.message else user_id
    if not is_admin(user_id): 
        bot.answer_callback_query(call.id)
        return

    # Guard: disable all 'Списки ботов' admin actions when feature is off
    try:
        bl_enabled_raw = get_setting('bots_list_feature_enabled')
        bl_enabled = str(bl_enabled_raw).strip() in ('1', 'true', 'True') if bl_enabled_raw is not None else True
    except Exception:
        bl_enabled = True
    if not bl_enabled and (call.data.startswith("admin_lists_") or call.data.startswith("admin_list_")):
        try:
            bot.answer_callback_query(call.id, "Функционал 'Списки ботов' отключен", show_alert=True)
        except Exception:
            pass
        return

    # CashLait management - должно быть раньше других обработчиков
    if call.data.startswith("admin_cashlait_"):
        bot.answer_callback_query(call.id)
        parts = call.data.split('_')
        sub_action = parts[2] if len(parts) > 2 else None
        
        if sub_action == "manage":
            if user_id in user_states:
                del user_states[user_id]

            cashlait_price = get_setting('cashlait_price') or '1.0'
            task_price = get_setting('cashlait_task_price') or '0.1'
            min_completions = get_setting('cashlait_min_completions') or '10'
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton(f"💰 Изменить цену CashLait ({cashlait_price} $)", callback_data="admin_cashlait_set_price"))
            markup.add(types.InlineKeyboardButton(f"💵 Цена за задание ({task_price} $)", callback_data="admin_cashlait_set_task_price"))
            markup.add(types.InlineKeyboardButton(f"📊 Мин. выполнений ({min_completions})", callback_data="admin_cashlait_set_min_completions"))
            markup.add(types.InlineKeyboardButton("🎁 Выдать CashLait бота", callback_data="admin_cashlait_grant"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
            try:
                bot.edit_message_text("💼 Управление CashLait ботом:", user_id, call.message.message_id, reply_markup=markup)
            except telebot.apihelper.ApiTelegramException:
                bot.send_message(user_id, "💼 Управление CashLait ботом:", reply_markup=markup)
            return
        
        elif sub_action == "set" and len(parts) > 3:
            if parts[3] == "price":
                msg = bot.edit_message_text("Введите новую цену для CashLait бота в долларах (минимум 1.0):", user_id, call.message.message_id,
                                            reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_cashlait_manage")))
                set_user_state(user_id, {'action': 'admin_change_setting', 'setting_key': 'cashlait_price', 'message_id': msg.message_id, 'call_id': call.id, 'message': call.message, 'min_value': 1.0})
                return
            elif parts[3] == "task" and len(parts) > 4 and parts[4] == "price":
                msg = bot.edit_message_text("Введите цену за одно задание в долларах:", user_id, call.message.message_id,
                                            reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_cashlait_manage")))
                set_user_state(user_id, {'action': 'admin_change_setting', 'setting_key': 'cashlait_task_price', 'message_id': msg.message_id, 'call_id': call.id, 'message': call.message, 'min_value': 0.01})
                return
            elif parts[3] == "min" and len(parts) > 4 and parts[4] == "completions":
                msg = bot.edit_message_text("Введите минимальное количество выполнений задания:", user_id, call.message.message_id,
                                            reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_cashlait_manage")))
                set_user_state(user_id, {'action': 'admin_change_setting', 'setting_key': 'cashlait_min_completions', 'message_id': msg.message_id, 'call_id': call.id, 'message': call.message, 'min_value': 1.0})
                return
        
        elif sub_action == "grant":
            cancel_markup = types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_cashlait_manage")
            )
            msg = bot.edit_message_text(
                "Введите ID пользователя, которому нужно выдать CashLait бота:", 
                user_id, 
                call.message.message_id,
                reply_markup=cancel_markup
            )
            set_user_state(user_id, {'action': 'admin_grant_cashlait', 'message_id': msg.message_id, 'call_id': call.id})
            return

    if call.data.startswith("admin_dicelite_"):
        bot.answer_callback_query(call.id)
        parts = call.data.split('_')
        sub_action = parts[2] if len(parts) > 2 else None

        if sub_action == "manage":
            if user_id in user_states:
                del user_states[user_id]
            dicelite_price = get_setting('dicelite_price') or '1.0'
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton(f"💰 Изменить цену DiceLite ({dicelite_price} $)", callback_data="admin_dicelite_set_price"))
            markup.add(types.InlineKeyboardButton("🎁 Выдать DiceLite бота", callback_data="admin_dicelite_grant"))
            markup.add(types.InlineKeyboardButton("🔗 Ссылка на креатора", callback_data="admin_dicelite_link"))
            markup.add(types.InlineKeyboardButton("📝 Текст кнопки", callback_data="admin_dicelite_linktext"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
            try:
                bot.edit_message_text("🎲 Управление DiceLite ботом:", user_id, call.message.message_id, reply_markup=markup)
            except telebot.apihelper.ApiTelegramException:
                bot.send_message(user_id, "🎲 Управление DiceLite ботом:", reply_markup=markup)
            return

        if sub_action == "set":
            try:
                msg = bot.edit_message_text(
                    "💰 Введите новую цену (в $) для DiceLite:",
                    user_id,
                    call.message.message_id,
                    reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_dicelite_manage"))
                )
            except telebot.apihelper.ApiTelegramException:
                msg = bot.send_message(
                    user_id,
                    "💰 Введите новую цену (в $) для DiceLite:",
                    reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_dicelite_manage"))
                )
            set_user_state(user_id, {'action': 'admin_change_setting', 'setting_key': 'dicelite_price', 'message_id': msg.message_id, 'call_id': call.id, 'message': call.message, 'min_value': 1.0})
            return

        if sub_action == "grant":
            try:
                msg = bot.edit_message_text(
                    "🎁 Введите ID пользователя, которому нужно выдать DiceLite бота:",
                    user_id,
                    call.message.message_id,
                    reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_dicelite_manage"))
                )
            except telebot.apihelper.ApiTelegramException:
                msg = bot.send_message(
                    user_id,
                    "🎁 Введите ID пользователя, которому нужно выдать DiceLite бота:",
                    reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_dicelite_manage"))
                )
            set_user_state(user_id, {'action': 'admin_grant_dicelite', 'message_id': msg.message_id, 'call_id': call.id})
            return

        if sub_action in ("link", "linktext"):
            key_map = {
                'link': 'constructor_bot_link',
                'linktext': 'constructor_bot_link_text',
            }
            text_key = key_map.get(sub_action)
            if text_key:
                prompt_custom_text_edit(user_id, text_key, call.message.message_id, back_callback="admin_dicelite_manage")
            return

        return

    if call.data.startswith("admin_anonchat_"):
        bot.answer_callback_query(call.id)
        parts = call.data.split('_')
        sub_action = parts[2] if len(parts) > 2 else None

        if sub_action == "manage":
            if user_id in user_states:
                del user_states[user_id]
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton("👥 Изменить приветствие", callback_data="admin_anonchat_welcome"))
            markup.add(types.InlineKeyboardButton("📊 Статистика ботов", callback_data="admin_anonchat_stats"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
            try:
                bot.edit_message_text("💬 Управление Анонимным чатом:", user_id, call.message.message_id, reply_markup=markup)
            except telebot.apihelper.ApiTelegramException:
                bot.send_message(user_id, "💬 Управление Анонимным чатом:", reply_markup=markup)
            return

        if sub_action == "welcome":
            prompt_custom_text_edit(user_id, 'creator_welcome', call.message.message_id, back_callback="admin_anonchat_manage")
            return

        if sub_action == "stats":
            # Показать статистику по анон чат ботам
            anonchat_bots = db_execute("SELECT COUNT(*) FROM bots WHERE bot_type = 'anonchat'", fetchone=True)[0]
            active_anonchat = db_execute("SELECT COUNT(*) FROM bots WHERE bot_type = 'anonchat' AND status = 'running'", fetchone=True)[0]
            text = f"📊 Статистика Анонимного чата:\n\n💬 Всего ботов: {anonchat_bots}\n▶️ Активных: {active_anonchat}"
            bot.edit_message_text(text, user_id, call.message.message_id, reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_anonchat_manage")))
            return

        return

    if call.data.startswith("admin_customization"):
        handle_admin_customization(call)
        return
    
    # Top-level shortcut to edit creator welcome (from main admin menu)
    if call.data == "admin_edit_creator_welcome":
        prompt_custom_text_edit(user_id, 'creator_welcome', getattr(call.message, 'message_id', None), back_callback="admin_back")
        bot.answer_callback_query(call.id)
        return

    # Crypto Pay settings
    if call.data == "admin_crypto_pay_manage":
        current_token = get_setting('crypto_pay_token') or '—'
        token_mask = 'установлен' if current_token and current_token != '—' else 'не установлен'
        text = (
            "🧩 <b>Настройки Crypto Pay</b>\n\n"
            f"Токен сейчас: <b>{token_mask}</b>\n\n"
            "Нажмите, чтобы вставить/изменить токен для приема оплат."
        )
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("🔑 Вставить/изменить токен", callback_data="admin_crypto_pay_set_token"))
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
        try:
            bot.edit_message_text(text, user_id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
        except telebot.apihelper.ApiTelegramException:
            bot.send_message(user_id, text, parse_mode="HTML", reply_markup=markup)
        return

    # Bot broadcast menu
    if call.data == "admin_broadcast_bots_menu":
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("📤 В одном боте", callback_data="admin_broadcast_bot_single"))
        markup.add(types.InlineKeyboardButton("📤 В нескольких ботах (очередь)", callback_data="admin_broadcast_bot_multi"))
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
        try:
            bot.edit_message_text("Выберите режим рассылки по ботам:", user_id, call.message.message_id, reply_markup=markup)
        except telebot.apihelper.ApiTelegramException:
            bot.send_message(user_id, "Выберите режим рассылки по ботам:", reply_markup=markup)
        return

    if call.data in ("admin_broadcast_bot_single", "admin_broadcast_bot_multi"):
        is_multi = call.data.endswith("_multi")
        prompt = "Введите ID бота, где сделать рассылку:" if not is_multi else "Введите количество активных ботов для массовой рассылки (очередью):"
        msg = bot.edit_message_text(prompt, user_id, call.message.message_id, reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_broadcast_bots_menu")))
        action_key = 'admin_broadcast_bot_single_id' if not is_multi else 'admin_broadcast_bot_multi_count'
        set_user_state(user_id, {'action': action_key, 'message_id': msg.message_id})
        return

    if call.data == "admin_crypto_pay_set_token":
        msg = bot.edit_message_text(
            "Отправьте новый <b>Crypto Pay API токен</b> (из @CryptoBot → Crypto Pay → Создать приложение):",
            user_id,
            call.message.message_id,
            parse_mode="HTML",
            reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_crypto_pay_manage"))
        )
        set_user_state(user_id, {'action': 'admin_set_crypto_pay_token', 'message_id': msg.message_id, 'call_id': call.id})
        return

    # Admin chat link setting
    if call.data == "admin_set_chat_link":
        msg = bot.edit_message_text(
            "Отправьте ссылку-приглашение на <b>чат администраторов</b> (например, https://t.me/your_chat):",
            user_id,
            call.message.message_id,
            parse_mode="HTML",
            reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
        )
        set_user_state(user_id, {'action': 'admin_set_chat_link', 'message_id': msg.message_id, 'call_id': call.id})
        return

    # Channel link setting
    if call.data == "admin_set_channel_link":
        msg = bot.edit_message_text(
            "Отправьте ссылку на <b>канал</b> (например, https://t.me/your_channel):",
            user_id,
            call.message.message_id,
            parse_mode="HTML",
            reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
        )
        set_user_state(user_id, {'action': 'admin_set_channel_link', 'message_id': msg.message_id, 'call_id': call.id})
        return

    if call.data.startswith("admin_my_op_"):
        parts = call.data.split('_')
        action = parts[3]
        
        if action == "menu":
            tasks = db_execute("SELECT * FROM admin_tasks ORDER BY id DESC", fetchall=True)
            text = "✨ <b>Управление заданиями 'Мои ОП'</b>\n\n"
            if not tasks:
                text += "Вы еще не добавили ни одного задания."
            markup = types.InlineKeyboardMarkup(row_width=1)
            for task in tasks:
                status_icon = "🟢" if task['is_active'] else "🔴"
                markup.add(types.InlineKeyboardButton(f"{status_icon} {escape(task['title'])} ({task['reward']} ₽)", callback_data=f"admin_my_op_manage_{task['id']}"))
            
            markup.add(types.InlineKeyboardButton("➕ Добавить новое задание", callback_data="admin_my_op_add"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад в админку", callback_data="admin_back"))
            
            try:
                bot.edit_message_text(text, user_id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
            except telebot.apihelper.ApiTelegramException:
                bot.send_message(user_id, text, parse_mode="HTML", reply_markup=markup)
            return

        elif action == "add":
            msg = bot.edit_message_text("<b>Шаг 1/3:</b> Введите название для задания (этот текст увидит пользователь):",
                                        user_id, call.message.message_id, parse_mode="HTML",
                                        reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Отмена", callback_data="admin_my_op_menu")))
            set_user_state(user_id, {'action': 'admin_my_op_add_title', 'message_id': msg.message_id})
            return
        
        task_id = int(parts[4])
        if action == "manage":
            task = db_execute("SELECT * FROM admin_tasks WHERE id = ?", (task_id,), fetchone=True)
            if not task:
                bot.answer_callback_query(call.id, "❌ Задание не найдено!", show_alert=True)
                return

            status_text = "🟢 Активно" if task['is_active'] else "🔴 Неактивно"
            text = (f"<b>Управление заданием:</b> {escape(task['title'])}\n\n"
                    f"<b>Ссылка:</b> {escape(task['resource_link'])}\n"
                    f"<b>Награда владельцу:</b> {task['reward']} ₽\n"
                    f"<b>Статус:</b> {status_text}")
            
            markup = types.InlineKeyboardMarkup(row_width=2)
            toggle_text = "🔴 Деактивировать" if task['is_active'] else "🟢 Активировать"
            markup.add(types.InlineKeyboardButton(toggle_text, callback_data=f"admin_my_op_toggle_{task_id}"))
            markup.add(types.InlineKeyboardButton("✏️ Изменить награду", callback_data=f"admin_my_op_editreward_{task_id}"))
            markup.add(types.InlineKeyboardButton("🗑️ Удалить задание", callback_data=f"admin_my_op_delete_{task_id}_confirm"))
            markup.add(types.InlineKeyboardButton("⬅️ К списку заданий", callback_data="admin_my_op_menu"))
            bot.edit_message_text(text, user_id, call.message.message_id, parse_mode="HTML", reply_markup=markup)

        elif action == "toggle":
            current_status = db_execute("SELECT is_active FROM admin_tasks WHERE id = ?", (task_id,), fetchone=True)['is_active']
            db_execute("UPDATE admin_tasks SET is_active = ? WHERE id = ?", (not current_status, task_id), commit=True)
            bot.answer_callback_query(call.id, "Статус изменен!")
            call.data = f"admin_my_op_manage_{task_id}"
            handle_admin_callbacks(call)

        elif action == "editreward":
            task = db_execute("SELECT reward FROM admin_tasks WHERE id = ?", (task_id,), fetchone=True)
            msg = bot.edit_message_text(f"Текущая награда: {task['reward']} ₽. Введите новое значение:",
                                        user_id, call.message.message_id,
                                        reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Отмена", callback_data=f"admin_my_op_manage_{task_id}")))
            set_user_state(user_id, {'action': 'admin_my_op_edit_reward', 'task_id': task_id, 'message_id': msg.message_id, 'call_id': call.id, 'message': call.message})
            
        elif action == "delete":
            if parts[5] == 'confirm':
                markup = types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("❗️ Да, удалить", callback_data=f"admin_my_op_delete_{task_id}_final"),
                    types.InlineKeyboardButton("⬅️ Отмена", callback_data=f"admin_my_op_manage_{task_id}")
                )
                bot.edit_message_text("Вы уверены, что хотите удалить это задание? Это действие необратимо.", 
                                      user_id, call.message.message_id, reply_markup=markup)
            elif parts[5] == 'final':
                db_execute("DELETE FROM admin_tasks WHERE id = ?", (task_id,), commit=True)
                db_execute("DELETE FROM user_completed_admin_tasks WHERE task_id = ?", (task_id,), commit=True)
                bot.answer_callback_query(call.id, "✅ Задание удалено!")
                call.data = "admin_my_op_menu"
                handle_admin_callbacks(call)
        return
    
    if call.data.startswith("admin_lists_") or call.data.startswith("admin_list_"):
        # Расширенные настройки для списков ботов
        parts = call.data.split('_')
        # admin_lists_menu, admin_lists_set_min, admin_list_byid_start, admin_list_view_*, admin_list_pin_*, admin_list_unpin_*, admin_list_hide_*, admin_list_unhide_*, admin_list_add_manual, admin_list_del_manual_*
        action = parts[2] if call.data.startswith("admin_lists_") else parts[1]
        if call.data == 'admin_lists_menu':
            current_min = get_setting('bots_list_min_users') or '30'
            pinned = json.loads(get_setting('bots_list_pinned') or '[]')
            manual = json.loads(get_setting('bots_list_manual') or '[]')
            hidden = json.loads(get_setting('bots_list_hidden') or '[]')
            summary = (
                f"📌 Закреплено: {len(pinned)} | ➕ Вручную: {len(manual)} | 🚫 Скрыто: {len(hidden)}"
            )
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton("🤖 Боты с Flyer ОП", callback_data="admin_lists_op"))
            markup.add(types.InlineKeyboardButton(f"⚙️ Порог для 'Списков ботов' ({current_min})", callback_data="admin_lists_set_min"))
            markup.add(types.InlineKeyboardButton("🔎 Управление по ID", callback_data="admin_list_byid_start"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
            bot.edit_message_text(f"📂 Выберите список для просмотра:\n\n{summary}", chat_id, call.message.message_id, reply_markup=markup)
            return
        if call.data == 'admin_lists_set_min':
            msg = bot.edit_message_text(
                "Введите минимальное число пользователей для показа в меню '📋 Списки ботов' (целое число):", chat_id, call.message.message_id,
                reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_lists_menu"))
            )
            set_user_state(user_id, {'action': 'admin_change_setting', 'setting_key': 'bots_list_min_users', 'message_id': msg.message_id, 'call_id': call.id, 'message': call.message})
            return
        # Удалено: отдельное управление списками через список. Используйте "Управление по ID".
        # Новый режим: управление по введенному ID
        if call.data == 'admin_list_byid_start':
            msg = bot.edit_message_text(
                "Введите ID бота для управления закрепом/скрытием/ручным добавлением:", chat_id, call.message.message_id,
                reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_lists_menu")
                )
            )
            set_user_state(user_id, {
                'action': 'admin_lists_by_id_input',
                'message_id': msg.message_id,
                'call_id': call.id,
                'message': call.message
            })
            return
        # Быстрый просмотр/переключение по ID из режима "управление по ID"
        if call.data.startswith("admin_list_view_"):
            parts_local = call.data.split('_')
            # Форматы: admin_list_view_{id} | admin_list_view_{toggle}_{id}
            def render_view(bid: int):
                bot_info = get_bot_by_id(bid)
                pinned_set = set(json.loads(get_setting('bots_list_pinned') or '[]'))
                hidden_set = set(json.loads(get_setting('bots_list_hidden') or '[]'))
                manual_set = set(json.loads(get_setting('bots_list_manual') or '[]'))
                name = 'Без имени'
                btype = '—'
                if bot_info:
                    try:
                        name = f"@{bot_info['bot_username']}" if bot_info['bot_username'] else 'Без имени'
                        btype = bot_info.get('bot_type', '—')
                    except Exception:
                        pass
                state_labels = []
                if bid in pinned_set: state_labels.append('📌')
                if bid in hidden_set: state_labels.append('🚫')
                if bid in manual_set: state_labels.append('➕')
                label = ' '.join(state_labels) or '—'
                text = (
                    f"<b>🔎 Управление ботом</b>\n\n"
                    f"ID: <code>{bid}</code> | {escape(name)}\n"
                    f"Тип: <code>{escape(btype)}</code>\n"
                    f"Состояние: [{label}]\n\n"
                    f"Нажмите на кнопки ниже, чтобы изменить состояние."
                )
                m = types.InlineKeyboardMarkup(row_width=2)
                # Pin toggle
                if bid in pinned_set:
                    m.add(types.InlineKeyboardButton("📌 Открепить", callback_data=f"admin_list_view_unpin_{bid}"))
                else:
                    m.add(types.InlineKeyboardButton("📌 Закрепить", callback_data=f"admin_list_view_pin_{bid}"))
                # Hide toggle
                if bid in hidden_set:
                    m.add(types.InlineKeyboardButton("🚫 Показать", callback_data=f"admin_list_view_unhide_{bid}"))
                else:
                    m.add(types.InlineKeyboardButton("🚫 Скрыть", callback_data=f"admin_list_view_hide_{bid}"))
                # Manual toggle
                if bid in manual_set:
                    m.add(types.InlineKeyboardButton("➖ Удалить из ручных", callback_data=f"admin_list_view_del_{bid}"))
                else:
                    m.add(types.InlineKeyboardButton("➕ Добавить в ручные", callback_data=f"admin_list_view_add_{bid}"))
                m.add(types.InlineKeyboardButton("⤴️ Ввести другой ID", callback_data="admin_list_byid_start"))
                m.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_lists_menu"))
                try:
                    bot.edit_message_text(text, chat_id, call.message.message_id, parse_mode="HTML", reply_markup=m)
                except telebot.apihelper.ApiTelegramException:
                    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=m)

            try:
                if len(parts_local) == 4 and parts_local[3].isdigit():
                    bid = int(parts_local[3])
                    if not get_bot_by_id(bid):
                        bot.answer_callback_query(call.id, "❌ Бот не найден", show_alert=True)
                        return
                    render_view(bid)
                    return
                elif len(parts_local) == 5 and parts_local[4].isdigit():
                    action2 = parts_local[3]
                    bid = int(parts_local[4])
                    if action2 in ("pin", "unpin"):
                        pinned = set(json.loads(get_setting('bots_list_pinned') or '[]'))
                        if action2 == "pin":
                            pinned.add(bid)
                        else:
                            pinned.discard(bid)
                        set_setting('bots_list_pinned', json.dumps(sorted(pinned)))
                        bot.answer_callback_query(call.id, "Готово")
                        render_view(bid)
                        return
                    if action2 in ("hide", "unhide"):
                        hidden = set(json.loads(get_setting('bots_list_hidden') or '[]'))
                        if action2 == "hide":
                            hidden.add(bid)
                        else:
                            hidden.discard(bid)
                        set_setting('bots_list_hidden', json.dumps(sorted(hidden)))
                        bot.answer_callback_query(call.id, "Готово")
                        render_view(bid)
                        return
                    if action2 in ("add", "del"):
                        manual = set(json.loads(get_setting('bots_list_manual') or '[]'))
                        if action2 == "add":
                            manual.add(bid)
                        else:
                            manual.discard(bid)
                        set_setting('bots_list_manual', json.dumps(sorted(manual)))
                        bot.answer_callback_query(call.id, "Готово")
                        render_view(bid)
                        return
                # Fallback
                bot.answer_callback_query(call.id)
                return
            except Exception:
                bot.answer_callback_query(call.id)
                return

        # Обработка конкретных действий
        if action in ("pin", "unpin", "hide", "unhide", "del", "add"):
            # Normalize for admin_list_*
            if call.data.startswith("admin_list_add_manual_"):
                try:
                    bid = int(call.data.split('_')[-1])
                except Exception:
                    bot.answer_callback_query(call.id)
                    return
                manual = set(json.loads(get_setting('bots_list_manual') or '[]'))
                manual.add(bid)
                set_setting('bots_list_manual', json.dumps(sorted(manual)))
                bot.answer_callback_query(call.id, "Готово")
                # Если пришли из режима просмотра по ID — вернемся туда
                try:
                    prev = call.message.reply_markup
                    # Простая эвристика: если в тексте есть "Управление ботом", значит это страница просмотра
                    if call.message and getattr(call.message, 'text', '') and 'Управление ботом' in call.message.text:
                        call.data = f"admin_list_view_{bid}"
                        handle_admin_callbacks(call)
                        return
                except Exception:
                    pass
                call.data = f'admin_list_view_{bid}'; handle_admin_callbacks(call); return
            if call.data.startswith("admin_list_add_manual"):
                msg = bot.edit_message_text("Введите ID бота, который нужно ДОБАВИТЬ в списки вручную:", chat_id, call.message.message_id,
                                            reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_lists_menu")))
                set_user_state(user_id, {'action': 'admin_lists_add_manual', 'message_id': msg.message_id, 'call_id': call.id, 'message': call.message})
                return
            try:
                bid = int(parts[-1])
            except Exception:
                bot.answer_callback_query(call.id)
                return
            if call.data.startswith("admin_list_pin_") or call.data.startswith("admin_list_unpin_"):
                pinned = set(json.loads(get_setting('bots_list_pinned') or '[]'))
                if call.data.startswith("admin_list_pin_"):
                    pinned.add(bid)
                else:
                    pinned.discard(bid)
                set_setting('bots_list_pinned', json.dumps(sorted(pinned)))
                bot.answer_callback_query(call.id, "Готово")
                call.data = f'admin_list_view_{bid}'; handle_admin_callbacks(call); return
            if call.data.startswith("admin_list_hide_") or call.data.startswith("admin_list_unhide_"):
                hidden = set(json.loads(get_setting('bots_list_hidden') or '[]'))
                if call.data.startswith("admin_list_hide_"):
                    hidden.add(bid)
                else:
                    hidden.discard(bid)
                set_setting('bots_list_hidden', json.dumps(sorted(hidden)))
                bot.answer_callback_query(call.id, "Готово")
                call.data = f'admin_list_view_{bid}'; handle_admin_callbacks(call); return
            if call.data.startswith("admin_list_del_manual_"):
                manual = set(json.loads(get_setting('bots_list_manual') or '[]'))
                manual.discard(bid)
                set_setting('bots_list_manual', json.dumps(sorted(manual)))
                bot.answer_callback_query(call.id, "Удалено из ручного списка")
                call.data = f'admin_list_view_{bid}'; handle_admin_callbacks(call); return
        # Не наши действия — даем выполниться стандартной логике ниже

    if call.data.startswith("admin_balance_"):
        bot.answer_callback_query(call.id)
        action = call.data.split('_')[2]
        if action == "add" and call.data.endswith("start"):
            msg = bot.edit_message_text("💸 Введите ID пользователя для пополнения баланса:",
                                        user_id, call.message.message_id,
                                        reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back")))
            set_user_state(user_id, {'action': 'awaiting_balance_user_id', 'message_id': msg.message_id})
        return

    # Start: transfer from hold flow
    if call.data == "admin_hold_transfer_start":
        bot.answer_callback_query(call.id)
        msg = bot.edit_message_text(
            "🔁 Введите ID пользователя для перевода с удержания:",
            user_id,
            call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
        )
        set_user_state(user_id, {'action': 'awaiting_hold_transfer_user_id', 'message_id': msg.message_id})
        return

    if call.data.startswith("admin_restart_filter_"):
        bot.answer_callback_query(call.id)
        action = call.data.split('_')[3]
        if action == "start":
            # Send a new message with a reply keyboard for cancel to ensure cancel works reliably
            msg = bot.send_message(user_id,
                                   "🔄 <b>Массовый перезапуск ботов</b>\n\n"
                                   "Введите минимальное количество пользователей для перезапуска.\n"
                                   "Например, <code>100</code> перезапустит всех ботов, у кого 100+ пользователей.\n\n"
                                   "Отправьте <code>-</code> или <code>0</code>, чтобы перезапустить <b>всех</b> ботов.",
                                   parse_mode="HTML",
                                   reply_markup=create_cancel_markup())
            set_user_state(user_id, {'action': 'awaiting_restart_filter_count', 'message_id': msg.message_id})
        elif action == "confirm":
            filter_count = int(call.data.split('_')[4])
            bot.edit_message_text("🚀 Запускаю процесс массового перезапуска в фоновом режиме...", user_id, call.message.message_id)
            
            def mass_restart_thread(admin_id, f_count):
                all_bots = db_execute("SELECT id, bot_type, status FROM bots WHERE status = 'running'", fetchall=True)
                restarted_count = 0
                failed_count = 0
                
                bots_to_restart = []
                if f_count <= 0:
                    bots_to_restart = all_bots
                else:
                    for b in all_bots:
                        user_c = get_child_bot_user_count(b['id'], b['bot_type'])
                        if user_c >= f_count:
                            bots_to_restart.append(b)
                
                total_to_restart = len(bots_to_restart)
                if total_to_restart == 0:
                    bot.send_message(admin_id, "✅ Ботов, подходящих под фильтр, не найдено.")
                    return

                for i, bot_info in enumerate(bots_to_restart):
                    try:
                        stop_bot_process(bot_info['id'])
                        time.sleep(0.5)
                        start_bot_process(bot_info['id'])
                        restarted_count += 1
                    except Exception as e:
                        logging.error(f"Ошибка при перезапуске бота {bot_info['id']}: {e}")
                        failed_count += 1
                    time.sleep(0.5)
                    if (i + 1) % 10 == 0:
                         try:
                             bot.send_message(admin_id, f"🔄 Перезапущено {i+1}/{total_to_restart} ботов...")
                         except: pass

                bot.send_message(admin_id, f"✅ Массовый перезапуск завершен!\n\n"
                                           f"👍 Успешно: {restarted_count}\n"
                                           f"👎 С ошибками: {failed_count}")

            threading.Thread(target=mass_restart_thread, args=(user_id, filter_count), daemon=True).start()
        return

    # New: Mass start by filter
    if call.data.startswith("admin_start_filter_"):
        bot.answer_callback_query(call.id)
        action = call.data.split('_')[3]
        if action == "start":
            msg = bot.send_message(user_id,
                                   "▶️ <b>Массовый запуск ботов</b>\n\n"
                                   "Введите минимальное количество пользователей для запуска.\n"
                                   "Например, <code>100</code> запустит все НЕзапущенные боты, у кого 100+ пользователей.\n\n"
                                   "Отправьте <code>-</code> или <code>0</code>, чтобы запустить <b>все</b> НЕзапущенные боты.",
                                   parse_mode="HTML",
                                   reply_markup=create_cancel_markup())
            set_user_state(user_id, {'action': 'awaiting_start_filter_count', 'message_id': msg.message_id})
        elif action == "confirm":
            filter_count = int(call.data.split('_')[4])
            bot.edit_message_text("🚀 Запускаю процесс массового запуска в фоновом режиме...", user_id, call.message.message_id)

            def mass_start_thread(admin_id, f_count):
                all_bots = db_execute("SELECT id, bot_type, status FROM bots WHERE status != 'running'", fetchall=True)
                started_count = 0
                failed_count = 0

                bots_to_start = []
                if f_count <= 0:
                    bots_to_start = all_bots
                else:
                    for b in all_bots:
                        user_c = get_child_bot_user_count(b['id'], b['bot_type'])
                        if user_c >= f_count:
                            bots_to_start.append(b)

                total_to_start = len(bots_to_start)
                if total_to_start == 0:
                    bot.send_message(admin_id, "✅ Ботов, подходящих под фильтр, не найдено.")
                    return

                for i, bot_info in enumerate(bots_to_start):
                    try:
                        success, start_message = start_bot_process(bot_info['id'])
                        if success:
                            started_count += 1
                        else:
                            failed_count += 1
                    except Exception as e:
                        logging.error(f"Ошибка при запуске бота {bot_info['id']}: {e}")
                        failed_count += 1
                    time.sleep(0.5)
                    if (i + 1) % 10 == 0:
                        try:
                            bot.send_message(admin_id, f"▶️ Запущено {i+1}/{total_to_start} ботов...")
                        except:
                            pass

                bot.send_message(admin_id, f"✅ Массовый запуск завершен!\n\n"
                                       f"👍 Успешно: {started_count}\n"
                                       f"👎 С ошибками: {failed_count}")

            threading.Thread(target=mass_start_thread, args=(user_id, filter_count), daemon=True).start()
        return

    if call.data.startswith("admin_limit_"):
        parts = call.data.split('_')
        sub_action = parts[2]
        bot_id = int(parts[3])
        
        if sub_action == "approve":
            new_limit = int(parts[4])
            update_bot_setting(bot_id, 'flyer_limit', new_limit)
            bot_info = get_bot_by_id(bot_id)
            
            stop_bot_process(bot_id)
            time.sleep(1)
            start_bot_process(bot_id)
            
            bot.answer_callback_query(call.id, f"✅ Лимит для бота #{bot_id} изменен и бот перезапущен.", show_alert=True)
            bot.edit_message_text(call.message.html_text + f"\n\n<b>Статус: ✅ ОДОБРЕНО (лимит {new_limit})</b>", chat_id, call.message.message_id, parse_mode="HTML")
            try:
                bot.send_message(bot_info['owner_id'], f"✅ Администратор одобрил смену лимита Flyer для вашего бота #{bot_id} на <b>{new_limit}</b>. Бот был автоматически перезапущен.", parse_mode="HTML")
            except Exception as e:
                logging.warning(f"Не удалось уведомить владельца {bot_info['owner_id']} о смене лимита: {e}")
            return

        elif sub_action == "decline":
            target_user_id = int(parts[4])
            bot.answer_callback_query(call.id, "Запрос отклонен.", show_alert=True)
            bot.edit_message_text(call.message.html_text + "\n\n<b>Статус: ❌ ОТКЛОНЕНО</b>", chat_id, call.message.message_id, parse_mode="HTML")
            try:
                bot.send_message(target_user_id, f"❌ Ваша заявка на изменение лимита Flyer для бота #{bot_id} была отклонена.")
            except Exception as e:
                 logging.warning(f"Не удалось уведомить владельца {target_user_id} об отклонении лимита: {e}")
            return

    parts = call.data.split('_')
    action = parts[1]
    
    bot.answer_callback_query(call.id)

    if action == "vip":
        sub_action = parts[2]
        if sub_action == "manage":
            if user_id in user_states:
                del user_states[user_id]

            vip_price = get_setting('vip_price') or '120.0'
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton(f"💰 Изменить цену VIP ({vip_price} ₽)", callback_data="admin_vip_set_price"))
            markup.add(types.InlineKeyboardButton("🎁 Выдать VIP", callback_data="admin_vip_grant"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
            bot.edit_message_text("₽ Управление VIP-статусом:", chat_id, call.message.message_id, reply_markup=markup)
        
        elif sub_action == "set" and parts[3] == "price":
            msg = bot.edit_message_text("Введите новую цену для VIP-статуса (например, 120.0):", chat_id, call.message.message_id,
                                        reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_vip_manage")))
            set_user_state(user_id, {'action': 'admin_change_setting', 'setting_key': 'vip_price', 'message_id': msg.message_id, 'call_id': call.id, 'message': call.message})
        
        elif sub_action == "grant":
            cancel_markup = types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_vip_manage")
            )
            msg = bot.edit_message_text(
                "Введите ID бота, которому нужно выдать VIP:", chat_id, call.message.message_id,
                reply_markup=cancel_markup
            )
            set_user_state(user_id, {'action': 'admin_grant_vip', 'message_id': msg.message_id, 'call_id': call.id})
        return
    
    # Удалено: админ-меню 'Креатор'

    if action == "back":
        bot.edit_message_text(get_custom_text('admin_menu_heading'), chat_id, call.message.message_id, reply_markup=create_admin_menu())

    elif action == "get" and parts[2] == "logs" and parts[3] == "start":
        cancel_markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад в админку", callback_data="admin_back"))
        msg = bot.edit_message_text(
            "<b>Введите ID бота, логи которого вы хотите получить:</b>", chat_id, call.message.message_id,
            reply_markup=cancel_markup,
            parse_mode="HTML"
        )
        set_user_state(user_id, {'action': 'awaiting_bot_id_for_logs', 'message_id': msg.message_id})

    elif action == "broadcast" and not (len(parts) >= 3 and parts[2] == "bots"):
        sub_action = parts[2]
        if sub_action == "start":
            markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Отмена", callback_data="admin_back"))
            msg = bot.edit_message_text("<b>Шаг 1/3:</b> Отправьте мне готовый пост (с текстом, фото и т.д.), который нужно разослать.", chat_id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
            set_user_state(user_id, {'action': 'admin_broadcast_get_content', 'message_id': msg.message_id})
        elif sub_action == "cancel":
            if user_id in user_states: del user_states[user_id]
            bot.delete_message(chat_id, call.message.message_id)
            bot.send_message(chat_id, "Рассылка отменена.", reply_markup=create_main_menu(user_id))
        elif sub_action == "confirm":
            message_to_send = bot.send_message(chat_id, "🚀 Рассылка запущена...")
            try: bot.delete_message(chat_id, call.message.message_id)
            except: pass
            preview_message_id = int(parts[3])
            def run_broadcast_thread():
                users_to_send = db_execute("SELECT user_id FROM users", fetchall=True)
                success_count, fail_count, total_users = 0, 0, len(users_to_send)
                start_time = time.time()
                try:
                    copied_message = bot.copy_message(chat_id, chat_id, preview_message_id)
                    reply_markup_to_send = copied_message.reply_markup
                    bot.delete_message(chat_id, copied_message.message_id)
                except Exception: reply_markup_to_send = None
                for i, user in enumerate(users_to_send):
                    try:
                        bot.copy_message(user['user_id'], chat_id, preview_message_id, reply_markup=reply_markup_to_send)
                        success_count += 1
                    except Exception: fail_count += 1
                    time.sleep(0.05) 
                    if (i + 1) % 20 == 0:
                        try: bot.edit_message_text(f"🚀 Рассылка... Отправлено {i+1}/{total_users}", chat_id, message_to_send.message_id)
                        except telebot.apihelper.ApiTelegramException: pass
                end_time = time.time()
                final_text = (f"✅ Рассылка завершена за {end_time - start_time:.2f} сек.\n\n"
                              f"📬 Всего пользователей: {total_users}\n"
                              f"👍 Успешно отправлено: {success_count}\n"
                              f"👎 Ошибок: {fail_count}")
                final_markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад в админку", callback_data="admin_back"))
                bot.edit_message_text(final_text, chat_id, message_to_send.message_id, reply_markup=final_markup)
            threading.Thread(target=run_broadcast_thread, daemon=True).start()

    elif call.data == "admin_broadcast_bots_confirm":
        global bots_broadcast_running
        if bots_broadcast_running:
            bot.answer_callback_query(call.id, "❌ Уже идет рассылка по ботам. Дождитесь завершения.", show_alert=True)
            return
        state = user_states.get(user_id, {})
        target_bot_ids = state.get('target_bot_ids') or []
        if not target_bot_ids:
            bot.answer_callback_query(call.id)
            bot.send_message(chat_id, "❌ Не выбраны боты для рассылки.")
            return
        # lock and run
        bots_broadcast_running = True
        bot.answer_callback_query(call.id)
        progress_msg = bot.send_message(chat_id, "🚀 Запускаю рассылку по выбранным ботам (очередью)...")
        preview_msg_id = state.get('preview_message_id')
        reply_markup_to_send = state.get('reply_markup')

        def run_bots_broadcast(ids, preview_id, reply_markup):
            global bots_broadcast_running
            total_bots = len(ids)
            ok, fail = 0, 0
            start_ts = time.time()
            for idx, bid in enumerate(ids):
                try:
                    # Each child bot should have its own DB; we broadcast to its users table if exists
                    bot_type_row = db_execute("SELECT bot_type FROM bots WHERE id = ?", (bid,), fetchone=True)
                    if not bot_type_row:
                        fail += 1
                        continue
                    btype = bot_type_row['bot_type']
                    db_file = f"dbs/bot_{bid}_data.db" if btype == 'ref' else f"dbs/bot_{bid}_stars_data.db"
                    if not os.path.exists(db_file):
                        fail += 1
                        continue
                    try:
                        child_conn = sqlite3.connect(db_file)
                        child_conn.row_factory = sqlite3.Row
                        users_rows = child_conn.cursor().execute("SELECT user_id FROM users").fetchall()
                        child_conn.close()
                    except Exception:
                        fail += 1
                        continue
                    sent, skipped = 0, 0
                    for ur in users_rows:
                        try:
                            bot.copy_message(ur['user_id'], chat_id, preview_id, reply_markup=reply_markup)
                            sent += 1
                        except Exception:
                            skipped += 1
                        time.sleep(0.03)
                    ok += 1
                except Exception:
                    fail += 1
                    if (idx + 1) % 1 == 0:
                        try:
                            bot.edit_message_text(f"🚀 Рассылка по ботам... {idx+1}/{total_bots}", chat_id, progress_msg.message_id)
                        except Exception:
                            pass
            dur = time.time() - start_ts
            bots_broadcast_running = False
            try:
                bot.edit_message_text(f"✅ Готово. Ботов обработано: {ok}/{total_bots}. Ошибок: {fail}. Время: {dur:.1f} сек.", chat_id, progress_msg.message_id,
                                      reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back")))
            except Exception:
                pass

        threading.Thread(target=run_bots_broadcast, args=(target_bot_ids, preview_msg_id, reply_markup_to_send), daemon=True).start()

    elif action == "lists":
        sub_action = parts[2]
        if sub_action == "menu":
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton("🤖 Боты с Flyer ОП", callback_data="admin_lists_op"))
            markup.add(types.InlineKeyboardButton("👥 Пользователи конструктора", callback_data="admin_lists_creator"))
            current_min = get_setting('bots_list_min_users') or '30'
            markup.add(types.InlineKeyboardButton(f"⚙️ Порог для 'Списков ботов' ({current_min})", callback_data="admin_lists_set_min"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
            bot.edit_message_text("📂 Выберите список для просмотра:", chat_id, call.message.message_id, reply_markup=markup)
        elif sub_action == "op":
            bots_list = db_execute(
                "SELECT id, bot_username, owner_id, bot_type FROM bots "
                "WHERE flyer_op_enabled = 1 OR stars_op_enabled = 1 OR clicker_op_enabled = 1 "
                "OR (anonchat_flyer_api_key IS NOT NULL AND TRIM(anonchat_flyer_api_key) != '')",
                fetchall=True
            )
            text = "<b>🤖 Боты с подключенным Flyer ОП:</b>\n\n" + ('\n'.join([f"- ID: <code>{b['id']}</code> (@{escape(b['bot_username'] or 'N/A')}) | Владелец: <code>{b['owner_id']}</code> | 👥 {get_child_bot_user_count(b['id'], b['bot_type'])}" for b in bots_list]) or "Список пуст.")
            markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад к спискам", callback_data="admin_lists_menu"))
            bot.edit_message_text(text, chat_id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
        # Удален подраздел "creator" (Пользователи конструктора)
        elif sub_action == "set":
            if len(parts) >= 4 and parts[3] in ('min', 'min_users'):
                msg = bot.edit_message_text(
                    "Введите минимальное число пользователей для показа в меню '📋 Списки ботов' (целое число):", chat_id, call.message.message_id,
                    reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_lists_menu"))
                )
                set_user_state(user_id, {'action': 'admin_change_setting', 'setting_key': 'bots_list_min_users', 'message_id': msg.message_id, 'call_id': call.id, 'message': call.message})

    elif action == "bots":
        sub_action = parts[2]
        if sub_action == "all":
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton("📋 Список всех ботов", callback_data="admin_bots_list_0"))
            markup.add(types.InlineKeyboardButton("🔎 Найти бота по ID", callback_data="admin_bots_find"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад в админку", callback_data="admin_back"))
            bot.edit_message_text("<b>🤖 Управление ботами</b>", call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
        
        elif sub_action == "list":
            page = int(parts[3])
            BOTS_PER_PAGE = 20
            offset = page * BOTS_PER_PAGE
            
            total_bots_count = db_execute("SELECT COUNT(*) FROM bots", fetchone=True)[0]
            all_bots = db_execute("SELECT id, bot_username, status, bot_type FROM bots ORDER BY id DESC LIMIT ? OFFSET ?", (BOTS_PER_PAGE, offset), fetchall=True)
            
            text = f"<b>📋 Список всех ботов (Страница {page + 1}):</b>\n\n"
            if not all_bots:
                text += "Ботов пока не создано."
            else:
                status_icons = {'running': '🟢', 'stopped': '🔴', 'unconfigured': '⚠️'}
                for b in all_bots:
                    icon = status_icons.get(b['status'], '❓')
                    user_count = get_child_bot_user_count(b['id'], b['bot_type'])
                    username = escape(b['bot_username'] or 'Без имени')
                    text += f"{icon} ID: <code>{b['id']}</code> | @{username} | 👥 {user_count}\n"

            markup = types.InlineKeyboardMarkup()
            nav_buttons = []
            if page > 0:
                nav_buttons.append(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"admin_bots_list_{page - 1}"))
            if (page + 1) * BOTS_PER_PAGE < total_bots_count:
                nav_buttons.append(types.InlineKeyboardButton("Вперед ➡️", callback_data=f"admin_bots_list_{page + 1}"))
            
            if nav_buttons:
                markup.row(*nav_buttons)
            markup.add(types.InlineKeyboardButton("⬅️ К управлению ботами", callback_data="admin_bots_all"))
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
        
        elif sub_action == "find":
            msg = bot.edit_message_text("<b>🔎 Введите ID бота для поиска:</b>", call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Отмена", callback_data="admin_bots_all")))
            set_user_state(user_id, {'action': 'admin_view_bot_by_id', 'message_id': msg.message_id})
            
    elif action == "bot":
        sub_action = parts[2]
        bot_id = int(parts[3])
        if sub_action == "info":
            show_admin_bot_info(call.from_user.id, call.message.message_id, bot_id)
        elif sub_action == "changekey":
            msg = bot.edit_message_text(f"<b>🔧 Введите новый Flyer API ключ для бота ID <code>{bot_id}</code>:</b>", call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Отмена", callback_data=f"admin_bot_info_{bot_id}")))
            set_user_state(user_id, {'action': 'admin_set_new_op_key_admin', 'bot_id': bot_id, 'message_id': msg.message_id, 'call_id': call.id})
        elif sub_action == "removekey":
            bot_info = get_bot_by_id(bot_id)
            if bot_info['bot_type'] == 'ref':
                update_bot_setting(bot_id, 'flyer_api_key', None); update_bot_setting(bot_id, 'flyer_op_enabled', False)
            elif bot_info['bot_type'] == 'stars':
                update_bot_setting(bot_id, 'stars_flyer_api_key', None); update_bot_setting(bot_id, 'stars_op_enabled', False)
            elif bot_info['bot_type'] == 'clicker':
                update_bot_setting(bot_id, 'clicker_flyer_api_key', None); update_bot_setting(bot_id, 'clicker_op_enabled', False)
            elif bot_info['bot_type'] == 'anonchat':
                update_bot_setting(bot_id, 'anonchat_flyer_api_key', None)
            bot.answer_callback_query(call.id, "✅ Ключ Flyer для бота удален! Перезапустите бота, чтобы применить.", show_alert=True)
            show_admin_bot_info(call.from_user.id, call.message.message_id, bot_id)
        elif sub_action == "restart":
            bot_id = int(parts[3])
            stop_bot_process(bot_id)
            time.sleep(1)
            success, message = start_bot_process(bot_id)
            if success:
                bot.answer_callback_query(call.id, f"✅ Бот ID {bot_id} перезапущен.", show_alert=True)
            else:
                bot.answer_callback_query(call.id, f"❌ Ошибка перезапуска бота ID {bot_id}: {message}", show_alert=True)
            show_admin_bot_info(call.from_user.id, call.message.message_id, bot_id)
        elif sub_action == "delete":
            if parts[4] == "confirm":
                markup = types.InlineKeyboardMarkup(row_width=1)
                markup.add(types.InlineKeyboardButton("❗️ ДА, УДАЛИТЬ БОТА", callback_data=f"admin_bot_delete_{bot_id}_final"), types.InlineKeyboardButton("⬅️ Отмена", callback_data=f"admin_bot_info_{bot_id}"))
                bot.edit_message_text(f"<b>Вы уверены, что хотите полностью удалить бота ID <code>{bot_id}</code>?</b>\n\nЭто действие необратимо.", call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
            elif parts[4] == "final":
                delete_bot_from_db(bot_id)
                bot.answer_callback_query(call.id, f"✅ Бот ID {bot_id} полностью удален!", show_alert=True)
                call.data = "admin_bots_all"; handle_admin_callbacks(call)

    elif action == "op":
        sub_action = parts[2]
        if sub_action == "manage":
            op_reward = get_setting('op_reward') or "1.0"
            stars_reward = get_setting('stars_sub_reward') or "1.0"
            creator_price = get_setting('creator_price') or "500.0"
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton(f"💸 Награда за Flyer ОП (реф. бот): {op_reward} ₽", callback_data="admin_op_set_reward"))
            markup.add(types.InlineKeyboardButton(f"⭐ Награда за подписку (звёзды): {stars_reward} ₽", callback_data="admin_op_set_stars_reward"))
            markup.add(types.InlineKeyboardButton(f"🎨 Цена бота Креатор: {creator_price} USDT", callback_data="admin_op_set_creator_price"))
            markup.add(types.InlineKeyboardButton("✏️ Изменить приветствие креатора", callback_data="admin_edit_creator_welcome"))
            markup.add(types.InlineKeyboardButton("📊 Лимит бесплатных ботов", callback_data="admin_set_max_bots"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
            bot.edit_message_text("⚙️ Управление настройками конструктора и доходом Flyer:", chat_id, call.message.message_id, reply_markup=markup)
        elif sub_action == "set":
            setting_type = parts[3]
            setting_key, prompt_text = None, None
            if setting_type == "reward":
                current_reward = get_setting('op_reward') or "1.0"; setting_key = 'op_reward'
                prompt_text = f"Текущая награда за Flyer ОП: {current_reward} ₽.\n\nВведите новое значение:"
            elif setting_type == "stars" and parts[4] == "reward":
                current_reward = get_setting('stars_sub_reward') or "1.0"; setting_key = 'stars_sub_reward'
                prompt_text = f"Текущая награда за подписку: {current_reward} ₽.\n\nВведите новое значение:"
            elif setting_type == "creator" and parts[4] == "price":
                current_price = get_setting('creator_price') or "500.0"; setting_key = 'creator_price'
                prompt_text = f"Текущая цена бота Креатор: {current_price} USDT.\n\nВведите новое значение:"
            if setting_key and prompt_text:
                markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_op_manage"))
                msg = bot.edit_message_text(prompt_text, chat_id, call.message.message_id, reply_markup=markup)
                set_user_state(user_id, {'action': 'admin_change_setting', 'setting_key': setting_key, 'message_id': msg.message_id, 'call_id': call.id, 'message': call.message})
        elif call.data == "admin_edit_creator_welcome":
            prompt_custom_text_edit(user_id, 'creator_welcome', getattr(call.message, 'message_id', None), back_callback="admin_op_manage")
            bot.answer_callback_query(call.id)
            return
        elif call.data == "admin_set_max_bots":
            current_limit = get_setting('MAX_BOTS_PER_USER') or str(MAX_BOTS_PER_USER)
            msg = bot.edit_message_text(f"📊 Текущий лимит бесплатных ботов: {current_limit}.\n\nВведите новое целое число:", chat_id, call.message.message_id,
                                        reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_op_manage")))
            set_user_state(user_id, {'action': 'admin_change_setting', 'setting_key': 'MAX_BOTS_PER_USER', 'message_id': msg.message_id, 'call_id': call.id, 'message': call.message})
            return
        return

    elif action == "wd":
        wd_action = parts[2]
        if wd_action == "list":
            pending_wds = db_execute("SELECT * FROM creator_withdrawals WHERE status = 'pending' ORDER BY id", fetchall=True)
            text = "<b>📬 Заявки на вывод:</b>\n\n" + ("Новых заявок нет." if not pending_wds else "")
            markup = types.InlineKeyboardMarkup(row_width=1)
            for wd in pending_wds:
                markup.add(types.InlineKeyboardButton(f"Заявка #{wd['id']} - {wd['amount']:.2f} ₽ от {wd['user_id']}", callback_data=f"admin_wd_view_{wd['id']}"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад в админку", callback_data="admin_back"))
            bot.edit_message_text(text, chat_id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
            return
        wd_id = int(parts[3])
        if wd_action == "view":
            wd_info = db_execute("SELECT * FROM creator_withdrawals WHERE id = ?", (wd_id,), fetchone=True)
            if not wd_info: bot.edit_message_text("Заявка не найдена.", chat_id, call.message.message_id); return
            user_info = get_user(wd_info['user_id'])
            username = escape(user_info['username'] or "N/A")
            text = (f"<b>📬 Заявка на вывод №{wd_id}</b>\n\n"
                    f"👤 Пользователь: <code>{wd_info['user_id']}</code> (@{username})\n"
                    f"💰 Сумма: <code>{wd_info['amount']:.2f} ₽</code>\n"
                    f"💳 Реквизиты: <code>{escape(wd_info['details'])}</code>\n"
                    f"Статус: <code>{wd_info['status']}</code>")
            markup = types.InlineKeyboardMarkup()
            if wd_info['status'] == 'pending':
                markup.row(types.InlineKeyboardButton("✅ Одобрить", callback_data=f"admin_wd_approve_{wd_id}_{wd_info['user_id']}"), types.InlineKeyboardButton("❌ Отклонить", callback_data=f"admin_wd_decline_{wd_id}_{wd_info['user_id']}"))
                markup.row(types.InlineKeyboardButton("💬 Ответить пользователю", callback_data=f"admin_wd_reply_{wd_id}_{wd_info['user_id']}"))
            markup.add(types.InlineKeyboardButton("⬅️ К списку заявок", callback_data="admin_wd_list"))
            bot.edit_message_text(text, chat_id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
            return
        target_user_id = int(parts[4])
        wd_info = db_execute("SELECT * FROM creator_withdrawals WHERE id = ?", (wd_id,), fetchone=True)
        if not wd_info or wd_info['status'] != 'pending':
            bot.answer_callback_query(call.id, "Эта заявка уже обработана.", show_alert=True)
            call.data = f"admin_wd_view_{wd_id}"; handle_admin_callbacks(call)
            return
        if wd_action == 'approve':
            db_execute("UPDATE creator_withdrawals SET status = 'approved' WHERE id = ?", (wd_id,), commit=True)
            bot.send_message(target_user_id, f"✅ Ваша заявка на вывод {wd_info['amount']:.2f} ₽ одобрена и будет выплачена в ближайшее время.")
        elif wd_action == 'decline':
            db_execute("UPDATE creator_withdrawals SET status = 'declined' WHERE id = ?", (wd_id,), commit=True)
            db_execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (wd_info['amount'], target_user_id), commit=True)
            bot.send_message(target_user_id, f"❌ Ваша заявка на вывод {wd_info['amount']:.2f} ₽ отклонена. Средства возвращены на баланс.")
        elif wd_action == 'reply':
            msg = bot.send_message(chat_id, f"Введите текст сообщения для пользователя {target_user_id}:", reply_markup=create_cancel_markup())
            set_user_state(user_id, {'action': 'admin_reply_text', 'target_user_id': target_user_id, 'bot_id': None, 'message_id': msg.message_id, 'call_id': call.id})
            return
        call.data = f"admin_wd_view_{wd_id}"; handle_admin_callbacks(call)

def show_admin_bot_info(user_id, message_id, bot_id):
    bot_info = get_bot_by_id(bot_id)
    if not bot_info:
        try: bot.edit_message_text(f"<b>❌ Бот с ID <code>{bot_id}</code> не найден.</b>", user_id, message_id, parse_mode="HTML")
        except telebot.apihelper.ApiTelegramException: bot.send_message(user_id, f"<b>❌ Бот с ID <code>{bot_id}</code> не найден.</b>", parse_mode="HTML")
        return
    owner_info = get_user(bot_info['owner_id'])
    owner_username = escape(owner_info['username'] or "N/A") if owner_info else "N/A"
    bot_username_value = bot_info['bot_username'] or "N/A"
    bot_username = escape(bot_username_value)
    user_count = get_child_bot_user_count(bot_id, bot_info['bot_type'])
    
    flyer_key = None
    flyer_enabled = False
    if bot_info['bot_type'] == 'ref':
        flyer_key = bot_info['flyer_api_key']
        flyer_enabled = bot_info['flyer_op_enabled']
    elif bot_info['bot_type'] == 'stars':
        flyer_key = bot_info['stars_flyer_api_key']
        flyer_enabled = bot_info['stars_op_enabled']
    elif bot_info['bot_type'] == 'clicker':
        flyer_key = bot_info['clicker_flyer_api_key']
        flyer_enabled = bot_info['clicker_op_enabled']
    elif bot_info['bot_type'] == 'anonchat':
        flyer_key = bot_info['anonchat_flyer_api_key']
        flyer_enabled = bool(flyer_key)

    type_titles = {
        'ref': 'Реферальный',
        'stars': 'Заработок Звёзд',
        'clicker': 'Кликер',
        'anonchat': 'Анонимный чат',
        'cashlait': 'CashLait',
        'dicelite': 'DiceLite',
    }
    bot_type_title = type_titles.get(bot_info['bot_type'], bot_info['bot_type'])

    text = (f"<b>ℹ️ Информация о боте ID <code>{bot_id}</code></b>\n\n"
            f"<b>Тип:</b> {bot_type_title}\n"
            f"<b>Username:</b> @{bot_username}\n"
            f"<b>Токен:</b> <code>{escape(bot_info['bot_token'] or 'Не установлен')}</code>\n"
            f"<b>Владелец:</b> <code>{bot_info['owner_id']}</code> (@{owner_username})\n"
            f"<b>Пользователей:</b> {user_count}\n"
            f"<b>Статус:</b> <code>{bot_info['status']}</code>\n"
            f"<b>Flyer ОП:</b> {'🟢 Включен' if flyer_enabled else '🔴 Выключен'}\n"
            f"<b>Flyer Key:</b> <code>{escape(flyer_key) if flyer_key else 'Не установлен'}</code>")
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("🔄 Перезапустить Бота", callback_data=f"admin_bot_restart_{bot_id}"))
    markup.add(types.InlineKeyboardButton("🔧 Изменить/Задать Flyer Key", callback_data=f"admin_bot_changekey_{bot_id}"))
    if flyer_key:
        markup.add(types.InlineKeyboardButton("🗑️ Удалить Flyer Key", callback_data=f"admin_bot_removekey_{bot_id}"))
    markup.add(types.InlineKeyboardButton("❗️ Удалить Бота", callback_data=f"admin_bot_delete_{bot_id}_confirm"))
    markup.add(types.InlineKeyboardButton("⬅️ Назад к управлению", callback_data="admin_bots_all"))
    try: bot.edit_message_text(text, user_id, message_id, parse_mode="HTML", reply_markup=markup)
    except telebot.apihelper.ApiTelegramException: pass

def get_bot_flyer_key_for_check(bot_id):
    bot_info = db_execute(
        "SELECT bot_type, flyer_api_key, stars_flyer_api_key, clicker_flyer_api_key, anonchat_flyer_api_key FROM bots WHERE id = ?",
        (bot_id,),
        fetchone=True
    )
    if not bot_info:
        return None
    key_map = {
        'ref': 'flyer_api_key',
        'stars': 'stars_flyer_api_key',
        'clicker': 'clicker_flyer_api_key',
        'anonchat': 'anonchat_flyer_api_key',
    }
    column = key_map.get(bot_info['bot_type'])
    if column:
        return bot_info[column]
    return None

def run_hold_checker():
    if not FLYER_IMPORTED_FOR_CHECKER:
        logging.error("[HOLD_CHECKER] Библиотека flyerapi не установлена. Воркер проверки холда не может быть запущен.")
        return
        
    logging.info("[HOLD_CHECKER] Воркер проверки холда запущен.")
    
    async def check_task_async(flyer_client, task):
        try:
            status = await flyer_client.check_task(signature=task['task_signature'])
            logging.info(f"[HOLD_CHECKER] Проверка задачи {task['id']} ({task['task_signature']}). Статус от Flyer: {status}")
            
            if status == 'complete':
                with db_lock:
                    conn.execute("UPDATE users SET balance = balance + ?, frozen_balance = frozen_balance - ? WHERE user_id = ?",
                                 (task['amount'], task['amount'], task['owner_id']))
                    conn.execute("DELETE FROM pending_flyer_rewards WHERE id = ?", (task['id'],))
                    conn.commit()
                logging.info(f"[HOLD_CHECKER] Успех! ID:{task['id']}. Статус 'complete'. {task['amount']} руб. переведено владельцу {task['owner_id']}.")
            elif status in ('abort', 'incomplete'):
                with db_lock:
                    conn.execute("UPDATE users SET frozen_balance = frozen_balance - ? WHERE user_id = ?", 
                                 (task['amount'], task['owner_id']))
                    conn.execute("DELETE FROM pending_flyer_rewards WHERE id = ?", (task['id'],))
                    conn.commit()
                logging.warning(f"[HOLD_CHECKER] Отмена! ID:{task['id']}. Статус '{status}'. Холд {task['amount']} руб. для {task['owner_id']} аннулирован.")
        
        except FlyerAPIError as e:
            logging.error(f"[HOLD_CHECKER] Ошибка API Flyer при проверке задачи {task['id']}: {e}")
        except Exception as e:
            logging.error(f"[HOLD_CHECKER] Необработанная ошибка при асинхронной проверке задачи {task['id']}: {e}")

    async def main_check_loop():
        while True:
            try:
                now_iso = datetime.utcnow().isoformat()
                pending_tasks = db_execute("SELECT * FROM pending_flyer_rewards WHERE check_after_timestamp <= ?", (now_iso,), fetchall=True)

                if pending_tasks:
                    logging.info(f"[HOLD_CHECKER] Найдено {len(pending_tasks)} задач для проверки.")
                    tasks_by_bot = {}
                    for task in pending_tasks:
                        tasks_by_bot.setdefault(task['bot_id'], []).append(task)

                    for bot_id, tasks in tasks_by_bot.items():
                        api_key = get_bot_flyer_key_for_check(bot_id)
                        if not api_key:
                            logging.warning(f"[HOLD_CHECKER] Не найден Flyer API ключ для бота {bot_id}. Пропускаем {len(tasks)} задач.")
                            continue
                        
                        try:
                            flyer_client = Flyer(key=api_key)
                            async_tasks = [check_task_async(flyer_client, task) for task in tasks]
                            await asyncio.gather(*async_tasks)
                            await flyer_client.close()
                        except Exception as e:
                            logging.error(f"[HOLD_CHECKER] Критическая ошибка при обработке пачки задач для бота {bot_id}: {e}")
                
            except Exception as e:
                logging.critical(f"[HOLD_CHECKER] Критическая ошибка во внешнем цикле воркера: {e}", exc_info=True)
            
            await asyncio.sleep(300)

    asyncio.run_coroutine_threadsafe(main_check_loop(), async_loop)
 # -------------------- НАЧАЛО БЛОКА МОНИТОРИНГА ПАМЯТИ --------------------


# Глобальный словарь для хранения предыдущих значений памяти {pid: memory_mb}
# Он нужен, чтобы сравнивать "было" и "стало".
previous_memory_usage = {}
memory_lock = threading.Lock() # Для безопасной работы с переменной из потока

def memory_monitor_worker():
    """
    Эта функция работает в фоне, в бесконечном цикле.
    Каждые 5 минут она собирает данные о памяти и записывает их в файл.
    """
    global previous_memory_usage
    
    while True:
        try:
            # --- Сбор текущих данных ---
            main_process = psutil.Process(os.getpid())
            current_processes = [main_process] + main_process.children(recursive=True)
            
            # Словарь для хранения данных этого замера: {pid: (mem_mb, delta_mb, bot_id_str)}
            current_usage_data = {}
            
            # Блокируем доступ к общей переменной, чтобы безопасно ее прочитать
            with memory_lock:
                for proc in current_processes:
                    try:
                        mem_mb = proc.memory_info().rss / 1024 / 1024
                        # Получаем старое значение, если его нет - считаем 0
                        prev_mem_mb = previous_memory_usage.get(proc.pid, 0)
                        delta_mb = mem_mb - prev_mem_mb
                        
                        # Пытаемся определить ID дочернего бота
                        bot_id_str = '???'
                        if proc.pid != main_process.pid:
                            cmd_line = " ".join(proc.cmdline())
                            if '--bot-id' in cmd_line:
                                bot_id_str = cmd_line.split('--bot-id')[-1].strip()

                        current_usage_data[proc.pid] = (mem_mb, delta_mb, bot_id_str)
                    except psutil.NoSuchProcess:
                        # Процесс мог умереть, пока мы его проверяли
                        continue
            
            # --- Формирование отчета ---
            report_lines = []
            report_lines.append(f"🧠 Отчет по памяти от {time.strftime('%Y-%m-%d %H:%M:%S')}")
            report_lines.append("=" * 40)
            
            total_current_mem = 0
            total_delta = 0
            
            # Отчет по главному процессу
            if main_process.pid in current_usage_data:
                mem, delta, _ = current_usage_data[main_process.pid]
                total_current_mem += mem
                total_delta += delta
                sign = '+' if delta >= 0 else ''
                report_lines.append(f"🔵 Главный бот (PID {main_process.pid}):")
                report_lines.append(f"   - Текущее: {mem:.2f} МБ (Изменение: {sign}{delta:.2f} МБ)")

            # Отчет по дочерним ботам
            children_count = len(current_usage_data) - 1
            if children_count > 0:
                report_lines.append(f"\n🤖 Дочерние боты ({children_count} шт.):")
                for pid, (mem, delta, bot_id) in current_usage_data.items():
                    if pid == main_process.pid: continue
                    total_current_mem += mem
                    total_delta += delta
                    sign = '+' if delta >= 0 else ''
                    report_lines.append(f"   - ID: {bot_id:<5} (PID {pid}): {mem:.2f} МБ ({sign}{delta:.2f} МБ)")

            # --- Итоговая сводка ---
            total_sign = '+' if total_delta >= 0 else ''
            report_lines.append("=" * 40)
            report_lines.append(f"📊 ИТОГО:")
            report_lines.append(f"   - Текущее потребление: {total_current_mem:.2f} МБ")
            report_lines.append(f"   - Суммарное изменение: {total_sign}{total_delta:.2f} МБ за 5 мин.")
            
            # --- Запись в файл ---
            with open('memory_usage_report.txt', 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            
            # --- Обновление "предыдущего" состояния для следующего замера ---
            with memory_lock:
                previous_memory_usage = {pid: data[0] for pid, data in current_usage_data.items()}

        except Exception as e:
            # Если что-то пошло не так, записываем ошибку в тот же файл
            with open('memory_usage_report.txt', 'w', encoding='utf-8') as f:
                f.write(f"Произошла ошибка в мониторинге памяти: {e}")
        
        # Ждем 5 минут (300 секунд) до следующей проверки
        time.sleep(300)

def start_memory_monitor():
    """Запускает фоновый поток для мониторинга памяти."""
    monitor_thread = threading.Thread(target=memory_monitor_worker, daemon=True)
    monitor_thread.start()
    # Эта строка появится в твоих логах при запуске, чтобы ты знал, что все работает
    print("✅ Фоновый мониторинг памяти запущен. Файл 'memory_usage_report.txt' будет обновляться каждые 5 минут.")

# -------------------- КОНЕЦ БЛОКА МОНИТОРИНГА ПАМЯТИ --------------------

if __name__ == '__main__':
    # <-- Этот код имеет отступ в 4 пробела
    init_db()
    
    # Load MAX_BOTS_PER_USER from settings
    try:
        saved_max_bots = get_setting('MAX_BOTS_PER_USER')
        if saved_max_bots:
            MAX_BOTS_PER_USER = int(float(saved_max_bots))
            logging.info(f"Загружен лимит ботов из настроек: {MAX_BOTS_PER_USER}")
    except Exception as e:
        logging.warning(f"Не удалось загрузить лимит ботов из настроек: {e}")
    
    # Load persisted Crypto Pay token if present
    try:
            saved_token = None
            try:
                saved_token = get_setting('crypto_pay_token')
            except Exception:
                saved_token = None
            if saved_token:
                CRYPTO_PAY_TOKEN = saved_token
                try:
                    crypto_pay = AioCryptoPay(token=CRYPTO_PAY_TOKEN, network=Networks.MAIN_NET)
                except Exception as e:
                    logging.error(f"Ошибка инициализации Crypto Pay при старте: {e}")
                    crypto_pay = None
    except Exception as e:
        logging.warning(f"Не удалось загрузить сохраненный токен Crypto Pay: {e}")
    logging.info("Запускаем фоновый event loop для asyncio...")
    loop_thread.start()
    start_memory_monitor()
    cleanup_thread = threading.Thread(target=cleanup_stale_states, daemon=True)
    cleanup_thread.start()
    logging.info("Запущен воркер для очистки зависших состояний.")

    threading.Thread(target=run_hold_checker, daemon=True).start()

    def run_payment_checker():
        # <-- Этот код имеет отступ в 8 пробелов
        async def check_payments_periodically():
            local_crypto = get_crypto_client()
            if not local_crypto:
                logging.warning("Проверка платежей Crypto Pay не запущена: API не инициализирован.")
                return

            while True:
                try:
                    pending_invoices = db_execute("SELECT invoice_id, bot_id FROM crypto_payments WHERE status = 'pending'", fetchall=True)
                    if pending_invoices:
                        invoice_ids = [str(inv['invoice_id']) for inv in pending_invoices]
                        checked_invoices = await local_crypto.get_invoices(invoice_ids=",".join(invoice_ids))
                        
                        for invoice in checked_invoices:
                            if invoice.status == 'paid':
                                payment_info = db_execute("SELECT bot_id, user_id FROM crypto_payments WHERE invoice_id = ?", (invoice.invoice_id,), fetchone=True)
                                if payment_info:
                                    bot_id_to_update = payment_info['bot_id']
                                    # Проверяем, это VIP или Креатор по payload
                                    if invoice.payload.startswith('vip_'):
                                        owner_id_vip = get_bot_by_id(bot_id_to_update)['owner_id'] if bot_id_to_update else payment_info['user_id']
                                        update_bot_setting(bot_id_to_update, 'vip_status', True)
                                        db_execute("UPDATE crypto_payments SET status = 'paid' WHERE invoice_id = ?", (invoice.invoice_id,), commit=True)
                                        logging.info(f"VIP статус для бота {bot_id_to_update} активирован после оплаты счета #{invoice.invoice_id}")
                                        try:
                                            bot.send_message(owner_id_vip, f"✅ VIP-статус для вашего бота #{bot_id_to_update} успешно активирован!")
                                        except Exception as e:
                                            logging.error(f"Не удалось уведомить владельца {owner_id_vip} о VIP: {e}")
                                    elif invoice.payload.startswith('cashlait_new_'):
                                        # Оплата CashLait без bot_id: создаем бота для оплатившего пользователя
                                        owner_id_new = payment_info['user_id']
                                        cashlait_bot_id = create_bot_in_db(owner_id_new, 'cashlait')
                                        db_execute("UPDATE crypto_payments SET status = 'paid' WHERE invoice_id = ?", (invoice.invoice_id,), commit=True)
                                        logging.info(f"Бот CashLait {cashlait_bot_id} создан после оплаты счета #{invoice.invoice_id} (новая покупка)")
                                        try:
                                            bot.send_message(owner_id_new, f"✅ Оплата прошла успешно! CashLait бот #{cashlait_bot_id} создан! Теперь он в списке ваших ботов.")
                                        except Exception as e:
                                            logging.error(f"Не удалось уведомить покупателя {owner_id_new} о создании CashLait: {e}")
                                        # Уведомляем администратора о покупке
                                        try:
                                            buyer = get_user(owner_id_new)
                                            bot.send_message(ADMIN_ID, f"🛒 Покупка CashLait (фон): пользователь <code>{owner_id_new}</code> (@{escape(buyer['username'] or 'N/A')}) оплатил счет #{invoice.invoice_id}. Создан бот #{cashlait_bot_id}.", parse_mode="HTML")
                                        except Exception as e:
                                            logging.warning(f"Не удалось уведомить админа о фоновой покупке CashLait: {e}")
                                    elif invoice.payload.startswith('creator_new_'):
                                        # Оплата Креатора без bot_id: создаем бота для оплатившего пользователя
                                        owner_id_new = payment_info['user_id']
                                        creator_bot_id = create_bot_in_db(owner_id_new, 'creator')
                                        db_execute("UPDATE crypto_payments SET status = 'paid' WHERE invoice_id = ?", (invoice.invoice_id,), commit=True)
                                        logging.info(f"Бот Креатор {creator_bot_id} создан после оплаты счета #{invoice.invoice_id} (новая покупка)")
                                        try:
                                            bot.send_message(owner_id_new, f"✅ Оплата прошла успешно! Бот Креатор #{creator_bot_id} создан!\n\nОжидайте выдачи бота! Вам напишет админ!")
                                        except Exception as e:
                                            logging.error(f"Не удалось уведомить покупателя {owner_id_new} о создании Креатора: {e}")
                                        # Уведомляем администратора о покупке
                                        try:
                                            buyer = get_user(owner_id_new)
                                            bot.send_message(ADMIN_ID, f"🛒 Покупка Креатора (фон): пользователь <code>{owner_id_new}</code> (@{escape(buyer['username'] or 'N/A')}) оплатил счет #{invoice.invoice_id}. Создан бот #{creator_bot_id}.", parse_mode="HTML")
                                        except Exception as e:
                                            logging.warning(f"Не удалось уведомить админа о фоновой покупке Креатора: {e}")
                                    elif invoice.payload.startswith('creator_'):
                                        # Оплата Креатора, привязанная к существующему боту (из меню бота)
                                        owner_id_existing = get_bot_by_id(bot_id_to_update)['owner_id']
                                        creator_bot_id = create_bot_in_db(owner_id_existing, 'creator')
                                        db_execute("UPDATE crypto_payments SET status = 'paid' WHERE invoice_id = ?", (invoice.invoice_id,), commit=True)
                                        logging.info(f"Бот Креатор {creator_bot_id} создан после оплаты счета #{invoice.invoice_id}")
                                        try:
                                            bot.send_message(owner_id_existing, f"✅ Оплата прошла успешно! Бот Креатор #{creator_bot_id} создан!\n\nОжидайте выдачи бота! Вам напишет админ!")
                                        except Exception as e:
                                            logging.error(f"Не удалось уведомить владельца {owner_id_existing} о создании Креатора: {e}")
                                        # Уведомляем администратора
                                        try:
                                            buyer = get_user(owner_id_existing)
                                            bot.send_message(ADMIN_ID, f"🛒 Покупка Креатора (фон): пользователь <code>{owner_id_existing}</code> (@{escape(buyer['username'] or 'N/A')}) оплатил счет #{invoice.invoice_id}. Создан бот #{creator_bot_id}.", parse_mode="HTML")
                                        except Exception as e:
                                            logging.warning(f"Не удалось уведомить админа о фоновой покупке Креатора (existing): {e}")
                except Exception as e:
                    logging.error(f"Ошибка в фоновой проверке платежей: {e}")
                
                await asyncio.sleep(120)
        
        asyncio.run_coroutine_threadsafe(check_payments_periodically(), async_loop)

    payment_thread = threading.Thread(target=run_payment_checker, daemon=True)
    payment_thread.start()
    logging.info("Фоновая проверка Crypto Pay платежей запущена.")

    def cleanup_zombie_processes():
        """Проверяет и сбрасывает зомби-процессы каждые 5 минут"""
        # <-- Этот код имеет отступ в 8 пробелов
        logging.info("🧹 Воркер очистки зомби-процессов запущен.")
        while True:
            # <-- Этот код имеет отступ в 12 пробелов
            try:
                running_bots = db_execute(
                    "SELECT id, pid FROM bots WHERE status = 'running' AND pid IS NOT NULL",
                    fetchall=True
                )
                cleaned_count = 0
                for bot_data in running_bots:
                    if not psutil.pid_exists(bot_data['pid']):
                        logging.warning(f"Найден 'зомби' процесс для бота ID: {bot_data['id']} (PID: {bot_data['pid']}). Сбрасываю статус.")
                        update_bot_process_info(bot_data['id'], 'stopped', None, None)
                        cleaned_count += 1
                if cleaned_count > 0:
                    logging.info(f"Очистка завершена. Исправлено {cleaned_count} 'зомби' записей.")
            except Exception as e:
                logging.error(f"Ошибка в воркере очистки зомби-процессов: {e}")
            
            time.sleep(300) # Проверка каждые 5 минут

    zombie_cleaner_thread = threading.Thread(target=cleanup_zombie_processes, daemon=True)
    zombie_cleaner_thread.start()


    @bot.message_handler(commands=['start'])
    def handle_start(message):
        get_user(message.from_user.id, message.from_user.username)
        welcome = get_custom_text('creator_welcome')
        wm_enabled_raw = get_setting('creator_watermark_enabled')
        try:
            wm_enabled = str(wm_enabled_raw).strip() in ('1', 'true', 'True')
        except Exception:
            wm_enabled = True
        if wm_enabled:
            welcome += "\n\n<i>Креатор создан с помощью</i> @MinxoCreate_bot"
        bot.send_message(message.chat.id, welcome, reply_markup=create_main_menu(message.from_user.id), parse_mode="HTML")

    @bot.message_handler(func=lambda message: message.from_user.id in user_states)
    def handle_state_messages(message):
        process_state_input(message)
        
    @bot.message_handler(func=lambda message: True, content_types=['text', 'photo', 'video', 'document', 'audio', 'voice', 'sticker', 'animation'])
    def handle_text_buttons(message):
        user_id = message.from_user.id
        if user_id in user_states and message.text != '❌ Отмена':
            del user_states[user_id]
        
        text_value = (getattr(message, 'text', '') or '').strip()
        main_buttons = get_main_menu_button_texts()

        # Secret trigger to show watermark toggle
        if text_value == '567293' and is_admin(user_id):
            wm_enabled_raw = get_setting('creator_watermark_enabled')
            wm_enabled = str(wm_enabled_raw).strip() in ('1', 'true', 'True')
            bl_enabled_raw = get_setting('bots_list_feature_enabled')
            bl_enabled = str(bl_enabled_raw).strip() in ('1', 'true', 'True') if bl_enabled_raw is not None else True
            wm_toggle_text = "🔕 Отключить подпись" if wm_enabled else "🔔 Включить подпись"
            wm_status_text = "Сейчас: ▶️ включена" if wm_enabled else "Сейчас: ⏸️ выключена"
            bl_toggle_text = "🗂️ Отключить 'Списки ботов'" if bl_enabled else "🗂️ Включить 'Списки ботов'"
            bl_status_text = "Списки: ▶️ включены" if bl_enabled else "Списки: ⏸️ выключены"
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton(wm_toggle_text, callback_data="wm_toggle"))
            markup.add(types.InlineKeyboardButton(bl_toggle_text, callback_data="bl_toggle"))
            bot.send_message(
                user_id,
                f"Настройки:\n• Подпись: {wm_status_text}\n• {bl_status_text}",
                reply_markup=markup,
            )
            return

        # Secret code to unlock 'Кликер' bot type for this user
        if text_value == CLICKER_UNLOCK_CODE:
            already_global = is_clicker_unlocked_globally()
            try:
                db_execute("UPDATE users SET clicker_unlocked = 1 WHERE user_id = ?", (user_id,), commit=True)
            except Exception as e:
                logging.error(f"Не удалось установить флаг clicker_unlocked для пользователя {user_id}: {e}")
            if not already_global:
                try:
                    unlock_clicker_globally()
                    logging.info(f"Пользователь {user_id} открыл доступ к типу 'Кликер' для всех пользователей")
                except Exception as e:
                    logging.error(f"Не удалось открыть доступ к кликеру глобально: {e}")
            confirmation_text = "✅ Тип бота 'Кликер' уже был доступен всем пользователям!" if already_global else "✅ Новый тип бота 'Кликер' теперь доступен всем пользователям!"
            bot.send_message(user_id, confirmation_text, parse_mode="HTML")
            bot.send_message(user_id, get_custom_text('create_bot_prompt'), parse_mode="HTML", reply_markup=create_bot_type_menu(user_id))
            return
        
        # Secret code to unlock 'Анонимный чат' bot type for this user
        if text_value == ANONCHAT_UNLOCK_CODE:
            already_global = is_anonchat_unlocked_globally()
            try:
                db_execute("UPDATE users SET anonchat_unlocked = 1 WHERE user_id = ?", (user_id,), commit=True)
            except Exception as e:
                logging.error(f"Не удалось установить флаг anonchat_unlocked для пользователя {user_id}: {e}")
            if not already_global:
                try:
                    unlock_anonchat_globally()
                    logging.info(f"Пользователь {user_id} открыл доступ к типу 'Анонимный чат' для всех пользователей")
                except Exception as e:
                    logging.error(f"Не удалось открыть доступ к анонимному чату глобально: {e}")
            confirmation_text = "✅ Тип бота 'Анонимный чат' уже был доступен всем пользователям!" if already_global else "✅ Новый тип бота 'Анонимный чат' теперь доступен всем пользователям!"
            bot.send_message(user_id, confirmation_text, parse_mode="HTML")
            bot.send_message(user_id, get_custom_text('create_bot_prompt'), parse_mode="HTML", reply_markup=create_bot_type_menu(user_id))
            return
        
        # Secret code to unlock 'CashLait' bot type for this user
        if text_value == CASHLAIT_UNLOCK_CODE:
            already_global = is_cashlait_unlocked_globally()
            try:
                db_execute("UPDATE users SET cashlait_unlocked = 1 WHERE user_id = ?", (user_id,), commit=True)
            except Exception as e:
                logging.error(f"Не удалось установить флаг cashlait_unlocked для пользователя {user_id}: {e}")
            if not already_global:
                try:
                    unlock_cashlait_globally()
                    logging.info(f"Пользователь {user_id} открыл доступ к типу 'CashLait' для всех пользователей")
                except Exception as e:
                    logging.error(f"Не удалось открыть доступ к CashLait глобально: {e}")
            confirmation_text = "✅ Тип бота 'CashLait' уже был доступен всем пользователям!" if already_global else "✅ Новый тип бота 'CashLait' теперь доступен всем пользователям!"
            bot.send_message(user_id, confirmation_text, parse_mode="HTML")
            bot.send_message(user_id, get_custom_text('create_bot_prompt'), parse_mode="HTML", reply_markup=create_bot_type_menu(user_id))
            return

        # Secret code to unlock 'DiceLite' bot type for this user
        if text_value == DICELITE_UNLOCK_CODE:
            already_global = is_dicelite_unlocked_globally()
            try:
                db_execute("UPDATE users SET dicelite_unlocked = 1 WHERE user_id = ?", (user_id,), commit=True)
            except Exception as e:
                logging.error(f"Не удалось установить флаг dicelite_unlocked для пользователя {user_id}: {e}")
            if not already_global:
                try:
                    unlock_dicelite_globally()
                    logging.info(f"Пользователь {user_id} открыл доступ к типу 'DiceLite' для всех пользователей")
                except Exception as e:
                    logging.error(f"Не удалось открыть доступ к DiceLite глобально: {e}")
            confirmation_text = (
                "✅ Тип бота 'DiceLite' уже был доступен всем пользователям!"
                if already_global
                else "✅ Новый тип бота 'DiceLite' теперь доступен всем пользователям!"
            )
            bot.send_message(user_id, confirmation_text, parse_mode="HTML")
            bot.send_message(user_id, get_custom_text('create_bot_prompt'), parse_mode="HTML", reply_markup=create_bot_type_menu(user_id))
            return

        # Secret code to unlock 'Exchange' bot type for this user
        if text_value == EXCHANGE_UNLOCK_CODE:
            already_global = is_exchange_unlocked_globally()
            try:
                db_execute("UPDATE users SET exchange_unlocked = 1 WHERE user_id = ?", (user_id,), commit=True)
            except Exception as e:
                logging.error(f"Не удалось установить флаг exchange_unlocked для пользователя {user_id}: {e}")
            if not already_global:
                try:
                    unlock_exchange_globally()
                    logging.info(f"Пользователь {user_id} открыл доступ к типу 'Бот Обменник' для всех пользователей")
                except Exception as e:
                    logging.error(f"Не удалось открыть доступ к Бот Обменник глобально: {e}")
            confirmation_text = (
                "✅ Тип бота 'Бот Обменник' уже был доступен всем пользователям!"
                if already_global
                else "✅ Новый тип бота 'Бот Обменник' теперь доступен всем пользователям!"
            )
            bot.send_message(user_id, confirmation_text, parse_mode="HTML")
            bot.send_message(user_id, get_custom_text('create_bot_prompt'), parse_mode="HTML", reply_markup=create_bot_type_menu(user_id))
            return

        if text_value == CUSTOMIZATION_UNLOCK_CODE and is_admin(user_id):
            if is_customization_unlocked():
                bot.send_message(user_id, "ℹ️ Режим кастомизации уже активирован.")
            else:
                set_setting(CUSTOMIZATION_SETTING_KEY, '1')
                bot.send_message(user_id, "✅ Режим кастомизации активирован! Кнопка 'Кастомизация' добавлена в админ-меню.")
            bot.send_message(user_id, get_custom_text('admin_menu_heading'), reply_markup=create_admin_menu())
            return

        if message.text == main_buttons['create']:
            count = get_user_bots_count(user_id)
            try:
                limit_setting = int(float(get_setting('MAX_BOTS_PER_USER') or MAX_BOTS_PER_USER))
            except Exception:
                limit_setting = MAX_BOTS_PER_USER
            if count >= limit_setting and not is_admin(user_id):
                bot.send_message(user_id, f"❌ *Лимит достигнут!* Вы создали {count} из {limit_setting} ботов.", parse_mode="Markdown")
                return
            bot.send_message(user_id, get_custom_text('create_bot_prompt'), parse_mode="HTML", reply_markup=create_bot_type_menu(user_id))
        elif message.text == main_buttons['lists']:
            # Check if feature enabled
            try:
                bl_enabled_raw = get_setting('bots_list_feature_enabled')
                bl_enabled = str(bl_enabled_raw).strip() in ('1', 'true', 'True') if bl_enabled_raw is not None else True
            except Exception:
                bl_enabled = True
            if not bl_enabled:
                return
            try:
                min_users = int(float(get_setting('bots_list_min_users') or 30))
            except Exception:
                min_users = 30
            listed = build_public_bots_list(min_users)
            if not listed:
                bot.send_message(user_id, f"Список пуст. Нет ботов с ≥ {min_users} пользователями.")
                return
            lines = []
            for bid, uname, btype, cnt, link in listed[:50]:
                type_icon = "💸" if btype == 'ref' else ("⭐" if btype == 'stars' else "🖱")
                username_show = f"@{uname}" if uname != 'Без имени' else 'Без имени'
                link_show = link if link != '—' else '—'
                lines.append(f"{type_icon} ID: <code>{bid}</code> | {username_show} | 👥 {cnt} | 🔗 {link_show}")
            text = "<b>📋 Списки ботов</b>\n\n" + "\n".join(lines)
            bot.send_message(user_id, text, parse_mode="HTML")
        elif message.text == main_buttons['my_bots']:
            bot.send_message(user_id, get_custom_text('my_bots_intro'), parse_mode="HTML", reply_markup=create_my_bots_menu(user_id))
        elif message.text == main_buttons['wallet']:
            handle_personal_cabinet(message)
        elif message.text == main_buttons['about']:
            total_users = db_execute("SELECT COUNT(*) FROM users", fetchone=True)[0]
            total_bots_created = db_execute("SELECT COUNT(*) FROM bots", fetchone=True)[0]
            running_bots = db_execute("SELECT COUNT(*) FROM bots WHERE status = 'running'", fetchone=True)[0]

            text = (
                "📊 <b>Статистика проекта</b> ❞\n"
                f" L 🗓️ Запуск: <b>{PROJECT_START_DATE}</b>\n"
                f" L 👥 Пользователей: <b>{total_users}</b>\n"
                "━━━━━━━━━━━━━━━\n"
                "🤖 <b>Боты в системе</b> ❞\n"
                f" L 🔢 Всего создано: <b>{total_bots_created}</b>\n"
                f" L 🟢 Активных на данный момент: <b>{running_bots}</b>"
            )

            markup = types.InlineKeyboardMarkup(row_width=2)
            owner_button = types.InlineKeyboardButton("Владелец 👨‍💻", url=f"tg://user?id={ADMIN_ID}")
            # Prefer dynamic settings over defaults
            admin_chat_link = get_setting('admin_chat_link') or ADMIN_CHAT_LINK
            channel_link = get_setting('channel_link') or CHANNEL_LINK
            chat_button = types.InlineKeyboardButton("Чат Администраторов 💬", url=admin_chat_link) if admin_chat_link else None
            channel_button = types.InlineKeyboardButton("Канал 📢", url=channel_link) if channel_link else None
            buttons_to_add = [btn for btn in [owner_button, chat_button, channel_button] if btn is not None]
            if len(buttons_to_add) % 2 != 0 and len(buttons_to_add) > 1:
                markup.add(*buttons_to_add[:-1]); markup.add(buttons_to_add[-1])
            else: markup.add(*buttons_to_add)
            bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=markup)
        elif message.text == main_buttons['admin'] and is_admin(user_id):
            bot.send_message(user_id, get_custom_text('admin_menu_heading'), reply_markup=create_admin_menu())

    @bot.callback_query_handler(func=lambda call: True)
    def handle_callback_query(call):
        try:
            with open("callback_debug.log", "a", encoding="utf-8") as f:
                f.write(f"Got callback: {call.data} from {call.from_user.id}\n")
        except: pass
        
        user_id = call.from_user.id
        try:
            # Handle watermark toggle callback (admin only)
            if call.data == 'wm_toggle' and is_admin(user_id):
                wm_enabled_raw = get_setting('creator_watermark_enabled')
                wm_enabled = str(wm_enabled_raw).strip() in ('1', 'true', 'True')
                new_value = '0' if wm_enabled else '1'
                set_setting('creator_watermark_enabled', new_value)
                bot.answer_callback_query(call.id, "Готово")
                # Update the message with new status and button text
                wm_enabled = not wm_enabled
                toggle_text = "🔕 Отключить подпись" if wm_enabled else "🔔 Включить подпись"
                status_text = "Сейчас: ▶️ включена" if wm_enabled else "Сейчас: ⏸️ выключена"
                markup = types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton(toggle_text, callback_data="wm_toggle")
                )
                try:
                    bot.edit_message_reply_markup(user_id, call.message.message_id, reply_markup=markup)
                except Exception:
                    pass
                try:
                    bot.edit_message_text(f"Настройка подписи к приветствию. {status_text}", user_id, call.message.message_id, reply_markup=markup)
                except Exception:
                    pass
                return

            # Handle bots list feature toggle (admin only)
            if call.data == 'bl_toggle' and is_admin(user_id):
                bl_enabled_raw = get_setting('bots_list_feature_enabled')
                bl_enabled = str(bl_enabled_raw).strip() in ('1', 'true', 'True') if bl_enabled_raw is not None else True
                new_value = '0' if bl_enabled else '1'
                set_setting('bots_list_feature_enabled', new_value)
                bot.answer_callback_query(call.id, "Готово")
                # Update message with both toggles' current states
                wm_enabled_raw = get_setting('creator_watermark_enabled')
                wm_enabled = str(wm_enabled_raw).strip() in ('1', 'true', 'True')
                bl_enabled = not bl_enabled
                wm_toggle_text = "🔕 Отключить подпись" if wm_enabled else "🔔 Включить подпись"
                wm_status_text = "Сейчас: ▶️ включена" if wm_enabled else "Сейчас: ⏸️ выключена"
                bl_toggle_text = "🗂️ Отключить 'Списки ботов'" if bl_enabled else "🗂️ Включить 'Списки ботов'"
                bl_status_text = "Списки: ▶️ включены" if bl_enabled else "Списки: ⏸️ выключены"
                markup = types.InlineKeyboardMarkup(row_width=1)
                markup.add(types.InlineKeyboardButton(wm_toggle_text, callback_data="wm_toggle"))
                markup.add(types.InlineKeyboardButton(bl_toggle_text, callback_data="bl_toggle"))
                try:
                    bot.edit_message_reply_markup(user_id, call.message.message_id, reply_markup=markup)
                except Exception:
                    pass
                try:
                    bot.edit_message_text(f"Настройки:\n• Подпись: {wm_status_text}\n• {bl_status_text}", user_id, call.message.message_id, reply_markup=markup)
                except Exception:
                    pass
                return

            if call.data.startswith('admin_'):
                if call.data == 'admin_lists_menu':
                    call.data = 'admin_lists_menu'
                if call.data == 'admin_lists_set_min' or call.data == 'admin_lists_set_min_users':
                    call.data = 'admin_lists_set_min'
                if call.data.startswith('admin_list_'):
                    # пробрасываем напрямую в обработчик списков
                    handle_admin_callbacks(call)
                    return
                handle_admin_callbacks(call)
                return

            if call.data.startswith('vip_'):
                parts = call.data.split('_')
                bot_id = int(parts[1])
                action = parts[2]
                
                if action == 'toggle':
                    bot_info = get_bot_by_id(bot_id)
                    if bot_info['vip_status']:
                        bot.answer_callback_query(call.id, "✅ У этого бота уже есть VIP-статус.", show_alert=True)
                        return

                    vip_price = float(get_setting('vip_price') or 120.0)
                    text = (f"⭐ <b>Покупка VIP-статуса</b>\n\n"
                            f"Стоимость: <b>{vip_price:.2f} USDT</b>\n\n"
                            f"Что дает VIP-статус?\n"
                            f"- Убирает кнопку 'Хочу такого же бота' в дочерних ботах.\n"
                            f"- Отключает сообщение 'Бот создан в...' при запуске дочерних ботов.\n\n"
                            f"Выберите способ оплаты:")
                    markup = types.InlineKeyboardMarkup(row_width=1)
                    if is_crypto_token_configured():
                        markup.add(types.InlineKeyboardButton("💳 Crypto Bot", callback_data=f"vip_{bot_id}_crypto_pay"))
                    markup.add(types.InlineKeyboardButton("👤 Другой способ", callback_data=f"vip_{bot_id}_other_payment"))
                    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"actions_{bot_id}"))
                    bot.edit_message_text(text, user_id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
                
                elif action == 'crypto':
                    if parts[3] == 'pay':
                        if not is_crypto_token_configured():
                            bot.answer_callback_query(call.id, "❌ Crypto Pay токен не настроен.", show_alert=True)
                            return
                        local_crypto = get_crypto_client()
                        if not local_crypto:
                            bot.answer_callback_query(call.id, "❌ Crypto Pay недоступен сейчас.", show_alert=True)
                            return
                        bot.answer_callback_query(call.id, "⏳ Создаю счет...")
                        vip_price = float(get_setting('vip_price') or 120.0)
                        
                        async def create_invoice_async():
                            try:
                                invoice = await local_crypto.create_invoice(asset='USDT', amount=vip_price, fiat='RUB', payload=f"vip_{bot_id}")
                                if invoice:
                                    db_execute("INSERT INTO crypto_payments (invoice_id, bot_id, user_id, amount, status) VALUES (?, ?, ?, ?, 'pending')",
                                               (invoice.invoice_id, bot_id, user_id, vip_price), commit=True)
                                    markup = types.InlineKeyboardMarkup(row_width=1)
                                    markup.add(types.InlineKeyboardButton("💳 Оплатить счет", url=invoice.bot_invoice_url))
                                    markup.add(types.InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"vip_{bot_id}_check_{invoice.invoice_id}"))
                                    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"vip_{bot_id}_toggle"))
                                    bot.edit_message_text("✅ Счет создан. Нажмите на кнопку ниже для оплаты.", user_id, call.message.message_id, reply_markup=markup)
                            except Exception as e:
                                logging.error(f"Ошибка создания счета CryptoPay: {e}")
                                bot.answer_callback_query(call.id, "❌ Не удалось создать счет. Попробуйте позже.", show_alert=True)
                        
                        run_async_task(create_invoice_async())

                elif action == 'other':
                    admin_info = bot.get_chat(ADMIN_ID)
                    bot.edit_message_text(f"Для покупки другим способом, пожалуйста, свяжитесь с администратором: @{admin_info.username}",
                                          user_id, call.message.message_id,
                                          reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"vip_{bot_id}_toggle")))

                elif action == 'check':
                    invoice_id_to_check = int(parts[3])
                    bot.answer_callback_query(call.id, "Проверяю статус платежа...")
                    
                    async def check_single_invoice():
                        local_crypto = get_crypto_client()
                        if not local_crypto:
                            bot.answer_callback_query(call.id, "❌ Crypto Pay недоступен сейчас.", show_alert=True)
                            return
                        invoices = await local_crypto.get_invoices(invoice_ids=str(invoice_id_to_check))
                        if invoices and invoices[0].status == 'paid':
                            update_bot_setting(bot_id, 'vip_status', True)
                            db_execute("UPDATE crypto_payments SET status = 'paid' WHERE invoice_id = ?", (invoice_id_to_check,), commit=True)
                            bot.edit_message_text(f"✅ Оплата прошла успешно! VIP-статус для бота #{bot_id} активирован.", user_id, call.message.message_id,
                                                  reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ В меню бота", callback_data=f"actions_{bot_id}")))
                        else:
                            bot.answer_callback_query(call.id, "❌ Платеж еще не прошел или счет истек.", show_alert=True)
                    
                    run_async_task(check_single_invoice())
                return
            
            if call.data.startswith('buy_creator_'):
                parts = call.data.split('_')
                bot_id = int(parts[2])
                creator_price = float(get_setting('creator_price') or 500.0)
                
                text = (f"🎨 <b>Покупка бота Креатор</b>\n\n"
                        f"Стоимость: <b>{creator_price:.2f} USDT</b>\n\n"
                        f"Что дает бот Креатор?\n"
                        f"- Создание собственных ботов\n"
                        f"- Управление ботами\n"
                        f"- Настройка параметров\n\n"
                        f"Выберите способ оплаты:")
                markup = types.InlineKeyboardMarkup(row_width=1)
                if is_crypto_token_configured():
                    markup.add(types.InlineKeyboardButton("💳 Crypto Bot", callback_data=f"creator_{bot_id}_crypto_pay"))
                markup.add(types.InlineKeyboardButton("👤 Другой способ", callback_data=f"creator_{bot_id}_other_payment"))
                markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"actions_{bot_id}"))
                bot.edit_message_text(text, user_id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
                return
            
            if call.data.startswith('creatornew_'):
                # Оплата Креатора из меню создания бота (без существующего bot_id)
                parts = call.data.split('_')
                action = parts[1]
                if action == 'crypto' and parts[2] == 'pay':
                    if not is_crypto_token_configured():
                        bot.answer_callback_query(call.id, "❌ Crypto Pay токен не настроен.", show_alert=True)
                        return
                    local_crypto = get_crypto_client()
                    if not local_crypto:
                        bot.answer_callback_query(call.id, "❌ Crypto Pay недоступен сейчас.", show_alert=True)
                        return
                    bot.answer_callback_query(call.id, "⏳ Создаю счет...")
                    creator_price = float(get_setting('creator_price') or 500.0)
                    async def create_creatornew_invoice_async():
                        try:
                            payload = f"creator_new_{user_id}"
                            invoice = await local_crypto.create_invoice(asset='USDT', amount=creator_price, fiat='RUB', payload=payload)
                            if invoice:
                                db_execute("INSERT INTO crypto_payments (invoice_id, bot_id, user_id, amount, status) VALUES (?, ?, ?, ?, 'pending')",
                                           (invoice.invoice_id, 0, user_id, creator_price), commit=True)
                                markup = types.InlineKeyboardMarkup(row_width=1)
                                markup.add(types.InlineKeyboardButton("💳 Оплатить счет", url=invoice.bot_invoice_url))
                                markup.add(types.InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"creatornew_check_{invoice.invoice_id}"))
                                markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"create_bot_creator"))
                                bot.edit_message_text("✅ Счет создан. Нажмите на кнопку ниже для оплаты.", user_id, call.message.message_id, reply_markup=markup)
                        except Exception as e:
                            logging.error(f"Ошибка создания счета CryptoPay для Креатора (new): {e}")
                            bot.answer_callback_query(call.id, "❌ Не удалось создать счет. Попробуйте позже.", show_alert=True)
                    run_async_task(create_creatornew_invoice_async())
                    return
                elif action == 'other' and parts[2] == 'payment':
                    admin_info = bot.get_chat(ADMIN_ID)
                    bot.edit_message_text(f"Для покупки бота Креатор другим способом, свяжитесь с администратором: @{admin_info.username}",
                                          user_id, call.message.message_id,
                                          reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"create_bot_creator")))
                    return
                elif action == 'check':
                    invoice_id_to_check = int(parts[2])
                    bot.answer_callback_query(call.id, "Проверяю статус платежа...")
                    async def check_creatornew_invoice():
                        local_crypto = get_crypto_client()
                        if not local_crypto:
                            bot.answer_callback_query(call.id, "❌ Crypto Pay недоступен сейчас.", show_alert=True)
                            return
                        invoices = await local_crypto.get_invoices(invoice_ids=str(invoice_id_to_check))
                        if invoices and invoices[0].status == 'paid':
                            creator_bot_id = create_bot_in_db(user_id, 'creator')
                            db_execute("UPDATE crypto_payments SET status = 'paid' WHERE invoice_id = ?", (invoice_id_to_check,), commit=True)
                            bot.edit_message_text(f"✅ Оплата прошла успешно! Бот Креатор #{creator_bot_id} создан!\n\nОжидайте выдачи бота! Вам напишет админ!", user_id, call.message.message_id,
                                                  reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ В меню ботов", callback_data="back_to_bots_list")))
                            try:
                                buyer = get_user(user_id)
                                bot.send_message(ADMIN_ID, f"🛒 Покупка Креатора: пользователь <code>{user_id}</code> (@{escape(buyer['username'] or 'N/A')}) оплатил счет #{invoice_id_to_check}. Создан бот #{creator_bot_id}.", parse_mode="HTML")
                            except Exception as e:
                                logging.warning(f"Не удалось уведомить админа о покупке Креатора: {e}")
                        else:
                            bot.answer_callback_query(call.id, "❌ Платеж еще не прошел или счет истек.", show_alert=True)
                    run_async_task(check_creatornew_invoice())
                return
            
            elif call.data.startswith('cashlaitnew_check_'):
                parts = call.data.split('_')
                invoice_id_to_check = int(parts[2])
                bot.answer_callback_query(call.id, "Проверяю статус платежа...")
                async def check_cashlaitnew_invoice():
                    local_crypto = get_crypto_client()
                    if not local_crypto:
                        bot.answer_callback_query(call.id, "❌ Crypto Pay недоступен сейчас.", show_alert=True)
                        return
                    invoices = await local_crypto.get_invoices(invoice_ids=str(invoice_id_to_check))
                    if invoices and invoices[0].status == 'paid':
                        cashlait_bot_id = create_bot_in_db(user_id, 'cashlait')
                        db_execute("UPDATE crypto_payments SET status = 'paid' WHERE invoice_id = ?", (invoice_id_to_check,), commit=True)
                        bot.edit_message_text(f"✅ Оплата прошла успешно! CashLait бот #{cashlait_bot_id} создан! Теперь он в списке ваших ботов:", user_id, call.message.message_id,
                                              reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ В меню ботов", callback_data="back_to_bots_list")))
                        try:
                            buyer = get_user(user_id)
                            bot.send_message(ADMIN_ID, f"🛒 Покупка CashLait: пользователь <code>{user_id}</code> (@{escape(buyer['username'] or 'N/A')}) оплатил счет #{invoice_id_to_check}. Создан бот #{cashlait_bot_id}.", parse_mode="HTML")
                        except Exception as e:
                            logging.warning(f"Не удалось уведомить админа о покупке CashLait: {e}")
                    else:
                        bot.answer_callback_query(call.id, "❌ Платеж еще не прошел или счет истек.", show_alert=True)
                
                run_async_task(check_cashlaitnew_invoice())
                return
            elif call.data.startswith('dicelitenew_check_'):
                parts = call.data.split('_')
                invoice_id_to_check = int(parts[2])
                bot.answer_callback_query(call.id, "Проверяю статус платежа...")
                async def check_dicelitenew_invoice():
                    local_crypto = get_crypto_client()
                    if not local_crypto:
                        bot.answer_callback_query(call.id, "❌ Crypto Pay недоступен сейчас.", show_alert=True)
                        return
                    invoices = await local_crypto.get_invoices(invoice_ids=str(invoice_id_to_check))
                    if invoices and invoices[0].status == 'paid':
                        dicelite_bot_id = create_bot_in_db(user_id, 'dicelite')
                        db_execute("UPDATE crypto_payments SET status = 'paid' WHERE invoice_id = ?", (invoice_id_to_check,), commit=True)
                        bot.edit_message_text(
                            f"✅ Оплата прошла успешно! DiceLite бот #{dicelite_bot_id} создан! Теперь он в списке ваших ботов.",
                            user_id,
                            call.message.message_id,
                            reply_markup=create_my_bots_menu(user_id)
                        )
                        try:
                            buyer = get_user(user_id)
                            bot.send_message(
                                ADMIN_ID,
                                f"🛒 Покупка DiceLite: пользователь <code>{user_id}</code> (@{escape(buyer['username'] or 'N/A')}) оплатил счет #{invoice_id_to_check}. Создан бот #{dicelite_bot_id}.",
                                parse_mode="HTML"
                            )
                        except Exception as e:
                            logging.warning(f"Не удалось уведомить админа о покупке DiceLite: {e}")
                    else:
                        bot.answer_callback_query(call.id, "❌ Платеж еще не прошел или счет истек.", show_alert=True)
                run_async_task(check_dicelitenew_invoice())
                return

            if call.data.startswith('creator_'):
                parts = call.data.split('_')
                # Обрабатываем только паттерны оплаты вида: creator_{botId}_{action}_...
                # Кнопки вывода средств имеют вид creator_withdraw_start и не должны попадать сюда.
                if len(parts) >= 4 and parts[1].isdigit():
                    bot_id = int(parts[1])
                    action = parts[2]
                    
                    if action == 'crypto' and parts[3] == 'pay':
                        if not is_crypto_token_configured():
                            bot.answer_callback_query(call.id, "❌ Crypto Pay токен не настроен.", show_alert=True)
                            return
                        local_crypto = get_crypto_client()
                        if not local_crypto:
                            bot.answer_callback_query(call.id, "❌ Crypto Pay недоступен сейчас.", show_alert=True)
                            return
                        bot.answer_callback_query(call.id, "⏳ Создаю счет...")
                        creator_price = float(get_setting('creator_price') or 500.0)
                        
                        async def create_creator_invoice_async():
                            try:
                                invoice = await local_crypto.create_invoice(asset='USDT', amount=creator_price, fiat='RUB', payload=f"creator_{bot_id}")
                                if invoice:
                                    db_execute("INSERT INTO crypto_payments (invoice_id, bot_id, user_id, amount, status) VALUES (?, ?, ?, ?, 'pending')",
                                               (invoice.invoice_id, bot_id, user_id, creator_price), commit=True)
                                    markup = types.InlineKeyboardMarkup(row_width=1)
                                    markup.add(types.InlineKeyboardButton("💳 Оплатить счет", url=invoice.bot_invoice_url))
                                    markup.add(types.InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"creator_{bot_id}_check_{invoice.invoice_id}"))
                                    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"buy_creator_{bot_id}"))
                                    bot.edit_message_text("✅ Счет создан. Нажмите на кнопку ниже для оплаты.", user_id, call.message.message_id, reply_markup=markup)
                            except Exception as e:
                                logging.error(f"Ошибка создания счета CryptoPay для Креатора: {e}")
                                bot.answer_callback_query(call.id, "❌ Не удалось создать счет. Попробуйте позже.", show_alert=True)
                        
                        run_async_task(create_creator_invoice_async())
                        return
                    
                    elif action == 'other':
                        admin_info = bot.get_chat(ADMIN_ID)
                        bot.edit_message_text(f"Для покупки бота Креатор другим способом, пожалуйста, свяжитесь с администратором: @{admin_info.username}",
                                              user_id, call.message.message_id,
                                              reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"buy_creator_{bot_id}")))
                        return
                    
                    elif action == 'check':
                        invoice_id_to_check = int(parts[3])
                        bot.answer_callback_query(call.id, "Проверяю статус платежа...")
                        
                        async def check_creator_invoice():
                            local_crypto = get_crypto_client()
                            if not local_crypto:
                                bot.answer_callback_query(call.id, "❌ Crypto Pay недоступен сейчас.", show_alert=True)
                                return
                            invoices = await local_crypto.get_invoices(invoice_ids=str(invoice_id_to_check))
                            if invoices and invoices[0].status == 'paid':
                                # Создаем бота Креатор
                                creator_bot_id = create_bot_in_db(user_id, 'creator')
                                db_execute("UPDATE crypto_payments SET status = 'paid' WHERE invoice_id = ?", (invoice_id_to_check,), commit=True)
                                bot.edit_message_text(f"✅ Оплата прошла успешно! Бот Креатор #{creator_bot_id} создан!\n\nОжидайте выдачи бота! Вам напишет админ!", user_id, call.message.message_id,
                                                      reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ В меню ботов", callback_data="back_to_bots_list")))
                            else:
                                bot.answer_callback_query(call.id, "❌ Платеж еще не прошел или счет истек.", show_alert=True)
                        
                        run_async_task(check_creator_invoice())
                        return
            
            if call.data == "creator_withdraw_start":
                bot.answer_callback_query(call.id)
                user_info = get_user(user_id)
                balance = user_info['balance']
                if balance < MIN_CREATOR_WITHDRAWAL:
                    bot.send_message(user_id, f"❌ Минимальная сумма для вывода: {MIN_CREATOR_WITHDRAWAL:.2f} ₽. У вас на балансе {balance:.2f} ₽.")
                    return
                msg = bot.edit_message_text(f"💰 Ваш баланс: {balance:.2f} ₽\nВы собираетесь вывести всю сумму.\n\nВведите реквизиты для вывода:", 
                                            chat_id=user_id, message_id=call.message.message_id,
                                            reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("❌ Отмена", callback_data="creator_cabinet_show")))
                set_user_state(user_id, {'action': 'creator_withdrawal_details', 'amount': balance, 'message_id': msg.message_id})
                return
            
            if call.data == "creator_cabinet_show":
                bot.answer_callback_query(call.id)
                if user_id in user_states: del user_states[user_id]
                handle_personal_cabinet(call)
                return

            if call.data == "creator_withdraw_history":
                bot.answer_callback_query(call.id)
                withdrawals = db_execute("SELECT * FROM creator_withdrawals WHERE user_id = ? ORDER BY id DESC LIMIT 10", (user_id,), fetchall=True)
                text = "📜 *Ваши последние 10 заявок на вывод:*\n\n"
                if not withdrawals:
                    text += "История пуста."
                else:
                    status_map = {'pending': '⏳ В ожидании', 'approved': '✅ Одобрено', 'declined': '❌ Отклонено'}
                    for wd in withdrawals:
                        try:
                            created_date = datetime.strptime(wd['created_at'], '%Y-%m-%d %H:%M:%S.%f').strftime('%d.%m.%Y')
                        except ValueError:
                            created_date = datetime.strptime(wd['created_at'], '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y')
                        text += f"ID: `{wd['id']}` | Сумма: `{wd['amount']:.2f} ₽` | Статус: {status_map.get(wd['status'], 'Неизвестно')} | Дата: {created_date}\n"
                
                markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад в кабинет", callback_data="creator_cabinet_show"))
                bot.edit_message_text(text, user_id, call.message.message_id, parse_mode="Markdown", reply_markup=markup)
                return

            if call.data.startswith('dop_zarabotok_'):
                bot_id = int(call.data.split('_')[2])
                text, markup = create_dop_zarabotok_menu(bot_id)
                bot.edit_message_text(text, user_id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                bot.answer_callback_query(call.id)
                return
                
            if call.data.startswith('flyer_op_'):
                parts = call.data.split('_')
                action = parts[2]
                bot_id = int(parts[3])
                
                if action == 'apply':
                    bot_info = get_bot_by_id(bot_id)
                    owner = get_user(bot_info['owner_id'])
                    user_count = get_child_bot_user_count(bot_id, bot_info['bot_type'])
                    owner_username = f"@{owner['username']}" if owner['username'] else "Не указан"
                    bot_username = f"@{bot_info['bot_username']}" if bot_info['bot_username'] else "Не указан"
                    admin_text = (f"🚨 <b>Заявка на подключение Flyer</b>\n\n"
                                  f"👤 <b>Владелец:</b> <code>{owner['user_id']}</code> ({escape(owner_username)})\n"
                                  f"🤖 <b>Бот:</b> {escape(bot_username)} (ID: <code>{bot_id}</code>)\n"
                                  f"👥 <b>Пользователей в боте:</b> {user_count}\n"
                                  f"🔑 <b>Токен:</b> <code>{escape(bot_info['bot_token'] or 'НЕ УСТАНОВЛЕН')}</code>\n")
                    markup = types.InlineKeyboardMarkup(row_width=3)
                    markup.add(
                        types.InlineKeyboardButton("✅ Одобрить", callback_data=f"flyer_op_approve_{bot_id}_{owner['user_id']}"),
                        types.InlineKeyboardButton("❌ Отклонить", callback_data=f"flyer_op_decline_{bot_id}_{owner['user_id']}"),
                        types.InlineKeyboardButton("💬 Ответить", callback_data=f"flyer_op_reply_{bot_id}_{owner['user_id']}")
                    )
                    bot.send_message(ADMIN_ID, admin_text, reply_markup=markup, parse_mode="HTML")
                    bot.answer_callback_query(call.id, "✅ Ваша заявка отправлена администратору!", show_alert=True)
                    bot.edit_message_text("⏳ Ваша заявка на рассмотрении. Администратор скоро с вами свяжется.", user_id, call.message.message_id)

                elif action in ['approve', 'decline', 'reply']:
                    target_user_id = int(parts[4])
                    
                    if action == 'approve':
                        bot.answer_callback_query(call.id)
                        msg = bot.edit_message_text(call.message.html_text + "\n\n<b>Статус: ОЖИДАНИЕ КЛЮЧА</b>", chat_id, call.message.message_id, parse_mode="HTML")
                        set_user_state(user_id, {
                            'action': 'admin_set_flyer_key', 'bot_id': bot_id, 'target_user_id': target_user_id,
                            'message_id': msg.message_id, 'original_text': call.message.html_text
                        })
                        bot.send_message(chat_id, f"🔑 Введите Flyer API ключ для бота #{bot_id}:", reply_markup=create_cancel_markup())

                    elif action == 'decline':
                        bot.send_message(target_user_id, f"❌ Ваша заявка на подключение Flyer для бота #{bot_id} была *отклонена*.")
                        bot.edit_message_text(call.message.html_text + "\n\n<b>Статус: ❌ ОТКЛОНЕНО</b>", chat_id, call.message.message_id, parse_mode="HTML")
                        bot.answer_callback_query(call.id)
                    
                    elif action == 'reply':
                        bot.answer_callback_query(call.id)
                        set_user_state(user_id, {
                            'action': 'admin_reply_text', 'target_user_id': target_user_id, 'bot_id': bot_id,
                            'message_id': call.message.message_id, 'call_id': call.id
                        })
                        bot.send_message(chat_id, "Введите текст сообщения для пользователя:", reply_markup=create_cancel_markup())
                return

            if call.data == "dummy": bot.answer_callback_query(call.id); return
            if call.data == "back_to_bots_list":
                bot.answer_callback_query(call.id)
                bot.edit_message_text("Ниже представлен список ваших ботов:", user_id, call.message.message_id, reply_markup=create_my_bots_menu(user_id)); return
            if call.data == "create_bot_ref":
                bot.answer_callback_query(call.id, "Бот создается...")
                bot_id = create_bot_in_db(user_id, 'ref')
                bot.edit_message_text(f"💸 Реферальный бот #{bot_id} создан! Теперь он в списке ваших ботов:", user_id, call.message.message_id, reply_markup=create_my_bots_menu(user_id)); return
            if call.data == "create_bot_stars":
                bot.answer_callback_query(call.id, "Бот создается...")
                bot_id = create_bot_in_db(user_id, 'stars')
                bot.edit_message_text(f"⭐ Бот 'Заработок Звёзд' #{bot_id} создан! Теперь он в списке ваших ботов:", user_id, call.message.message_id, reply_markup=create_my_bots_menu(user_id)); return
            if call.data == "create_bot_clicker":
                bot.answer_callback_query(call.id, "Бот создается...")
                bot_id = create_bot_in_db(user_id, 'clicker')
                bot.edit_message_text(f"🖱 Бот 'Кликер' #{bot_id} создан! Теперь он в списке ваших ботов:", user_id, call.message.message_id, reply_markup=create_my_bots_menu(user_id)); return
            if call.data == "create_bot_anonchat":
                bot.answer_callback_query(call.id, "Бот создается...")
                bot_id = create_bot_in_db(user_id, 'anonchat')
                bot.edit_message_text(f"💬 Бот 'Анонимный чат' #{bot_id} создан! Теперь он в списке ваших ботов:", user_id, call.message.message_id, reply_markup=create_my_bots_menu(user_id)); return
            if call.data == "create_bot_cashlait":
                if not is_crypto_token_configured():
                    bot.answer_callback_query(call.id, "❌ Crypto Pay токен не настроен.", show_alert=True)
                    return
                local_crypto = get_crypto_client()
                if not local_crypto:
                    bot.answer_callback_query(call.id, "❌ Crypto Pay недоступен сейчас.", show_alert=True)
                    return
                bot.answer_callback_query(call.id, "⏳ Создаю счет...")
                cashlait_price = float(get_setting('cashlait_price') or 1.0)
                async def create_cashlait_invoice_async():
                    try:
                        payload = f"cashlait_new_{user_id}"
                        invoice = await local_crypto.create_invoice(asset='USDT', amount=cashlait_price, fiat='USD', payload=payload)
                        if invoice:
                            db_execute("INSERT INTO crypto_payments (invoice_id, bot_id, user_id, amount, status) VALUES (?, ?, ?, ?, 'pending')",
                                       (invoice.invoice_id, 0, user_id, cashlait_price), commit=True)
                            markup = types.InlineKeyboardMarkup(row_width=1)
                            markup.add(types.InlineKeyboardButton("💳 Оплатить счет", url=invoice.bot_invoice_url))
                            markup.add(types.InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"cashlaitnew_check_{invoice.invoice_id}"))
                            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="create_bot_cashlait"))
                            bot.edit_message_text(
                                f"💼 Стоимость CashLait бота: {cashlait_price:.2f} $.\n"
                                f"Оплатите счет по кнопке ниже и возвращайтесь сюда для проверки оплаты.",
                                user_id,
                                call.message.message_id,
                                reply_markup=markup
                            )
                    except Exception as e:
                        logging.error(f"Ошибка создания счета CryptoPay для CashLait: {e}")
                        bot.answer_callback_query(call.id, "❌ Не удалось создать счет. Попробуйте позже.", show_alert=True)
                run_async_task(create_cashlait_invoice_async())
                return
            if call.data == "create_bot_dicelite":
                if not is_crypto_token_configured():
                    bot.answer_callback_query(call.id, "❌ Crypto Pay токен не настроен.", show_alert=True)
                    return
                local_crypto = get_crypto_client()
                if not local_crypto:
                    bot.answer_callback_query(call.id, "❌ Crypto Pay недоступен сейчас.", show_alert=True)
                    return
                bot.answer_callback_query(call.id, "⏳ Создаю счет...")
                dicelite_price = float(get_setting('dicelite_price') or 1.0)
                async def create_dicelite_invoice_async():
                    try:
                        payload = f"dicelite_new_{user_id}"
                        invoice = await local_crypto.create_invoice(asset='USDT', amount=dicelite_price, fiat='USD', payload=payload)
                        if invoice:
                            db_execute("INSERT INTO crypto_payments (invoice_id, bot_id, user_id, amount, status) VALUES (?, ?, ?, ?, 'pending')",
                                       (invoice.invoice_id, 0, user_id, dicelite_price), commit=True)
                            markup = types.InlineKeyboardMarkup(row_width=1)
                            markup.add(types.InlineKeyboardButton("💳 Оплатить счет", url=invoice.bot_invoice_url))
                            markup.add(types.InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"dicelitenew_check_{invoice.invoice_id}"))
                            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="create_bot_dicelite"))
                            bot.edit_message_text(
                                f"🎲 Стоимость DiceLite бота: {dicelite_price:.2f} $.\n"
                                f"Оплатите счет по кнопке ниже и возвращайтесь сюда для проверки оплаты.",
                                user_id,
                                call.message.message_id,
                                reply_markup=markup
                            )
                    except Exception as e:
                        logging.error(f"Ошибка создания счета CryptoPay для DiceLite: {e}")
                        bot.answer_callback_query(call.id, "❌ Не удалось создать счет. Попробуйте позже.", show_alert=True)
                run_async_task(create_dicelite_invoice_async())
                return
            if call.data == "create_bot_exchange":
                bot.answer_callback_query(call.id, "Бот создается...")
                bot_id = create_bot_in_db(user_id, 'exchange')
                bot.edit_message_text(f"💱 Бот 'Обменник' #{bot_id} создан! Теперь он в списке ваших ботов:", user_id, call.message.message_id, reply_markup=create_my_bots_menu(user_id)); return
            # Удалены: create_bot_creator

            data = call.data.split('_')
            action = data[0]
            bot_id = int(data[1])
            
            bot.answer_callback_query(call.id)

            if action == 'actions':
                bot_info = get_bot_by_id(bot_id)
                bot_name = f"@{bot_info['bot_username']}" if bot_info['bot_username'] else f"Бот #{bot_id}"
                type_names = {
                    'ref': "Реферальный",
                    'stars': "Заработок Звёзд",
                    'clicker': "Кликер",
                    'anonchat': "Анонимный чат",
                    'cashlait': "CashLait",
                    'dicelite': "DiceLite",
                }
                bot_type_name = type_names.get(bot_info['bot_type'], "Неизвестный шаблон")
                if bot_info['status'] == 'running' and bot_info['pid'] and psutil.pid_exists(bot_info['pid']):
                    resources = get_process_resources(bot_info['pid'])
                    start_time_val = bot_info['start_time']
                    uptime = time.time() - start_time_val if start_time_val else 0
                    status_text = "🟢 Запущен"
                else:
                    if bot_info['status'] == 'running': update_bot_process_info(bot_id, 'stopped', None, None)
                    resources = {"ram": 0, "cpu": 0}; uptime = 0
                    status_text = "🔴 Остановлен" if bot_info['status'] == 'stopped' else "⚠️ Не настроен"
                text = (f"🤖 <b>Бот:</b> <code>{escape(bot_name)}</code> (ID: <code>{bot_id}</code>)\n"
                        f"🧢 <b>Шаблон:</b> {bot_type_name}\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"📊 <b>Состояние:</b> {status_text}\n"
                        f"💾 <b>RAM:</b> {resources['ram']:.2f} МБ\n"
                        f"⚙️ <b>CPU:</b> {resources['cpu']:.1f}%\n"
                        f"⏱️ <b>Аптайм:</b> {format_uptime(uptime)}")
                bot.edit_message_text(text, user_id, call.message.message_id, reply_markup=create_bot_actions_menu(bot_id), parse_mode="HTML")
            
            elif action == 'config':
                bot_info = get_bot_by_id(bot_id)
                name = f"@{bot_info['bot_username']}" if bot_info['bot_username'] else f"Бот #{bot_id}"
                config_menu = None
                if bot_info['bot_type'] == 'ref':
                    config_menu = create_ref_bot_config_menu(bot_id)
                elif bot_info['bot_type'] == 'stars':
                    config_menu = create_stars_bot_config_menu(bot_id)
                elif bot_info['bot_type'] == 'clicker':
                    config_menu = create_clicker_bot_config_menu(bot_id)
                elif bot_info['bot_type'] == 'anonchat':
                    config_menu = create_anonchat_bot_config_menu(bot_id)
                elif bot_info['bot_type'] == 'cashlait':
                    config_menu = create_cashlait_bot_config_menu(bot_id)
                elif bot_info['bot_type'] == 'dicelite':
                    config_menu = create_dicelite_bot_config_menu(bot_id)
                elif bot_info['bot_type'] == 'exchange':
                    config_menu = create_exchange_bot_config_menu(bot_id)
                bot.edit_message_text(f"⚙️ Меню конфигурации бота {name}", user_id, call.message.message_id, reply_markup=config_menu)
            
            elif action == 'transfer' and data[2] == 'start':
                msg = bot.edit_message_text("📲 Введите ID пользователя, которому хотите передать бота:", user_id, call.message.message_id, 
                                            reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Отмена", callback_data=f"actions_{bot_id}")))
                set_user_state(user_id, {'action': 'transfer_bot', 'bot_id': bot_id, 'message_id': msg.message_id})
            
            elif action == 'transfer' and data[2] == 'confirm':
                new_owner_id = int(data[3])
                update_bot_setting(bot_id, 'owner_id', new_owner_id)
                bot.edit_message_text(f"✅ Бот успешно передан пользователю <code>{new_owner_id}</code>.", user_id, call.message.message_id, reply_markup=create_my_bots_menu(user_id), parse_mode="HTML")

            elif action == 'logs' and data[2] == 'get':
                try:
                    with open(f"logs/bot_{bot_id}.log", "rb") as log_file: bot.send_document(user_id, log_file, caption=f"📄 Логи для бота #{bot_id}")
                except FileNotFoundError: bot.answer_callback_query(call.id, "❌ Лог-файл не найден.", show_alert=True)

            elif action == 'delete' and data[2] == 'confirm':
                # Подтверждение удаления бота целиком
                markup = types.InlineKeyboardMarkup(row_width=2)
                markup.add(types.InlineKeyboardButton("✅ Да, удалить", callback_data=f"delete_{bot_id}_final"),
                           types.InlineKeyboardButton("❌ Отмена", callback_data=f"actions_{bot_id}"))
                bot.edit_message_text("Вы уверены, что хотите удалить этого бота? Это действие необратимо.", user_id, call.message.message_id, reply_markup=markup)
            elif action == 'delete' and data[2] == 'final':
                # Удаление бота: только владелец или админ
                bot_info = get_bot_by_id(bot_id)
                if not bot_info:
                    bot.answer_callback_query(call.id, "❌ Бот не найден.", show_alert=True)
                    return
                if not is_admin(user_id) and user_id != bot_info['owner_id']:
                    bot.answer_callback_query(call.id, "❌ Только владелец или админ могут удалять бота.", show_alert=True)
                    return
                delete_bot_from_db(bot_id)
                bot.edit_message_text("✅ Бот успешно удален.", user_id, call.message.message_id, reply_markup=create_my_bots_menu(user_id))
            
            elif action == 'users' and data[2] == 'export':
                try:
                    bot_info = get_bot_by_id(bot_id)
                    db_filename_map = {
                        'ref': f"dbs/bot_{bot_id}_data.db",
                        'stars': f"dbs/bot_{bot_id}_stars_data.db",
                        'clicker': f"dbs/bot_{bot_id}_clicker_data.db",
                        'anonchat': f"dbs/bot_{bot_id}_anonchat.db",
                        'cashlait': f"dbs/bot_{bot_id}_cashlait.db",
                        'dicelite': f"dbs/bot_{bot_id}_dicelite.db",
                    }
                    db_filename = db_filename_map.get(bot_info['bot_type'])
                    if not db_filename or not os.path.exists(db_filename):
                        bot.answer_callback_query(call.id, "В базе данных этого бота пока нет пользователей.", show_alert=True)
                        return
                    child_conn = sqlite3.connect(f'file:{db_filename}?mode=ro', uri=True)
                    users = child_conn.cursor().execute("SELECT user_id FROM users").fetchall()
                    child_conn.close()
                    if users:
                        user_ids = "\n".join([str(u[0]) for u in users])
                        file_path = f"dbs/export_users_bot_{bot_id}.txt"
                        with open(file_path, "w") as f: f.write(user_ids)
                        with open(file_path, "rb") as f: bot.send_document(user_id, f, caption=f"📁 Пользователи бота #{bot_id} ({len(users)} чел.)")
                        os.remove(file_path)
                    else: bot.answer_callback_query(call.id, "В базе данных этого бота пока нет пользователей.", show_alert=True)
                except (sqlite3.OperationalError, FileNotFoundError): bot.answer_callback_query(call.id, "❌ Не удалось получить доступ к базе данных бота.", show_alert=True)

            elif action == 'admins' and data[2] == 'manage':
                 bot_info = get_bot_by_id(bot_id)
                 admins = json.loads(bot_info['admins'])
                 text = f"Текущие админы: <code>{', '.join(map(str, admins)) if admins else 'нет'}</code>"
                 markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("➕ Добавить админа", callback_data=f"admins_{bot_id}_add"), types.InlineKeyboardButton("⬅️ Назад", callback_data=f"config_{bot_id}"))
                 bot.edit_message_text(text, user_id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

            elif action == 'admins' and data[2] == 'add':
                 msg = bot.edit_message_text("➕ Введите ID нового администратора:", user_id, call.message.message_id,
                                             reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Отмена", callback_data=f"admins_{bot_id}_manage")))
                 set_user_state(user_id, {'action': 'add_admin', 'bot_id': bot_id, 'message_id': msg.message_id})
                 
            elif action == 'logs':
                 log_path = f"logs/bot_{bot_id}.log"
                 if os.path.exists(log_path):
                     try:
                         # Create a temp copy to avoid file locking issues on Windows
                         temp_log = f"logs/temp_read_{bot_id}_{int(time.time())}.log"
                         try:
                             import shutil
                             shutil.copy2(log_path, temp_log)
                             with open(temp_log, "r", encoding="utf-8") as f:
                                 lines = f.readlines()
                                 last_lines = "".join(lines[-30:]) # Read last 30 lines
                                 if not last_lines.strip(): last_lines = "Лог пуст."
                         finally:
                             if os.path.exists(temp_log):
                                 try: os.remove(temp_log)
                                 except: pass
                     except Exception as e:
                         last_lines = f"Ошибка чтения лога: {e}"
                 else:
                     last_lines = "Файл логов не найден. Бот еще не запускался?"
                 
                 try:
                     # Split long messages if needed
                     msg_text = f"📜 <b>Логи бота #{bot_id}:</b>\n\n<pre>{escape(last_lines)}</pre>"
                     if len(msg_text) > 4000: msg_text = msg_text[-4000:]
                     bot.send_message(user_id, msg_text, parse_mode="HTML")
                 except Exception as e:
                     bot.send_message(user_id, f"Ошибка отправки лога: {e}")
                 
                 try: bot.answer_callback_query(call.id)
                 except: pass

            elif action == 'control':
                command = data[2]
                if command in ['start', 'stop']:
                    success, message = start_bot_process(bot_id) if command == 'start' else stop_bot_process(bot_id)
                    bot.answer_callback_query(call.id, message, show_alert=not success)
                    call.data = f"config_{bot_id}"; handle_callback_query(call)
                elif command == 'restart':
                    stop_success, stop_message = stop_bot_process(bot_id)
                    time.sleep(1) 
                    start_success, start_message = start_bot_process(bot_id)
                    if start_success:
                        bot.answer_callback_query(call.id, "Бот успешно перезапущен.", show_alert=True)
                    else:
                        bot.answer_callback_query(call.id, f"Ошибка перезапуска: {start_message}", show_alert=True)
                    call.data = f"config_{bot_id}"; handle_callback_query(call)
            
            elif action == 'edit':
                setting_name = '_'.join(data[2:])
                bot_info = get_bot_by_id(bot_id)
                current_value = bot_info[setting_name]
                prompts = {
                    'bot_token': "🔑 Введите новый токен:", 'welcome_message': "👋 Введите новое приветственное сообщение (HTML):", 
                    'ref_reward_1': "💰 Введите награду за L1:", 'ref_reward_2': "💰 Введите награду за L2:", 
                    'withdrawal_limit': "💳 Введите мин. сумму для вывода:", 'withdrawal_method_text': "🏧 Введите текст способа вывода:",
                    'payout_channel': "📢 Введите @username или ID канала выплат:", 'chat_link': "🔗 Введите полную ссылку на чат поддержки:", 
                    'regulations_text': "📜 Введите текст регламента (HTML):", 'flyer_limit': f"📈 Введите новый лимит ОП (от 1 до 10):",
                    'stars_payments_channel': "📢 Введите @username канала выплат:", 'stars_support_chat': "💬 Введите полную ссылку на чат поддержки:",
                    'stars_welcome_bonus': "🎁 Введите приветственный бонус (звёзд):",
                    'stars_daily_bonus': "🎁 Введите новую награду за ежедневный подарок (звёзд):",
                    'stars_daily_cooldown': "⏱️ Введите новый интервал для подарка (в часах):",
                    'stars_ref_bonus_referrer': "🤝 Введите новый бонус за приглашение друга (звёзд):",
                    'stars_ref_bonus_new_user': "👤 Введите новый бонус для приглашенного пользователя (звёзд):",
                    'click_reward_min': "₽ Введите мин. и макс. награду за клик через | (Пример: 0.001|0.005):",
                    'energy_max': "⚡️ Введите макс. количество энергии:",
                    'energy_regen_rate': "⚡️ Введите скорость восстановления энергии (в сек):",
                    'welcome_bonus_clicker': "🎁 Введите приветственный бонус (монет):",
                    'daily_bonus_clicker': "🎁 Введите ежедневный бонус (монет):",
                    'daily_bonus_cooldown_clicker': "⏱️ Введите интервал для подарка (в часах):",
                    'ref_bonus_referrer_clicker': "🤝 Введите бонус за приглашение друга (монет):",
                    'ref_bonus_new_user_clicker': "👤 Введите бонус для приглашенного (монет):",
                    'withdrawal_min_clicker': "💳 Введите мин. сумму для вывода (монет):",
                    'withdrawal_method_text_clicker': "🏧 Введите текст способа вывода:",
                    'payments_channel_clicker': "📢 Введите @username канала выплат:",
                    'support_chat_clicker': "💬 Введите полную ссылку на чат поддержки:",
                    'anonchat_welcome_message': "👋 Введите приветственное сообщение для анонимного чата (поддерживается HTML):",
                    'anonchat_channel_id': "📢 Введите @username канала для обязательной подписки (или '-' чтобы отключить проверку):",
                    'anonchat_vip_price': "💎 Введите стоимость VIP-поиска (в ₽):",
                    'anonchat_crypto_api_token': "🔐 Введите Crypto Pay API токен (из @CryptoBot):",
                    'anonchat_flyer_api_key': "🪁 Введите ключ Flyer API:",
                    'anonchat_flyer_tasks_limit': "📦 Введите максимальное количество заданий Flyer (целое число):",
                    'cashlait_flyer_api_key': "🪁 Введите ключ Flyer API для CashLait:",
                    'cashlait_crypto_pay_token': "💳 Введите Crypto Pay токен (из @CryptoBot):",
                    'cashlait_currency_symbol': "💱 Введите символ валюты (пример: USDT):",
                    'cashlait_welcome_text': "👋 Введите приветственное сообщение для CashLait (поддерживается HTML):",
                    'dicelite_crypto_pay_token': "💳 Введите Crypto Pay токен (из @CryptoBot) для DiceLite:",
                    'dicelite_welcome_text': "👋 Введите приветственное сообщение для DiceLite (поддерживается HTML):",
                    'exchange_welcome_text': "👋 Введите приветственное сообщение для Бота Обменника (поддерживается HTML):",
                }
                if setting_name in prompts:
                    current_value_str = escape(str(current_value)) if current_value is not None else "Не установлено"
                    if setting_name == 'click_reward_min':
                        current_value_str = f"{(bot_info['click_reward_min'] or 0.001)}|{(bot_info['click_reward_max'] or 0.005)}"
                    
                    msg = bot.edit_message_text(f"Текущее значение:\n<code>{current_value_str}</code>\n\n{prompts[setting_name]}", user_id, call.message.message_id,
                                                reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Отмена", callback_data=f"config_{bot_id}")), parse_mode="HTML")
                    set_user_state(user_id, {'action': 'editing_setting', 'bot_id': bot_id, 'setting': setting_name, 'message_id': msg.message_id})

        except Exception as e:
            logging.critical(f"Критическая ошибка в callback: {e}", exc_info=True)
    
    logging.info("Бот-конструктор запущен...")
    try:
        bot.infinity_polling(timeout=20, long_polling_timeout=10, skip_pending=True)
    except Exception as e:
        logging.critical(f"Критическая ошибка в главном цикле: {e}", exc_info=True)
        time.sleep(15)