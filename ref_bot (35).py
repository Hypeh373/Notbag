# -*- coding: utf-8 -*-
import telebot
from telebot import types
try:
    from telebot.formatting import escape_markdown
except ImportError:
    def escape_markdown(text):
        """
        Minimal fallback to escape Markdown special characters when telebot.formatting is unavailable.
        """
        escape_chars = r'\_*[]()~`>#+-=|{}.!'
        escaped = ""
        for char in text:
            if char in escape_chars:
                escaped += f"\\{char}"
            else:
                escaped += char
        return escaped
import sqlite3
import sys
import os
import json
from datetime import datetime, timedelta
from html import escape
import threading
import time
import logging
from functools import wraps
import random
import traceback

# --- ИМПОРТЫ ДЛЯ FLYER API ---
try:
    import asyncio
    from flyerapi import Flyer, APIError as FlyerAPIError
    FLYER_IMPORTED = True
except ImportError:
    FLYER_IMPORTED = False
# -----------------------------------

# =================================================================================
# --------------------------- ИНИЦИАЛИЗАЦИЯ И НАСТРОЙКИ ---------------------------
# =================================================================================

CONSTRUCTOR_BOT_USERNAME = "@CreatorShop1_Bot"
SHOW_BRANDING = os.environ.get('CREATOR_BRANDING') == 'true'

if len(sys.argv) < 2:
    print("Ошибка: Не указан ID бота..."); sys.exit(1)

BOT_ID = sys.argv[1]
MAIN_DB_NAME = 'creator_data4.db'
BOT_DB_NAME = f'dbs/bot_{BOT_ID}_data.db'

START_TIME = datetime.now()
SETTINGS = {}
user_states = {}

def format_username_md(username):
    """Возвращает username, экранированный для Markdown, чтобы сохранить символы вроде '_'."""
    if not username:
        return "N/A"
    return escape_markdown(f"@{username}")

main_db_lock = threading.Lock() 
bot_db_lock = threading.RLock()
last_check_click_time = {} 

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s - BOT_ID:{BOT_ID} - %(levelname)s - %(message)s")

sqlite3.register_adapter(datetime, lambda val: val.isoformat())
sqlite3.register_converter("DATETIME", lambda val: datetime.fromisoformat(val.decode()))

def get_creator_setting(key):
    with main_db_lock:
        try:
            conn = sqlite3.connect(f'file:{MAIN_DB_NAME}?mode=ro', uri=True, timeout=15)
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else None
        except sqlite3.Error as e:
            logging.error(f"Ошибка чтения настройки '{key}' из главной БД: {e}")
            return None

def load_settings():
    global SETTINGS
    if not os.path.exists(MAIN_DB_NAME):
        logging.critical(f"Главная база данных {MAIN_DB_NAME} не найдена."); sys.exit(1)
    
    with main_db_lock:
        conn = sqlite3.connect(f'file:{MAIN_DB_NAME}?mode=ro', uri=True, timeout=15)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM bots WHERE id = ?", (BOT_ID,))
        settings_row = cursor.fetchone()
        conn.close()
    
    if not settings_row:
        logging.critical(f"Настройки для бота с ID {BOT_ID} не найдены."); sys.exit(1)
    
    SETTINGS = dict(settings_row)
    try:
        admins_json = SETTINGS.get('admins')
        owner_id = SETTINGS.get('owner_id')
        if admins_json:
            SETTINGS['admins'] = json.loads(admins_json)
        else:
            SETTINGS['admins'] = []
        
        if owner_id and owner_id not in SETTINGS['admins']:
             SETTINGS['admins'].append(owner_id)
             
    except (json.JSONDecodeError, TypeError):
        SETTINGS['admins'] = [SETTINGS.get('owner_id')] if SETTINGS.get('owner_id') else []

load_settings()
bot = telebot.TeleBot(SETTINGS['bot_token'])

# =================================================================================
# --------------------------- НАСТРОЙКА FLYER API ---------------------------------
# =================================================================================

FLYER_API_KEY = SETTINGS.get('flyer_api_key') or os.environ.get('FLYER_API_KEY')
FLYER_ENABLED = SETTINGS.get('flyer_op_enabled', False)
flyer = None
async_loop = None

if FLYER_IMPORTED:
    async_loop = asyncio.new_event_loop()

    def run_async_from_sync(coro):
        """Безопасно запускает корутину из синхронного кода в отдельном потоке."""
        if not async_loop or not async_loop.is_running():
            logging.error("Asyncio-цикл не запущен. Невозможно выполнить async-задачу.")
            coro.close()
            return None

        future = asyncio.run_coroutine_threadsafe(coro, async_loop)
        try:
            return future.result(timeout=20)
        except Exception as e:
            logging.error(f"Ошибка при выполнении async-задачи: {e}", exc_info=True)
            return None

    if FLYER_API_KEY and FLYER_ENABLED:
        try:
            flyer = Flyer(key=FLYER_API_KEY)
            logging.info("Flyer API клиент успешно инициализирован.")
        except Exception as e:
            logging.error(f"Критическая ошибка при инициализации клиента Flyer API: {e}", exc_info=True)
            flyer = None
    else:
        logging.warning(f"Flyer API не будет использоваться (API ключ не найден или отключен). Enabled: {FLYER_ENABLED}")

else:
    logging.warning("Flyer API не будет использоваться (библиотека flyerapi не установлена).")
    def run_async_from_sync(coro):
        coro.close()
        return True

def get_admin_op_tasks(user_id):
    """Получает активные, невыполненные админские задания ('Мои ОП') для пользователя."""
    admin_tasks = []
    try:
        with main_db_lock:
            conn_creator = sqlite3.connect(f'file:{MAIN_DB_NAME}?mode=ro', uri=True, timeout=15)
            conn_creator.row_factory = sqlite3.Row
            cursor = conn_creator.cursor()
            query = """
                SELECT a.id, a.title, a.resource_link, a.reward
                FROM admin_tasks AS a
                LEFT JOIN user_completed_admin_tasks AS u ON a.id = u.task_id AND u.user_id = ?
                WHERE u.user_id IS NULL AND a.is_active = 1
            """
            cursor.execute(query, (user_id,))
            tasks_from_db = cursor.fetchall()
            conn_creator.close()

        for task_row in tasks_from_db:
            admin_tasks.append({
                'task': task_row['title'],
                'links': [task_row['resource_link']],
                'signature': f"admin_op_{task_row['id']}",
                'reward': task_row['reward']
            })
        if admin_tasks:
            logging.info(f"[ADMIN_OP] Найдено {len(admin_tasks)} новых заданий 'Мои ОП' для пользователя {user_id}.")
        return admin_tasks
    except sqlite3.Error as e:
        logging.error(f"Ошибка получения заданий 'Мои ОП' из БД конструктора: {e}")
        return []

def credit_owner_for_admin_op(owner_id, user_id, task_id, reward):
    """Начисляет награду владельцу и отмечает админ-задание как выполненное."""
    try:
        with main_db_lock:
            conn_creator = sqlite3.connect(MAIN_DB_NAME, timeout=15)
            cursor = conn_creator.cursor()
            cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (reward, owner_id))
            cursor.execute("INSERT OR IGNORE INTO user_completed_admin_tasks (user_id, task_id) VALUES (?, ?)", (user_id, task_id))
            conn_creator.commit()
            conn_creator.close()
        logging.info(f"[ADMIN_OP_CREDIT] Владельцу {owner_id} начислено {reward} ₽ за задание #{task_id} от юзера {user_id}.")
    except Exception as e:
        logging.error(f"Критическая ошибка в credit_owner_for_admin_op: {e}", exc_info=True)

def credit_owner_for_task(owner_id: int, amount: float, user_id: int, task: dict):
    task_signature = task.get('signature')
    task_type = task.get('task')
    
    if not task_signature:
        logging.error(f"[BotID:{BOT_ID}] Не удалось получить signature для задачи. Начисление невозможно. Task: {task}")
        return

    with main_db_lock:
        try:
            conn_creator = sqlite3.connect(MAIN_DB_NAME, timeout=15)
            cursor_creator = conn_creator.cursor()
            
            if task_type == 'subscribe channel':
                check_after = datetime.utcnow() + timedelta(hours=24)
                try:
                    cursor_creator.execute(
                        "INSERT INTO pending_flyer_rewards (owner_id, bot_id, task_signature, amount, check_after_timestamp) VALUES (?, ?, ?, ?, ?)",
                        (owner_id, BOT_ID, task_signature, amount, check_after)
                    )
                    cursor_creator.execute("UPDATE users SET frozen_balance = frozen_balance + ? WHERE user_id = ?", (amount, owner_id))
                    conn_creator.commit()
                    logging.info(f"[FLYER_CREDIT_HOLD] [BotID:{BOT_ID}] Начислено {amount:.4f} руб. НА УДЕРЖАНИЕ владельцу {owner_id} за задачу {task_signature}")
                except sqlite3.IntegrityError:
                    logging.warning(f"[BotID:{BOT_ID}] Попытка повторно добавить задачу {task_signature} в очередь. Пропускаем.")

            else:
                cursor_creator.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, owner_id))
                conn_creator.commit()
                logging.info(f"[FLYER_CREDIT_DIRECT] [BotID:{BOT_ID}] Начислено {amount:.4f} руб. НАПРЯМУЮ владельцу {owner_id} за задачу {task_signature} (тип: {task_type}).")

            conn_creator.close()
        except Exception as e:
            logging.error(f"[BotID:{BOT_ID}] Критическая ошибка в credit_owner_for_task для владельца {owner_id}: {e}")
            traceback.print_exc()

async def is_flyer_check_passed_async(user_id: int):
    # Главный фикс: если Flyer не инициализирован, пропускаем все проверки ОП
    if not flyer:
        logging.info(f"[BotID:{BOT_ID}] [OP_CHECK] Flyer API неактивен. Проверка ОП пропущена для user_id: {user_id}")
        return True
    
    admin_op_tasks = get_admin_op_tasks(user_id)

    user_flyer_data = db_query("""
        SELECT flyer_tasks_json, flyer_tasks_timestamp, 
               flyer_locked_tasks_json, flyer_locked_timestamp,
               rewarded_flyer_tasks
        FROM users WHERE user_id = ?
    """, (user_id,), fetchone=True)
    
    now = datetime.now()
    
    rewarded_tasks_json = user_flyer_data['rewarded_flyer_tasks'] if user_flyer_data else '[]'
    rewarded_signatures = set(json.loads(rewarded_tasks_json or '[]'))

    locked_tasks = []
    if user_flyer_data and user_flyer_data['flyer_locked_tasks_json'] and user_flyer_data['flyer_locked_timestamp']:
        try:
            locked_tasks_json = user_flyer_data['flyer_locked_tasks_json']
            locked_timestamp = user_flyer_data['flyer_locked_timestamp']
            if now - locked_timestamp < timedelta(hours=24):
                locked_tasks = json.loads(locked_tasks_json or '[]')
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logging.warning(f"[Flyer] [ID: {user_id}] Не удалось прочитать locked_tasks_json: {e}")

    new_tasks = []
    should_fetch_new = True
    if user_flyer_data and user_flyer_data['flyer_tasks_json'] and user_flyer_data['flyer_tasks_timestamp']:
        try:
            tasks_json = user_flyer_data['flyer_tasks_json']
            timestamp = user_flyer_data['flyer_tasks_timestamp']
            if now - timestamp < timedelta(minutes=10):
                should_fetch_new = False
                new_tasks = json.loads(tasks_json or '[]')
        except (json.JSONDecodeError, TypeError, ValueError) as e:
             logging.warning(f"[Flyer] [ID: {user_id}] Не удалось прочитать flyer_tasks_json из кэша: {e}")
    
    if should_fetch_new:
        try:
            limit = int(SETTINGS.get('flyer_limit', 10))
            fetched_tasks = await flyer.get_tasks(user_id=user_id, limit=limit) or []
            db_query("UPDATE users SET flyer_tasks_json = ?, flyer_tasks_timestamp = ? WHERE user_id = ?",
                     (json.dumps(fetched_tasks), now, user_id), commit=True)
            new_tasks = fetched_tasks
        except Exception as e:
            logging.error(f"[Flyer] [ID: {user_id}] ОШИБКА запроса к Flyer API: {e}")
            if user_flyer_data and user_flyer_data['flyer_tasks_json']: 
                try: 
                    new_tasks = json.loads(user_flyer_data['flyer_tasks_json'] or '[]')
                except: 
                    new_tasks = []
            else:
                new_tasks = []

    all_tasks_dict = {task['signature']: task for task in locked_tasks if 'signature' in task}
    all_tasks_dict.update({task['signature']: task for task in new_tasks if 'signature' in task})
    all_tasks_dict.update({task['signature']: task for task in admin_op_tasks if 'signature' in task})
    all_tasks_to_check = list(all_tasks_dict.values())

    if not all_tasks_to_check:
        return True

    FLYER_INCOMPLETE_STATUSES = ('incomplete', 'abort')
    failed_tasks = []
    for task in all_tasks_to_check:
        if task['signature'].startswith('admin_op_'):
            failed_tasks.append(task)
            continue
            
        try:
            status = await flyer.check_task(user_id=user_id, signature=task['signature'])
            if status in FLYER_INCOMPLETE_STATUSES:
                failed_tasks.append(task)
            else:
                if task['signature'] not in rewarded_signatures:
                    sub_reward_str = get_creator_setting('op_reward') or "1.0"
                    reward = float(sub_reward_str)
                    credit_owner_for_task(SETTINGS['owner_id'], reward, user_id, task)
                    rewarded_signatures.add(task['signature'])
        except Exception as e:
            logging.error(f"[Flyer][ID: {user_id}] Ошибка при проверке/начислении за задание {task.get('signature')}: {e}")

    db_query("UPDATE users SET rewarded_flyer_tasks = ? WHERE user_id = ?",
                   (json.dumps(list(rewarded_signatures)), user_id), commit=True)

    if failed_tasks:
        show_flyer_task_message(user_id, failed_tasks)
        return False

    db_query("UPDATE users SET flyer_locked_tasks_json = ?, flyer_locked_timestamp = ? WHERE user_id = ?",
                   (json.dumps(all_tasks_to_check), now, user_id), commit=True)
    
    return True

def show_flyer_task_message(user_id: int, tasks, text_prefix=""):
    FLYER_BUTTON_TEXTS = {
        'start bot': '➕ Запустить бота', 'subscribe channel': '➕ Подписаться',
        'give boost': '➕ Голосовать', 'follow link': '➕ Перейти',
        'perform action': '➕ Выполнить',
    }
    FLYER_WELCOME_TEXT = "✅ *Отлично!* Для доступа к боту, пожалуйста, выполните *спонсорские задания*:"
    
    if not tasks: return
    try:
        markup = types.InlineKeyboardMarkup(row_width=2)
        task_buttons = [types.InlineKeyboardButton(
            text=FLYER_BUTTON_TEXTS.get(t['task'], t['task'].capitalize()), 
            url=link
        ) for t in tasks for link in t.get('links', [])]
        markup.add(*task_buttons)
        markup.add(types.InlineKeyboardButton(text='☑️ Проверить', callback_data='check_subscriptions'))
        final_text = text_prefix + FLYER_WELCOME_TEXT
        bot.send_message(user_id, final_text, reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"[Flyer][ID: {user_id}] Ошибка при показе заданий: {e}")

# =================================================================================
# --------------------------- РАБОТА С БД БОТА ------------------------------------
# =================================================================================

def db_connect():
    return sqlite3.connect(BOT_DB_NAME, timeout=15, detect_types=sqlite3.PARSE_DECLTYPES)

def init_bot_db():
    with bot_db_lock:
        conn = db_connect()
        cursor = conn.cursor()
        cursor.execute('PRAGMA journal_mode=WAL;') 
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, balance REAL DEFAULT 0,
            frozen_balance REAL DEFAULT 0, referrer_l1_id INTEGER, referrer_l2_id INTEGER,
            registration_date DATETIME, is_active INTEGER DEFAULT 0, last_gift_time DATETIME,
            is_banned INTEGER DEFAULT 0,
            op_reward_awarded INTEGER DEFAULT 0,
            flyer_tasks_json TEXT,         
            flyer_tasks_timestamp DATETIME,    
            flyer_locked_tasks_json TEXT,  
            flyer_locked_timestamp DATETIME,
            rewarded_flyer_tasks TEXT DEFAULT '[]'
        )''')
        
        table_info = cursor.execute("PRAGMA table_info(users)").fetchall()
        column_names = [info[1] for info in table_info]

        if 'flyer_locked_tasks_json' not in column_names:
            cursor.execute("ALTER TABLE users ADD COLUMN flyer_tasks_json TEXT")
            cursor.execute("ALTER TABLE users ADD COLUMN flyer_tasks_timestamp DATETIME")
            cursor.execute("ALTER TABLE users ADD COLUMN flyer_locked_tasks_json TEXT")
            cursor.execute("ALTER TABLE users ADD COLUMN flyer_locked_timestamp DATETIME")
            logging.info("Колонки 'flyer_locked' успешно добавлены в БД.")
        
        if 'rewarded_flyer_tasks' not in column_names:
            cursor.execute("ALTER TABLE users ADD COLUMN rewarded_flyer_tasks TEXT DEFAULT '[]'")
            logging.info("Колонка 'rewarded_flyer_tasks' успешно добавлена в БД.")

        if 'is_banned' not in column_names:
            cursor.execute("ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0")
            logging.info("Колонка 'is_banned' успешно добавлена в БД.")

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL,
            wallet_details TEXT, status TEXT DEFAULT 'pending', request_date DATETIME
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS op_channels (channel_username TEXT PRIMARY KEY)''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS promocodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE NOT NULL, reward REAL NOT NULL,
            total_activations INTEGER NOT NULL, used_activations INTEGER DEFAULT 0
        )''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS promocode_activations (
            code_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
            FOREIGN KEY(code_id) REFERENCES promocodes(id) ON DELETE CASCADE,
            PRIMARY KEY (code_id, user_id)
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)''')
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('gift_reward', '1.0')")
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('rules_text', '')")
        conn.commit()
        conn.close()

init_bot_db()

def db_query(query, params=(), fetchone=False, commit=False):
    with bot_db_lock:
        conn = db_connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            if commit:
                conn.commit()
                last_row_id = cursor.lastrowid
                return last_row_id
            if fetchone:
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()
            return result
        except sqlite3.Error as e:
            logging.error(f"Ошибка БД при выполнении запроса '{query}': {e}")
            return None if fetchone else []
        finally:
            conn.close()

def get_bot_setting(key, default=None):
    res = db_query("SELECT value FROM settings WHERE key = ?", (key,), fetchone=True)
    return res['value'] if res else default

def set_bot_setting(key, value):
    db_query("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)), commit=True)

def get_user(user_id):
    return db_query("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True)

def add_user(user_id, username, referrer_id=None):
    with bot_db_lock:
        conn = db_connect()
        cursor = conn.cursor()
        referrer_l1, referrer_l2 = None, None
        if referrer_id:
            ref_data = cursor.execute("SELECT referrer_l1_id FROM users WHERE user_id = ?", (referrer_id,)).fetchone()
            if ref_data: referrer_l1, referrer_l2 = referrer_id, ref_data[0]
        cursor.execute(
            "INSERT OR IGNORE INTO users (user_id, username, referrer_l1_id, referrer_l2_id, registration_date) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, referrer_l1, referrer_l2, datetime.now())
        )
        conn.commit()
        conn.close()

def get_referrals_count(user_id):
    l1_result = db_query("SELECT COUNT(*) as count FROM users WHERE referrer_l1_id = ? AND is_active = 1", (user_id,), fetchone=True)
    l2_result = db_query("SELECT COUNT(*) as count FROM users WHERE referrer_l2_id = ? AND is_active = 1", (user_id,), fetchone=True)
    l1 = l1_result['count'] if l1_result else 0
    l2 = l2_result['count'] if l2_result else 0
    return l1, l2

def get_bot_stats():
    total_users_res = db_query("SELECT COUNT(*) as count FROM users", fetchone=True)
    new_today_res = db_query("SELECT COUNT(*) as count FROM users WHERE DATE(registration_date) >= DATE('now', 'localtime')", fetchone=True)
    total_paid_res = db_query("SELECT SUM(amount) as sum FROM withdrawals WHERE status = 'approved'", fetchone=True)
    
    total_users = total_users_res['count'] if total_users_res else 0
    new_today = new_today_res['count'] if new_today_res else 0
    total_paid = total_paid_res['sum'] if total_paid_res and total_paid_res['sum'] is not None else 0.0
    return total_users, new_today, total_paid

def get_top_referrers(period='all'):
    query = "SELECT referrer_l1_id, COUNT(*) as ref_count FROM users WHERE referrer_l1_id IS NOT NULL AND is_active = 1 "
    if period == 'day': query += " AND registration_date >= date('now','-1 day')"
    query += " GROUP BY referrer_l1_id ORDER BY ref_count DESC LIMIT 10"
    return db_query(query)

def get_user_by_id_or_username(identifier):
    try: 
        return db_query("SELECT * FROM users WHERE user_id = ?", (int(identifier),), fetchone=True)
    except (ValueError, TypeError): 
        return db_query("SELECT * FROM users WHERE username = ? COLLATE NOCASE", (str(identifier).replace('@',''),), fetchone=True)

# =================================================================================
# ------------------------------ ГЕНЕРАТОРЫ КЛАВИАТУР -----------------------------
# =================================================================================

def create_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("👤 Личный кабинет"), types.KeyboardButton("🤝 Заработать"))
    markup.add(types.KeyboardButton("🎁 Подарок"), types.KeyboardButton("📊 О боте"))
    if user_id in SETTINGS.get('admins', []):
        markup.add(types.KeyboardButton("👑 Админ-меню"))
    return markup

def create_inline_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    admin_id = SETTINGS.get('owner_id')
    markup.add(types.InlineKeyboardButton("👑 Администратор", url=f"tg://user?id={admin_id}"))

    if SHOW_BRANDING:
        markup.add(types.InlineKeyboardButton("🚀 Хочу такого бота!", url=f"https://t.me/{CONSTRUCTOR_BOT_USERNAME}"))
    
    def create_url_button(setting_key, button_text):
        value = SETTINGS.get(setting_key, '').strip()
        if not value: return None
        url = value
        if not value.startswith('http'):
            cleaned_value = value.replace('@', '').replace('https://', '').replace('t.me/', '')
            url = f"https://t.me/{cleaned_value}"
        return types.InlineKeyboardButton(button_text, url=url) if url else None

    optional_buttons = []
    
    chat_button = create_url_button('chat_link', "💬 Чат")
    if chat_button: optional_buttons.append(chat_button)

    payout_button = create_url_button('payout_channel', "💸 Выплаты")
    if payout_button: optional_buttons.append(payout_button)

    optional_buttons.append(types.InlineKeyboardButton("📜 Правила", callback_data="show_rules"))

    if optional_buttons: markup.add(*optional_buttons)

    markup.add(
        types.InlineKeyboardButton("🏆 Топ за день", callback_data="top_day"),
        types.InlineKeyboardButton("🏆 Топ за всё время", callback_data="top_all"),
        types.InlineKeyboardButton("🐞 Нашёл баг?", url=f"tg://user?id={admin_id}")
    )
    return markup

def create_admin_menu():
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("📣 Рассылка", callback_data="admin_broadcast"),
        types.InlineKeyboardButton("🎁 Промокоды", callback_data="admin_promo_menu"),
        types.InlineKeyboardButton("⚙️ Настройки бота", callback_data="admin_settings_menu"),
        types.InlineKeyboardButton("📜 Правила", callback_data="admin_rules_edit"),
        types.InlineKeyboardButton("📜 Рефералы пользователя", callback_data="admin_get_referrals"),
        types.InlineKeyboardButton("💬 Написать пользователю", callback_data="admin_message_user"),
        types.InlineKeyboardButton("🚫 Бан/Разбан", callback_data="admin_ban_user"),
        types.InlineKeyboardButton("📋 Заявки на вывод", callback_data="admin_wd_list_1"),
        types.InlineKeyboardButton("📢 Каналы (ОП)", callback_data="admin_op_manage")
    )
    return markup

def create_cancel_markup():
    return types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add("❌ Отмена")

# =================================================================================
# ----------------------- СИСТЕМА ПРОВЕРКИ И АКТИВАЦИИ ----------------------------
# =================================================================================

def check_ban_status(func):
    @wraps(func)
    def wrapper(message, *args, **kwargs):
        user_id = message.from_user.id
        user_data = get_user(user_id)
        if user_data and user_data['is_banned']:
            bot.send_message(user_id, "Вам запрещено пользоваться ботом.")
            return
        return func(message, *args, **kwargs)
    return wrapper

def get_unsubscribed_op_channels(user_id):
    op_channels_rows = db_query("SELECT channel_username FROM op_channels")
    if not op_channels_rows: return []
    op_channels = [row['channel_username'] for row in op_channels_rows]
    
    unsubscribed = []
    for channel in op_channels:
        try:
            member = bot.get_chat_member(channel, user_id)
            if member.status not in ['member', 'administrator', 'creator']: unsubscribed.append(channel)
        except telebot.apihelper.ApiTelegramException as e:
            logging.warning(f"Не удалось проверить подписку на {channel} для {user_id}. Ошибка: {e}")
            unsubscribed.append(channel)
        except Exception as e:
            logging.error(f"Критическая ошибка при проверке подписки на {channel}: {e}")
            unsubscribed.append(channel)
    return unsubscribed

def show_op_channels_message(user_id, channels_list):
    markup = types.InlineKeyboardMarkup()
    for channel in channels_list:
        try:
            markup.add(types.InlineKeyboardButton(f"Подписаться на {channel}", url=f"https://t.me/{channel.replace('@', '')}"))
        except:
            continue
    markup.add(types.InlineKeyboardButton("✅ Я подписался", callback_data="check_subscriptions"))
    bot.send_message(user_id, "Для доступа к боту, пожалуйста, подпишитесь на эти каналы:", reply_markup=markup)

def activate_user_and_give_rewards(user_id):
    with bot_db_lock:
        conn = db_connect()
        cursor = conn.cursor()
        user_data = cursor.execute("SELECT is_active, referrer_l1_id FROM users WHERE user_id = ?", (user_id,)).fetchone()
        
        if not user_data or user_data[0]:
            conn.close()
            return
        
        is_active, referrer_id = user_data

        cursor.execute("UPDATE users SET is_active = 1 WHERE user_id = ?", (user_id,))
        conn.commit()

        if referrer_id:
            try:
                ref_reward_1 = float(SETTINGS.get('ref_reward_1', 0))
                if ref_reward_1 > 0:
                    cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (ref_reward_1, referrer_id))
                    conn.commit()
                    bot.send_message(referrer_id, f"🎉 Ваш реферал активировал бота! Начислено: {ref_reward_1:.2f}₽")
                
                ref2_data = cursor.execute("SELECT referrer_l1_id FROM users WHERE user_id = ?", (referrer_id,)).fetchone()
                if ref2_data and ref2_data[0]:
                    ref_reward_2 = float(SETTINGS.get('ref_reward_2', 0))
                    if ref_reward_2 > 0:
                        cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (ref_reward_2, ref2_data[0]))
                        conn.commit()
                        bot.send_message(ref2_data[0], f"🎉 Реферал 2-го уровня активировал бота! Начислено: {ref_reward_2:.2f}₽")
            except Exception as e: 
                logging.error(f"Не удалось уведомить реферера {referrer_id}: {e}")
        
        conn.close()

def is_subscription_valid(user_id):
    flyer_check_result = run_async_from_sync(is_flyer_check_passed_async(user_id))
    if flyer_check_result is False:
        return False
    if flyer_check_result is None:
        bot.send_message(user_id, "Возникла временная техническая ошибка. Пожалуйста, попробуйте еще раз через несколько секунд.")
        return False

    op_unsubscribed = get_unsubscribed_op_channels(user_id)
    if op_unsubscribed:
        show_op_channels_message(user_id, op_unsubscribed)
        return False
        
    return True

def check_and_activate_user(user_id):
    if is_subscription_valid(user_id):
        activate_user_and_give_rewards(user_id)
        return True
    return False

def require_subscription(func):
    @wraps(func)
    def wrapper(message, *args, **kwargs):
        user_id = message.from_user.id
        user = get_user(user_id)

        if not user or not user['is_active']:
            if not check_and_activate_user(user_id):
                return
        else:
            if not is_subscription_valid(user_id):
                return
        
        return func(message, *args, **kwargs)
    return wrapper

# =================================================================================
# -------------------- ГЛАВНЫЙ ОБРАБОТЧИК СООБЩЕНИЙ -------------------------------
# =================================================================================

@bot.message_handler(commands=['start'])
@check_ban_status
def handle_start(message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    if not get_user(user_id):
        referrer_id = None
        try:
            ref_id_candidate = int(message.text.split()[1])
            if ref_id_candidate != user_id and get_user(ref_id_candidate):
                referrer_id = ref_id_candidate
        except (ValueError, IndexError): pass
        add_user(user_id, username, referrer_id)
        logging.info(f"Новый пользователь {user_id} (@{username}) зарегистрирован.")

    if check_and_activate_user(user_id):
        welcome_message = SETTINGS.get('welcome_message', '👋 Добро пожаловать!')
        if SHOW_BRANDING:
            welcome_message += f"\n\nБот создан с помощью @{CONSTRUCTOR_BOT_USERNAME}"
        
        bot.send_message(user_id, welcome_message, reply_markup=create_main_menu(user_id))

@bot.message_handler(func=lambda m: m.text in ["👤 Личный кабинет", "🤝 Заработать", "🎁 Подарок", "📊 О боте", "👑 Админ-меню"])
@check_ban_status
@require_subscription
def handle_main_menu_buttons(message):
    user_id = message.from_user.id
    
    if message.text == "👑 Админ-меню":
        handle_admin_menu(message)
        return

    if user_id in user_states:
        del user_states[user_id]
        bot.send_message(user_id, "Действие отменено.", reply_markup=create_main_menu(user_id))

    if message.text == "👤 Личный кабинет":
        handle_profile(message)
    elif message.text == "🤝 Заработать":
        handle_earn(message)
    elif message.text == "🎁 Подарок":
        handle_gift(message)
    elif message.text == "📊 О боте":
        show_main_page(message.chat.id)

@bot.message_handler(func=lambda message: True, content_types=['text'])
@check_ban_status
def handle_other_text_commands(message):
    if user_states.get(message.from_user.id):
        process_state_input(message)

def show_main_page(chat_id):
    total_users, new_today, total_paid = get_bot_stats()
    uptime = datetime.now() - START_TIME
    uptime_str = str(uptime).split('.')[0]
    text = (f"📊 *Статистика бота*\n\n"
            f"⏱️ Аптайм: *{uptime_str}*\n"
            f"👥 Всего пользователей: *{total_users}*\n"
            f"🆕 Новых за сегодня: *{new_today}*\n"
            f"💳 Всего выплачено: *{total_paid:.2f} ₽*")
    bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=create_inline_menu())

def handle_profile(message):
    user_id = message.from_user.id
    user_data = get_user(user_id)
    if not user_data: return bot.send_message(user_id, "Вы не зарегистрированы. Нажмите /start")
    
    reg_date_str = "N/A"
    if user_data['registration_date']:
        reg_date_str = user_data['registration_date'].strftime('%d.%m.%Y')

    l1_count, l2_count = get_referrals_count(user_id)
    text = (f"👤 *Ваш личный кабинет*\n\n"
            f"🆔 Ваш ID: `{user_id}`\n"
            f"📅 Дата регистрации: *{reg_date_str}*\n\n"
            f"💰 Баланс: *{user_data['balance']:.2f} ₽*\n"
            f"🤝 Рефералов 1-го уровня: *{l1_count}*\n"
            f"👨‍👩‍👧‍👦 Рефералов 2-го уровня: *{l2_count}*\n")
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("📤 Вывести средства", callback_data="withdraw"),
               types.InlineKeyboardButton("🎁 Ввести промокод", callback_data="promo_enter"))
    bot.send_message(user_id, text, parse_mode="Markdown", reply_markup=markup)

def handle_earn(message):
    bot_username = bot.get_me().username
    ref_link = f"https://t.me/{bot_username}?start={message.from_user.id}"
    ref_reward_1 = float(SETTINGS.get('ref_reward_1', 0))
    ref_reward_2 = float(SETTINGS.get('ref_reward_2', 0))
    text = (f"🔗 *Ваша реферальная ссылка для приглашений*\n\n"
            f"`{ref_link}`\n\n"
            f"💰 За каждого приглашенного пользователя (1-й уровень) вы получите: *{ref_reward_1:.2f} ₽*\n"
            f"💰 За пользователя, приглашенного вашим рефералом (2-й уровень): *{ref_reward_2:.2f} ₽*")
    bot.send_message(message.chat.id, text, parse_mode="Markdown")

def handle_gift(message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user: return

    now = datetime.now()
    if user['last_gift_time']:
        last_gift = user['last_gift_time']
        if now - last_gift < timedelta(hours=24):
            rem = timedelta(hours=24) - (now - last_gift)
            rem_h, rem_m = rem.seconds // 3600, (rem.seconds % 3600) // 60
            bot.send_message(user_id, f"❌ Вы уже получали подарок. Следующий можно будет получить через {rem_h} ч. {rem_m} мин.")
            return

    gift_reward = float(get_bot_setting('gift_reward', 1.0))
    db_query("UPDATE users SET balance = balance + ?, last_gift_time = ? WHERE user_id = ?", (gift_reward, now, user_id), commit=True)
    bot.send_message(user_id, f"🎉 Вы получили подарок в размере {gift_reward:.2f} ₽!")

def handle_admin_menu(message):
    if message.from_user.id in SETTINGS.get('admins', []):
        bot.send_message(message.chat.id, "👑 Админ-меню:", reply_markup=create_admin_menu())

@bot.callback_query_handler(func=lambda call: True)
def handle_callback_query(call):
    user_id = call.from_user.id
    
    user_data = get_user(user_id)
    if user_data and user_data['is_banned']:
        bot.answer_callback_query(call.id, "Вам запрещено пользоваться ботом.", show_alert=True)
        return

    if user_states.get(user_id):
         del user_states[user_id]

    dummy_message = types.Message(message_id=0, from_user=call.from_user, date=None, chat=call.message.chat, content_type='text', options={}, json_string="")

    if call.data == 'check_subscriptions':
        cooldown_seconds = 7
        current_time = time.time()
        if user_id in last_check_click_time and (current_time - last_check_click_time[user_id]) < cooldown_seconds:
            bot.answer_callback_query(call.id, f"⏳ Пожалуйста, подождите {cooldown_seconds} секунд.", show_alert=True)
            return
        last_check_click_time[user_id] = current_time
        
        bot.answer_callback_query(call.id, text="Проверяю...")
        try: bot.delete_message(call.message.chat.id, call.message.message_id)
        except: pass
        
        # Добавляем логику зачисления награды за "Мои ОП"
        admin_op_tasks_to_credit = get_admin_op_tasks(user_id)
        if admin_op_tasks_to_credit:
            owner_id = SETTINGS.get('owner_id')
            if owner_id:
                for task in admin_op_tasks_to_credit:
                    task_id_str = task['signature'].replace('admin_op_', '')
                    if task_id_str.isdigit():
                        credit_owner_for_admin_op(owner_id, user_id, int(task_id_str), task['reward'])
            else:
                logging.error(f"[BotID:{BOT_ID}] Не удалось начислить за 'Мои ОП', ID владельца не найден в настройках.")

        handle_start(dummy_message) 
        return

    if call.data.startswith("admin_"):
        handle_admin_callbacks(call)
        return

    if call.data == "promo_enter":
        bot.answer_callback_query(call.id)
        user_states[user_id] = {'action': 'promo_enter_code'}
        bot.send_message(user_id, "Введите ваш промокод:", reply_markup=create_cancel_markup())

    elif call.data == "withdraw":
        @require_subscription
        def withdraw_action(message):
            user_data = get_user(user_id)
            if not user_data: return
            min_withdrawal = float(SETTINGS.get('withdrawal_limit', 0.0))
            if user_data['balance'] >= min_withdrawal:
                bot.answer_callback_query(call.id)
                user_states[user_id] = {'action': 'withdraw_amount'}
                bot.send_message(user_id, f"💰 Ваш баланс: {user_data['balance']:.2f} ₽\n\nВведите сумму вывода (минимум {min_withdrawal} ₽):", reply_markup=create_cancel_markup())
            else:
                bot.answer_callback_query(call.id, f"❌ Недостаточно средств. Мин. сумма: {min_withdrawal} ₽", show_alert=True)
        withdraw_action(dummy_message)

    elif call.data == "show_rules":
        bot.answer_callback_query(call.id)
        rules_text = get_bot_setting('rules_text') or "Правила не установлены администратором."
        bot.send_message(user_id, f"📜 *Правила бота*\n\n{escape(rules_text)}", parse_mode="HTML")

    elif call.data.startswith("top_"):
        bot.answer_callback_query(call.id)
        period = 'day' if call.data == "top_day" else "all"
        top_referrers = get_top_referrers(period)
        title = "🏆 Топ рефералов за " + ("сегодня" if period == 'day' else "все время")
        text = f"{title}\n\n" + ('\n'.join([f"{i}. ID `{u['referrer_l1_id']}` - *{u['ref_count']}* реф." for i, u in enumerate(top_referrers, 1)]) or "Пока что здесь пусто.")
        try: bot.edit_message_text(text, user_id, call.message.message_id, reply_markup=create_inline_menu(), parse_mode="Markdown")
        except: pass

def process_state_input(message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    if not state: return

    if message.text == "❌ Отмена":
        del user_states[user_id]
        bot.send_message(user_id, "Действие отменено.", reply_markup=create_main_menu(user_id))
        if user_id in SETTINGS.get('admins', []): bot.send_message(user_id, "👑 Админ-меню:", reply_markup=create_admin_menu())
        return

    action = state.get('action')
    
    if action == 'admin_edit_rules':
        new_rules = message.text
        set_bot_setting('rules_text', new_rules)
        del user_states[user_id]
        bot.send_message(user_id, "✅ Правила успешно обновлены!", reply_markup=create_main_menu(user_id))
        bot.send_message(user_id, "👑 Админ-меню:", reply_markup=create_admin_menu())
        
    elif action == 'withdraw_amount':
        try:
            amount = float(message.text.replace(',', '.'))
            user_data = get_user(user_id)
            if not user_data: return
            if amount < float(SETTINGS.get('withdrawal_limit', 0)) or amount > user_data['balance']:
                bot.send_message(user_id, f"❌ Неверная сумма. Ваш баланс: {user_data['balance']:.2f} ₽. Попробуйте еще раз.")
                return
            user_states[user_id].update({'action': 'withdraw_details', 'amount': amount})
            bot.send_message(user_id, f"Введите {SETTINGS.get('withdrawal_method_text', 'реквизиты')}:")
        except (ValueError, TypeError): bot.send_message(user_id, "❌ Неверный формат. Введите число.")

    elif action == 'withdraw_details':
        amount, details = state['amount'], message.text.strip()
        with bot_db_lock:
            conn = db_connect()
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = balance - ?, frozen_balance = frozen_balance + ? WHERE user_id = ?", (amount, amount, user_id))
            cursor.execute("INSERT INTO withdrawals (user_id, amount, wallet_details, request_date) VALUES (?, ?, ?, ?)", (user_id, amount, details, datetime.now()))
            w_id = cursor.lastrowid
            conn.commit(); conn.close()
        del user_states[user_id]
        bot.send_message(user_id, "✅ Ваша заявка на вывод принята!", reply_markup=create_main_menu(user_id))
        
        username = message.from_user.username
        username_str = f"({format_username_md(username)})" if username else ""
        admin_text = f"🚨 Новая заявка на вывод №{w_id}\n\nПользователь: `{user_id}` {username_str}\nСумма: `{amount:.2f} ₽`\nРеквизиты: `{escape(details)}`"
        admin_markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("✅ Одобрить", callback_data=f"admin_wd_approve_{w_id}"), types.InlineKeyboardButton("❌ Отклонить", callback_data=f"admin_wd_decline_{w_id}"))
        for admin_id in SETTINGS.get('admins', []):
            try: bot.send_message(admin_id, admin_text, reply_markup=admin_markup, parse_mode="Markdown")
            except Exception as e: logging.error(f"Не удалось уведомить админа {admin_id}: {e}")

    elif action == 'set_gift_reward':
        try:
            new_reward = float(message.text.replace(',', '.'))
            if new_reward < 0: raise ValueError
            set_bot_setting('gift_reward', new_reward)
            del user_states[user_id]
            bot.send_message(user_id, f"✅ Награда за подарок установлена: {new_reward:.2f} ₽.", reply_markup=create_main_menu(user_id))
            bot.send_message(user_id, "👑 Админ-меню:", reply_markup=create_admin_menu())
        except ValueError: bot.send_message(user_id, "❌ Ошибка. Введите положительное число.")

    elif action == 'promo_enter_code':
        code = message.text.strip()
        with bot_db_lock:
            conn = db_connect()
            cursor = conn.cursor()
            promo = cursor.execute("SELECT id, reward, total_activations, used_activations FROM promocodes WHERE code = ?", (code,)).fetchone()
            if not promo: bot.send_message(user_id, "❌ Промокод не найден."); conn.close(); return
            
            promo_id, reward, total, used = promo
            if used >= total: bot.send_message(user_id, "❌ Этот промокод закончился."); conn.close(); return

            is_activated = cursor.execute("SELECT 1 FROM promocode_activations WHERE code_id = ? AND user_id = ?", (promo_id, user_id)).fetchone()
            if is_activated: bot.send_message(user_id, "❌ Вы уже использовали этот промокод."); conn.close(); return

            cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (reward, user_id))
            cursor.execute("UPDATE promocodes SET used_activations = used_activations + 1 WHERE id = ?", (promo_id,))
            cursor.execute("INSERT INTO promocode_activations (code_id, user_id) VALUES (?, ?)", (promo_id, user_id))
            conn.commit()
            conn.close()
        del user_states[user_id]
        bot.send_message(user_id, f"✅ Промокод успешно активирован! Вам начислено {reward:.2f} ₽.", reply_markup=create_main_menu(user_id))

    elif action == 'promo_create_name':
        user_states[user_id].update({'action': 'promo_create_reward', 'name': message.text.strip()})
        bot.send_message(user_id, "Шаг 2/3: Введите сумму награды (число):")
    elif action == 'promo_create_reward':
        try:
            reward = float(message.text.replace(',', '.'))
            user_states[user_id].update({'action': 'promo_create_activations', 'reward': reward})
            bot.send_message(user_id, "Шаг 3/3: Введите количество активаций (число):")
        except ValueError: bot.send_message(user_id, "❌ Введите число.")
    elif action == 'promo_create_activations':
        try:
            activations = int(message.text)
            name, reward = state['name'], state['reward']
            db_query("INSERT INTO promocodes (code, reward, total_activations) VALUES (?, ?, ?)", (name, reward, activations), commit=True)
            del user_states[user_id]
            bot.send_message(user_id, f"✅ Промокод `{name}` создан!", parse_mode="Markdown", reply_markup=create_main_menu(user_id))
            bot.send_message(user_id, "👑 Админ-меню:", reply_markup=create_admin_menu())
        except sqlite3.IntegrityError: bot.send_message(user_id, "❌ Промокод с таким названием уже существует.")
        except ValueError: bot.send_message(user_id, "❌ Введите целое число.")

    elif action == 'add_op_channel':
        channel_username = message.text.strip()
        if not channel_username.startswith('@'):
            bot.send_message(user_id, "❌ Неверный формат. Юзернейм должен начинаться с @."); return
        try:
            member = bot.get_chat_member(channel_username, bot.get_me().id)
            if member.status != 'administrator':
                bot.send_message(user_id, f"❌ Бот не является администратором в канале {channel_username}. Сначала выдайте права.")
                return
        except Exception as e:
            bot.send_message(user_id, f"❌ Не удалось проверить статус в канале {channel_username}. Ошибка: {e}")
            return
        
        db_query("INSERT OR IGNORE INTO op_channels (channel_username) VALUES (?)", (channel_username,), commit=True)
        del user_states[user_id]
        bot.send_message(user_id, f"✅ Канал {channel_username} добавлен в ОП.", reply_markup=create_main_menu(user_id))
        bot.send_message(user_id, "👑 Админ-меню:", reply_markup=create_admin_menu())
    
    elif action == 'get_referrals_input':
        logging.info(f"[ADMIN] Админ {user_id} ищет рефералов для '{message.text}'")
        target_user = get_user_by_id_or_username(message.text)
        del user_states[user_id]
        if not target_user: 
            bot.send_message(user_id, "❌ Пользователь не найден.", reply_markup=create_main_menu(user_id))
        else: 
            show_user_referrals(user_id, call=None, target_user_id=target_user['user_id'], page=1)
        bot.send_message(user_id, "👑 Админ-меню:", reply_markup=create_admin_menu())
    
    elif action == 'message_user_input':
        target_user = get_user_by_id_or_username(message.text)
        if not target_user:
            del user_states[user_id]
            bot.send_message(user_id, "❌ Пользователь не найден.", reply_markup=create_main_menu(user_id))
            bot.send_message(user_id, "👑 Админ-меню:", reply_markup=create_admin_menu())
        else:
            user_states[user_id] = {'action': 'message_user_text', 'target_user_id': target_user['user_id']}
            bot.send_message(user_id, f"Введите текст сообщения для `{target_user['user_id']}`:", parse_mode="Markdown")
    
    elif action == 'message_user_text':
        target_user_id = state['target_user_id']
        del user_states[user_id]
        try:
            bot.send_message(target_user_id, f"Сообщение от администратора:\n\n{message.text}")
            bot.send_message(user_id, "✅ Сообщение успешно отправлено.")
        except Exception as e:
            bot.send_message(user_id, f"❌ Не удалось отправить сообщение: {e}")
        bot.send_message(user_id, "👑 Админ-меню:", reply_markup=create_admin_menu())
    
    elif action == 'ban_user_input':
        target_user = get_user_by_id_or_username(message.text)
        del user_states[user_id]
        if not target_user:
            bot.send_message(user_id, "❌ Пользователь не найден.", reply_markup=create_main_menu(user_id))
        else:
            target_id = target_user['user_id']
            new_status = 1 if not target_user['is_banned'] else 0
            db_query("UPDATE users SET is_banned = ? WHERE user_id = ?", (new_status, target_id), commit=True)
            status_text = "забанен" if new_status == 1 else "разбанен"
            bot.send_message(user_id, f"✅ Пользователь `{target_id}` успешно {status_text}.", parse_mode="Markdown")
        bot.send_message(user_id, "👑 Админ-меню:", reply_markup=create_admin_menu())

    elif action == 'broadcast_content':
        user_states[user_id] = {'action': 'broadcast_button', 'content_message': message}
        bot.send_message(user_id, "Шаг 2/3: Введите кнопку в формате `Текст#https://ссылка` или отправьте `-`, если кнопка не нужна.", parse_mode="Markdown")
    
    elif action == 'broadcast_button':
        markup = None
        if message.text != "-":
            parts = message.text.split('#', 1)
            if len(parts) == 2 and parts[1].strip().startswith('https://'):
                markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton(parts[0].strip(), url=parts[1].strip()))
            else:
                bot.send_message(user_id, "❌ Неверный формат кнопки. Попробуйте снова.")
                return
        
        del user_states[user_id]
        
        bot.copy_message(user_id, user_id, state['content_message'].message_id, reply_markup=markup)
        msg = bot.send_message(user_id, "✅ Рассылка готова к отправке. Напишите `ДА` для подтверждения.", reply_markup=create_cancel_markup(), parse_mode="Markdown")
        bot.register_next_step_handler(msg, confirm_broadcast, state['content_message'], markup)

def confirm_broadcast(message, content_message, markup):
    user_id = message.from_user.id
    if message.text.lower() != 'да':
        bot.send_message(user_id, "Рассылка отменена.", reply_markup=create_main_menu(user_id))
        bot.send_message(user_id, "👑 Админ-меню:", reply_markup=create_admin_menu())
        return

    bot.send_message(user_id, "🚀 Рассылка запущена...", reply_markup=create_main_menu(user_id))
    threading.Thread(target=run_broadcast, args=(user_id, content_message, markup), daemon=True).start()

def handle_admin_callbacks(call):
    admin_id = call.from_user.id
    if admin_id not in SETTINGS.get('admins', []): return
    
    if user_states.get(admin_id):
        del user_states[admin_id]

    bot.answer_callback_query(call.id)
    data = call.data.split('_')
    action = "_".join(data[1:])

    if action.startswith("promo_"): handle_promo_callbacks(call, action)
    elif action.startswith("settings_"): handle_settings_callbacks(call, action)
    elif action.startswith("wd_"): handle_withdrawal_callbacks(call, action)
    elif action.startswith("op_"): handle_op_callbacks(call, action)
    elif action in ["back_to_menu", "back"]:
        bot.edit_message_text("👑 Админ-меню:", admin_id, call.message.message_id, reply_markup=create_admin_menu())
    elif action.startswith("refpage_"):
        target_user_id, page = int(data[2]), int(data[3]); show_user_referrals(admin_id, call, target_user_id, page)
    else:
        try: bot.edit_message_reply_markup(admin_id, call.message.message_id, reply_markup=None)
        except: pass
        cancel_markup = create_cancel_markup()
        if action == "broadcast": user_states[admin_id] = {'action': 'broadcast_content'}; bot.send_message(admin_id, "Шаг 1/3: Отправьте контент для рассылки.", reply_markup=cancel_markup)
        elif action == "get_referrals": user_states[admin_id] = {'action': 'get_referrals_input'}; bot.send_message(admin_id, "Введите ID или юзернейм пользователя.", reply_markup=cancel_markup)
        elif action == "message_user": user_states[admin_id] = {'action': 'message_user_input'}; bot.send_message(admin_id, "Введите ID или юзернейм пользователя.", reply_markup=cancel_markup)
        elif action == "ban_user": user_states[admin_id] = {'action': 'ban_user_input'}; bot.send_message(admin_id, "Введите ID или юзернейм для бана/разбана.", reply_markup=cancel_markup)
        elif action == "rules_edit": user_states[admin_id] = {'action': 'admin_edit_rules'}; bot.send_message(admin_id, "Введите новый текст правил:", reply_markup=cancel_markup)

def handle_promo_callbacks(call, action):
    admin_id = call.from_user.id
    if action == "promo_menu":
        markup = types.InlineKeyboardMarkup(row_width=2).add(types.InlineKeyboardButton("➕ Создать", callback_data="admin_promo_create"), types.InlineKeyboardButton("📋 Список", callback_data="admin_promo_list"), types.InlineKeyboardButton("🗑️ Удалить", callback_data="admin_promo_delete_menu"), types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
        bot.edit_message_text("🎁 Управление промокодами:", admin_id, call.message.message_id, reply_markup=markup)
    elif action == "promo_create": user_states[admin_id] = {'action': 'promo_create_name'}; bot.send_message(admin_id, "Шаг 1/3: Введите название промокода:", reply_markup=create_cancel_markup())
    elif action == "promo_list":
        promos = db_query("SELECT * FROM promocodes ORDER BY id DESC")
        text = "📋 *Список промокодов:*\n\n" + ('\n'.join([f"`{p['code']}` | {p['reward']:.2f}₽ | {p['used_activations']}/{p['total_activations']} акт." for p in promos]) or "Пусто.")
        bot.edit_message_text(text, admin_id, call.message.message_id, reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_promo_menu")), parse_mode="Markdown")
    elif action.startswith("promo_delete"):
        if action == "promo_delete_menu":
            promos = db_query("SELECT id, code FROM promocodes")
            markup = types.InlineKeyboardMarkup()
            for p in promos: markup.add(types.InlineKeyboardButton(f"🗑️ {p['code']}", callback_data=f"admin_promo_delete_{p['id']}"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_promo_menu"))
            bot.edit_message_text("Выберите промокод для удаления:", admin_id, call.message.message_id, reply_markup=markup)
        else:
            promo_id = int(action.split('_')[-1])
            db_query("DELETE FROM promocodes WHERE id = ?", (promo_id,), commit=True)
            bot.answer_callback_query(call.id, "Промокод удален."); handle_promo_callbacks(call, "promo_delete_menu")

def handle_settings_callbacks(call, action):
    admin_id = call.from_user.id
    if action == "settings_menu":
        gift_reward = get_bot_setting('gift_reward', 1.0)
        markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton(f"🎁 Награда за подарок: {gift_reward}₽", callback_data="admin_settings_gift"), types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
        bot.edit_message_text("⚙️ Настройки бота:", admin_id, call.message.message_id, reply_markup=markup)
    elif action == "settings_gift":
        user_states[admin_id] = {'action': 'set_gift_reward'}; bot.send_message(admin_id, "Введите новую сумму награды:", reply_markup=create_cancel_markup())

def handle_op_callbacks(call, action):
    admin_id = call.from_user.id
    if action == "op_manage":
        channels = db_query("SELECT channel_username FROM op_channels")
        text = "📢 *Управление каналами для Обязательной Подписки:*\n\n" + ('\n'.join(f"`{ch['channel_username']}`" for ch in channels) if channels else "Список пуст.")
        markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("➕ Добавить канал", callback_data="admin_op_add"), types.InlineKeyboardButton("➖ Удалить канал", callback_data="admin_op_remove"), types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
        bot.edit_message_text(text, admin_id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    elif action == "op_add": user_states[admin_id] = {'action': 'add_op_channel'}; bot.send_message(admin_id, "Введите юзернейм канала (напр. @channel).", reply_markup=create_cancel_markup())
    elif action.startswith("op_delete_"):
        channel = call.data.split('_')[-1].strip()
        db_query("DELETE FROM op_channels WHERE channel_username = ?", (f'@{channel}',), commit=True)
        bot.answer_callback_query(call.id, f"Канал @{channel} удален."); handle_op_callbacks(call, "op_remove")
    elif action == "op_remove":
        channels = db_query("SELECT channel_username FROM op_channels")
        if not channels: return bot.answer_callback_query(call.id, "Список каналов пуст.", show_alert=True)
        markup = types.InlineKeyboardMarkup()
        for ch in channels: markup.add(types.InlineKeyboardButton(f"Удалить {ch['channel_username']}", callback_data=f"admin_op_delete_{ch['channel_username'].replace('@','')} "))
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_op_manage"))
        bot.edit_message_text("Выберите канал для удаления:", admin_id, call.message.message_id, reply_markup=markup)

def handle_withdrawal_callbacks(call, action):
    admin_id = call.from_user.id
    data = call.data.split('_')
    if action.startswith("wd_list_"):
        page_num = int(data[3]); show_withdrawal_requests(admin_id, call, page_num)
    elif action.startswith("wd_approve_") or action.startswith("wd_decline_"):
        act, w_id = data[2], int(data[3])
        process_withdrawal_action(call, admin_id, w_id, act)
        show_withdrawal_requests(admin_id, call, user_states.get(admin_id, {}).get('wd_page', 1))

def show_withdrawal_requests(admin_id, call, page):
    user_states[admin_id] = {'wd_page': page}; per_page = 5; offset = (page - 1) * per_page
    reqs = db_query("SELECT * FROM withdrawals WHERE status = 'pending' ORDER BY id DESC LIMIT ? OFFSET ?", (per_page, offset))
    total_res = db_query("SELECT COUNT(*) as count FROM withdrawals WHERE status = 'pending'", fetchone=True)
    total = total_res['count'] if total_res else 0
    if not reqs and page > 1: return show_withdrawal_requests(admin_id, call, page - 1)
    text = f"📋 *Заявки на вывод (Стр. {page}):*\n\n" + ("Новых заявок нет." if not reqs else "")
    markup = types.InlineKeyboardMarkup(row_width=2)
    for r in reqs:
        text += f"🆔 *{r['id']}:* `{r['amount']:.2f} ₽` от `{r['user_id']}`\n💳: `{escape(r['wallet_details'])}`\n---\n"
        markup.add(types.InlineKeyboardButton(f"✅ {r['id']}", callback_data=f"admin_wd_approve_{r['id']}"), types.InlineKeyboardButton(f"❌ {r['id']}", callback_data=f"admin_wd_decline_{r['id']}"))
    total_pages = (total + per_page - 1) // per_page if total > 0 else 1
    if total_pages > 1:
        nav = [b for b in [types.InlineKeyboardButton("⬅️", callback_data=f"admin_wd_list_{page-1}") if page > 1 else None,
                           types.InlineKeyboardButton(f"{page}/{total_pages}", callback_data="dummy"),
                           types.InlineKeyboardButton("➡️", callback_data=f"admin_wd_list_{page+1}") if page < total_pages else None] if b]
        markup.row(*nav)
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
    try: bot.edit_message_text(text, admin_id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    except telebot.apihelper.ApiTelegramException as e:
        if 'message is not modified' not in str(e): logging.warning(f"Ошибка при обновлении списка заявок: {e}")

def process_withdrawal_action(call, admin_id, w_id, action):
    wd_data = db_query("SELECT * FROM withdrawals WHERE id = ?", (w_id,), fetchone=True)
    if not wd_data or wd_data['status'] != 'pending': bot.answer_callback_query(call.id, "Заявка уже обработана.", show_alert=True); return
    user_id, amount = wd_data['user_id'], wd_data['amount']
    if action == "approve":
        db_query("UPDATE withdrawals SET status = 'approved' WHERE id = ?", (w_id,), commit=True)
        db_query("UPDATE users SET frozen_balance = frozen_balance - ? WHERE user_id = ?", (amount, user_id), commit=True)
        try: bot.send_message(user_id, f"✅ Ваша заявка на вывод {amount:.2f} ₽ одобрена!")
        except: pass
        payout_channel = SETTINGS.get('payout_channel')
        if payout_channel:
            try: bot.send_message(payout_channel, f"✅ Выплата!\nСумма: *{amount:.2f} ₽*", parse_mode="Markdown")
            except Exception as e: logging.error(f"Не удалось отправить в канал выплат {payout_channel}: {e}")
    elif action == "decline":
        db_query("UPDATE withdrawals SET status = 'declined' WHERE id = ?", (w_id,), commit=True)
        db_query("UPDATE users SET balance = balance + ?, frozen_balance = frozen_balance - ? WHERE user_id = ?", (amount, amount, user_id), commit=True)
        try: bot.send_message(user_id, f"❌ Ваша заявка на вывод {amount:.2f} ₽ отклонена. Средства возвращены.")
        except: pass

def show_user_referrals(admin_id, call, target_user_id, page):
    per_page = 10; offset = (page - 1) * per_page
    refs = db_query("SELECT user_id, username, registration_date FROM users WHERE referrer_l1_id = ? ORDER BY registration_date DESC LIMIT ? OFFSET ?", (target_user_id, per_page, offset))
    total_res = db_query("SELECT COUNT(*) as count FROM users WHERE referrer_l1_id = ?", (target_user_id,), fetchone=True)
    total = total_res['count'] if total_res else 0
    text = f"👥 *Рефералы пользователя `{target_user_id}` (Всего: {total})*\n\n"
    if not refs:
        text += "Нет рефералов."
    else:
        lines = []
        for r in refs:
            username_display = format_username_md(r['username']) if r['username'] else "N/A"
            lines.append(f"ID: `{r['user_id']}` ({username_display}) - {r['registration_date'].strftime('%d.%m.%Y')}")
        text += "\n".join(lines)
    total_pages = (total + per_page - 1) // per_page if total > 0 else 1
    markup = types.InlineKeyboardMarkup()
    if total_pages > 1:
        nav = [b for b in [types.InlineKeyboardButton("⬅️", callback_data=f"admin_refpage_{target_user_id}_{page-1}") if page > 1 else None,
                           types.InlineKeyboardButton(f"{page}/{total_pages}", callback_data="dummy"),
                           types.InlineKeyboardButton("➡️", callback_data=f"admin_refpage_{target_user_id}_{page+1}") if page < total_pages else None] if b]
        markup.row(*nav)
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
    try:
        if call: bot.edit_message_text(text, admin_id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        else: bot.send_message(admin_id, text, reply_markup=markup, parse_mode="Markdown")
    except telebot.apihelper.ApiTelegramException as e:
         if 'message is not modified' not in str(e): logging.warning(f"Ошибка при показе рефералов: {e}")

def run_broadcast(admin_id, content_message, markup):
    users = db_query("SELECT user_id FROM users WHERE is_active = 1 AND is_banned = 0")
    if not users: bot.send_message(admin_id, "Нет активных пользователей для рассылки."); return
    success, fail, total = 0, 0, len(users)
    for i, user in enumerate(users):
        try: bot.copy_message(user['user_id'], admin_id, content_message.message_id, reply_markup=markup); success += 1
        except Exception: fail += 1
        time.sleep(0.05)
        if (i+1) % 50 == 0:
            try: bot.send_message(admin_id, f"Рассылка... Отправлено {i+1}/{total}")
            except: pass
    bot.send_message(admin_id, f"✅ Рассылка завершена!\n\nУспешно: {success}\nОшибок: {fail}")
    bot.send_message(admin_id, "👑 Админ-меню:", reply_markup=create_admin_menu())

# =================================================================================
# ----------------------------------- ЗАПУСК --------------------------------------
# =================================================================================

if __name__ == '__main__':
    try:
        bot_info = bot.get_me()
        logging.info(f"Бот #{BOT_ID} @{bot_info.username} запущен...")
    except Exception as e:
        logging.critical(f"Критическая ошибка при запуске бота #{BOT_ID}: {e}")
        sys.exit(1)

    if async_loop:
        threading.Thread(target=async_loop.run_forever, daemon=True).start()
        logging.info("Asyncio event loop для фоновых задач запущен.")
    
    while True:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=10, skip_pending=True)
        except Exception as e:
            logging.critical(f"Критическая ошибка в главном цикле бота #{BOT_ID}: {e}")
            traceback.print_exc()
            time.sleep(15)
            logging.info("Перезапуск бота...")
