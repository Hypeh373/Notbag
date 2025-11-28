# -*- coding: utf-8 -*-
import telebot
from telebot import types
import sqlite3
import logging
from datetime import datetime, timedelta
import threading
import time
import re
import random
import sys
import os
import json
from html import escape
import traceback

# --- ИНТЕГРАЦИЯ FLYER API ---
try:
    import asyncio
    from flyerapi import Flyer, APIError as FlyerAPIError
    from functools import wraps
    FLYER_AVAILABLE = True
except ImportError:
    FLYER_AVAILABLE = False
    def wraps(f): return f
    class Flyer: pass
    class FlyerAPIError(Exception): pass
# -----------------------------

# =================================================================================
# --------------------------- ЗАГРУЗКА КОНФИГУРАЦИИ -------------------------------
# =================================================================================

CONSTRUCTOR_BOT_USERNAME = "GrillCreate_bot"
SHOW_BRANDING = os.environ.get('CREATOR_BRANDING') == 'true'

if len(sys.argv) < 2 or not sys.argv[1].isdigit():
    print(f"ОШИБКА: Запустите скрипт с ID бота в качестве аргумента. Пример: python {sys.argv[0]} 123")
    sys.exit(1)

BOT_ID = int(sys.argv[1])
CREATOR_DB_NAME = 'creator_data2.db'
creator_db_lock = threading.Lock()

def load_config():
    """Загружает конфигурацию для этого бота из БД конструктора."""
    try:
        with creator_db_lock:
            conn = sqlite3.connect(f'file:{CREATOR_DB_NAME}?mode=ro', uri=True, timeout=10)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM bots WHERE id = ?", (BOT_ID,))
            config = cursor.fetchone()
            conn.close()
        
        if config:
            return dict(config)
        else:
            logging.critical(f"ОШИБКА: Конфигурация для бота с ID {BOT_ID} не найдена в {CREATOR_DB_NAME}")
            sys.exit(1)
    except sqlite3.Error as e:
        logging.critical(f"ОШИБКА: Не удалось прочитать базу данных конструктора {CREATOR_DB_NAME}. Ошибка: {e}")
        sys.exit(1)

config = load_config()

TOKEN = config.get('bot_token')
if not TOKEN:
    logging.critical(f"ОШИБКА: Для бота ID {BOT_ID} не установлен токен. Запуск невозможен.")
    sys.exit(1)

ADMIN_ID = config.get('owner_id')
try:
    admins_json = config.get('admins')
    if admins_json:
        ADMINS_LIST = json.loads(admins_json)
    else:
        ADMINS_LIST = []
    
    if ADMIN_ID and ADMIN_ID not in ADMINS_LIST:
        ADMINS_LIST.append(ADMIN_ID)
except (json.JSONDecodeError, TypeError):
    ADMINS_LIST = [ADMIN_ID] if ADMIN_ID else []
    
DB_NAME = f'dbs/bot_{BOT_ID}_stars_data.db'

PAYMENTS_CHANNEL = config.get('stars_payments_channel') or '@канал_не_указан'
SUPPORT_CHAT = config.get('stars_support_chat') or 'https://t.me/ссылка_не_указана'

FLYER_API_KEY = config.get('stars_flyer_api_key') or os.environ.get('FLYER_API_KEY')
FLYER_ENABLED = config.get('stars_op_enabled', False)

WELCOME_BONUS = float(config.get('stars_welcome_bonus', 0))
DAILY_BONUS_REWARD = float(config.get('stars_daily_bonus', 1))
DAILY_BONUS_COOLDOWN_HOURS = int(config.get('stars_daily_cooldown', 24))
REFERRAL_BONUS_REFERRER = float(config.get('stars_ref_bonus_referrer', 15))
REFERRAL_BONUS_NEW_USER = float(config.get('stars_ref_bonus_new_user', 10))

GIFTS = {
    'teddy_bear': {'name': '🧸', 'cost': 15}, 'heart_box':  {'name': '💝', 'cost': 15},
    'rose':       {'name': '🌹', 'cost': 25}, 'gift_box':   {'name': '🎁', 'cost': 25},
    'champagne':  {'name': '🍾', 'cost': 50}, 'bouquet':    {'name': '💐', 'cost': 50},
    'rocket':     {'name': '🚀', 'cost': 50}, 'cake':       {'name': '🎂', 'cost': 50},
    'trophy':     {'name': '🏆', 'cost': 100}, 'ring':       {'name': '💍', 'cost': 100},
    'diamond':    {'name': '💎', 'cost': 100},
    'tg_premium_6m': {'name': 'Telegram Premium 6мес.', 'cost': 1700, 'full_width': True},
}

# =================================================================================
# --------------------------- НАСТРОЙКА FLYER API ---------------------------------
# =================================================================================

flyer = None
async_loop = None

if FLYER_AVAILABLE:
    async_loop = asyncio.new_event_loop()
    def run_async_from_sync(coro):
        """Безопасно запускает корутину из синхронного кода в отдельном потоке."""
        if not async_loop or not async_loop.is_running():
            logging.error("Asyncio-цикл не запущен. Невозможно выполнить async-задачу.")
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
            logging.info(f"[BotID:{BOT_ID}] Flyer API успешно инициализирован.")
        except Exception as e:
            logging.error(f"[BotID:{BOT_ID}] ОШИБКА инициализации Flyer API: {e}", exc_info=True)
    else:
        logging.warning(f"[BotID:{BOT_ID}] Flyer API не будет использоваться (Enabled: {FLYER_ENABLED}, KeySet: {bool(FLYER_API_KEY)})")

else:
    logging.warning(f"[BotID:{BOT_ID}] Библиотека flyerapi не найдена. Async-проверки полностью отключены.")
    def run_async_from_sync(coro):
        coro.close() 
        return True 


def get_admin_op_tasks(user_id):
    admin_tasks = []
    try:
        with creator_db_lock:
            conn_creator = sqlite3.connect(f'file:{CREATOR_DB_NAME}?mode=ro', uri=True, timeout=15)
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
    try:
        with creator_db_lock:
            conn_creator = sqlite3.connect(CREATOR_DB_NAME, timeout=15)
            cursor = conn_creator.cursor()
            cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (reward, owner_id))
            cursor.execute("INSERT OR IGNORE INTO user_completed_admin_tasks (user_id, task_id) VALUES (?, ?)", (user_id, task_id))
            conn_creator.commit()
            conn_creator.close()
        logging.info(f"[ADMIN_OP_CREDIT] Владельцу {owner_id} начислено {reward} ₽ за задание #{task_id} от юзера {user_id}.")
    except Exception as e:
        logging.error(f"Критическая ошибка в credit_owner_for_admin_op: {e}", exc_info=True)


def get_creator_setting(key):
    with creator_db_lock:
        try:
            conn = sqlite3.connect(f'file:{CREATOR_DB_NAME}?mode=ro', uri=True, timeout=15)
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else None
        except sqlite3.Error as e:
            logging.error(f"Ошибка чтения настройки '{key}' из главной БД: {e}")
            return None

def credit_owner_for_task(owner_id: int, amount: float, user_id: int, task: dict):
    task_signature = task.get('signature', 'unknown_signature')
    task_type = task.get('task')
    
    with creator_db_lock:
        try:
            conn_creator = sqlite3.connect(CREATOR_DB_NAME, timeout=15)
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
                    logging.info(f"[FLYER_CREDIT_HOLD] [BotID:{BOT_ID}] Начислено {amount:.4f} руб. НА УДЕРЖАНИЕ владельцу {owner_id} за подписку от {user_id}")
                except sqlite3.IntegrityError:
                     logging.warning(f"[BotID:{BOT_ID}] Попытка повторно добавить задачу {task_signature} в очередь. Пропускаем.")
            else:
                cursor_creator.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, owner_id))
                conn_creator.commit()
                logging.info(f"[FLYER_CREDIT_DIRECT] [BotID:{BOT_ID}] Начислено {amount:.4f} руб. НАПРЯМУЮ владельцу {owner_id} за задание '{task_signature}' от {user_id}")
            conn_creator.close()
        except Exception as e:
            logging.error(f"[BotID:{BOT_ID}] Критическая ошибка в credit_owner_for_task для владельца {owner_id}: {e}")
            traceback.print_exc()

async def is_flyer_check_passed_async(user_id: int):
    # Если Flyer API не инициализирован (отключен в настройках или ключ не задан),
    # то никакие ОП (ни Flyer, ни админские) не показываем. Доступ разрешен.
    if not flyer:
        logging.info(f"[BotID:{BOT_ID}] [OP_CHECK] Flyer API неактивен. Проверка ОП пропущена для user_id: {user_id}")
        return True

    # 1. Получаем "Мои ОП" от админа конструктора.
    # Эта логика теперь будет работать, только если flyer активен (проверка выше).
    admin_op_tasks = get_admin_op_tasks(user_id)
    
    # 2. Логика кеширования и блокировки заданий Flyer
    conn = sqlite3.connect(DB_NAME, check_same_thread=False, timeout=10)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT flyer_tasks_json, flyer_tasks_timestamp, 
               flyer_locked_tasks_json, flyer_locked_timestamp,
               rewarded_flyer_tasks
        FROM users WHERE user_id = ?
    """, (user_id,))
    user_flyer_data = cursor.fetchone()

    now = datetime.now()
    
    rewarded_tasks_json = user_flyer_data[4] if user_flyer_data else '[]'
    rewarded_signatures = set(json.loads(rewarded_tasks_json or '[]'))

    locked_tasks = []
    if user_flyer_data and user_flyer_data[2] and user_flyer_data[3]:
        try:
            locked_tasks_json, locked_timestamp_str = user_flyer_data[2], user_flyer_data[3]
            locked_timestamp = datetime.fromisoformat(locked_timestamp_str)
            if now - locked_timestamp < timedelta(hours=24):
                locked_tasks = json.loads(locked_tasks_json or '[]')
        except (json.JSONDecodeError, ValueError) as e:
            logging.warning(f"[Flyer] [ID: {user_id}] Не удалось прочитать locked_tasks_json: {e}")

    new_tasks = []
    should_fetch_new = True
    if user_flyer_data and user_flyer_data[0] and user_flyer_data[1]:
        try:
            tasks_json, timestamp_str = user_flyer_data[0], user_flyer_data[1]
            timestamp = datetime.fromisoformat(timestamp_str)
            if now - timestamp < timedelta(minutes=10):
                should_fetch_new = False
                new_tasks = json.loads(tasks_json or '[]')
        except (json.JSONDecodeError, ValueError) as e:
             logging.warning(f"[Flyer] [ID: {user_id}] Не удалось прочитать flyer_tasks_json из кэша: {e}")
    
    if should_fetch_new:
        try:
            fetched_tasks = await flyer.get_tasks(user_id=user_id, limit=5) or []
            cursor.execute("UPDATE users SET flyer_tasks_json = ?, flyer_tasks_timestamp = ? WHERE user_id = ?",
                           (json.dumps(fetched_tasks), now.isoformat(), user_id))
            conn.commit()
            new_tasks = fetched_tasks
        except Exception as e:
            logging.error(f"[Flyer][ID: {user_id}] Ошибка при получении новых заданий: {e}")
            if user_flyer_data and user_flyer_data[0]: 
                try: new_tasks = json.loads(user_flyer_data[0] or '[]')
                except: new_tasks = []
    
    # 3. Объединяем ВСЕ типы заданий
    all_tasks_dict = {task['signature']: task for task in locked_tasks if 'signature' in task}
    all_tasks_dict.update({task['signature']: task for task in new_tasks if 'signature' in task})
    all_tasks_dict.update({task['signature']: task for task in admin_op_tasks if 'signature' in task})
    
    all_tasks_to_check = list(all_tasks_dict.values())

    if not all_tasks_to_check:
        conn.close()
        return True

    # 4. Проверяем статусы
    FLYER_INCOMPLETE_STATUSES = ('incomplete', 'abort')
    failed_tasks = []
    completed_flyer_tasks_now = []

    for task in all_tasks_to_check:
        if task['signature'].startswith('admin_op_'):
            failed_tasks.append(task)
            continue
        
        try:
            status = await flyer.check_task(user_id=user_id, signature=task['signature'])
            if status in FLYER_INCOMPLETE_STATUSES:
                failed_tasks.append(task)
            else:
                completed_flyer_tasks_now.append(task)
                if task['signature'] not in rewarded_signatures:
                    sub_reward_str = get_creator_setting('stars_sub_reward') or "1.0"
                    reward = float(sub_reward_str)
                    credit_owner_for_task(ADMIN_ID, reward, user_id, task)
                    rewarded_signatures.add(task['signature'])
        except Exception as e:
            logging.error(f"[Flyer][ID: {user_id}] Ошибка при проверке/начислении за задание {task.get('signature')}: {e}")

    cursor.execute("UPDATE users SET rewarded_flyer_tasks = ? WHERE user_id = ?",
                   (json.dumps(list(rewarded_signatures)), user_id))
    conn.commit()

    if failed_tasks:
        show_task_message(user_id, failed_tasks)
        conn.close()
        return False

    # 5. Блокируем выполненные Flyer-задания
    cursor.execute("UPDATE users SET flyer_locked_tasks_json = ?, flyer_locked_timestamp = ? WHERE user_id = ?",
                   (json.dumps(completed_flyer_tasks_now), now.isoformat(), user_id))
    conn.commit()
    conn.close()
    
    return True

def show_task_message(user_id: int, tasks):
    if not tasks: return
    try:
        markup = types.InlineKeyboardMarkup(row_width=2)
        task_buttons = [types.InlineKeyboardButton(f"➕ {t.get('task','Задание').capitalize()}", url=link) for t in tasks for link in t.get('links',[])]
        markup.add(*task_buttons)
        markup.add(types.InlineKeyboardButton('☑️ Проверить', callback_data='check_all_tasks'))
        bot.send_message(user_id, "<b>Для продолжения, пожалуйста, выполните спонсорские задания:</b>", reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logging.error(f"[show_task_message] Ошибка при показе заданий: {e}")

def require_flyer_check(func):
    @wraps(func)
    def wrapper(message_or_call, *args, **kwargs):
        is_callback = isinstance(message_or_call, types.CallbackQuery)
        user = message_or_call.from_user
        
        unsubscribed = check_all_required_subscriptions(user.id)
        if unsubscribed:
            if is_callback:
                bot.answer_callback_query(message_or_call.id, "Сначала подпишитесь на каналы!", show_alert=True)
            bot.send_message(user.id, "<b>👋 Для доступа к боту, подпишитесь на наши каналы:</b>", reply_markup=get_subscription_markup(unsubscribed), parse_mode='HTML')
            return

        flyer_check_result = run_async_from_sync(is_flyer_check_passed_async(user.id))
        
        if flyer_check_result is False:
            if is_callback:
                bot.answer_callback_query(message_or_call.id, "Сначала выполните спонсорские задания!", show_alert=True)
            return
        
        if flyer_check_result is None:
            if is_callback:
                bot.answer_callback_query(message_or_call.id, "Техническая ошибка проверки. Попробуйте ещё раз.", show_alert=True)
            else:
                bot.send_message(user.id, "Техническая ошибка проверки. Попробуйте ещё раз.")
            return

        return func(message_or_call, *args, **kwargs)
    return wrapper

# =================================================================================
# --------------------------- ОСНОВНОЙ КОД БОТА -----------------------------------
# =================================================================================

BOT_START_TIME = datetime.now()
logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [BotID:{BOT_ID}] - %(levelname)s - %(message)s")
bot = telebot.TeleBot(TOKEN)
user_states = {}
last_check_sub_time = {}

try:
    bot_info = bot.get_me()
except Exception as e:
    logging.critical(f"Неверный токен. Ошибка: {e}")
    sys.exit(1)

def init_db():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False, timeout=10)
    cursor = conn.cursor()
    cursor.execute('PRAGMA journal_mode=WAL;')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
            balance REAL DEFAULT 0, registered_at TEXT, referred_by INTEGER,
            referral_count INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0,
            referrer_bonus_awarded INTEGER DEFAULT 0,
            last_daily_bonus_claim TEXT,
            sub_reward_awarded INTEGER DEFAULT 0,
            flyer_tasks_json TEXT,         
            flyer_tasks_timestamp TEXT,    
            flyer_locked_tasks_json TEXT,  
            flyer_locked_timestamp TEXT,
            rewarded_flyer_tasks TEXT DEFAULT '[]'
        )
    ''')
    table_info = cursor.execute("PRAGMA table_info(users)").fetchall()
    column_names = [info[1] for info in table_info]
    
    if 'flyer_locked_tasks_json' not in column_names:
        cursor.execute("ALTER TABLE users ADD COLUMN flyer_tasks_json TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN flyer_tasks_timestamp TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN flyer_locked_tasks_json TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN flyer_locked_timestamp TEXT")
        logging.info(f"[BotID:{BOT_ID}] Колонки 'flyer_locked' успешно добавлены в БД.")
    
    if 'rewarded_flyer_tasks' not in column_names:
        cursor.execute("ALTER TABLE users ADD COLUMN rewarded_flyer_tasks TEXT DEFAULT '[]'")
        logging.info(f"[BotID:{BOT_ID}] Колонка 'rewarded_flyer_tasks' успешно добавлена в БД.")
        
    if 'is_banned' not in column_names:
        cursor.execute("ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0")
        logging.info(f"[BotID:{BOT_ID}] Колонка 'is_banned' успешно добавлена в БД.")

    cursor.execute('CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY)')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, gift_name TEXT, 
            amount_stars REAL, status TEXT DEFAULT 'pending', created_at TEXT,
            recipient_id INTEGER, recipient_info TEXT
        )
    ''')
    cursor.execute('CREATE TABLE IF NOT EXISTS required_channels (channel_username TEXT PRIMARY KEY)')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY, reward REAL, total_uses INTEGER, used_count INTEGER DEFAULT 0
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS promo_activations (user_id INTEGER, code TEXT, PRIMARY KEY (user_id, code))
    ''')
    cursor.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)')
    conn.commit()
    conn.close()

def add_main_admin():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False, timeout=10)
    cursor = conn.cursor()
    for admin in ADMINS_LIST:
        cursor.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (admin,))
    conn.commit()
    conn.close()

init_db()
add_main_admin()

def get_required_channels():
    try:
        conn = sqlite3.connect(DB_NAME, check_same_thread=False, timeout=10)
        cursor = conn.cursor()
        cursor.execute('SELECT channel_username FROM required_channels')
        channels = [row[0] for row in cursor.fetchall()]
        conn.close()
        return channels
    except Exception as e:
        logging.error(f"Ошибка получения списка каналов из БД: {e}")
        return []

def check_all_required_subscriptions(user_id):
    required = get_required_channels()
    if not required:
        return []
        
    unsubscribed = []
    for channel in required:
        try:
            member = bot.get_chat_member(channel, user_id)
            if member.status not in ['member', 'administrator', 'creator']:
                unsubscribed.append(channel)
        except telebot.apihelper.ApiTelegramException as e:
            logging.warning(f"Не удалось проверить подписку на {channel} для {user_id}. Ошибка: {e}")
            unsubscribed.append(channel)
        except Exception as e:
            logging.error(f"Критическая ошибка при проверке подписки на {channel}: {e}")
            unsubscribed.append(channel)
    return unsubscribed

def is_admin(user_id):
    return user_id in ADMINS_LIST

def get_main_menu_keyboard(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("⭐ Личный кабинет"), types.KeyboardButton("👥 Рефералы"))
    markup.add(types.KeyboardButton("🎁 Подарок"), types.KeyboardButton("📊 О боте"))
    if is_admin(user_id):
        markup.add(types.KeyboardButton("👑 Админ-панель"))
    return markup

def get_admin_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("⚙️ Настройки каналов"), types.KeyboardButton("📣 Рассылка"))
    markup.add(types.KeyboardButton("📊 Статистика пользователей"), types.KeyboardButton("🚫 Бан/Разбан"))
    markup.add(types.KeyboardButton("📜 Рефералы пользователя"), types.KeyboardButton("💬 Написать пользователю"))
    markup.add(types.KeyboardButton("🎁 Промокоды"), types.KeyboardButton("📜 Правила"))
    markup.add(types.KeyboardButton("◀️ Главное меню"))
    return markup

def get_cancel_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add("❌ Отмена")
    return markup
    
def get_subscription_markup(channels):
    markup = types.InlineKeyboardMarkup()
    for channel in channels:
        try:
            clean_channel = channel.replace('@', '')
            markup.add(types.InlineKeyboardButton(f"Подписаться на {channel}", url=f"https://t.me/{clean_channel}"))
        except:
            continue
    markup.add(types.InlineKeyboardButton("✅ Я подписался", callback_data="check_sub_after_join"))
    return markup
    
@bot.message_handler(commands=['start'])
def start_handler(message):
    user = message.from_user
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("SELECT is_banned FROM users WHERE user_id = ?", (user.id,))
    user_status = cursor.fetchone()

    if user_status and user_status[0] == 1:
        conn.close()
        bot.send_message(user.id, "<b>Вам запрещено пользоваться ботом.</b>", parse_mode='HTML')
        return

    cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user.id,))
    is_registered = cursor.fetchone()
    
    is_newly_registered = False
    if not is_registered:
        is_newly_registered = True
        referrer_id = None
        match = re.search(r'start ref(\d+)', message.text)
        if match:
            try:
                potential_referrer_id = int(match.group(1))
                if potential_referrer_id != user.id:
                    cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (potential_referrer_id,))
                    if cursor.fetchone(): referrer_id = potential_referrer_id
            except (ValueError, IndexError):
                pass
        initial_balance = WELCOME_BONUS + (REFERRAL_BONUS_NEW_USER if referrer_id else 0)
        cursor.execute(
            "INSERT INTO users (user_id, username, first_name, balance, registered_at, referred_by) VALUES (?, ?, ?, ?, ?, ?)",
            (user.id, user.username, user.first_name, initial_balance, datetime.now().isoformat(), referrer_id)
        )
        conn.commit()
    conn.close()
    
    unsubscribed_channels = check_all_required_subscriptions(user.id)
    if unsubscribed_channels:
        bot.send_message(user.id, "<b>👋 Для доступа к боту, подпишитесь на наши каналы:</b>", reply_markup=get_subscription_markup(unsubscribed_channels), disable_web_page_preview=True, parse_mode='HTML')
        return
    
    if run_async_from_sync(is_flyer_check_passed_async(user.id)) is False:
        return

    welcome_message = config.get('welcome_message', '👋 Добро пожаловать!')
    if SHOW_BRANDING:
        welcome_message += f"\n\nБот создан с помощью @{CONSTRUCTOR_BOT_USERNAME}"

    if is_newly_registered:
        handle_successful_subscription(user.id)
    
    bot.send_message(user.id, welcome_message, reply_markup=get_main_menu_keyboard(user.id), parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data == 'check_all_tasks')
def handle_check_tasks_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    bot.answer_callback_query(call.id, text="Проверяю...")

    admin_op_tasks_to_credit = get_admin_op_tasks(user_id)
    if admin_op_tasks_to_credit:
        logging.info(f"[CALLBACK_CHECK] {user_id} нажал проверку. Начисляю награды за {len(admin_op_tasks_to_credit)} заданий 'Мои ОП'.")
        for task in admin_op_tasks_to_credit:
            task_id_str = task['signature'].replace('admin_op_', '')
            if task_id_str.isdigit():
                credit_owner_for_admin_op(ADMIN_ID, user_id, int(task_id_str), task['reward'])
    
    if run_async_from_sync(is_flyer_check_passed_async(user_id)):
        try: bot.delete_message(call.message.chat.id, call.message.message_id)
        except: pass
        
        unsubscribed_channels = check_all_required_subscriptions(user_id)
        if unsubscribed_channels:
            bot.send_message(user_id, "<b>👋 Для доступа к боту, подпишитесь на наши каналы:</b>", reply_markup=get_subscription_markup(unsubscribed_channels), parse_mode='HTML')
            return

        bot.send_message(user_id, "🎉 <b>Спасибо! Доступ открыт.</b>", reply_markup=get_main_menu_keyboard(user_id), parse_mode='HTML')

def format_timedelta(td):
    days = td.days; hours, rem = divmod(td.seconds, 3600); minutes, _ = divmod(rem, 60)
    return f"{days}д {hours:02}:{minutes:02}"

@bot.message_handler(func=lambda message: message.text == "📊 О боте")
@require_flyer_check
def about_bot_handler(message):
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    last_24h_iso = (datetime.now() - timedelta(hours=24)).isoformat()
    cursor.execute("SELECT COUNT(*) FROM users WHERE registered_at >= ?", (last_24h_iso,))
    new_users_24h = cursor.fetchone()[0]
    cursor.execute("SELECT SUM(amount_stars) FROM withdrawals WHERE status = 'approved'")
    total_paid_out = cursor.fetchone()[0] or 0
    conn.close()
    
    uptime = datetime.now() - BOT_START_TIME
    
    text = (f"📊 <b>Статистика бота</b>\n\n"
            f"⏱️ <b>Аптайм:</b> <code>{format_timedelta(uptime)}</code>\n"
            f"👥 <b>Всего пользователей:</b> {total_users}\n"
            f"🆕 <b>Новых за 24ч:</b> {new_users_24h}\n"
            f"⭐ <b>Всего выплачено:</b> {total_paid_out} ⭐")
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    admin_btn = types.InlineKeyboardButton("🔥 Администратор", url=f"tg://user?id={ADMIN_ID}")
    chat_btn = types.InlineKeyboardButton("💬 Чат", url=SUPPORT_CHAT)
    payments_btn = types.InlineKeyboardButton("💰 Выплаты", url=f"https://t.me/{PAYMENTS_CHANNEL.replace('@','')}")
    rules_btn = types.InlineKeyboardButton("📜 Правила", callback_data="show_rules")
    bug_btn = types.InlineKeyboardButton("🐞 Нашёл баг?", url=f"tg://user?id={ADMIN_ID}")
    
    markup.add(admin_btn, chat_btn)
    markup.add(payments_btn, rules_btn)
    markup.add(bug_btn)

    if SHOW_BRANDING:
        creator_bot_btn = types.InlineKeyboardButton("Хочу такого же бота (free)", url=f"https://t.me/{CONSTRUCTOR_BOT_USERNAME}")
        markup.add(creator_bot_btn)
    
    bot.send_message(message.chat.id, text, reply_markup=markup, disable_web_page_preview=True, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data == "show_rules")
def show_rules_callback(call):
    bot.answer_callback_query(call.id)
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = 'rules_text'")
    result = cursor.fetchone()
    conn.close()
    
    if result and result[0]:
        rules_text = result[0]
    else:
        rules_text = "🚫 Запрещены мульти-аккаунты.\n📉 Запрещен некачественный трафик (накрутка)."
    
    bot.send_message(call.message.chat.id, f"📜 <b>Правила бота:</b>\n\n{escape(rules_text)}", parse_mode='HTML')

def handle_successful_subscription(user_id):
    conn_bot = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor_bot = conn_bot.cursor()
    cursor_bot.execute('SELECT referred_by, referrer_bonus_awarded FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor_bot.fetchone()
    
    if user_data:
        referrer_id, ref_bonus_awarded = user_data
        
        if referrer_id and not ref_bonus_awarded and REFERRAL_BONUS_REFERRER > 0:
            cursor_bot.execute('UPDATE users SET referral_count = referral_count + 1 WHERE user_id = ?', (referrer_id,))
            bonus_amount = REFERRAL_BONUS_REFERRER
            cursor_bot.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (bonus_amount, referrer_id))
            cursor_bot.execute('UPDATE users SET referrer_bonus_awarded = 1 WHERE user_id = ?', (user_id,))
            conn_bot.commit()
            try:
                bot.send_message(referrer_id, f"🎉 Ваш реферал подписался! Вам начислено <b>{bonus_amount} ⭐</b>.", parse_mode='HTML')
            except Exception as e:
                logging.warning(f"Не удалось уведомить реферера {referrer_id}: {e}")

    conn_bot.close()

@bot.message_handler(func=lambda message: message.text == "👑 Админ-панель")
def admin_panel_handler(message):
    if not is_admin(message.from_user.id): return
    bot.send_message(message.chat.id, "<b>Админ-панель</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML')

@bot.message_handler(func=lambda message: message.text == "📜 Правила")
def admin_rules_handler(message):
    if not is_admin(message.from_user.id): return
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = 'rules_text'")
    result = cursor.fetchone()
    conn.close()
    current_rules = result[0] if result and result[0] else "Не установлены."
    msg = bot.send_message(message.chat.id, f"Текущие правила:\n\n<code>{escape(current_rules)}</code>\n\nОтправьте новый текст правил. Он будет отображаться пользователям.", reply_markup=get_cancel_keyboard(), parse_mode='HTML')
    bot.register_next_step_handler(msg, process_new_rules)

def process_new_rules(message):
    if not is_admin(message.from_user.id): return
    if message.text == '❌ Отмена':
        bot.send_message(message.chat.id, "Отмена.", reply_markup=get_admin_keyboard())
        return
    new_rules = message.text
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('rules_text', ?)", (new_rules,))
    conn.commit()
    conn.close()
    bot.send_message(message.chat.id, "✅ Правила успешно обновлены!", reply_markup=get_admin_keyboard())

@bot.message_handler(func=lambda message: message.text == "◀️ Главное меню")
def back_to_main_menu(message):
    bot.send_message(message.chat.id, "<b>Возвращаюсь в главное меню.</b>", reply_markup=get_main_menu_keyboard(message.from_user.id), parse_mode='HTML')

@bot.message_handler(func=lambda message: message.text == "📊 Статистика пользователей")
def user_stats_handler(message):
    if not is_admin(message.from_user.id): return
    conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users"); total_users = cursor.fetchone()[0]
    last_24h_iso = (datetime.now() - timedelta(hours=24)).isoformat()
    cursor.execute("SELECT COUNT(*) FROM users WHERE registered_at >= ?", (last_24h_iso,)); new_users_24h = cursor.fetchone()[0]
    cursor.execute("SELECT SUM(referral_count) FROM users"); total_referrals = cursor.fetchone()[0] or 0
    conn.close()
    stats_text = (f"📊 <b>Статистика пользователей</b>\n\n" 
                  f"👥 Всего пользователей: <b>{total_users}</b>\n" 
                  f"🆕 Новых за 24ч: <b>{new_users_24h}</b>\n"
                  f"💌 Приглашено друзей: <b>{total_referrals}</b>")
    bot.send_message(message.chat.id, stats_text, parse_mode='HTML')

@bot.message_handler(func=lambda message: message.text == "⚙️ Настройки каналов")
def manage_channels_start(message):
    if not is_admin(message.from_user.id): return
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("➕ Добавить канал", callback_data="manage_channel_add"), types.InlineKeyboardButton("➖ Удалить канал", callback_data="manage_channel_remove"), types.InlineKeyboardButton("👀 Посмотреть список", callback_data="manage_channel_list"))
    bot.send_message(message.chat.id, "<b>Управление обязательными каналами:</b>", reply_markup=markup, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith('manage_channel_'))
def handle_manage_channels(call):
    if not is_admin(call.from_user.id): return
    action = call.data.split('_')[2]
    bot.answer_callback_query(call.id)
    if action == 'add':
        try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except: pass
        msg = bot.send_message(call.message.chat.id, "Введите юзернейм канала (например, @channelname).", reply_markup=get_cancel_keyboard())
        bot.register_next_step_handler(msg, process_add_channel)
    elif action == 'remove':
        channels = get_required_channels()
        if not channels: bot.answer_callback_query(call.id, "Список пуст."); bot.edit_message_text("Список пуст.", call.message.chat.id, call.message.message_id); return
        markup = types.InlineKeyboardMarkup(row_width=1)
        for channel in channels: markup.add(types.InlineKeyboardButton(f"➖ {channel}", callback_data=f"remove_channel_{channel}"))
        bot.edit_message_text("Выберите канал для удаления:", call.message.chat.id, call.message.message_id, reply_markup=markup)
    elif action == 'list':
        channels = get_required_channels()
        if not channels: bot.answer_callback_query(call.id, "Список пуст.", show_alert=True)
        else: channel_list = "\n".join([f"▪️ {c}" for c in channels]); bot.send_message(call.message.chat.id, f"<b>Текущие каналы:</b>\n\n{channel_list}", parse_mode='HTML')
    
def process_add_channel(message):
    if not is_admin(message.from_user.id) or message.text == "❌ Отмена": bot.send_message(message.chat.id, "<b>Отмена.</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML'); return
    channel_username = message.text.strip()
    if not re.match(r'^@[\w]{5,}$', channel_username): 
        msg = bot.send_message(message.chat.id, "<b>Неверный формат.</b> Попробуйте еще раз.", reply_markup=get_cancel_keyboard(), parse_mode='HTML')
        bot.register_next_step_handler(msg, process_add_channel); return
    conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
    try:
        if bot.get_chat_member(channel_username, bot_info.id).status != 'administrator': bot.send_message(message.chat.id, f"<b>Ошибка:</b> бот не админ в {channel_username}.", reply_markup=get_admin_keyboard(), parse_mode='HTML')
        else:
            cursor.execute("INSERT OR IGNORE INTO required_channels (channel_username) VALUES (?)", (channel_username,)); conn.commit()
            if cursor.rowcount > 0: bot.send_message(message.chat.id, f"✅ Канал <b>{channel_username}</b> добавлен.", reply_markup=get_admin_keyboard(), parse_mode='HTML')
            else: bot.send_message(message.chat.id, "ℹ️ Канал уже в списке.", reply_markup=get_admin_keyboard(), parse_mode='HTML')
    except telebot.apihelper.ApiTelegramException as e: bot.send_message(message.chat.id, f"Не удалось найти канал <b>{channel_username}</b>. Ошибка: {e}", reply_markup=get_admin_keyboard(), parse_mode='HTML')
    finally: conn.close()

@bot.callback_query_handler(func=lambda call: call.data.startswith('remove_channel_'))
def handle_remove_channel(call):
    if not is_admin(call.from_user.id): return
    channel_username = call.data.split('_', 2)[2]
    conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
    cursor.execute("DELETE FROM required_channels WHERE channel_username = ?", (channel_username,)); conn.commit(); conn.close()
    bot.answer_callback_query(call.id, f"Канал {channel_username} удален.", show_alert=True)
    channels = get_required_channels()
    if channels:
        markup = types.InlineKeyboardMarkup(row_width=1)
        for channel in channels: markup.add(types.InlineKeyboardButton(f"➖ {channel}", callback_data=f"remove_channel_{channel}"))
        bot.edit_message_text("Выберите канал для удаления:", call.message.chat.id, call.message.message_id, reply_markup=markup)
    else: bot.edit_message_text(f"Канал <b>{channel_username}</b> удален. Список пуст.", call.message.chat.id, call.message.message_id, reply_markup=None, parse_mode='HTML')

def find_user_by_id_or_username(identifier):
    conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor(); user_data = None
    try:
        if identifier.isdigit(): cursor.execute('SELECT user_id, first_name, username FROM users WHERE user_id = ?', (int(identifier),)); user_data = cursor.fetchone()
        else: cursor.execute('SELECT user_id, first_name, username FROM users WHERE username = ? COLLATE NOCASE', (identifier.replace('@', ''),)); user_data = cursor.fetchone()
    except Exception as e: logging.error(f"Error finding user '{identifier}': {e}")
    finally: conn.close()
    return user_data

@bot.message_handler(func=lambda message: message.text == "📣 Рассылка")
def broadcast_start(message):
    if not is_admin(message.from_user.id): return
    msg = bot.send_message(message.chat.id, "Отправьте пост для рассылки.", reply_markup=get_cancel_keyboard())
    bot.register_next_step_handler(msg, get_broadcast_content)

def get_broadcast_content(message):
    if message.text == "❌ Отмена": bot.send_message(message.chat.id, "<b>Отмена.</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML'); return
    msg = bot.send_message(message.chat.id, "Отправьте текст и ссылку для кнопки в формате: `Текст | https://ссылка.com`\nИли `-` если кнопка не нужна.", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
    bot.register_next_step_handler(msg, get_broadcast_button, message.message_id)

def get_broadcast_button(message, content_message_id):
    if message.text == "❌ Отмена": bot.send_message(message.chat.id, "<b>Отмена.</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML'); return
    markup = None
    if message.text != "-":
        parts = message.text.split('|', 1)
        if len(parts) == 2 and parts[1].strip().startswith(('http://', 'https://')):
            markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton(parts[0].strip(), url=parts[1].strip()))
        else:
            msg = bot.send_message(message.chat.id, "❌ <b>Неверный формат.</b> Попробуйте еще раз.", reply_markup=get_cancel_keyboard(), parse_mode='HTML')
            bot.register_next_step_handler(msg, get_broadcast_button, content_message_id); return
    bot.send_message(message.chat.id, "<b>Предпросмотр:</b>", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
    try: bot.copy_message(message.chat.id, message.chat.id, content_message_id, reply_markup=markup)
    except Exception as e: bot.send_message(message.chat.id, f"Не удалось создать предпросмотр: {e}"); bot.send_message(message.chat.id, "Рассылка отменена.", reply_markup=get_admin_keyboard()); return
    confirm_markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add(types.KeyboardButton("✅ Начать рассылку"), types.KeyboardButton("❌ Отмена"))
    msg = bot.send_message(message.chat.id, "Подтверждаете?", reply_markup=confirm_markup)
    bot.register_next_step_handler(msg, confirm_and_run_broadcast, content_message_id, markup)

def confirm_and_run_broadcast(message, content_message_id, markup):
    if message.text != "✅ Начать рассылку": bot.send_message(message.chat.id, "<b>Отмена.</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML'); return
    conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE is_banned != 1"); users = cursor.fetchall(); conn.close()
    if not users: bot.send_message(message.chat.id, "Нет пользователей для рассылки.", reply_markup=get_admin_keyboard()); return
    sent, failed, total = 0, 0, len(users)
    bot.send_message(message.chat.id, f"🚀 <b>Рассылка запущена для {total} пользователей...</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML')
    for user_tuple in users:
        try: bot.copy_message(user_tuple[0], message.chat.id, content_message_id, reply_markup=markup); sent += 1
        except: failed += 1
        time.sleep(0.05)
    report_text = f"✅ <b>Рассылка завершена.</b>\n\n▪️ Успешно: <b>{sent}</b>\n▪️ Ошибка: <b>{failed}</b>"
    bot.send_message(message.chat.id, report_text, parse_mode='HTML')

@bot.message_handler(func=lambda message: message.text == "🚫 Бан/Разбан")
def ban_unban_start(message):
    if not is_admin(message.from_user.id): return
    msg = bot.send_message(message.chat.id, "Введите ID или юзернейм.", reply_markup=get_cancel_keyboard())
    bot.register_next_step_handler(msg, process_ban_unban)

def process_ban_unban(message):
    if not is_admin(message.from_user.id) or message.text == "❌ Отмена": bot.send_message(message.chat.id, "<b>Отмена.</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML'); return
    target_user = find_user_by_id_or_username(message.text)
    if not target_user: msg = bot.send_message(message.chat.id, "❌ Пользователь не найден."); bot.register_next_step_handler(msg, process_ban_unban); return
    target_id = target_user[0]
    conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
    cursor.execute('SELECT is_banned FROM users WHERE user_id = ?', (target_id,)); result = cursor.fetchone()
    if not result: conn.close(); bot.send_message(message.chat.id, f"❌ ID <code>{target_id}</code> не найден.", reply_markup=get_admin_keyboard(), parse_mode='HTML'); return
    new_status = 1 if result[0] == 0 else 0
    cursor.execute('UPDATE users SET is_banned = ? WHERE user_id = ?', (new_status, target_id)); conn.commit(); conn.close()
    bot.send_message(message.chat.id, f"✅ Пользователь <code>{target_id}</code> <b>{'забанен' if new_status == 1 else 'разбанен'}</b>.", reply_markup=get_admin_keyboard(), parse_mode='HTML')

@bot.message_handler(func=lambda message: message.text == "💬 Написать пользователю")
def send_message_start(message):
    if not is_admin(message.from_user.id): return
    msg = bot.send_message(message.chat.id, "Введите ID или юзернейм.", reply_markup=get_cancel_keyboard())
    bot.register_next_step_handler(msg, process_message_user)

def process_message_user(message):
    if not is_admin(message.from_user.id) or message.text == "❌ Отмена": bot.send_message(message.chat.id, "<b>Отмена.</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML'); return
    target_user = find_user_by_id_or_username(message.text)
    if not target_user: msg = bot.send_message(message.chat.id, "❌ Пользователь не найден."); bot.register_next_step_handler(msg, process_message_user); return
    msg = bot.send_message(message.chat.id, "Введите текст сообщения.")
    bot.register_next_step_handler(msg, process_message_text, target_user[0])

def process_message_text(message, target_id):
    if not is_admin(message.from_user.id) or message.text == "❌ Отмена": bot.send_message(message.chat.id, "<b>Отмена.</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML'); return
    try:
        bot.send_message(target_id, f"💬 <b>Сообщение от администратора:</b>\n\n{escape(message.text)}", parse_mode='HTML')
        bot.send_message(message.chat.id, f"✅ Сообщение отправлено.", reply_markup=get_admin_keyboard())
    except Exception as e: bot.send_message(message.chat.id, f"❌ Не удалось отправить. Ошибка: {e}", reply_markup=get_admin_keyboard())

@bot.message_handler(func=lambda message: message.text == "📜 Рефералы пользователя")
def view_referrals_start(message):
    if not is_admin(message.from_user.id): return
    msg = bot.send_message(message.chat.id, "Введите ID или юзернейм пользователя для просмотра его рефералов.", reply_markup=get_cancel_keyboard())
    bot.register_next_step_handler(msg, process_view_referrals)

def process_view_referrals(message):
    if not is_admin(message.from_user.id) or message.text == "❌ Отмена": 
        bot.send_message(message.chat.id, "<b>Отмена.</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML')
        return
    
    target_user = find_user_by_id_or_username(message.text)
    if not target_user:
        msg = bot.send_message(message.chat.id, "❌ Пользователь не найден. Попробуйте снова.")
        bot.register_next_step_handler(msg, process_view_referrals)
        return
        
    target_id, target_name, target_username = target_user
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, first_name, username FROM users WHERE referred_by = ?", (target_id,))
    referrals = cursor.fetchall()
    conn.close()
    
    if not referrals:
        bot.send_message(message.chat.id, f"У пользователя <code>{target_id}</code> нет рефералов.", reply_markup=get_admin_keyboard(), parse_mode='HTML')
        return
        
    response_text = f"👥 <b>Рефералы пользователя {escape(target_name or '')} (<code>{target_id}</code>)</b> ({len(referrals)} чел.):\n\n"
    for ref_id, name, username in referrals:
        user_mention = f"@{username}" if username else f"ID: <code>{ref_id}</code>"
        response_text += f"▪️ {escape(name or 'Имя не указано')} ({user_mention})\n"
        
    if len(response_text) > 4096:
        response_text = response_text[:4090] + "\n..."
        
    bot.send_message(message.chat.id, response_text, reply_markup=get_admin_keyboard(), parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data == "check_sub_after_join")
def handle_check_sub(call):
    user_id = call.from_user.id
    
    cooldown_seconds = 7
    current_time = time.time()
    if user_id in last_check_sub_time and (current_time - last_check_sub_time[user_id]) < cooldown_seconds:
        bot.answer_callback_query(call.id, "⏳ Пожалуйста, подождите немного перед следующей проверкой.", show_alert=True)
        return
    
    last_check_sub_time[user_id] = current_time

    unsubscribed_channels = check_all_required_subscriptions(user_id) 
    if not unsubscribed_channels:
        bot.answer_callback_query(call.id, "Спасибо, основная подписка проверена!")
        try: bot.delete_message(call.message.chat.id, call.message.message_id)
        except: pass
        
        handle_successful_subscription(user_id)
        
        if run_async_from_sync(is_flyer_check_passed_async(user_id)) is False:
             return

        welcome_message = "✅ <b>Отлично! Доступ к боту открыт.</b>"
        if SHOW_BRANDING:
            welcome_message += f"\n\nБот создан с помощью @{CONSTRUCTOR_BOT_USERNAME}"

        bot.send_message(user_id, welcome_message, reply_markup=get_main_menu_keyboard(user_id), parse_mode='HTML')
    else:
        bot.answer_callback_query(call.id, "Вы еще не подписались на все каналы.", show_alert=True)
        try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=get_subscription_markup(unsubscribed_channels))
        except: pass

@bot.message_handler(func=lambda message: message.text == "🎁 Подарок")
@require_flyer_check
def daily_bonus_handler(message):
    user_id = message.from_user.id; conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
    cursor.execute("SELECT last_daily_bonus_claim FROM users WHERE user_id = ?", (user_id,)); last_claim_str = (cursor.fetchone() or [None])[0]
    cooldown_hours = DAILY_BONUS_COOLDOWN_HOURS
    if last_claim_str:
        try:
            last_claim_dt = datetime.fromisoformat(last_claim_str)
            if datetime.now() < last_claim_dt + timedelta(hours=cooldown_hours):
                time_left = (last_claim_dt + timedelta(hours=cooldown_hours)) - datetime.now()
                hours, rem = divmod(int(time_left.total_seconds()), 3600); minutes, _ = divmod(rem, 60)
                bot.send_message(user_id, f"⏳ <b>Подождите.</b> Следующий подарок через <b>{hours} ч. {minutes} мин.</b>", parse_mode='HTML'); conn.close(); return
        except:
            pass
    reward = DAILY_BONUS_REWARD
    cursor.execute("UPDATE users SET balance = balance + ?, last_daily_bonus_claim = ? WHERE user_id = ?", (reward, datetime.now().isoformat(), user_id)); conn.commit(); conn.close()
    bot.send_message(user_id, f"🎉 <b>Поздравляем!</b> Вы получили: <b>+{reward} ⭐</b>.\nСледующий подарок через {cooldown_hours} часов.", parse_mode='HTML')

@bot.message_handler(func=lambda message: message.text == "⭐ Личный кабинет")
@require_flyer_check
def profile_handler(message):
    show_profile(message.from_user.id, message.chat.id)

def show_profile(user_id, chat_id, message_id=None):
    conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
    cursor.execute('SELECT balance, registered_at, referral_count FROM users WHERE user_id = ?', (user_id,)); user_data = cursor.fetchone()
    if not user_data: return
    balance, reg_date_str, ref_count = user_data
    try: reg_date = datetime.fromisoformat(reg_date_str).strftime("%d.%m.%Y")
    except: reg_date = "N/A"
    
    status_text = "👤 Обычный"
    conn.close()
    profile_text = (f"👤 <b>Личный кабинет</b>\n\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"⭐️ Статус: <b>{status_text}</b>\n"
                    f"⭐ Баланс: <b>{balance}</b>\n"
                    f"📅 Регистрация: <b>{reg_date}</b>\n\n"
                    f"📈 <b>Статистика:</b>\n"
                    f"  - Приглашено друзей: <b>{ref_count}</b>\n\n"
                    f"🤝 <b>Ваша реф. ссылка:</b>\n"
                    f"<code>https://t.me/{bot_info.username}?start=ref{user_id}</code>")
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("⭐ Вывод звёзд", callback_data="claim_gift_menu"))
    markup.add(types.InlineKeyboardButton("🎁 Ввести промокод", callback_data="enter_promo"))
    if message_id:
        try: bot.edit_message_text(profile_text, chat_id, message_id, reply_markup=markup, disable_web_page_preview=True, parse_mode='HTML')
        except: pass
    else: bot.send_message(chat_id, profile_text, reply_markup=markup, disable_web_page_preview=True, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data == "back_to_profile")
def back_to_profile_handler(call):
    bot.answer_callback_query(call.id)
    if call.from_user.id in user_states:
        del user_states[call.from_user.id]
    show_profile(call.from_user.id, call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data == "claim_gift_menu")
@require_flyer_check
def show_gift_menu(call):
    bot.answer_callback_query(call.id)
    text = "Здесь ты можешь обменять звёзды на подарки для себя или отправить их другу:"
    markup = types.InlineKeyboardMarkup(row_width=2); grid_buttons, full_width_buttons = [], []
    for key, data in GIFTS.items():
        button = types.InlineKeyboardButton(f"{data['cost']} ⭐ ({data['name']})", callback_data=f"claim_gift_{key}")
        (full_width_buttons if data.get('full_width') else grid_buttons).append(button)
    for i in range(0, len(grid_buttons), 2): markup.row(*grid_buttons[i:i+2])
    for btn in full_width_buttons: markup.add(btn)
    markup.add(types.InlineKeyboardButton("🎁 Подарить другу", callback_data="gift_to_friend_start"))
    markup.add(types.InlineKeyboardButton("⬅️ В личный кабинет", callback_data="back_to_profile"))
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith("claim_gift_"))
@require_flyer_check
def process_gift_claim_self(call):
    gift_key = call.data[len('claim_gift_'):]
    if gift_key not in GIFTS: 
        bot.answer_callback_query(call.id, "Ошибка: подарок не найден.", show_alert=True)
        return
    process_gift_claim(call, call.from_user.id, gift_key)

def process_gift_claim(call, recipient_id, gift_key, is_a_gift=False, recipient_info_str=None):
    sender_id = call.from_user.id
    gift = GIFTS.get(gift_key)
    if not gift: 
        bot.answer_callback_query(call.id, "Ошибка: подарок не найден.", show_alert=True)
        return
    
    gift_cost = gift['cost']
    
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('SELECT balance FROM users WHERE user_id = ?', (sender_id,))
    balance = (cursor.fetchone() or [0])[0]

    if balance < gift_cost:
        bot.answer_callback_query(call.id, "Недостаточно звёзд.", show_alert=True)
        conn.close()
        return

    bot.answer_callback_query(call.id)
    cursor.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (gift_cost, sender_id))
    cursor.execute('INSERT INTO withdrawals (user_id, gift_name, amount_stars, created_at, recipient_id, recipient_info) VALUES (?, ?, ?, ?, ?, ?)', (sender_id, gift['name'], gift_cost, datetime.now().isoformat(), recipient_id, recipient_info_str))
    withdrawal_id = cursor.lastrowid
    conn.commit()
    conn.close()

    try:
        bot.edit_message_text(f"✅ <b>{'Подарок' if is_a_gift else 'Заявка'} отправлен{'а' if not is_a_gift else ''} на рассмотрение!</b>", call.message.chat.id, call.message.message_id, parse_mode='HTML')
    except telebot.apihelper.ApiTelegramException as e:
        if 'message is not modified' not in str(e):
            logging.error(f"Ошибка при редактировании сообщения о подарке: {e}")
            
    sender_info = call.from_user
    admin_text = (f"🎁 <b>Новый {'ПОДАРОК' if is_a_gift else 'ВЫВОД'} №{withdrawal_id}</b>\n\n" + (f"➡️ <b>Отправитель:</b> {escape(sender_info.first_name)} (@{escape(sender_info.username or 'N/A')}, <code>{sender_id}</code>)\n⬅️ <b>Получатель:</b> {recipient_info_str}\n" if is_a_gift else f"👤 <b>Пользователь:</b> {escape(sender_info.first_name)} (@{escape(sender_info.username or 'N/A')}, <code>{sender_id}</code>)\n") + f"✨ <b>Подарок:</b> {gift['name']}\n💰 <b>Стоимость:</b> {gift_cost} ⭐")
    admin_markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("✅ Одобрить", callback_data=f"wd_approve_{withdrawal_id}"), types.InlineKeyboardButton("❌ Отклонить", callback_data=f"wd_decline_{withdrawal_id}"))
    
    for admin_user_id in ADMINS_LIST:
        try:
            bot.send_message(admin_user_id, admin_text, reply_markup=admin_markup, parse_mode='HTML')
        except Exception as e:
            logging.error(f"Could not send gift request to admin {admin_user_id}: {e}")

@bot.callback_query_handler(func=lambda call: call.data == "gift_to_friend_start")
@require_flyer_check
def gift_to_friend_start(call):
    bot.answer_callback_query(call.id)
    try:
        msg = bot.edit_message_text("Введите ID или @username пользователя.", call.message.chat.id, call.message.message_id, reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад", callback_data="claim_gift_menu")))
        bot.register_next_step_handler(call.message, gift_to_friend_get_user)
    except:
        pass

def gift_to_friend_get_user(message):
    if message.text == "❌ Отмена": bot.send_message(message.chat.id, "<b>Отмена.</b>", reply_markup=get_main_menu_keyboard(message.from_user.id), parse_mode='HTML'); return
    recipient_data = find_user_by_id_or_username(message.text)
    if not recipient_data: msg = bot.send_message(message.chat.id, "❌ Пользователь не найден.", reply_markup=get_cancel_keyboard()); bot.register_next_step_handler(msg, gift_to_friend_get_user); return
    recipient_id, name, username = recipient_data
    if recipient_id == message.from_user.id: bot.send_message(message.chat.id, "Нельзя дарить себе.", reply_markup=get_main_menu_keyboard(message.from_user.id)); return
    recipient_info_str = f"{escape(name or '')} (@{escape(username or 'N/A')} <code>{recipient_id}</code>)"
    markup = types.InlineKeyboardMarkup(row_width=2); grid_buttons, full_width_buttons = [], []
    for key, data in GIFTS.items():
        button = types.InlineKeyboardButton(f"{data['cost']} ⭐ ({data['name']})", callback_data=f"gift_friend_{key}_{recipient_id}")
        (full_width_buttons if data.get('full_width') else grid_buttons).append(button)
    for i in range(0, len(grid_buttons), 2): markup.row(*grid_buttons[i:i+2])
    for btn in full_width_buttons: markup.add(btn)
    bot.send_message(message.chat.id, f"Выберите подарок для {recipient_info_str}:", reply_markup=markup, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith("gift_friend_"))
def process_gift_claim_friend(call):
    try:
        payload = call.data[len('gift_friend_'):]
        gift_key, recipient_id_str = payload.rsplit('_', 1)
        recipient_id = int(recipient_id_str)
    except (ValueError, IndexError):
        bot.answer_callback_query(call.id, "Ошибка: неверные данные подарка.", show_alert=True)
        return

    conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
    cursor.execute('SELECT first_name, username FROM users WHERE user_id = ?', (recipient_id,)); recipient_data = cursor.fetchone(); conn.close()
    if not recipient_data: bot.answer_callback_query(call.id, "Получатель не найден.", show_alert=True); return
    name, username = recipient_data; recipient_info_str = f"{escape(name or '')} (@{escape(username or 'N/A')} <code>{recipient_id}</code>)"
    process_gift_claim(call, recipient_id, gift_key, is_a_gift=True, recipient_info_str=recipient_info_str)


@bot.callback_query_handler(func=lambda call: call.data.startswith('wd_'))
def handle_withdrawal_admin(call):
    if not is_admin(call.from_user.id): return
    bot.answer_callback_query(call.id)
    action, withdrawal_id = call.data.split('_')[1], int(call.data.split('_')[2]); conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
    cursor.execute('SELECT user_id, gift_name, amount_stars, status, recipient_id FROM withdrawals WHERE id = ?', (withdrawal_id,)); res = cursor.fetchone()
    if not res or res[3] != 'pending': conn.close(); bot.edit_message_text(call.message.text + "\n\n⚠️ <b>Уже обработано.</b>", call.message.chat.id, call.message.message_id, reply_markup=None, parse_mode='HTML'); return
    sender_id, gift_name, amount_stars, _, recipient_id = res; is_a_gift = (sender_id != recipient_id)
    if action == 'approve':
        cursor.execute("UPDATE withdrawals SET status = 'approved' WHERE id = ?", (withdrawal_id,))
        bot.edit_message_text(call.message.text + "\n\n✅ <b>ОДОБРЕНО</b>", call.message.chat.id, call.message.message_id, reply_markup=None, parse_mode='HTML')
        try:
            cursor.execute('SELECT first_name FROM users WHERE user_id = ?', (recipient_id,)); recipient_name = telebot.util.escape((cursor.fetchone() or ["Пользователь"])[0])
            bot.send_message(PAYMENTS_CHANNEL, f"✅ <b>Новый вывод!</b>\n\n👤 <b>Пользователь:</b> {recipient_name} (<code>{recipient_id}</code>)\n🎁 <b>Получил:</b> {gift_name}", parse_mode='HTML')
        except Exception as e: logging.error(f"Could not post to payments channel: {e}")
        try:
            if is_a_gift:
                cursor.execute('SELECT first_name FROM users WHERE user_id = ?', (sender_id,)); sender_name = (cursor.fetchone() or ["Кто-то"])[0]
                bot.send_message(sender_id, f"✅ Ваш подарок '{gift_name}' для <code>{recipient_id}</code> одобрен!", parse_mode='HTML')
                bot.send_message(recipient_id, f"🎁 Вам пришел подарок от {sender_name} (<code>{sender_id}</code>): '{gift_name}'.", parse_mode='HTML')
            else: bot.send_message(sender_id, f"✅ Ваша заявка на '{gift_name}' одобрена!", parse_mode='HTML')
        except: pass
    elif action == 'decline':
        cursor.execute("UPDATE withdrawals SET status = 'declined' WHERE id = ?", (withdrawal_id,)); cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount_stars, sender_id))
        bot.edit_message_text(call.message.text + "\n\n❌ <b>ОТКЛОНЕНО</b>", call.message.chat.id, call.message.message_id, reply_markup=None, parse_mode='HTML')
        try: bot.send_message(sender_id, f"❌ Ваша заявка на {'подарок' if is_a_gift else 'вывод'} '{gift_name}' отклонена. Звёзды возвращены.", parse_mode='HTML')
        except: pass
    conn.commit(); conn.close()

@bot.message_handler(func=lambda message: message.text == "👥 Рефералы")
@require_flyer_check
def show_referrals(message):
    user_id = message.from_user.id; conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
    cursor.execute("SELECT referral_count FROM users WHERE user_id = ?", (user_id,)); ref_count = (cursor.fetchone() or [0])[0]; conn.close()
    total_earned = ref_count * REFERRAL_BONUS_REFERRER
    text = (f"👥 <b>Реферальная программа</b>\n\n" f"Приглашайте друзей и зарабатывайте звёзды!\n\n" f"▫️ Вы получаете: <b>{REFERRAL_BONUS_REFERRER} ⭐</b> за каждого друга.\n" f"▫️ Ваш друг получает: <b>{REFERRAL_BONUS_NEW_USER} ⭐</b>.\n\n" f"📈 <b>Ваша статистика:</b>\n" f"  - Приглашено друзей: <b>{ref_count} чел.</b>\n" f"  - Заработано: <b>{total_earned} ⭐</b>")
    bot.send_message(user_id, text, parse_mode='HTML')

@bot.message_handler(func=lambda message: user_states.get(message.from_user.id, {}).get('action'))
def handle_state_message(message):
    state = user_states.get(message.from_user.id, {})
    if state.get('action') == 'awaiting_promo':
        process_promo_code(message)

@bot.callback_query_handler(func=lambda call: call.data == "enter_promo")
def enter_promo_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.edit_message_text("🎁 <b>Введите ваш промокод:</b>", call.message.chat.id, call.message.message_id,
                                reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Назад в профиль", callback_data="back_to_profile")), parse_mode='HTML')
    user_states[call.from_user.id] = {'action': 'awaiting_promo', 'message_id': msg.message_id}

def process_promo_code(message):
    user_id = message.from_user.id
    if user_id not in user_states: return
    
    state = user_states.pop(user_id)
    original_message_id = state['message_id']
    
    code = message.text.strip()
    
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor = conn.cursor()
    
    cursor.execute("SELECT 1 FROM promo_activations WHERE user_id = ? AND code = ?", (user_id, code))
    if cursor.fetchone():
        bot.send_message(user_id, "❌ Вы уже использовали этот промокод.")
    else:
        cursor.execute("SELECT reward, total_uses, used_count FROM promo_codes WHERE code = ?", (code,))
        promo_data = cursor.fetchone()
        if not promo_data:
            bot.send_message(user_id, "❌ Такого промокода не существует.")
        else:
            reward, total_uses, used_count = promo_data
            if used_count >= total_uses:
                bot.send_message(user_id, "❌ К сожалению, этот промокод уже закончился.")
            else:
                cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (reward, user_id))
                cursor.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE code = ?", (code,))
                cursor.execute("INSERT INTO promo_activations (user_id, code) VALUES (?, ?)", (user_id, code))
                conn.commit()
                bot.send_message(user_id, f"✅ Промокод успешно активирован! Вам начислено <b>{reward} ⭐</b>.", parse_mode='HTML')

    conn.close()
    try:
        bot.delete_message(user_id, message.message_id)
    except:
        pass
    show_profile(user_id, user_id, original_message_id)

@bot.message_handler(func=lambda message: message.text == "🎁 Промокоды")
def promo_admin_menu(message):
    if not is_admin(message.from_user.id): return
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("➕ Создать промокод", callback_data="promo_create"))
    markup.add(types.InlineKeyboardButton("➖ Удалить промокод", callback_data="promo_delete"))
    markup.add(types.InlineKeyboardButton("📋 Список промокодов", callback_data="promo_list"))
    bot.send_message(message.chat.id, "🎁 <b>Управление промокодами:</b>", reply_markup=markup, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith('promo_'))
def handle_promo_callbacks(call):
    if not is_admin(call.from_user.id): return
    bot.answer_callback_query(call.id)
    action = call.data.split('_')[1]
    
    if action == "create":
        try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except: pass
        msg = bot.send_message(call.message.chat.id, "Введите данные для промокода в формате:\n`НАЗВАНИЕ НАГРАДА КОЛИЧЕСТВО`\n\nПример: `newyear 100 50`", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        bot.register_next_step_handler(msg, process_create_promo)
    
    elif action == "delete":
        try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except: pass
        msg = bot.send_message(call.message.chat.id, "Введите название промокода для удаления:", reply_markup=get_cancel_keyboard())
        bot.register_next_step_handler(msg, process_delete_promo)
    
    elif action == "list":
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("SELECT code, reward, used_count, total_uses FROM promo_codes")
        promos = cursor.fetchall()
        conn.close()
        if not promos:
            bot.send_message(call.message.chat.id, "Список промокодов пуст.")
            return
        
        text = "📝 <b>Список активных промокодов:</b>\n\n"
        for code, reward, used, total in promos:
            text += f"<code>{code}</code> - <b>{reward} ⭐</b> (Исп: {used}/{total})\n"
        try:
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode='HTML')
        except:
            bot.send_message(call.message.chat.id, text, parse_mode='HTML')

def process_create_promo(message):
    if message.text == "❌ Отмена": bot.send_message(message.chat.id, "<b>Отмена.</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML'); return
    try:
        code, reward, uses = message.text.split()
        reward = int(reward)
        uses = int(uses)
        if reward <= 0 or uses <= 0: raise ValueError
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO promo_codes (code, reward, total_uses, used_count) VALUES (?, ?, ?, 0)", (code, reward, uses))
        conn.commit()
        conn.close()
        bot.send_message(message.chat.id, f"✅ Промокод <code>{code}</code> создан/обновлен.\nНаграда: <b>{reward} ⭐</b>, Активаций: <b>{uses}</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML')
    except Exception:
        msg = bot.send_message(message.chat.id, "❌ <b>Ошибка!</b> Неверный формат. Попробуйте снова.", reply_markup=get_cancel_keyboard(), parse_mode='HTML')
        bot.register_next_step_handler(msg, process_create_promo)

def process_delete_promo(message):
    if message.text == "❌ Отмена": bot.send_message(message.chat.id, "<b>Отмена.</b>", reply_markup=get_admin_keyboard(), parse_mode='HTML'); return
    code_to_delete = message.text.strip()
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM promo_codes WHERE code = ?", (code_to_delete,))
    conn.commit()
    if cursor.rowcount > 0:
        bot.send_message(message.chat.id, f"✅ Промокод <code>{code_to_delete}</code> удален.", reply_markup=get_admin_keyboard(), parse_mode='HTML')
    else:
        msg = bot.send_message(message.chat.id, f"❌ Промокод <code>{code_to_delete}</code> не найден. Попробуйте снова.", reply_markup=get_cancel_keyboard(), parse_mode='HTML')
        bot.register_next_step_handler(msg, process_delete_promo)
    conn.close()

@bot.message_handler(commands=['promo', 'delpromo', 'promolist'])
def handle_promo_commands(message):
    if not is_admin(message.from_user.id): return
    if message.text.startswith('/promo'):
        process_create_promo_cmd(message)
    elif message.text.startswith('/delpromo'):
        process_delete_promo_cmd(message)
    elif message.text.startswith('/promolist'):
        process_promo_list_cmd(message)

def process_create_promo_cmd(message):
    try:
        _, code, reward, uses = message.text.split()
        reward = int(reward); uses = int(uses)
        if reward <= 0 or uses <= 0: raise ValueError
        conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO promo_codes (code, reward, total_uses, used_count) VALUES (?, ?, ?, 0)",(code, reward, uses))
        conn.commit(); conn.close()
        bot.reply_to(message, f"✅ Промокод <code>{code}</code> создан/обновлен.\nНаграда: <b>{reward} ⭐</b>, Активаций: <b>{uses}</b>", parse_mode='HTML')
    except Exception: bot.reply_to(message, "❌ <b>Ошибка!</b> Используйте формат:\n<code>/promo НАЗВАНИЕ НАГРАДА КОЛИЧЕСТВО</code>\nПример: <code>/promo newyear 100 50</code>", parse_mode='HTML')

def process_delete_promo_cmd(message):
    try:
        code_to_delete = message.text.split()[1]
        conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
        cursor.execute("DELETE FROM promo_codes WHERE code = ?", (code_to_delete,)); conn.commit()
        if cursor.rowcount > 0: bot.reply_to(message, f"✅ Промокод <code>{code_to_delete}</code> удален.", parse_mode='HTML')
        else: bot.reply_to(message, f"❌ Промокод <code>{code_to_delete}</code> не найден.", parse_mode='HTML')
        conn.close()
    except: bot.reply_to(message, "❌ <b>Ошибка!</b> Используйте: <code>/delpromo НАЗВАНИЕ</code>", parse_mode='HTML')

def process_promo_list_cmd(message):
    conn = sqlite3.connect(DB_NAME, check_same_thread=False); cursor = conn.cursor()
    cursor.execute("SELECT code, reward, used_count, total_uses FROM promo_codes"); promos = cursor.fetchall(); conn.close()
    if not promos: bot.reply_to(message, "Список промокодов пуст."); return
    text = "📝 <b>Список активных промокодов:</b>\n\n"
    for code, reward, used, total in promos:
        text += f"<code>{code}</code> - <b>{reward} ⭐</b> (Исп: {used}/{total})\n"
    bot.reply_to(message, text, parse_mode='HTML')

# =================================================================================
# ----------------------------------- ЗАПУСК --------------------------------------
# =================================================================================
if __name__ == '__main__':
    if async_loop:
        async_thread = threading.Thread(target=async_loop.run_forever, daemon=True)
        async_thread.start()
        logging.info("Asyncio event loop started for Flyer.")
    
    logging.info(f"Запуск бота со звездами (ID: {BOT_ID}) с токеном ...{TOKEN[-6:]}")
    while True:
        try:
            bot.infinity_polling(skip_pending=True, timeout=20)
        except Exception as e:
            logging.critical(f"Критическая ошибка в главном цикле бота: {e}")
            traceback.print_exc()
            time.sleep(15)
            logging.info("Перезапуск бота...")
