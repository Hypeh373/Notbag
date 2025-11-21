import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
import requests
import time
import sqlite3
import sys
import os
from datetime import datetime
from html import escape

# Укажите @username конструктора; при желании можно переопределить через переменную окружения CREATOR_USERNAME.
CREATOR_USERNAME = os.getenv('CREATOR_USERNAME', '@GrillCreate_bot').strip() or '@GrillCreate_bot'

# BOT_ID передается как аргумент командной строки
BOT_ID = int(sys.argv[1]) if len(sys.argv) > 1 else None
if BOT_ID is None:
    print("ОШИБКА: BOT_ID не передан! Использование: python anonchatik.py <bot_id>")
    sys.exit(1)

# Путь к БД Creator
CREATOR_DB_PATH = 'creator_data2.db'
_CREATOR_BOTS_COLUMNS_CACHE = None
_CREATOR_MISSING_COLUMN_WARNINGS = set()


def _load_creator_bots_columns():
    """Загружает список колонок таблицы bots из Creator БД и кэширует результат."""
    global _CREATOR_BOTS_COLUMNS_CACHE
    if _CREATOR_BOTS_COLUMNS_CACHE is not None:
        return _CREATOR_BOTS_COLUMNS_CACHE
    conn = None
    try:
        conn = sqlite3.connect(CREATOR_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(bots)")
        _CREATOR_BOTS_COLUMNS_CACHE = {row[1] for row in cursor.fetchall()}
    except Exception as e:
        print(f"Не удалось получить структуру таблицы bots в Creator БД: {e}")
        _CREATOR_BOTS_COLUMNS_CACHE = set()
    finally:
        if conn:
            conn.close()
    return _CREATOR_BOTS_COLUMNS_CACHE


def _creator_table_has_column(column_name):
    """Проверяет наличие нужного столбца в таблице bots."""
    if not column_name:
        return False
    columns = _load_creator_bots_columns()
    return column_name in columns


def _warn_missing_creator_column(column_name):
    """Логирует предупреждение об отсутствующей колонке, но только один раз."""
    if column_name in _CREATOR_MISSING_COLUMN_WARNINGS:
        return
    print(
        f"⚠️ Колонка '{column_name}' отсутствует в таблице bots Creator. "
        f"Используется значение по умолчанию."
    )
    _CREATOR_MISSING_COLUMN_WARNINGS.add(column_name)


def get_bot_setting_from_creator(bot_id, setting_name, default_value=None):
    """Получает настройку бота из БД Creator"""
    global _CREATOR_BOTS_COLUMNS_CACHE
    if not _creator_table_has_column(setting_name):
        _warn_missing_creator_column(setting_name)
        return default_value
    conn = None
    try:
        conn = sqlite3.connect(CREATOR_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT {setting_name} FROM bots WHERE id = ?", (bot_id,))
        result = cursor.fetchone()
        if result and result[0] is not None:
            return result[0]
        return default_value
    except sqlite3.OperationalError as e:
        if "no such column" in str(e).lower():
            # Таблица изменилась после кэширования — сбросим кэш и вернём default.
            _CREATOR_BOTS_COLUMNS_CACHE = None
            _warn_missing_creator_column(setting_name)
            return default_value
        print(f"Ошибка получения настройки {setting_name}: {e}")
        return default_value
    except Exception as e:
        print(f"Ошибка получения настройки {setting_name}: {e}")
        return default_value
    finally:
        if conn:
            conn.close()

# Загружаем настройки из Creator БД
TOKEN = get_bot_setting_from_creator(BOT_ID, 'bot_token', '')
CRYPTO_API_TOKEN = get_bot_setting_from_creator(BOT_ID, 'anonchat_crypto_api_token', '')
RAW_CHANNEL_ID = get_bot_setting_from_creator(BOT_ID, 'anonchat_channel_id', '')
VIP_PRICE = float(get_bot_setting_from_creator(BOT_ID, 'anonchat_vip_price', 45.0))
WELCOME_MESSAGE = get_bot_setting_from_creator(BOT_ID, 'anonchat_welcome_message', 'Добро пожаловать! Начните общение 🐣.')
FLYER_API_KEY = get_bot_setting_from_creator(BOT_ID, 'anonchat_flyer_api_key', '')
try:
    FLYER_TASKS_LIMIT = int(get_bot_setting_from_creator(BOT_ID, 'anonchat_flyer_tasks_limit', 5) or 5)
except ValueError:
    FLYER_TASKS_LIMIT = 5


def _parse_admin_ids(raw_value):
    ids = set()
    if not raw_value:
        return ids
    cleaned = str(raw_value).replace(';', ',')
    for chunk in cleaned.split(','):
        token = chunk.strip()
        if not token:
            continue
        if token.startswith('+'):
            token = token[1:]
        if token.lstrip('-').isdigit():
            try:
                ids.add(int(token))
            except ValueError:
                continue
    return ids


ADMIN_IDS = _parse_admin_ids(os.getenv('ADMIN_IDS'))
if not ADMIN_IDS:
    ADMIN_IDS = _parse_admin_ids(get_bot_setting_from_creator(BOT_ID, 'admin_ids', ''))

if not ADMIN_IDS:
    fallback_admin_id = (
        os.getenv('DEFAULT_ADMIN_ID')
        or os.getenv('ADMIN_ID_DEFAULT')
        or os.getenv('ADMIN_ID')
        or get_bot_setting_from_creator(BOT_ID, 'admin_id', '')
    )
    if fallback_admin_id and str(fallback_admin_id).lstrip('-').isdigit():
        ADMIN_IDS = {int(fallback_admin_id)}

if not ADMIN_IDS:
    try:
        ADMIN_IDS = {int(get_bot_setting_from_creator(BOT_ID, 'owner_id', ''))}
    except (TypeError, ValueError):
        ADMIN_IDS = set()

if not ADMIN_IDS:
    # Последний резерв — вручную пропишите свой ID здесь, если нигде больше не задан.
    ADMIN_IDS = {6745031200}

def normalize_channel(raw_value: str) -> str:
    if not raw_value:
        return ''
    value = str(raw_value).strip()
    if not value:
        return ''
    if value.startswith('https://t.me/'):
        rest = value.split('https://t.me/', 1)[1]
        rest = rest.split('/', 1)[0]
        value = '@' + rest
    if not value.startswith('@') and not value.lstrip('-').isdigit():
        value = '@' + value
    return value

CHANNEL_ID = normalize_channel(RAW_CHANNEL_ID)
CHANNEL_USERNAME = CHANNEL_ID[1:] if CHANNEL_ID.startswith('@') else CHANNEL_ID
SUBSCRIPTION_REQUIRED = bool(CHANNEL_ID)
DEFAULT_SEARCH_GENDER = "Любой"

def _resolve_creator_username() -> str:
    """Возвращает @username конструктора в нормализованном виде."""
    username = CREATOR_USERNAME.strip()
    if not username:
        return ""
    username = username.replace(" ", "")
    if not username:
        return ""
    if not username.startswith('@'):
        username = f"@{username.lstrip('@')}"
    return username


def _build_creator_branding_text() -> str:
    handle = _resolve_creator_username()
    if not handle:
        return ""
    return f"Создан с помощью {handle}"

if not TOKEN:
    print(f"ОШИБКА: Токен бота #{BOT_ID} не найден в БД Creator!")
    sys.exit(1)

bot = telebot.TeleBot(TOKEN)

# Словари для хранения данных
chat_partners = {}  # Для активных пар
waiting_users = set()  # Для ожидания
user_data = {}  # Для хранения данных пользователей: пол, премиум-статус
user_states = {}  # Для состояний админских действий
user_invoices = {}  # Для хранения инвойсов пользователей
last_check_time = {}  # Время последней проверки статуса

# Проверка подписки
def check_subscription(user_id):
    if not SUBSCRIPTION_REQUIRED:
        return True
    try:
        member = bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception:
        return False
# Путь к БД пользователей бота
USER_DB_PATH = f'dbs/bot_{BOT_ID}_anonchat.db'

# Создаем папку dbs если её нет
if not os.path.exists('dbs'):
    os.makedirs('dbs')

def ensure_user_record(user_id):
    conn = sqlite3.connect(USER_DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        'INSERT OR IGNORE INTO users (user_id, search_gender) VALUES (?, ?)',
        (user_id, DEFAULT_SEARCH_GENDER)
    )
    conn.commit()
    conn.close()


def refresh_user_cache(user_id):
    conn = sqlite3.connect(USER_DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT gender, premium, search_gender FROM users WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        user_data[user_id] = {
            "gender": row[0],
            "premium": bool(row[1]),
            "search_gender": row[2] or DEFAULT_SEARCH_GENDER
        }
    else:
        user_data[user_id] = {
            "gender": None,
            "premium": False,
            "search_gender": DEFAULT_SEARCH_GENDER
        }


def ensure_user_loaded(user_id):
    ensure_user_record(user_id)
    if user_id not in user_data:
        refresh_user_cache(user_id)


def update_user_data(user_id, gender=None, premium=None, search_gender=None):
    ensure_user_record(user_id)
    updates = []
    params = []
    if gender is not None:
        updates.append('gender = ?')
        params.append(gender)
    if premium is not None:
        updates.append('premium = ?')
        params.append(int(bool(premium)))
    if search_gender is not None:
        updates.append('search_gender = ?')
        params.append(search_gender)

    if updates:
        conn = sqlite3.connect(USER_DB_PATH)
        cursor = conn.cursor()
        params.append(user_id)
        cursor.execute(f"UPDATE users SET {', '.join(updates)} WHERE user_id = ?", params)
        conn.commit()
        conn.close()

    refresh_user_cache(user_id)


def set_user_gender(user_id, gender):
    update_user_data(user_id, gender=gender)


def set_search_gender(user_id, search_gender):
    update_user_data(user_id, search_gender=search_gender)


def set_premium_status(user_id, is_premium):
    update_user_data(user_id, premium=bool(is_premium))

def ban_user(user_id, reason):
    conn = sqlite3.connect(USER_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET banned = 1 WHERE user_id = ?", (user_id,))
    cursor.execute("INSERT OR REPLACE INTO bans (user_id, reason, created_at) VALUES (?, ?, ?)",
                   (user_id, reason, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def unban_user(user_id):
    conn = sqlite3.connect(USER_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET banned = 0 WHERE user_id = ?", (user_id,))
    cursor.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def is_banned(user_id):
    conn = sqlite3.connect(USER_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT banned FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result and result[0] == 1

# Инициализация БД пользователей
def _ensure_user_columns(cursor):
    cursor.execute("PRAGMA table_info(users)")
    existing = {row[1] for row in cursor.fetchall()}

    safe_search_gender = DEFAULT_SEARCH_GENDER.replace("'", "''")
    required_columns = {
        'gender': "TEXT",
        'premium': "INTEGER DEFAULT 0",
        'search_gender': f"TEXT DEFAULT '{safe_search_gender}'",
        'banned': "INTEGER DEFAULT 0",
    }

    for column_name, ddl in required_columns.items():
        if column_name not in existing:
            cursor.execute(f"ALTER TABLE users ADD COLUMN {column_name} {ddl}")


def init_user_db():
    conn = sqlite3.connect(USER_DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        gender TEXT,
        premium INTEGER DEFAULT 0,
        search_gender TEXT DEFAULT '{DEFAULT_SEARCH_GENDER}',
        banned INTEGER DEFAULT 0
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS bans (
        user_id INTEGER PRIMARY KEY,
        reason TEXT,
        created_at TEXT
    )''')
    _ensure_user_columns(cursor)
    conn.commit()
    conn.close()

# Инициализируем БД при запуске
init_user_db()

# Отправка кнопок для подписки
def send_subscription_buttons(chat_id):
    if not SUBSCRIPTION_REQUIRED:
        bot.send_message(chat_id, "Подписка на канал не требуется. Нажмите /start, чтобы продолжить.")
        return
    markup = InlineKeyboardMarkup()
    channel_button = InlineKeyboardButton("Подписаться на канал", url=f"https://t.me/{CHANNEL_USERNAME}")
    check_button = InlineKeyboardButton("Проверить подписку ✅", callback_data="check_subscription")
    markup.add(channel_button)
    markup.add(check_button)
    bot.send_message(chat_id, "Для использования бота необходимо подписаться на канал:", reply_markup=markup)

# Проверка подписки перед выполнением действий
def is_user_subscribed(user_id):
    if not SUBSCRIPTION_REQUIRED:
        return True
    if not check_subscription(user_id):
        send_subscription_buttons(user_id)
        return False
    return True

# Спрашиваем пол пользователя
def ask_gender(user_id):
    markup = InlineKeyboardMarkup()
    boy_button = InlineKeyboardButton("Мальчик 👦", callback_data="gender_boy")
    girl_button = InlineKeyboardButton("Девочка 👩", callback_data="gender_girl")
    markup.add(boy_button, girl_button)
    bot.send_message(user_id, "Выберите ваш пол:", reply_markup=markup)

# Callback обработчик для выбора пола и подписки
@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    user_id = call.from_user.id
    ensure_user_loaded(user_id)

    admin_callbacks = {"broadcast", "ban_menu", "ban_add", "ban_remove", "ban_list", "admin_back", "stats"}
    if call.data in admin_callbacks:
        if handle_admin_callback(call):
            return

    # Ответ на проверку подписки
    if call.data == "check_subscription":
        if check_subscription(user_id):
            bot.answer_callback_query(call.id, "Вы подписаны! Добро пожаловать 😊.")
            bot.send_message(user_id, "Вы успешно подписались на канал!")
            if not user_data[user_id]["gender"]:
                ask_gender(user_id)
            else:
                show_main_buttons(user_id)
        else:
            bot.answer_callback_query(call.id, "Вы не подписаны. Подпишитесь и попробуйте снова 😥.")
            send_subscription_buttons(user_id)

    # Ответ на выбор пола
    elif call.data == "gender_boy":
        set_user_gender(user_id, "Мальчик")
        bot.answer_callback_query(call.id, "Вы выбрали: Мальчик 👦.")
        bot.send_message(user_id, "Ваш выбор сохранён: Мальчик 👦.")
        show_main_buttons(user_id)

    elif call.data == "gender_girl":
        set_user_gender(user_id, "Девочка")
        bot.answer_callback_query(call.id, "Вы выбрали: Девочка 👩.")
        bot.send_message(user_id, "Ваш выбор сохранён: Девочка 👩.")
        show_main_buttons(user_id)

    elif call.data == "buy_premium":
        bot.answer_callback_query(call.id)
        create_invoice_for_premium(call.message)

    elif call.data == "check_payment":
        bot.answer_callback_query(call.id)
        check_payment_status(call)

    elif call.data == "premium_settings":
        if user_data[user_id]["premium"]:
            show_premium_settings(user_id)
            bot.answer_callback_query(call.id)
        else:
            bot.answer_callback_query(call.id, "У вас нет премиум подписки.")

    elif call.data in {"search_gender_any", "search_gender_male", "search_gender_female"}:
        if not user_data[user_id]["premium"]:
            bot.answer_callback_query(call.id, "Настройки доступны только премиум пользователям.")
            return
        mapping = {
            "search_gender_any": DEFAULT_SEARCH_GENDER,
            "search_gender_male": "Мальчик",
            "search_gender_female": "Девочка"
        }
        target = mapping.get(call.data, DEFAULT_SEARCH_GENDER)
        set_search_gender(user_id, target)
        bot.answer_callback_query(call.id, f"Пол для поиска: {target}")
        show_premium_settings(user_id)

# Основные кнопки (Начать поиск)
MENU_BUTTON_TEXTS = {
    "Начать поиск 🔍",
    "Личный кабинет 👤",
    "Премиум поиск 👑",
    "⚙️ Админка",
    "❌ Остановить поиск собеседника",
}


def is_control_command(text: str) -> bool:
    if not text:
        return False
    normalized = text.strip()
    if not normalized:
        return False
    lowered = normalized.lower()
    if normalized in MENU_BUTTON_TEXTS:
        return True
    if normalized.startswith("/"):
        return True
    if lowered == "alluser":
        return True
    if lowered.startswith("rassilka"):
        return True
    return False


def _is_regular_incoming_message(message):
    text = getattr(message, "text", None)
    if text and is_control_command(text):
        return False
    return True


def show_main_buttons(chat_id, prompt_text="Выберите действие:"):
    markup = ReplyKeyboardMarkup(resize_keyboard=True)
    search_button = KeyboardButton("Начать поиск 🔍")
    profile_button = KeyboardButton("Личный кабинет 👤")
    premium_button = KeyboardButton("Премиум поиск 👑")
    markup.add(search_button)
    markup.add(profile_button, premium_button)
    if is_admin(chat_id):
        markup.add(KeyboardButton("⚙️ Админка"))
    bot.send_message(chat_id, prompt_text, reply_markup=markup)

# Admin state handling
@bot.message_handler(func=lambda m: m.from_user.id in user_states)
def handle_admin_states(message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return

    state = user_states.get(user_id)
    if state == 'waiting_broadcast':
        # Send broadcast
        conn = sqlite3.connect(USER_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users WHERE banned = 0")
        users = cursor.fetchall()
        conn.close()

        success_count = 0
        failure_count = 0
        for user in users:
            try:
                bot.send_message(user[0], message.text)
                success_count += 1
            except:
                failure_count += 1

        bot.send_message(user_id, f"✅ Рассылка завершена!\nУспешно: {success_count}\nОшибок: {failure_count}")
        del user_states[user_id]

    elif state == 'waiting_ban':
        parts = message.text.split(maxsplit=1)
        if len(parts) >= 2:
            try:
                target_id = int(parts[0])
                reason = parts[1]
                ban_user(target_id, reason)
                bot.send_message(user_id, f"✅ Пользователь {target_id} забанен. Причина: {reason}")
            except ValueError:
                bot.send_message(user_id, "❌ Неверный ID.")
        else:
            bot.send_message(user_id, "Формат: ID Причина")
        del user_states[user_id]

    elif state == 'waiting_unban':
        try:
            target_id = int(message.text)
            unban_user(target_id)
            bot.send_message(user_id, f"✅ Пользователь {target_id} разбанен.")
        except ValueError:
            bot.send_message(user_id, "❌ Неверный ID.")
        del user_states[user_id]

def send_bulk_message(message_text):
    conn = sqlite3.connect(USER_DB_PATH)
    cursor = conn.cursor()

    # Получаем все ID пользователей
    cursor.execute('SELECT user_id FROM users')
    users = cursor.fetchall()

    success_count = 0
    failure_count = 0

    for user in users:
        user_id = user[0]
        try:
            bot.send_message(user_id, message_text)
            success_count += 1  # Успешная доставка
        except Exception as e:
            failure_count += 1  # Неудачная доставка
            print(f"Не удалось отправить сообщение пользователю {user_id}: {str(e)}")

    conn.close()

    # Возвращаем количество доставленных и недоставленных сообщений
    return success_count, failure_count

# Обработка команды для рассылки
@bot.message_handler(func=lambda message: bool(message.text and message.text.startswith("Rassilka")))
def handle_rassilka(message):
    user_id = message.chat.id
    if not is_admin(user_id):
        bot.send_message(user_id, "У вас нет прав для отправки рассылки.")
        return

    message_text = message.text[8:].strip()
    if not message_text:
        bot.send_message(user_id, "Пожалуйста, укажите текст для рассылки.")
        return

    success_count, failure_count = send_bulk_message(message_text)
    report = (
        "Рассылка завершена!\n\n"
        f"✅ Успешно доставлено: {success_count} сообщений\n"
        f"❌ Не удалось доставить: {failure_count} сообщений"
    )
    bot.send_message(user_id, report)

# Обработка кнопки "Премиум поиск 👑"
@bot.message_handler(func=lambda message: message.text == "Премиум поиск 👑")
def premium_search(message):
    user_id = message.chat.id
    ensure_user_loaded(user_id)
    if not is_user_subscribed(user_id):
        return

    if user_id in user_data and user_data[user_id]["premium"]:
        markup = InlineKeyboardMarkup()
        premium_settings_button = InlineKeyboardButton("Премиум настройки", callback_data="premium_settings")
        markup.add(premium_settings_button)
        bot.send_message(user_id, "У вас есть премиум подписка 🥳. Нажмите кнопку ниже, чтобы настроить поиск 🔍", reply_markup=markup)
    else:
        # Если нет премиум подписки, показываем кнопку для перехода к оплате
        markup = InlineKeyboardMarkup()
        payment_button = InlineKeyboardButton(f"Перейти к оплате в CryptoBot - {VIP_PRICE}₽", callback_data="buy_premium")
        markup.add(payment_button)
        bot.send_message(
            user_id,
            "🌟 *Откройте для себя эксклюзивные возможности с премиум-подпиской!* 🌟\n\n"
            "Чтобы получить доступ к *премиум поиску*, просто приобретите нашу *премиум-подписку* и откройте для себя новые горизонты! *ПОКУПКА ПОДПИСКИ НАВСЕГДА* 🚀\n\n"
            "С премиум-доступом вы сможете:\n\n"
            "🔍 *Выбирать пол для поиска* – Настройте поиск так, как вам удобно, и найдите именно то, что ищете!\n"
            "⚡ *Приоритетный поиск* – Получайте собеседников быстрее остальных пользователей!\n"
            "💬 *Открытие новых возможностей* – Включите функции, которые делают общение более удобным и безопасным!\n\n"
            "💳 *Выберите способ оплаты ниже* и откройте доступ к уникальным возможностям!",
            reply_markup=markup,
            parse_mode='Markdown'
        )

        

# Функция для создания инвойса
def create_invoice_for_premium(message):
    user_id = message.chat.id
    ensure_user_loaded(user_id)
    amount = VIP_PRICE  # Сумма для премиум подписки из настроек Creator

    if not CRYPTO_API_TOKEN:
        bot.send_message(user_id, "Оплата через Crypto Pay временно недоступна. Обратитесь к администратору.")
        return
        
    data = {
        'currency_type': 'fiat',
        'amount': amount,
        'fiat': 'RUB',
        'description': 'Оплата за премиум подписку',
    }

    headers = {
        'Crypto-Pay-API-Token': CRYPTO_API_TOKEN
    }

    try:
        response = requests.post('https://pay.crypt.bot/api/createInvoice', json=data, headers=headers)

        if response.status_code == 200:
            invoice_data = response.json()
            invoice_url = invoice_data.get('result', {}).get('bot_invoice_url', None)
            invoice_id = invoice_data.get('result', {}).get('invoice_id', None)

            if invoice_url and invoice_id:
                user_invoices[user_id] = {'invoice_id': invoice_id, 'amount': amount}

                markup = InlineKeyboardMarkup()
                payment_button = InlineKeyboardButton(text="Перейти к оплате", url=invoice_url)
                check_button = InlineKeyboardButton(text="Проверить статус", callback_data="check_payment")
                markup.add(payment_button, check_button)

                bot.send_message(user_id, f"Для оплаты {amount} RUB перейдите по следующей ссылке: {invoice_url}\nВ CryptoBot вам будут предложены различные способы оплаты.", reply_markup=markup)
            else:
                bot.send_message(user_id, "Не удалось получить ссылку для оплаты.")
        else:
            bot.send_message(user_id, f'Ошибка при создании инвойса: {response.text}')

    except requests.exceptions.RequestException as e:
        bot.send_message(user_id, f'Ошибка при подключении к платежной системе: {str(e)}')

# Проверка статуса инвойса
def check_payment_status(call):
    user_id = call.message.chat.id
    invoice_id = user_invoices.get(user_id, {}).get('invoice_id')

    if not invoice_id:
        bot.send_message(user_id, "Не удалось найти инвойс для проверки.")
        return

    current_time = time.time()
    if user_id in last_check_time and current_time - last_check_time[user_id] < 300:
        bot.send_message(user_id, "Пожалуйста, подождите 5 минут перед следующей проверкой статуса.")
        return

    params = {'invoice_ids': invoice_id}
    headers = {'Crypto-Pay-API-Token': CRYPTO_API_TOKEN}

    try:
        response = requests.get('https://pay.crypt.bot/api/getInvoices', headers=headers, params=params)

        if response.status_code == 200:
            invoice_data = response.json()
            invoices = invoice_data.get('result', {}).get('items', [])

            if invoices:
                status = invoices[0].get('status')
                if status == 'paid':
                    bot.send_message(user_id, "Оплата успешно выполнена! Подписка активирована.", parse_mode="HTML")
                    set_premium_status(user_id, True)
                    print(f"Премиум подписка активирована для пользователя {user_id}")

                    # Удаляем инвойс после успешной оплаты
                    del user_invoices[user_id]
                    # Показываем кнопку для премиум настроек
                    show_premium_settings(user_id)
                elif status == 'expired':
                    bot.send_message(user_id, "Срок действия счета истек.")
                else:
                    bot.send_message(user_id, "Инвойс еще не оплачен. Пожалуйста, подождите.")
            else:
                bot.send_message(user_id, "Инвойс не найден.")
        else:
            bot.send_message(user_id, f'Ошибка при получении статуса инвойса: {response.text}')

    except requests.exceptions.RequestException as e:
        bot.send_message(user_id, f'Ошибка при подключении к платежной системе: {str(e)}')

    last_check_time[user_id] = current_time

# Премиум настройки
def show_premium_settings(user_id):
    ensure_user_loaded(user_id)
    preference = user_data[user_id].get("search_gender") or DEFAULT_SEARCH_GENDER
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("Любой пол", callback_data="search_gender_any"))
    markup.add(InlineKeyboardButton("Искать мальчиков 👦", callback_data="search_gender_male"))
    markup.add(InlineKeyboardButton("Искать девочек 👩", callback_data="search_gender_female"))
    bot.send_message(user_id, f"Премиум настройки:\nТекущий выбор: {preference}", reply_markup=markup)

# Поиск собеседников
def show_stop_search_button(chat_id):
    markup = ReplyKeyboardMarkup(resize_keyboard=True)
    stop_button = KeyboardButton("❌ Остановить поиск собеседника")
    markup.add(stop_button)
    bot.send_message(chat_id, "Поиск собеседника... Нажмите кнопку, чтобы остановить поиск.", reply_markup=markup)


def _user_preference(user_id):
    data = user_data.get(user_id, {})
    if data.get("premium"):
        return data.get("search_gender") or DEFAULT_SEARCH_GENDER
    return DEFAULT_SEARCH_GENDER


def _user_gender(user_id):
    return user_data.get(user_id, {}).get("gender")


def can_users_chat(user_a, user_b):
    gender_a = _user_gender(user_a)
    gender_b = _user_gender(user_b)
    if gender_a is None or gender_b is None:
        return False

    pref_a = _user_preference(user_a)
    pref_b = _user_preference(user_b)

    if pref_a != DEFAULT_SEARCH_GENDER and gender_b != pref_a:
        return False
    if pref_b != DEFAULT_SEARCH_GENDER and gender_a != pref_b:
        return False
    return True


def find_partner_for_user(user_id):
    if not waiting_users:
        return None
    for partner_id in list(waiting_users):
        if partner_id == user_id:
            continue
        if can_users_chat(user_id, partner_id):
            waiting_users.remove(partner_id)
            return partner_id
    return None


def send_chat_controls(chat_id):
    bot.send_message(
        chat_id,
        "🔥 Собеседник найден! Начинайте общение.\n"
        "/next — найти другого собеседника\n"
        "/stop — закончить диалог\n"
        "/start — вернуться в меню",
        reply_markup=ReplyKeyboardRemove()
    )


def connect_users(user_id, partner_id):
    chat_partners[user_id] = partner_id
    chat_partners[partner_id] = user_id
    send_chat_controls(user_id)
    send_chat_controls(partner_id)


def begin_search_for_user(user_id):
    ensure_user_loaded(user_id)
    if user_id in waiting_users:
        bot.send_message(user_id, "Вы уже в очереди. Ожидайте собеседника.")
        return

    bot.send_message(user_id, "Поиск начат. Кнопки больше не доступны.", reply_markup=ReplyKeyboardRemove())
    show_stop_search_button(user_id)

    partner_id = find_partner_for_user(user_id)
    if partner_id:
        connect_users(user_id, partner_id)
    else:
        waiting_users.add(user_id)
        bot.send_message(user_id, "Вы добавлены в очередь. Ожидайте собеседника.")

@bot.message_handler(func=lambda message: message.text == "Начать поиск 🔍")
def start_search(message):
    user_id = message.chat.id
    ensure_user_loaded(user_id)
    if not is_user_subscribed(user_id):
        return

    if not user_data[user_id]["gender"]:
        ask_gender(user_id)
        return

    begin_search_for_user(user_id)

# Остановить поиск
@bot.message_handler(func=lambda message: message.text == "❌ Остановить поиск собеседника")
def stop_search(message):
    user_id = message.chat.id
    if user_id in waiting_users:
        waiting_users.remove(user_id)
        bot.send_message(user_id, "Поиск собеседника остановлен 🥲.")
    else:
        bot.send_message(user_id, "Вы не в поиске🤚.")
    show_main_buttons(user_id)

# Обработка команды для получения количества пользователей
@bot.message_handler(func=lambda message: bool(message.text) and message.text.lower() == "alluser")
def handle_alluser(message):
    user_id = message.chat.id
    if not is_admin(user_id):
        bot.send_message(user_id, "У вас нет прав для получения этой информации.")
        return

    conn = sqlite3.connect(USER_DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM users')
    user_count = cursor.fetchone()[0]
    conn.close()
    bot.send_message(user_id, f"Количество пользователей, запустивших бота: {user_count}")

# Разрыв связи при команде "/stop"
@bot.message_handler(func=lambda message: message.text == "/stop")
def stop_chat(message):
    user_id = message.chat.id
    if user_id in chat_partners:
        partner_id = chat_partners[user_id]
        bot.send_message(user_id, "Вы разорвали связь.")
        bot.send_message(partner_id, "Собеседник разорвал с вами связь😔.")
        del chat_partners[user_id]
        del chat_partners[partner_id]
        show_main_buttons(user_id)
        show_main_buttons(partner_id)
    else:
        bot.send_message(user_id, "У вас нет активного диалога. Используйте 'Начать поиск 🔍'.")
        show_main_buttons(user_id)

@bot.message_handler(func=lambda message: message.text == "/next")
def next_chat(message):
    user_id = message.chat.id
    ensure_user_loaded(user_id)
    if user_id not in chat_partners:
        bot.send_message(user_id, "У вас нет активного диалога. Используйте 'Начать поиск 🔍'.")
        return

    partner_id = chat_partners[user_id]
    bot.send_message(user_id, "Ищем нового собеседника... 🔍")
    bot.send_message(partner_id, "Собеседник завершил диалог и начал новый поиск 🔍.")

    del chat_partners[user_id]
    del chat_partners[partner_id]
    show_main_buttons(partner_id)

    if not user_data.get(user_id, {}).get("gender"):
        ask_gender(user_id)
        return

    begin_search_for_user(user_id)

@bot.message_handler(func=lambda message: message.text == "Личный кабинет 👤")
def user_profile(message):
    user_id = message.chat.id
    ensure_user_loaded(user_id)
    if not is_user_subscribed(user_id):
        return

    if user_id in user_data:
        tg_username = message.from_user.username
        username_display = f"@{tg_username}" if tg_username else "Не указан"
        gender_value = user_data[user_id].get("gender")
        gender = gender_value or "Не выбран"
        premium_status = "Да" if user_data[user_id]["premium"] else "Нет"
        
        # Формируем сообщение с информацией о пользователе
        profile_message = (
            f"👤 <b>Личный кабинет</b>\n\n"
            f"📛 <b>Username:</b> {username_display}\n"
            f"💎 <b>Премиум подписка:</b> {premium_status}\n"
            f"🚻 <b>Пол:</b> {gender}\n\n"
            f"🔒 Анонимность: <b>всегда</b>"
        )
        bot.send_message(user_id, profile_message, parse_mode="HTML")
        if not gender_value:
            bot.send_message(user_id, "Пожалуйста, выберите пол для корректного подбора собеседников.")
            ask_gender(user_id)
    else:
        bot.send_message(user_id, "Не удалось найти информацию о вашем аккаунте. Пожалуйста, выберите пол.")
        ask_gender(user_id)  # Попросим выбрать пол, если этого ещё не сделали.

# Обработка сообщений (пересылка)
@bot.message_handler(
    func=_is_regular_incoming_message,
    content_types=['text', 'photo', 'video', 'audio', 'voice', 'document', 'sticker']
)
def forward_message(message):
    user_id = message.chat.id
    ensure_user_loaded(user_id)
    if not is_user_subscribed(user_id):
        return

    if user_id in user_states:
        # Админ выполняет действие, не мешаем обработчикам состояний
        return

    message_text = (message.text or "").strip()
    if message_text and is_control_command(message_text):
        # Команда уже будет обработана соответствующим хэндлером
        return

    if user_id in chat_partners:
        partner_id = chat_partners[user_id]

        # Пересылаем сообщение собеседнику (анонимно, без показа username)
        bot.copy_message(partner_id, user_id, message.message_id)
    else:
        show_main_buttons(user_id, "У вас сейчас нет собеседника. Нажмите 'Начать поиск 🔍', чтобы найти.")

# Функция для загрузки данных пользователя из базы данных в user_data
def load_user_data():
    conn = sqlite3.connect(USER_DB_PATH)
    cursor = conn.cursor()

    # Получаем все данные из базы
    cursor.execute('SELECT user_id, gender, premium, search_gender FROM users')
    users = cursor.fetchall()  # Получаем всех пользователей

    for user in users:
        user_id, gender, premium, search_gender = user
        user_data[user_id] = {
            "gender": gender,
            "premium": bool(premium),
            "search_gender": search_gender or DEFAULT_SEARCH_GENDER
        }

    conn.close()

def send_creator_branding_banner(chat_id):
    ensure_user_loaded(chat_id)
    user_info = user_data.get(chat_id, {})
    if user_info.get("premium"):
        return
    text = _build_creator_branding_text()
    if text:
        bot.send_message(chat_id, text)

# Admin functions
def is_admin(user_id):
    return user_id in ADMIN_IDS

def admin_menu():
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📣 Рассылка", callback_data="broadcast"))
    markup.add(InlineKeyboardButton("🚫 Бан/Разбан", callback_data="ban_menu"))
    markup.add(InlineKeyboardButton("📊 Статистика", callback_data="stats"))
    return markup

def ban_menu():
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("➕ Забанить", callback_data="ban_add"))
    markup.add(InlineKeyboardButton("♻️ Разбанить", callback_data="ban_remove"))
    markup.add(InlineKeyboardButton("📋 Список банов", callback_data="ban_list"))
    markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
    return markup


def handle_admin_callback(call):
    user_id = call.from_user.id
    if not is_admin(user_id):
        bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
        return True
    data = call.data

    def edit_panel(text, markup):
        try:
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
        except Exception:
            bot.send_message(call.message.chat.id, text, reply_markup=markup)

    if data == "broadcast":
        user_states[user_id] = 'waiting_broadcast'
        bot.answer_callback_query(call.id)
        bot.send_message(
            user_id,
            "📣 Введите текст рассылки. Сообщение получат все активные пользователи.",
        )
        return True

    if data == "ban_menu":
        bot.answer_callback_query(call.id)
        edit_panel("🚫 Управление банами:", ban_menu())
        return True

    if data == "ban_add":
        user_states[user_id] = 'waiting_ban'
        bot.answer_callback_query(call.id)
        bot.send_message(
            user_id,
            "Введите ID пользователя и причину бана через пробел.\nНапример: <code>123456789 спам</code>",
            parse_mode="HTML",
        )
        return True

    if data == "ban_remove":
        user_states[user_id] = 'waiting_unban'
        bot.answer_callback_query(call.id)
        bot.send_message(user_id, "Введите ID пользователя для разблокировки.")
        return True

    if data == "ban_list":
        conn = sqlite3.connect(USER_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT user_id, reason, created_at FROM bans ORDER BY created_at DESC LIMIT 20"
        )
        rows = cursor.fetchall()
        conn.close()
        if not rows:
            text = "🚫 Список банов пуст."
        else:
            lines = ["🚫 Активные баны:", ""]
            for banned_id, reason, created_at in rows:
                reason_text = escape(reason or "Без причины")
                timestamp = escape(created_at or "")
                lines.append(f"<b>{banned_id}</b> — {reason_text}")
                if timestamp:
                    lines.append(f"└ {timestamp}")
        bot.answer_callback_query(call.id)
        bot.send_message(user_id, "\n".join(lines) if rows else text, parse_mode="HTML")
        return True

    if data == "stats":
        conn = sqlite3.connect(USER_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE premium = 1")
        premium_users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE banned = 1")
        banned_users = cursor.fetchone()[0]
        conn.close()
        waiting = len(waiting_users)
        active_pairs = len(chat_partners) // 2
        stats_text = (
            "📊 Статистика бота:\n"
            f"• Всего пользователей: {total_users}\n"
            f"• Премиум подписчиков: {premium_users}\n"
            f"• Заблокировано: {banned_users}\n"
            f"• В очереди: {waiting}\n"
            f"• Активных диалогов: {active_pairs}"
        )
        bot.answer_callback_query(call.id)
        bot.send_message(user_id, stats_text)
        return True

    if data == "admin_back":
        bot.answer_callback_query(call.id)
        edit_panel("⚙️ Админ панель:", admin_menu())
        return True

    return False

@bot.message_handler(commands=['start'])
def start(message):
    user_id = message.chat.id
    ensure_user_loaded(user_id)
    if is_banned(user_id):
        bot.send_message(user_id, "🚫 Вы заблокированы.")
        return
    if not is_user_subscribed(user_id):
        return

    send_creator_branding_banner(user_id)
    bot.send_message(user_id, WELCOME_MESSAGE)

    if not user_data[user_id]["gender"]:
        ask_gender(user_id)
    show_main_buttons(user_id)

# Admin panel handler
@bot.message_handler(func=lambda message: message.text == "⚙️ Админка")
def admin_panel(message):
    if not is_admin(message.from_user.id):
        return
    bot.send_message(message.chat.id, "⚙️ Админ панель:", reply_markup=admin_menu())

# Запуск бота
load_user_data()
bot.infinity_polling() #сделай чтобы премиум подписка сохранялась в базу данных SQlite а также почему премиум поиск по полу соединяет с рандом челами? Надо же чтобы пол был который выбран