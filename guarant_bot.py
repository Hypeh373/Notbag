import os
import sqlite3
import logging
import asyncio
import json
import threading
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from html import escape

import telebot
from telebot import types
from aiocryptopay import AioCryptoPay, Networks
import aiohttp

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s:%(name)s:%(message)s')

# -----------------------------------------------------------------------------
# ----------------------------- НАСТРОЙКИ БОТА --------------------------------
# -----------------------------------------------------------------------------
BOT_TOKEN = os.getenv('GUARANT_BOT_TOKEN', 'PASTE_YOUR_TELEGRAM_BOT_TOKEN')
CRYPTO_PAY_TOKEN = os.getenv('GUARANT_CRYPTO_PAY_TOKEN', '')
ADMIN_IDS = [int(x) for x in os.getenv('GUARANT_ADMIN_IDS', '123456789').split(',') if x.strip().isdigit()]
DB_PATH = os.getenv('GUARANT_DB_PATH', 'guarant.db')
FLYER_API_KEY = os.getenv('GUARANT_FLYER_API_KEY', '')

# Комиссия гаранта (в %)
GUARANT_FEE = float(os.getenv('GUARANT_FEE', '5.0'))
# Минимальная сумма сделки
MIN_DEAL_AMOUNT = float(os.getenv('MIN_DEAL_AMOUNT', '1.0'))

_raw_creator = os.getenv('CREATOR_USERNAME', '@TGCreator_bot').strip() or '@TGCreator_bot'
if 't.me/' in _raw_creator:
    _username_part = _raw_creator.split('t.me/')[-1].split('/')[0].split('?')[0].strip()
    CREATOR_USERNAME = f"@{_username_part}" if _username_part else '@TGCreator_bot'
elif _raw_creator.startswith('@'):
    CREATOR_USERNAME = _raw_creator
else:
    CREATOR_USERNAME = f"@{_raw_creator}"
HIDE_BRANDING = os.getenv('GUARANT_HIDE_BRANDING', '0') == '1'
BRANDING_TEXT = f"\n\n🤖 Бот создан с помощью {CREATOR_USERNAME}" if not HIDE_BRANDING else ""

# -----------------------------------------------------------------------------
bot = telebot.TeleBot(BOT_TOKEN)

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row

async_loop = asyncio.new_event_loop()
crypto_pay_client: Optional[AioCryptoPay] = None
user_states: Dict[int, Dict[str, Any]] = {}


def run_async_task(coro):
    def _callback(fut):
        try:
            fut.result()
        except Exception as exc:
            logging.error(f"Async task failed: {exc}")
    future = asyncio.run_coroutine_threadsafe(coro, async_loop)
    future.add_done_callback(_callback)
    return future


def init_async_loop():
    def _run_loop(loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()
    thread = threading.Thread(target=_run_loop, args=(async_loop,), daemon=True)
    thread.start()


def init_db():
    with conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE,
            username TEXT,
            first_name TEXT,
            rating REAL DEFAULT 5.0,
            deals_completed INTEGER DEFAULT 0,
            deals_failed INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        
        conn.execute("""CREATE TABLE IF NOT EXISTS guarants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE,
            username TEXT,
            first_name TEXT,
            added_by INTEGER,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        
        conn.execute("""CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            buyer_id INTEGER,
            seller_id INTEGER,
            guarant_id INTEGER,
            amount REAL,
            fee REAL,
            description TEXT,
            status TEXT DEFAULT 'pending_guarant',
            invoice_id INTEGER,
            buyer_confirmed BOOLEAN DEFAULT FALSE,
            seller_confirmed BOOLEAN DEFAULT FALSE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            guarant_confirmed_at TEXT,
            paid_at TEXT,
            completed_at TEXT,
            cancelled_at TEXT
        )""")
        
        conn.execute("""CREATE TABLE IF NOT EXISTS disputes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER,
            initiator_id INTEGER,
            reason TEXT,
            status TEXT DEFAULT 'open',
            guarant_decision TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            resolved_at TEXT
        )""")
        
        conn.execute("""CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        
        conn.execute("""CREATE TABLE IF NOT EXISTS payments (
            invoice_id INTEGER PRIMARY KEY,
            deal_id INTEGER,
            user_id INTEGER,
            amount REAL,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        
        conn.execute("""CREATE TABLE IF NOT EXISTS op_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT UNIQUE,
            channel_name TEXT,
            channel_link TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    
    # Применяем настройки из env
    if CRYPTO_PAY_TOKEN and CRYPTO_PAY_TOKEN.strip() and CRYPTO_PAY_TOKEN not in ('', 'YOUR_CRYPTO_PAY_API_TOKEN', '—'):
        set_setting('crypto_pay_token', CRYPTO_PAY_TOKEN.strip())
        logging.info("Crypto Pay токен применён из переменной окружения")


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    cur = conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cur.fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str):
    with conn:
        conn.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))


def ensure_user(user: telebot.types.User) -> sqlite3.Row:
    cur = conn.execute("SELECT * FROM users WHERE tg_id = ?", (user.id,))
    row = cur.fetchone()
    if row:
        if row['username'] != user.username:
            with conn:
                conn.execute("UPDATE users SET username = ? WHERE tg_id = ?", (user.username, user.id))
        return conn.execute("SELECT * FROM users WHERE tg_id = ?", (user.id,)).fetchone()
    with conn:
        conn.execute("INSERT INTO users (tg_id, username, first_name) VALUES (?, ?, ?)",
                     (user.id, user.username, user.first_name))
    return ensure_user(user)


def get_guarants() -> List[sqlite3.Row]:
    return conn.execute("SELECT * FROM guarants").fetchall()


# ======================== ОП ФУНКЦИИ ========================
def get_op_channels() -> List[Dict]:
    """Возвращает список каналов для ОП"""
    rows = conn.execute("SELECT * FROM op_channels ORDER BY id").fetchall()
    return [dict(r) for r in rows]


def add_op_channel(channel_id: str, channel_name: str, channel_link: str) -> bool:
    """Добавляет канал для ОП"""
    try:
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO op_channels (channel_id, channel_name, channel_link) VALUES (?, ?, ?)",
                (channel_id, channel_name, channel_link)
            )
        return True
    except Exception as e:
        logging.error(f"Failed to add OP channel: {e}")
        return False


def remove_op_channel(channel_id: str) -> bool:
    """Удаляет канал из ОП"""
    try:
        with conn:
            conn.execute("DELETE FROM op_channels WHERE channel_id = ?", (channel_id,))
        return True
    except:
        return False


def is_op_enabled() -> bool:
    """Проверяет включена ли ОП"""
    return len(get_op_channels()) > 0 or bool(FLYER_API_KEY)


def check_user_subscriptions(user_id: int) -> tuple:
    """Проверяет подписку пользователя на все ОП каналы.
    Возвращает (all_subscribed: bool, not_subscribed: List[Dict])
    """
    channels = get_op_channels()
    if not channels:
        return True, []
    
    not_subscribed = []
    
    # Проверяем все каналы (включая Flyer API каналы, которые были добавлены при старте)
    for ch in channels:
        try:
            # Убираем префикс flyer_ если есть
            channel_id = ch['channel_id']
            if channel_id.startswith('flyer_'):
                channel_id = channel_id.replace('flyer_', '', 1)
            
            # Используем быструю проверку с таймаутом
            try:
                member = bot.get_chat_member(channel_id, user_id)
                if member.status in ['left', 'kicked']:
                    not_subscribed.append(ch)
            except telebot.apihelper.ApiTelegramException as e:
                if "chat not found" in str(e).lower() or "user not found" in str(e).lower():
                    # Канал не найден или пользователь не найден - пропускаем
                    logging.warning(f"Channel or user not found: {channel_id}")
                else:
                    # Другая ошибка - считаем что не подписан
                    not_subscribed.append(ch)
        except Exception as e:
            logging.warning(f"Cannot check subscription for {ch['channel_id']}: {e}")
            # При ошибке считаем что не подписан для безопасности
            not_subscribed.append(ch)
    
    return len(not_subscribed) == 0, not_subscribed


def build_subscription_keyboard(not_subscribed: List[Dict]) -> types.InlineKeyboardMarkup:
    """Создает клавиатуру с кнопками подписки"""
    markup = types.InlineKeyboardMarkup()
    for ch in not_subscribed:
        link = ch.get('channel_link') or f"https://t.me/{ch['channel_id'].lstrip('@')}"
        name = ch.get('channel_name') or ch['channel_id']
        markup.add(types.InlineKeyboardButton(f"📢 {name}", url=link))
    markup.add(types.InlineKeyboardButton("✅ Я подписался", callback_data="check_op_subscription"))
    return markup


def is_guarant(user_id: int) -> bool:
    row = conn.execute("SELECT id FROM guarants WHERE tg_id = ?", (user_id,)).fetchone()
    return row is not None


def get_crypto_client() -> Optional[AioCryptoPay]:
    global crypto_pay_client
    token = get_setting('crypto_pay_token') or CRYPTO_PAY_TOKEN
    if not token or token.strip() in ('', 'YOUR_CRYPTO_PAY_API_TOKEN', '—'):
        return None
    
    token = token.strip()
    if ':' not in token:
        return None
    
    if crypto_pay_client is None:
        try:
            crypto_pay_client = AioCryptoPay(token=token, network=Networks.MAIN_NET)
            logging.info("Crypto Pay client initialized")
        except RuntimeError as exc:
            if "no current event loop" in str(exc).lower():
                try:
                    future = asyncio.run_coroutine_threadsafe(
                        _async_create_client(token), async_loop
                    )
                    crypto_pay_client = future.result(timeout=5)
                except Exception as e:
                    logging.error(f"Async client creation failed: {e}")
                    return None
            else:
                logging.error(f"Crypto Pay init error: {exc}")
                return None
        except Exception as exc:
            logging.error(f"Crypto Pay init error: {exc}")
            return None
    elif hasattr(crypto_pay_client, 'token') and crypto_pay_client.token != token:
        try:
            crypto_pay_client = AioCryptoPay(token=token, network=Networks.MAIN_NET)
        except Exception as exc:
            logging.error(f"Crypto Pay reinit error: {exc}")
            return None
    
    return crypto_pay_client


async def _async_create_client(token: str) -> Optional[AioCryptoPay]:
    try:
        client = AioCryptoPay(token=token, network=Networks.MAIN_NET)
        await client.get_me()
        return client
    except Exception as e:
        logging.error(f"Async client creation failed: {e}")
        return None


def main_menu() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("➕ Создать сделку", "📋 Мои сделки")
    markup.row("👤 Профиль", "ℹ️ О проекте")
    return markup


def format_deal(deal: Dict) -> str:
    status_icons = {
        'pending_guarant': '⏳',
        'guarant_confirmed': '✅',
        'pending_payment': '💳',
        'paid': '💰',
        'waiting_completion': '⏰',
        'completed': '✅',
        'cancelled': '❌',
        'dispute': '⚖️'
    }
    status_texts = {
        'pending_guarant': 'Ожидает подтверждения гаранта',
        'guarant_confirmed': 'Гарант подтвердил, ожидает оплаты',
        'pending_payment': 'Ожидает оплаты',
        'paid': 'Оплачено, ожидайте гаранта',
        'waiting_completion': 'Ожидает подтверждения выполнения',
        'completed': 'Завершена',
        'cancelled': 'Отменена',
        'dispute': 'Спор'
    }
    icon = status_icons.get(deal['status'], '📄')
    status_text = status_texts.get(deal['status'], deal['status'])
    
    fee_amount = deal.get('fee') or (deal['amount'] * GUARANT_FEE / 100)
    total = deal['amount'] + fee_amount
    
    text = (
        f"{icon} <b>Сделка #{deal['id']}</b>\n\n"
        f"💰 Сумма: {deal['amount']:.2f} USDT\n"
        f"💸 Комиссия: {fee_amount:.2f} USDT ({GUARANT_FEE}%)\n"
        f"📊 Всего: {total:.2f} USDT\n"
        f"📝 Описание: {deal.get('description', 'Не указано')}\n"
        f"📌 Статус: {status_text}\n"
    )
    
    if deal.get('buyer_id'):
        buyer = conn.execute("SELECT username, first_name FROM users WHERE tg_id = ?", (deal['buyer_id'],)).fetchone()
        buyer_name = f"@{buyer['username']}" if buyer and buyer['username'] else f"ID: {deal['buyer_id']}"
        text += f"👤 Покупатель: {buyer_name}\n"
    
    if deal.get('seller_id'):
        seller = conn.execute("SELECT username, first_name FROM users WHERE tg_id = ?", (deal['seller_id'],)).fetchone()
        seller_name = f"@{seller['username']}" if seller and seller['username'] else f"ID: {deal['seller_id']}"
        text += f"🏪 Продавец: {seller_name}\n"
    
    if deal.get('guarant_id'):
        guarant = conn.execute("SELECT username, first_name FROM guarants WHERE tg_id = ?", (deal['guarant_id'],)).fetchone()
        if guarant:
            guarant_name = f"@{guarant['username']}" if guarant['username'] else f"ID: {deal['guarant_id']}"
            text += f"🛡️ Гарант: {guarant_name}\n"
    
    if deal['status'] == 'waiting_completion':
        buyer_confirmed = "✅" if deal.get('buyer_confirmed') else "❌"
        seller_confirmed = "✅" if deal.get('seller_confirmed') else "❌"
        text += f"\n📋 Подтверждения:\nПокупатель: {buyer_confirmed}\nПродавец: {seller_confirmed}"
    
    return text


@bot.message_handler(commands=['start'])
def handle_start(message: types.Message):
    ensure_user(message.from_user)
    
    # Проверяем ОП
    if is_op_enabled():
        subscribed, not_sub = check_user_subscriptions(message.from_user.id)
        if not subscribed:
            text = "📋 <b>Для использования бота подпишитесь на каналы:</b>"
            bot.send_message(message.chat.id, text, parse_mode="HTML", 
                           reply_markup=build_subscription_keyboard(not_sub))
            return
    
    text = (
        "🛡️ <b>Гарант-бот</b>\n\n"
        "Безопасные сделки с гарантом!\n"
        "Создавайте сделки, оплачивайте через бота, получайте защиту.\n\n"
        "💡 <b>Как это работает:</b>\n"
        "1. Создайте сделку с указанием продавца\n"
        "2. Гарант подтверждает сделку\n"
        "3. Оплатите сумму + комиссию гаранта\n"
        "4. После получения товара подтвердите выполнение\n"
        "5. Продавец получит оплату\n\n"
        f"💸 Комиссия гаранта: {GUARANT_FEE}%\n"
        f"💰 Минимальная сумма: {MIN_DEAL_AMOUNT} USDT\n\n"
        "📋 <b>Команды:</b>\n"
        "/confirm_deal - подтвердить выполнение сделки\n"
        "/open_dispute - открыть спор"
        f"{BRANDING_TEXT}"
    )
    bot.send_message(message.chat.id, text, reply_markup=main_menu(), parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "check_op_subscription")
def handle_check_op(call: types.CallbackQuery):
    subscribed, not_sub = check_user_subscriptions(call.from_user.id)
    if subscribed:
        bot.answer_callback_query(call.id, "✅ Спасибо за подписку!", show_alert=True)
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        text = (
            "🛡️ <b>Гарант-бот</b>\n\n"
            "Безопасные сделки с гарантом!\n\n"
            "💡 <b>Как это работает:</b>\n"
            "1. Создайте сделку с указанием продавца\n"
            "2. Гарант подтверждает сделку\n"
            "3. Оплатите сумму + комиссию гаранта\n"
            "4. После получения товара подтвердите выполнение\n"
            "5. Продавец получит оплату\n\n"
            f"💸 Комиссия гаранта: {GUARANT_FEE}%\n"
            f"💰 Минимальная сумма: {MIN_DEAL_AMOUNT} USDT"
            f"{BRANDING_TEXT}"
        )
        bot.send_message(call.message.chat.id, text, reply_markup=main_menu(), parse_mode="HTML")
    else:
        bot.answer_callback_query(call.id, "❌ Вы не подписаны на все каналы!", show_alert=True)
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id,
                                         reply_markup=build_subscription_keyboard(not_sub))
        except:
            pass


@bot.message_handler(commands=['confirm_deal'])
def handle_confirm_deal_cmd(message: types.Message):
    ensure_user(message.from_user)
    
    # Находим активные сделки пользователя
    deals = conn.execute(
        "SELECT * FROM deals WHERE (buyer_id=? OR seller_id=?) AND status='waiting_completion' ORDER BY id DESC",
        (message.from_user.id, message.from_user.id)
    ).fetchall()
    
    if not deals:
        bot.send_message(message.chat.id, "❌ У вас нет сделок, ожидающих подтверждения.", reply_markup=main_menu())
        return
    
    if len(deals) == 1:
        deal = deals[0]
        confirm_deal_completion(message.from_user.id, deal['id'], message.chat.id)
    else:
        markup = types.InlineKeyboardMarkup()
        for deal in deals[:10]:
            markup.add(types.InlineKeyboardButton(
                f"Сделка #{deal['id']} - {deal['amount']:.2f} USDT",
                callback_data=f"confirm_deal_{deal['id']}"
            ))
        bot.send_message(message.chat.id, "📋 Выберите сделку для подтверждения:", reply_markup=markup)


@bot.message_handler(commands=['open_dispute'])
def handle_open_dispute_cmd(message: types.Message):
    ensure_user(message.from_user)
    
    deals = conn.execute(
        "SELECT * FROM deals WHERE (buyer_id=? OR seller_id=?) AND status IN ('paid', 'waiting_completion') ORDER BY id DESC",
        (message.from_user.id, message.from_user.id)
    ).fetchall()
    
    if not deals:
        bot.send_message(message.chat.id, "❌ У вас нет активных сделок для открытия спора.", reply_markup=main_menu())
        return
    
    if len(deals) == 1:
        deal = deals[0]
        user_states[message.from_user.id] = {'mode': 'dispute_reason', 'deal_id': deal['id']}
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))
        bot.send_message(message.chat.id, "📝 Опишите причину спора:", reply_markup=markup)
    else:
        markup = types.InlineKeyboardMarkup()
        for deal in deals[:10]:
            markup.add(types.InlineKeyboardButton(
                f"Сделка #{deal['id']} - {deal['amount']:.2f} USDT",
                callback_data=f"dispute_deal_{deal['id']}"
            ))
        bot.send_message(message.chat.id, "📋 Выберите сделку для открытия спора:", reply_markup=markup)


@bot.message_handler(commands=['admin'])
def handle_admin_cmd(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    send_admin_panel(message.chat.id)


def send_admin_panel(chat_id: int):
    total_deals = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
    active_deals = conn.execute("SELECT COUNT(*) FROM deals WHERE status IN ('pending_guarant', 'guarant_confirmed', 'pending_payment', 'paid', 'waiting_completion', 'dispute')").fetchone()[0]
    total_volume = conn.execute("SELECT COALESCE(SUM(amount), 0) FROM deals WHERE status='completed'").fetchone()[0]
    total_fees = conn.execute("SELECT COALESCE(SUM(fee), 0) FROM deals WHERE status='completed'").fetchone()[0]
    open_disputes = conn.execute("SELECT COUNT(*) FROM disputes WHERE status='open'").fetchone()[0]
    guarants_count = conn.execute("SELECT COUNT(*) FROM guarants").fetchone()[0]
    
    crypto_token = get_setting('crypto_pay_token') or CRYPTO_PAY_TOKEN
    crypto_status = "✅" if crypto_token and crypto_token.strip() not in ('', 'YOUR_CRYPTO_PAY_API_TOKEN', '—') and ':' in crypto_token else "❌"
    
    text = (
        "<b>⚙️ Админ-панель</b>\n\n"
        f"📊 Всего сделок: {total_deals}\n"
        f"🔄 Активных: {active_deals}\n"
        f"💰 Объём: {total_volume:.2f} USDT\n"
        f"💸 Комиссии: {total_fees:.2f} USDT\n"
        f"⚖️ Споров: {open_disputes}\n"
        f"🛡️ Гарантов: {guarants_count}\n"
        f"💳 Crypto Pay: {crypto_status}"
    )
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("💳 Настроить Crypto Pay", callback_data="admin_set_crypto"))
    markup.add(types.InlineKeyboardButton("🛡️ Управление гарантами", callback_data="admin_guarants"))
    markup.add(types.InlineKeyboardButton("📢 ОП каналы", callback_data="admin_op_channels"))
    markup.add(types.InlineKeyboardButton("⚖️ Споры", callback_data="admin_disputes"))
    markup.add(types.InlineKeyboardButton("🗑️ Очистить все сделки", callback_data="admin_clear_deals"))
    markup.add(types.InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"))
    markup.add(types.InlineKeyboardButton("📨 Рассылка", callback_data="admin_broadcast"))
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_"))
def handle_admin_callbacks(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id)
        return
    
    if call.data == "admin_set_crypto":
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="admin_cancel"))
        msg = bot.send_message(call.message.chat.id,
            "💳 Отправьте токен Crypto Pay (@CryptoBot → Crypto Pay → Create App):",
            reply_markup=markup)
        user_states[call.from_user.id] = {'mode': 'set_crypto', 'message_id': msg.message_id}
        bot.answer_callback_query(call.id)
    
    elif call.data == "admin_guarants":
        guarants = get_guarants()
        text = f"🛡️ <b>Гаранты ({len(guarants)})</b>\n\n"
        for g in guarants:
            name = f"@{g['username']}" if g['username'] else f"ID: {g['tg_id']}"
            text += f"• {name}\n"
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("➕ Добавить гаранта", callback_data="admin_add_guarant"))
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
        bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)
        bot.answer_callback_query(call.id)
    
    elif call.data == "admin_add_guarant":
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="admin_cancel"))
        msg = bot.send_message(call.message.chat.id,
            "🛡️ Отправьте @username или ID пользователя для добавления в гаранты:",
            reply_markup=markup)
        user_states[call.from_user.id] = {'mode': 'add_guarant', 'message_id': msg.message_id}
        bot.answer_callback_query(call.id)
    
    elif call.data == "admin_op_channels":
        channels = get_op_channels()
        text = f"📢 <b>ОП каналы ({len(channels)})</b>\n\n"
        for ch in channels:
            text += f"• {ch.get('channel_name', ch['channel_id'])}\n"
        if FLYER_API_KEY:
            text += "\n✅ Flyer API подключен"
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("➕ Добавить канал", callback_data="admin_add_op_channel"))
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
        bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)
        bot.answer_callback_query(call.id)
    
    elif call.data == "admin_add_op_channel":
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="admin_cancel"))
        msg = bot.send_message(call.message.chat.id,
            "📢 Отправьте @username канала или его ID:",
            reply_markup=markup)
        user_states[call.from_user.id] = {'mode': 'add_op_channel', 'message_id': msg.message_id}
        bot.answer_callback_query(call.id)
    
    elif call.data == "admin_clear_deals":
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("✅ Да, очистить", callback_data="admin_clear_deals_confirm"),
            types.InlineKeyboardButton("❌ Отмена", callback_data="admin_back")
        )
        count = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
        bot.send_message(call.message.chat.id, 
            f"⚠️ <b>Внимание!</b>\n\nВы уверены, что хотите удалить все сделки ({count} шт.)?\n\nЭто действие необратимо!",
            parse_mode="HTML", reply_markup=markup)
        bot.answer_callback_query(call.id)
    
    elif call.data == "admin_clear_deals_confirm":
        with conn:
            conn.execute("DELETE FROM deals")
            conn.execute("DELETE FROM disputes")
            conn.execute("DELETE FROM payments")
        bot.send_message(call.message.chat.id, "✅ Все сделки очищены!")
        send_admin_panel(call.message.chat.id)
        bot.answer_callback_query(call.id)
    
    elif call.data == "admin_disputes":
        disputes = conn.execute(
            "SELECT d.*, de.description, de.amount FROM disputes d "
            "JOIN deals de ON d.deal_id = de.id WHERE d.status='open' ORDER BY d.id DESC"
        ).fetchall()
        
        if not disputes:
            bot.answer_callback_query(call.id, "Нет открытых споров", show_alert=True)
            return
        
        for dispute in disputes[:5]:
            deal = conn.execute("SELECT * FROM deals WHERE id=?", (dispute['deal_id'],)).fetchone()
            initiator = conn.execute("SELECT username FROM users WHERE tg_id=?", (dispute['initiator_id'],)).fetchone()
            initiator_name = f"@{initiator['username']}" if initiator and initiator['username'] else f"ID: {dispute['initiator_id']}"
            
            text = (
                f"⚖️ <b>Спор #{dispute['id']}</b>\n\n"
                f"📄 Сделка: #{dispute['deal_id']}\n"
                f"💰 Сумма: {deal['amount']:.2f} USDT\n"
                f"👤 Инициатор: {initiator_name}\n"
                f"📝 Причина: {dispute['reason']}"
            )
            
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("✅ В пользу покупателя", callback_data=f"dispute_resolve_{dispute['id']}_buyer"),
                types.InlineKeyboardButton("✅ В пользу продавца", callback_data=f"dispute_resolve_{dispute['id']}_seller")
            )
            bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)
        
        bot.answer_callback_query(call.id)
    
    elif call.data == "admin_stats":
        total_deals = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
        completed = conn.execute("SELECT COUNT(*) FROM deals WHERE status='completed'").fetchone()[0]
        cancelled = conn.execute("SELECT COUNT(*) FROM deals WHERE status='cancelled'").fetchone()[0]
        total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        
        text = (
            "<b>📊 Статистика</b>\n\n"
            f"👥 Пользователей: {total_users}\n"
            f"📄 Всего сделок: {total_deals}\n"
            f"✅ Завершено: {completed}\n"
            f"❌ Отменено: {cancelled}"
        )
        try:
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                                 reply_markup=call.message.reply_markup, parse_mode="HTML")
        except:
            pass
        bot.answer_callback_query(call.id)
    
    elif call.data == "admin_broadcast":
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="admin_cancel"))
        msg = bot.send_message(call.message.chat.id, "📨 Отправьте текст рассылки:", reply_markup=markup)
        user_states[call.from_user.id] = {'mode': 'broadcast', 'message_id': msg.message_id}
        bot.answer_callback_query(call.id)
    
    elif call.data == "admin_back":
        send_admin_panel(call.message.chat.id)
        bot.answer_callback_query(call.id)
    
    elif call.data == "admin_cancel":
        user_states.pop(call.from_user.id, None)
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        send_admin_panel(call.message.chat.id)
        bot.answer_callback_query(call.id)
    
    elif call.data.startswith("dispute_resolve_"):
        parts = call.data.split("_")
        dispute_id = int(parts[2])
        winner = parts[3]  # buyer or seller
        
        dispute = conn.execute("SELECT * FROM disputes WHERE id=?", (dispute_id,)).fetchone()
        if not dispute or dispute['status'] != 'open':
            bot.answer_callback_query(call.id, "Спор уже решён", show_alert=True)
            return
        
        deal = conn.execute("SELECT * FROM deals WHERE id=?", (dispute['deal_id'],)).fetchone()
        if not deal or deal['status'] != 'dispute':
            bot.answer_callback_query(call.id, "Сделка не в статусе спора", show_alert=True)
            return
        
        # Решаем спор
        with conn:
            conn.execute("UPDATE disputes SET status='resolved', guarant_decision=?, resolved_at=? WHERE id=?",
                        (winner, datetime.now().isoformat(), dispute_id))
            conn.execute("UPDATE deals SET status=? WHERE id=?",
                        ('completed' if winner == 'seller' else 'cancelled', dispute['deal_id']))
        
        # Выплачиваем средства
        if winner == 'seller':
            run_async_task(transfer_to_seller(dispute['deal_id']))
        else:
            run_async_task(refund_to_buyer(dispute['deal_id']))
        
        bot.answer_callback_query(call.id, f"Спор решён в пользу {'продавца' if winner == 'seller' else 'покупателя'}", show_alert=True)
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        send_admin_panel(call.message.chat.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("guarant_"))
def handle_guarant_callbacks(call: types.CallbackQuery):
    if not is_guarant(call.from_user.id):
        bot.answer_callback_query(call.id, "Вы не являетесь гарантом", show_alert=True)
        return
    
    if call.data.startswith("guarant_confirm_deal_"):
        deal_id = int(call.data.split("_")[-1])
        deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
        
        if not deal or deal['status'] != 'pending_guarant':
            bot.answer_callback_query(call.id, "Сделка уже обработана", show_alert=True)
            return
        
        # Подтверждаем сделку
        with conn:
            conn.execute("UPDATE deals SET status='guarant_confirmed', guarant_id=?, guarant_confirmed_at=? WHERE id=?",
                        (call.from_user.id, datetime.now().isoformat(), deal_id))
        
        # Создаем счет для оплаты
        create_payment_invoice(deal['buyer_id'], deal_id, deal['amount'] + deal['fee'], deal['buyer_id'])
        
        bot.answer_callback_query(call.id, "Сделка подтверждена! Счет отправлен покупателю.", show_alert=True)
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
    
    elif call.data.startswith("guarant_reject_deal_"):
        deal_id = int(call.data.split("_")[-1])
        deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
        
        if not deal or deal['status'] != 'pending_guarant':
            bot.answer_callback_query(call.id, "Сделка уже обработана", show_alert=True)
            return
        
        with conn:
            conn.execute("UPDATE deals SET status='cancelled', cancelled_at=? WHERE id=?",
                        (datetime.now().isoformat(), deal_id))
        
        # Уведомляем участников
        try:
            bot.send_message(deal['buyer_id'], f"❌ Сделка #{deal_id} отклонена гарантом.")
        except:
            pass
        try:
            bot.send_message(deal['seller_id'], f"❌ Сделка #{deal_id} отклонена гарантом.")
        except:
            pass
        
        bot.answer_callback_query(call.id, "Сделка отклонена", show_alert=True)
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_deal_"))
def handle_confirm_deal_callback(call: types.CallbackQuery):
    deal_id = int(call.data.split("_")[-1])
    confirm_deal_completion(call.from_user.id, deal_id, call.message.chat.id)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("dispute_deal_"))
def handle_dispute_deal_callback(call: types.CallbackQuery):
    deal_id = int(call.data.split("_")[-1])
    user_states[call.from_user.id] = {'mode': 'dispute_reason', 'deal_id': deal_id}
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))
    bot.send_message(call.message.chat.id, "📝 Опишите причину спора:", reply_markup=markup)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data == "cancel_action")
def handle_cancel_action(call: types.CallbackQuery):
    user_states.pop(call.from_user.id, None)
    bot.send_message(call.message.chat.id, "❌ Действие отменено.", reply_markup=main_menu())
    bot.answer_callback_query(call.id)


def confirm_deal_completion(user_id: int, deal_id: int, chat_id: int):
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    
    if not deal:
        bot.send_message(chat_id, "Сделка не найдена", reply_markup=main_menu())
        return
    
    if deal['status'] != 'waiting_completion':
        bot.send_message(chat_id, "Сделка не в статусе ожидания подтверждения", reply_markup=main_menu())
        return
    
    if deal['buyer_id'] == user_id:
        field = 'buyer_confirmed'
    elif deal['seller_id'] == user_id:
        field = 'seller_confirmed'
    else:
        bot.send_message(chat_id, "Вы не являетесь участником этой сделки", reply_markup=main_menu())
        return
    
    # Обновляем подтверждение
    with conn:
        conn.execute(f"UPDATE deals SET {field}=TRUE WHERE id=?", (deal_id,))
        deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    
    # Проверяем оба подтверждения
    if deal['buyer_confirmed'] and deal['seller_confirmed']:
        # Оба подтвердили - автоматически завершаем
        with conn:
            conn.execute("UPDATE deals SET status='completed', completed_at=? WHERE id=?",
                        (datetime.now().isoformat(), deal_id))
        
        # Переводим средства продавцу
        run_async_task(transfer_to_seller(deal_id))
        
        # Обновляем рейтинги
        with conn:
            conn.execute("UPDATE users SET deals_completed=deals_completed+1, rating=rating+0.1 WHERE tg_id IN (?, ?)",
                        (deal['buyer_id'], deal['seller_id']))
        
        bot.send_message(chat_id, "✅ Сделка завершена! Средства переведены продавцу.", reply_markup=main_menu())
        
        # Уведомляем другого участника
        other_id = deal['seller_id'] if user_id == deal['buyer_id'] else deal['buyer_id']
        try:
            bot.send_message(other_id, f"✅ Сделка #{deal_id} завершена! Средства переведены.")
        except:
            pass
    else:
        bot.send_message(chat_id, "✅ Ваше подтверждение получено! Ожидайте подтверждения второй стороны.")


# Обработчики кнопок меню должны быть ПЕРЕД handle_states
# Используем более строгую проверку текста
@bot.message_handler(func=lambda m: m.text is not None and (m.text == "➕ Создать сделку" or m.text.strip() == "➕ Создать сделку"))
def cmd_create_deal(message: types.Message):
    logging.info(f"[CREATE_DEAL] Handler called by user {message.from_user.id}, text: '{message.text}', type: {type(message.text)}")
    
    # Очищаем старое состояние если есть
    if message.from_user.id in user_states:
        logging.info(f"[CREATE_DEAL] Clearing old state for user {message.from_user.id}")
        user_states.pop(message.from_user.id, None)
    
    try:
        ensure_user(message.from_user)
        
        # Создаем новое состояние
        user_states[message.from_user.id] = {'mode': 'create_deal', 'step': 'seller', 'data': {}}
        logging.info(f"[CREATE_DEAL] Created new state for user {message.from_user.id}")
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))
        
        bot.send_message(message.chat.id, 
            "🏪 Укажите продавца:\n\n"
            "Отправьте @username или числовой ID пользователя, с которым хотите совершить сделку.",
            reply_markup=markup)
        logging.info(f"[CREATE_DEAL] Message sent to user {message.from_user.id}")
    except Exception as e:
        logging.error(f"[CREATE_DEAL] Error: {e}", exc_info=True)
        try:
            bot.send_message(message.chat.id, "❌ Произошла ошибка. Попробуйте позже.", reply_markup=main_menu())
        except:
            pass


@bot.message_handler(func=lambda m: m.text == "📋 Мои сделки")
def cmd_my_deals(message: types.Message):
    ensure_user(message.from_user)
    user_states.pop(message.from_user.id, None)  # Очищаем состояние
    
    buyer_deals = conn.execute(
        "SELECT * FROM deals WHERE buyer_id=? ORDER BY id DESC LIMIT 10",
        (message.from_user.id,)
    ).fetchall()
    
    seller_deals = conn.execute(
        "SELECT * FROM deals WHERE seller_id=? ORDER BY id DESC LIMIT 10",
        (message.from_user.id,)
    ).fetchall()
    
    if not buyer_deals and not seller_deals:
        bot.send_message(message.chat.id, "📋 У вас пока нет сделок.", reply_markup=main_menu())
        return
    
    if buyer_deals:
        bot.send_message(message.chat.id, "📋 <b>Сделки (как покупатель):</b>", parse_mode="HTML")
        for deal in buyer_deals:
            show_deal_details(message.chat.id, deal['id'])
    
    if seller_deals:
        bot.send_message(message.chat.id, "📋 <b>Сделки (как продавец):</b>", parse_mode="HTML")
        for deal in seller_deals:
            show_deal_details(message.chat.id, deal['id'])


@bot.message_handler(func=lambda m: m.text == "👤 Профиль")
def cmd_profile(message: types.Message):
    ensure_user(message.from_user)
    user_states.pop(message.from_user.id, None)  # Очищаем состояние
    
    user = ensure_user(message.from_user)
    deals_completed = user['deals_completed'] or 0
    deals_failed = user['deals_failed'] or 0
    rating = user['rating'] or 5.0
    
    text = (
        f"👤 <b>Профиль</b>\n\n"
        f"🆔 ID: <code>{message.from_user.id}</code>\n"
        f"⭐ Рейтинг: {rating:.1f}/5.0\n"
        f"✅ Завершено: {deals_completed}\n"
        f"❌ Отменено: {deals_failed}"
    )
    bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=main_menu())


@bot.message_handler(func=lambda m: m.text == "ℹ️ О проекте")
def cmd_about(message: types.Message):
    ensure_user(message.from_user)
    user_states.pop(message.from_user.id, None)  # Очищаем состояние
    
    text = (
        "🛡️ <b>Гарант-бот</b>\n\n"
        "Безопасные сделки с гарантом!\n\n"
        "💡 <b>Как это работает:</b>\n"
        "1. Создайте сделку с указанием продавца\n"
        "2. Гарант подтверждает сделку\n"
        "3. Оплатите сумму + комиссию гаранта\n"
        "4. После получения товара подтвердите выполнение\n"
        "5. Продавец получит оплату\n\n"
        "⚖️ При споре гарант рассмотрит его и примет решение.\n"
        f"💸 Комиссия гаранта: {GUARANT_FEE}%"
    )
    bot.send_message(message.chat.id, text + BRANDING_TEXT, parse_mode="HTML")


@bot.message_handler(func=lambda message: message.from_user.id in user_states and message.text and message.text.strip() not in ["➕ Создать сделку", "📋 Мои сделки", "👤 Профиль", "ℹ️ О проекте"] and (not message.text or not message.text.startswith('/')))
def handle_states(message: types.Message):
    # Этот обработчик НЕ срабатывает для кнопок меню благодаря условию в func
    logging.info(f"[HANDLE_STATES] Processing state for user {message.from_user.id}, text: '{message.text}'")
    
    state = user_states.get(message.from_user.id)
    if not state:
        return
    
    mode = state.get('mode')
    if not mode:
        return
    
    if mode == 'set_crypto':
        token = message.text.strip()
        if ':' not in token:
            bot.send_message(message.chat.id, "❌ Неверный формат токена")
            user_states.pop(message.from_user.id, None)
            send_admin_panel(message.chat.id)
            return
        
        set_setting('crypto_pay_token', token)
        global crypto_pay_client
        crypto_pay_client = None
        bot.send_message(message.chat.id, "✅ Crypto Pay токен сохранён!")
        user_states.pop(message.from_user.id, None)
        send_admin_panel(message.chat.id)
    
    elif mode == 'add_op_channel':
        channel_input = message.text.strip()
        try:
            # Получаем информацию о канале
            chat_info = bot.get_chat(channel_input)
            channel_id = str(chat_info.id) if not channel_input.startswith('@') else channel_input
            channel_name = chat_info.title or channel_input
            
            # Формируем ссылку
            if chat_info.username:
                channel_link = f"https://t.me/{chat_info.username}"
            else:
                channel_link = f"https://t.me/c/{str(chat_info.id).replace('-100', '')}"
            
            if add_op_channel(channel_id, channel_name, channel_link):
                bot.send_message(message.chat.id, f"✅ Канал <b>{channel_name}</b> добавлен в ОП!", parse_mode="HTML")
            else:
                bot.send_message(message.chat.id, "❌ Ошибка добавления")
        except Exception as e:
            bot.send_message(message.chat.id, f"❌ Не удалось получить информацию о канале.\nУбедитесь что бот - админ канала.\n\nОшибка: {e}")
        user_states.pop(message.from_user.id, None)
        send_admin_panel(message.chat.id)
    
    elif mode == 'add_guarant':
        guarant_input = message.text.strip()
        guarant_id = None
        
        if guarant_input.startswith('@'):
            user_row = conn.execute("SELECT tg_id FROM users WHERE username=?", (guarant_input[1:],)).fetchone()
            if user_row:
                guarant_id = user_row['tg_id']
            else:
                bot.send_message(message.chat.id, "❌ Пользователь не найден. Укажите @username или ID.")
                return
        else:
            try:
                guarant_id = int(guarant_input)
            except ValueError:
                bot.send_message(message.chat.id, "❌ Укажите @username или числовой ID.")
                return
        
        # Проверяем существует ли пользователь
        user_row = conn.execute("SELECT * FROM users WHERE tg_id=?", (guarant_id,)).fetchone()
        if not user_row:
            bot.send_message(message.chat.id, "❌ Пользователь не найден в базе. Попросите его запустить бота.")
            return
        
        # Проверяем не добавлен ли уже
        existing = conn.execute("SELECT id FROM guarants WHERE tg_id=?", (guarant_id,)).fetchone()
        if existing:
            bot.send_message(message.chat.id, "❌ Этот пользователь уже является гарантом.")
            user_states.pop(message.from_user.id, None)
            send_admin_panel(message.chat.id)
            return
        
        # Добавляем гаранта
        with conn:
            conn.execute("INSERT INTO guarants (tg_id, username, first_name, added_by) VALUES (?, ?, ?, ?)",
                        (guarant_id, user_row['username'], user_row['first_name'], message.from_user.id))
        
        bot.send_message(message.chat.id, f"✅ Гарант добавлен!")
        user_states.pop(message.from_user.id, None)
        send_admin_panel(message.chat.id)
    
    elif mode == 'broadcast':
        text = message.text
        rows = conn.execute("SELECT tg_id FROM users").fetchall()
        sent = 0
        for row in rows:
            try:
                bot.send_message(row['tg_id'], f"📢 <b>Оповещение</b>\n\n{text}", parse_mode="HTML")
                sent += 1
            except:
                pass
        bot.send_message(message.chat.id, f"✅ Доставлено: {sent}/{len(rows)}")
        user_states.pop(message.from_user.id, None)
    
    elif mode == 'create_deal':
        process_deal_creation(message, state)
    
    elif mode == 'dispute_reason':
        deal_id = state.get('deal_id')
        if not deal_id:
            bot.send_message(message.chat.id, "❌ Ошибка. Попробуйте снова.")
            user_states.pop(message.from_user.id, None)
            return
        
        reason = message.text[:500]
        if not reason or not reason.strip():
            bot.send_message(message.chat.id, "❌ Причина не может быть пустой.")
            return
        
        deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not deal:
            bot.send_message(message.chat.id, "❌ Сделка не найдена.")
            user_states.pop(message.from_user.id, None)
            return
        
        if deal['status'] not in ('paid', 'waiting_completion'):
            bot.send_message(message.chat.id, "❌ Нельзя открыть спор для этой сделки.")
            user_states.pop(message.from_user.id, None)
            return
        
        # Проверяем нет ли уже спора
        existing = conn.execute("SELECT id FROM disputes WHERE deal_id=? AND status='open'", (deal_id,)).fetchone()
        if existing:
            bot.send_message(message.chat.id, "❌ Спор по этой сделке уже открыт.")
            user_states.pop(message.from_user.id, None)
            return
        
        with conn:
            conn.execute("INSERT INTO disputes (deal_id, initiator_id, reason) VALUES (?, ?, ?)",
                        (deal_id, message.from_user.id, reason))
            conn.execute("UPDATE deals SET status='dispute' WHERE id=?", (deal_id,))
        
        # Уведомляем гаранта
        if deal.get('guarant_id'):
            try:
                text = (
                    f"⚖️ <b>Открыт спор по сделке #{deal_id}</b>\n\n"
                    f"💰 Сумма: {deal['amount']:.2f} USDT\n"
                    f"👤 Инициатор: @{message.from_user.username if message.from_user.username else message.from_user.id}\n"
                    f"📝 Причина: {reason}"
                )
                bot.send_message(deal['guarant_id'], text, parse_mode="HTML")
            except:
                pass
        
        bot.send_message(message.chat.id, "✅ Спор создан! Гарант рассмотрит его в ближайшее время.")
        user_states.pop(message.from_user.id, None)
        show_deal_details(message.chat.id, deal_id)
    
    elif mode == 'remove_guarant':
        guarant_id = int(message.text.strip())
        with conn:
            conn.execute("DELETE FROM guarants WHERE tg_id=?", (guarant_id,))
        bot.send_message(message.chat.id, "✅ Гарант удалён!")
        user_states.pop(message.from_user.id, None)
        send_admin_panel(message.chat.id)


def process_deal_creation(message: types.Message, state: Dict[str, Any]):
    step = state.get('step')
    data = state.get('data', {})
    text = message.text.strip()
    
    cancel_markup = types.InlineKeyboardMarkup()
    cancel_markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))
    
    if step == 'seller':
        seller_input = text
        seller_id = None
        
        if seller_input.startswith('@'):
            seller_row = conn.execute("SELECT tg_id FROM users WHERE username=?", (seller_input[1:],)).fetchone()
            if seller_row:
                seller_id = seller_row['tg_id']
            else:
                bot.send_message(message.chat.id, "❌ Пользователь не найден. Укажите @username или ID.")
                return
        else:
            try:
                seller_id = int(seller_input)
            except ValueError:
                bot.send_message(message.chat.id, "❌ Укажите @username или числовой ID продавца.")
                return
        
        if seller_id == message.from_user.id:
            bot.send_message(message.chat.id, "❌ Вы не можете создать сделку с самим собой.")
            return
        
        data['seller_id'] = seller_id
        state['step'] = 'amount'
        bot.send_message(message.chat.id, f"💰 Введите сумму сделки (минимум {MIN_DEAL_AMOUNT} USDT):", reply_markup=cancel_markup)
    
    elif step == 'amount':
        try:
            amount = float(text.replace(',', '.'))
            if amount < MIN_DEAL_AMOUNT:
                bot.send_message(message.chat.id, f"❌ Минимальная сумма: {MIN_DEAL_AMOUNT} USDT", reply_markup=cancel_markup)
                return
            data['amount'] = amount
            data['fee'] = amount * GUARANT_FEE / 100
            state['step'] = 'description'
            bot.send_message(message.chat.id, "📝 Опишите предмет сделки:", reply_markup=cancel_markup)
        except ValueError:
            bot.send_message(message.chat.id, "❌ Введите число")
    
    elif step == 'description':
        data['description'] = text[:1000]
        state['step'] = 'confirm'
        
        seller = conn.execute("SELECT username, first_name FROM users WHERE tg_id=?", (data['seller_id'],)).fetchone()
        seller_name = f"@{seller['username']}" if seller and seller['username'] else f"ID: {data['seller_id']}"
        
        total = data['amount'] + data['fee']
        preview = (
            f"📋 <b>Подтверждение сделки</b>\n\n"
            f"🏪 Продавец: {seller_name}\n"
            f"💰 Сумма: {data['amount']:.2f} USDT\n"
            f"💸 Комиссия: {data['fee']:.2f} USDT ({GUARANT_FEE}%)\n"
            f"📊 Всего к оплате: {total:.2f} USDT\n"
            f"📝 Описание: {data['description']}"
        )
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("✅ Создать сделку", callback_data="confirm_deal"))
        markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_deal"))
        bot.send_message(message.chat.id, preview, parse_mode="HTML", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data == "confirm_deal")
def handle_confirm_deal(call: types.CallbackQuery):
    state = user_states.get(call.from_user.id)
    if not state or state.get('mode') != 'create_deal' or state.get('step') != 'confirm':
        bot.answer_callback_query(call.id, "Сессия истекла", show_alert=True)
        return
    
    data = state.get('data', {})
    user_states.pop(call.from_user.id, None)
    
    # Получаем список гарантов
    guarants = get_guarants()
    if not guarants:
        bot.send_message(call.message.chat.id, "❌ Нет доступных гарантов. Обратитесь к администратору.", reply_markup=main_menu())
        bot.answer_callback_query(call.id)
        return
    
    # Выбираем первого доступного гаранта (можно улучшить логику выбора)
    guarant = guarants[0]
    
    # Создаем сделку
    with conn:
        cur = conn.execute(
            "INSERT INTO deals (buyer_id, seller_id, guarant_id, amount, fee, description, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (call.from_user.id, data['seller_id'], guarant['tg_id'], data['amount'], data['fee'], data['description'], 'pending_guarant')
        )
        deal_id = cur.lastrowid
    
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    except:
        pass
    
    # Отправляем запрос гаранту
    seller = conn.execute("SELECT username, first_name FROM users WHERE tg_id=?", (data['seller_id'],)).fetchone()
    seller_name = f"@{seller['username']}" if seller and seller['username'] else f"ID: {data['seller_id']}"
    
    text = (
        f"🛡️ <b>Новая сделка #{deal_id}</b>\n\n"
        f"👤 Покупатель: @{call.from_user.username if call.from_user.username else call.from_user.id}\n"
        f"🏪 Продавец: {seller_name}\n"
        f"💰 Сумма: {data['amount']:.2f} USDT\n"
        f"💸 Комиссия: {data['fee']:.2f} USDT\n"
        f"📝 Описание: {data['description']}\n\n"
        f"Подтвердите сделку для создания счета на оплату."
    )
    
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Подтвердить", callback_data=f"guarant_confirm_deal_{deal_id}"),
        types.InlineKeyboardButton("❌ Отклонить", callback_data=f"guarant_reject_deal_{deal_id}")
    )
    
    try:
        bot.send_message(guarant['tg_id'], text, parse_mode="HTML", reply_markup=markup)
    except:
        pass
    
    bot.send_message(call.message.chat.id, f"✅ Сделка #{deal_id} создана! Ожидайте подтверждения гаранта.", reply_markup=main_menu())
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data == "cancel_deal")
def handle_cancel_deal(call: types.CallbackQuery):
    user_states.pop(call.from_user.id, None)
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    except:
        pass
    bot.send_message(call.message.chat.id, "❌ Создание сделки отменено.", reply_markup=main_menu())
    bot.answer_callback_query(call.id)


def create_payment_invoice(user_id: int, deal_id: int, amount: float, chat_id: int):
    client = get_crypto_client()
    if not client:
        bot.send_message(chat_id, "❌ Платёжная система не настроена. Обратитесь к админу.", reply_markup=main_menu())
        return
    
    async def create_invoice():
        try:
            invoice = await client.create_invoice(
                amount=amount,
                asset='USDT',
                fiat='USD',
                payload=f'guarant_deal_{deal_id}'
            )
            with conn:
                conn.execute(
                    "INSERT INTO payments (invoice_id, deal_id, user_id, amount) VALUES (?, ?, ?, ?)",
                    (invoice.invoice_id, deal_id, user_id, amount)
                )
                conn.execute("UPDATE deals SET invoice_id=?, status='pending_payment' WHERE id=?", (invoice.invoice_id, deal_id))
            
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("💳 Оплатить", url=invoice.bot_invoice_url))
            markup.add(types.InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"check_payment_{deal_id}"))
            
            bot.send_message(chat_id, f"💳 Для активации сделки оплатите <b>{amount:.2f} USDT</b>", 
                           reply_markup=markup, parse_mode="HTML")
        except Exception as exc:
            logging.error(f"Invoice creation failed: {exc}")
            bot.send_message(chat_id, "❌ Ошибка создания счёта. Попробуйте позже.", reply_markup=main_menu())
    
    run_async_task(create_invoice())


@bot.callback_query_handler(func=lambda call: call.data.startswith("check_payment_"))
def handle_check_payment(call: types.CallbackQuery):
    deal_id = int(call.data.split("_")[-1])
    run_async_task(check_payment_status(call.from_user.id, deal_id))
    bot.answer_callback_query(call.id, "Проверяю оплату...")


async def check_payment_status(user_id: int, deal_id: int):
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal or deal['invoice_id'] is None:
        bot.send_message(user_id, "Счёт не найден")
        return
    
    client = get_crypto_client()
    if not client:
        bot.send_message(user_id, "Платёжная система недоступна")
        return
    
    try:
        invoices = await client.get_invoices(invoice_ids=str(deal['invoice_id']))
        if not invoices:
            bot.send_message(user_id, "⏳ Оплата не найдена. Попробуйте позже.")
            return
        
        invoice = invoices[0]
        if invoice.status != 'paid':
            bot.send_message(user_id, "⏳ Оплата пока не получена. Попробуйте позже.")
            return
        
        # Обновляем статус сделки
        with conn:
            conn.execute("UPDATE deals SET status='paid', paid_at=? WHERE id=?",
                        (datetime.now().isoformat(), deal_id))
            conn.execute("UPDATE payments SET status='paid' WHERE invoice_id=?", (deal['invoice_id'],))
        
        deal_dict = dict(deal)
        deal_dict['status'] = 'paid'
        deal_dict['paid_at'] = datetime.now().isoformat()
        
        bot.send_message(user_id, "✅ Оплата получена! Ожидайте гаранта.", reply_markup=main_menu())
        show_deal_details(user_id, deal_id)
        
        # Уведомляем продавца
        seller_id = deal['seller_id']
        try:
            bot.send_message(seller_id, f"💰 Получена оплата по сделке #{deal_id}!\n\nОжидайте подтверждения от покупателя после передачи товара.")
        except:
            pass
        
        # Уведомляем гаранта
        if deal.get('guarant_id'):
            try:
                text = (
                    f"💰 <b>Оплата получена по сделке #{deal_id}</b>\n\n"
                    f"💰 Сумма: {deal['amount']:.2f} USDT\n"
                    f"👤 Покупатель: ID {deal['buyer_id']}\n"
                    f"🏪 Продавец: ID {deal['seller_id']}\n"
                    f"📝 Описание: {deal['description']}\n\n"
                    f"Следите за выполнением сделки. При споре вы сможете принять решение."
                )
                bot.send_message(deal['guarant_id'], text, parse_mode="HTML")
            except:
                pass
        
        # Переводим статус в ожидание выполнения
        with conn:
            conn.execute("UPDATE deals SET status='waiting_completion' WHERE id=?", (deal_id,))
        
    except Exception as exc:
        logging.error(f"Payment check failed: {exc}")
        bot.send_message(user_id, "❌ Ошибка проверки платежа.")


def show_deal_details(chat_id: int, deal_id: int):
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal:
        bot.send_message(chat_id, "Сделка не найдена", reply_markup=main_menu())
        return
    
    deal_dict = dict(deal)
    text = format_deal(deal_dict)
    
    markup = types.InlineKeyboardMarkup()
    
    if deal['status'] == 'waiting_completion':
        # Показываем кнопки подтверждения
        if deal.get('buyer_confirmed') == False or deal.get('seller_confirmed') == False:
            markup.add(types.InlineKeyboardButton("✅ Подтвердить выполнение", callback_data=f"confirm_deal_{deal_id}"))
        markup.add(types.InlineKeyboardButton("⚖️ Открыть спор", callback_data=f"dispute_deal_{deal_id}"))
    elif deal['status'] == 'pending_payment':
        markup.add(types.InlineKeyboardButton("💳 Оплатить", callback_data=f"deal_pay_{deal_id}"))
        markup.add(types.InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"check_payment_{deal_id}"))
    elif deal['status'] in ('paid', 'dispute'):
        markup.add(types.InlineKeyboardButton("⚖️ Открыть спор", callback_data=f"dispute_deal_{deal_id}"))
    
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("deal_pay_"))
def handle_deal_pay(call: types.CallbackQuery):
    deal_id = int(call.data.split("_")[-1])
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    
    if not deal or deal['status'] not in ('guarant_confirmed', 'pending_payment'):
        bot.answer_callback_query(call.id, "Сделка не требует оплаты", show_alert=True)
        return
    
    if deal['invoice_id']:
        client = get_crypto_client()
        if client:
            async def get_invoice_url():
                try:
                    invoices = await client.get_invoices(invoice_ids=str(deal['invoice_id']))
                    if invoices:
                        invoice = invoices[0]
                        markup = types.InlineKeyboardMarkup()
                        markup.add(types.InlineKeyboardButton("💳 Оплатить", url=invoice.bot_invoice_url))
                        markup.add(types.InlineKeyboardButton("🔄 Проверить", callback_data=f"check_payment_{deal_id}"))
                        bot.send_message(call.message.chat.id, f"💳 Оплатите <b>{deal['amount'] + deal['fee']:.2f} USDT</b>",
                                       reply_markup=markup, parse_mode="HTML")
                except:
                    pass
            run_async_task(get_invoice_url())
    else:
        total = deal['amount'] + deal['fee']
        create_payment_invoice(call.from_user.id, deal_id, total, call.message.chat.id)
    
    bot.answer_callback_query(call.id)


async def transfer_to_seller(deal_id: int):
    """Переводит средства продавцу"""
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal:
        return
    
    # В реальном боте здесь был бы перевод через Crypto Pay API
    logging.info(f"Transfer {deal['amount']:.2f} USDT to seller {deal['seller_id']} for deal #{deal_id} (fee: {deal['fee']:.2f} USDT)")


async def refund_to_buyer(deal_id: int):
    """Возвращает средства покупателю"""
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal:
        return
    
    # В реальном боте здесь был бы возврат через Crypto Pay API
    logging.info(f"Refund {deal['amount'] + deal['fee']:.2f} USDT to buyer {deal['buyer_id']} for deal #{deal_id}")


@bot.message_handler(func=lambda m: True, content_types=['text'])
def fallback_handler(message: types.Message):
    # Не обрабатываем кнопки меню - они обрабатываются отдельными обработчиками выше
    menu_buttons = ["➕ Создать сделку", "📋 Мои сделки", "👤 Профиль", "ℹ️ О проекте"]
    if message.text and (message.text in menu_buttons or message.text.strip() in menu_buttons):
        logging.info(f"[FALLBACK] Ignoring menu button: '{message.text}'")
        return
    
    if message.text == "/cancel":
        user_states.pop(message.from_user.id, None)
        bot.send_message(message.chat.id, "Отменено.", reply_markup=main_menu())
        return
    
    # Не обрабатываем если пользователь в состоянии (это обрабатывается handle_states)
    if message.from_user.id in user_states:
        logging.info(f"[FALLBACK] User {message.from_user.id} is in state, skipping")
        return
    
    logging.info(f"[FALLBACK] Handling unknown text: '{message.text}' from user {message.from_user.id}")
    bot.send_message(message.chat.id, "Выберите действие через меню:", reply_markup=main_menu())


# Фоновая проверка платежей
def check_payments_background():
    """Периодически проверяет платежи"""
    async def check_loop():
        while True:
            try:
                pending = conn.execute(
                    "SELECT d.* FROM deals d "
                    "JOIN payments p ON d.invoice_id = p.invoice_id "
                    "WHERE d.status='pending_payment' AND p.status='pending'"
                ).fetchall()
                
                client = get_crypto_client()
                if client and pending:
                    for deal in pending:
                        await check_payment_status(deal['buyer_id'], deal['id'])
                
                await asyncio.sleep(60)  # Проверка каждую минуту
            except Exception as e:
                logging.error(f"Payment check loop error: {e}")
                await asyncio.sleep(60)
    
    run_async_task(check_loop())


async def load_flyer_channels():
    """Загружает каналы из Flyer API и добавляет их в БД"""
    if not FLYER_API_KEY:
        return
    
    try:
        # Удаляем старые Flyer каналы
        with conn:
            conn.execute("DELETE FROM op_channels WHERE channel_id LIKE 'flyer_%'")
        
        async with aiohttp.ClientSession() as session:
            headers = {'Authorization': f'Bearer {FLYER_API_KEY}'}
            async with session.get('https://api.flyer.app/v1/channels', headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    flyer_channels = data.get('channels', [])
                    for ch in flyer_channels:
                        channel_id = ch.get('channel_id')
                        if channel_id:
                            channel_name = ch.get('name', 'Flyer Channel')
                            channel_link = ch.get('link', f"https://t.me/{channel_id.lstrip('@')}")
                            # Добавляем с префиксом чтобы отличать от своих
                            add_op_channel(f"flyer_{channel_id}", channel_name, channel_link)
                    logging.info(f"Loaded {len(flyer_channels)} channels from Flyer API")
                else:
                    logging.warning(f"Flyer API returned status {resp.status}")
    except asyncio.TimeoutError:
        logging.error("Flyer API request timeout")
    except Exception as e:
        logging.error(f"Failed to load Flyer channels: {e}")


def main():
    init_db()
    init_async_loop()
    
    # Загружаем каналы из Flyer API при старте
    if FLYER_API_KEY:
        run_async_task(load_flyer_channels())
    
    # Запускаем фоновую проверку платежей
    check_payments_background()
    
    logging.info("Guarant Bot started")
    logging.info(f"Registered handlers count: {len(bot.message_handlers)}")
    # Логируем все обработчики для отладки
    for i, handler in enumerate(bot.message_handlers):
        logging.info(f"Handler {i}: {handler}")
    
    bot.infinity_polling()


if __name__ == '__main__':
    main()
