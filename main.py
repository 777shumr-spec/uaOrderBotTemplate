import os
import json
import logging
import asyncio
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional

from aiohttp import web, ClientSession, ClientTimeout

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import CommandStart, Command
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject

# =========================
# LOGGING (Render-friendly)
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("tg-order-bot")

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = set(int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit())
MANAGER_CHAT_ID = int((os.getenv("MANAGER_CHAT_ID", "0").strip() or "0"))

GS_ENDPOINT = os.getenv("GS_ENDPOINT", "").strip()
GS_KEY = os.getenv("GS_KEY", "").strip()
BIZ_ID = os.getenv("BIZ_ID", "demo").strip()
CURRENCY = os.getenv("CURRENCY", "UAH").strip()
SOURCE = os.getenv("SOURCE", "Telegram").strip()

WEBHOOK_BASE = (os.getenv("WEBHOOK_BASE", "").strip() or os.getenv("RENDER_EXTERNAL_URL", "").strip())
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook").strip()
PORT = int((os.getenv("PORT", "10000") or "10000"))

WEBHOOK_BASE = WEBHOOK_BASE.rstrip("/") if WEBHOOK_BASE else ""

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not GS_ENDPOINT:
    raise RuntimeError("GS_ENDPOINT is required")
if not GS_KEY:
    raise RuntimeError("GS_KEY is required")
if MANAGER_CHAT_ID == 0:
    raise RuntimeError("MANAGER_CHAT_ID is required")

# =========================
# Catalog (file_id під твого нового бота)
# =========================
CATALOG = {
    "Десерти": [
        {"sku": "cake_napoleon", "title": "Торт «Наполеон»", "price": 650,
         "photo": "AgACAgIAAxkBAAMHaagCWmo_c_YK4YRk5llKms4gd5MAAmEUaxvf1kFJr4WH6F_jZ_YBAAMCAANtAAM6BA"},
        {"sku": "cake_honey", "title": "Торт «Медовик»", "price": 620,
         "photo": "AgACAgIAAxkBAAMJaagCZA-gL42QRDl6OvKYS399bb8AAmIUaxvf1kFJMk5lUqSqMKYBAAMCAANtAAM6BA"},
        {"sku": "cupcake", "title": "Капкейки (1 шт)", "price": 55,
         "photo": "AgACAgIAAxkBAAMLaagCaUlnH66fW90ivi4WoagV48QAAmQUaxvf1kFJJql135zQU8gBAAMCAAN5AAM6BA"},
    ],
    "Напої": [
        {"sku": "coffee", "title": "Кава", "price": 60,
         "photo": "AgACAgIAAxkBAAMNaagCbRZCO8cFb1ZEzUQd8PwYcDkAAmUUaxvf1kFJF2S_0uLiApMBAAMCAAN4AAM6BA"},
        {"sku": "tea", "title": "Чай", "price": 40,
         "photo": "AgACAgIAAxkBAAMPaagCcL_xVc5L4W67KjQmOuOBggYAAmYUaxvf1kFJZLj7jsw7_5sBAAMCAAN5AAM6BA"},
    ],
    "Інше": [
        {"sku": "gift_box", "title": "Подарункова коробка", "price": 80,
         "photo": "AgACAgIAAxkBAAMRaagCdB_y-QfBwFl_9LLiQPrO_SIAAmcUaxvf1kFJhZ7nhJhjw9ABAAMCAAN4AAM6BA"},
    ],
}

# =========================
# State (RAM) — може скидатися при рестарті
# =========================
carts: Dict[int, Dict[str, int]] = {}
draft: Dict[int, Dict[str, Any]] = {}
fileid_mode: Dict[int, bool] = {}

# =========================
# Global stability settings
# =========================
TG_CALL_TIMEOUT_SEC = 12          # таймаут на конкретний виклик Telegram API
UPDATE_PROCESS_TIMEOUT_SEC = 20   # таймаут на обробку одного апдейту (флоу + відправка)
UPDATE_QUEUE_MAX = 2000           # щоб не з'їсти RAM якщо Telegram "поливає"

update_queue: asyncio.Queue = asyncio.Queue(maxsize=UPDATE_QUEUE_MAX)

# =========================
# Helpers
# =========================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛒 Зробити замовлення")],
            [KeyboardButton(text="📦 Каталог / Меню"), KeyboardButton(text="🚚 Доставка та оплата")],
            [KeyboardButton(text="☎️ Контакти"), KeyboardButton(text="🧾 Мої замовлення")],
        ],
        resize_keyboard=True
    )

def categories_kb() -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton(text=cat, callback_data=f"cat:{cat}")] for cat in CATALOG.keys()]
    buttons.append([InlineKeyboardButton(text="🧺 Кошик", callback_data="cart")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def product_kb(sku: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Додати", callback_data=f"add:{sku}"),
            InlineKeyboardButton(text="➖ Забрати", callback_data=f"rem:{sku}")
        ],
        [InlineKeyboardButton(text="🧺 Кошик", callback_data="cart")],
        [InlineKeyboardButton(text="⬅️ Категорії", callback_data="cats")]
    ])

def cart_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Оформити", callback_data="checkout")],
        [InlineKeyboardButton(text="🗑 Очистити", callback_data="clear")],
        [InlineKeyboardButton(text="⬅️ Категорії", callback_data="cats")]
    ])

def manager_status_kb(order_id: str, user_tg_id: str) -> InlineKeyboardMarkup:
    def b(text, status):
        return InlineKeyboardButton(text=text, callback_data=f"st:{order_id}:{status}:{user_tg_id}")
    return InlineKeyboardMarkup(inline_keyboard=[
        [b("✅ Прийнято", "ACCEPTED"), b("⏳ В роботі", "IN_PROGRESS")],
        [b("🚚 Доставляється", "DELIVERING"), b("✅ Виконано", "DONE")],
        [b("❌ Скасовано", "CANCELED")]
    ])

def find_item_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    for _, items in CATALOG.items():
        for it in items:
            if it["sku"] == sku:
                return it
    return None

def calc_total(user_id: int) -> int:
    total = 0
    for sku, qty in carts.get(user_id, {}).items():
        item = find_item_by_sku(sku)
        if item:
            total += int(item["price"]) * int(qty)
    return total

def cart_text(user_id: int) -> str:
    items = carts.get(user_id, {})
    if not items:
        return "🧺 Кошик порожній."
    lines = ["🧺 Ваш кошик:"]
    for sku, qty in items.items():
        item = find_item_by_sku(sku)
        if not item:
            continue
        lines.append(f"• {item['title']} x{qty} = {int(item['price']) * int(qty)}")
    lines.append(f"\nРазом: {calc_total(user_id)} {CURRENCY}")
    return "\n".join(lines)

def lost_session_text() -> str:
    return (
        "⚠️ Сесія оформлення зникла (перезапуск сервера).\n\n"
        "Будь ласка, відкрийте 🧺 Кошик → ✅ Оформити ще раз.\n"
        "Якщо кошик теж порожній — додайте товари з каталогу."
    )

async def tg_call(coro, what: str = "tg_call"):
    """
    Обгортка для будь-якого Telegram API виклику:
    - дає таймаут
    - логування якщо зависло/впало
    """
    try:
        return await asyncio.wait_for(coro, timeout=TG_CALL_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        log.error("⏱ Telegram API TIMEOUT in %s", what)
    except Exception as e:
        log.warning("Telegram API error in %s: %r", what, e)

async def safe_send(m: Message, text: str, reply_markup=None):
    await tg_call(m.answer(text, reply_markup=reply_markup), what="message.answer")

async def safe_edit(cb: CallbackQuery, text: str, reply_markup=None):
    msg = cb.message
    try:
        if msg.text is not None:
            await tg_call(msg.edit_text(text, reply_markup=reply_markup), what="edit_text")
            return
        if msg.caption is not None or msg.photo or msg.document or msg.video:
            await tg_call(msg.edit_caption(caption=text, reply_markup=reply_markup), what="edit_caption")
            return
    except Exception as e:
        log.warning("safe_edit internal failed: %r", e)

    await tg_call(msg.answer(text, reply_markup=reply_markup), what="safe_edit.fallback_send")

# =========================
# GS calls
# =========================
async def gs_create_order(session: ClientSession, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        async with session.post(GS_ENDPOINT, json=payload) as resp:
            text = await resp.text()
            try:
                return json.loads(text)
            except Exception:
                return {"ok": False, "error": f"Bad response: {text[:200]}"}
    except Exception as e:
        return {"ok": False, "error": f"GS request failed: {repr(e)}"}

async def gs_update_status(session: ClientSession, order_id: str, status: str) -> Dict[str, Any]:
    payload = {"key": GS_KEY, "action": "updateStatus", "bizId": BIZ_ID, "orderId": order_id, "status": status}
    try:
        async with session.post(GS_ENDPOINT, json=payload) as resp:
            text = await resp.text()
            try:
                return json.loads(text)
            except Exception:
                return {"ok": False, "error": f"Bad response: {text[:200]}"}
    except Exception as e:
        return {"ok": False, "error": f"GS request failed: {repr(e)}"}

# =========================
# Bot + Dispatcher
# =========================
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

class CrashGuardMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        try:
            return await handler(event, data)
        except Exception as e:
            log.error("🔥 UNHANDLED ERROR: %r", e)
            log.error(traceback.format_exc())
            return

dp.update.middleware(CrashGuardMiddleware())

# =========================
# Admin debug
# =========================
@dp.message(Command("debug_state"))
async def debug_state(m: Message):
    if ADMIN_IDS and m.from_user.id not in ADMIN_IDS:
        return
    uid = m.from_user.id
    d = draft.get(uid)
    c = carts.get(uid)
    await safe_send(m, f"DEBUG\nuser={uid}\ndraft={d}\ncart={c}\nqueue={update_queue.qsize()}")

@dp.message(Command("ping"))
async def ping(m: Message):
    await safe_send(m, "pong ✅")

# =========================
# Admin file_id
# =========================
@dp.message(Command("fileid"))
async def fileid_help(m: Message):
    if ADMIN_IDS and m.from_user.id not in ADMIN_IDS:
        return
    fileid_mode[m.from_user.id] = True
    await safe_send(
        m,
        "✅ Режим file_id увімкнено.\n"
        "Надішли 1 фото як повідомлення (не файлом). Я відповім file_id.\n"
        "Щоб вимкнути — /fileidoff"
    )

@dp.message(Command("fileidoff"))
async def fileid_off(m: Message):
    if ADMIN_IDS and m.from_user.id not in ADMIN_IDS:
        return
    fileid_mode[m.from_user.id] = False
    await safe_send(m, "✅ Режим file_id вимкнено.")

@dp.message(F.photo)
async def fileid_photo(m: Message):
    if ADMIN_IDS and m.from_user.id not in ADMIN_IDS:
        return
    if not fileid_mode.get(m.from_user.id, False):
        return
    photo = m.photo[-1]
    await safe_send(m, f"✅ file_id:\n{photo.file_id}")

# =========================
# Main bot
# =========================
@dp.message(CommandStart())
async def start(m: Message):
    welcome_text = (
        "👋 Вітаємо у нашій кондитерській!\n\n"
        "🎂 Замовляйте торти та десерти онлайн за 1 хвилину.\n"
        "Менеджер одразу підтвердить замовлення.\n\n"
        "Оберіть дію нижче 👇"
    )
    await safe_send(m, welcome_text, reply_markup=main_menu_kb())

@dp.message(F.text == "📦 Каталог / Меню")
@dp.message(F.text == "🛒 Зробити замовлення")
async def show_catalog(m: Message):
    await safe_send(m, "Оберіть категорію:", reply_markup=categories_kb())

@dp.message(F.text == "🚚 Доставка та оплата")
async def delivery(m: Message):
    await safe_send(
        m,
        "🚚 Доставка та оплата:\n"
        "• Доставка по місту\n"
        "• Самовивіз\n"
        "Оплата: готівка/переказ (на старті)."
    )

@dp.message(F.text == "☎️ Контакти")
async def contacts(m: Message):
    await safe_send(m, "☎️ Контакти:\nМенеджер: @ruslanshum\nТел: +380973080330")

@dp.message(F.text == "🧾 Мої замовлення")
async def my_orders_stub(m: Message):
    await safe_send(m, "🧾 Поки що в демо показ 'Мої замовлення' буде на наступному кроці.")

@dp.callback_query(F.data == "cats")
async def cats(cb: CallbackQuery):
    await safe_edit(cb, "Оберіть категорію:", reply_markup=categories_kb())
    await tg_call(cb.answer(), what="cb.answer(cats)")

@dp.callback_query(F.data.startswith("cat:"))
async def cat(cb: CallbackQuery):
    cat_name = cb.data.split(":", 1)[1]
    items = CATALOG.get(cat_name, [])
    kb = []
    for it in items:
        kb.append([InlineKeyboardButton(
            text=f"{it['title']} — {it['price']} {CURRENCY}",
            callback_data=f"prod:{it['sku']}"
        )])
    kb.append([InlineKeyboardButton(text="🧺 Кошик", callback_data="cart")])
    kb.append([InlineKeyboardButton(text="⬅️ Категорії", callback_data="cats")])

    await safe_edit(
        cb,
        f"📦 {cat_name}:\nОберіть товар нижче (натисніть на назву).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )
    await tg_call(cb.answer(), what="cb.answer(cat)")

@dp.callback_query(F.data.startswith("prod:"))
async def prod(cb: CallbackQuery):
    sku = cb.data.split(":", 1)[1]
    item = find_item_by_sku(sku)
    if not item:
        await tg_call(cb.answer("Товар не знайдено", show_alert=True), what="cb.answer(prod_not_found)")
        return

    text = (
        f"🧾 {item['title']}\n"
        f"💰 Ціна: {item['price']} {CURRENCY}\n\n"
        "Додати в кошик?"
    )

    photo_id = (item.get("photo") or "").strip()
    if photo_id:
        try:
            await tg_call(
                cb.message.answer_photo(photo=photo_id, caption=text, reply_markup=product_kb(sku)),
                what="answer_photo"
            )
        except Exception as e:
            log.warning("answer_photo failed (sku=%s): %r", sku, e)
            await tg_call(
                cb.message.answer(text + "\n\n⚠️ Фото тимчасово недоступне.", reply_markup=product_kb(sku)),
                what="answer_text_no_photo"
            )
    else:
        await tg_call(cb.message.answer(text, reply_markup=product_kb(sku)), what="answer_product_text")

    await tg_call(cb.answer(), what="cb.answer(prod)")

@dp.callback_query(F.data.startswith("add:"))
async def add(cb: CallbackQuery):
    sku = cb.data.split(":", 1)[1]
    carts.setdefault(cb.from_user.id, {})
    carts[cb.from_user.id][sku] = carts[cb.from_user.id].get(sku, 0) + 1
    await tg_call(cb.answer("Додано ✅"), what="cb.answer(add)")

@dp.callback_query(F.data.startswith("rem:"))
async def rem(cb: CallbackQuery):
    sku = cb.data.split(":", 1)[1]
    if cb.from_user.id in carts and sku in carts[cb.from_user.id]:
        carts[cb.from_user.id][sku] -= 1
        if carts[cb.from_user.id][sku] <= 0:
            del carts[cb.from_user.id][sku]
        await tg_call(cb.answer("Забрано ✅"), what="cb.answer(rem)")
    else:
        await tg_call(cb.answer("У кошику немає", show_alert=False), what="cb.answer(rem_none)")

@dp.callback_query(F.data == "cart")
async def cart(cb: CallbackQuery):
    await safe_edit(cb, cart_text(cb.from_user.id), reply_markup=cart_kb())
    await tg_call(cb.answer(), what="cb.answer(cart)")

@dp.callback_query(F.data == "clear")
async def clear(cb: CallbackQuery):
    carts[cb.from_user.id] = {}
    draft.pop(cb.from_user.id, None)
    await safe_edit(cb, "🧺 Кошик очищено.", reply_markup=categories_kb())
    await tg_call(cb.answer(), what="cb.answer(clear)")

@dp.callback_query(F.data == "checkout")
async def checkout(cb: CallbackQuery):
    if not carts.get(cb.from_user.id):
        await tg_call(cb.answer("Кошик порожній", show_alert=True), what="cb.answer(checkout_empty)")
        return
    draft[cb.from_user.id] = {"step": "name"}
    log.info("checkout -> step=name user=%s", cb.from_user.id)
    await tg_call(cb.message.answer("✍️ Введіть ваше ім’я:"), what="ask_name")
    await tg_call(cb.answer(), what="cb.answer(checkout)")

# --- phone via contact ---
@dp.message(F.contact)
async def flow_contact(m: Message):
    user_id = m.from_user.id

    if user_id not in draft:
        await safe_send(m, lost_session_text(), reply_markup=main_menu_kb())
        return

    if draft[user_id].get("step") != "phone":
        await safe_send(m, "ℹ️ Контакт отримано, але зараз не етап телефону. Продовжіть оформлення.", reply_markup=main_menu_kb())
        return

    phone = (m.contact.phone_number or "").strip()
    draft[user_id]["phone"] = phone
    draft[user_id]["step"] = "deliveryType"
    log.info("flow_contact -> step=deliveryType user=%s phone=%s", user_id, phone)

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚚 Доставка"), KeyboardButton(text="🏃 Самовивіз")]],
        resize_keyboard=True, one_time_keyboard=True
    )
    await safe_send(m, "Оберіть тип отримання:", reply_markup=kb)

# FLOW: text
@dp.message(F.text)
async def flow(m: Message):
    user_id = m.from_user.id
    if user_id not in draft:
        return

    step = draft[user_id].get("step")
    text = (m.text or "").strip()
    log.info("flow user=%s step=%s text=%s", user_id, step, text[:80])

    if step == "name":
        draft[user_id]["name"] = text
        draft[user_id]["step"] = "phone"
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📱 Поділитися контактом", request_contact=True)]],
            resize_keyboard=True, one_time_keyboard=True
        )
        await safe_send(m, "📱 Надішліть телефон (кнопкою) або введіть вручну:", reply_markup=kb)
        return

    if step == "phone":
        draft[user_id]["phone"] = text
        draft[user_id]["step"] = "deliveryType"
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🚚 Доставка"), KeyboardButton(text="🏃 Самовивіз")]],
            resize_keyboard=True, one_time_keyboard=True
        )
        await safe_send(m, "Оберіть тип отримання:", reply_markup=kb)
        return

    if step == "deliveryType":
        if "Самовивіз" in text:
            draft[user_id]["deliveryType"] = "PICKUP"
            draft[user_id]["address"] = "-"
            draft[user_id]["step"] = "datetime"
            await safe_send(m, "🕒 Вкажіть дату/час (наприклад: завтра 14:00):", reply_markup=main_menu_kb())
            return
        if "Доставка" in text:
            draft[user_id]["deliveryType"] = "DELIVERY"
            draft[user_id]["step"] = "address"
            await safe_send(m, "🏠 Введіть адресу доставки:", reply_markup=main_menu_kb())
            return
        await safe_send(m, "Будь ласка, натисніть кнопку: 🚚 Доставка або 🏃 Самовивіз")
        return

    if step == "address":
        draft[user_id]["address"] = text
        draft[user_id]["step"] = "datetime"
        await safe_send(m, "🕒 Вкажіть дату/час (наприклад: сьогодні 19:30):")
        return

    if step == "datetime":
        draft[user_id]["datetime"] = text
        draft[user_id]["step"] = "comment"
        await safe_send(m, "💬 Коментар (якщо НП — місто, відділення, ПІБ, телефон):")
        return

    if step == "comment":
        comment = text
        if comment == "-":
            comment = ""
        draft[user_id]["comment"] = comment

        items: List[Dict[str, Any]] = []
        for sku, qty in carts.get(user_id, {}).items():
            it = find_item_by_sku(sku)
            if it:
                items.append({"sku": sku, "title": it["title"], "qty": qty, "price": it["price"]})

        total = calc_total(user_id)

        summary = [
            "✅ Перевірте замовлення:",
            cart_text(user_id),
            "",
            f"Ім’я: {draft[user_id].get('name','')}",
            f"Телефон: {draft[user_id].get('phone','')}",
            f"Тип: {draft[user_id].get('deliveryType','')}",
            f"Адреса: {draft[user_id].get('address','-')}",
            f"Дата/час: {draft[user_id].get('datetime','')}",
            f"Коментар: {draft[user_id].get('comment','') or '-'}",
        ]

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Підтвердити", callback_data="confirm")],
            [InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel")]
        ])

        draft[user_id]["items"] = items
        draft[user_id]["total"] = total
        draft[user_id]["step"] = "confirm_wait"

        await safe_send(m, "\n".join(summary), reply_markup=kb)
        return

@dp.callback_query(F.data == "cancel")
async def cancel(cb: CallbackQuery):
    carts[cb.from_user.id] = {}
    draft.pop(cb.from_user.id, None)
    await safe_edit(cb, "❌ Замовлення скасовано.")
    await tg_call(cb.answer(), what="cb.answer(cancel)")

@dp.callback_query(F.data == "confirm")
async def confirm(cb: CallbackQuery):
    user_id = cb.from_user.id
    if user_id not in draft or not carts.get(user_id):
        await tg_call(cb.answer("Немає активного замовлення", show_alert=True), what="cb.answer(confirm_noactive)")
        return

    d = draft[user_id]
    items = d.get("items", [])
    total = d.get("total", 0)

    order_payload = {
        "key": GS_KEY,
        "action": "createOrder",
        "bizId": BIZ_ID,
        "order": {
            "orderId": "AUTO",
            "createdAt": "AUTO",
            "status": "NEW",
            "userTgId": str(user_id),
            "username": cb.from_user.username or "",
            "name": d.get("name", ""),
            "phone": d.get("phone", ""),
            "deliveryType": d.get("deliveryType", ""),
            "address": d.get("address", ""),
            "datetime": d.get("datetime", ""),
            "comment": d.get("comment", ""),
            "items": items,
            "total": total,
            "currency": CURRENCY,
            "source": SOURCE
        }
    }

    timeout = ClientTimeout(total=25)
    async with ClientSession(timeout=timeout) as session:
        res = await gs_create_order(session, order_payload)

    if not res.get("ok"):
        await tg_call(cb.message.answer(f"❌ Помилка збереження замовлення.\n{res}"), what="send_gs_error")
        await tg_call(cb.answer(), what="cb.answer(confirm_err)")
        return

    order_id = str(res.get("orderId", ""))
    await safe_edit(cb, f"🎉 Дякуємо! Замовлення прийнято.\nНомер: #{order_id}\nМенеджер скоро зв’яжеться.")
    await tg_call(cb.answer("Готово ✅"), what="cb.answer(confirm_ok)")

    mgr_text = [
        f"🆕 НОВЕ ЗАМОВЛЕННЯ #{order_id}",
        f"Ім’я: {d.get('name','')}",
        f"Телефон: {d.get('phone','')}",
        f"Telegram: @{cb.from_user.username}" if cb.from_user.username else f"Telegram ID: {user_id}",
        f"Тип: {d.get('deliveryType','')}",
        f"Адреса/самовивіз: {d.get('address','-')}",
        f"Дата/час: {d.get('datetime','')}",
        f"Коментар: {d.get('comment','-') or '-'}",
        "",
        "Склад:"
    ]
    for it in items:
        mgr_text.append(f"• {it['title']} x{it['qty']} = {int(it['price']) * int(it['qty'])}")
    mgr_text.append(f"\nРазом: {total} {CURRENCY}")
    mgr_text.append(f"Час: {now_str()}")

    try:
        await tg_call(
            bot.send_message(
                chat_id=MANAGER_CHAT_ID,
                text="\n".join(mgr_text),
                reply_markup=manager_status_kb(order_id, str(user_id))
            ),
            what="send_to_manager"
        )
    except Exception as e:
        log.warning("send_message to manager failed: %r", e)

    carts[user_id] = {}
    draft.pop(user_id, None)

@dp.callback_query(F.data.startswith("st:"))
async def set_status(cb: CallbackQuery):
    if ADMIN_IDS and cb.from_user.id not in ADMIN_IDS and cb.from_user.id != MANAGER_CHAT_ID:
        await tg_call(cb.answer("Немає доступу", show_alert=True), what="cb.answer(st_denied)")
        return

    _, order_id, status, user_tg_id = cb.data.split(":", 3)

    timeout = ClientTimeout(total=25)
    async with ClientSession(timeout=timeout) as session:
        res = await gs_update_status(session, order_id, status)

    if res.get("ok"):
        await tg_call(cb.answer(f"Статус: {status} ✅"), what="cb.answer(st_ok)")
        await tg_call(bot.send_message(int(user_tg_id), f"📦 Статус замовлення #{order_id}: {status}"), what="notify_user_status")
    else:
        await tg_call(cb.answer("Помилка оновлення статусу", show_alert=True), what="cb.answer(st_err)")

# =========================
# Webhook worker (QUEUE) — стабільність 10/10
# =========================
async def _process_update_with_timeout(update: dict):
    try:
        await asyncio.wait_for(dp.feed_raw_update(bot, update), timeout=UPDATE_PROCESS_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        log.error("⏱ feed_raw_update TIMEOUT (%ss). Update skipped.", UPDATE_PROCESS_TIMEOUT_SEC)
    except Exception as e:
        log.error("🔥 feed_raw_update failed: %r", e)
        log.error(traceback.format_exc())

async def update_worker():
    log.info("✅ update_worker started")
    while True:
        upd = await update_queue.get()
        try:
            await _process_update_with_timeout(upd)
        finally:
            update_queue.task_done()

# =========================
# Webhook server (aiohttp)
# =========================
async def on_startup(app: web.Application):
    # старт воркера
    app["worker_task"] = asyncio.create_task(update_worker())

    if not WEBHOOK_BASE:
        log.warning("WEBHOOK_BASE is empty. Webhook will NOT be set.")
        return

    webhook_url = f"{WEBHOOK_BASE}{WEBHOOK_PATH}"
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.set_webhook(webhook_url, allowed_updates=["message", "callback_query"])
        log.info("✅ Webhook set to: %s", webhook_url)
    except Exception as e:
        log.exception("❌ set_webhook failed: %r", e)

async def on_shutdown(app: web.Application):
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    # стоп воркера
    wt: asyncio.Task = app.get("worker_task")
    if wt:
        wt.cancel()
        try:
            await wt
        except Exception:
            pass

    await bot.session.close()

async def handle_webhook(request: web.Request):
    """
    Відповідаємо Telegram МИТТЄВО (200 OK),
    апдейт кладемо в чергу (послідовна обробка + таймаути).
    """
    try:
        update = await request.json()
    except Exception:
        return web.Response(text="ok")

    # якщо черга переповнилась — не вбиваєм процес, але логнемо
    if update_queue.full():
        log.error("❌ UPDATE QUEUE FULL (%s). Dropping update.", UPDATE_QUEUE_MAX)
        return web.Response(text="ok")

    update_queue.put_nowait(update)
    return web.Response(text="ok")

def build_app():
    app = web.Application()

    async def health(_request):
        return web.Response(text="ok")

    app.router.add_get("/", health)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app

if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=PORT)












