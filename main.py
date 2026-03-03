import os
import json
from datetime import datetime
from typing import Dict, Any, List

from aiohttp import web, ClientSession
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import CommandStart, Command

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = set(int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit())
MANAGER_CHAT_ID = int(os.getenv("MANAGER_CHAT_ID", "0").strip() or "0")

GS_ENDPOINT = os.getenv("GS_ENDPOINT", "").strip()
GS_KEY = os.getenv("GS_KEY", "").strip()
BIZ_ID = os.getenv("BIZ_ID", "demo").strip()
CURRENCY = os.getenv("CURRENCY", "UAH").strip()
SOURCE = os.getenv("SOURCE", "Telegram").strip()

WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "").strip()  # e.g. https://tg-order-bot-lywy.onrender.com
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook").strip()
PORT = int(os.getenv("PORT", "10000"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not GS_ENDPOINT:
    raise RuntimeError("GS_ENDPOINT is required")
if not GS_KEY:
    raise RuntimeError("GS_KEY is required")
if MANAGER_CHAT_ID == 0:
    raise RuntimeError("MANAGER_CHAT_ID is required")
if not WEBHOOK_BASE:
    raise RuntimeError("WEBHOOK_BASE is required")

# =========================
# Simple demo catalog (in code)
# Later move to /admin editing if needed.
# =========================
CATALOG = {
    "Десерти": [
        {"sku": "cake_napoleon", "title": "Торт «Наполеон»", "price": 650, "photo": "AgACAgIAAxkBAAOdaab0HY12878k8OAnDwV_pJjkVQsAAkYTaxvOjDlJQYBq-IPJcSQBAAMCAANtAAM6BA"},
        {"sku": "cake_honey", "title": "Торт «Медовик»", "price": 620, "photo": "AgACAgIAAxkBAAObaab0FnC3pkssG8ZdFQuYd8WY9IwAAkUTaxvOjDlJTBar3BbZh5EBAAMCAANtAAM6BA"},
        {"sku": "cupcake", "title": "Капкейки (1 шт)", "price": 55, "photo": "AgACAgIAAxkBAAOVaabz-C5YXwKqrPRzXw7UtzJfhHkAAkITaxvOjDlJdC34GFtFGf0BAAMCAAN5AAM6BA"},
    ],
    "Напої": [
        {"sku": "coffee", "title": "Кава", "price": 60, "photo": "AgACAgIAAxkBAAOZaab0Dv8hYBTSwZZBIY-7YFayRFMAAkQTaxvOjDlJ_TLHnFebP9cBAAMCAAN4AAM6BA"},
        {"sku": "tea", "title": "Чай", "price": 40, "photo": "AgACAgIAAxkBAAOXaab0AAEGGf_f6oDotHPYl8agvHVzAAJDE2sbzow5SRdQOqvfBqu9AQADAgADeQADOgQ"},
    ],
    "Інше": [
        {"sku": "gift_box", "title": "Подарункова коробка", "price": 80, "photo": "AgACAgIAAxkBAAN7aabwno4x3rFsLX6VfmbBFE9vdtsAAhMTaxvOjDlJj4MhYEhqQ_cBAAMCAAN4AAM6BA"},
    ],
}

# =========================
# State (RAM) - OK for demo
# =========================
carts: Dict[int, Dict[str, int]] = {}   # user_id -> {sku: qty}
draft: Dict[int, Dict[str, Any]] = {}   # user_id -> order draft fields

# /fileid mode flag (so we don't break normal flow)
fileid_mode: Dict[int, bool] = {}       # user_id -> True/False


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
    buttons = []
    for cat in CATALOG.keys():
        buttons.append([InlineKeyboardButton(text=cat, callback_data=f"cat:{cat}")])
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


def find_item_by_sku(sku: str) -> Dict[str, Any] | None:
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


async def gs_create_order(session: ClientSession, payload: Dict[str, Any]) -> Dict[str, Any]:
    async with session.post(GS_ENDPOINT, json=payload, timeout=20) as resp:
        text = await resp.text()
        try:
            return json.loads(text)
        except Exception:
            return {"ok": False, "error": f"Bad response: {text[:200]}"}


async def gs_update_status(session: ClientSession, order_id: str, status: str) -> Dict[str, Any]:
    payload = {"key": GS_KEY, "action": "updateStatus", "bizId": BIZ_ID, "orderId": order_id, "status": status}
    async with session.post(GS_ENDPOINT, json=payload, timeout=20) as resp:
        text = await resp.text()
        try:
            return json.loads(text)
        except Exception:
            return {"ok": False, "error": f"Bad response: {text[:200]}"}


# =========================
# Bot + Dispatcher
# =========================
bot = Bot(BOT_TOKEN)
dp = Dispatcher()





# ---------- file_id helper (admin only, safe) ----------
@dp.message(Command("fileid"))
async def fileid_help(m: Message):
    if ADMIN_IDS and m.from_user.id not in ADMIN_IDS:
        return
    fileid_mode[m.from_user.id] = True
    await m.answer(
        "✅ Режим file_id увімкнено.\n"
        "Надішли 1 фото як повідомлення (не файлом). Я відповім file_id.\n"
        "Якщо надсилаєш кілька фото одразу (альбом) — я теж відповім.\n"
        "Щоб вимкнути — /fileidoff"
    )


@dp.message(Command("fileidoff"))
async def fileid_off(m: Message):
    if ADMIN_IDS and m.from_user.id not in ADMIN_IDS:
        return
    fileid_mode[m.from_user.id] = False
    await m.answer("✅ Режим file_id вимкнено.")


# 1) Окреме фото (працює в більшості випадків)
@dp.message(F.photo)
async def fileid_photo(m: Message):
    if ADMIN_IDS and m.from_user.id not in ADMIN_IDS:
        return
    if not fileid_mode.get(m.from_user.id, False):
        return

    photo = m.photo[-1]
    await m.answer(f"✅ file_id:\n{photo.file_id}")


# 2) Альбом (media_group)
@dp.message(F.media_group_id)
async def fileid_album(m: Message):
    if ADMIN_IDS and m.from_user.id not in ADMIN_IDS:
        return
    if not fileid_mode.get(m.from_user.id, False):
        return

    if m.photo:
        photo = m.photo[-1]
        await m.answer(f"✅ file_id:\n{photo.file_id}")


# 3) Гарантований перехват (коли Telegram/aiogram не дає спрацювати F.photo)
#    Працює тільки коли увімкнений /fileid, і не дає flow "з'їдати" фото.
@dp.message()
async def admin_fileid_catchall(m: Message):
    if ADMIN_IDS and m.from_user.id not in ADMIN_IDS:
        return
    if not fileid_mode.get(m.from_user.id, False):
        return

    if m.photo:
        photo = m.photo[-1]
        await m.answer(f"✅ file_id:\n{photo.file_id}")
        return

    await m.answer("Надішли саме фото (Gallery/Фото), не файл. Або вимкни режим: /fileidoff")


# ---------- main bot ----------
@dp.message(CommandStart())
async def start(m: Message):
    welcome_text = (
        "👋 Вітаємо у нашій кондитерській!\n\n"
        "🎂 Замовляйте торти та десерти онлайн за 1 хвилину.\n"
        "Менеджер одразу підтвердить замовлення.\n\n"
        "Оберіть дію нижче 👇"
    )
    await m.answer(welcome_text, reply_markup=main_menu_kb())


@dp.message(F.text == "📦 Каталог / Меню")
@dp.message(F.text == "🛒 Зробити замовлення")
async def show_catalog(m: Message):
    await m.answer("Оберіть категорію:", reply_markup=categories_kb())


@dp.message(F.text == "🚚 Доставка та оплата")
async def delivery(m: Message):
    await m.answer(
        "🚚 Доставка та оплата:\n"
        "• Доставка по місту\n"
        "• Самовивіз\n"
        "Оплата: готівка/переказ (на старті)."
    )


@dp.message(F.text == "☎️ Контакти")
async def contacts(m: Message):
    await m.answer("☎️ Контакти:\nМенеджер: @ruslanshum\nТел: +380973080330")


@dp.message(F.text == "🧾 Мої замовлення")
async def my_orders_stub(m: Message):
    await m.answer("🧾 Поки що в демо показ 'Мої замовлення' буде на наступному кроці.")


@dp.callback_query(F.data == "cats")
async def cats(cb: CallbackQuery):
    await cb.message.edit_text("Оберіть категорію:", reply_markup=categories_kb())
    await cb.answer()


@dp.callback_query(F.data.startswith("cat:"))
async def cat(cb: CallbackQuery):
    cat_name = cb.data.split(":", 1)[1]
    items = CATALOG.get(cat_name, [])
    text_lines = [f"📦 {cat_name}:\nОберіть товар нижче (натисніть на назву)."]
    kb = []
    for it in items:
        kb.append([InlineKeyboardButton(text=f"{it['title']} — {it['price']} {CURRENCY}", callback_data=f"prod:{it['sku']}")])
    kb.append([InlineKeyboardButton(text="🧺 Кошик", callback_data="cart")])
    kb.append([InlineKeyboardButton(text="⬅️ Категорії", callback_data="cats")])
    await cb.message.edit_text("\n".join(text_lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await cb.answer()


@dp.callback_query(F.data.startswith("prod:"))
async def prod(cb: CallbackQuery):
    sku = cb.data.split(":", 1)[1]
    item = find_item_by_sku(sku)

    if not item:
        await cb.answer("Товар не знайдено", show_alert=True)
        return

    text = (
        f"🧾 {item['title']}\n"
        f"💰 Ціна: {item['price']} {CURRENCY}\n\n"
        "Додати в кошик?"
    )

    # Якщо є фото — показуємо фото
    if item.get("photo"):
        await cb.message.answer_photo(
            photo=item["photo"],
            caption=text,
            reply_markup=product_kb(sku)
        )
    else:
        await cb.message.answer(
            text,
            reply_markup=product_kb(sku)
        )

    await cb.answer()


@dp.callback_query(F.data.startswith("add:"))
async def add(cb: CallbackQuery):
    sku = cb.data.split(":", 1)[1]
    carts.setdefault(cb.from_user.id, {})
    carts[cb.from_user.id][sku] = carts[cb.from_user.id].get(sku, 0) + 1
    await cb.answer("Додано ✅")


@dp.callback_query(F.data.startswith("rem:"))
async def rem(cb: CallbackQuery):
    sku = cb.data.split(":", 1)[1]
    if cb.from_user.id in carts and sku in carts[cb.from_user.id]:
        carts[cb.from_user.id][sku] -= 1
        if carts[cb.from_user.id][sku] <= 0:
            del carts[cb.from_user.id][sku]
        await cb.answer("Забрано ✅")
    else:
        await cb.answer("У кошику немає", show_alert=False)


@dp.callback_query(F.data == "cart")
async def cart(cb: CallbackQuery):
    await cb.message.edit_text(cart_text(cb.from_user.id), reply_markup=cart_kb())
    await cb.answer()


@dp.callback_query(F.data == "clear")
async def clear(cb: CallbackQuery):
    carts[cb.from_user.id] = {}
    draft.pop(cb.from_user.id, None)
    await cb.message.edit_text("🧺 Кошик очищено.", reply_markup=categories_kb())
    await cb.answer()


@dp.callback_query(F.data == "checkout")
async def checkout(cb: CallbackQuery):
    if not carts.get(cb.from_user.id):
        await cb.answer("Кошик порожній", show_alert=True)
        return

    draft[cb.from_user.id] = {"step": "name"}
    await cb.message.answer("✍️ Введіть ваше ім’я:")
    await cb.answer()


# IMPORTANT: flow must be TEXT-only, so it won't swallow photos (file_id mode etc.)
@dp.message(F.text)
async def flow(m: Message):
    user_id = m.from_user.id
    if user_id not in draft:
        return

    step = draft[user_id].get("step")

    if step == "name":
        draft[user_id]["name"] = (m.text or "").strip()
        draft[user_id]["step"] = "phone"
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📱 Поділитися контактом", request_contact=True)]],
            resize_keyboard=True, one_time_keyboard=True
        )
        await m.answer("📱 Надішліть телефон (кнопкою) або введіть вручну:", reply_markup=kb)
        return

    if step == "phone":
        phone = ""
        if m.contact and m.contact.phone_number:
            phone = m.contact.phone_number
        else:
            phone = (m.text or "").strip()
        draft[user_id]["phone"] = phone
        draft[user_id]["step"] = "deliveryType"
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🚚 Доставка"), KeyboardButton(text="🏃 Самовивіз")]],
            resize_keyboard=True, one_time_keyboard=True
        )
        await m.answer("Оберіть тип отримання:", reply_markup=kb)
        return

    if step == "deliveryType":
        t = (m.text or "").strip()
        if "Самовивіз" in t:
            draft[user_id]["deliveryType"] = "PICKUP"
            draft[user_id]["address"] = "-"
            draft[user_id]["step"] = "datetime"
            await m.answer("🕒 Вкажіть дату/час (наприклад: завтра 14:00):", reply_markup=main_menu_kb())
            return
        if "Доставка" in t:
            draft[user_id]["deliveryType"] = "DELIVERY"
            draft[user_id]["step"] = "address"
            await m.answer("🏠 Введіть адресу доставки:", reply_markup=main_menu_kb())
            return

        await m.answer("Будь ласка, натисніть кнопку: 🚚 Доставка або 🏃 Самовивіз")
        return

    if step == "address":
        draft[user_id]["address"] = (m.text or "").strip()
        draft[user_id]["step"] = "datetime"
        await m.answer("🕒 Вкажіть дату/час (наприклад: сьогодні 19:30):")
        return

    if step == "datetime":
        draft[user_id]["datetime"] = (m.text or "").strip()
        draft[user_id]["step"] = "comment"
        await m.answer("💬 Коментар (якщо доставка Новою Поштою, то вкажіть місто та номер відділення та ваші ПІБ отримувача і телефон отримувача):")
        return

    if step == "comment":
        comment = (m.text or "").strip()
        if comment == "-":
            comment = ""
        draft[user_id]["comment"] = comment

        items: List[Dict[str, Any]] = []
        for sku, qty in carts.get(user_id, {}).items():
            it = find_item_by_sku(sku)
            if not it:
                continue
            items.append({"sku": sku, "title": it["title"], "qty": qty, "price": it["price"]})

        total = calc_total(user_id)

        summary = [
            "✅ Перевірте замовлення:",
            cart_text(user_id),
            "",
            f"Ім’я: {draft[user_id]['name']}",
            f"Телефон: {draft[user_id]['phone']}",
            f"Тип: {draft[user_id]['deliveryType']}",
            f"Адреса: {draft[user_id].get('address','-')}",
            f"Дата/час: {draft[user_id]['datetime']}",
            f"Коментар: {draft[user_id]['comment'] or '-'}",
        ]

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Підтвердити", callback_data="confirm")],
            [InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel")]
        ])

        draft[user_id]["items"] = items
        draft[user_id]["total"] = total

        await m.answer("\n".join(summary), reply_markup=kb)
        draft[user_id]["step"] = "confirm_wait"
        return


@dp.callback_query(F.data == "cancel")
async def cancel(cb: CallbackQuery):
    carts[cb.from_user.id] = {}
    draft.pop(cb.from_user.id, None)
    await cb.message.edit_text("❌ Замовлення скасовано.")
    await cb.answer()


@dp.callback_query(F.data == "confirm")
async def confirm(cb: CallbackQuery):
    user_id = cb.from_user.id
    if user_id not in draft or not carts.get(user_id):
        await cb.answer("Немає активного замовлення", show_alert=True)
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

    async with ClientSession() as session:
        res = await gs_create_order(session, order_payload)

    if not res.get("ok"):
        await cb.message.answer(f"❌ Помилка збереження замовлення.\n{res}")
        await cb.answer()
        return

    order_id = str(res.get("orderId", ""))
    await cb.message.edit_text(f"🎉 Дякуємо! Замовлення прийнято.\nНомер: #{order_id}\nМенеджер скоро зв’яжеться.")

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

    await bot.send_message(
        chat_id=MANAGER_CHAT_ID,
        text="\n".join(mgr_text),
        reply_markup=manager_status_kb(order_id, str(user_id))
    )

    carts[user_id] = {}
    draft.pop(user_id, None)

    await cb.answer("Готово ✅")


@dp.callback_query(F.data.startswith("st:"))
async def set_status(cb: CallbackQuery):
    if ADMIN_IDS and cb.from_user.id not in ADMIN_IDS and cb.from_user.id != MANAGER_CHAT_ID:
        await cb.answer("Немає доступу", show_alert=True)
        return

    _, order_id, status, user_tg_id = cb.data.split(":", 3)

    async with ClientSession() as session:
        res = await gs_update_status(session, order_id, status)

    if res.get("ok"):
        await cb.answer(f"Статус: {status} ✅")
        try:
            await bot.send_message(int(user_tg_id), f"📦 Статус замовлення #{order_id}: {status}")
        except Exception:
            pass
    else:
        await cb.answer("Помилка оновлення статусу", show_alert=True)


# =========================
# Webhook server (aiohttp)
# =========================
async def on_startup(app: web.Application):
    webhook_url = f"{WEBHOOK_BASE}{WEBHOOK_PATH}"
    await bot.set_webhook(webhook_url)


async def on_shutdown(app: web.Application):
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.session.close()


async def handle_webhook(request: web.Request):
    try:
        if request.method != "POST":
            return web.Response(text="ok")

        try:
            update = await request.json()
        except Exception as e:
            body = await request.text()
            print("Webhook JSON parse error:", repr(e), "Body head:", body[:200])
            return web.Response(text="ok")

        await dp.feed_raw_update(bot, update)
        return web.Response(text="ok")

    except Exception as e:
        print("Webhook handler error:", repr(e))
        return web.Response(text="ok")


def build_app():
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=PORT)









