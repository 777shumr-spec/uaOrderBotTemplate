import os
import json
import logging
import asyncio
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional, Set
import uuid
import time

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
from aiogram.client.session.aiohttp import AiohttpSession


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

# SUPERADMINS from env (your ids) - keep as is
ADMIN_IDS: Set[int] = set(int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit())

MANAGER_CHAT_ID = int((os.getenv("MANAGER_CHAT_ID", "0").strip() or "0"))

GS_ENDPOINT = os.getenv("GS_ENDPOINT", "").strip()
GS_KEY = os.getenv("GS_KEY", "").strip()
BIZ_ID = os.getenv("BIZ_ID", "demo").strip()
CURRENCY = os.getenv("CURRENCY", "UAH").strip()
SOURCE = os.getenv("SOURCE", "Telegram").strip()

WEBHOOK_BASE = (os.getenv("WEBHOOK_BASE", "").strip() or os.getenv("RENDER_EXTERNAL_URL", "").strip())
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook").strip()
PORT = int((os.getenv("PORT", "10000") or "10000"))

DROP_PENDING_UPDATES = os.getenv("DROP_PENDING_UPDATES", "0").strip() in ("1", "true", "True", "YES", "yes")
CATALOG_AUTOLOAD = os.getenv("CATALOG_AUTOLOAD", "0").strip() in ("1", "true", "True", "YES", "yes")
ROLES_AUTOLOAD = os.getenv("ROLES_AUTOLOAD", "1").strip() in ("1", "true", "True", "YES", "yes")

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
# Catalog (fallback hardcoded)
# =========================
CATALOG = {
    "Десерти": [
        {"sku": "cake_napoleon", "title": "Торт «Наполеон»", "price": 650,
         "photo": "AgACAgIAAxkBAAMHaagCWmo_c_YK4YRk5llKms4gd5MAAmEUaxvf1kFJr4WH6F_jZ_YBAAMCAANtAAM6BA",
         "description": "", "status": "IN_STOCK"},
        {"sku": "cake_honey", "title": "Торт «Медовик»", "price": 620,
         "photo": "AgACAgIAAxkBAAMJaagCZA-gL42QRDl6OvKYS399bb8AAmIUaxvf1kFJMk5lUqSqMKYBAAMCAANtAAM6BA",
         "description": "", "status": "IN_STOCK"},
        {"sku": "cupcake", "title": "Капкейки (1 шт)", "price": 55,
         "photo": "AgACAgIAAxkBAAMLaagCaUlnH66fW90ivi4WoagV48QAAmQUaxvf1kFJJql135zQU8gBAAMCAAN5AAM6BA",
         "description": "", "status": "IN_STOCK"},
    ],
    "Напої": [
        {"sku": "coffee", "title": "Кава", "price": 60,
         "photo": "AgACAgIAAxkBAAMNaagCbRZCO8cFb1ZEzUQd8PwYcDkAAmUUaxvf1kFJF2S_0uLiApMBAAMCAAN4AAM6BA",
         "description": "", "status": "IN_STOCK"},
        {"sku": "tea", "title": "Чай", "price": 40,
         "photo": "AgACAgIAAxkBAAMPaagCcL_xVc5L4W67KjQmOuOBggYAAmYUaxvf1kFJZLj7jsw7_5sBAAMCAAN5AAM6BA",
         "description": "", "status": "IN_STOCK"},
    ],
    "Інше": [
        {"sku": "gift_box", "title": "Подарункова коробка", "price": 80,
         "photo": "AgACAgIAAxkBAAMRaagCdB_y-QfBwFl_9LLiQPrO_SIAAmcUaxvf1kFJhZ7nhJhjw9ABAAMCAAN4AAM6BA",
         "description": "", "status": "IN_STOCK"},
    ],
}

CATALOG_RUNTIME: Dict[str, List[Dict[str, Any]]] = {}
CATALOG_LOADED_AT: Optional[str] = None
catalog_lock = asyncio.Lock()


# =========================
# Roles (runtime from sheet)
# =========================
ROLES_RUNTIME: Dict[int, str] = {}  # tg_id -> ROLE
ROLES_LOADED_AT: Optional[str] = None
roles_lock = asyncio.Lock()


# =========================
# State (RAM)
# =========================
carts: Dict[int, Dict[str, int]] = {}
draft: Dict[int, Dict[str, Any]] = {}
fileid_mode: Dict[int, bool] = {}


# =========================
# Queue (Webhook -> workers)
# =========================
UPDATE_QUEUE_MAX = 2000
update_queue: asyncio.Queue = asyncio.Queue(maxsize=UPDATE_QUEUE_MAX)

WORKERS = int(os.getenv("WORKERS", "4"))
MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT", "50"))
inflight_sem = asyncio.Semaphore(MAX_INFLIGHT)

_user_locks: Dict[int, asyncio.Lock] = {}
_user_locks_guard = asyncio.Lock()


async def _get_user_lock(uid: int) -> asyncio.Lock:
    async with _user_locks_guard:
        lk = _user_locks.get(uid)
        if lk is None:
            lk = asyncio.Lock()
            _user_locks[uid] = lk
        return lk


def _extract_uid_from_update(upd: Dict[str, Any]) -> int:
    try:
        if "message" in upd and upd["message"] and "from" in upd["message"]:
            return int(upd["message"]["from"]["id"])
        if "callback_query" in upd and upd["callback_query"] and "from" in upd["callback_query"]:
            return int(upd["callback_query"]["from"]["id"])
    except Exception:
        pass
    return 0


# =========================
# Persistent state
# =========================
STATE_FILE = os.getenv("STATE_FILE", "state.json")
state_lock = asyncio.Lock()
boot_id = os.getenv("BOOT_ID", "") or str(uuid.uuid4())[:8]
process_id = os.getpid()
boot_ts = int(time.time())

SCRIPT_SIGNATURE = "BOT_BUILD_2026-03-05__WHOAMI_FIX_V3"

def _serialize_state() -> Dict[str, Any]:
    return {
        "meta": {"boot_id": boot_id, "pid": process_id, "boot_ts": boot_ts},
        "carts": {str(uid): cart for uid, cart in carts.items()},
        "draft": {str(uid): d for uid, d in draft.items()},
        "fileid_mode": {str(uid): bool(v) for uid, v in fileid_mode.items()},
    }


def _restore_state(data: Dict[str, Any]):
    carts.clear()
    draft.clear()
    fileid_mode.clear()

    carts_data = data.get("carts", {}) or {}
    draft_data = data.get("draft", {}) or {}
    fileid_data = data.get("fileid_mode", {}) or {}

    for k, v in carts_data.items():
        try:
            carts[int(k)] = {str(sku): int(qty) for sku, qty in (v or {}).items()}
        except Exception:
            pass

    for k, v in draft_data.items():
        try:
            draft[int(k)] = v if isinstance(v, dict) else {}
        except Exception:
            pass

    for k, v in fileid_data.items():
        try:
            fileid_mode[int(k)] = bool(v)
        except Exception:
            pass


def _write_state_file(payload: Dict[str, Any]):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


async def save_state(reason: str = ""):
    async with state_lock:
        payload = _serialize_state()
        try:
            await asyncio.to_thread(_write_state_file, payload)
            log.info("💾 state saved (%s) carts=%d draft=%d", reason, len(carts), len(draft))
        except Exception as e:
            log.warning("💾 state save failed: %r", e)


async def load_state():
    async with state_lock:
        if not os.path.exists(STATE_FILE):
            log.info("💾 state file not found -> start fresh")
            return
        try:
            def _read():
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)

            data = await asyncio.to_thread(_read)
            _restore_state(data)
            meta = (data.get("meta") or {})
            log.info("💾 state loaded carts=%d draft=%d (prev boot_id=%s pid=%s)",
                     len(carts), len(draft), meta.get("boot_id"), meta.get("pid"))
        except Exception as e:
            log.warning("💾 state load failed: %r", e)


# =========================
# Helpers
# =========================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def norm_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return " ".join(str(s).replace("\u00a0", " ").split()).strip()


async def safe_typing_delay():
    await asyncio.sleep(0.08)


def _active_catalog() -> Dict[str, List[Dict[str, Any]]]:
    return CATALOG_RUNTIME if CATALOG_RUNTIME else CATALOG


def _role_of(uid: int) -> str:
    if uid in ADMIN_IDS:
        return "SUPERADMIN"
    return ROLES_RUNTIME.get(uid, "")


def _has_any_role(uid: int, allowed: Set[str]) -> bool:
    r = _role_of(uid)
    return (r in allowed)


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
    cat_src = _active_catalog()
    buttons = [[InlineKeyboardButton(text=cat, callback_data=f"cat:{cat}")] for cat in cat_src.keys()]
    buttons.append([InlineKeyboardButton(text="🧺 Кошик", callback_data="cart")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def product_kb(sku: str, can_add: bool = True) -> InlineKeyboardMarkup:
    row1 = []
    if can_add:
        row1.append(InlineKeyboardButton(text="➕ Додати", callback_data=f"add:{sku}"))
    row1.append(InlineKeyboardButton(text="➖ Забрати", callback_data=f"rem:{sku}"))

    return InlineKeyboardMarkup(inline_keyboard=[
        row1,
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
    cat_src = _active_catalog()
    for _, items in cat_src.items():
        for it in items:
            if it.get("sku") == sku:
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


def in_flow(uid: int) -> bool:
    return uid in draft


# =========================
# Telegram safe wrappers
# =========================
async def tg_call(coro, what: str = "tg_call"):
    try:
        return await coro
    except Exception as e:
        log.warning("Telegram API error in %s: %r", what, e)


async def safe_send(m: Message, text: str, reply_markup=None):
    await safe_typing_delay()
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
# GS calls (global session)
# =========================
gs_http: Optional[ClientSession] = None


async def _gs_post(payload: Dict[str, Any]) -> Dict[str, Any]:
    global gs_http
    if gs_http is None:
        return {"ok": False, "error": "GS session not initialized"}

    try:
        async with gs_http.post(GS_ENDPOINT, json=payload) as resp:
            text = await resp.text()
            try:
                data = json.loads(text)
                # attach status for debugging
                if isinstance(data, dict):
                    data["_http_status"] = resp.status
                return data
            except Exception:
                return {"ok": False, "error": f"Bad JSON response (status={resp.status}): {text[:400]}"}
    except Exception as e:
        return {"ok": False, "error": f"GS request failed: {repr(e)}"}


async def gs_create_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await _gs_post(payload)


async def gs_update_status(order_id: str, status: str) -> Dict[str, Any]:
    payload = {"key": GS_KEY, "action": "updateStatus", "bizId": BIZ_ID, "orderId": order_id, "status": status}
    return await _gs_post(payload)


async def gs_get_catalog() -> Dict[str, Any]:
    payload = {"key": GS_KEY, "action": "getCatalog", "bizId": BIZ_ID}
    return await _gs_post(payload)


async def gs_get_roles() -> Dict[str, Any]:
    payload = {"key": GS_KEY, "action": "getRoles", "bizId": BIZ_ID}
    return await _gs_post(payload)


def _build_catalog_runtime(gs_payload: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    catalog = (gs_payload.get("catalog") or {})
    categories = catalog.get("categories") or []
    products = catalog.get("products") or []

    id_to_title: Dict[str, str] = {}
    categories_sorted = sorted(
        categories,
        key=lambda x: (int(float(x.get("sort") or 0)), str(x.get("title") or ""))
    )
    for c in categories_sorted:
        cid = str(c.get("id") or "").strip()
        title = str(c.get("title") or "").strip()
        if cid and title:
            id_to_title[cid] = title

    out: Dict[str, List[Dict[str, Any]]] = {title: [] for title in id_to_title.values()}

    for p in products:
        sku = str(p.get("sku") or "").strip()
        cat_id = str(p.get("category_id") or "").strip()
        title = str(p.get("title") or "").strip()
        descr = str(p.get("description") or "").strip()
        price = int(float(p.get("price") or 0))
        image_url = str(p.get("image_url") or "").strip()
        status = str(p.get("status") or "IN_STOCK").strip()

        cat_title = id_to_title.get(cat_id)
        if not (sku and cat_title and title):
            continue

        out.setdefault(cat_title, []).append({
            "sku": sku,
            "title": title,
            "description": descr,
            "price": price,
            "photo": image_url,   # URL works for Telegram sendPhoto if public https
            "status": status
        })

    for k in list(out.keys()):
        out[k] = sorted(out[k], key=lambda it: it.get("title", ""))

    return out


async def refresh_catalog(reason: str = "") -> Dict[str, Any]:
    global CATALOG_RUNTIME, CATALOG_LOADED_AT
    async with catalog_lock:
        res = await gs_get_catalog()
        if not res.get("ok"):
            return res
        try:
            new_cat = _build_catalog_runtime(res)
            CATALOG_RUNTIME = new_cat
            CATALOG_LOADED_AT = now_str()
            log.info("📦 catalog refreshed (%s) categories=%d", reason, len(CATALOG_RUNTIME))
            return {"ok": True, "categories": len(CATALOG_RUNTIME), "loaded_at": CATALOG_LOADED_AT}
        except Exception as e:
            log.error("catalog parse failed: %r", e)
            log.error(traceback.format_exc())
            return {"ok": False, "error": f"catalog parse failed: {repr(e)}"}



async def refresh_roles(reason: str = "") -> Dict[str, Any]:
    global ROLES_RUNTIME, ROLES_LOADED_AT
    async with roles_lock:
        res = await gs_get_roles()
        if not res.get("ok"):
            return res
        try:
            roles_map = res.get("roles") or {}
            parsed: Dict[int, str] = {}
            for k, v in roles_map.items():
                try:
                    uid = int(str(k).strip())
                    role = str(v or "").strip().upper()
                    if role:
                        parsed[uid] = role
                except Exception:
                    pass
            ROLES_RUNTIME = parsed
            ROLES_LOADED_AT = now_str()
            log.info("👤 roles refreshed (%s) users=%d", reason, len(ROLES_RUNTIME))
            return {"ok": True, "users": len(ROLES_RUNTIME), "loaded_at": ROLES_LOADED_AT}
        except Exception as e:
            log.error("roles parse failed: %r", e)
            log.error(traceback.format_exc())
            return {"ok": False, "error": f"roles parse failed: {repr(e)}"}


# =========================
# Bot + Dispatcher
# =========================
tg_timeout = ClientTimeout(total=20, connect=10, sock_connect=10, sock_read=20)
bot = Bot(BOT_TOKEN, session=AiohttpSession(timeout=tg_timeout))
dp = Dispatcher()


class CrashGuardMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        try:
            return await handler(event, data)
        except Exception as e:
            log.error("🔥 UNHANDLED ERROR: %r", e)
            log.error(traceback.format_exc())
            # do not crash, but also do not silently ignore in command flow
            return


dp.update.middleware(CrashGuardMiddleware())

@dp.message()
async def _debug_log_all_messages(m: Message):
    # Лише лог, без відповіді. Допомагає побачити що реально прилітає.
    try:
        txt = (m.text or m.caption or "")
        log.info("📩 IN msg from=%s chat=%s text=%r entities=%s",
                 m.from_user.id if m.from_user else None,
                 m.chat.id if m.chat else None,
                 txt[:200],
                 [(e.type, e.offset, e.length) for e in (m.entities or [])][:5])
    except Exception:
        pass

# =========================
# Admin commands (with roles)
# =========================

@dp.message(Command("whoami"))
async def cmd_whoami(m: Message):
    uid = m.from_user.id
    await safe_send(
        m,
        f"you={uid}\n"
        f"your_role={_role_of(uid) or 'NONE'}\n"
        f"roles_loaded_at={ROLES_LOADED_AT}\n"
        f"roles_users={len(ROLES_RUNTIME)}\n"
        f"admin_ids_env={sorted(list(ADMIN_IDS))}"
    )

@dp.message(Command("gs_roles_raw"))
async def cmd_gs_roles_raw(m: Message):
    uid = m.from_user.id
    if uid not in ADMIN_IDS:
        return
    res = await gs_get_roles()
    await safe_send(m, json.dumps(res, ensure_ascii=False)[:3500])

@dp.message(Command("refresh_roles"))
async def cmd_refresh_roles(m: Message):
    uid = m.from_user.id

    # аварійний ключ: якщо ти в ADMIN_IDS env — можеш оновити ролі завжди
    if uid not in ADMIN_IDS and not _has_any_role(uid, {"SUPERADMIN", "ADMIN"}):
        await safe_send(m, "⛔ Немає доступу (потрібна роль ADMIN/SUPERADMIN).")
        return

    await safe_send(m, "⏳ Оновлюю ролі з Google Sheets...")
    res = await refresh_roles("manual")
    await safe_send(m, f"✅ {res}" if res.get("ok") else f"❌ {res}")


@dp.message(Command("refresh_catalog"))
async def cmd_refresh_catalog(m: Message):
    uid = m.from_user.id
    if not _has_any_role(uid, {"SUPERADMIN", "ADMIN"}):
        await safe_send(m, "⛔ Немає доступу (потрібна роль ADMIN/SUPERADMIN).")
        return

    await safe_send(m, "⏳ Оновлюю каталог з Google Sheets...")

    try:
        res = await refresh_catalog("manual")
        if res.get("ok"):
            await safe_send(m, f"✅ Каталог оновлено. Категорій: {res.get('categories')} | {res.get('loaded_at')}")
        else:
            await safe_send(m, f"❌ Не вдалося оновити каталог: {res}")
    except Exception as e:
        await safe_send(m, f"❌ refresh_catalog crashed: {repr(e)}")


@dp.message(Command("catalog_info"))
async def cmd_catalog_info(m: Message):
    uid = m.from_user.id
    if not _has_any_role(uid, {"SUPERADMIN", "ADMIN"}):
        await safe_send(m, "⛔ Немає доступу (потрібна роль ADMIN/SUPERADMIN).")
        return

    src = _active_catalog()
    total_items = sum(len(v) for v in src.values())
    await safe_send(
        m,
        f"📦 Catalog info\n"
        f"loaded_at={CATALOG_LOADED_AT}\n"
        f"categories={len(src)} items={total_items}\n"
        f"source={'GS' if CATALOG_RUNTIME else 'fallback'}\n"
        f"roles_loaded_at={ROLES_LOADED_AT} roles_users={len(ROLES_RUNTIME)}\n"
        f"your_role={_role_of(uid)}"
    )


@dp.message(Command("debug_state"))
async def debug_state(m: Message):
    uid = m.from_user.id
    if not _has_any_role(uid, {"SUPERADMIN", "ADMIN"}):
        await safe_send(m, "⛔ Немає доступу (потрібна роль ADMIN/SUPERADMIN).")
        return

    await safe_send(
        m,
        f"DEBUG boot_id={boot_id} pid={process_id}\nuser={uid}\n"
        f"your_role={_role_of(uid)}\n"
        f"draft={draft.get(uid)}\ncart={carts.get(uid)}\nqueue={update_queue.qsize()}\n"
        f"workers={WORKERS} inflight_limit={MAX_INFLIGHT}\n"
        f"drop_pending={DROP_PENDING_UPDATES}\n"
        f"catalog_loaded_at={CATALOG_LOADED_AT} runtime_categories={len(CATALOG_RUNTIME)}\n"
        f"roles_loaded_at={ROLES_LOADED_AT} roles_users={len(ROLES_RUNTIME)}"
    )


@dp.message(Command("ping"))
async def ping(m: Message):
    await safe_send(m, f"pong ✅ boot_id={boot_id} pid={process_id}")


@dp.message(Command("reset"))
async def reset(m: Message):
    uid = m.from_user.id
    carts.pop(uid, None)
    draft.pop(uid, None)
    await save_state("reset")
    await safe_send(m, "✅ Стан очищено. Можете почати заново.", reply_markup=main_menu_kb())


# =========================
# Admin file_id
# =========================
@dp.message(Command("fileid"))
async def fileid_help(m: Message):
    uid = m.from_user.id
    if not _has_any_role(uid, {"SUPERADMIN", "ADMIN"}):
        await safe_send(m, "⛔ Немає доступу (потрібна роль ADMIN/SUPERADMIN).")
        return
    fileid_mode[uid] = True
    await save_state("fileid_on")
    await safe_send(
        m,
        "✅ Режим file_id увімкнено.\n"
        "Надішли 1 фото як повідомлення (не файлом). Я відповім file_id.\n"
        "Щоб вимкнути — /fileidoff"
    )


@dp.message(Command("fileidoff"))
async def fileid_off(m: Message):
    uid = m.from_user.id
    if not _has_any_role(uid, {"SUPERADMIN", "ADMIN"}):
        await safe_send(m, "⛔ Немає доступу (потрібна роль ADMIN/SUPERADMIN).")
        return
    fileid_mode[uid] = False
    await save_state("fileid_off")
    await safe_send(m, "✅ Режим file_id вимкнено.")


@dp.message(F.photo)
async def fileid_photo(m: Message):
    uid = m.from_user.id
    if not _has_any_role(uid, {"SUPERADMIN", "ADMIN"}):
        return
    if not fileid_mode.get(uid, False):
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


async def _warn_in_flow(m: Message) -> bool:
    if in_flow(m.from_user.id):
        await safe_send(m, "⚠️ Ви зараз оформлюєте замовлення. Будь ласка, завершіть крок або введіть /reset щоб почати з нуля.")
        return True
    return False


@dp.message(F.text == "🛒 Зробити замовлення")
@dp.message(F.text == "📦 Каталог / Меню")
async def show_catalog(m: Message):
    if await _warn_in_flow(m):
        return
    await safe_send(m, "Оберіть категорію:", reply_markup=categories_kb())


@dp.message(F.text == "🚚 Доставка та оплата")
async def delivery(m: Message):
    if await _warn_in_flow(m):
        return
    await safe_send(
        m,
        "🚚 Доставка та оплата:\n"
        "• Доставка по місту\n"
        "• Самовивіз\n"
        "Оплата: готівка/переказ (на старті)."
    )


@dp.message(F.text == "☎️ Контакти")
async def contacts(m: Message):
    if await _warn_in_flow(m):
        return
    await safe_send(m, "☎️ Контакти:\nМенеджер: @ruslanshum\nТел: +380973080330")


@dp.message(F.text == "🧾 Мої замовлення")
async def my_orders_stub(m: Message):
    if await _warn_in_flow(m):
        return
    await safe_send(m, "🧾 Демо показ. 'Мої замовлення' буде на наступному кроці.")


@dp.callback_query(F.data == "cats")
async def cats(cb: CallbackQuery):
    await safe_edit(cb, "Оберіть категорію:", reply_markup=categories_kb())
    await tg_call(cb.answer(), what="cb.answer(cats)")


@dp.callback_query(F.data.startswith("cat:"))
async def cat(cb: CallbackQuery):
    cat_name = cb.data.split(":", 1)[1]
    items = _active_catalog().get(cat_name, [])
    kb = []
    for it in items:
        status = str(it.get("status") or "IN_STOCK").strip()
        mark = "✅" if status == "IN_STOCK" else "⏳"
        kb.append([InlineKeyboardButton(
            text=f"{mark} {it['title']} — {it['price']} {CURRENCY}",
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

    status = str(item.get("status") or "IN_STOCK").strip()
    can_add = (status == "IN_STOCK")

    desc = (item.get("description") or "").strip()
    status_line = "✅ В наявності" if can_add else f"⏳ Статус: {status}"

    text = (
        f"🧾 {item['title']}\n"
        f"💰 Ціна: {item['price']} {CURRENCY}\n"
        f"{status_line}\n\n"
        f"{desc}\n\n"
        "Додати в кошик?"
    ).strip()

    photo_ref = (item.get("photo") or "").strip()
    if photo_ref:
        await tg_call(
            cb.message.answer_photo(photo=photo_ref, caption=text, reply_markup=product_kb(sku, can_add=can_add)),
            what="answer_photo"
        )
    else:
        await tg_call(cb.message.answer(text, reply_markup=product_kb(sku, can_add=can_add)), what="answer_product_text")

    await tg_call(cb.answer(), what="cb.answer(prod)")


@dp.callback_query(F.data.startswith("add:"))
async def add(cb: CallbackQuery):
    sku = cb.data.split(":", 1)[1]
    item = find_item_by_sku(sku)
    if not item:
        await tg_call(cb.answer("Товар не знайдено", show_alert=True), what="cb.answer(add_not_found)")
        return

    status = str(item.get("status") or "IN_STOCK").strip()
    if status != "IN_STOCK":
        await tg_call(cb.answer("Цього товару зараз немає в наявності ⏳", show_alert=True), what="cb.answer(add_no_stock)")
        return

    carts.setdefault(cb.from_user.id, {})
    carts[cb.from_user.id][sku] = carts[cb.from_user.id].get(sku, 0) + 1
    log.info("🛒 add user=%s sku=%s qty=%s cart=%s",
             cb.from_user.id, sku, carts[cb.from_user.id][sku], carts[cb.from_user.id])
    await save_state("add")
    await tg_call(cb.answer("Додано ✅"), what="cb.answer(add)")


@dp.callback_query(F.data.startswith("rem:"))
async def rem(cb: CallbackQuery):
    sku = cb.data.split(":", 1)[1]
    if cb.from_user.id in carts and sku in carts[cb.from_user.id]:
        carts[cb.from_user.id][sku] -= 1
        if carts[cb.from_user.id][sku] <= 0:
            del carts[cb.from_user.id][sku]
        log.info("🛒 rem user=%s sku=%s cart=%s", cb.from_user.id, sku, carts.get(cb.from_user.id, {}))
        await save_state("rem")
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
    await save_state("clear")
    await safe_edit(cb, "🧺 Кошик очищено.", reply_markup=categories_kb())
    await tg_call(cb.answer(), what="cb.answer(clear)")


@dp.callback_query(F.data == "checkout")
async def checkout(cb: CallbackQuery):
    if not carts.get(cb.from_user.id):
        await tg_call(cb.answer("Кошик порожній", show_alert=True), what="cb.answer(checkout_empty)")
        return
    draft[cb.from_user.id] = {"step": "name"}
    await save_state("checkout")
    log.info("checkout -> step=name user=%s", cb.from_user.id)
    await tg_call(cb.message.answer("✍️ Введіть ваше ім’я:"), what="ask_name")
    await tg_call(cb.answer(), what="cb.answer(checkout)")


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
    await save_state("phone_contact")

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚚 Доставка"), KeyboardButton(text="🏃 Самовивіз")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await safe_send(m, "Оберіть тип отримання:", reply_markup=kb)


@dp.message(F.text)
async def flow(m: Message):
    # ❗ не чіпаємо команди типу /ping /whoami /refresh_roles
    if m.text and m.text.strip().startswith("/"):
        return

    user_id = m.from_user.id
    if user_id not in draft:
        return

    step = draft[user_id].get("step")
    text = norm_text(m.text)
    log.info("flow user=%s step=%s text=%r", user_id, step, text[:120])

    if step == "name":
        draft[user_id]["name"] = text
        draft[user_id]["step"] = "phone"
        await save_state("name")
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📱 Поділитися контактом", request_contact=True)]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await safe_send(m, "📱 Надішліть телефон (кнопкою) або введіть вручну:", reply_markup=kb)
        return

    if step == "phone":
        draft[user_id]["phone"] = text
        draft[user_id]["step"] = "deliveryType"
        await save_state("phone_text")
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🚚 Доставка"), KeyboardButton(text="🏃 Самовивіз")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await safe_send(m, "Оберіть тип отримання:", reply_markup=kb)
        return

    if step == "deliveryType":
        if "Самовивіз" in text:
            draft[user_id]["deliveryType"] = "PICKUP"
            draft[user_id]["address"] = "-"
            draft[user_id]["step"] = "datetime"
            await save_state("delivery_pickup")
            await safe_send(m, "🕒 Вкажіть біжіну дату/час (наприклад: завтра 14:00):")
            return

        if "Доставка" in text:
            draft[user_id]["deliveryType"] = "DELIVERY"
            draft[user_id]["step"] = "address"
            await save_state("delivery_delivery")
            await safe_send(m, "🏠 Введіть адресу доставки:")
            return

        await safe_send(m, "Будь ласка, натисніть кнопку: 🚚 Доставка або 🏃 Самовивіз")
        return

    if step == "address":
        draft[user_id]["address"] = text
        draft[user_id]["step"] = "datetime"
        await save_state("address")
        await safe_send(m, "🕒 Вкажіть бажану дату/час (наприклад: сьогодні 19:30):")
        return

    if step == "datetime":
        draft[user_id]["datetime"] = text
        draft[user_id]["step"] = "comment"
        await save_state("datetime")
        await safe_send(m, "💬 Коментар (напишіть коментар або просто напишіть - щоб перейти до підтвердження замовлення):")
        return

    if step == "comment":
        comment = "" if text == "-" else text
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
        await save_state("comment_confirm_wait")

        await safe_send(m, "\n".join(summary), reply_markup=kb)
        return


@dp.callback_query(F.data == "cancel")
async def cancel(cb: CallbackQuery):
    carts[cb.from_user.id] = {}
    draft.pop(cb.from_user.id, None)
    await save_state("cancel")
    await safe_edit(cb, "❌ Замовлення скасовано.")
    await tg_call(cb.answer(), what="cb.answer(cancel)")

    await tg_call(
        cb.message.answer(
            "Ок. Щоб почати заново — натисніть 🛒 Зробити замовлення або 📦 Каталог / Меню.",
            reply_markup=main_menu_kb()
        ),
        what="post_cancel_main_menu"
    )


@dp.callback_query(F.data == "confirm")
async def confirm(cb: CallbackQuery):
    user_id = cb.from_user.id
    if user_id not in draft or not carts.get(user_id):
        await tg_call(cb.answer("Немає активного замовлення", show_alert=True), what="cb.answer(confirm_noactive)")
        return

    if draft[user_id].get("_confirming"):
        await tg_call(cb.answer("⏳ Уже зберігаю...", show_alert=False), what="cb.answer(confirm_busy)")
        return
    draft[user_id]["_confirming"] = True
    await save_state("confirm_pressed")

    await tg_call(cb.answer("⏳ Зберігаю...", show_alert=False), what="cb.answer(confirm_progress)")

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

    res = await gs_create_order(order_payload)

    if not res.get("ok"):
        draft[user_id]["_confirming"] = False
        await save_state("confirm_gs_error")
        await tg_call(cb.message.answer(f"❌ Помилка збереження замовлення.\n{res}"), what="send_gs_error")
        return

    order_id = str(res.get("orderId", ""))
    await safe_edit(cb, f"🎉 Дякуємо! Замовлення прийнято.\nНомер: #{order_id}\nМенеджер скоро зв’яжеться.")

    await tg_call(
        bot.send_message(
            chat_id=user_id,
            text="✅ Якщо бажаєте зробити наступне замовлення — натисніть 🛒 Зробити замовлення або відкрийте 📦 Каталог / Меню.",
            reply_markup=main_menu_kb()
        ),
        what="post_confirm_main_menu"
    )

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

    await tg_call(
        bot.send_message(
            chat_id=MANAGER_CHAT_ID,
            text="\n".join(mgr_text),
            reply_markup=manager_status_kb(order_id, str(user_id))
        ),
        what="send_to_manager"
    )

    carts[user_id] = {}
    draft.pop(user_id, None)
    await save_state("confirm_done")


@dp.callback_query(F.data.startswith("st:"))
async def set_status(cb: CallbackQuery):
    uid = cb.from_user.id
    # allow: SUPERADMIN/ADMIN/MANAGER or MANAGER_CHAT_ID
    if uid != MANAGER_CHAT_ID and not _has_any_role(uid, {"SUPERADMIN", "ADMIN", "MANAGER"}):
        await tg_call(cb.answer("Немає доступу", show_alert=True), what="cb.answer(st_denied)")
        return

    _, order_id, status, user_tg_id = cb.data.split(":", 3)
    res = await gs_update_status(order_id, status)

    if res.get("ok"):
        await tg_call(cb.answer(f"Статус: {status} ✅"), what="cb.answer(st_ok)")
        await tg_call(bot.send_message(int(user_tg_id), f"📦 Статус замовлення #{order_id}: {status}"),
                      what="notify_user_status")
    else:
        await tg_call(cb.answer("Помилка оновлення статусу", show_alert=True), what="cb.answer(st_err)")



# =========================
# Fallback for ANY command (must be AFTER all specific command handlers)
# =========================
@dp.message(Command())
async def any_command_fallback(m: Message):
    # Якщо сюди потрапили — значить специфічний хендлер не спрацював.
    cmd = (m.text or "").split()[0]

    await safe_send(
        m,
        "🤖 Команду отримав, але відповідний хендлер не спрацював.\n\n"
        f"Команда: {cmd}\n\n"
        "Спробуйте:\n"
        "/ping\n"
        "/whoami\n"
        "/refresh_roles\n"
        "/gs_roles_raw\n"
        "/debug_state\n\n"
        "Якщо навіть /ping не відповідає — значить деплой/код не той або webhook веде не туди."
    )



# =========================
# Workers
# =========================
async def _handle_one_update(upd: Dict[str, Any]):
    uid = _extract_uid_from_update(upd)
    lk = await _get_user_lock(uid) if uid else None

    async with inflight_sem:
        if lk:
            async with lk:
                await dp.feed_raw_update(bot, upd)
        else:
            await dp.feed_raw_update(bot, upd)


async def update_worker(worker_id: int):
    log.info("✅ update_worker[%s] started boot_id=%s pid=%s", worker_id, boot_id, process_id)
    while True:
        upd = await update_queue.get()
        try:
            await _handle_one_update(upd)
        except Exception as e:
            log.error("🔥 feed_raw_update failed (worker=%s): %r", worker_id, e)
            log.error(traceback.format_exc())
        finally:
            update_queue.task_done()


# =========================
# Webhook server lifecycle
# =========================
async def app_lifecycle(app: web.Application):
    global gs_http
    log.info("🚀 BOOT boot_id=%s pid=%s drop_pending=%s", boot_id, process_id, DROP_PENDING_UPDATES)
    log.info("🧩 SCRIPT_SIGNATURE=%s", SCRIPT_SIGNATURE)
    worker_tasks: List[asyncio.Task] = []

    try:
        await load_state()

        gs_timeout = ClientTimeout(total=25, connect=10, sock_connect=10, sock_read=25)
        gs_http = ClientSession(timeout=gs_timeout)

        for i in range(WORKERS):
            t = asyncio.create_task(update_worker(i + 1))
            worker_tasks.append(t)
        app["worker_tasks"] = worker_tasks

        # auto-load roles first
        if ROLES_AUTOLOAD:
            rr = await refresh_roles("boot")
            log.info("roles autoload result: %s", rr)

        if CATALOG_AUTOLOAD:
            rc = await refresh_catalog("boot")
            log.info("catalog autoload result: %s", rc)

        if WEBHOOK_BASE:
            webhook_url = f"{WEBHOOK_BASE}{WEBHOOK_PATH}"
            try:
                if DROP_PENDING_UPDATES:
                    await bot.delete_webhook(drop_pending_updates=True)

                await bot.set_webhook(
                    webhook_url,
                    allowed_updates=["message", "callback_query"],
                    max_connections=40
                )
                log.info("✅ Webhook set to: %s", webhook_url)
            except Exception as e:
                log.exception("❌ set_webhook failed: %r", e)
        else:
            log.warning("WEBHOOK_BASE is empty. Webhook will NOT be set.")

        yield  # RUNNING

    finally:
        for t in worker_tasks:
            t.cancel()
        for t in worker_tasks:
            try:
                await t
            except Exception:
                pass

        if gs_http is not None:
            try:
                await gs_http.close()
            except Exception:
                pass
            gs_http = None

        try:
            await bot.session.close()
        except Exception:
            pass

        log.info("🧹 SHUTDOWN done boot_id=%s pid=%s", boot_id, process_id)


async def handle_webhook(request: web.Request):
    try:
        update = await request.json()
    except Exception:
        return web.Response(text="ok")

    if update_queue.full():
        log.error("❌ UPDATE QUEUE FULL (%s). Dropping update.", UPDATE_QUEUE_MAX)
        return web.Response(text="ok")

    update_queue.put_nowait(update)
    return web.Response(text="ok")


def build_app():
    app = web.Application()

    async def health(_request):
        return web.Response(text="ok")

    async def healthz(_request):
        return web.json_response({
            "ok": True,
            "boot_id": boot_id,
            "pid": process_id,
            "queue": update_queue.qsize(),
            "workers": WORKERS,
            "inflight_limit": MAX_INFLIGHT,
            "drop_pending": DROP_PENDING_UPDATES,
            "catalog_loaded_at": CATALOG_LOADED_AT,
            "runtime_categories": len(CATALOG_RUNTIME),
            "roles_loaded_at": ROLES_LOADED_AT,
            "roles_users": len(ROLES_RUNTIME),
        })

    app.router.add_get("/", health)
    app.router.add_get("/healthz", healthz)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)

    app.cleanup_ctx.append(app_lifecycle)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=PORT)


























