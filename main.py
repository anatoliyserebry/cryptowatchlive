
import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional, List

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BotCommand

# ======================
# Конфигурация
# ======================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")  # Вставьте токен сюда, если не используете переменные окружения
POLL_INTERVAL_SECONDS = 60  # период фоновой проверки цен
DB_PATH = os.getenv("DB_PATH", "cryptowatchlive.db")

# Фиатные валюты (ISO-коды) — расширяемый набор
FIAT_CODES = {
    "USD", "EUR", "RUB", "UAH", "KZT", "GBP", "JPY", "CNY", "TRY", "CHF",
    "PLN", "CZK", "SEK", "NOK", "DKK", "AUD", "CAD", "INR", "BRL", "ZAR",
}

# Сопоставление для «удобных» ссылок на биржи/обменники
BINANCE_BASE_URL = "https://www.binance.com/en/trade/{base}_{quote}?type=spot"
COINBASE_URL = "https://www.coinbase.com/advanced-trade/{base}-{quote}"
KRAKEN_URL = "https://pro.kraken.com/app/trade/{base}-{quote}"
COINGECKO_MARKETS_URL = "https://www.coingecko.com/en/coins/{coin_id}"
XE_CONVERTER_URL = "https://www.xe.com/currencyconverter/convert/?Amount=1&From={base}&To={quote}"
WISE_URL = "https://wise.com/transfer/{base}-to-{quote}"

# Для Binance часто используют USDT вместо USD — подменим в ссылке
BINANCE_QUOTE_ALIAS = {"USD": "USDT"}

# ======================
# Утилиты БД
# ======================
CREATE_USERS_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    is_muted INTEGER DEFAULT 0
);
"""

CREATE_SUBS_SQL = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    base TEXT NOT NULL,
    quote TEXT NOT NULL,
    asset_type TEXT NOT NULL, -- 'crypto', 'fiat', 'mixed', 'cc' (crypto/crypto)
    operator TEXT NOT NULL,   -- '>', '<', '>=', '<='
    threshold REAL NOT NULL,
    is_active INTEGER DEFAULT 1,
    last_eval INTEGER,        -- NULL/0/1: предыдущее значение условия
    created_at TEXT NOT NULL
);
"""

READ_SUBS_SQL = """
SELECT id, user_id, base, quote, asset_type, operator, threshold, is_active, last_eval
FROM subscriptions
WHERE is_active = 1;
"""

@dataclass
class Subscription:
    id: int
    user_id: int
    base: str
    quote: str
    asset_type: str
    operator: str
    threshold: float
    is_active: int
    last_eval: Optional[int]


# ======================
# Работа с API цен
# ======================
class PriceService:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self._cg_symbol_to_id: Dict[str, str] = {}
        self._cg_ready = False

    async def ensure_coingecko_map(self):
        """Загружаем карту symbol->id (берём топ по капе, если дубликаты)."""
        if self._cg_ready:
            return
        # Пробуем markets (топ-кап) — лучший способ выбрать «главный» id для символа
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1,
            "price_change_percentage": "24h",
        }
        mapping: Dict[str, str] = {}
        try:
            async with self.session.get(url, params=params, timeout=30) as r:
                r.raise_for_status()
                data = await r.json()
                for coin in data:
                    sym = str(coin.get("symbol", "")).upper()
                    cid = coin.get("id")
                    if sym and cid and sym not in mapping:
                        mapping[sym] = cid
        except Exception:
            # fallback на /coins/list
            url2 = "https://api.coingecko.com/api/v3/coins/list?include_platform=false"
            async with self.session.get(url2, timeout=30) as r:
                r.raise_for_status()
                data = await r.json()
                for coin in data:
                    sym = str(coin.get("symbol", "")).upper()
                    cid = coin.get("id")
                    if sym and cid and sym not in mapping:
                        mapping[sym] = cid
        self._cg_symbol_to_id = mapping
        self._cg_ready = True

    def is_fiat(self, code: str) -> bool:
        return code.upper() in FIAT_CODES

    async def _fetch_crypto_simple(self, base_id: str, quote: str) -> Tuple[float, Optional[float]]:
        """Цена crypto->fiat с 24h change (в процентах), если доступно."""
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": base_id,
            "vs_currencies": quote.lower(),
            "include_24hr_change": "true",
        }
        async with self.session.get(url, params=params, timeout=30) as r:
            r.raise_for_status()
            data = await r.json()
        if base_id not in data:
            raise ValueError("Asset not found on CoinGecko")
        price = float(data[base_id][quote.lower()])
        ch_key = f"{quote.lower()}_24h_change"
        ch = data[base_id].get(ch_key)
        change_pct = float(ch) if ch is not None else None
        return price, change_pct

    async def _fetch_fiat_rate(self, base: str, quote: str, date: Optional[str] = None) -> float:
        """Курс фиат->фиат на дату (YYYY-MM-DD) или текущий."""
        if date:
            url = f"https://api.exchangerate.host/{date}"
        else:
            url = "https://api.exchangerate.host/latest"
        params = {"base": base.upper(), "symbols": quote.upper()}
        async with self.session.get(url, params=params, timeout=30) as r:
            r.raise_for_status()
            data = await r.json()
        rates = data.get("rates") or {}
        if quote.upper() not in rates:
            raise ValueError("Fiat quote not supported")
        return float(rates[quote.upper()])

    async def get_price_and_change(self, base: str, quote: str) -> Tuple[float, Optional[float]]:
        """Возвращает (цена base/quote, 24h pct change)."""
        base_u = base.upper()
        quote_u = quote.upper()
        # fiat/fiat
        if self.is_fiat(base_u) and self.is_fiat(quote_u):
            now = await self._fetch_fiat_rate(base_u, quote_u)
            # 24h change относительно вчера
            y = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
            prev = await self._fetch_fiat_rate(base_u, quote_u, date=y)
            change = ((now - prev) / prev) * 100 if prev != 0 else None
            return now, change

        await self.ensure_coingecko_map()
        # crypto/fiat
        if not self.is_fiat(base_u) and self.is_fiat(quote_u):
            base_id = self._cg_symbol_to_id.get(base_u)
            if not base_id:
                raise ValueError("Unknown crypto symbol")
            return await self._fetch_crypto_simple(base_id, quote_u)

        # fiat/crypto  => 1 / (crypto/fiat)
        if self.is_fiat(base_u) and not self.is_fiat(quote_u):
            quote_id = self._cg_symbol_to_id.get(quote_u)
            if not quote_id:
                raise ValueError("Unknown crypto symbol")
            price_c, ch = await self._fetch_crypto_simple(quote_id, base_u)
            inv = 1.0 / price_c if price_c != 0 else float("inf")
            # изменение процентов для инвертированной пары приблизим как -ch
            return inv, (-ch if ch is not None else None)

        # crypto/crypto  => (base/USD) / (quote/USD), change ≈ ch_base - ch_quote
        base_id = self._cg_symbol_to_id.get(base_u)
        quote_id = self._cg_symbol_to_id.get(quote_u)
        if not base_id or not quote_id:
            raise ValueError("Unknown crypto symbol(s)")
        pb_usd, chb = await self._fetch_crypto_simple(base_id, "USD")
        pq_usd, chq = await self._fetch_crypto_simple(quote_id, "USD")
        price = pb_usd / pq_usd if pq_usd != 0 else float("inf")
        if chb is not None and chq is not None:
            change = chb - chq
        else:
            change = None
        return price, change


# ======================
# Проверка условий
# ======================
OPS = {">": lambda x, y: x > y, "<": lambda x, y: x < y, ">=": lambda x, y: x >= y, "<=": lambda x, y: x <= y}


def parse_watch_args(text: str) -> Optional[Tuple[str, str, float, str]]:
    """
    Парсим команлу /watch <BASE> <OP> <THRESHOLD> <QUOTE?>
    Примеры: 
      /watch BTC > 30000 USD
      /watch EUR < 95 RUB
      /watch ETH >= 0.06 BTC (crypto/crypto)
      /watch TON > 400 RUB
    Возвращает (base, quote, threshold, op)
    """
    # Уберём /watch и возможные лишние пробелы
    body = re.sub(r"^/watch\s*", "", text, flags=re.I).strip()
    if not body:
        return None
    # Разобьём, сохраняя операторы
    m = re.match(r"^(?P<base>[A-Za-z]{2,10})\s*(?P<op>>=|<=|>|<)\s*(?P<thresh>[0-9]+(?:[\.,][0-9]+)?)\s*(?P<quote>[A-Za-z]{2,10})?$",
                 body)
    if not m:
        return None
    base = m.group("base").upper()
    op = m.group("op")
    thresh_raw = m.group("thresh").replace(",", ".")
    try:
        threshold = float(thresh_raw)
    except ValueError:
        return None
    quote = (m.group("quote") or ("USD" if base not in FIAT_CODES else "RUB")).upper()
    return base, quote, threshold, op


# ======================
# Клавиатуры ссылок
# ======================

def make_exchange_keyboard(base: str, quote: str, price_service: PriceService) -> InlineKeyboardMarkup:
    base_u, quote_u = base.upper(), quote.upper()
    buttons: List[List[InlineKeyboardButton]] = []
    # Crypto рынки
    if not price_service.is_fiat(base_u) or not price_service.is_fiat(quote_u):
        # Для Binance заменим USD на USDT
        bq = BINANCE_QUOTE_ALIAS.get(quote_u, quote_u)
        try_pairs = [
            ("Binance", BINANCE_BASE_URL.format(base=base_u, quote=bq)),
            ("Coinbase", COINBASE_URL.format(base=base_u, quote=quote_u)),
            ("Kraken", KRAKEN_URL.format(base=base_u, quote=quote_u)),
        ]
        row = [InlineKeyboardButton(text=name, url=url) for name, url in try_pairs]
        buttons.append(row)
    # Fiat обменники/конвертеры
    if price_service.is_fiat(base_u) or price_service.is_fiat(quote_u):
        row2 = [
            InlineKeyboardButton(text="XE Converter", url=XE_CONVERTER_URL.format(base=base_u, quote=quote_u)),
            InlineKeyboardButton(text="Wise", url=WISE_URL.format(base=base_u.lower(), quote=quote_u.lower())),
        ]
        buttons.append(row2)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ======================
# Бот и хендлеры
# ======================

bot: Bot
router = Dispatcher()


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_USERS_SQL)
        await db.execute(CREATE_SUBS_SQL)
        await db.commit()


@router.message(Command("start"))
async def start_cmd(msg: Message):
    await ensure_user(msg.from_user.id)
    text = (
        "👋 Привет! Я CryptoWatchLive.

"
        "Я помогу отслеживать курсы криптовалют и фиатных валют и напомню, когда цена пересечёт нужный порог.

"
        "Примеры:
"
        "• /watch BTC > 30000 USD
"
        "• /watch EUR < 95 RUB
"
        "• /price BTC USD

"
        "Управление:
"
        "• /list — ваши подписки
"
        "• /pause <id> /resume <id> — пауза/возобновление
"
        "• /remove <id> — удалить
"
        "• /clear — удалить все
"
        "• /mute /unmute — глобально вкл/выкл уведомления
"
    )
    await msg.answer(text, reply_markup=main_menu_kb())


async def ensure_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            await db.execute("INSERT INTO users(user_id, is_muted) VALUES(?, 0)", (user_id,))
            await db.commit()


@router.message(Command("watch"))
async def watch_cmd(msg: Message):
    parsed = parse_watch_args(msg.text or "")
    if not parsed:
        await msg.answer(
            "Использование: /watch <BASE> <оператор> <значение> <QUOTE?>\n"
            "Примеры:\n"
            "• /watch BTC > 30000 USD\n"
            "• /watch EUR < 95 RUB\n"
            "• /watch ETH >= 0.06 BTC\n"
        )
        return
    base, quote, threshold, op = parsed
    asset_type = infer_asset_type(base, quote)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO subscriptions(user_id, base, quote, asset_type, operator, threshold, is_active, last_eval, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 1, NULL, ?)",
            (msg.from_user.id, base, quote, asset_type, op, threshold, datetime.utcnow().isoformat()),
        )
        await db.commit()
        cur = await db.execute("SELECT last_insert_rowid()")
        rowid = (await cur.fetchone())[0]
    await msg.answer(
        f"✅ Подписка #{rowid} создана: {base}/{quote} {op} {threshold}.\n"
        f"Буду присылать уведомление при срабатывании условия."
    )


def infer_asset_type(base: str, quote: str) -> str:
    b_f, q_f = base.upper() in FIAT_CODES, quote.upper() in FIAT_CODES
    if b_f and q_f:
        return "fiat"
    if not b_f and not q_f:
        return "cc"  # crypto/crypto
    return "mixed"


@router.message(Command("price"))
async def price_cmd(msg: Message):
    parts = (msg.text or "").split()
    if len(parts) < 3:
        await msg.answer("Использование: /price <BASE> <QUOTE>\nНапример: /price BTC USD или /price EUR RUB")
        return
    base, quote = parts[1].upper(), parts[2].upper()
    async with aiohttp.ClientSession() as session:
        ps = PriceService(session)
        try:
            price, change = await ps.get_price_and_change(base, quote)
        except Exception as e:
            await msg.answer(f"Не удалось получить цену: {e}")
            return
    kb = make_exchange_keyboard(base, quote, ps)
    ch_txt = f" ({change:+.2f}% за 24ч)" if change is not None else ""
    await msg.answer(f"Текущий курс {base}/{quote}: {price:.8f}{ch_txt}", reply_markup=kb)


@router.message(Command("list"))
async def list_cmd(msg: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, base, quote, operator, threshold, is_active FROM subscriptions WHERE user_id=? ORDER BY id",
            (msg.from_user.id,),
        )
        rows = await cur.fetchall()
    if not rows:
        await msg.answer("У вас пока нет подписок. Добавьте: /watch BTC > 30000 USD")
        return
    lines = ["Ваши подписки:"]
    for (sid, base, quote, op, thr, active) in rows:
        status = "⏸️" if not active else "✅"
        lines.append(f"#{sid}: {base}/{quote} {op} {thr} {status}")
    await msg.answer("\n".join(lines))


@router.message(Command("pause"))
async def pause_cmd(msg: Message):
    sid = extract_id_arg(msg.text)
    if sid is None:
        await msg.answer("Укажите id: /pause 3")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE subscriptions SET is_active=0 WHERE id=? AND user_id=?", (sid, msg.from_user.id))
        await db.commit()
    await msg.answer(f"Подписка #{sid} поставлена на паузу.")


@router.message(Command("resume"))
async def resume_cmd(msg: Message):
    sid = extract_id_arg(msg.text)
    if sid is None:
        await msg.answer("Укажите id: /resume 3")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE subscriptions SET is_active=1 WHERE id=? AND user_id=?", (sid, msg.from_user.id))
        await db.commit()
    await msg.answer(f"Подписка #{sid} возобновлена.")


@router.message(Command("remove"))
async def remove_cmd(msg: Message):
    sid = extract_id_arg(msg.text)
    if sid is None:
        await msg.answer("Укажите id: /remove 3")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM subscriptions WHERE id=? AND user_id=?", (sid, msg.from_user.id))
        await db.commit()
    await msg.answer(f"Подписка #{sid} удалена.")


@router.message(Command("clear"))
async def clear_cmd(msg: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM subscriptions WHERE user_id=?", (msg.from_user.id,))
        await db.commit()
    await msg.answer("Все ваши подписки удалены.")


@router.message(Command("mute"))
async def mute_cmd(msg: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_muted=1 WHERE user_id=?", (msg.from_user.id,))
        await db.commit()
    await msg.answer("🔕 Уведомления отключены. /unmute — чтобы включить")


@router.message(Command("unmute"))
async def unmute_cmd(msg: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_muted=0 WHERE user_id=?", (msg.from_user.id,))
        await db.commit()
    await msg.answer("🔔 Уведомления включены.")


def extract_id_arg(text: str) -> Optional[int]:
    parts = (text or "").split()
    if len(parts) < 2:
        return None
    try:
        return int(parts[1])
    except Exception:
        return None


# ======================
# Фоновая задача слежения за ценами
# ======================
async def price_watcher(bot: Bot):
    await asyncio.sleep(2)  # небольшая задержка после старта
    async with aiohttp.ClientSession() as session:
        ps = PriceService(session)
        while True:
            try:
                # Вытащим все активные подписки
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute(READ_SUBS_SQL) as cur:
                        rows = await cur.fetchall()
                subs = [Subscription(*row) for row in rows]
                if not subs:
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

                # Для каждого проверим условие
                for s in subs:
                    try:
                        price, change = await ps.get_price_and_change(s.base, s.quote)
                        ok = OPS[s.operator](price, s.threshold)
                    except Exception:
                        # пропустим ошибочные пары, чтобы не ломать цикл
                        continue

                    # Читаем is_muted пользователя
                    muted = False
                    async with aiosqlite.connect(DB_PATH) as db:
                        cur = await db.execute("SELECT is_muted FROM users WHERE user_id=?", (s.user_id,))
                        row = await cur.fetchone()
                        muted = bool(row and row[0])

                    # Логика уведомлений: отправляем при переходе из False/NULL -> True
                    should_notify = (not muted) and ok and (s.last_eval in (None, 0))

                    # Обновим last_eval
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("UPDATE subscriptions SET last_eval=? WHERE id=?", (1 if ok else 0, s.id))
                        await db.commit()

                    if should_notify:
                        ch_txt = f" ({change:+.2f}% за 24ч)" if change is not None else ""
                        text = (
                            f"⚡ Условие выполнено: #{s.id}\n"
                            f"{s.base}/{s.quote} {s.operator} {s.threshold}\n"
                            f"Текущий курс: {price:.8f}{ch_txt}"
                        )
                        kb = make_exchange_keyboard(s.base, s.quote, ps)
                        try:
                            await bot.send_message(chat_id=s.user_id, text=text, reply_markup=kb)
                        except Exception:
                            pass

            except Exception:
                # Глобальную ошибку цикла проглатываем, чтобы не завершать задачу
                pass
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


# ======================
# Точка входа
# ======================
async def main():
    global bot
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN (переменная окружения или константа в коде)")
    await init_db()
    bot = Bot(BOT_TOKEN, parse_mode=None)
    dp = Dispatcher()
    dp.include_router(router)

    # Запустим фоновую задачу
    loop = asyncio.get_event_loop()
    loop.create_task(price_watcher(bot))

    await bot.set_my_commands([
        BotCommand(command="start", description="Приветствие и меню"),
        BotCommand(command="menu", description="Показать главное меню"),
        BotCommand(command="watch", description="Создать подписку"),
        BotCommand(command="price", description="Текущий курс"),
        BotCommand(command="list", description="Мои подписки"),
        BotCommand(command="pause", description="Пауза подписки"),
        BotCommand(command="resume", description="Возобновить подписку"),
        BotCommand(command="remove", description="Удалить подписку"),
        BotCommand(command="clear", description="Удалить все подписки"),
        BotCommand(command="mute", description="Отключить уведомления"),
        BotCommand(command="unmute", description="Включить уведомления"),
    ])
    print("CryptoWatchLive запущен. Нажмите Ctrl+C для остановки.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("CryptoWatchLive остановлен.")


# ======================
# Главное меню (ReplyKeyboard) и быстрые кнопки
# ======================

def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Подписка"), KeyboardButton(text="📈 Цена")],
            [KeyboardButton(text="🗂️ Мои подписки")],
            [KeyboardButton(text="🔕 Mute"), KeyboardButton(text="🔔 Unmute")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие…",
    )


@router.message(Command("menu"))
async def menu_cmd(msg: Message):
    await msg.answer("Главное меню:", reply_markup=main_menu_kb())


# Быстрые кнопки (тексты)
from aiogram import F as _F  # алиас если F уже импортирован

@router.message(_F.text == "🗂️ Мои подписки")
async def btn_list(msg: Message):
    await list_cmd(msg)


@router.message(_F.text == "➕ Подписка")
async def btn_subscribe(msg: Message):
    await msg.answer(
        "Создать подписку командой:
"
        "• /watch BTC > 30000 USD
"
        "• /watch EUR < 95 RUB
"
        "• /watch ETH >= 0.06 BTC"
    )


@router.message(_F.text == "📈 Цена")
async def btn_price(msg: Message):
    await msg.answer("Запросите так: /price <BASE> <QUOTE>
Например: /price BTC USD")


@router.message(_F.text == "🔕 Mute")
async def btn_mute(msg: Message):
    await mute_cmd(msg)


@router.message(_F.text == "🔔 Unmute")
async def btn_unmute(msg: Message):
    await unmute_cmd(msg)
