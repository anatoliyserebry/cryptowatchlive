import asyncio
import os
import re
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional, List

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand,
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('cryptowatchlive.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN") or "8240587038:AAGDO8RzkQ1N34tl-xrn9I1JbEW6kn9JgJQ"  
POLL_INTERVAL_SECONDS = 60               
DB_PATH = os.getenv("DB_PATH", "cryptowatchlive.db")

FIAT_CODES = {
    "USD", "EUR", "RUB", "UAH", "KZT", "GBP", "JPY", "CNY", "TRY", "CHF",
    "PLN", "CZK", "SEK", "NOK", "DKK", "AUD", "CAD", "INR", "BRL", "ZAR",
}

BINANCE_BASE_URL = "https://www.binance.com/en/trade/{base}_{quote}?type=spot"
COINBASE_URL     = "https://www.coinbase.com/advanced-trade/{base}-{quote}"
KRAKEN_URL       = "https://pro.kraken.com/app/trade/{base}-{quote}"
XE_CONVERTER_URL = "https://www.xe.com/currencyconverter/convert/?Amount=1&From={base}&To={quote}"
WISE_URL         = "https://wise.com/transfer/{base}-to-{quote}"
BINANCE_QUOTE_ALIAS = {"USD": "USDT"}  

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
    asset_type TEXT NOT NULL, -- 'crypto', 'fiat', 'mixed', 'cc'
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

class PriceService:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self._cg_symbol_to_id: Dict[str, str] = {}
        self._cg_ready = False
        logger.info("PriceService инициализирован")

    async def ensure_coingecko_map(self):
        """Загружаем карту symbol->id (берём топ по капе, если дубликаты)."""
        if self._cg_ready:
            return
            
        logger.info("Загрузка карты символов CoinGecko...")
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
            logger.info(f"Успешно загружено {len(mapping)} символов из CoinGecko markets API")
        except Exception as e:
            logger.warning(f"Ошибка при загрузке из markets API: {e}, пробуем альтернативный метод")
            
            try:
                url2 = "https://api.coingecko.com/api/v3/coins/list?include_platform=false"
                async with self.session.get(url2, timeout=30) as r:
                    r.raise_for_status()
                    data = await r.json()
                    for coin in data:
                        sym = str(coin.get("symbol", "")).upper()
                        cid = coin.get("id")
                        if sym and cid and sym not in mapping:
                            mapping[sym] = cid
                logger.info(f"Успешно загружено {len(mapping)} символов из CoinGecko list API")
            except Exception as e2:
                logger.error(f"Критическая ошибка при загрузке карты символов: {e2}")
                raise

        self._cg_symbol_to_id = mapping
        self._cg_ready = True
        logger.debug(f"Карта символов готова, всего символов: {len(mapping)}")

    def is_fiat(self, code: str) -> bool:
        return code.upper() in FIAT_CODES

    async def _fetch_crypto_simple(self, base_id: str, quote: str) -> Tuple[float, Optional[float]]:
        logger.debug(f"Запрос цены криптовалюты: {base_id}/{quote}")
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": base_id,
            "vs_currencies": quote.lower(),
            "include_24hr_change": "true",
        }
        try:
            async with self.session.get(url, params=params, timeout=30) as r:
                r.raise_for_status()
                data = await r.json()
            if base_id not in data:
                logger.warning(f"Ассет {base_id} не найден на CoinGecko")
                raise ValueError("Asset not found on CoinGecko")
            price = float(data[base_id][quote.lower()])
            ch_key = f"{quote.lower()}_24h_change"
            ch = data[base_id].get(ch_key)
            change_pct = float(ch) if ch is not None else None
            logger.debug(f"Получена цена {base_id}/{quote}: {price}, изменение: {change_pct}")
            return price, change_pct
        except Exception as e:
            logger.error(f"Ошибка при получении цены {base_id}/{quote}: {e}")
            raise

    async def _fetch_fiat_rate(self, base: str, quote: str, date: Optional[str] = None) -> float:
        """
        Курс фиат->фиат на дату (YYYY-MM-DD) или текущий.
        1) Пытаемся через XML ЦБ РФ
        2) Фоллбек: frankfurter.app (ECB)
        """
        base_u, quote_u = base.upper(), quote.upper()
        logger.debug(f"Запрос курса фиата: {base_u}/{quote_u}, дата: {date}")

        async def _cbr_xml() -> Optional[float]:
            """Получение курса от ЦБ РФ в XML формате"""
            try:
                # Формируем URL для XML API ЦБ РФ
                if date:
                    # Для исторических данных используем формат DD/MM/YYYY
                    date_obj = datetime.strptime(date, "%Y-%m-%d")
                    xml_date = date_obj.strftime("%d/%m/%Y")
                    url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={xml_date}"
                else:
                    url = "https://www.cbr.ru/scripts/XML_daily.asp"
                
                logger.debug(f"Запрос к ЦБ РФ XML: {url}")
                
                async with self.session.get(url, timeout=30) as r:
                    r.raise_for_status()
                    xml_content = await r.text()
                
                # Парсим XML
                root = ET.fromstring(xml_content)
                
                # Получаем базовую валюту (обычно RUB для ЦБ РФ)
                # ЦБ РФ предоставляет курсы относительно RUB
                if base_u == "RUB":
                    # Если базовая валюта RUB, ищем целевую валюту
                    for valute in root.findall('Valute'):
                        char_code = valute.find('CharCode').text
                        if char_code == quote_u:
                            value_str = valute.find('Value').text.replace(',', '.')
                            nominal = int(valute.find('Nominal').text)
                            value = float(value_str) / nominal
                            logger.debug(f"ЦБ РФ XML: {base_u}/{quote_u} = {value} (через RUB)")
                            return value
                
                elif quote_u == "RUB":
                    # Если целевая валюта RUB, ищем базовую валюту
                    for valute in root.findall('Valute'):
                        char_code = valute.find('CharCode').text
                        if char_code == base_u:
                            value_str = valute.find('Value').text.replace(',', '.')
                            nominal = int(valute.find('Nominal').text)
                            value = float(value_str) / nominal
                            # Инвертируем курс, так как ЦБ дает курс к RUB
                            inv_value = 1.0 / value if value != 0 else float('inf')
                            logger.debug(f"ЦБ РФ XML: {base_u}/{quote_u} = {inv_value} (через RUB)")
                            return inv_value
                
                # Для пар, где обе валюты не RUB, конвертируем через RUB
                base_to_rub = None
                quote_to_rub = None
                
                for valute in root.findall('Valute'):
                    char_code = valute.find('CharCode').text
                    if char_code == base_u:
                        value_str = valute.find('Value').text.replace(',', '.')
                        nominal = int(valute.find('Nominal').text)
                        base_to_rub = float(value_str) / nominal
                    elif char_code == quote_u:
                        value_str = valute.find('Value').text.replace(',', '.')
                        nominal = int(valute.find('Nominal').text)
                        quote_to_rub = float(value_str) / nominal
                
                if base_to_rub is not None and quote_to_rub is not None:
                    cross_rate = base_to_rub / quote_to_rub
                    logger.debug(f"ЦБ РФ XML: {base_u}/{quote_u} = {cross_rate} (кросс-курс через RUB)")
                    return cross_rate
                
                logger.warning(f"Не удалось найти курсы для {base_u} и/или {quote_u} в данных ЦБ РФ")
                return None
                
            except Exception as e:
                logger.warning(f"Ошибка при парсинге XML ЦБ РФ для {base_u}/{quote_u}: {e}")
                return None

        async def _cbrjson_daily() -> Optional[float]:
            """Альтернативный метод через JSON API (если доступен)"""
            url = f"https://www.cbr-xml-daily.ru/{date}" if date else "https://www.cbr-xml-daily.ru/latest.js"
            params = {"base": base_u, "symbols": quote_u}
            try:
                async with self.session.get(url, params=params, timeout=30) as r:
                    r.raise_for_status()
                    data = await r.json()
                rates = data.get("rates") or {}
                val = rates.get(quote_u)
                result = float(val) if val is not None else None
                if result:
                    logger.debug(f"CBR JSON Daily: {base_u}/{quote_u} = {result}")
                return result
            except Exception as e:
                logger.warning(f"Ошибка CBR JSON Daily для {base_u}/{quote_u}: {e}")
                return None

        async def _frankfurter() -> Optional[float]:
            url = f"https://api.frankfurter.app/{date}" if date else "https://api.frankfurter.app/latest"
            params = {"from": base_u, "to": quote_u}
            try:
                async with self.session.get(url, params=params, timeout=30) as r:
                    r.raise_for_status()
                    data = await r.json()
                rates = data.get("rates") or {}
                val = rates.get(quote_u)
                result = float(val) if val is not None else None
                if result:
                    logger.debug(f"Frankfurter: {base_u}/{quote_u} = {result}")
                return result
            except Exception as e:
                logger.warning(f"Ошибка Frankfurter для {base_u}/{quote_u}: {e}")
                return None

        # Сначала пробуем XML ЦБ РФ
        try:
            val = await _cbr_xml()
            if val is not None:
                logger.info(f"Курс {base_u}/{quote_u} получен от ЦБ РФ XML: {val}")
                return val
        except Exception as e:
            logger.warning(f"Неожиданная ошибка ЦБ РФ XML: {e}")

        # Затем пробуем JSON API ЦБ (если доступен)
        try:
            val = await _cbrjson_daily()
            if val is not None:
                logger.info(f"Курс {base_u}/{quote_u} получен от CBR JSON: {val}")
                return val
        except Exception as e:
            logger.warning(f"Неожиданная ошибка CBR JSON: {e}")

        # Затем Frankfurter
        try:
            val = await _frankfurter()
            if val is not None:
                logger.info(f"Курс {base_u}/{quote_u} получен от Frankfurter: {val}")
                return val
        except Exception as e:
            logger.warning(f"Неожиданная ошибка Frankfurter: {e}")

        logger.error(f"Не удалось получить курс фиата {base_u}/{quote_u} ни от одного источника")
        raise ValueError("Fiat quote not supported")

    async def get_price_and_change(self, base: str, quote: str) -> Tuple[float, Optional[float]]:
        """Возвращает (цена base/quote, 24h pct change)."""
        b, q = base.upper(), quote.upper()
        b_fiat, q_fiat = b in FIAT_CODES, q in FIAT_CODES
        logger.info(f"Получение цены: {b}/{q}, типы: base_fiat={b_fiat}, quote_fiat={q_fiat}")

        # Фиат-фиат
        if b_fiat and q_fiat:
            logger.debug(f"Фиат-фиат пара: {b}/{q}")
            now = await self._fetch_fiat_rate(b, q)
            y = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
            prev = await self._fetch_fiat_rate(b, q, y)
            change = ((now - prev) / prev) * 100 if prev != 0 else None
            logger.info(f"Курс фиата {b}/{q}: {now}, изменение: {change}")
            return now, change

        await self.ensure_coingecko_map()

        # Крипто-фиат
        if not b_fiat and q_fiat:
            logger.debug(f"Крипто-фиат пара: {b}/{q}")
            base_id = self._cg_symbol_to_id.get(b)
            if not base_id:
                logger.error(f"Неизвестный символ криптовалюты: {b}")
                raise ValueError("Unknown crypto symbol")
            return await self._fetch_crypto_simple(base_id, q)

        # Фиат-крипто
        if b_fiat and not q_fiat:
            logger.debug(f"Фиат-крипто пара: {b}/{q}")
            quote_id = self._cg_symbol_to_id.get(q)
            if not quote_id:
                logger.error(f"Неизвестный символ криптовалюты: {q}")
                raise ValueError("Unknown crypto symbol")
            price_q_b, ch = await self._fetch_crypto_simple(quote_id, b)
            inv = 1.0 / price_q_b if price_q_b != 0 else float("inf")
            result_change = -ch if ch is not None else None
            logger.info(f"Инвертированный курс {b}/{q}: {inv}, изменение: {result_change}")
            return inv, result_change

        # Крипто-крипто
        logger.debug(f"Крипто-крипто пара: {b}/{q}")
        base_id = self._cg_symbol_to_id.get(b)
        quote_id = self._cg_symbol_to_id.get(q)
        if not base_id or not quote_id:
            logger.error(f"Неизвестные символы криптовалют: base={b}({base_id}), quote={q}({quote_id})")
            raise ValueError("Unknown crypto symbol(s)")
        
        pb_usd, chb = await self._fetch_crypto_simple(base_id, "USD")
        pq_usd, chq = await self._fetch_crypto_simple(quote_id, "USD")
        price = pb_usd / pq_usd if pq_usd != 0 else float("inf")
        change = (chb - chq) if (chb is not None and chq is not None) else None
        logger.info(f"Кросс-курс крипто {b}/{q}: {price}, изменение: {change}")
        return price, change

OPS = {
    ">":  lambda x, y: x >  y,
    "<":  lambda x, y: x <  y,
    ">=": lambda x, y: x >= y,
    "<=": lambda x, y: x <= y,
}

def infer_asset_type(base: str, quote: str) -> str:
    b_f, q_f = base.upper() in FIAT_CODES, quote.upper() in FIAT_CODES
    if b_f and q_f:
        return "fiat"
    if not b_f and not q_f:
        return "cc"
    return "mixed"

WATCH_RE = re.compile(
    r"^(?P<base>[A-Za-z]{2,10})\s*(?P<op>>=|<=|>|<)\s*(?P<thresh>[0-9]+(?:[\.,][0-9]+)?)\s*(?P<quote>[A-Za-z]{2,10})?$"
)

def parse_watch_args(text: str) -> Optional[Tuple[str, str, float, str]]:
    """
    /watch <BASE> <OP> <THRESHOLD> <QUOTE?>
    Примеры:
      /watch BTC > 30000 USD
      /watch EUR < 95 RUB
      /watch ETH >= 0.06 BTC
    """
    body = re.sub(r"^/watch\s*", "", text, flags=re.I).strip()
    m = WATCH_RE.match(body)
    if not m:
        logger.debug(f"Не удалось разобрать команду watch: {text}")
        return None
    base = m.group("base").upper()
    op = m.group("op")
    thresh_raw = m.group("thresh").replace(",", ".")
    try:
        threshold = float(thresh_raw)
    except ValueError:
        logger.debug(f"Неверный формат порога: {thresh_raw}")
        return None
    quote = (m.group("quote") or ("USD" if base not in FIAT_CODES else "RUB")).upper()
    logger.debug(f"Разобрана команда watch: base={base}, quote={quote}, op={op}, threshold={threshold}")
    return base, quote, threshold, op

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

def make_exchange_keyboard(base: str, quote: str, price_service: PriceService) -> InlineKeyboardMarkup:
    b, q = base.upper(), quote.upper()
    rows: List[List[InlineKeyboardButton]] = []

    # Крипто-биржи
    if not price_service.is_fiat(b) or not price_service.is_fiat(q):
        bq = BINANCE_QUOTE_ALIAS.get(q, q)
        rows.append([
            InlineKeyboardButton(text="Binance",  url=BINANCE_BASE_URL.format(base=b, quote=bq)),
            InlineKeyboardButton(text="Coinbase", url=COINBASE_URL.format(base=b, quote=q)),
            InlineKeyboardButton(text="Kraken",   url=KRAKEN_URL.format(base=b, quote=q)),
        ])

    # Фиат-сервисы
    if price_service.is_fiat(b) or price_service.is_fiat(q):
        rows.append([
            InlineKeyboardButton(text="XE Converter", url=XE_CONVERTER_URL.format(base=b, quote=q)),
            InlineKeyboardButton(text="Wise",         url=WISE_URL.format(base=b.lower(), quote=q.lower())),
        ])

    return InlineKeyboardMarkup(inline_keyboard=rows)

router = Router()

async def init_db():
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(CREATE_USERS_SQL)
            await db.execute(CREATE_SUBS_SQL)
            await db.commit()
        logger.info("База данных инициализирована")
    except Exception as e:
        logger.error(f"Ошибка инициализации БД: {e}")
        raise

async def ensure_user(user_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
            row = await cur.fetchone()
            if not row:
                await db.execute("INSERT INTO users(user_id, is_muted) VALUES(?, 0)", (user_id,))
                await db.commit()
                logger.info(f"Создан новый пользователь: {user_id}")
    except Exception as e:
        logger.error(f"Ошибка при создании пользователя {user_id}: {e}")

@router.message(Command("start"))
async def start_cmd(msg: Message):
    logger.info(f"Команда start от пользователя {msg.from_user.id}")
    await ensure_user(msg.from_user.id)
    text = (
        "👋 Привет! Я CryptoWatchLive.\n\n"
        "Команды:\n"
        "• /watch BTC > 30000 USD — создать подписку\n"
        "• /price BTC USD — текущий курс\n"
        "• /list — мои подписки\n"
        "• /pause <id> /resume <id> — пауза/возобновление\n"
        "• /remove <id> — удалить\n"
        "• /clear — удалить все\n"
        "• /mute /unmute — глобально выкл/вкл уведомления\n\n"
        "Или воспользуйтесь кнопками ниже."
    )
    await msg.answer(text, reply_markup=main_menu_kb())

@router.message(Command("menu"))
async def menu_cmd(msg: Message):
    logger.info(f"Команда menu от пользователя {msg.from_user.id}")
    await msg.answer("Главное меню:", reply_markup=main_menu_kb())

@router.message(Command("watch"))
async def watch_cmd(msg: Message):
    logger.info(f"Команда watch от пользователя {msg.from_user.id}: {msg.text}")
    parsed = parse_watch_args(msg.text or "")
    if not parsed:
        logger.warning(f"Неверный формат команды watch от {msg.from_user.id}: {msg.text}")
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
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO subscriptions(user_id, base, quote, asset_type, operator, threshold, is_active, last_eval, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 1, NULL, ?)",
                (msg.from_user.id, base, quote, asset_type, op, threshold, datetime.utcnow().isoformat()),
            )
            await db.commit()
            cur = await db.execute("SELECT last_insert_rowid()")
            rowid = (await cur.fetchone())[0]
        logger.info(f"Создана подписка #{rowid} для пользователя {msg.from_user.id}: {base}/{quote} {op} {threshold}")
        await msg.answer(
            f"✅ Подписка #{rowid} создана: {base}/{quote} {op} {threshold}.\n"
            f"Буду присылать уведомление при срабатывании условия."
        )
    except Exception as e:
        logger.error(f"Ошибка при создании подписки для {msg.from_user.id}: {e}")
        await msg.answer("❌ Ошибка при создании подписки. Попробуйте позже.")

@router.message(Command("price"))
async def price_cmd(msg: Message):
    logger.info(f"Команда price от пользователя {msg.from_user.id}: {msg.text}")
    parts = (msg.text or "").split()
    if len(parts) < 3:
        logger.warning(f"Неверный формат команды price от {msg.from_user.id}: {msg.text}")
        await msg.answer("Использование: /price <BASE> <QUOTE>\nНапример: /price BTC USD или /price EUR RUB")
        return
    base, quote = parts[1].upper(), parts[2].upper()
    logger.info(f"Запрос цены {base}/{quote} от пользователя {msg.from_user.id}")
    
    async with aiohttp.ClientSession() as session:
        ps = PriceService(session)
        try:
            price, change = await ps.get_price_and_change(base, quote)
            kb = make_exchange_keyboard(base, quote, ps)
            ch_txt = f" ({change:+.2f}% за 24ч)" if change is not None else ""
            response_text = f"Текущий курс {base}/{quote}: {price:.8f}{ch_txt}"
            await msg.answer(response_text, reply_markup=kb)
            logger.info(f"Успешно отправлена цена {base}/{quote} пользователю {msg.from_user.id}")
        except Exception as e:
            logger.error(f"Ошибка при получении цены {base}/{quote} для пользователя {msg.from_user.id}: {e}")
            await msg.answer(f"Не удалось получить цену: {e}")

@router.message(Command("list"))
async def list_cmd(msg: Message):
    logger.info(f"Команда list от пользователя {msg.from_user.id}")
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "SELECT id, base, quote, operator, threshold, is_active FROM subscriptions WHERE user_id=? ORDER BY id",
                (msg.from_user.id,),
            )
            rows = await cur.fetchall()
        if not rows:
            logger.info(f"У пользователя {msg.from_user.id} нет подписок")
            await msg.answer("У вас пока нет подписок. Добавьте: /watch BTC > 30000 USD")
            return
        lines = ["Ваши подписки:"]
        for (sid, base, quote, op, thr, active) in rows:
            status = "⏸️" if not active else "✅"
            lines.append(f"#{sid}: {base}/{quote} {op} {thr} {status}")
        response_text = "\n".join(lines)
        await msg.answer(response_text)
        logger.info(f"Отправлен список подписок пользователю {msg.from_user.id}, всего: {len(rows)}")
    except Exception as e:
        logger.error(f"Ошибка при получении списка подписок для пользователя {msg.from_user.id}: {e}")
        await msg.answer("❌ Ошибка при получении списка подписок.")

def _extract_id_arg(text: str) -> Optional[int]:
    parts = (text or "").split()
    if len(parts) < 2:
        return None
    try:
        return int(parts[1])
    except Exception:
        return None

@router.message(Command("pause"))
async def pause_cmd(msg: Message):
    sid = _extract_id_arg(msg.text)
    logger.info(f"Команда pause от пользователя {msg.from_user.id} для подписки {sid}")
    if sid is None:
        await msg.answer("Укажите id: /pause 3")
        return
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            result = await db.execute("UPDATE subscriptions SET is_active=0 WHERE id=? AND user_id=?", (sid, msg.from_user.id))
            await db.commit()
            if result.rowcount == 0:
                logger.warning(f"Попытка паузы несуществующей подписки #{sid} пользователем {msg.from_user.id}")
                await msg.answer(f"Подписка #{sid} не найдена.")
            else:
                logger.info(f"Подписка #{sid} пользователя {msg.from_user.id} поставлена на паузу")
                await msg.answer(f"Подписка #{sid} поставлена на паузу.")
    except Exception as e:
        logger.error(f"Ошибка при паузе подписки #{sid} пользователя {msg.from_user.id}: {e}")
        await msg.answer("❌ Ошибка при изменении подписки.")

@router.message(Command("resume"))
async def resume_cmd(msg: Message):
    sid = _extract_id_arg(msg.text)
    logger.info(f"Команда resume от пользователя {msg.from_user.id} для подписки {sid}")
    if sid is None:
        await msg.answer("Укажите id: /resume 3")
        return
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            result = await db.execute("UPDATE subscriptions SET is_active=1 WHERE id=? AND user_id=?", (sid, msg.from_user.id))
            await db.commit()
            if result.rowcount == 0:
                logger.warning(f"Попытка возобновления несуществующей подписки #{sid} пользователем {msg.from_user.id}")
                await msg.answer(f"Подписка #{sid} не найдена.")
            else:
                logger.info(f"Подписка #{sid} пользователя {msg.from_user.id} возобновлена")
                await msg.answer(f"Подписка #{sid} возобновлена.")
    except Exception as e:
        logger.error(f"Ошибка при возобновлении подписки #{sid} пользователя {msg.from_user.id}: {e}")
        await msg.answer("❌ Ошибка при изменении подписки.")

@router.message(Command("remove"))
async def remove_cmd(msg: Message):
    sid = _extract_id_arg(msg.text)
    logger.info(f"Команда remove от пользователя {msg.from_user.id} для подписки {sid}")
    if sid is None:
        await msg.answer("Укажите id: /remove 3")
        return
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            result = await db.execute("DELETE FROM subscriptions WHERE id=? AND user_id=?", (sid, msg.from_user.id))
            await db.commit()
            if result.rowcount == 0:
                logger.warning(f"Попытка удаления несуществующей подписки #{sid} пользователем {msg.from_user.id}")
                await msg.answer(f"Подписка #{sid} не найдена.")
            else:
                logger.info(f"Подписка #{sid} пользователя {msg.from_user.id} удалена")
                await msg.answer(f"Подписка #{sid} удалена.")
    except Exception as e:
        logger.error(f"Ошибка при удалении подписки #{sid} пользователя {msg.from_user.id}: {e}")
        await msg.answer("❌ Ошибка при удалении подписки.")

@router.message(Command("clear"))
async def clear_cmd(msg: Message):
    logger.info(f"Команда clear от пользователя {msg.from_user.id}")
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            result = await db.execute("DELETE FROM subscriptions WHERE user_id=?", (msg.from_user.id,))
            await db.commit()
            count = result.rowcount
        logger.info(f"Удалены все подписки пользователя {msg.from_user.id}, всего: {count}")
        await msg.answer("Все ваши подписки удалены.")
    except Exception as e:
        logger.error(f"Ошибка при очистке подписок пользователя {msg.from_user.id}: {e}")
        await msg.answer("❌ Ошибка при удалении подписок.")

@router.message(Command("mute"))
async def mute_cmd(msg: Message):
    logger.info(f"Команда mute от пользователя {msg.from_user.id}")
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET is_muted=1 WHERE user_id=?", (msg.from_user.id,))
            await db.commit()
        logger.info(f"Пользователь {msg.from_user.id} отключил уведомления")
        await msg.answer("🔕 Уведомления отключены. /unmute — включить")
    except Exception as e:
        logger.error(f"Ошибка при отключении уведомлений пользователя {msg.from_user.id}: {e}")
        await msg.answer("❌ Ошибка при отключении уведомлений.")

@router.message(Command("unmute"))
async def unmute_cmd(msg: Message):
    logger.info(f"Команда unmute от пользователя {msg.from_user.id}")
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET is_muted=0 WHERE user_id=?", (msg.from_user.id,))
            await db.commit()
        logger.info(f"Пользователь {msg.from_user.id} включил уведомления")
        await msg.answer("🔔 Уведомления включены.")
    except Exception as e:
        logger.error(f"Ошибка при включении уведомлений пользователя {msg.from_user.id}: {e}")
        await msg.answer("❌ Ошибка при включении уведомлений.")

@router.message(F.text == "🗂️ Мои подписки")
async def btn_list(msg: Message):
    logger.info(f"Кнопка 'Мои подписки' от пользователя {msg.from_user.id}")
    await list_cmd(msg)

@router.message(F.text == "➕ Подписка")
async def btn_subscribe(msg: Message):
    logger.info(f"Кнопка 'Подписка' от пользователя {msg.from_user.id}")
    await msg.answer(
        "Создать подписку командой:\n"
        "• /watch BTC > 30000 USD\n"
        "• /watch EUR < 95 RUB\n"
        "• /watch ETH >= 0.06 BTC"
    )

@router.message(F.text == "📈 Цена")
async def btn_price(msg: Message):
    logger.info(f"Кнопка 'Цена' от пользователя {msg.from_user.id}")
    await msg.answer(
        "Запросите так: /price <BASE> <QUOTE>\n"
        "Например: /price BTC USD"
    )

@router.message(F.text == "🔕 Mute")
async def btn_mute(msg: Message):
    logger.info(f"Кнопка 'Mute' от пользователя {msg.from_user.id}")
    await mute_cmd(msg)

@router.message(F.text == "🔔 Unmute")
async def btn_unmute(msg: Message):
    logger.info(f"Кнопка 'Unmute' от пользователя {msg.from_user.id}")
    await unmute_cmd(msg)

async def price_watcher(bot: Bot):
    logger.info("Запуск монитора цен...")
    await asyncio.sleep(2)  
    async with aiohttp.ClientSession() as session:
        ps = PriceService(session)
        while True:
            try:
                logger.debug("Начало цикла проверки подписок")
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute(READ_SUBS_SQL) as cur:
                        rows = await cur.fetchall()
                subs = [Subscription(*row) for row in rows]
                
                if not subs:
                    logger.debug("Нет активных подписок для проверки")
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

                logger.info(f"Проверка {len(subs)} активных подписок")
                notifications_sent = 0

                for s in subs:
                    try:
                        price, change = await ps.get_price_and_change(s.base, s.quote)
                        ok = OPS[s.operator](price, s.threshold)
                        logger.debug(f"Подписка #{s.id}: {s.base}/{s.quote} = {price}, условие {s.operator} {s.threshold} = {ok}")
                    except Exception as e:
                        logger.warning(f"Ошибка при проверке подписки #{s.id} ({s.base}/{s.quote}): {e}")
                        continue

                    # Проверяем muted статус
                    async with aiosqlite.connect(DB_PATH) as db:
                        cur = await db.execute("SELECT is_muted FROM users WHERE user_id=?", (s.user_id,))
                        row = await cur.fetchone()
                        muted = bool(row and row[0])

                    should_notify = (not muted) and ok and (s.last_eval in (None, 0))
                    
                    # Обновляем last_eval
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
                            notifications_sent += 1
                            logger.info(f"Отправлено уведомление пользователю {s.user_id} для подписки #{s.id}")
                        except Exception as e:
                            logger.error(f"Ошибка при отправке уведомления пользователю {s.user_id}: {e}")

                if notifications_sent > 0:
                    logger.info(f"Отправлено {notifications_sent} уведомлений в этом цикле")
                
                logger.debug(f"Завершение цикла проверки, ожидание {POLL_INTERVAL_SECONDS} секунд")
                await asyncio.sleep(POLL_INTERVAL_SECONDS)

            except Exception as e:
                logger.error(f"Критическая ошибка в мониторе цен: {e}")
                await asyncio.sleep(POLL_INTERVAL_SECONDS)

async def main():
    logger.info("Запуск CryptoWatchLive бота...")
    
    if not BOT_TOKEN:
        logger.critical("Не задан BOT_TOKEN (переменная окружения или константа в коде)")
        raise RuntimeError("Не задан BOT_TOKEN (переменная окружения или константа в коде)")

    # Инициализация БД
    await init_db()

    # Создание бота и диспетчера
    bot = Bot(BOT_TOKEN, parse_mode=None)
    dp = Dispatcher()
    dp.include_router(router)

    # Установка команд бота
    try:
        await bot.set_my_commands([
            BotCommand(command="start",  description="Приветствие и меню"),
            BotCommand(command="menu",   description="Показать главное меню"),
            BotCommand(command="watch",  description="Создать подписку"),
            BotCommand(command="price",  description="Текущий курс"),
            BotCommand(command="list",   description="Мои подписки"),
            BotCommand(command="pause",  description="Пауза подписки"),
            BotCommand(command="resume", description="Возобновить подписку"),
            BotCommand(command="remove", description="Удалить подписку"),
            BotCommand(command="clear",  description="Удалить все подписки"),
            BotCommand(command="mute",   description="Отключить уведомления"),
            BotCommand(command="unmute", description="Включить уведомления"),
        ])
        logger.info("Команды бота установлены")
    except Exception as e:
        logger.error(f"Ошибка при установке команд бота: {e}")

    # Запуск монитора цен
    asyncio.get_event_loop().create_task(price_watcher(bot))
    logger.info("Монитор цен запущен")

    logger.info("CryptoWatchLive запущен. Нажмите Ctrl+C для остановки.")
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.critical(f"Критическая ошибка при запуске бота: {e}")
        raise
    finally:
        logger.info("CryptoWatchLive остановлен.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("CryptoWatchLive остановлен по сигналу пользователя.")
    except Exception as e:
        logger.critical(f"Неожиданная ошибка: {e}")