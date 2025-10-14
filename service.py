import re
import xml.etree.ElementTree as ET
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional, List

from config import logger, FIAT_CODES, OPS
from models import Subscription

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

class PriceService:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self._cg_symbol_to_id: Dict[str, str] = {}
        self._cg_ready = False
        logger.info("PriceService инициализирован")

    async def ensure_coingecko_map(self):
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
        base_u, quote_u = base.upper(), quote.upper()
        logger.debug(f"Запрос курса фиата: {base_u}/{quote_u}, дата: {date}")

        async def _cbr_xml() -> Optional[float]:
            try:
                if date:
                    date_obj = datetime.strptime(date, "%Y-%m-%d")
                    xml_date = date_obj.strftime("%d/%m/%Y")
                    url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={xml_date}"
                else:
                    url = "https://www.cbr.ru/scripts/XML_daily.asp"
                
                logger.debug(f"Запрос к ЦБ РФ XML: {url}")
                
                async with self.session.get(url, timeout=30) as r:
                    r.raise_for_status()
                    xml_content = await r.text()
                
                root = ET.fromstring(xml_content)
                
                if base_u == "RUB":
                    for valute in root.findall('Valute'):
                        char_code = valute.find('CharCode').text
                        if char_code == quote_u:
                            value_str = valute.find('Value').text.replace(',', '.')
                            nominal = int(valute.find('Nominal').text)
                            value = float(value_str) / nominal
                            logger.debug(f"ЦБ РФ XML: {base_u}/{quote_u} = {value} (через RUB)")
                            return value
                
                elif quote_u == "RUB":
                    for valute in root.findall('Valute'):
                        char_code = valute.find('CharCode').text
                        if char_code == base_u:
                            value_str = valute.find('Value').text.replace(',', '.')
                            nominal = int(valute.find('Nominal').text)
                            value = float(value_str) / nominal
                            inv_value = 1.0 / value if value != 0 else float('inf')
                            logger.debug(f"ЦБ РФ XML: {base_u}/{quote_u} = {inv_value} (через RUB)")
                            return inv_value
                
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

        try:
            val = await _cbr_xml()
            if val is not None:
                logger.info(f"Курс {base_u}/{quote_u} получен от ЦБ РФ XML: {val}")
                return val
        except Exception as e:
            logger.warning(f"Неожиданная ошибка ЦБ РФ XML: {e}")

        try:
            val = await _cbrjson_daily()
            if val is not None:
                logger.info(f"Курс {base_u}/{quote_u} получен от CBR JSON: {val}")
                return val
        except Exception as e:
            logger.warning(f"Неожиданная ошибка CBR JSON: {e}")

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
        b, q = base.upper(), quote.upper()
        b_fiat, q_fiat = b in FIAT_CODES, q in FIAT_CODES
        logger.info(f"Получение цены: {b}/{q}, типы: base_fiat={b_fiat}, quote_fiat={q_fiat}")

        if b_fiat and q_fiat:
            logger.debug(f"Фиат-фиат пара: {b}/{q}")
            now = await self._fetch_fiat_rate(b, q)
            y = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
            prev = await self._fetch_fiat_rate(b, q, y)
            change = ((now - prev) / prev) * 100 if prev != 0 else None
            logger.info(f"Курс фиата {b}/{q}: {now}, изменение: {change}")
            return now, change

        await self.ensure_coingecko_map()

        if not b_fiat and q_fiat:
            logger.debug(f"Крипто-фиат пара: {b}/{q}")
            base_id = self._cg_symbol_to_id.get(b)
            if not base_id:
                logger.error(f"Неизвестный символ криптовалюты: {b}")
                raise ValueError("Unknown crypto symbol")
            return await self._fetch_crypto_simple(base_id, q)

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

class SubscriptionService:
    def __init__(self, price_service: PriceService):
        self.price_service = price_service

    async def check_subscription_condition(self, subscription: Subscription) -> Tuple[bool, float, Optional[float]]:
        try:
            price, change = await self.price_service.get_price_and_change(subscription.base, subscription.quote)
            condition_met = OPS[subscription.operator](price, subscription.threshold)
            return condition_met, price, change
        except Exception as e:
            logger.warning(f"Ошибка при проверке подписки #{subscription.id}: {e}")
            raise