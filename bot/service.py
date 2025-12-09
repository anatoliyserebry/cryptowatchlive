import re
import xml.etree.ElementTree as ET
import aiohttp
import json
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
        logger.info("PriceService инициализирован")

    async def _get_yahoo_finance_price(self, base: str, quote: str) -> Tuple[float, Optional[float]]:
        """Получает цену через Yahoo Finance API (работает для BTC-USD, ETH-USD и других основных пар)"""
        try:
            # Формирование символа для Yahoo Finance 
            symbol = f"{base}-{quote}"
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            
            async with self.session.get(url, timeout=30) as r:
                if r.status == 200:
                    data = await r.json()
                    
                    result = data.get('chart', {}).get('result', [{}])[0]
                    meta = result.get('meta', {})
                    
                    price = meta.get('regularMarketPrice', 0)
                    previous_close = meta.get('previousClose', 0)
                    
                    if price and previous_close:
                        change_percent = ((price - previous_close) / previous_close) * 100
                    else:
                        change_percent = None
                    
                    logger.debug(f"Yahoo Finance: {base}/{quote} = {price}, изменение: {change_percent}%")
                    return float(price), change_percent
                else:
                    raise ValueError(f"Пара {symbol} не найдена на Yahoo Finance")
                    
        except Exception as e:
            logger.debug(f"Yahoo Finance API не сработал для {base}/{quote}: {e}")
            raise

    async def _get_binance_public_price(self, base: str, quote: str) -> Tuple[float, Optional[float]]:
        """Получает цену с Binance Public API (работает без ключа)"""
        try:
            # For Binance use USDT instead of USD 
            binance_quote = "USDT" if quote.upper() == "USD" else quote.upper()
            symbol = f"{base.upper()}{binance_quote.upper()}"
            
            # trying to optain price
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            
            async with self.session.get(url, timeout=30) as r:
                if r.status == 200:
                    price_data = await r.json()
                    price = float(price_data['price'])
                    
                    # 24h Stats 
                    stats_url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
                    async with self.session.get(stats_url, timeout=30) as r2:
                        if r2.status == 200:
                            stats_data = await r2.json()
                            price_change = float(stats_data.get('priceChangePercent', 0))
                        else:
                            price_change = None
                    
                    logger.debug(f"Binance: {base}/{quote} = {price}, изменение: {price_change}%")
                    return price, price_change
                else:
                    raise ValueError(f"Пара {symbol} не найдена на Binance")
                    
        except Exception as e:
            logger.debug(f"Binance API не сработал для {base}/{quote}: {e}")
            raise

    async def _get_coingecko_simple_price(self, base: str, quote: str) -> Tuple[float, Optional[float]]:
        """Получает цену с CoinGecko Simple API (без ключа)"""
        try:
            # Получаем ID монеты
            coin_id = base.lower()
            
            url = f"https://api.coingecko.com/api/v3/simple/price"
            params = {
                'ids': coin_id,
                'vs_currencies': quote.lower(),
                'include_24hr_change': 'true'
            }
            
            async with self.session.get(url, params=params, timeout=30) as r:
                if r.status == 200:
                    data = await r.json()
                    
                    if coin_id in data:
                        coin_data = data[coin_id]
                        price = coin_data.get(quote.lower(), 0)
                        change_key = f"{quote.lower()}_24h_change"
                        change = coin_data.get(change_key)
                        
                        logger.debug(f"CoinGecko: {base}/{quote} = {price}, изменение: {change}%")
                        return float(price), float(change) if change is not None else None
                    else:
                        raise ValueError(f"Монета {base} не найдена на CoinGecko")
                else:
                    raise ValueError(f"Ошибка CoinGecko API: {r.status}")
                    
        except Exception as e:
            logger.debug(f"CoinGecko API не сработал для {base}/{quote}: {e}")
            raise

    async def _get_mexc_price(self, base: str, quote: str) -> Tuple[float, Optional[float]]:
        """Получает цену с MEXC API"""
        try:
            symbol = f"{base.upper()}{quote.upper()}"
            url = f"https://api.mexc.com/api/v3/ticker/price?symbol={symbol}"
            
            async with self.session.get(url, timeout=30) as r:
                if r.status == 200:
                    data = await r.json()
                    price = float(data['price'])
                    
                    # 24h update changes 
                    stats_url = f"https://api.mexc.com/api/v3/ticker/24hr?symbol={symbol}"
                    async with self.session.get(stats_url, timeout=30) as r2:
                        if r2.status == 200:
                            stats_data = await r2.json()
                            price_change = float(stats_data.get('priceChangePercent', 0))
                        else:
                            price_change = None
                    
                    logger.debug(f"MEXC: {base}/{quote} = {price}, изменение: {price_change}%")
                    return price, price_change
                else:
                    raise ValueError(f"Пара {symbol} не найдена на MEXC")
                    
        except Exception as e:
            logger.debug(f"MEXC API не сработал для {base}/{quote}: {e}")
            raise

    def is_fiat(self, code: str) -> bool:
        return code.upper() in FIAT_CODES

    async def _fetch_fiat_rate(self, base: str, quote: str, date: Optional[str] = None) -> float:
        """Курс фиат->фиат"""
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
        """Возвращает (цена base/quote, 24h pct change)."""
        b, q = base.upper(), quote.upper()
        b_fiat, q_fiat = self.is_fiat(b), self.is_fiat(q)
        logger.info(f"Получение цены: {b}/{q}, типы: base_fiat={b_fiat}, quote_fiat={q_fiat}")

        # Fiat to fiat 
        if b_fiat and q_fiat:
            logger.debug(f"Фиат-фиат пара: {b}/{q}")
            now = await self._fetch_fiat_rate(b, q)
            y = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
            prev = await self._fetch_fiat_rate(b, q, y)
            change = ((now - prev) / prev) * 100 if prev != 0 else None
            logger.info(f"Курс фиата {b}/{q}: {now}, изменение: {change}")
            return now, change

        # crypto to fiat (например, BTC/USD)
        if not b_fiat and q_fiat:
            logger.debug(f"Крипто-фиат пара: {b}/{q}")
            
            # Trying different API 
            apis_to_try = [
                self._get_binance_public_price,  # Binance with USDT for USD 
                self._get_coingecko_simple_price,  # CoinGecko
                self._get_yahoo_finance_price,    # Yahoo Finance
                self._get_mexc_price              # MEXC
            ]
            
            last_error = None
            for api_func in apis_to_try:
                try:
                    result = await api_func(b, q)
                    logger.info(f"Успешно получена цена {b}/{q} через {api_func.__name__}: {result[0]}")
                    return result
                except Exception as e:
                    last_error = e
                    logger.debug(f"API {api_func.__name__} не сработал для {b}/{q}: {e}")
                    continue
            
            logger.error(f"Все API не сработали для {b}/{q}. Последняя ошибка: {last_error}")
            raise ValueError(f"Не удалось получить цену для пары {b}/{q}. Попробуйте позже.")

        # Fiat to Crypto (например, USD/BTC) - инвертируем пару
        if b_fiat and not q_fiat:
            logger.debug(f"Фиат-крипто пара: {b}/{q}")
            try:
                # Give pirce if = QUOTE/BASE (крипто/фиат) and invert
                quote_to_base, quote_change = await self.get_price_and_change(q, b)
                inverted_price = 1.0 / quote_to_base if quote_to_base != 0 else float("inf")
                logger.info(f"Инвертированный курс {b}/{q}: {inverted_price}, изменение: {-quote_change if quote_change else None}%")
                return inverted_price, -quote_change if quote_change else None
            except Exception as e:
                logger.error(f"Ошибка при получении инвертированного курса {b}/{q}: {e}")
                raise

        # Crypto to Crypto (например, BTC/ETH)
        logger.debug(f"Крипто-крипто пара: {b}/{q}")
        
        # Diffrents API 
        apis_to_try = [
            self._get_binance_public_price,
            self._get_coingecko_simple_price,
            self._get_mexc_price
        ]
        
        last_error = None
        for api_func in apis_to_try:
            try:
                result = await api_func(b, q)
                logger.info(f"Успешно получена цена {b}/{q} через {api_func.__name__}: {result[0]}")
                return result
            except Exception as e:
                last_error = e
                logger.debug(f"API {api_func.__name__} не сработал для {b}/{q}: {e}")
                continue
        
        # Если прямые пары не найдены, try with USD 
        logger.debug(f"Прямые пары не найдены, пробуем через USD для {b}/{q}")
        try:
            b_to_usd, b_change = await self.get_price_and_change(b, "USD")
            q_to_usd, q_change = await self.get_price_and_change(q, "USD")
            
            if q_to_usd == 0:
                raise ValueError("Деление на ноль")
                
            cross_price = b_to_usd / q_to_usd
            
            
            cross_change = (b_change - q_change) if (b_change is not None and q_change is not None) else None
            
            logger.info(f"Кросс-курс крипто {b}/{q}: {cross_price}, изменение: {cross_change}%")
            return cross_price, cross_change
            
        except Exception as usd_error:
            logger.error(f"Не удалось получить кросс-курс для {b}/{q} даже через USD: {usd_error}")
            raise ValueError(f"Не удалось получить цену для пары {b}/{q}")

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