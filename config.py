import os
import logging

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

# Константы для пагинации
SUBSCRIPTIONS_PER_PAGE = 5  # Количество подписок на одной странице

FIAT_CODES = {
    "USD", "EUR", "RUB", "UAH", "KZT", "GBP", "JPY", "CNY", "TRY", "CHF",
    "PLN", "CZK", "SEK", "NOK", "DKK", "AUD", "CAD", "INR", "BRL", "ZAR",
}

BINANCE_BASE_URL = "https://www.binance.com/en/trade/{base}_{quote}?type=spot"
COINBASE_URL     = "https://www.coinbase.com/advanced-trade/{base}-{quote}"
KRAKEN_URL       = "https://pro.kraken.com/app/trade/{base}-{quote}"
XE_CONVERTER_URL = "https://www.xe.com/currencyconverter/convert/?Amount=1&From={base}&To={quote}"
WISE_URL         = "https://wise.com/transfer/{base}-to-{quote}"
PROFEE_URL       = "https://profee.com/en"
COINPAPRIKA_BASE_URL = "https://coinpaprika.com/coin/"
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
    asset_type TEXT NOT NULL,
    operator TEXT NOT NULL,
    threshold REAL NOT NULL,
    is_active INTEGER DEFAULT 1,
    last_eval INTEGER,
    created_at TEXT NOT NULL
);
"""

READ_SUBS_SQL = """
SELECT id, user_id, base, quote, asset_type, operator, threshold, is_active, last_eval
FROM subscriptions
WHERE is_active = 1;
"""

OPS = {
    ">":  lambda x, y: x >  y,
    "<":  lambda x, y: x <  y,
    ">=": lambda x, y: x >= y,
    "<=": lambda x, y: x <= y,
}