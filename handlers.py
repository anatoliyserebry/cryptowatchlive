from datetime import datetime
from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery,
)
from aiogram.filters import StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from config import logger, BINANCE_BASE_URL, COINBASE_URL, KRAKEN_URL, XE_CONVERTER_URL, WISE_URL, PROFEE_URL, BINANCE_QUOTE_ALIAS
from repository import UserRepository, SubscriptionRepository
from service import PriceService, parse_watch_args, infer_asset_type

router = Router()

class ListStates(StatesGroup):
    viewing_list = State()

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
    rows = []

    if not price_service.is_fiat(b) or not price_service.is_fiat(q):
        bq = BINANCE_QUOTE_ALIAS.get(q, q)
        rows.append([
            InlineKeyboardButton(text="Binance",  url=BINANCE_BASE_URL.format(base=b, quote=bq)),
            InlineKeyboardButton(text="Coinbase", url=COINBASE_URL.format(base=b, quote=q)),
            InlineKeyboardButton(text="Kraken",   url=KRAKEN_URL.format(base=b, quote=q)),
        ])

    converter_buttons = []
    converter_buttons.append(InlineKeyboardButton(text="XE Converter", url=XE_CONVERTER_URL.format(base=b, quote=q)))
    converter_buttons.append(InlineKeyboardButton(text="Wise", url=WISE_URL.format(base=b.lower(), quote=q.lower())))
    converter_buttons.append(InlineKeyboardButton(text="Profee", url=PROFEE_URL))
    
    for i in range(0, len(converter_buttons), 2):
        rows.append(converter_buttons[i:i+2])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def create_pagination_keyboard(page: int, total_pages: int, user_id: int) -> InlineKeyboardMarkup:
    """Создает клавиатуру пагинации для списка подписок"""
    buttons = []
    
    # Navigation Buttons 
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"list_{user_id}_{page-1}"))
    
    nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="current_page"))
    
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"list_{user_id}_{page+1}"))
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    # Close button 
    buttons.append([InlineKeyboardButton(text="❌ Закрыть", callback_data="close_list")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def send_subscriptions_page(msg: Message, user_id: int, page: int = 0, page_size: int = 5):
    """Отправляет страницу со списком подписок"""
    try:
        rows = await SubscriptionRepository.get_user_subscriptions(user_id)
        if not rows:
            await msg.answer("У вас пока нет подписок. Добавьте: /watch BTC > 30000 USD")
            return

        total_subscriptions = len(rows)
        total_pages = (total_subscriptions + page_size - 1) // page_size
        
        if page >= total_pages:
            page = total_pages - 1
        
        start_idx = page * page_size
        end_idx = min(start_idx + page_size, total_subscriptions)
        
        lines = [f"📋 Ваши подписки (стр. {page+1}/{total_pages}):"]
        
        for i in range(start_idx, end_idx):
            sid, base, quote, op, thr, active = rows[i]
            status = "⏸️" if not active else "✅"
            lines.append(f"#{sid}: {base}/{quote} {op} {thr} {status}")
        
        # Add Stats 
        lines.append(f"\n📊 Всего подписок: {total_subscriptions}")
        active_count = sum(1 for _, _, _, _, _, active in rows if active)
        lines.append(f"✅ Активных: {active_count}")
        lines.append(f"⏸️ На паузе: {total_subscriptions - active_count}")
        
        response_text = "\n".join(lines)
        kb = create_pagination_keyboard(page, total_pages, user_id)
        
        # If it's a callback, update page 
        if isinstance(msg, CallbackQuery):
            await msg.message.edit_text(response_text, reply_markup=kb)
        else:
            await msg.answer(response_text, reply_markup=kb)
            
        logger.info(f"Отправлена страница {page+1}/{total_pages} списка подписок пользователю {user_id}")
        
    except Exception as e:
        logger.error(f"Ошибка при отправке страницы подписок пользователю {user_id}: {e}")
        error_msg = "❌ Ошибка при получении списка подписок."
        if isinstance(msg, CallbackQuery):
            await msg.message.edit_text(error_msg)
        else:
            await msg.answer(error_msg)

@router.message(Command("start"))
async def start_cmd(msg: Message):
    logger.info(f"Команда start от пользователя {msg.from_user.id}")
    await UserRepository.ensure_user(msg.from_user.id)
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
        subscription_id = await SubscriptionRepository.create_subscription(
            msg.from_user.id, base, quote, asset_type, op, threshold, datetime.utcnow().isoformat()
        )
        await msg.answer(
            f"✅ Подписка #{subscription_id} создана: {base}/{quote} {op} {threshold}.\n"
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
    
    import aiohttp
    async with aiohttp.ClientSession() as session:
        ps = PriceService(session)
        try:
            price, change = await ps.get_price_and_change(base, quote)
            kb = make_exchange_keyboard(base, quote, ps)
            ch_txt = f" ({change:+.2f}% за 24ч)" if change is not None else ""
            response_text = f"Текущий курс {base}/{quote}: {price:.8f}{ch_txt}"
            await msg.answer(response_text, reply_markup=kb)
            logger.info(f"Успешно отправлена цена {base}/{quote} пользователю {msg.from_user.id} с клавиатурой конвертеров")
        except Exception as e:
            logger.error(f"Ошибка при получении цены {base}/{quote} для пользователя {msg.from_user.id}: {e}")
            await msg.answer(f"Не удалось получить цену: {e}")

@router.message(Command("list"))
async def list_cmd(msg: Message):
    logger.info(f"Команда list от пользователя {msg.from_user.id}")
    await send_subscriptions_page(msg, msg.from_user.id)

@router.callback_query(F.data.startswith("list_"))
async def handle_list_pagination(callback: CallbackQuery):
    """Обработка пагинации списка подписок"""
    try:
        # data format: "list_{user_id}_{page}"
        parts = callback.data.split("_")
        if len(parts) >= 3:
            user_id = int(parts[1])
            page = int(parts[2])
            
            # Verification if the user have acces to the list of subs  
            if callback.from_user.id == user_id:
                await send_subscriptions_page(callback, user_id, page)
            else:
                await callback.answer("❌ Доступ запрещен", show_alert=True)
        else:
            await callback.answer("❌ Ошибка пагинации", show_alert=True)
    except Exception as e:
        logger.error(f"Ошибка при обработке пагинации: {e}")
        await callback.answer("❌ Ошибка при загрузке страницы", show_alert=True)

@router.callback_query(F.data == "close_list")
async def handle_close_list(callback: CallbackQuery):
    """Закрытие списка подписок"""
    try:
        await callback.message.delete()
        await callback.answer("Список закрыт")
    except Exception as e:
        logger.error(f"Ошибка при закрытии списка: {e}")
        await callback.answer("❌ Ошибка при закрытии", show_alert=True)

@router.callback_query(F.data == "current_page")
async def handle_current_page(callback: CallbackQuery):
    """Обработка нажатия на кнопку текущей страницы"""
    await callback.answer(f"Вы на странице {callback.message.text.split('стр. ')[1].split(')')[0]}")

def _extract_id_arg(text: str) -> int:
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
        success = await SubscriptionRepository.update_subscription_status(sid, msg.from_user.id, False)
        if not success:
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
        success = await SubscriptionRepository.update_subscription_status(sid, msg.from_user.id, True)
        if not success:
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
        success = await SubscriptionRepository.delete_subscription(sid, msg.from_user.id)
        if not success:
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
        count = await SubscriptionRepository.delete_all_user_subscriptions(msg.from_user.id)
        logger.info(f"Удалены все подписки пользователя {msg.from_user.id}, всего: {count}")
        await msg.answer("Все ваши подписки удалены.")
    except Exception as e:
        logger.error(f"Ошибка при очистке подписок пользователя {msg.from_user.id}: {e}")
        await msg.answer("❌ Ошибка при удалении подписок.")

@router.message(Command("mute"))
async def mute_cmd(msg: Message):
    logger.info(f"Команда mute от пользователя {msg.from_user.id}")
    try:
        await UserRepository.update_user_muted_status(msg.from_user.id, True)
        await msg.answer("🔕 Уведомления отключены. /unmute — включить")
    except Exception as e:
        logger.error(f"Ошибка при отключении уведомлений пользователя {msg.from_user.id}: {e}")
        await msg.answer("❌ Ошибка при отключении уведомлений.")

@router.message(Command("unmute"))
async def unmute_cmd(msg: Message):
    logger.info(f"Команда unmute от пользователя {msg.from_user.id}")
    try:
        await UserRepository.update_user_muted_status(msg.from_user.id, False)
        await msg.answer("🔔 Уведомления включены.")
    except Exception as e:
        logger.error(f"Ошибка при включении уведомлений пользователя {msg.from_user.id}: {e}")
        await msg.answer("❌ Ошибка при включении уведомлений.")

@router.message(F.text == "🗂️ Мои подписки")
async def btn_list(msg: Message):
    logger.info(f"Кнопка 'Мои подписки' от пользователя {msg.from_user.id}")
    await send_subscriptions_page(msg, msg.from_user.id)

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