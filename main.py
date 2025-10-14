import asyncio
import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand

from config import logger, BOT_TOKEN, POLL_INTERVAL_SECONDS
from repository import UserRepository, SubscriptionRepository
from service import PriceService, SubscriptionService
from handlers import router

async def price_watcher(bot: Bot):
    logger.info("Запуск монитора цен...")
    await asyncio.sleep(2)  
    async with aiohttp.ClientSession() as session:
        price_service = PriceService(session)
        subscription_service = SubscriptionService(price_service)
        
        while True:
            try:
                logger.debug("Начало цикла проверки подписок")
                subs = await SubscriptionRepository.get_active_subscriptions()
                
                if not subs:
                    logger.debug("Нет активных подписок для проверки")
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

                logger.info(f"Проверка {len(subs)} активных подписок")
                notifications_sent = 0

                for subscription in subs:
                    try:
                        condition_met, price, change = await subscription_service.check_subscription_condition(subscription)
                        logger.debug(f"Подписка #{subscription.id}: {subscription.base}/{subscription.quote} = {price}, условие {subscription.operator} {subscription.threshold} = {condition_met}")
                    except Exception as e:
                        logger.warning(f"Ошибка при проверке подписки #{subscription.id} ({subscription.base}/{subscription.quote}): {e}")
                        continue

                    muted = await UserRepository.get_user_muted_status(subscription.user_id)
                    should_notify = (not muted) and condition_met and (subscription.last_eval in (None, 0))
                    
                    await SubscriptionRepository.update_subscription_eval(subscription.id, condition_met)

                    if should_notify:
                        from handlers import make_exchange_keyboard
                        ch_txt = f" ({change:+.2f}% за 24ч)" if change is not None else ""
                        text = (
                            f"⚡ Условие выполнено: #{subscription.id}\n"
                            f"{subscription.base}/{subscription.quote} {subscription.operator} {subscription.threshold}\n"
                            f"Текущий курс: {price:.8f}{ch_txt}"
                        )
                        kb = make_exchange_keyboard(subscription.base, subscription.quote, price_service)
                        try:
                            await bot.send_message(chat_id=subscription.user_id, text=text, reply_markup=kb)
                            notifications_sent += 1
                            logger.info(f"Отправлено уведомление пользователю {subscription.user_id} для подписки #{subscription.id}")
                        except Exception as e:
                            logger.error(f"Ошибка при отправке уведомления пользователю {subscription.user_id}: {e}")

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
        logger.critical("Не задан BOT_TOKEN")
        raise RuntimeError("Не задан BOT_TOKEN")

    await UserRepository.init_db()

    bot = Bot(BOT_TOKEN, parse_mode=None)
    dp = Dispatcher()
    dp.include_router(router)

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

    asyncio.create_task(price_watcher(bot))
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