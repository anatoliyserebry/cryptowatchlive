import aiosqlite
from config import logger, DB_PATH, CREATE_USERS_SQL, CREATE_SUBS_SQL, READ_SUBS_SQL
from models import Subscription

class UserRepository:
    @staticmethod
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

    @staticmethod
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

    @staticmethod
    async def get_user_muted_status(user_id: int) -> bool:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute("SELECT is_muted FROM users WHERE user_id=?", (user_id,))
                row = await cur.fetchone()
                return bool(row and row[0])
        except Exception as e:
            logger.error(f"Ошибка при получении статуса пользователя {user_id}: {e}")
            return False

    @staticmethod
    async def update_user_muted_status(user_id: int, is_muted: bool):
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE users SET is_muted=? WHERE user_id=?", (1 if is_muted else 0, user_id))
                await db.commit()
            logger.info(f"Пользователь {user_id} {'отключил' if is_muted else 'включил'} уведомления")
        except Exception as e:
            logger.error(f"Ошибка при обновлении статуса пользователя {user_id}: {e}")
            raise

class SubscriptionRepository:
    @staticmethod
    async def create_subscription(user_id: int, base: str, quote: str, asset_type: str, 
                                operator: str, threshold: float, created_at: str) -> int:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO subscriptions(user_id, base, quote, asset_type, operator, threshold, is_active, last_eval, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, 1, NULL, ?)",
                    (user_id, base, quote, asset_type, operator, threshold, created_at),
                )
                await db.commit()
                cur = await db.execute("SELECT last_insert_rowid()")
                rowid = (await cur.fetchone())[0]
            logger.info(f"Создана подписка #{rowid} для пользователя {user_id}: {base}/{quote} {operator} {threshold}")
            return rowid
        except Exception as e:
            logger.error(f"Ошибка при создании подписки для {user_id}: {e}")
            raise

    @staticmethod
    async def get_user_subscriptions(user_id: int):
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute(
                    "SELECT id, base, quote, operator, threshold, is_active FROM subscriptions WHERE user_id=? ORDER BY id",
                    (user_id,),
                )
                rows = await cur.fetchall()
            return rows
        except Exception as e:
            logger.error(f"Ошибка при получении подписок пользователя {user_id}: {e}")
            raise

    @staticmethod
    async def get_active_subscriptions():
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(READ_SUBS_SQL) as cur:
                    rows = await cur.fetchall()
            return [Subscription(*row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка при получении активных подписок: {e}")
            raise

    @staticmethod
    async def update_subscription_status(subscription_id: int, user_id: int, is_active: bool):
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                result = await db.execute(
                    "UPDATE subscriptions SET is_active=? WHERE id=? AND user_id=?",
                    (1 if is_active else 0, subscription_id, user_id)
                )
                await db.commit()
            return result.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка при обновлении подписки #{subscription_id}: {e}")
            raise

    @staticmethod
    async def update_subscription_eval(subscription_id: int, eval_result: bool):
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE subscriptions SET last_eval=? WHERE id=?",
                    (1 if eval_result else 0, subscription_id)
                )
                await db.commit()
        except Exception as e:
            logger.error(f"Ошибка при обновлении eval подписки #{subscription_id}: {e}")
            raise

    @staticmethod
    async def delete_subscription(subscription_id: int, user_id: int):
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                result = await db.execute(
                    "DELETE FROM subscriptions WHERE id=? AND user_id=?",
                    (subscription_id, user_id)
                )
                await db.commit()
            return result.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка при удалении подписки #{subscription_id}: {e}")
            raise

    @staticmethod
    async def delete_all_user_subscriptions(user_id: int):
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                result = await db.execute("DELETE FROM subscriptions WHERE user_id=?", (user_id,))
                await db.commit()
            return result.rowcount
        except Exception as e:
            logger.error(f"Ошибка при удалении всех подписок пользователя {user_id}: {e}")
            raise