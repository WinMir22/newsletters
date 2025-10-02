import asyncio
import json
import logging
from datetime import datetime, UTC

from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.errors import KeyNotFoundError, KeyWrongLastSequenceError
from nats.js.kv import KeyValue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.database import Newsletter, User
from app.enums import UserRole, NewsletterStatus
from app.nats.publisher import Publisher

logger = logging.getLogger(__name__)


class Consumer:
    def __init__(
        self,
        nc: Client,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot,
        kv: KeyValue,
    ) -> None:
        self.nc = nc
        self.js = nc.jetstream()
        self.sessionmaker = sessionmaker
        self.bot = bot
        self._tasks = []
        self.kv = kv

    async def subscribe(self) -> JetStreamContext.PushSubscription:
        subscription = await self.js.subscribe(
            subject="newsletter",
            durable="newsletter_consumer_durable",
            cb=self._process_task,
        )
        return subscription

    async def _process_task(self, msg: Msg) -> None:
        data: dict[str, str] = json.loads(msg.data.decode("utf-8"))
        logger.info(f"Found task {data=}")
        if "user_id" not in data:
            await self._process_newsletter_task(msg=msg, data=data)
        else:
            task = asyncio.create_task(self._process_user_task(data))
            self._tasks.append(task)

            if len(self._tasks) == 25:
                await asyncio.gather(*self._tasks)

                self._tasks.clear()
                await asyncio.sleep(1)

            try:
                blocked = int(
                    (
                        await self.kv.get(f"blocked_{data['newsletter_id']}")
                    ).value.decode()
                )
            except KeyNotFoundError:
                blocked = 0
                await self.kv.put(f"blocked_{data['newsletter_id']}", b"0")

            try:
                errors = int(
                    (
                        await self.kv.get(f"errors_{data['newsletter_id']}")
                    ).value.decode()
                )
            except KeyNotFoundError:
                errors = 0
                await self.kv.put(f"errors_{data['newsletter_id']}", b"0")

            try:
                success = int(
                    (
                        await self.kv.get(f"success_{data['newsletter_id']}")
                    ).value.decode()
                )
            except KeyNotFoundError:
                success = 0
                await self.kv.put(f"success_{data['newsletter_id']}", b"0")

            total_users = await self.kv.get(
                f"total_users_{data['newsletter_id']}"
            )

            logger.info(f"{total_users=}")

            if blocked + success + errors + len(self._tasks) >= int(
                total_users.value.decode()
            ):
                logger.debug(blocked + success + errors + len(self._tasks))
                if self._tasks:
                    await asyncio.gather(*self._tasks)
                    self._tasks.clear()

                blocked = int(
                    (
                        await self.kv.get(f"blocked_{data['newsletter_id']}")
                    ).value.decode()
                )
                errors = int(
                    (
                        await self.kv.get(f"errors_{data['newsletter_id']}")
                    ).value.decode()
                )
                success = int(
                    (
                        await self.kv.get(f"success_{data['newsletter_id']}")
                    ).value.decode()
                )
                await self._finalize_newsletter(
                    data["newsletter_id"], success, blocked, errors
                )
            await msg.ack()

    async def _process_newsletter_task(
        self, msg: Msg, data: dict[str, str | int | bool]
    ) -> None:
        try:
            async with self.sessionmaker() as session:
                newsletter = await session.get(
                    Newsletter, int(data["newsletter_id"])
                )

                if (
                    newsletter is None
                    or newsletter.start_datetime > datetime.now(UTC)
                ):
                    return None

            publisher = Publisher(nc=self.nc, sessionmaker=self.sessionmaker)
            await publisher.start_newsletter(
                newsletter_id=data["newsletter_id"],
                newsletter_text=data["newsletter_text"],
                newsletter_photo=data["newsletter_photo"],
                is_game_notification=data["is_game_notification"],
                kv=self.kv,
            )

        finally:
            await msg.ack()

    async def _process_user_task(self, data: dict[str, str]) -> None:
        try:
            if data["is_game_notification"]:
                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="Начать игру", callback_data="start_game"
                            )
                        ]
                    ]
                )
            else:
                keyboard = None
            if data["newsletter_photo"]:
                await self.bot.send_photo(
                    chat_id=data["user_id"],
                    photo=data["newsletter_photo"],
                    caption=data["newsletter_text"],
                    reply_markup=keyboard,
                )
            else:
                await self.bot.send_message(
                    chat_id=data["user_id"],
                    text=data["newsletter_text"],
                    reply_markup=keyboard,
                )
            await self._atomic_increment(f"success_{data['newsletter_id']}")

        except TelegramForbiddenError:
            await self._atomic_increment(f"blocked_{data['newsletter_id']}")

        except TelegramRetryAfter:
            await asyncio.sleep(1)

        except Exception as e:
            await self._atomic_increment(f"errors_{data['newsletter_id']}")
            logger.error(e)

    async def _atomic_increment(self, key: str) -> None:
        max_retries = 10
        retry_delay = 0.1

        for attempt in range(max_retries):
            try:
                entry = await self.kv.get(key)
                current_value = int(entry.value.decode())
                new_value = current_value + 1

                await self.kv.update(
                    key, str(new_value).encode(), last=entry.revision
                )
                return

            except KeyNotFoundError:
                try:
                    await self.kv.put(key, "1".encode())
                    return
                except KeyWrongLastSequenceError:
                    await asyncio.sleep(retry_delay)
                    continue

            except KeyWrongLastSequenceError:
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    raise Exception(
                        f"Failed to increment {key} after {max_retries} attempts"
                    )

    async def _finalize_newsletter(
        self, newsletter_id: str, success: int, blocked: int, errors: int
    ) -> None:
        await self.kv.delete(key=f"success_{newsletter_id}")
        await self.kv.delete(key=f"errors_{newsletter_id}")
        await self.kv.delete(key=f"blocked_{newsletter_id}")
        statement = select(User.tg_id).where(
            User.role.in_((UserRole.ADMIN, UserRole.OWNER))
        )
        async with self.sessionmaker() as session:
            admin_ids = (await session.scalars(statement)).all()
            newsletter = await session.get(Newsletter, int(newsletter_id))
            newsletter.status = NewsletterStatus.COMPLETED
            await session.commit()

        for admin_id in admin_ids:
            await self.bot.send_message(
                admin_id,
                f"Рассылка завершена! \n\nУспешно отправлено: {success}\n"
                f"Заблокировано пользователями: {blocked}\nДругие ошибки: {errors}",
            )
