import asyncio
import logging
from typing import Any

from aiogram import Bot
from aiogram.exceptions import (
    TelegramRetryAfter,
    TelegramForbiddenError,
    TelegramBadRequest,
)
from dishka import FromDishka
from faststream import FastStream
from faststream.nats import NatsBroker, PullSub
from nats.aio.client import Client
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.infrastructure.database import NewsletterRepository, Newsletter
from app.infrastructure.enums import NewsletterStatus
from app.services.newsletters.publisher import Publisher
from app.services.newsletters.schemas import NewsletterUserData, NewsletterData
from config import load_config

broker = NatsBroker(
    load_config().nats_config.nats_url,
    connect_timeout=10,
    reconnect_time_wait=2,
    max_reconnect_attempts=5,
)
app = FastStream(broker)
logger = logging.getLogger(__name__)


def unpack_data(data: dict[str, Any]) -> NewsletterUserData | NewsletterData:
    if "user_id" in data:
        return NewsletterUserData(**data)
    return NewsletterData(**data)


@broker.subscriber(
    "newsletter.tasks", durable="tasks_consumer", stream="newsletter_stream"
)
async def process_newsletter_task(
    message: dict[str, Any],
    nc: FromDishka[Client],
    sessionmaker: FromDishka[async_sessionmaker[AsyncSession]],
    newsletter_repository: FromDishka[NewsletterRepository],
    redis: FromDishka[Redis],
) -> None:
    publisher = Publisher(
        js=nc.jetstream(), sessionmaker=sessionmaker, redis=redis
    )
    data = unpack_data(message)
    if await newsletter_repository.can_start_newsletter(data.newsletter_id):
        await publisher.start_newsletter(data.newsletter_id, text=data.text)


@broker.subscriber(
    "newsletter.messages",
    durable="user_tasks_consumer",
    pull_sub=PullSub(batch_size=25, timeout=1),
    stream="newsletter_stream",
)
async def process_user_task(
    message: dict[str, Any],
    bot: FromDishka[Bot],
    redis: FromDishka[Redis],
    sessionmaker: FromDishka[async_sessionmaker[AsyncSession]],
) -> None:
    flag = True
    data = unpack_data(message)
    try:
        await bot.send_message(chat_id=data.user_id, text=data.text)
        await redis.incr(f"{data.newsletter_id}-success")
    except TelegramRetryAfter as e:
        logger.warning(f"Ура, лимит исчерпан! Сплю {e.retry_after} сек")
        flag = False
        await asyncio.sleep(e.retry_after)
        raise
    except TelegramForbiddenError:
        logger.info(f"User {data.user_id} has blocked bot")
    except TelegramBadRequest as e:
        if "chat not found" in str(e).lower():
            logger.info(f"User {data.user_id} has blocked bot")
    except Exception as e:
        logger.error(e)
        await redis.incr(f"{data.newsletter_id}-errors")
    finally:
        if flag:
            await redis.incr(f"{data.newsletter_id}-total")

    if await redis.get(f"{data.newsletter_id}-total") == await redis.get(
        f"{data.newsletter_id}-target"
    ):
        async with sessionmaker() as session:
            newsletter = await session.get(Newsletter, data.newsletter_id)
            newsletter.status = NewsletterStatus.COMPLETED
            owner_id = newsletter.owner_id
            await session.commit()

        success = await redis.get(f"{data.newsletter_id}-success") or 0
        blocked = await redis.get(f"{data.newsletter_id}-blocked") or 0
        errors = await redis.get(f"{data.newsletter_id}-errors") or 0

        await redis.delete(f"{data.newsletter_id}-total")
        await redis.delete(f"{data.newsletter_id}-target")
        await redis.delete(f"{data.newsletter_id}-success")
        await redis.delete(f"{data.newsletter_id}-errors")

        await bot.send_message(
            int(str(owner_id)),
            f"Рассылка завершена! \n\nУспешно отправлено: {success}\n"
            f"Заблокировано пользователями: {blocked}\nДругие ошибки: {errors}",
        )
