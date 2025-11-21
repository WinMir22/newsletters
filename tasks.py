import logging

from dishka import make_async_container, FromDishka
from dishka.integrations.taskiq import inject, setup_dishka
from nats.aio.client import Client
from nats.js.errors import NoStreamResponseError, NotFoundError
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.infrastructure.database import Newsletter
from app.infrastructure.di.providers import (
    ConfigProvider,
    SessionmakerProvider,
    NatsConnectProvider,
    RedisProvider,
)
from app.infrastructure.enums import NewsletterStatus
from app.services.newsletters.publisher import Publisher
from app.services.newsletters.setup_stream import setup_stream
from app.services.scheduler.broker import broker

logger = logging.getLogger(__name__)

container = make_async_container(
    ConfigProvider(),
    NatsConnectProvider(),
    SessionmakerProvider(),
    RedisProvider(),
)
setup_dishka(container=container, broker=broker)


@broker.task(schedule=[{"cron": "* * * * *"}])
@inject
async def check_newsletters(
    nc: FromDishka[Client],
    sessionmaker: FromDishka[async_sessionmaker[AsyncSession]],
    redis: FromDishka[Redis],
) -> None:
    try:
        await nc.jetstream().stream_info("newsletter_stream")
    except (NoStreamResponseError, NotFoundError):
        await setup_stream(js=nc.jetstream())

    statement = select(Newsletter.id, Newsletter.text).where(
        Newsletter.status == NewsletterStatus.WAIT
    )
    async with sessionmaker() as session:
        newsletters_ids = (await session.execute(statement)).all()
        logger.debug(f"Found {newsletters_ids=}")

        publisher = Publisher(
            sessionmaker=sessionmaker, js=nc.jetstream(), redis=redis
        )
        for newsletter_id, newsletter_text in newsletters_ids:
            await publisher.publish_newsletter_task(
                newsletter_id=newsletter_id, text=newsletter_text
            )
