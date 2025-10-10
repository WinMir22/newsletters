import logging

from dishka import make_async_container, FromDishka
from dishka.integrations.taskiq import inject, setup_dishka
from nats.aio.client import Client
from nats.js.errors import NoStreamResponseError, NotFoundError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.infrastructure.database import Newsletter
from app.infrastructure.di.providers import (
    ConfigProvider,
    SessionmakerProvider,
    NatsConnectProvider,
)
from app.infrastructure.enums import NewsletterStatus
from app.services.newsletters.publisher import base_polling_task
from app.services.newsletters.setup_stream import setup_stream
from app.services.scheduler.broker import broker

logger = logging.getLogger(__name__)

container = make_async_container(
    ConfigProvider(), NatsConnectProvider(), SessionmakerProvider()
)
setup_dishka(container=container, broker=broker)


@broker.task(schedule=[{"cron": "* * * * *"}])
@inject
async def check_newsletters(
    nc: FromDishka[Client],
    sessionmaker: FromDishka[async_sessionmaker[AsyncSession]],
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
        logger.info(f"Found {newsletters_ids=}")

        for newsletter_id, newsletter_text in newsletters_ids:
            await base_polling_task(
                nc=nc,
                sessionmaker=sessionmaker,
                newsletter_id=newsletter_id,
                text=newsletter_text,
            )
