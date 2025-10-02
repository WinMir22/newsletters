import logging
from datetime import datetime, UTC

from dishka import make_async_container, FromDishka
from dishka.integrations.taskiq import inject, setup_dishka
from nats.aio.client import Client
from nats.js.errors import NoStreamResponseError, NotFoundError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.database import Newsletter
from app.dishka_settings.providers import (
    ConfigProvider,
    SessionmakerProvider,
    NatsConnectProvider,
)
from app.enums import NewsletterStatus
from app.nats.publisher import Publisher
from app.nats.setup_stream import setup_stream
from app.taskiq.broker import broker

logger = logging.getLogger(__name__)

container = make_async_container(
    ConfigProvider(), NatsConnectProvider(), SessionmakerProvider()
)
setup_dishka(container=container, broker=broker)


@broker.task(schedule=[{"cron": "16 45 * * * "}])
@inject
async def start_mailing(
    sessionmaker: FromDishka[async_sessionmaker[AsyncSession]],
) -> None:
    first_newsletter = Newsletter(
        text="В 20:00 мск начинается игра!",
        start_datetime=datetime.now(UTC).replace(
            hour=16, minute=50, second=0, microsecond=0
        ),
        is_game_notification=True,
    )
    second_newsletter = Newsletter(
        text="В 20:00 мск начинается игра!",
        start_datetime=datetime.now(UTC).replace(
            hour=16, minute=58, second=0, microsecond=0
        ),
        is_game_notification=True,
    )
    async with sessionmaker() as session:
        session.add_all((first_newsletter, second_newsletter))
        await session.commit()


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

    statement = select(
        Newsletter.id,
        Newsletter.text,
        Newsletter.photo,
        Newsletter.is_game_notification,
    ).where(
        Newsletter.status == NewsletterStatus.WAIT,
        Newsletter.start_datetime <= datetime.now(UTC),
    )
    async with sessionmaker() as session:
        newsletters_ids = (await session.execute(statement)).all()
        logger.info(f"Found {newsletters_ids=}")

        publisher = Publisher(nc=nc, sessionmaker=sessionmaker)
        for (
            newsletter_id,
            newsletter_text,
            photo,
            is_game_notification,
        ) in newsletters_ids:
            await publisher.publish_newsletter_task(
                newsletter_id=newsletter_id,
                newsletter_text=newsletter_text,
                newsletter_photo=photo,
                is_game_notification=is_game_notification,
            )
