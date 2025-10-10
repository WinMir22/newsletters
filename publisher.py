import json

from nats.aio.client import Client
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.infrastructure.database import User


async def base_polling_task(
    nc: Client,
    sessionmaker: async_sessionmaker[AsyncSession],
    newsletter_id: int,
    text: str,
) -> None:
    jetstream = nc.jetstream()

    async with sessionmaker() as session:
        statement = select(User.tg_id).where(User.banned.is_(False))
        user_ids = await session.stream_scalars(statement)

        async for user_id in user_ids:
            await jetstream.publish(
                subject="newsletter.messages",
                payload=json.dumps(
                    {
                        "user_id": user_id,
                        "text": text,
                        "newsletter_id": newsletter_id,
                    },
                # headers={"Nats-Msg-Id": f"{newsletter_id}:{user_id}"},
                ).encode("utf-8"),
            )
