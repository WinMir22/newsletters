import json
import logging
import asyncio
from datetime import datetime, UTC
from collections.abc import AsyncGenerator

from nats.js import JetStreamContext
from redis.asyncio import Redis
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.infrastructure.database import User, Newsletter
from app.infrastructure.enums import NewsletterStatus

logger = logging.getLogger(__name__)


class Publisher:
    def __init__(
        self,
        js: JetStreamContext,
        sessionmaker: async_sessionmaker[AsyncSession],
        redis: Redis,
        batch_size: int = 1000,
    ) -> None:
        self.sessionmaker = sessionmaker
        self.js = js
        self.batch_size = batch_size
        self.redis = redis

    async def publish_newsletter_task(
        self, newsletter_id: int, text: str
    ) -> None:
        message = json.dumps(
            {"newsletter_id": newsletter_id, "text": text}
        ).encode("utf-8")

        logger.info(
            f"Publishing newsletter task with newsletter_id={newsletter_id}"
        )
        await self.js.publish(subject="newsletter.tasks", payload=message)

    async def start_newsletter(self, newsletter_id: int, text: str) -> None:
        cutoff_time = datetime.now(UTC)

        await self._update_newsletter_status(
            newsletter_id, NewsletterStatus.IN_PROCESS
        )

        try:
            user_ids_stream = self._stream_user_ids(
                cutoff_time, newsletter_id=newsletter_id
            )

            batch_tasks = []
            async for user_id in user_ids_stream:
                batch_tasks.append(
                    self._publish_user_task(user_id, newsletter_id, text)
                )

                if len(batch_tasks) >= self.batch_size:
                    await asyncio.gather(*batch_tasks, return_exceptions=True)
                    batch_tasks = []
                    logger.debug(
                        f"Published batch of {self.batch_size} messages"
                    )

            if batch_tasks:
                await asyncio.gather(*batch_tasks, return_exceptions=True)
                logger.debug(
                    f"Published final batch of {len(batch_tasks)} messages"
                )

            logger.info(f"Successfully completed newsletter {newsletter_id}")

        except Exception as e:
            logger.error(f"Failed to publish newsletter {newsletter_id}: {e}")
            await self._update_newsletter_status(
                newsletter_id, NewsletterStatus.WAIT
            )
            raise

    async def _stream_user_ids(
        self, cutoff_time: datetime, newsletter_id: int
    ) -> AsyncGenerator[int, None]:
        async with self.sessionmaker() as session:
            statement = select(User.tg_id).where(
                and_(User.banned.is_(False), User.created_at <= cutoff_time)
            )
            stmt = select(User.tg_id).where(User.banned.is_(False))

            result = await session.stream_scalars(statement)
            await self.redis.incr(
                f"{newsletter_id}-target", await session.scalar(stmt)
            )
            async for user_id in result:
                yield user_id

    async def _update_newsletter_status(
        self, newsletter_id: int, status: NewsletterStatus
    ) -> None:
        async with self.sessionmaker() as session:
            newsletter = await session.get(Newsletter, newsletter_id)
            if newsletter:
                newsletter.status = status
                await session.commit()
            else:
                raise ValueError(f"Newsletter {newsletter_id} not found")

    async def _publish_user_task(
        self, user_id: int, newsletter_id: int, text: str
    ) -> None:
        message = json.dumps(
            {"user_id": user_id, "newsletter_id": newsletter_id, "text": text}
        ).encode("utf-8")

        await self.js.publish(
            subject="newsletter.messages",
            payload=message,
            # headers={"Nats-Msg-Id": f"{newsletter_id}:{user_id}"},
        )
