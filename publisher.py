import json
import logging
from collections.abc import AsyncGenerator
from datetime import datetime, UTC

from nats.aio.client import Client
from nats.js.kv import KeyValue
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.database import User, Newsletter
from app.enums import NewsletterStatus

logger = logging.getLogger(__name__)


class Publisher:
    def __init__(
        self, nc: Client, sessionmaker: async_sessionmaker[AsyncSession]
    ) -> None:
        self.sessionmaker = sessionmaker
        self.nc = nc
        self.js = nc.jetstream()

    async def publish_newsletter_task(
        self,
        newsletter_id: str,
        newsletter_text: str,
        newsletter_photo: int = None,
        is_game_notification: bool = False,
    ) -> None:
        message = json.dumps(
            {
                "newsletter_id": newsletter_id,
                "newsletter_text": newsletter_text,
                "newsletter_photo": newsletter_photo,
                "is_game_notification": is_game_notification,
            }
        ).encode("utf-8")

        logger.info(f"Publishing newsletter task with {newsletter_id=}")
        await self.js.publish(subject="newsletter", payload=message)

    async def start_newsletter(
        self,
        newsletter_id: str,
        newsletter_text: str,
        newsletter_photo: int,
        is_game_notification: bool,
        kv: KeyValue,
    ) -> None:
        cutoff_time = datetime.now(UTC)

        try:
            async with self.sessionmaker() as session:
                total_statement = select(func.count()).where(
                    and_(
                        User.banned.is_(False), User.created_at <= cutoff_time
                    )
                )
                total_users = await session.scalar(total_statement)

                newsletter = await session.get(Newsletter, int(newsletter_id))
                newsletter.status = NewsletterStatus.IN_PROCESS
                await kv.put(
                    f"total_users_{newsletter_id}", str(total_users).encode()
                )
                await session.commit()

            async with self.sessionmaker() as session:
                statement = select(User.tg_id).where(
                    and_(
                        User.banned.is_(False), User.created_at <= cutoff_time
                    )
                )
                users_ids = await session.stream_scalars(statement)

                async for user_id in users_ids:
                    await self._publish_user_task(
                        user_id=user_id,
                        newsletter_id=str(newsletter_id),
                        newsletter_text=newsletter_text,
                        newsletter_photo=newsletter_photo,
                        is_game_notification=is_game_notification,
                    )

        except:
            async with self.sessionmaker() as session:
                newsletter = await session.get(Newsletter, int(newsletter_id))
                newsletter.status = NewsletterStatus.WAIT
                await session.commit()
            raise

    async def _get_users(self, cutoff_time: datetime) -> AsyncGenerator[int]:
        statement = select(User.tg_id).where(
            and_(User.banned.is_(False), User.created_at <= cutoff_time)
        )

        async with self.sessionmaker() as session:
            users_ids = await session.stream_scalars(statement)

            async for user_id in users_ids:
                yield user_id

    async def _publish_user_task(
        self,
        user_id: int,
        newsletter_id: str,
        newsletter_text: str,
        newsletter_photo: int,
        is_game_notification: bool,
    ) -> None:
        message = json.dumps(
            {
                "user_id": user_id,
                "newsletter_id": newsletter_id,
                "newsletter_text": newsletter_text,
                "newsletter_photo": newsletter_photo,
                "is_game_notification": is_game_notification,
            }
        ).encode("utf-8")

        await self.js.publish(
            subject="newsletter",
            payload=message,
            headers={"Nats-Msg-Id": f"{newsletter_id}_{user_id}"},
        )
