import asyncio
import json
import logging

from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError
from nats.js import JetStreamContext

from app.services.newsletters.schemas import NewsletterUserData

logger = logging.getLogger(__name__)


async def nats_polling(bot: Bot, jetstream: JetStreamContext) -> None:
    subscribe = await jetstream.subscribe(
        subject="newsletter.messages", durable="get_message", manual_ack=True
    )

    async for message in subscribe.messages:
        try:
            data = NewsletterUserData(
                **json.loads(message.data.decode("utf-8"))
            )
            await bot.send_message(chat_id=data.user_id, text=data.text)
        except TimeoutError:
            pass
        except TelegramRetryAfter as ex:
            logger.warning(f"Limit exceeded, continue in: {ex.retry_after}")
            await asyncio.sleep(float(ex.retry_after))
            continue
        except TelegramForbiddenError:
            logger.info("User blocked Bot")
            await message.ack()
            continue
        except BaseException as ex:
            logger.error(f"Unexpected error: {ex}")
            continue
