from datetime import datetime

from sqlalchemy import DateTime, text as text_f
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base
from app.enums import NewsletterStatus


class Newsletter(Base):
    __tablename__ = "newsletters"

    id: Mapped[int] = mapped_column(primary_key=True)
    status: Mapped[str] = mapped_column(server_default=NewsletterStatus.WAIT)
    text: Mapped[str] = mapped_column(nullable=False)
    offset: Mapped[int] = mapped_column(server_default="0")
    start_datetime: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=text_f("TIMEZONE('utc', NOW())"),
        nullable=False,
    )
    buttons: Mapped[dict[int, list[str]]] = mapped_column(JSON, nullable=True)
    is_game_notification: Mapped[bool] = mapped_column(
        nullable=False, server_default="False"
    )
    photo: Mapped[str] = mapped_column(nullable=True)
