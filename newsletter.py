from datetime import datetime

from sqlalchemy import DateTime, BigInteger
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database import Base
from app.infrastructure.enums import NewsletterStatus


class Newsletter(Base):
    __tablename__ = "newsletters"

    id: Mapped[int] = mapped_column(primary_key=True)
    status: Mapped[str] = mapped_column(server_default=NewsletterStatus.WAIT)
    text: Mapped[str] = mapped_column(nullable=False)
    owner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    start_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True))
