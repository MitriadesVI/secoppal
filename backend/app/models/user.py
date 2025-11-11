"""User model."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, String

from ..db.postgres import Base


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    phone_number = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


__all__ = ["User"]
