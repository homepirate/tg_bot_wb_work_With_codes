from sqlalchemy import Column, String, DateTime, func
from .base import Base

class PrintedCode(Base):
    __tablename__ = "printed_codes"

    code = Column(String(128), primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
