from sqlalchemy import Column, Integer, BigInteger, String, DateTime, func, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base

class OrderLog(Base):
    __tablename__ = "order_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)

    user_id = Column(
        BigInteger,
        ForeignKey("allowed_users.user_id", ondelete="SET NULL"),
        nullable=True,
    )

    article = Column(String(255), nullable=False)
    size = Column(Integer, nullable=False)
    qty_requested = Column(Integer, nullable=False)
    qty_sent = Column(Integer, nullable=False, default=0)
    shortage = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    user = relationship("AllowedUser", back_populates="logs", lazy="joined")
