from sqlalchemy import Column, BigInteger, Enum
from sqlalchemy.orm import relationship

from .user_role import UserRole
from .base import Base

class AllowedUser(Base):
    __tablename__ = "allowed_users"

    user_id = Column(BigInteger, primary_key=True)
    role = Column(Enum(UserRole), nullable=False, default=UserRole.USER)

    logs = relationship(
        "OrderLog",
        back_populates="user",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
