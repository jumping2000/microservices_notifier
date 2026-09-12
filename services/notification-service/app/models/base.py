from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """This service's own registry. See spec correction 3.12."""
