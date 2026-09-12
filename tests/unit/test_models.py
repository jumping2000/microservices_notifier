from notification_shared.models import (
    OutboxMixin,
    ProcessedEventMixin,
    TimestampMixin,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Outbox(Base, OutboxMixin):
    __tablename__ = "outbox"


class ProcessedEvent(Base, ProcessedEventMixin):
    __tablename__ = "processed_events"


class Thing(Base, TimestampMixin):
    __tablename__ = "thing"
    from sqlalchemy.orm import Mapped, mapped_column

    id: Mapped[int] = mapped_column(primary_key=True)


def test_mixins_declare_no_table_of_their_own():
    assert not hasattr(OutboxMixin, "__tablename__")
    assert not hasattr(ProcessedEventMixin, "__tablename__")
    assert not hasattr(TimestampMixin, "__tablename__")


def test_outbox_columns_match_the_spec():
    assert set(Outbox.__table__.columns.keys()) == {
        "id",
        "stream",
        "payload",
        "published",
        "created_at",
    }
    assert Outbox.__table__.c.published.nullable is False
    assert Outbox.__table__.c.payload.nullable is False


def test_outbox_has_an_index_supporting_the_pending_query():
    indexed = [tuple(ix.columns.keys()) for ix in Outbox.__table__.indexes]
    assert ("published", "created_at") in indexed


def test_processed_events_columns_match_the_spec():
    assert set(ProcessedEvent.__table__.columns.keys()) == {
        "id",
        "event_id",
        "consumer_group",
        "status",
        "fail_count",
        "processed_at",
    }


def test_processed_events_is_unique_on_event_and_group():
    unique_sets = [
        set(c.columns.keys())
        for c in ProcessedEvent.__table__.constraints
        if c.__class__.__name__ == "UniqueConstraint"
    ]
    assert {"event_id", "consumer_group"} in unique_sets


def test_timestamp_mixin_adds_both_columns_with_server_defaults():
    assert Thing.__table__.c.created_at.server_default is not None
    assert Thing.__table__.c.updated_at.server_default is not None
    assert Thing.__table__.c.updated_at.onupdate is not None


def test_two_bases_can_materialize_the_mixins_independently():
    class OtherBase(DeclarativeBase):
        pass

    class OtherOutbox(OtherBase, OutboxMixin):
        __tablename__ = "outbox"

    assert "outbox" in OtherBase.metadata.tables
    assert "outbox" in Base.metadata.tables
    assert OtherBase.metadata is not Base.metadata
