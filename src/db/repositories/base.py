"""
Template Method pattern — shared session/transaction primitives.
Subclasses inherit these helpers and override only domain-specific queries.
"""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class BaseRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory

    def _session(self) -> AsyncSession:
        return self._session_factory()
