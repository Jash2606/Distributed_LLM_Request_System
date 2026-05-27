from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


def build_engine(database_url: str):
    return create_async_engine(
        database_url,
        pool_size=10,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False,
    )


def build_session_factory(engine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
