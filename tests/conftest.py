import os
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from src.core.config import settings
from src.core.database import Base

SQLALCHEMY_DATABASE_URL = settings.DATABASE_URL.replace("5433/vod", "5433/vod_test")
engine = create_engine(SQLALCHEMY_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="session", autouse=True)
def setup_test_db():
    from sqlalchemy_utils import database_exists, create_database, drop_database
    if not database_exists(SQLALCHEMY_DATABASE_URL):
        create_database(SQLALCHEMY_DATABASE_URL)
    
    from alembic.config import Config
    from alembic import command
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", SQLALCHEMY_DATABASE_URL)
    
    import logging
    log = logging.getLogger("conftest")
    log.info("\n[ALEMBIC] Checking locks before upgrade...")
    from sqlalchemy import text
    with engine.connect() as db:
        res = db.execute(text("SELECT locktype, relation::regclass, mode, granted, pg_locks.pid FROM pg_locks JOIN pg_stat_activity ON pg_locks.pid = pg_stat_activity.pid WHERE datname = 'vod_test'")).fetchall()
        log.info(f"LOCKS: {res}")
    
    log.info("\n[ALEMBIC] Upgrading to head...")
    command.upgrade(alembic_cfg, "head")
    log.info("\n[ALEMBIC] Upgrade finished!")
    yield
    engine.dispose()

@pytest.fixture(autouse=True)
def clear_db():
    db = TestingSessionLocal()
    for table in reversed(Base.metadata.sorted_tables):
        db.execute(table.delete())
    db.commit()
    db.close()
    
    # Also flush redis for tests
    from redis import Redis
    redis_conn = Redis.from_url(settings.REDIS_URL)
    redis_conn.flushdb()
    redis_conn.close()

@pytest.fixture
def db_session():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
