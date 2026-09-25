import os
from pathlib import Path
from typing import AsyncGenerator
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.models import Base

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Replace 'postgres' with your actual PostgreSQL password if you changed it
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "sovereign_grid")

ENCODED_PASS = quote_plus(PG_PASS)
DATABASE_URL = f"postgresql+asyncpg://{PG_USER}:{ENCODED_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

engine = create_async_engine(DATABASE_URL, echo=False, pool_size=10, max_overflow=20)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    try:
        async with engine.begin() as conn:
            # Generates nodes, jobs, chunks, tasks tables automatically
            await conn.run_sync(Base.metadata.create_all)
    except Exception as exc:
        print(f"[DB ERROR] Could not connect to PostgreSQL database '{PG_DB}' on {PG_HOST}:{PG_PORT}.")
        print("[DB ERROR] Verify that the database exists, the password is correct, and the host/port match your local PostgreSQL instance.")
        raise exc

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session