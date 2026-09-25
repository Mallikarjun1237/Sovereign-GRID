from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.database import init_db

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initializes tables in PostgreSQL upon startup
    await init_db()
    yield

app = FastAPI(title="Sovereign Compute Orchestrator", lifespan=lifespan)