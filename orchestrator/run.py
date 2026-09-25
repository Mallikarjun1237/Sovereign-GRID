import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.database import init_db

from app.routes.nodes import router as node_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[*] Connecting to PostgreSQL and creating tables...")
    await init_db()
    print("[+] Database connected and tables verified!")
    yield

app = FastAPI(title="Sovereign Compute Orchestrator", lifespan=lifespan)
app.include_router(node_router)

# Allow React dashboard and workers to connect across LAN
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {"status": "ok", "db": "connected"}

if __name__ == "__main__":
    uvicorn.run("run:app", host="0.0.0.0", port=8000, reload=True)