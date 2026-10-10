
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import SQLAlchemyError


DATABASE_URL = os.getenv("DATABASE_URL")

engine = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine

    if DATABASE_URL:
        database_url = DATABASE_URL

        # Render PostgreSQL URLs may use the postgres:// scheme.
        if database_url.startswith("postgres://"):
            database_url = database_url.replace(
                "postgres://", "postgresql+asyncpg://", 1
            )
        elif database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )

        engine = create_async_engine(
            database_url,
            pool_pre_ping=True,
        )

        try:
            async with engine.begin() as connection:
                await connection.execute(text("""
                    CREATE TABLE IF NOT EXISTS bingo_users (
                        id BIGSERIAL PRIMARY KEY,
                        telegram_id BIGINT UNIQUE,
                        username VARCHAR(100),
                        language VARCHAR(10) NOT NULL DEFAULT 'en',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """))
            print("PostgreSQL connected; bingo_users table is ready.")
        except SQLAlchemyError:
            print("PostgreSQL connection failed. Check DATABASE_URL.")
            await engine.dispose()
            engine = None

    else:
        print("DATABASE_URL is not configured.")

    yield

    if engine is not None:
        await engine.dispose()


app = FastAPI(
    title="Telegram Bingo",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Telegram Bingo</title>
    </head>
    <body>
        <h1>🎱 Telegram Bingo</h1>
        <p>Your Bingo server is running.</p>
        <p>Database setup is being checked by the server.</p>
    </body>
    </html>
    """


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/health/database")
async def database_health():
    if engine is None:
        return {
            "status": "not_connected",
            "message": "Check Render logs and DATABASE_URL."
        }

    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

        return {"status": "connected"}

    except SQLAlchemyError:
        return {"status": "error"}
