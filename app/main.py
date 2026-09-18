from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title="Telegram Bingo")


@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Telegram Bingo</title>
    </head>
    <body>
        <h1>🎱 Telegram Bingo</h1>
        <p>Server is running successfully.</p>
    </body>
    </html>
    """


@app.get("/health")
async def health():
    return {"status": "ok"}
