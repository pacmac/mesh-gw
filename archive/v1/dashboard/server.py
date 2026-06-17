"""
Standalone dashboard server.
Serves static files from dashboard/static/ on port 8000.
The dashboard talks to mesh-gw on port 8001 (configured in config.js).
"""
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pathlib import Path

app = FastAPI(title="mesh-gw dashboard", docs_url=None, redoc_url=None)

STATIC = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=STATIC, html=True), name="static")

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
