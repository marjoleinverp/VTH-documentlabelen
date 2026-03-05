"""
LEEF Datashare API - Standalone FastAPI applicatie

Document classificatie en labels voor LEEF Datashare zaken.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from pathlib import Path
import os

from logging_config import get_logger

logger = get_logger(__name__)

app = FastAPI(
    title="LEEF Datashare API",
    description="Document classificatie en labels voor LEEF Datashare",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Security headers middleware
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response


app.add_middleware(SecurityHeadersMiddleware)


# Mount leef routes met prefix /geo (zodat HTML files ongewijzigd werken)
from leef_routes import router as leef_router
app.include_router(leef_router)


# Mount static files
static_dir = Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# Startup: init database
@app.on_event("startup")
async def startup():
    logger.info("LEEF Datashare API starting...")
    try:
        from init_database import init_leef_tables
        init_leef_tables()
        logger.info("Database tabellen geinitialiseerd")
    except Exception as e:
        logger.error(f"Database initialisatie mislukt: {e}")


# Health endpoint
@app.get("/health")
async def health():
    from db_config import check_database_health
    db_health = check_database_health()
    return {
        "status": "healthy" if db_health["healthy"] else "unhealthy",
        "service": "leef-datashare-api",
        "database": db_health,
    }


# Root redirect naar static
@app.get("/")
async def root():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/static/leef_datashare.html")
