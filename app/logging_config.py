"""
Logging Configuration - LEEF Datashare API

Structured logging voor productie omgeving.
AVG-compliant: geen persoonsgegevens in logs (geen BAG ID's, adressen, namen).

Gebruik:
    from logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("Actie uitgevoerd", extra={"request_id": "xxx"})
"""
import logging
import os
import json
from datetime import datetime
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """
    JSON formatter voor structured logging.
    Maakt logs makkelijk te parsen door log aggregators (ELK, Grafana Loki, etc.)
    """

    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "service": "leef-datashare-api",
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }

        # Voeg extra fields toe (request_id, etc.)
        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id

        if hasattr(record, "duration_ms"):
            log_data["duration_ms"] = record.duration_ms

        # Exception info
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, ensure_ascii=False)


class SimpleFormatter(logging.Formatter):
    """
    Leesbare formatter voor development.
    """

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        return f"{timestamp} [{record.levelname}] {record.module}.{record.funcName}: {record.getMessage()}"


def get_logger(name: str) -> logging.Logger:
    """
    Verkrijg een geconfigureerde logger.

    Args:
        name: Naam van de module (gebruik __name__)

    Returns:
        Geconfigureerde logger instance
    """
    logger = logging.getLogger(name)

    # Voorkom dubbele handlers
    if logger.handlers:
        return logger

    # Bepaal log level uit environment
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    # Kies formatter op basis van environment
    handler = logging.StreamHandler()
    environment = os.getenv("ENVIRONMENT", "development")

    if environment == "production":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(SimpleFormatter())

    logger.addHandler(handler)

    # Voorkom propagatie naar root logger
    logger.propagate = False

    return logger
