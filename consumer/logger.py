import json
import logging
from datetime import datetime, timezone


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    return logger


class JsonFormatter(logging.Formatter):
    """Formata cada log como uma linha JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }

        # Adiciona campos extras se existirem (ex: product_id, msg_id)
        if hasattr(record, "extra"):
            log_entry.update(record.extra)

        return json.dumps(log_entry, ensure_ascii=False)
