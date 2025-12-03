from __future__ import annotations
import logging
import logging.config
import os

def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    access_level = os.getenv("ACCESS_LOG_LEVEL", "WARNING").upper()
    fmt = "%(asctime)s | %(levelname)-8s | %(message)s"

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "std": {"format": fmt},
        },
        "handlers": {
            "console": {"class": "logging.StreamHandler", "formatter": "std"},
        },
        "root": {"level": level, "handlers": ["console"]},
        # Keep uvicorn logs but don’t let them drown our app logs
        "loggers": {
            "uvicorn": {"level": level, "handlers": ["console"], "propagate": False},
            "uvicorn.error": {"level": level, "handlers": ["console"], "propagate": False},
            "uvicorn.access": {"level": access_level, "handlers": ["console"], "propagate": False},
            # Our app loggers
            "sf-listener": {"level": level, "handlers": ["console"], "propagate": False},
            "listener-manager": {"level": level, "handlers": ["console"], "propagate": False},
            "multi-client": {"level": level, "handlers": ["console"], "propagate": False},
        },
    })
