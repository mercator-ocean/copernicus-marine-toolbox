import logging
import logging.config
import time

logging_configuration_dict = {
    "disable_existing_loggers": False,
    "formatters": {
        "blank": {"format": "%(message)s"},
        "simple": {
            "datefmt": "%Y-%m-%dT%H:%M:%SZ",
            "format": "%(levelname)s - %(asctime)s - %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "level": "DEBUG",
            "stream": "ext://sys.stderr",
        },
        "console_blank": {
            "class": "logging.StreamHandler",
            "formatter": "blank",
            "level": "DEBUG",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "copernicusmarine_blank_logger": {
            "handlers": ["console_blank"],
            "level": "INFO",
            "propagate": False,
        },
        "copernicusmarine": {
            "handlers": ["console"],
            "level": "INFO",
        },
    },
    "version": 1,
}

logging.config.dictConfig(logging_configuration_dict)
logging.Formatter.converter = time.gmtime
