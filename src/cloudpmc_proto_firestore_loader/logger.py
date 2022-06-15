import sys

from loguru import logger

config = {
    "handlers": [
        {
            "sink": sys.stderr,
            "format": "{time:YYYY-MM-DDTHH:mm:ss.SSS} - <level>{message}</level>",
            "colorize": True,
        },
    ],
}

logger.configure(**config)
