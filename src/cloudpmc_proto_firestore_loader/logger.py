import sys

from loguru import logger

CONFIG = {
    "handlers": [
        {
            "sink": sys.stderr,
            "format": "{time:YYYY-MM-DDTHH:mm:ss.SSS} - <level>{message}</level>",
            "colorize": True,
            "level": "INFO",
        },
    ],
}

CONFIG_DEBUG = {
    "handlers": [
        {
            "sink": sys.stderr,
            "format": (
                "{time:YYYY-MM-DDTHH:mm:ss.SSS} - <level>{message}</level> "
                "<{file}:{line}> [elapsed={elapsed}]"
            ),
            "colorize": True,
            "level": "DEBUG",
        },
    ]
}
