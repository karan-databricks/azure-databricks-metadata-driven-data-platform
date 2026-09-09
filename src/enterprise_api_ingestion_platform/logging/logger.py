import logging


_LOG_FORMAT = (
    "%(asctime)s %(levelname)s "
    "%(name)s - %(message)s"
)


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured application logger.
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    handler = logging.StreamHandler()

    handler.setFormatter(
        logging.Formatter(_LOG_FORMAT)
    )

    logger.addHandler(handler)

    logger.setLevel(logging.INFO)

    logger.propagate = False

    return logger