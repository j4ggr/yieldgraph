import logging

logging.addLevelName(5, 'TRACE')


class _LogConfig:
    TRACE: int = 5
    DEBUG: int = logging.DEBUG
    INFO: int = logging.INFO
    WARNING: int = logging.WARNING
    ERROR: int = logging.ERROR
    TRACEBACK: bool = True


LOG = _LogConfig()


class LoggingBehavior:
    def log(self, message: str, level: int) -> None:
        logging.getLogger(type(self).__name__).log(level, message)


__all__ = ['LOG', 'LoggingBehavior']
