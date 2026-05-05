"""Configuration module for the `yieldgraph` library.

This module defines constants and classes related to environment
configuration and logging behavior in the `yieldgraph` library. It
includes the :data:`ENV` instance of :class:`_ENV_`, which exposes the
current values of environment variables as boolean properties; the
:data:`LOG` instance of :class:`_Log_`, which contains log level
constants; and the :class:`LoggingBehavior` mixin class that provides
logging capabilities to classes that inherit from it.

The `LoggingBehavior` class defines a :attr:`logger` attribute that can 
be used by classes that inherit from it to log messages at various log 
levels. The log levels are defined in the `LOG` instance, and include 
TRACE, DEBUG, INFO, WARNING, ERROR, and CRITICAL. Whether traceback 
information is included in error log messages is controlled by the 
``YIELDGRAPH_LOG_TRACEBACK`` environment variable via 
:attr:`ENV.LOG_TRACEBACK`. All logging can be suppressed entirely by 
setting the ``YIELDGRAPH_LOG_DISABLED`` environment variable to a truthy
value, which causes every call to :meth:`LoggingBehavior.log` to return
immediately without emitting anything.

The `LoggingBehavior` class provides a :attr:`log_title` property that
can be overridden by classes that inherit from it to provide a custom 
log title for log messages. If not overridden, the log title defaults to 
the class name. The :attr:`log` method is used to log messages with a 
specified log level, and there are convenience methods for logging at 
specific levels (e.g., :attr:`log_info`, :attr:`log_warning`, etc.) that 
call the :attr:`log` method with the appropriate log level. This allows 
classes that inherit from `LoggingBehavior` to easily log messages with 
consistent formatting and log levels throughout the `yieldgraph` 
library.
"""

import os
import logging

from typing import Tuple
from typing import Literal
from typing import Optional
from dataclasses import dataclass


__all__ = [
    'LOG',
    'LoggingBehavior',
    'START_NODE_NAME',
    'ENV',
]


START_NODE_NAME = '__START__'
"""Sentinel name for the implicit start node that seeds pipeline chains."""

class _ENV_:
    """Class providing runtime values of environment variables used to
    configure the `yieldgraph` library. Properties are evaluated at
    access time, so changes to environment variables are always
    reflected immediately without reloading the module."""

    THREADED_KEY: str = 'YIELDGRAPH_THREADED'
    """Name of the environment variable that enables threaded execution."""

    LOG_TRACEBACK_KEY: str = 'YIELDGRAPH_LOG_TRACEBACK'
    """Name of the environment variable that configures traceback logging."""

    LOG_DISABLED_KEY: str = 'YIELDGRAPH_LOG_DISABLED'
    """Name of the environment variable that disables all logging output."""

    TRUEISH_VALUES: Tuple[str, ...] = ('1', 'true', 'yes')
    """Case-insensitive values that are considered "true" for 
    environment variables."""

    @property
    def THREADED(self) -> bool:
        """Whether threaded execution is currently enabled.

        Returns ``True`` if the ``YIELDGRAPH_THREADED`` environment
        variable is set to one of the values in :attr:`TRUEISH_VALUES`
        (case-insensitive), ``False`` otherwise.

        Examples
        --------
        ```bash
        YIELDGRAPH_THREADED=1 python my_pipeline.py
        ```
        or in Python:
        ```python
        import os
        os.environ['YIELDGRAPH_THREADED'] = '1'
        g = Graph()
        g.add_chain(extract, transform, load)
        g.run()
        ```
        """
        threaded_value = os.environ.get(self.THREADED_KEY, '')
        return threaded_value.lower() in self.TRUEISH_VALUES

    @property
    def LOG_TRACEBACK(self) -> bool:
        """Whether traceback information is included in log messages.

        Returns ``True`` if the ``YIELDGRAPH_LOG_TRACEBACK`` environment
        variable is set to one of the values in :attr:`TRUEISH_VALUES`
        (case-insensitive), ``False`` otherwise.
        """
        traceback_value = os.environ.get(self.LOG_TRACEBACK_KEY, '')
        return traceback_value.lower() in self.TRUEISH_VALUES

    @property
    def LOG_DISABLED(self) -> bool:
        """Whether all logging output is disabled.

        Returns ``True`` if the ``YIELDGRAPH_LOG_DISABLED`` environment
        variable is set to one of the values in :attr:`TRUEISH_VALUES`
        (case-insensitive), ``False`` otherwise. When ``True``, every
        call to :meth:`~yieldgraph.config.LoggingBehavior.log` returns
        immediately without emitting anything.
        """
        return os.environ.get(self.LOG_DISABLED_KEY, '').lower() in self.TRUEISH_VALUES

ENV = _ENV_()
"""Instance of :class:`_ENV_` that provides the current values of
environment variables used to configure the `yieldgraph` library.

Properties are evaluated at access time, so changes to environment
variables are immediately reflected. Use the ``_KEY`` class attributes
(e.g. :attr:`~_ENV_.THREADED_KEY`, :attr:`~_ENV_.LOG_TRACEBACK_KEY`,
:attr:`~_ENV_.LOG_DISABLED_KEY`) to access the raw key names when
manipulating ``os.environ`` directly (e.g., in tests).

Quick reference:

+------------------------------+---------------------------+----------------------------------------+
| Property                     | Key constant              | Effect when truthy                     |
+==============================+===========================+========================================+
| :attr:`ENV.THREADED`         | ``ENV.THREADED_KEY``      | Run graph nodes in threads             |
+------------------------------+---------------------------+----------------------------------------+
| :attr:`ENV.LOG_TRACEBACK`    | ``ENV.LOG_TRACEBACK_KEY`` | Include tracebacks in error messages   |
+------------------------------+---------------------------+----------------------------------------+
| :attr:`ENV.LOG_DISABLED`     | ``ENV.LOG_DISABLED_KEY``  | Suppress all logging output            |
+------------------------------+---------------------------+----------------------------------------+
"""

@dataclass(frozen=True)
class _Log_:
    """Class containing log level constants for logging behavior. This
    class defines constants for the log levels TRACE, DEBUG, INFO,
    WARNING, ERROR, and CRITICAL, as well as the custom
    :attr:`TRACE_LEVEL` integer.
    
    Notes
    -----
    - Use the `LOG` instance of this class to access the log level 
    constants throughout the `yieldgraph` library.
    - Use :attr:`ENV.LOG_TRACEBACK` to check whether traceback output
    is enabled."""
    TRACE: Literal['TRACE'] = 'TRACE'
    """Log level for trace messages."""
    DEBUG: Literal['DEBUG'] = 'DEBUG'
    """Log level for debug messages."""
    INFO: Literal['INFO'] = 'INFO'
    """Log level for informational messages."""
    WARNING: Literal['WARNING'] = 'WARNING'
    """Log level for warning messages."""
    ERROR: Literal['ERROR'] = 'ERROR'
    """Log level for error messages."""
    CRITICAL: Literal['CRITICAL'] = 'CRITICAL'
    """Log level for critical messages."""
    TRACE_LEVEL: Literal[5] = 5
    """Custom log level number for TRACE level. This is set to 5, which is
    lower than the standard DEBUG level (10) to allow for more fine-grained
    logging. This log level can be used to log very detailed information 
    that is typically only useful for debugging specific issues."""
    DEBUG_LEVEL: Literal[10] = logging.DEBUG
    """Log level number for DEBUG level, set to the standard 
    `logging.DEBUG` value."""
    INFO_LEVEL: Literal[20] = logging.INFO
    """Log level number for INFO level, set to the standard
    `logging.INFO` value."""
    WARNING_LEVEL: Literal[30] = logging.WARNING
    """Log level number for WARNING level, set to the standard
    `logging.WARNING` value."""
    ERROR_LEVEL: Literal[40] = logging.ERROR
    """Log level number for ERROR level, set to the standard
    `logging.ERROR` value."""
    CRITICAL_LEVEL: Literal[50] = logging.CRITICAL
    """Log level number for CRITICAL level, set to the standard
    `logging.CRITICAL` value."""
    def __getitem__(self, key: str | int) -> int:
        """Allow dictionary-like access to log level numbers by name.
        For example, `LOG['DEBUG']` would return the integer value for 
        the DEBUG level.

        Parameters
        ----------
        key : str | int
            The log level name as a string (e.g., 'DEBUG') or the log level
            number as an integer (e.g., 10 for DEBUG).

        Returns
        -------
        int
            The corresponding log level integer.
        """
        if isinstance(key, int):
            return key
        
        key = key.upper()
        if not hasattr(self, f'{key}_LEVEL'):
            raise KeyError(f'Invalid log level: {key}')
        
        return getattr(self, f'{key}_LEVEL')
    
    def __call__(self, level: int | str) -> str:
        """Allow callable access to log level names by number. For 
        example, `LOG(10)` would return 'DEBUG'.

        Parameters
        ----------
        level : int | str
            The log level number as an integer (e.g., 10 for DEBUG) or 
            the log level name as a string (e.g., 'DEBUG').

        Returns
        -------
        str
            The corresponding log level name as a string.
        """

        if isinstance(level, int):
            for attr in dir(self):
                if attr.endswith('_LEVEL') and getattr(self, attr) == level:
                    return attr.split('_')[0]
            raise KeyError(f'Invalid log level number: {level}')

        level = level.upper()
        if not hasattr(self, level):
            raise KeyError(f'Invalid log level name: {level}')
        return level

LOG = _Log_()
"""Instance of the `_Log_` class containing log level constants and
configuration for logging behavior.

This instance can be used throughout the `yieldgraph` library to access
log level constants (e.g., `LOG.DEBUG`, `LOG.INFO`, etc.). Use
:attr:`ENV.LOG_TRACEBACK` to check whether traceback information should
be included in log output.

The `LoggingBehavior` mixin class uses the `LOG` instance to determine
log levels when logging messages. This allows for consistent log level
references across the library and makes it easy to change log level 
values in one place if needed.

To access log level numbers by name, you can use dictionary-like access 
(e.g., `LOG['DEBUG']`), and to access log level names by number, you can 
call the instance (e.g., `LOG(10)` returns 'DEBUG').
"""

try:
    from loguru import logger # pyright: ignore[reportMissingImports]
    from logging import Logger
    
except ImportError:
    from logging import Logger
    logging.addLevelName(LOG.TRACE_LEVEL, 'TRACE')
    logger = logging.getLogger('yieldgraph')

class LoggingBehavior:
    """Mixin class that provides logging capabilities to classes that 
    inherit from it. 

    This class defines a :attr:`logger` attribute that can be used to 
    log messages. It also provides a :attr:`log_title` property that can 
    be used to customize the log title for the class. The :attr:`log` 
    method is used to log messages with a specified log level, and there 
    are convenience methods for logging at specific levels 
    (e.g., :attr:`log_info`, :attr:`log_warning`, etc.).
    
    To use this mixin, simply inherit from it in your class and use the
    logging methods as needed. You can also override the 
    :attr:`log_title` property to provide a custom log title. If you do 
    not override the :attr:`log_title` property, it will default to the
    class name, which will be included in the log messages.
    
    Examples
    --------
    ```python
    class MyClass(LoggingBehavior):
        def do_something(self):
            self.log_info('Doing something...')
    my_instance = MyClass()
    my_instance.do_something()
    # Output: MyClass: Doing something... (logged at INFO level)
    ```
    """
    
    logger: Logger = logger
    """Logger instance to be used for logging. By default, it uses the
    logger from the loguru library if available, otherwise it uses the
    standard logging library's logger."""

    @property
    def log_title(self) -> str:
        """Get log title for class (read-only).
        
        By default, it returns the class name. You can override this
        property in your class to provide a custom log title."""
        return self.__class__.__name__

    def log(
            self,
            message: str,
            level: int | str,
            exception: Optional[Exception] = None) -> None:
        """Log a message with the given log level.
        
        Does nothing if :attr:`ENV.LOG_DISABLED` is ``True``
        (i.e. ``YIELDGRAPH_LOG_DISABLED`` is set to a truthy value).

        This method uses the :attr:`log_title` property to include the 
        class name in the log message. The log level can be specified as 
        an integer or a string. If the log level is a string, it is 
        converted to the corresponding integer value.
        
        Parameters
        ----------
        message : str
            The message to log.
        level : int | str
            The log level, either as an integer or a string.
        exception : Optional[Exception], optional
            An optional exception to include in the log message. If 
            provided, it will be included in the log output."""
        if ENV.LOG_DISABLED:
            return
        
        if self.log_title:
            message = f'{self.log_title}: {message}'
        try:
            (self.logger
            .opt(depth=1, exception=exception) # pyright: ignore[reportAttributeAccessIssue]
            .log(level, message))
        except AttributeError:
            self.logger.log(LOG[level], message)
    
    def log_exception(self, message: str, exception: Exception) -> None:
        """Log an exception with the given message.
        
        This method uses the :attr:`log_title` property to include the 
        class name in the log message. The exception is included in the 
        log message as well.
        
        Parameters
        ----------
        message : str
            The message to log.
        exception : Exception
            The exception to log."""
        self.log(message, LOG.ERROR, exception)
    
    def log_trace(self, message: str) -> None:
        """Log a trace message with the given message.
        
        This method uses the :attr:`log_title` property to include the 
        class name in the log message. The log level is set to TRACE.
        
        Parameters
        ----------
        message : str
            The message to log."""
        self.log(message, LOG.TRACE)

    def log_debug(self, message: str) -> None:
        """Log a debug message with the given message.
        
        This method uses the :attr:`log_title` property to include the 
        class name in the log message. The log level is set to DEBUG.
        
        Parameters
        ----------
        message : str
            The message to log."""
        self.log(message, LOG.DEBUG)
    
    def log_info(self, message: str) -> None:
        """Log an info message with the given message.
        
        This method uses the :attr:`log_title` property to include the 
        class name in the log message. The log level is set to INFO.
        
        Parameters
        ----------
        message : str
            The message to log."""
        self.log(message, LOG.INFO)
    
    def log_warning(self, message: str) -> None:
        """Log a warning message with the given message.
        
        This method uses the :attr:`log_title` property to include the 
        class name in the log message. The log level is set to WARNING.
        
        Parameters
        ----------
        message : str
            The message to log."""
        self.log(message, LOG.WARNING)
    
    def log_error(self, message: str) -> None:
        """Log an error message with the given message.
        
        This method uses the :attr:`log_title` property to include the 
        class name in the log message. The log level is set to ERROR.
        
        Parameters
        ----------
        message : str
            The message to log."""
        self.log(message, LOG.ERROR)
    
    def log_critical(self, message: str) -> None:
        """Log a critical message with the given message.
        
        This method uses the :attr:`log_title` property to include the 
        class name in the log message. The log level is set to CRITICAL.
        
        Parameters
        ----------
        message : str
            The message to log."""
        self.log(message, LOG.CRITICAL)
    