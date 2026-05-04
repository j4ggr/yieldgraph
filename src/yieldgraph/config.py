"""Configuration module for the `yieldgraph` library.

This module defines constants and classes related to logging behavior in 
the `yieldgraph` library. It includes the `LOG` instance of the `_Log_` 
class, which contains log level constants and configuration for logging 
behavior, as well as the :class:`LoggingBehavior` mixin class that 
provides logging capabilities to classes that inherit from it.

The `LoggingBehavior` class defines a :attr:`logger` attribute that can 
be used by classes that inherit from it to log messages at various log 
levels. The log levels are defined in the `LOG` instance, and include 
TRACE, DEBUG, INFO, WARNING, ERROR, and CRITICAL. The `LOG` instance 
also contains a flag for whether to include traceback information in log
messages. This can be set using the `YIELDGRAPH_LOG_TRACEBACK` 
environment variable.

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

from typing import Literal
from typing import Optional
from logging import _nameToLevel
from dataclasses import dataclass


__all__ = [
    'LOG',
    'LoggingBehavior',
    'START_NODE_NAME',
    'THREADED_ENV_VAR',
]


START_NODE_NAME = '__START__'
"""Sentinel name for the implicit start node that seeds pipeline chains."""

THREADED_ENV_VAR = 'YIELDGRAPH_THREADED'
"""Name of the environment variable that enables threaded execution.

Set this variable to ``'1'``, ``'true'``, or ``'yes'`` (case-insensitive)
before running a :class:`~yieldgraph.graph.Graph` to execute all nodes
concurrently in separate threads instead of sequentially.

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

@dataclass(frozen=True)
class _Log_:
    """Class containing log level constants and configuration for 
    logging behavior. This class defines constants for various log 
    levels (e.g., TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL) and a 
    flag for whether to include traceback information in log messages.
    
    Notes
    -----
    - Use the `LOG` instance of this class to access the log level 
    constants and configuration for logging behavior throughout the 
    `yieldgraph` library."""
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
    TRACEBACK: bool = os.getenv(
        'YIELDGRAPH_LOG_TRACEBACK', 'False').lower() in ('true', '1', 't')
    """Flag indicating whether to include traceback information in log
    messages when an exception is logged. This can be set using the
    `YIELDGRAPH_LOG_TRACEBACK` environment variable. If the variable is 
    set to a truthy value (e.g., 'True', '1', 't'), traceback 
    information will be included in the log output when an exception is 
    logged. If the variable is not set or is set to a falsy value, 
    traceback information will not be included in the log output."""
    TRACE_LEVEL_NUM: Literal[5] = 5
    """Custom log level number for TRACE level. This is set to 5, which is
    lower than the standard DEBUG level (10) to allow for more fine-grained
    logging. This log level can be used to log very detailed information 
    that is typically only useful for debugging specific issues."""

LOG = _Log_()
"""Instance of the `_Log_` class containing log level constants and
configuration for logging behavior.

This instance can be used throughout the `yieldgraph` library to access
log level constants (e.g., `LOG.DEBUG`, `LOG.INFO`, etc.) and to check
the `LOG.TRACEBACK` flag when logging exceptions to determine whether
to include traceback information in the log output.
"""

try:
    from loguru import logger # pyright: ignore[reportMissingImports]
    from logging import Logger
    
except ImportError:
    from logging import Logger
    logging.addLevelName(LOG.TRACE_LEVEL_NUM, 'TRACE')
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
    
    @staticmethod
    def name_to_level(name: str | int) -> int:
        """Convert log level name to log level integer.
        
        This method takes a log level name as input and returns the 
        corresponding log level integer. If the log level name is not 
        recognized, it defaults to INFO level.
        
        Parameters
        ----------
        name : str | int
            The name or integer of the log level (e.g., 'TRACE', 
            'DEBUG', 'INFO',  'WARNING', 'ERROR', 'CRITICAL'). If an
            integer is provided, it checks if it is a valid log level
            integer and returns it. If a string is provided, it looks up
            the corresponding integer value.
            
        Returns
        -------
        int
            The corresponding log level integer."""
        if name == 'TRACE':
            return LOG.TRACE_LEVEL_NUM
        
        if isinstance(name, int) and name in _nameToLevel.values():
            return name
        
        if isinstance(name, str) and name in _nameToLevel:
            return _nameToLevel[name]
        
        return logging.INFO

    def log(
            self,
            message: str,
            level: int | str,
            exception: Optional[Exception] = None) -> None:
        """Log a message with the given log level.
        
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
        if self.log_title:
            message = f'{self.log_title}: {message}'
        try:
            (self.logger
            .opt(depth=1, exception=exception) # pyright: ignore[reportAttributeAccessIssue]
            .log(level, message))
        except AttributeError:
            self.logger.log(self.name_to_level(level), message)
    
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
    