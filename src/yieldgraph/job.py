import inspect

from typing import Any
from typing import Callable
from typing import Generator

from .config import LoggingBehavior


class Job(LoggingBehavior):
    """
    Represents a job function that can be executed in an interruptible 
    manner.
    
    The `Job` class wraps a given function and provides an interruptible 
    generator that can be used to execute the function. The `interrupt` 
    flag can be used to interrupt the execution of the generator.
    
    The `_ensure_generator_` method ensures that the given function is a 
    generator function. If it is not, it wraps the function in a 
    generator function that yields the result of the function.
    
    The `_interruptible_generator_` method wraps the generator function 
    in a loop that can be interrupted by setting the `interrupt` flag to 
    `True`.
    
    The `__call__` method returns the interruptible generator function, 
    which can be used to execute the job function."""

    name: str
    """Name of the given function."""

    interrupt: bool
    """Flag indicating to interrupt loop of generation process."""

    running: bool
    """Flag indicating if the job is currently running."""
    
    def __init__(
            self,
            function: Callable,
            designation: str = ''
            ) -> None:
        self.name = function.__name__
        self.interruptible_generator = self._interruptible_generator_(function)
        self._designation = designation
        self.running = False
    
    @property
    def designation(self) -> str:
        """Provided designation when initializing object. If no 
        designation is given, uses the uppercase function names splitted 
        by '_' and joint by ' '.(read-only)."""
        if not self._designation:
            self._designation = ' '.join(
                s.upper() for s in self.name.split('_'))
        return self._designation

    @staticmethod
    def _ensure_generator_(function: Callable) -> Callable:
        """Ensures given function is a generator function"""
        if inspect.isgeneratorfunction(function):
            return function
        
        def gen_function(*args) -> Generator[Any, Any, None]:
            result: Any = function(*args)
            yield result
        return gen_function

    def _interruptible_generator_(self, function: Callable) -> Callable:
        """Wraps the given generator function in a interruptable loop.
        The loop is interrupted if the flag `interrupt` is set to True."""
        def interruptible_generator(
                *args, **kwargs) -> Generator[Any, Any, None]:
            self.running = True
            for result in self._ensure_generator_(function)(*args, **kwargs):
                if self.interrupt:
                    self.running = False
                    break
                
                yield result
            self.running = False
        return interruptible_generator
    
    def __call__(self, *args, **kwargs) -> Generator[Any, Any, None]:
        self.interrupt = False
        return self.interruptible_generator(*args, **kwargs)
    
    def __repr__(self) -> str:
        return f'{self.name} ({self.designation})'


__all__ = ['Job']
