"""Interruptible job wrapper for generator-driven pipelines.

This module provides the building blocks for wrapping ordinary callables
into cancellable, generator-based execution units used by :class:`Node`
inside a :class:`Graph`.

The two module-level helpers are kept separate from :class:`Job` so they
can be used and tested independently:

- :func:`_as_generator` — normalises any callable into a generator
  function.
- :func:`_wrap` — decorates a generator function with a cancellation
  check between every yielded value.

Typical usage
-------------
Plain function (single result):

```python
def double(x):
    return x * 2

job = Job(double)
results = list(job(21))   # → [42]
```

Generator function (multiple results):

```python
def count_up(start, stop):
    for i in range(start, stop):
        yield i

job = Job(count_up, label='Count Up')
results = list(job(0, 5))  # → [0, 1, 2, 3, 4]
```

Early cancellation:

```python
job = Job(count_up)
gen = job(0, 1_000_000)
results = []
for value in gen:
    results.append(value)
    if value >= 2:
        job.cancelled = True   # runner exits after the next yield
```
"""

import inspect

from typing import Any
from typing import Callable
from typing import Generator

from .config import LoggingBehavior


def _as_generator(fn: Callable) -> Callable:
    """Normalise *fn* into a generator function.

    Generator functions are returned unchanged. Regular functions are
    wrapped so their single return value is yielded once, making them
    compatible with the generator-driven pipeline protocol.

    Parameters
    ----------
    fn : Callable
        Any callable — a plain function, a lambda, or an already-defined
        generator function.

    Returns
    -------
    Callable
        A generator function whose signature mirrors *fn*.

    Examples
    --------
    Wrapping a plain function:

    ```python
    gen_fn = _as_generator(lambda x: x * 2)
    assert list(gen_fn(3)) == [6]
    ```

    Passing through an existing generator:

    ```python
    def gen(n):
        yield from range(n)

    assert _as_generator(gen) is gen   # same object, no wrapping
    ```
    """
    if inspect.isgeneratorfunction(fn):
        return fn

    def wrapper(*args: Any) -> Generator[Any, Any, None]:
        yield fn(*args)

    return wrapper


def _wrap(fn: Callable, job: 'Job') -> Callable:
    """Decorate *fn* with a per-yield cancellation check.

    The returned *runner* drives the generator loop produced by
    :func:`_as_generator`.  After every yielded value it checks
    ``job.cancelled``; if the flag is set the loop exits immediately and
    no further values are produced.  :attr:`Job._running` is kept in
    sync: it is set to ``True`` when the runner starts and to ``False``
    when it exits — either normally or via cancellation.

    Parameters
    ----------
    fn : Callable
        The function to wrap; passed through :func:`_as_generator`
        internally, so plain functions are accepted too.
    job : Job
        The :class:`Job` instance that owns this runner.  Its
        ``cancelled`` and ``_running`` attributes are read/written by the
        runner.

    Returns
    -------
    Callable
        A generator function with the same positional/keyword signature
        as *fn*.

    Examples
    --------
    Normal completion:

    ```python
    job = Job(lambda: None)   # _wrap called internally
    results = list(job())     # → [None]
    assert not job.running
    ```

    Cancellation mid-stream:

    ```python
    def slow_gen():
        for i in range(100):
            yield i

    job = Job(slow_gen)
    gen = job()
    collected = []
    for v in gen:
        collected.append(v)
        if v == 2:
            job.cancelled = True

    assert collected == [0, 1, 2]
    assert not job.running
    ```
    """
    def runner(*args: Any, **kwargs: Any) -> Generator[Any, Any, None]:
        job._running = True
        for result in _as_generator(fn)(*args, **kwargs):
            if job.cancelled:
                job._running = False
                break
            yield result
        job._running = False

    return runner


class Job(LoggingBehavior):
    """Wraps a callable for interruptible, generator-driven execution.

    :class:`Job` is the execution unit used by :class:`~yieldgraph.node.Node`.
    It normalises any callable into a generator, adds a per-yield
    cancellation gate, and exposes a simple ``cancelled`` flag that
    external code (e.g. the owning :class:`~yieldgraph.graph.Graph`) can
    set to stop a long-running job gracefully.

    Parameters
    ----------
    function : Callable
        The callable to wrap.  May be a plain function or a generator
        function.
    label : str, optional
        Human-readable display name.  When omitted the label is derived
        automatically from *function.__name__* by splitting on ``_`` and
        uppercasing each part (e.g. ``"load_data"`` → ``"LOAD DATA"``).

    Attributes
    ----------
    name : str
        Raw function name (``function.__name__``).
    cancelled : bool
        Set to ``True`` to request early termination.  The runner checks
        this flag after every yielded value and exits on the next
        iteration.  Reset to ``False`` automatically on each new
        :meth:`__call__`.

    Examples
    --------
    Wrapping a plain function:

    ```python
    job = Job(lambda x: x ** 2)
    assert list(job(4)) == [16]
    ```

    Wrapping a generator:

    ```python
    def letters():
        yield from 'abc'

    job = Job(letters, label='Letters')
    assert list(job()) == ['a', 'b', 'c']
    assert job.label == 'Letters'
    ```

    Auto-derived label:

    ```python
    job = Job(lambda: None)
    job.name = 'extract_raw_data'    # simulate a named function
    assert job.label == 'EXTRACT RAW DATA'  # after label is cleared
    ```
    """

    name: str
    """Raw name of the wrapped function (``function.__name__``)."""

    cancelled: bool = False
    """Cancellation request flag.

    Set to ``True`` from outside the generator loop (e.g. from the
    owning graph) to stop iteration after the current yield.  Cleared
    automatically at the start of every new call so that a :class:`Job`
    instance can be reused across pipeline runs.
    """

    _running: bool = False

    @property
    def running(self) -> bool:
        """``True`` while the generator is actively yielding (read-only).

        Managed internally by :func:`_wrap`: set to ``True`` when the
        runner starts and to ``False`` when it finishes or is cancelled.
        Safe to poll from another thread to check job status.
        """
        return self._running
    
    _label: str = ''
    """Internal storage for the human-readable label.  If empty, the 
    label is derived lazily from :attr:`name` by splitting on ``_`` and
    uppercasing each token.
    """

    @property
    def label(self) -> str:
        """Human-readable display name for this job (read-only).

        Returns the value passed as *label* to :meth:`__init__`.  If
        none was given, the label is derived lazily from :attr:`name` by
        splitting on ``_`` and uppercasing each token:

        ```python
        job = Job(load_raw_data)
        job.label   # → 'LOAD RAW DATA'
        ```
        """
        if not self._label:
            self._label = ' '.join(s.upper() for s in self.name.split('_'))
        return self._label

    def __init__(self, function: Callable, label: str = '') -> None:
        self.name = function.__name__
        self._label = label
        self._wrapped = _wrap(function, self)

    def __call__(self, *args: Any, **kwargs: Any) -> Generator[Any, Any, None]:
        """Execute the wrapped function and return its generator.

        Resets :attr:`cancelled` to ``False`` before each call so that a
        :class:`Job` instance can be reused safely across multiple
        pipeline runs without manually clearing the flag.

        Parameters
        ----------
        *args, **kwargs
            Forwarded verbatim to the wrapped function.

        Returns
        -------
        Generator
            A lazy generator that yields the results of the wrapped
            function one at a time.
        """
        self.cancelled = False
        return self._wrapped(*args, **kwargs)

    def __repr__(self) -> str:
        return f'{self.name} ({self.label})'


__all__ = ['Job', '_as_generator', '_wrap']
