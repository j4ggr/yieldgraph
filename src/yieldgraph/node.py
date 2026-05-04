"""Pipeline node that wraps a :class:`~yieldgraph.job.Job` and manages
its input/output edges inside a :class:`~yieldgraph.graph.Graph`.

Each :class:`Node` pulls items from one or more incoming 
:class:`~yieldgraph.edge.Edge`  ueues, feeds them one at a time into its 
:class:`~yieldgraph.job.Job`, and fans every yielded result out to all
outgoing edges.  Error handling, progress tracking, and cancellation are
all managed here so that :class:`~yieldgraph.graph.Graph` only needs to 
iterate over nodes in order.

The module-level helper :func:`_ensure_tuple` is kept separate from the
class so it can be used and tested independently.

Typical usage
-------------

```python
from yieldgraph import Graph

def extract(graph):
    for row in graph.output:
        yield row

def transform(row):
    yield row.upper()

g = Graph()
g.add_chain(extract, transform)
g.run()
```
"""

import traceback

from copy import deepcopy

from typing import Any
from typing import List
from typing import Tuple
from typing import Callable

from .config import LOG
from .config import LoggingBehavior
from .config import START_NODE_NAME
from .edge import Edge
from .job import Job


def _ensure_tuple(value: Any) -> Tuple[Any, ...]:
    """Wrap *value* in a one-element tuple if it is not already a tuple.

    Generator functions in a pipeline may yield either a bare value or a
    tuple.  This helper normalises both cases so downstream nodes always
    receive a tuple they can unpack as ``*job_data``.

    Parameters
    ----------
    value : Any
        The value to normalise.

    Returns
    -------
    tuple
        *value* unchanged if it is already a ``tuple``, otherwise
        ``(value,)``.

    Examples
    --------
    ```python
    assert _ensure_tuple(42) == (42,)
    assert _ensure_tuple((1, 2)) == (1, 2)
    assert _ensure_tuple('hello') == ('hello',)
    ```
    """
    return value if type(value) is tuple else (value,)


class Node(LoggingBehavior):
    """A single processing step in an ETL pipeline graph.

    Each :class:`Node` wraps one :class:`~yieldgraph.job.Job` and is
    responsible for:

    - pulling items from its incoming :class:`~yieldgraph.edge.Edge` 
      queues,
    - feeding each item as ``*args`` into the job,
    - fanning every yielded result to all outgoing edges,
    - tracking counts (``n_consumed``, ``n_produced``) and caught
      errors,
    - cooperating with the owning :class:`~yieldgraph.graph.Graph` on
      cancellation.

    Parameters
    ----------
    graph : Graph
        The owning graph.  The node reads ``graph.cancel`` to decide
        whether to skip or abort jobs.
    job_function : Callable
        The callable that does the actual work.  May be a plain function
        or a generator function.
    inputs_from : str
        Name of the upstream node (or 
        :data:`~yieldgraph.config.START_NODE_NAME` for the first node) 
        whose edges feed this node.
    label : str, optional
        Human-readable display name passed through to the underlying
        :class:`~yieldgraph.job.Job`.
    first : bool, optional
        Mark this node as the first in its chain, by default ``True``.
    last : bool, optional
        Mark this node as the last in its chain, by default ``False``.

    Attributes
    ----------
    inputs_from : str
        Name of the upstream node that feeds this node.
    first : bool
        ``True`` for the first node of a chain.
    last : bool
        ``True`` for the last node of a chain.
    n_consumed : int
        Number of input items consumed so far in the current run.
    n_produced : int
        Number of output items produced so far in the current run.
    errors : list[Exception]
        Exceptions caught during :meth:`_run_one` in the current run.
    info : str
        Free-form string available for progress displays or annotations.
    """

    # --- private state ------------------------------------------------

    _graph: Any
    """Owning :class:`~yieldgraph.graph.Graph`; read for ``cancel`` 
    flag."""

    _job: Job
    """Wrapped :class:`~yieldgraph.job.Job` instance."""

    _inputs: List[Edge]
    """List of all incoming edge queues."""

    _active_edge_index: int
    """Index into :attr:`_inputs` pointing at the first non-empty edge."""

    _col_width: int
    """Column width used to align :meth:`__repr__` output across nodes."""

    _last_output: Tuple[Any, ...]
    """Most recent output tuple produced by the job."""

    _processing_first: bool
    """``True`` while :meth:`_run_one` is handling the first queued 
    item."""

    _processing_last: bool
    """``True`` while :meth:`_run_one` is handling the last queued item."""

    # --- public state -------------------------------------------------

    inputs_from: str
    """Name of the upstream node that feeds this node."""

    first: bool
    """``True`` for the first node of a chain."""
    
    last: bool
    """``True`` for the last node of a chain."""

    n_queued: int
    """Total number of input items queued at the start of 
    :meth:`process`."""

    n_consumed: int
    """Number of input items pulled from the active edge so far."""

    n_produced: int
    """Number of output tuples pushed to outgoing edges so far."""

    errors: List[Exception]
    """List of exceptions caught during :meth:`_run_one` in the current 
    run."""

    outputs: List[Edge]
    """List of outgoing edges."""

    info: str
    """Free-form string available for progress displays or annotations."""

    # --- class-level default ------------------------------------------

    _col_width: int = 25

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
            self,
            graph,
            job_function: Callable,
            inputs_from: str,
            label: str = '',
            first: bool = True,
            last: bool = False,
            ) -> None:
        self._graph = graph
        self._job = Job(job_function, label)
        self.inputs_from = inputs_from
        self.first = first
        self.last = last
        self.reset()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """Function name of the wrapped job (read-only).

        Derived from ``job_function.__name__`` at construction time.
        """
        return self._job.name

    @property
    def n_errors(self) -> int:
        """Number of exceptions caught so far in the current run 
        (read-only)."""
        return len(self.errors)

    @property
    def inputs(self) -> Edge:
        """Active input edge (read-only getter).

        Returns the first non-empty edge from :attr:`_inputs`, as
        tracked by :attr:`_active_edge_index`.  Returns an empty
        :class:`~yieldgraph.edge.Edge` when no inputs have been set.

        The *setter* accepts a list of edges and automatically selects
        the first non-empty one.
        """
        if not self._inputs:
            return Edge()
        return self._inputs[self._active_edge_index]

    @inputs.setter
    def inputs(self, edges: List[Edge]) -> None:
        self._inputs = edges
        self.log_trace(f'Inputs changed, n inputs = {len(edges)}')
        if self._inputs:
            for idx, edge in enumerate(self._inputs):
                if len(edge):
                    self._active_edge_index = idx
                    break

    @property
    def input_count(self) -> int:
        """Number of items available on the active input edge 
        (read-only)."""
        if type(self.inputs) is tuple and self.inputs:
            return 1
        return len(self.inputs)

    @property
    def output_count(self) -> int:
        """Number of items sitting on the first output edge (read-only)."""
        if type(self.outputs) is tuple and self.outputs:
            return 1
        return len(self.outputs)

    @property
    def outputs_empty(self) -> bool:
        """``True`` if every outgoing edge is empty (read-only)."""
        if not self.outputs:
            return True
        return not any(bool(len(edge)) for edge in self.outputs)

    @property
    def progress(self) -> float:
        """Fraction of queued items processed, clamped to 
        ``[0.01, 1.0]`` (read-only).

        Returns ``0.5`` when :attr:`n_queued` is zero (e.g. a source
        node that generates its own data rather than receiving inputs).
        """
        p = 0.5 if self.n_queued == 0 else self.n_consumed / self.n_queued
        return max(0.01, min(p, 1.0))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Clear all counters, errors, and edge references for a new 
        run.

        Called automatically from :meth:`__init__` and by
        :class:`~yieldgraph.graph.Graph` before each run.
        """
        self.n_queued = 0
        self.n_consumed = 0
        self.n_produced = 0
        self.errors = []
        self._inputs = []
        self._active_edge_index = 0
        self.outputs = []
        self.info = ''
        self._processing_first = False
        self._processing_last = False

    def process(self, edges_in: List[Edge], edges_out: List[Edge]) -> None:
        """Consume all items from *edges_in* and push results to 
        *edges_out*.

        Iterates over every queued input item and calls :meth:`_run_one`
        for each one.  Sets :attr:`_processing_first` and
        :attr:`_processing_last` flags so the job function can branch on
        position if needed.

        Parameters
        ----------
        edges_in : list[Edge]
            Incoming edge queues to drain.
        edges_out : list[Edge]
            Outgoing edge queues to fill with results.
        """
        self.log_info(f'{self.input_count} jobs for node "{self._job.label}"')
        self.inputs = edges_in
        self.outputs = edges_out
        self.n_queued = deepcopy(self.input_count)
        self._job.cancelled = False
        for job_count in range(1, self.n_queued + 1):
            self._processing_first = job_count == 1
            self._processing_last = job_count == self.n_queued
            self._run_one(self.inputs.popleft())

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_one(self, job_data: Tuple[Any, ...]) -> None:
        """Run the job for a single input item and collect its outputs.

        Unpacks *job_data* as positional arguments into the job generator.
        Each yielded value is normalised to a tuple via 
        :func:`_ensure_tuple` and pushed to all outgoing edges via 
        :meth:`_fan_out`.

        Cancellation is checked after every yielded value.  A
        :exc:`KeyboardInterrupt` sets ``graph.cancel`` so all subsequent
        nodes are skipped.  Any other exception is caught, logged, and
        appended to :attr:`errors` so the pipeline continues.

        Parameters
        ----------
        job_data : tuple
            Arguments to unpack into the job callable.
        """
        try:
            if self._graph.cancelled:
                self.log_trace('Skip job because of graph cancel')
                return

            for output in self._job(*job_data):
                if self._graph.cancelled:
                    self._job.cancelled = True
                    break

                self._fan_out(_ensure_tuple(output))

        except KeyboardInterrupt as e:
            self._graph.cancelled = True
            self.log_info(f'{self} interrupted because: {e}')

        except Exception as e:
            self.log_warning(
                f'Caught error = {e}\nError occurred @ node {repr(self)}')
            if LOG.TRACEBACK:
                self.log_exception(traceback.format_exc(), e)
            self.errors.append(e)

        finally:
            self.n_consumed += 1

    def _fan_out(self, output: Tuple[Any, ...]) -> None:
        """Push *output* to every outgoing edge and increment 
        :attr:`n_produced`.

        Uses :meth:`~yieldgraph.edge.Edge.put` which is thread-safe and
        wakes up any consumer blocked in
        :meth:`~yieldgraph.edge.Edge.get`. Works correctly in sequential 
        mode too (put is equivalent to append plus a no-op notification 
        when no consumer is waiting).

        Parameters
        ----------
        output : tuple
            The normalised output tuple to fan out to all outgoing edges.
        """
        self._last_output = output
        for edge in self.outputs:
            edge.put(output)
        self.log_trace(f'Stored output @ node {self}')
        self.n_produced += 1

    def process_streaming(self, edges_in: List[Edge], edges_out: List[Edge]) -> None:
        """Consume items from *edges_in* using blocking 
        :meth:`~yieldgraph.edge.Edge.get` calls.

        Used in threaded execution mode where items arrive incrementally
        from a concurrently-running producer node.  Loops until every 
        input edge is :attr:`~yieldgraph.edge.Edge.closed` and empty, or 
        until the owning graph signals cancellation.

        Each input edge is drained fully before the next one is started
        (same ordering as the sequential :meth:`process`).
        :attr:`_processing_first` is set for the very first item;
        :attr:`_processing_last` is always ``False`` because the end of
        the stream is not known in advance.

        Parameters
        ----------
        edges_in : list[Edge]
            Incoming edge queues to read from (blocking).
        edges_out : list[Edge]
            Outgoing edge queues to push results into.

        Notes
        -----
        For fan-out pipelines with multiple chains attached to the same
        node each input edge is consumed by exactly one downstream node
        in sequential mode (first-non-empty assignment).  In threaded
        mode the caller (:meth:`~yieldgraph.graph.Graph._run_threaded`)
        is responsible for passing the correct subset of *edges_in* to
        each node.
        """
        self.inputs = edges_in
        self.outputs = edges_out
        self._job.cancelled = False
        first_item = True

        for edge in edges_in:
            while True:
                if self._graph.cancelled:
                    self._job.cancelled = True
                    return

                item = edge.get(timeout=0.05)

                if item is None:
                    if edge.closed:
                        break        # this edge exhausted — move to next
                    continue         # timeout — loop back to check cancel

                self._processing_first = first_item
                self._processing_last = False
                first_item = False
                self._run_one(item)

    # ------------------------------------------------------------------
    # Dunder methods
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        if self.first:
            inputs = self.inputs
            if not isinstance(inputs, tuple):
                inputs = (inputs,)
            try:
                return len(inputs[0])
            except TypeError:
                return 0
        return self.input_count

    def __str__(self) -> str:
        return f'{self.name} ({self.inputs_from})'

    def __repr__(self) -> str:
        name = self.name.ljust(self._col_width + 1)
        r = f'  |-{name}|{self.n_consumed: 4}|{self.n_produced: 4}|{self.n_errors: 4}|'
        if self.first:
            r = (
                f'{self.inputs_from.ljust(self._col_width + 6)}'
                + ' '.join(s.ljust(4) for s in ('in', 'out', 'err'))
                + f'\n{r}'
            )
        return r


__all__ = ['Node', 'START_NODE_NAME', '_ensure_tuple']
