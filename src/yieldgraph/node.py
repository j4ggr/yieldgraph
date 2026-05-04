"""Pipeline node that wraps a :class:`~yieldgraph.job.Job` and manages
its input/output edges inside a :class:`~yieldgraph.graph.Graph`.

Each :class:`Node` pulls items from one or more incoming :class:`~yieldgraph.edge.Edge`
queues, feeds them one at a time into its :class:`~yieldgraph.job.Job`, and
fans every yielded result out to all outgoing edges.  Error handling,
progress tracking, and cancellation are all managed here so that
:class:`~yieldgraph.graph.Graph` only needs to iterate over nodes in order.

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

    - pulling items from its incoming :class:`~yieldgraph.edge.Edge` queues,
    - feeding each item as ``*args`` into the job,
    - fanning every yielded result to all outgoing edges,
    - tracking counts (``count_in``, ``count_out``) and caught errors,
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
        Name of the upstream node (or :data:`~yieldgraph.config.START_NODE_NAME`
        for the first node) whose edges feed this node.
    label : str, optional
        Human-readable display name passed through to the underlying
        :class:`~yieldgraph.job.Job`.
    first : bool, optional
        Mark this node as the first in its chain, by default ``True``.
    last : bool, optional
        Mark this node as the last in its chain, by default ``False``.

    Attributes
    ----------
    name : str
        Function name of the wrapped job.
    job : Job
        The underlying :class:`~yieldgraph.job.Job` instance.
    inputs_from : str
        Name of the upstream node that feeds this node.
    first : bool
        ``True`` for the first node of a chain.
    last : bool
        ``True`` for the last node of a chain.
    count_in : int
        Number of input items consumed so far in the current run.
    count_out : int
        Number of output items produced so far in the current run.
    errors : list[Exception]
        Exceptions caught during :meth:`do_job` in the current run.
    """

    name: str
    job: Job
    inputs_from: str
    first: bool
    last: bool

    repr_len: int = 25
    """Column width used when formatting :meth:`__repr__` output."""

    n_jobs_todo: int
    """Total number of input items queued at the start of :meth:`loop`."""

    count_in: int
    count_out: int
    errors: List[Exception]

    _inputs: List[Edge]
    _inputs_idx: int
    outputs: List[Edge]

    additional_info: str
    """Free-form string shown in progress displays."""

    last_output: Tuple[Any, ...]
    """Most recent output tuple produced by the job."""

    is_first_job: bool
    """``True`` while processing the first item in :meth:`loop`."""

    is_last_job: bool
    """``True`` while processing the last item in :meth:`loop`."""

    def __init__(
            self,
            graph,
            job_function: Callable,
            inputs_from: str,
            label: str = '',
            first: bool = True,
            last: bool = False,
            ) -> None:
        self.graph = graph
        self.job = Job(job_function, label)
        self.name = self.job.name
        self.inputs_from = inputs_from
        self.first = first
        self.last = last
        self.preset()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def count_err(self) -> int:
        """Number of errors caught so far in the current run (read-only)."""
        return len(self.errors)

    @property
    def inputs(self) -> Edge:
        """Active input edge selected by :attr:`_inputs_idx` (read-only getter).

        The setter accepts a list of edges and automatically finds the
        first non-empty one, storing its index in ``_inputs_idx``.
        """
        if not self._inputs:
            return Edge()
        return self._inputs[self._inputs_idx]

    @inputs.setter
    def inputs(self, edges: List[Edge]) -> None:
        self._inputs = edges
        self.log(f'Inputs changed, n inputs = {len(edges)}', LOG.TRACE)
        if self._inputs:
            for idx, edge in enumerate(self._inputs):
                if len(edge):
                    self._inputs_idx = idx
                    break

    @property
    def n_inputs(self) -> int:
        """Number of items currently available on the active input edge (read-only)."""
        if type(self.inputs) is tuple and self.inputs:
            return 1
        return len(self.inputs)

    @property
    def n_outputs(self) -> int:
        """Number of items on the first output edge (read-only)."""
        if type(self.outputs) is tuple and self.outputs:
            return 1
        return len(self.outputs)

    @property
    def no_outputs(self) -> bool:
        """``True`` if all output edges are empty (read-only)."""
        if not self.outputs:
            return True
        return not any(bool(len(edge)) for edge in self.outputs)

    @property
    def progress(self) -> float:
        """Fraction of input items processed, clamped to ``[0.01, 1.0]`` (read-only).

        Returns ``0.5`` when no items were queued (e.g. a source node
        that generates its own data).
        """
        p = 0.5 if self.n_jobs_todo == 0 else self.count_in / self.n_jobs_todo
        return max(0.01, min(p, 1.0))

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def preset(self) -> None:
        """Reset all counters, errors, and edge references for a new run.

        Called automatically from :meth:`__init__` and by
        :class:`~yieldgraph.graph.Graph` before each run.
        """
        self.n_jobs_todo = 0
        self.count_in = 0
        self.count_out = 0
        self.errors = []
        self._inputs = []
        self._inputs_idx = 0
        self.outputs = []
        self.additional_info = ''
        self.is_first_job = False
        self.is_last_job = False

    def loop(self, edges_in: List[Edge], edges_out: List[Edge]) -> None:
        """Consume all items from *edges_in* and push results to *edges_out*.

        Iterates over every queued input item and calls :meth:`do_job`
        for each one.  Sets :attr:`is_first_job` and :attr:`is_last_job`
        flags so the job function can branch on position if needed.

        Parameters
        ----------
        edges_in : list[Edge]
            Incoming edge queues to drain.
        edges_out : list[Edge]
            Outgoing edge queues to fill with results.
        """
        self.log(
            f'{self.n_inputs} jobs for node "{self.job.label}"',
            LOG.INFO)
        self.inputs = edges_in
        self.outputs = edges_out
        self.n_jobs_todo = deepcopy(self.n_inputs)
        self.job.cancelled = False
        for job_count in range(1, self.n_jobs_todo + 1):
            self.is_first_job = job_count == 1
            self.is_last_job = job_count == self.n_jobs_todo
            self.do_job(self.inputs.popleft())

    def do_job(self, job_data: Tuple[Any, ...]) -> None:
        """Run the job for a single input item and collect its outputs.

        Unpacks *job_data* as positional arguments into the job generator.
        Each yielded value is normalised to a tuple via :func:`_ensure_tuple`
        and pushed to all outgoing edges via :meth:`_store_output`.

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
            if self.graph.cancel:
                self.log('Skip job because of graph cancel', LOG.TRACE)
                return

            for output in self.job(*job_data):
                if self.graph.cancel:
                    self.job.cancelled = True
                    break

                self._store_output(_ensure_tuple(output))

        except KeyboardInterrupt as e:
            self.graph.cancel = True
            self.log(f'{self} interrupted because: {e}', LOG.INFO)

        except Exception as e:
            self.log(
                f'Caught error = {e}\nError occurred @ node {repr(self)}',
                LOG.WARNING)
            if LOG.TRACEBACK:
                self.log(traceback.format_exc(), LOG.ERROR)
            self.errors.append(e)

        finally:
            self.count_in += 1

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _store_output(self, output: Tuple[Any, ...]) -> None:
        """Append *output* to every outgoing edge and increment :attr:`count_out`.

        Parameters
        ----------
        output : tuple
            The normalised output tuple to fan out.
        """
        self.last_output = output
        for edge in self.outputs:
            edge.append(output)
        self.log(f'Stored output @ node {self}', LOG.TRACE)
        self.count_out += 1

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
        return self.n_inputs

    def __str__(self) -> str:
        return f'{self.name} ({self.inputs_from})'

    def __repr__(self) -> str:
        name = self.name.ljust(self.repr_len + 1)
        r = f'  |-{name}|{self.count_in: 4}|{self.count_out: 4}|{self.count_err: 4}|'
        if self.first:
            r = (
                f'{self.inputs_from.ljust(self.repr_len + 6)}'
                + ' '.join(s.ljust(4) for s in ('in', 'out', 'err'))
                + f'\n{r}'
            )
        return r


__all__ = ['Node', 'START_NODE_NAME']

