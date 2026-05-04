"""ETL pipeline graph that connects and runs a sequence of nodes.

A :class:`Graph` holds an ordered collection of
:class:`~yieldgraph.node.Node` objects connected by
:class:`~yieldgraph.edge.Edge` queues. Calling :meth:`Graph.run` 
(or the graph itself) drives each node in order: outputs of one node are
automatically routed to the inputs of the next.

Multiple independent chains can be attached to the same graph to create
fan-out or parallel branches.

Typical usage
-------------

```python
from yieldgraph import Graph

def source(graph):
    for item in [1, 2, 3]:
        yield item

def double(x):
    yield x * 2

g = Graph()
g.add_chain(source, double)
g.run()
print(g.output)   # → [(2,), (4,), (6,)]
```
"""

import os
import threading

from collections import defaultdict

from typing import Any
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import Tuple

from .config import LOG
from .config import LoggingBehavior
from .config import START_NODE_NAME
from .config import THREADED_ENV_VAR
from .edge import Edge
from .node import Node


class Graph(LoggingBehavior):
    """Directed graph of :class:`~yieldgraph.node.Node` objects forming 
    an ETL pipeline.

    Nodes are added via :meth:`add_chain` and executed in insertion
    order by :meth:`run`.  The graph manages all 
    :class:`~yieldgraph.edge.Edge` queues between nodes and exposes the
    final outputs through :attr:`output`.

    Attributes
    ----------
    nodes : dict[str, Node]
        Ordered mapping of node name → :class:`~yieldgraph.node.Node`.
    edges : defaultdict[str, list[Edge]]
        All edge queues keyed by the name of the *source* node (or
        :data:`~yieldgraph.config.START_NODE_NAME` for the pipeline 
        entry).
    terminal_nodes : list[str]
        Names of the last node in each chain; their edges feed 
        :attr:`output`.
    cancelled : bool
        Set to ``True`` to request cancellation of a running pipeline.
        Also set automatically on :exc:`KeyboardInterrupt`.
    finished : bool
        ``True`` once :meth:`run` has completed (successfully or not).
    error : str
        Non-empty string describing the first error that halted the run.
    labels : dict[str, str]
        Optional display labels keyed by node name, used by :attr:`step`.
    """

    # --- public state -------------------------------------------------

    nodes: Dict[str, Node]
    """Ordered mapping of node name → :class:`~yieldgraph.node.Node`.

    Nodes are inserted in the order :meth:`add_chain` is called and
    executed in that same order by :meth:`run`."""

    edges: DefaultDict[str, List[Edge]]
    """All edge queues, keyed by the name of the *source* node.

    The special key :data:`~yieldgraph.config.START_NODE_NAME` holds the
    seed edges for chains that do not attach to an existing node."""

    terminal_nodes: List[str]
    """Names of the last node in each chain.

    Their outgoing edges are collected into :attr:`output` after the
    run."""

    cancelled: bool
    """Set to ``True`` to request early termination of a running
    pipeline.

    Checked by each node before and after every yielded value.  Also set
    automatically when a :exc:`KeyboardInterrupt` is raised inside a
    node."""

    finished: bool
    """``True`` once :meth:`run` has returned, regardless of outcome."""

    error: str
    """Non-empty description of the first error that halted the run.

    Empty string when the run succeeded.  Check :attr:`succeeded` as a
    convenient boolean alternative."""

    labels: Dict[str, str]
    """Optional display labels keyed by node name.

    Used by :attr:`step` and :meth:`_node_label` to produce
    human-readable step names for progress displays.  If a node name is
    absent the label is derived automatically by uppercasing each 
    ``_``-separated token."""

    # --- private state ------------------------------------------------

    _output: List[Tuple[Any, ...]]
    """Cached flat list of all outputs collected from 
    :attr:`terminal_nodes`."""

    _current_node_name: str
    """Name of the node currently being processed by :meth:`run`."""

    _node_index: int
    """1-based index of the node currently being processed."""

    _threaded: bool
    """``True`` when the graph runs nodes as concurrent threads.

    Set automatically from the
    :data:`~yieldgraph.config.THREADED_ENV_VAR` environment variable at
    the start of each :meth:`run`. Override by setting the variable
    before the run::

        YIELDGRAPH_THREADED=1 python my_pipeline.py
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self) -> None:
        self.nodes = {}
        self.edges = defaultdict(list)
        self.terminal_nodes = []
        self._output = []
        self._current_node_name = ''
        self._node_index = 0
        self._threaded = False
        self.cancelled = False
        self.finished = False
        self.error = ''
        self.labels = {}

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def succeeded(self) -> bool:
        """``True`` if the run completed without errors (read-only).

        Both :attr:`finished` must be ``True`` and :attr:`error` must be
        empty for this to return ``True``.
        """
        return not bool(self.error) and self.finished

    @property
    def has_output(self) -> bool:
        """``True`` if the run finished and produced at least one output
        item (read-only)."""
        return bool(self._output) and self.finished

    @property
    def current_node(self) -> Node:
        """The :class:`~yieldgraph.node.Node` currently being executed
        (read-only)."""
        return self.nodes[self._current_node_name]

    @property
    def output(self) -> List[Tuple[Any, ...]]:
        """Flat list of all output tuples from the terminal nodes 
        (read-only).

        Collected lazily on first access after :meth:`run` completes.
        Each item is a tuple that was yielded by the last node of a 
        chain.

        ```python
        g.run()
        for row in g.output:
            print(row)
        ```
        """
        if self._output:
            return self._output

        for node_name in self.terminal_nodes:
            for edge in self.edges[node_name]:
                self._output.extend(list(edge))
        self.log(f'{len(self._output)} outputs found', LOG.TRACE)
        return self._output

    @property
    def at_first_node(self) -> bool:
        """``True`` when :attr:`current_node` is the very first node
        added to the graph (read-only)."""
        node_names = list(self.nodes.keys())
        if not node_names:
            return False
        return self._current_node_name == node_names[0]

    @property
    def step(self) -> str:
        """Human-readable label of the currently executing node
        (read-only).

        Returns an empty string when cancelled, ``'ETL Prozess Ende'``
        when finished, or the node label during execution.  Intended for 
        progress displays.
        """
        label = self._node_label(self._current_node_name)
        if self.cancelled:
            return ''
        if self.finished:
            return 'ETL Prozess Ende'
        return label

    @property
    def progress(self) -> int:
        """Progress of the currently executing node as an integer
        percentage ``[0, 100]`` (read-only)."""
        return int(100 * self.current_node.progress)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_chain(
            self,
            *job_functions: Callable,
            labels: Tuple[str, ...] = (),
            initial_input: Tuple[Any, ...] = (),
            attach_to: str = '') -> None:
        """Add an ordered sequence of callables to the graph as a chain.

        Each function's outputs are automatically routed as inputs to
        the next function in the sequence. If *attach_to* is given the
        chain is spliced in as a parallel branch starting from the named
        node's output edge.

        Parameters
        ----------
        *job_functions : Callable
            The callables that form the chain, in execution order.
        labels : tuple[str, ...], optional
            Human-readable display labels for each node, in the same 
            order as *job_functions*.  Defaults to auto-derived labels.
        initial_input : tuple, optional
            Seed arguments passed to the first node of a *new* chain 
            (i.e. when *attach_to* is not given). The graph instance is 
            prepended automatically if not already present.
        attach_to : str, optional
            Name of an existing node whose output edge this chain should
            tap into, creating a parallel branch.

        Notes
        -----
        The first function of a new (non-attached) chain receives the
        graph instance as its first argument so it can inspect
        :attr:`output`, react to :attr:`cancelled`, etc.

        Examples
        --------
        ```python
        g = Graph()
        g.add_chain(source, transform, load)

        # parallel branch from transform onwards
        g.add_chain(alternate_load, attach_to='transform')
        ```
        """
        n_nodes = len(job_functions)
        if not n_nodes:
            return

        if not attach_to:
            if self not in initial_input:
                initial_input = (self,) + initial_input
            self.edges[START_NODE_NAME].append(Edge([initial_input]))
            self.log(f'Initial input = {initial_input}', LOG.DEBUG)

        edge_in = attach_to if attach_to else START_NODE_NAME
        if not labels:
            labels = tuple('' for _ in job_functions)

        for i, (fn, label) in enumerate(zip(job_functions, labels)):
            node = Node(
                graph=self,
                job_function=fn,
                inputs_from=edge_in,
                label=label,
                first=(i == 0),
                last=(i == n_nodes - 1),
            )
            self.nodes[node.name] = node
            if edge_in == attach_to:
                self.edges[attach_to].append(Edge())
            if node.last:
                self.terminal_nodes.append(node.name)
            self.edges[node.name].append(Edge())
            self.log(f'Added node {node}', LOG.DEBUG)
            edge_in = node.name

    def run(self) -> None:
        """Execute all nodes — sequentially or in parallel threads.

        Reads :data:`~yieldgraph.config.THREADED_ENV_VAR` from the
        environment to decide the execution mode, then delegates to
        :meth:`_run_sequential` or :meth:`_run_threaded`.  Sets
        :attr:`finished` to ``True`` when done regardless of outcome.

        Raises
        ------
        Nothing — node-level exceptions are stored in
        :attr:`~yieldgraph.node.Node.errors`.  Graph-level errors are
        recorded in :attr:`error`.
        """
        self._reset()
        if self._threaded:
            self._run_threaded()
        else:
            self._run_sequential()
        self.finished = True
        self.log(
            'ETL process done:\n' + '\n'.join(repr(n) for n in self.nodes.values()),
            LOG.INFO)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_sequential(self) -> None:
        """Execute all nodes one at a time in insertion order.

        Each node fully drains its input edges before the next node 
        starts. This is the default execution mode.
        """
        for i, (name, node) in enumerate(self.nodes.items()):
            if self.cancelled:
                self.error = 'ETL Auftrag abgebrochen'
                self.log('ETL job cancelled', LOG.WARNING)
                break

            self._node_index = i + 1
            edges_in = self.edges[node.inputs_from]
            if not any(edges_in) and node.inputs_from != START_NODE_NAME:
                self.error = f'Keine Eingangsdaten für Knoten: {node.name}'
                self.log(f'No inputs for {name}, edges = \n{edges_in}', LOG.ERROR)
                break

            self._current_node_name = name
            node.reset()
            node.process(edges_in, self.edges[name])
            self.edges[node.inputs_from] = node._inputs
            self.edges[name] = node.outputs

    def _run_threaded(self) -> None:
        """Execute all nodes concurrently in daemon threads.

        Each node starts immediately and blocks in
        :meth:`~yieldgraph.node.Node.process_streaming` until its
        upstream edge delivers data.  When a node finishes it closes all
        its output edges, which unblocks the downstream nodes.

        The seed edges (keyed by 
        :data:`~yieldgraph.config.START_NODE_NAME`) are pre-filled
        before threads start and are closed immediately so first-chain
        nodes know when the initial input is exhausted.

        Notes
        -----
        Fan-out pipelines with multiple chains attached to the same node
        work as long as each downstream branch reads from a distinct
        edge. The graph populates one output edge per attached chain in
        :meth:`add_chain`; in threaded mode :meth:`_run_threaded` passes
        each node only the single edge that belongs to it.
        """
        # Close pre-filled seed edges so first-chain nodes don't block forever.
        for edge in self.edges[START_NODE_NAME]:
            edge.close()

        threads: List[threading.Thread] = []

        for i, (name, node) in enumerate(self.nodes.items()):
            self._node_index = i + 1
            node.reset()

            # Determine which single output edge from inputs_from belongs to
            # this node.  In a linear chain there is exactly one; in a
            # fan-out chain add_chain appended one per branch.
            all_in_edges = self.edges[node.inputs_from]
            # Count how many nodes upstream of this one share the same source.
            branch_index = sum(
                1 for n in self.nodes.values()
                if n.inputs_from == node.inputs_from and list(self.nodes.keys()).index(n.name) < i
            )
            edge_index = min(branch_index, len(all_in_edges) - 1)
            edges_in = [all_in_edges[edge_index]]
            edges_out = list(self.edges[name])

            def _target(
                    n: Node = node,
                    ei: List[Edge] = edges_in,
                    eo: List[Edge] = edges_out) -> None:
                n.process_streaming(ei, eo)
                for edge in eo:
                    edge.close()

            t = threading.Thread(target=_target, name=name, daemon=True)
            threads.append(t)
            self.log(f'Created thread for node {name}', LOG.DEBUG)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def _reset(self) -> None:
        """Prepare the graph for a new run.

        Reads :data:`~yieldgraph.config.THREADED_ENV_VAR` to set
        :attr:`_threaded`, clears cached outputs and flags, aligns node
        column widths, and sets the current-node cursor.
        """
        self._output = []
        self.finished = False
        self.error = ''
        self.cancelled = False
        self._threaded = os.environ.get(THREADED_ENV_VAR, '').lower() in ('1', 'true', 'yes')
        self._adjust_col_widths()
        self._current_node_name = next(iter(self.nodes.keys())) if self.nodes else ''
        mode = 'threaded' if self._threaded else 'sequential'
        self.log(f'Run ({mode}) following graph\n{repr(self)}', LOG.INFO)

    def _adjust_col_widths(self) -> None:
        """Align the ``_col_width`` of all nodes to the longest node 
        name.

        Ensures :meth:`~yieldgraph.node.Node.__repr__` columns line up
        when the graph summary is logged.
        """
        if not self.nodes:
            return
        col_width = max(len(n) for n in self.nodes.keys())
        for node in self.nodes.values():
            node._col_width = col_width

    def _node_label(self, node_name: str) -> str:
        """Return the display label for *node_name*.

        Looks up :attr:`labels` first; falls back to uppercasing each
        ``_``-separated token of the node name.

        Parameters
        ----------
        node_name : str
            The internal name of the node (``function.__name__``).
        """
        label = self.labels.get(node_name, '')
        if not label:
            label = ' '.join(n.upper() for n in node_name.split('_'))
        return label

    # ------------------------------------------------------------------
    # Dunder methods
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        arrow = ' -> '
        parts = ['START']
        for name, node in self.nodes.items():
            if node.first and node.inputs_from != START_NODE_NAME:
                parts.append(f'\n\t\t...{arrow}{node.inputs_from}')
            parts.append(f'{arrow}{name}')
            if node.last:
                parts.append(f'{arrow}END')
        if self.cancelled:
            parts.append('\nETL PROCESS WAS INTERRUPTED BY USER!')
        return ''.join(parts)

    def __call__(self) -> None:
        """Execute the pipeline — equivalent to calling :meth:`run`."""
        self.log('Start ETL process', LOG.TRACE)
        self.run()


__all__ = ['Graph']
