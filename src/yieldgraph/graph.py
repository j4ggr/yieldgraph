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
from .edge import Edge
from .node import Node


class Graph(LoggingBehavior):
    """
    The `Graph` class represents an ETL (Extract, Transform, Load) graph, 
    which is a collection of interconnected nodes that perform data 
    processing tasks. The graph can be configured with a set of 
    callables (functions or classes) that are chained together to form 
    the ETL pipeline.
    
    The `Graph` class provides the following functionality:
    
    - Initializes the graph with a logger and other properties.
    - Allows adding a chain of callables to the graph, specifying the 
      input for the first node and the node to which the chain should be
      attached.
    - Runs the ETL process by executing all the nodes in the graph 
      chain(s).
    - Provides properties to get information about the graph's state,
      such as whether the ETL job succeeded, the current step, progress.
    - Adjusts the representation length of the nodes for better 
      formatting.
    - Provides a string representation of the graph, showing the chain 
      of nodes.
    """

    nodes: Dict[str, Node]
    """Dictionary of nodes in the graph.
        - Key: name of the node.
        - Value: corresponding `Node` object."""

    edges: DefaultDict[str, List[Edge]]
    """Dictionary of all edges in the graph.
        - Key: name of the node the edge is coming from.
        - Value: list of `Edge` objects going out of the node."""

    end_nodes: List[str]
    """List of names of end nodes in the graph."""

    _output: List[Tuple[Any, ...]]
    """Flatten list of all outputs of the end nodes in the graph."""

    current_node_name: str
    """Name of the current node being processed."""

    node_counter: int
    """Counter for the number of nodes in the graph."""

    cancel: bool
    """Flag indicating whether the ETL job should be canceled."""

    finished: bool
    """Flag indicating whether the ETL job is finished."""

    error_msg: str
    """Error message if the ETL job fails."""

    designations: Dict[str, str]
    """Dictionary of designations for the node names."""

    def __init__(self) -> None:
        
        self.nodes = {}
        self.edges = defaultdict(list)
        self.end_nodes = []
        self._output = []
        self.current_node_name = ''
        self.node_counter = 0
        self.cancel: bool = False
        self.finished = False
        self.error_msg = ''
        self.designations = {}
    
    @property
    def succeed(self) -> bool:
        """Return True if etl job is done without any errors (read-only)."""
        return not bool(self.error_msg) and self.finished
    
    @property
    def has_loads(self) -> bool:
        """Return True if the graph has finished and has outputs 
        (read-only)."""
        return bool(self._output) and self.finished
    
    @property
    def current_node(self) -> Node:
        """Get current node (read-only)."""
        return self.nodes[self.current_node_name]
    
    @property
    def output(self) -> List[Tuple[Any, ...]]:
        """Get edge values as a single list of each node at the end of 
        the graph (read-only)."""
        if self._output:
            return self._output
        
        for node_name in self.end_nodes:
            for edge in self.edges[node_name]:
                self._output.extend(list(edge))
        self.log(f'{len(self._output)} outputs found', LOG.TRACE)
        return self._output
    
    @property
    def running_first_node(self) -> bool:
        """True if current node is the very first node of the graph
        (read-only)."""
        node_names = list(self.nodes.keys())
        if not node_names:
            return False
        
        return self.current_node_name == node_names[0]
    
    @property
    def step(self) -> str:
        """Current step, used by ProgressObserver (read-only)."""
        name = self.node_designation(self.current_node_name)
        if self.cancel:
            step = ''
        elif self.finished:
            step = 'ETL Prozess Ende'
        else:
            step = name
        return step
    
    @property
    def progress(self) -> int:
        """Get progress of current node in %"""
        progress = int(100 * self.current_node.progress)
        return progress
    
    def node_designation(self, node_name: str) -> str:
        """Get designation of node name"""
        designation = self.designations.get(node_name, '')
        if not designation:
            designation = ' '.join(
                n.upper() for n in node_name.split('_'))
        return designation

    def add_chain(
            self,
            *job_functions: Callable,
            designations: Tuple[str, ...] = (),
            input_first: Tuple[Any, ...] = (), 
            input_of: str = '') -> None:
        """Give a list of callables to add to the graph as a chain.
        
        The functions are chained together in the order they are in the 
        list, so that the output of the previous function is passed on 
        as input (arguments) to the next functions. If the function is a 
        multi-output generator. Each one is generated individually and 
        passed on to the next function.
        
        Parameters
        ----------
        job_functions : Callable
            Callables for the graph chain.
        input_first : tuple, optional
            Arguments for the first node if needed. This argument is
            only considered if no `input_of` is given.
        input_of : callable, optional
            If nodes should be added as a parallel chain to a existing
            chain, specify which node the chain should be attached to,
            by default None
        
        Notes
        -----
        The first node must accept the graph as the first argument.
        This is helpful to keep track on the cancel flag within a Node
        and to get the output of the graph.
        """
        n_nodes = len(job_functions)
        if not n_nodes:
            return

        if not input_of:
            if self not in input_first:
                input_first = (self, ) + input_first
            self.edges[START_NODE_NAME].append(Edge([input_first]))
            self.log(f'Input first = {input_first}', LOG.DEBUG)

        edge_in = input_of if input_of else START_NODE_NAME
        if not designations:
            designations = tuple('' for _ in job_functions)
        for i, (fun, label) in enumerate(zip(job_functions, designations)):
            node = Node(
                graph=self,
                job_function=fun,
                inputs_from=edge_in,
                label=label,
                first=(i == 0),
                last=(i == n_nodes - 1))
            self.nodes[node.name] = node
            if edge_in == input_of:
                self.edges[input_of].append(Edge())
            if node.last:
                self.end_nodes.append(node.name)
            self.edges[node.name].append(Edge())
            self.log(f'Add node {node}', LOG.DEBUG)
            edge_in = node.name

    def run(self) -> None:
        """Start ETL process by run all the nodes in the graph chain(s)"""
        self._prepare_()
        for i, (name, node) in enumerate(self.nodes.items()):
            if self.cancel: 
                self.error_msg = 'ETL Auftrag abgebrochen'
                self.log(f'ETL job canceled', LOG.WARNING)
                break
            
            self.node_counter = i+1
            edges_in = self.edges[node.inputs_from]
            if not any(edges_in) and not node.inputs_from == START_NODE_NAME:
                self.error_msg = f'Keine Eingangsdaten für Knoten: {node.name}'
                self.log(f'No inputs for {name}, inputs = \n{edges_in}', LOG.ERROR)
                break
            
            self.current_node_name = name
            node.preset()
            node.loop(edges_in, self.edges[name])
            self.edges[node.inputs_from] = node._inputs
            self.edges[name] = node.outputs
        self.finished = True
        self.log(
            f'ETL process done:\n'
            + '\n'.join([repr(n) for n in self.nodes.values()]),
            LOG.INFO)

    def _prepare_(self) -> None:
        """Prepare for new run"""
        self._output = []
        self.finished = False
        self.error_msg = ''
        self.cancel = False
        self.adjust_node_repr_len()
        self.current_node_name = next(iter(self.nodes.keys())) if self.nodes else ''
        self.log(f'Run following graph\n{repr(self)}', LOG.INFO)

    def adjust_node_repr_len(self) -> None:
        repr_len = max([len(n) for n in self.nodes.keys()])
        for node in self.nodes.values(): 
            node.repr_len = repr_len

    def __repr__(self) -> str:
        e = ' -> '
        s = f'START'
        for name, node in self.nodes.items():
            if node.first and node.inputs_from != START_NODE_NAME:
                s += f'\n\t\t...{e}{node.inputs_from}'
            s += f'{e}{name}'
            if node.last:s += f'{e}END'
        if self.cancel:
            s += '\nETL PROCESS WAS INTERRUPTED BY USER!'
        return s
    
    def __call__(self) -> None:
        """Start ETL process by run all the nodes in the graph chain(s)."""
        self.log(f'Start ETL process', LOG.TRACE)
        self.run()


__all__ = ['Graph']
