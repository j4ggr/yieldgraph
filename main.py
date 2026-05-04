import inspect
import traceback

from copy import deepcopy

from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Deque
from typing import Callable
from typing import TypeAlias
from typing import Generator
from typing import DefaultDict
from collections import deque
from collections import defaultdict

# Edge: TypeAlias = Deque[Tuple[Any, ...]]

from config import LOG
from config import LoggingBehavior

BEGIN = 'Start'


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



class Edge(deque): 
    """The `Edge` class represents an edge in a graph, which connects two
    nodes in the graph. The edge is responsible for passing data between
    the nodes.

    The `Edge` class provides the following functionality:
    """


class Node(LoggingBehavior):
    """Represents a node in an ETL (Extract, Transform, Load) graph.
    Each node contains a job function that performs a specific task in 
    the ETL process. The node manages the inputs, outputs, and errors
    associated with the job function, and provides properties to track 
    the progress and status of the node.

    Parameters
    ----------
    graph : Graph
        The ETL Graph object that this node belongs to.
    job_function : Callable
        The job function that this node represents.
    inputs_from : str
        The name of the node that this node receives input from.
    designation : str
        The designation of the node.
    first : bool, optional
        Whether this node is the first node in the graph,
        by default True.
    last : bool, optional
        Whether this node is the last node in the graph,
        by default False.
    """

    name: str
    """Name of the node. Same as name of given job function."""

    job: Job
    """Job function that this node represents."""

    inputs_from: str
    """Name of the node that this node receives input from."""

    first: bool
    """Whether this node is the first node in the graph."""

    last: bool
    """Whether this node is the last node in the graph."""

    repr_len: int = 25
    """Length of the string representation of the node."""

    n_jobs_todo: int
    """Number of available inputs when starting loop."""

    count_in: int
    """Number of inputs received so far."""
    
    count_out = 0
    """Number of outputs generated so far."""
    
    errors = []
    """Collection of errors encountered during job execution."""

    _inputs: List[Edge] = []
    """List of input edges for the node."""
    
    _inputs_idx: int = 0
    """Index of the current input edge."""
    
    outputs = []
    """List of outputs generated by the job function."""

    additional_info: str = ''
    """"Additional information to be displayed in the node."""

    last_output: Tuple[Any, ...]
    """Last output generated by the job function."""

    is_first_job: bool = False
    """Whether current job is the first job for this node."""

    is_last_job: bool = False
    """Whether current job is the last job for this node."""
    
    def __init__(
            self,
            graph,
            job_function: Callable,
            inputs_from: str,
            designation: str = '',
            first: bool = True,
            last: bool = False
            ) -> None:
        self.graph = graph
        self.job = Job(job_function, designation)
        self.name = self.job.name
        self.inputs_from = inputs_from
        self.first = first
        self.last = last
        self.preset()

    @property
    def count_err(self) -> int:
        """Get amount of occured errors during job execution (read-only)."""
        return len(self.errors)
    
    @property
    def inputs(self) -> Edge:
        """get inputs by stored index for given input edge
        
        set inputs of given edge list by getting the index of a non 
        empty deque in the edge list"""
        if not self._inputs:
            return Edge()
        return self._inputs[self._inputs_idx]
    @inputs.setter
    def inputs(self, edges: List[Edge]) -> None:
        self._inputs = edges
        self.log(f'Inputs changed, n inputs = {len(edges)}', LOG.TRACE)
        if self._inputs:
            for idx, inputs in enumerate(self._inputs):
                if len(inputs): 
                    self._inputs_idx = idx
                    break
    
    @property
    def n_inputs(self) -> int:
        """Get amount of available inputs (read-only)."""
        if type(self.inputs) is tuple and self.inputs:
            return 1
        
        return len(self.inputs)
    
    @property
    def n_outputs(self) -> int:
        """Get amount of available outputs (read-only)."""
        if type(self.outputs) is tuple and self.outputs:
            return 1
        
        return len(self.outputs)
    
    @property
    def no_outputs(self) -> bool:
        """True if no output is generated (read-only)."""
        if not self.outputs:
            return True
        
        return not any([bool(len(edge)) for edge in self.outputs])
    
    @property
    def progress(self) -> float:
        """Get progress of jobs done as {p | 0 < p <= 1} (read-only)."""
        if self.n_jobs_todo == 0:
            p = 0.5
        else:
            p = self.count_in/self.n_jobs_todo
        return max(0.01, min(p, 100))
    
    def preset(self) -> None:
        """set counters to 0 and empty errors, inputs and outputs"""
        self.n_jobs_todo = 0
        self.count_in = 0
        self.count_out = 0
        self.errors = []
        self._inputs: List[Edge] = []
        self._inputs_idx: int = 0
        self.outputs = []
        self.additional_info = ''

    def loop(self, edges_in: List[Edge], edges_out: List[Edge]) -> None:
        """Start loop over the jobs done by the job_function"""
        self.log(
            f'{self.n_inputs} jobs for node "{self.job.designation}"',
            LOG.INFO)
        self.inputs = edges_in
        self.outputs = edges_out
        self.n_jobs_todo = deepcopy(self.n_inputs)
        self.job.interrupt = False
        for job_count in range(1, self.n_jobs_todo + 1):
            self.is_first_job = job_count == 1
            self.is_last_job = job_count == self.n_jobs_todo
            self.do_job(self.inputs.popleft())
    
    def do_job(self, job_data: Tuple[Any, ...]) -> None:
        """Generate outputs done by the job_function by giving a new job 
        as arguments for generetor."""
        try:
            if self.graph.cancel:
                self.log('Skip job because of graph cancel', LOG.TRACE)
                return
            
            for output in self.job(*job_data):
                if self.graph.cancel:
                    self.job.interrupt = True
                    break
                
                output = self._ensure_tuple_(output)
                self._store_output_(output)
        
        except KeyboardInterrupt as e:
            self.graph.cancel = True
            self.log(f'{self} interrupted because: {e}', LOG.INFO)
        
        except Exception as e:
            self.log(
                f'Cached error = {e}\nError occured @ node{repr(self)}',
                LOG.WARNING)
            if LOG.TRACEBACK:
                self.log(f'{traceback.format_exc()}', LOG.ERROR)
            self.errors.append(e)
        
        finally:
            self.count_in += 1
    
    @staticmethod
    def _ensure_tuple_(output) -> Tuple[Any, ...]:
        """If a single value is returned, wrap it in a tuple"""
        return output if type(output) is tuple else (output, )
    
    def _store_output_(self, output: Tuple[Any, ...]) -> None:
        """Store output in all output queues"""
        self.last_output = output
        for q in self.outputs:
            q.append(output)
        self.log(f'Stored output @ node {self}' , LOG.TRACE)
        self.count_out += 1
    
    def __len__(self) -> int:
        if self.first:
            inputs = self.inputs
            if not type(inputs) is tuple: 
                inputs = (inputs, )
            try:
                length = len(inputs[0])
            except TypeError:
                length = 0
        else:
            length = self.n_inputs
        return length

    def __str__(self) -> str:
        return f'{self.name} ({self.inputs_from})'

    def __repr__(self) -> str:
        name = self.name.ljust(self.repr_len+1)
        r = f'  |-{name}|{self.count_in: 4}|{self.count_out: 4}|{self.count_err: 4}|'
        if self.first: 
            r = (f'{self.inputs_from.ljust(self.repr_len+6)}'
                + ' '.join([s.ljust(4) for s in ('in', 'out', 'err')])
                + f'\n{r}')
        return r


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
            self.edges[BEGIN].append(Edge([input_first]))
            self.log(f'Input first = {input_first}', LOG.DEBUG)
        
        edge_in = input_of if input_of else BEGIN
        if not designations:
            designations = tuple('' for _ in job_functions)
        for i, (fun, desig) in enumerate(zip(job_functions, designations)):
            node = Node(
                graph=self, 
                job_function=fun,
                inputs_from=edge_in,
                designation=desig,
                first=(i==0),
                last=(i==n_nodes-1))
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
            if not any(edges_in) and not node.inputs_from == BEGIN:
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
            if node.first and node.inputs_from != BEGIN:
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

__all__ = [
    'BEGIN',
    'Node',
    'Graph']
