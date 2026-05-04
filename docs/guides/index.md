# Guides

## Building a simple pipeline

Every pipeline starts with a `Graph`. Add a chain of callables with `add_chain`, then call `run()`.

The first callable in a chain always receives the `Graph` instance as its first argument.  
Subsequent callables receive whatever the previous callable `yield`s.

```python
from yieldgraph import Graph

def source(graph):
    for item in [1, 2, 3, 4, 5]:
        yield item

def double(x):
    yield x * 2

def to_str(x):
    yield str(x)

g = Graph()
g.add_chain(source, double, to_str)
g.run()

print(g.output)  # ['2', '4', '6', '8', '10']
```

## Using regular functions

Callables do not need to be generators. A plain function that returns a value is
automatically wrapped into a one-shot generator.

```python
def add_one(x):
    return x + 1
```

## Fan-out (multiple downstream nodes)

Call `add_chain` multiple times to branch from the same source node.

```python
g = Graph()
g.add_chain(source, branch_a)
g.add_chain(source, branch_b)
g.run()
```

Both `branch_a` and `branch_b` receive every item produced by `source`.

## Threaded execution

Set the environment variable `YIELDGRAPH_THREADED=1` before running to execute
each node in its own thread. Edges become thread-safe blocking queues.

```bash
YIELDGRAPH_THREADED=1 python my_pipeline.py
```

Or from Python:

```python
import os
os.environ['YIELDGRAPH_THREADED'] = '1'
g.run()
```

## Cancellation

Set `graph.cancelled = True` at any point (e.g. from a signal handler or UI
callback) and all nodes will stop after their current yield.

## Accessing results

After `run()` completes:

```python
g.succeeded   # True if no errors
g.output      # list of items from the terminal node(s)
g.error       # error message string, or None
```
