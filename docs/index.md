# yieldgraph

> A lightweight Python library for building interruptible, generator-driven ETL pipelines as directed graphs.

---

## Overview

**yieldgraph** lets you compose data processing pipelines by chaining Python callables into a directed graph of nodes connected by edges. Each node wraps a *job* — any regular function or generator function — and passes its outputs downstream as inputs to the next node. The pipeline supports cooperative cancellation, per-node error collection, and real-time progress tracking.

Key concepts:

| Concept | Description |
|---------|-------------|
| `Job`   | Wraps a callable (regular or generator). Provides an interruptible generator loop that can be stopped at any yield point. |
| `Edge`  | A `deque`-based queue that connects two nodes and carries data tuples between them. |
| `Node`  | A processing unit that runs its `Job` over all inputs from an incoming `Edge` and pushes results to one or more outgoing `Edge`s. |
| `Graph` | Owns the collection of nodes and edges. Chains are added declaratively; the graph is executed by calling `graph.run()` (or `graph()`). |

---

## Installation

```bash
pip install yieldgraph
```

## Quick Start

```python
from yieldgraph import Graph

def source(graph):
    for i in range(5):
        yield i

def double(x):
    yield x * 2

g = Graph()
g.add_chain(source, double)
g.run()

print(g.output)  # [0, 2, 4, 6, 8]
```

See the [Guides](guides/index.md) for more usage examples and the [API Reference](api/index.md) for full documentation.
