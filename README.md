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

Or from source:

```bash
git clone https://github.com/<your-org>/yieldgraph.git
cd yieldgraph
pip install -e .
```

---

## Quick Start

```python
from yieldgraph import Graph

# Define processing steps as plain functions or generators
def extract(graph, source):
    for record in source:
        yield record          # multi-output: each record flows to the next node

def transform(record):
    return record.strip().upper()

def load(record):
    print(record)

# Build the graph
g = Graph()
g.add_chain(
    extract, transform, load,
    input_first=(["  hello ", " world "],),
)

# Run
g.run()
print("Success:", g.succeed)
```

---

## Branching / Parallel Chains

Attach additional chains to an existing node with `input_of`:

```python
g = Graph()
g.add_chain(extract, transform_a, load_a, input_first=(data,))
g.add_chain(transform_b, load_b, input_of="transform_a")
g.run()
```

---

## Cooperative Cancellation

Set `graph.cancel = True` from any thread or from within a job to stop the pipeline cleanly at the next yield point. No data is silently dropped — in-flight results are flushed before the run exits.

```python
import threading

g = Graph()
g.add_chain(slow_extract, transform, load, input_first=(large_dataset,))

t = threading.Thread(target=g.run)
t.start()

# Cancel after 5 seconds
import time; time.sleep(5)
g.cancel = True
t.join()
```

---

## Progress & Status

```python
g.run()

g.succeed        # bool  — finished without errors
g.has_loads      # bool  — finished and produced output
g.step           # str   — human-readable name of the current node
g.progress       # int   — 0–100 % progress of the current node
g.output         # list  — flat list of all tuples emitted by end nodes
```

---

## Error Handling

Exceptions inside a job are caught per-record, stored on the node, and do not abort the pipeline. After the run, inspect errors per node:

```python
for name, node in g.nodes.items():
    if node.count_err:
        print(f"{name}: {node.errors}")
```

---

## API Reference

### `Job(function, designation="")`

| Attribute / Method | Description |
|--------------------|-------------|
| `name`             | Name of the wrapped function. |
| `designation`      | Human-readable label (auto-derived from name if not provided). |
| `interrupt`        | Set to `True` to stop the generator at the next yield. |
| `running`          | `True` while the generator loop is active. |
| `__call__(*args)`  | Returns an interruptible generator for one invocation. |

### `Edge`

A `collections.deque` subclass. Nodes `append` outputs and `popleft` inputs.

### `Node(graph, job_function, inputs_from, designation="", first=True, last=False)`

| Property / Method | Description |
|-------------------|-------------|
| `count_in`        | Records consumed so far. |
| `count_out`       | Records produced so far. |
| `count_err`       | Number of caught exceptions. |
| `progress`        | Float in (0, 1] representing completion. |
| `loop(edges_in, edges_out)` | Run the job over all available inputs. |

### `Graph()`

| Method / Property | Description |
|-------------------|-------------|
| `add_chain(*fns, input_first=(), input_of="", designations=())` | Add a linear chain of nodes. |
| `run()`           | Execute the full graph. |
| `cancel`          | Set to `True` to request cooperative cancellation. |
| `succeed`         | `True` if finished with no errors. |
| `output`          | Flat list of all end-node outputs. |

---

## Requirements

- Python ≥ 3.10 (uses `TypeAlias`)

---

## License

MIT
