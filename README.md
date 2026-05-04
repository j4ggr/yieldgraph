# yieldgraph

> Build interruptible, generator-driven ETL pipelines as directed graphs — in pure Python.

[![CI](https://github.com/j4ggr/yieldgraph/actions/workflows/ci.yml/badge.svg)](https://github.com/j4ggr/yieldgraph/actions/workflows/ci.yml)
[![Python 3.14+](https://img.shields.io/badge/python-3.14%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-teal)](https://j4ggr.github.io/yieldgraph/)

---

## What is yieldgraph?

**yieldgraph** lets you compose data-processing pipelines by connecting plain Python
callables into a directed graph. Each callable becomes a **Node**. Nodes are linked
by **Edges** — lightweight queues that carry data tuples downstream. Call `graph.run()`
and the pipeline does the rest.

There is no framework to learn. Your business logic stays in ordinary Python functions.
yieldgraph just wires them together and gets out of the way.

---

## Features

- **Pure Python, zero runtime dependencies** — stdlib only; `loguru` is optional.
- **Generator-native** — `yield` zero, one, or many results per input; everything stays lazy.
- **Cooperative cancellation** — set `graph.cancelled = True` or press `Ctrl+C`; the pipeline
  stops cleanly at the next `yield`.
- **Fan-out branching** — attach multiple downstream chains to any node with a single
  `add_chain` call.
- **Threaded mode** — flip `YIELDGRAPH_THREADED=1` to run all nodes concurrently; edges
  become thread-safe blocking queues automatically.
- **Built-in observability** — every node exposes `n_consumed`, `n_produced`, `errors`,
  and a `progress` fraction you can poll at any time.

---

## Installation

```bash
pip install yieldgraph
```

From source:

```bash
git clone https://github.com/j4ggr/yieldgraph.git
cd yieldgraph
pip install -e .
```

---

## Quick start

```python
from yieldgraph import Graph

def source(graph):
    """Emit raw records — receives the Graph instance as first argument."""
    for row in [
        {"name": "Alice", "score": 95},
        {"name": "Bob",   "score": 72},
        {"name": "Carol", "score": 88},
    ]:
        yield row

def grade(record):
    """Add a pass/fail label to each record."""
    record["grade"] = "pass" if record["score"] >= 85 else "fail"
    yield record

def format_output(record):
    """Format as a human-readable string."""
    yield f"{record['name']}: {record['grade']} ({record['score']})"

g = Graph()
g.add_chain(source, grade, format_output)
g.run()

for row in g.output:
    print(row[0])
# Alice: pass (95)
# Bob: fail (72)
# Carol: pass (88)
```

---

## Fan-out — multiple downstream chains

```python
def source(graph):
    for x in range(1, 6):
        yield x

def store_db(x):
    yield f"db:{x}"

def send_queue(x):
    yield f"mq:{x}"

g = Graph()
g.add_chain(source, store_db)    # chain 1
g.add_chain(source, send_queue)  # chain 2 — same source, parallel branch
g.run()

print(g.output)
# [('db:1',), ('db:2',), ..., ('mq:1',), ('mq:2',), ...]
```

---

## Cooperative cancellation

```python
def source(graph):
    for i in range(1_000_000):
        if i >= 5:
            graph.cancelled = True   # stop cleanly after this item
            return
        yield i

g = Graph()
g.add_chain(source, process)
g.run()

print(len(g.output))   # 5
print(g.cancelled)     # True
```

---

## Threaded execution

```bash
YIELDGRAPH_THREADED=1 python my_pipeline.py
```

Or in Python before the run:

```python
import os
os.environ["YIELDGRAPH_THREADED"] = "1"

g = Graph()
g.add_chain(fetch_from_api, transform, write_to_db)
g.run()
```

---

## Error handling

Exceptions raised inside a node are caught per-item and stored — they never abort the
pipeline. Inspect them after the run:

```python
g.run()

for name, node in g.nodes.items():
    if node.n_errors:
        print(f"{name}: {node.n_errors} error(s)")
        for err in node.errors:
            print(f"  {type(err).__name__}: {err}")

if g.succeeded:
    print(f"Done — {len(g.output)} rows produced")
```

---

## Status & progress

| Expression | Type | Description |
|---|---|---|
| `g.output` | `list[tuple]` | All tuples emitted by terminal nodes |
| `g.succeeded` | `bool` | Finished without errors |
| `g.has_output` | `bool` | Finished and produced at least one row |
| `g.error` | `str` | Graph-level error description (empty on success) |
| `g.step` | `str` | Human-readable label of the currently executing node |
| `g.progress` | `int` | 0–100 % progress of the current node |
| `g.cancelled` | `bool` | Whether the run was cancelled |
| `g.finished` | `bool` | Whether `run()` has returned |

---

## Documentation

Full guides and API reference at **https://j4ggr.github.io/yieldgraph/**

- [Getting Started](https://j4ggr.github.io/yieldgraph/guides/getting-started/)
- [Patterns & Recipes](https://j4ggr.github.io/yieldgraph/guides/patterns/)
- [Configuration](https://j4ggr.github.io/yieldgraph/guides/configuration/)
- [API Reference](https://j4ggr.github.io/yieldgraph/api/)

---

## Development

```bash
# Install dev dependencies
pdm install -G test -G doc

# Run tests
pdm run pytest

# Build docs locally
pdm run mkdocs serve
```

---

## License

[MIT](LICENSE)
