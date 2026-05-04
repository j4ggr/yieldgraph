# Guides

Welcome to the yieldgraph guides. These pages walk you through building pipelines — from a simple two-step chain all the way to multi-branch, threaded production pipelines.

<div class="grid cards" markdown>

-   :material-rocket-launch: **Getting Started**

    ---

    Install yieldgraph and build your first pipeline in minutes. Covers sources, transforms, output, and the tuple data model.

    [:octicons-arrow-right-24: Getting Started](getting-started.md)

-   :material-puzzle: **Patterns & Recipes**

    ---

    Fan-out branches, `attach_to`, error handling, cooperative cancellation, progress monitoring, and threaded execution.

    [:octicons-arrow-right-24: Patterns & Recipes](patterns.md)

-   :material-cog: **Configuration**

    ---

    Environment variables, log levels, the `LoggingBehavior` mixin, and the `ENV` / `LOG` constants objects.

    [:octicons-arrow-right-24: Configuration](configuration.md)

</div>

---

## Quick reference

```python
from yieldgraph import Graph

# Define steps
def source(graph):
    for x in range(1, 6):
        yield x

def square(x):
    yield x ** 2

# Build
g = Graph()
g.add_chain(source, square)

# Run
g.run()

# Consume
print(g.output)
# [(1,), (4,), (9,), (16,), (25,)]
```

| Task | How |
|---|---|
| Build a linear pipeline | `g.add_chain(fn1, fn2, fn3)` |
| Branch from a node | `g.add_chain(fn, attach_to="node_name")` |
| Run sequentially | `g.run()` |
| Run with threads | `YIELDGRAPH_THREADED=1 python ...` |
| Check success | `g.succeeded` |
| Get errors | `node.errors` for each node |
| Cancel early | `graph.cancelled = True` inside a node |

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
