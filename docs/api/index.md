# API Reference

Complete reference for all public classes, methods, and module-level helpers in yieldgraph.

---

## Graph

The main entry point. Build a pipeline with `add_chain`, then execute it with `run`.

```python
from yieldgraph import Graph

g = Graph()
g.add_chain(source, transform, load)
g.run()
print(g.output)
```

::: yieldgraph.graph.Graph

---

## Node

A single processing step managed internally by `Graph`. You rarely instantiate `Node` directly — use `Graph.add_chain` instead. The attributes and properties are useful for inspecting pipeline state during or after a run.

```python
# After g.run(), inspect each node:
for name, node in g.nodes.items():
    print(f"{name}: consumed={node.n_consumed}, produced={node.n_produced}, errors={node.n_errors}")
```

::: yieldgraph.node.Node

---

## Job

The execution wrapper around a callable. Normalises plain functions and generators into a uniform, cancellable generator protocol.

```python
from yieldgraph import Job

def process(x):
    return x * 2

job = Job(process, label="Double It")
results = list(job(21))   # → [42]
print(job.label)          # → 'Double It'
```

::: yieldgraph.job.Job

---

## Edge

A directed queue that carries data tuples between nodes. Subclasses `collections.deque` for sequential use; adds thread-safe `put` / `get` / `close` for threaded pipelines.

```python
from yieldgraph import Edge

# Sequential mode
e = Edge()
e.append((1, 2))
print(e.popleft())   # → (1, 2)

# Threaded mode
e2 = Edge()
e2.put(("hello",))
print(e2.get())      # → ('hello',)
e2.close()
print(e2.get())      # → None  (closed + empty)
```

::: yieldgraph.edge.Edge

---

## Config

Module-level constants and the `LoggingBehavior` mixin used throughout yieldgraph.

```python
from yieldgraph import LOG, ENV, LoggingBehavior, START_NODE_NAME

# Log level constants
print(LOG.INFO)           # 'INFO'
print(LOG.TRACE_LEVEL) # 5

# Environment variable names
print(ENV.THREADED)       # 'YIELDGRAPH_THREADED'

# Sentinel for the implicit start node
print(START_NODE_NAME)    # '__START__'
```

::: yieldgraph.config

