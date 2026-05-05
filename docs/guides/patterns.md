# Patterns & Recipes

A collection of practical patterns for building real-world pipelines with yieldgraph.

---

## Fan-out — multiple branches from one source

Call `add_chain` a second time to attach a new chain to the **same source node**. Both chains receive every item the source produces.

```python
from yieldgraph import Graph

def source(graph):
    for record in [
        {"product": "Widget", "qty": 5,  "price": 9.99},
        {"product": "Gadget", "qty": 2,  "price": 49.99},
        {"product": "Doohickey", "qty": 12, "price": 2.49},
    ]:
        yield record

def save_to_database(record):
    print(f"  DB ← {record['product']}")
    yield record["product"]

def send_to_warehouse(record):
    print(f"  WH ← {record['product']} x{record['qty']}")
    yield (record["product"], record["qty"])

g = Graph()
g.add_chain(source, save_to_database)   # (1)!
g.add_chain(source, send_to_warehouse)  # (2)!
g.run()
```

1. First chain: source → database.
2. Second chain: *same* source function — yieldgraph detects the name match and fans the data to both branches.

!!! warning "Function identity"
    yieldgraph uses the function's `__name__` as the node key. Two different functions with the same name will be treated as the same node. Use distinct function names when building fan-out pipelines.

---

## Attaching a chain to an existing node

Use `attach_to` to splice a new branch into the output of any *already-registered* node — not just the source:

```python
def source(graph):
    for x in range(1, 6):
        yield x

def validate(x):
    if x % 2 == 0:
        yield x             # only pass even numbers

def store(x):
    yield f"stored:{x}"

def audit_log(x):
    yield f"audit:{x}"

g = Graph()
g.add_chain(source, validate, store)

# Tap into validate's output for a parallel audit branch
g.add_chain(audit_log, attach_to="validate")  # (1)!

g.run()

print("store output:", [row[0] for row in g.output if "stored" in row[0]])
print("audit output:", [row[0] for row in g.output if "audit"  in row[0]])
# store output: ['stored:2', 'stored:4']
# audit output: ['audit:2', 'audit:4']
```

1. `attach_to` accepts the **function name** (string) of the node whose output you want to tap.

---

## Seeding a pipeline with initial data

By default the first node receives only the `Graph` instance. Use `initial_input` to pass extra seed values:

```python
def process(graph, config):       # (1)!
    for item in config["items"]:
        yield item

config = {"items": ["a", "b", "c"]}

g = Graph()
g.add_chain(process, initial_input=(config,))   # (2)!
g.run()
print(g.output)
# [('a',), ('b',), ('c',)]
```

1. The graph instance is always prepended automatically — you only need to list the *extra* arguments.
2. `initial_input` must be a tuple.

---

## Error handling

Exceptions raised inside a node's job function are **caught and collected** — they do not crash the pipeline:

```python
from yieldgraph import Graph

def source(graph):
    for x in [2, 0, 4, "bad", 8]:
        yield x

def safe_divide(x):
    result = 100 / x        # (1)!
    yield result

g = Graph()
g.add_chain(source, safe_divide)
g.run()

# Inspect per-node errors
for name, node in g.nodes.items():
    if node.n_errors:
        print(f"{name}: {node.n_errors} error(s)")
        for err in node.errors:
            print(f"  → {type(err).__name__}: {err}")

# Still get the successful results
print(g.output)
```

1. Division by zero and `TypeError` (from the string `"bad"`) are caught, stored in `node.errors`, and the pipeline continues with the remaining items.

!!! info "Graph-level vs node-level errors"
    - `node.errors` — list of `Exception` objects caught during that node's run
    - `node.n_errors` — convenience count
    - `g.error` — a non-empty string describes *graph-level* problems (e.g. no input data for a node)
    - `g.succeeded` — `True` only when the run completed AND `g.error` is empty

---

## Cooperative cancellation

Set `graph.cancelled = True` at any point — from inside a node function or from an external thread — to stop the pipeline gracefully after the current `yield`.

### Cancelling from inside the pipeline

```python
from yieldgraph import Graph

MAX_RESULTS = 3

def source(graph):
    for i in range(1, 100):
        yield i

def collect(x):
    yield x

def early_stop(x):
    yield x
    # Cancel after we have collected enough
    # (check the owning graph's node count as a proxy)

# Better: cancel from the source itself
def source_with_limit(graph):
    for i in range(1, 100):
        if i > MAX_RESULTS:
            graph.cancelled = True  # (1)!
            return
        yield i

g = Graph()
g.add_chain(source_with_limit, collect)
g.run()

print(g.output)       # [(1,), (2,), (3,)]
print(g.cancelled)    # True
print(g.succeeded)    # False (cancelled counts as not succeeded)
```

1. Setting `graph.cancelled` inside a node stops all subsequent nodes. The current node exits cleanly after its function returns.

### Cancelling from an external thread

```python
import threading
from yieldgraph import Graph

def slow_source(graph):
    for i in range(1_000_000):
        yield i

def process(x):
    yield x * 2

g = Graph()
g.add_chain(slow_source, process)

def watchdog():
    import time
    time.sleep(0.01)        # let the pipeline start
    g.cancelled = True      # signal cancellation

t = threading.Thread(target=watchdog, daemon=True)
t.start()

g.run()
print(f"Processed {len(g.output)} items before cancel")
```

### KeyboardInterrupt

Pressing <kbd>Ctrl</kbd>+<kbd>C</kbd> while the pipeline runs sets `graph.cancelled = True` automatically. No special handling required.

---

## Progress monitoring

Every `Node` exposes real-time counters that you can poll from a background thread or a UI update loop:

| Attribute | Type | Description |
|---|---|---|
| `node.n_consumed` | `int` | Items pulled from input edges so far |
| `node.n_produced` | `int` | Items pushed to output edges so far |
| `node.n_queued` | `int` | Total items queued at the start of the run |
| `node.progress` | `float` | `n_consumed / n_queued`, clamped to `[0.01, 1.0]` |
| `node.errors` | `list` | Exceptions caught during this node's run |

The `Graph` itself provides aggregated progress helpers:

| Attribute | Type | Description |
|---|---|---|
| `g.step` | `str` | Human-readable label of the currently executing node |
| `g.progress` | `int` | `int(100 * current_node.progress)` |
| `g.finished` | `bool` | `True` once `run()` has returned |

### Polling example

```python
import threading
from yieldgraph import Graph

def source(graph):
    for i in range(1000):
        yield i

def slow_transform(x):
    import time
    time.sleep(0.001)
    yield x * 2

g = Graph()
g.add_chain(source, slow_transform)

def progress_reporter():
    import time
    while not g.finished:
        print(f"Step: {g.step!r:30s}  progress: {g.progress:3d}%")
        time.sleep(0.05)

reporter = threading.Thread(target=progress_reporter, daemon=True)
reporter.start()
g.run()
reporter.join()
print("Done!")
```

---

## Labelling nodes

By default, node labels are derived from the function name by splitting on `_` and uppercasing each part:  
`load_raw_data` → `"LOAD RAW DATA"`.

Override with the `labels` parameter:

```python
g.add_chain(
    fetch_records,
    clean_data,
    persist_results,
    labels=("Fetch from API", "Clean & validate", "Write to DB"),
)
```

Labels appear in `g.step` during the run and in `node._job.label`.

---

## Threaded execution

Enable concurrent node execution by setting the `YIELDGRAPH_THREADED` environment variable before the run:

=== "Shell"

    ```bash
    YIELDGRAPH_THREADED=1 python my_pipeline.py
    ```

=== "Python"

    ```python
    import os
    os.environ["YIELDGRAPH_THREADED"] = "1"

    g = Graph()
    g.add_chain(fetch, transform, load)
    g.run()     # nodes now run in parallel daemon threads
    ```

In threaded mode:

- All nodes **start simultaneously** as daemon threads.
- Each node **blocks** on `edge.get()` until upstream data arrives.
- When a node finishes it **closes** its output edges, unblocking downstream nodes.
- Edges are automatically thread-safe — `put` / `get` use a `Condition` variable internally.

### When to use threaded mode

| Sequential (default) | Threaded |
|---|---|
| CPU-bound transforms | I/O-bound nodes (network, disk) |
| Simple linear chains | Long pipelines with independent stages |
| Easier to debug | Better throughput for slow I/O steps |

!!! warning "Thread safety in your code"
    yieldgraph's `Edge` queues are thread-safe, but any shared state inside your own functions (e.g. a global counter, a shared file handle) must be protected with a lock.

### Thread-safe fan-out

In threaded mode each branch of a fan-out pipeline gets its **own** `Edge` instance, so branches do not interfere:

```python
os.environ["YIELDGRAPH_THREADED"] = "1"

def source(graph):
    for x in range(10):
        yield x

def branch_a(x):
    yield f"A:{x}"

def branch_b(x):
    yield f"B:{x}"

g = Graph()
g.add_chain(source, branch_a)
g.add_chain(source, branch_b)
g.run()
```

---

## Reusing a graph

A `Graph` instance can be run multiple times. Each call to `run()` resets all node counters and clears the output cache:

```python
g = Graph()
g.add_chain(source, transform)

g.run()
first_output = list(g.output)

# Mutate source data, re-run
g.run()
second_output = list(g.output)
```

!!! note
    `edges` and `nodes` are *not* cleared between runs — only per-run counters (`n_consumed`, `n_produced`, `errors`) are reset by `Node.reset()`.

---

## Using `graph.output` as a lookup table

Because `graph.output` is a plain list, you can use it as a lookup or pass it into a subsequent pipeline:

```python
# Run stage 1
g1 = Graph()
g1.add_chain(extract_ids, fetch_details)
g1.run()

# Pass stage 1 results into stage 2
processed_ids = {row[0] for row in g1.output}

def filter_new(graph):
    for record in all_records:
        if record["id"] not in processed_ids:
            yield record

g2 = Graph()
g2.add_chain(filter_new, enrich, save)
g2.run()
```

---

## Async / `asyncio` — not supported

yieldgraph job functions must be **synchronous**. `async def` functions
and async generators (`async def` + `yield`) are not supported.

### Why

The inner execution loop in `Node._run_one` uses a plain `for` statement
to iterate over each job's output:

```python
for output in self._job(*job_data):   # plain for — cannot drive an AsyncGenerator
    ...
```

Passing an `async def` generator here means the `for` loop never calls
`__anext__`, so the coroutine is never scheduled and the node produces
zero output (the error is caught, logged as a node warning, and
execution continues).

Passing a plain `async def` function (non-generator) is worse: `_as_generator`
wraps it in a sync generator that *returns* the coroutine object as a
value — it is never awaited, and that object propagates as unexpected
output to the next node.

### What to do instead

For **I/O-bound concurrency** (network calls, database queries, file I/O),
enable threaded execution. Each node runs in its own thread, so blocking
I/O inside a regular generator is fine and gives you the same throughput
benefit as `asyncio`:

```python
import os, requests
os.environ["YIELDGRAPH_THREADED"] = "1"

def fetch_url(url):          # plain sync generator — fine in threaded mode
    response = requests.get(url, timeout=10)
    yield response.json()

g = Graph()
g.add_chain(source_urls, fetch_url, parse_response)
g.run()
```

If you are already inside an `asyncio` event loop and need to call
`g.run()`, wrap it in `asyncio.get_event_loop().run_in_executor(None,
g.run)` to avoid blocking the loop.

!!! warning
    There are no plans to add a native `Graph.run_async()` at this time.
    If you have a concrete use-case, please open an issue.
