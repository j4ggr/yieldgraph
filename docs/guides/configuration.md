# Configuration

yieldgraph is configured through environment variables and an optional structured-logging integration. This page documents every knob available.

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `YIELDGRAPH_THREADED` | `""` (off) | Enable concurrent node execution |
| `YIELDGRAPH_LOG_TRACEBACK` | `""` (off) | Include full tracebacks in error log messages |

Both variables are read once at import time via the `ENV` constants object. Set them **before** importing yieldgraph (or at least before calling `g.run()`) to ensure they are picked up.

---

## `YIELDGRAPH_THREADED`

Enable threaded execution so all nodes run concurrently in daemon threads.

**Accepted truthy values** (case-insensitive): `1`, `true`, `yes`, `t`

=== "Shell"

    ```bash
    YIELDGRAPH_THREADED=1 python my_pipeline.py
    ```

=== "Python (before run)"

    ```python
    import os
    os.environ["YIELDGRAPH_THREADED"] = "1"

    from yieldgraph import Graph

    g = Graph()
    g.add_chain(extract, transform, load)
    g.run()
    ```

When the variable is absent or falsy, the pipeline runs sequentially (default).

See [Threaded execution](patterns.md#threaded-execution) in the Patterns guide for a full discussion of when to use threads.

---

## `YIELDGRAPH_LOG_TRACEBACK`

By default, exceptions caught inside node job functions are logged as a short summary (`WARNING` level) without a stack trace. Set this variable to include the full traceback in an additional `ERROR` log message — useful during development or in CI.

**Accepted truthy values** (case-insensitive): `1`, `true`, `yes`, `t`

=== "Shell"

    ```bash
    YIELDGRAPH_LOG_TRACEBACK=1 python my_pipeline.py
    ```

=== "Python"

    ```python
    import os
    os.environ["YIELDGRAPH_LOG_TRACEBACK"] = "1"
    ```

---

## Logging

yieldgraph uses Python's standard `logging` module by default, with an additional custom `TRACE` level (numeric value `5`) below `DEBUG`.

### Log levels

| Level | Numeric | Usage |
|---|---|---|
| `TRACE` | 5 | Very fine-grained internal state (edge routing, cancel checks) |
| `DEBUG` | 10 | Node additions, initial inputs, edge wiring |
| `INFO` | 20 | Job counts, overall pipeline completion summary |
| `WARNING` | 30 | Caught node errors, cancellation notices |
| `ERROR` | 40 | Detailed error messages, tracebacks (when `ENV.LOG_TRACEBACK` is on) |
| `CRITICAL` | 50 | (Reserved for future use) |

### Configuring the standard logger

```python
import logging

# Show INFO and above from yieldgraph
logging.basicConfig(level=logging.INFO)

# Or just yieldgraph's own logger
logging.getLogger("yieldgraph").setLevel(logging.DEBUG)
```

### Using loguru (optional)

If `loguru` is installed, yieldgraph automatically switches to it for richer output:

```bash
pip install loguru
```

```python
from loguru import logger
import sys

# Configure loguru before running the pipeline
logger.remove()
logger.add(sys.stderr, level="DEBUG")

from yieldgraph import Graph

g = Graph()
g.add_chain(source, transform)
g.run()
```

Loguru provides coloured output, structured sinks, and file rotation out of the box — no additional yieldgraph configuration needed.

---

## `LoggingBehavior` mixin

All yieldgraph classes (`Graph`, `Node`, `Job`) inherit from `LoggingBehavior`. You can use the same mixin in your own classes to get consistent log formatting:

```python
from yieldgraph import LoggingBehavior

class MyPipelineStep(LoggingBehavior):

    @property
    def log_title(self) -> str:
        return "MyPipelineStep"     # (1)!

    def process(self, items):
        self.log_info(f"Processing {len(items)} items")
        for item in items:
            self.log_debug(f"  item: {item}")
            # ... do work ...
        self.log_warning("Something looks off but continuing")
```

1. Override `log_title` to customise the prefix in log messages. Defaults to the class name.

### Convenience methods

| Method | Log level |
|---|---|
| `self.log_trace(msg)` | `TRACE` |
| `self.log_debug(msg)` | `DEBUG` |
| `self.log_info(msg)` | `INFO` |
| `self.log_warning(msg)` | `WARNING` |
| `self.log_error(msg)` | `ERROR` |
| `self.log_critical(msg)` | `CRITICAL` |
| `self.log_exception(msg, exc)` | `ERROR` with exception |
| `self.log(msg, level)` | Any level (string or int) |

---

## `ENV` constants

The `ENV` object exposes the current values of environment variables as
boolean properties, evaluated at access time:

```python
from yieldgraph import ENV
import os

# Check whether threaded mode is active
if ENV.THREADED:
    print("Running in threaded mode")

# Toggle at runtime (must be set before g.run())
os.environ[ENV.THREADED_KEY] = "1"
os.environ[ENV.LOG_TRACEBACK_KEY] = "1"
```

| Property | Key constant | Description |
|---|---|---|
| `ENV.THREADED` | `ENV.THREADED_KEY` | `True` when `YIELDGRAPH_THREADED` is set |
| `ENV.LOG_TRACEBACK` | `ENV.LOG_TRACEBACK_KEY` | `True` when `YIELDGRAPH_LOG_TRACEBACK` is set |

---

## `LOG` constants

The `LOG` object provides typed constants for all log levels:

```python
from yieldgraph import LOG, ENV

print(LOG.TRACE)    # 'TRACE'
print(LOG.DEBUG)    # 'DEBUG'
print(LOG.INFO)     # 'INFO'
print(LOG.WARNING)  # 'WARNING'
print(LOG.ERROR)    # 'ERROR'
print(LOG.CRITICAL) # 'CRITICAL'

print(ENV.LOG_TRACEBACK)    # bool — reflects YIELDGRAPH_LOG_TRACEBACK
print(LOG.TRACE_LEVEL)  # 5
```

Use `LOG` constants with the `log()` method for type-safe log level references:

```python
class MyClass(LoggingBehavior):
    def do_work(self):
        self.log("Starting", LOG.INFO)
        self.log("Detail", LOG.TRACE)
```
