# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

- [Changelog](#changelog)
  - [0.1.0 — 2026-05-04](#010--2026-05-04)
    - [Added](#added)
    - [Changed](#changed)
    - [Fixed](#fixed)

## [0.1.0] — 2026-05-04

First public release. The library was extracted from a monolithic `main.py` and
restructured as an installable package under `src/yieldgraph/`.

### Added

**Core library**

- `Graph` — directed graph that owns and executes an ordered collection of
  `Node` objects. Supports sequential and threaded execution, cooperative
  cancellation, and fan-out branching via `add_chain`.
- `Node` — single processing step that pulls items from incoming `Edge` queues,
  runs its `Job`, and fans results to all outgoing edges. Exposes per-run counters
  (`n_consumed`, `n_produced`, `n_queued`), a `progress` fraction, and a
  caught-exception list (`errors`).
- `Job` — execution wrapper around any callable. Normalises plain functions and
  generator functions into a uniform, cancellable generator protocol via the
  module-level helpers `_as_generator` and `_wrap`.
- `Edge` — `deque`-based directed queue connecting two nodes. Added thread-safe
  `put`, `get`, and `close` methods backed by a `threading.Condition` for use
  in threaded pipelines.
- `LoggingBehavior` — mixin class providing consistent `log`, `log_trace`,
  `log_debug`, `log_info`, `log_warning`, `log_error`, `log_critical`, and
  `log_exception` methods. Automatically switches to `loguru` when available.
- `LOG` — frozen dataclass with typed constants for all log levels (`TRACE`,
  `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`) and the `TRACEBACK` flag.
- `ENV` — frozen dataclass exposing environment variable name constants
  (`YIELDGRAPH_THREADED`, `YIELDGRAPH_LOG_TRACEBACK`).
- `START_NODE_NAME` — sentinel string `'__START__'` that identifies the implicit
  pipeline entry edge.
- Custom `TRACE` log level (numeric `5`) registered with the standard `logging`
  module.

**Threaded execution mode**

- `Graph._run_threaded` — executes all nodes as concurrent daemon threads; seed
  edges are pre-filled and closed before threads start.
- `Node.process_streaming` — streaming variant of `Node.process` used in threaded
  mode; blocks on `Edge.get` until upstream data arrives then closes output edges
  on completion.
- `Edge.put` / `Edge.get` / `Edge.close` — thread-safe producer/consumer API.
- `YIELDGRAPH_THREADED` environment variable controls execution mode at runtime.

**Test suite** (168 tests across 4 modules)

- `tests/test_config.py` — 27 tests for `LOG`, `LoggingBehavior`, and
  `START_NODE_NAME`.
- `tests/test_job.py` — 27 tests for `Job`, `_as_generator`, and `_wrap`
  including cancellation and `running` flag behaviour.
- `tests/test_node.py` — 59 tests for `Node` construction, properties,
  `process`, `_run_one`, `_fan_out`, error collection, and cancellation.
- `tests/test_graph.py` — 55 tests for `Graph` construction, `add_chain`,
  sequential run, threaded run, fan-out, cancellation, and output collection.

**Documentation**

- MkDocs + Material site with GitHub Pages deployment via CI.
- Landing page with feature grid, Mermaid architecture diagram, and annotated
  quick-start.
- *Getting Started* guide — sources, transforms, tuple model, multi-step chains,
  output consumption.
- *Patterns & Recipes* guide — fan-out, `attach_to`, `initial_input`, error
  handling, cooperative cancellation, progress monitoring, threaded mode, node
  labelling, graph reuse.
- *Configuration* guide — environment variables, log levels, `LoggingBehavior`
  mixin, `ENV` and `LOG` constant objects.
- Full API reference auto-generated from docstrings via `mkdocstrings`.
- CI workflow (`.github/workflows/ci.yml`) — runs pytest and deploys docs to
  GitHub Pages on every push to `main`.

### Changed

- Restructured from a single `main.py` into a proper Python package under
  `src/yieldgraph/` with `__init__.py`, `config.py`, `edge.py`, `job.py`,
  `node.py`, and `graph.py`.
- Renamed internal symbols across `Node` and `Job` for clarity (e.g.
  `THREADED_ENV_VAR` -> `ENV.THREADED`).
- All docstrings rewritten to NumPy style with full parameter, return, and
  example sections.
- `Node.process` now accepts explicit `edges_in` / `edges_out` lists instead of
  reading from graph-level attributes.

### Fixed

- `_ensure_tuple` helper extracted from `Node` so it can be tested and used
  independently; fixed edge case where a bare tuple value was double-wrapped.
- `Node._run_one` now correctly sets `graph.cancelled` on `KeyboardInterrupt`
  instead of re-raising, allowing the pipeline to exit cleanly.
- `Edge.get` correctly returns `None` when the edge is closed and empty,
  preventing consumer threads from blocking indefinitely.

---

[0.1.0]: https://github.com/j4ggr/yieldgraph/releases/tag/v0.1.0
