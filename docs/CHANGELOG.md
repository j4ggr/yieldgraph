# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

- [Changelog](#changelog)
  - [0.2.0 — 2026-05-05](#020--2026-05-05)
    - [Added](#added)
    - [Changed](#changed)
  - [0.1.0 — 2026-05-04](#010--2026-05-04)
    - [Added](#added-1)
    - [Changed](#changed-1)
    - [Fixed](#fixed)

## [0.2.0] — 2026-05-05

### Added

**`ENV` — live boolean properties** (`8d3c2de`, `201a3a7`)

- `ENV.THREADED` — boolean property; `True` when `YIELDGRAPH_THREADED` is set
  to a truthy value (`1`, `true`, `yes`). Evaluated at call time, so changes to
  `os.environ` are reflected immediately (important for tests).
- `ENV.LOG_TRACEBACK` — boolean property replacing the removed `LOG.TRACEBACK`
  field; reads `YIELDGRAPH_LOG_TRACEBACK` at access time.
- `ENV.LOG_DISABLED` — new boolean property; set `YIELDGRAPH_LOG_DISABLED=1` to
  suppress all library logging output. When `True`, `LoggingBehavior.log()`
  returns immediately without emitting anything.
- `ENV.THREADED_KEY`, `ENV.LOG_TRACEBACK_KEY`, `ENV.LOG_DISABLED_KEY` — class
  attributes exposing raw key strings for direct `os.environ` manipulation
  (e.g. in tests).
- `ENV.TRUEISH_VALUES` — tuple `('1', 'true', 'yes')` centralising the
  truthy-string logic used by all three properties.
- `LOG.__getitem__` — dictionary-style level lookup: `LOG['DEBUG']` → `10`,
  `LOG['TRACE']` → `5`. Raises `KeyError` for unknown names.
- `LOG.__call__` — reverse lookup by integer: `LOG(10)` → `'DEBUG'`.
  Raises `KeyError` for unknown numbers.
- `LOG.*_LEVEL` numeric constants — `LOG.TRACE_LEVEL` (5), `LOG.DEBUG_LEVEL`
  (10), `LOG.INFO_LEVEL` (20), `LOG.WARNING_LEVEL` (30), `LOG.ERROR_LEVEL`
  (40), `LOG.CRITICAL_LEVEL` (50).

**Documentation** (`70a07f5`)

- `node.py` module docstring: `.. warning::` block documenting that `async def`
  generators silently produce zero output and `async def` plain functions yield
  the coroutine object as a value. Points to threaded mode as the correct
  alternative for I/O concurrency.
- `job.py` module docstring: `.. note::` listing supported callable types and
  the two distinct async failure modes.
- *Patterns & Recipes* guide: new **"Async / asyncio — not supported"** section
  explaining both failure modes with code-level detail, the recommended
  `YIELDGRAPH_THREADED=1` workaround, and an `asyncio.run_in_executor` hint for
  callers already inside an event loop.

### Changed

- `_ENV_` converted from a `@dataclass(frozen=True)` of string constants to a
  plain class with `@property` accessors. All properties read `os.environ` on
  every access.
- `LOG.TRACEBACK` field removed; use `ENV.LOG_TRACEBACK` instead.
- `Graph._reset()`: `self._threaded = ENV.THREADED` replaces the inline
  `os.environ.get(…).lower() in (…)` expression.
- `node.py`: `LOG.TRACEBACK` → `ENV.LOG_TRACEBACK`; removed now-unused `LOG`
  import.
- `LoggingBehavior.log()` gains an early-return guard: `if ENV.LOG_DISABLED: return`.
- `LoggingBehavior.name_to_level()` removed; level resolution now done via
  `LOG.__getitem__`.
- `test_config.py` rewritten: dropped `TestNameToLevel`; added `TestLogGetItem`,
  `TestLogCall`, `TestENV` (all three properties, truthy/falsy, runtime changes),
  and `TestLoggingBehavior.test_log_disabled_silences_all_output`.
  Test count: 27 → 43.
- `test_graph.py`: `os.environ[ENV.THREADED]` → `os.environ[ENV.THREADED_KEY]`
  throughout.
- All stale `THREADED_ENV_VAR` and `LOG.TRACEBACK` cross-references removed from
  docstrings in `config.py`, `graph.py`, and `docs/guides/configuration.md`.
- *Configuration* guide `ENV` section rewritten to describe properties and key
  constants; added quick-reference table.

---

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

[0.2.0]: https://github.com/j4ggr/yieldgraph/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/j4ggr/yieldgraph/releases/tag/v0.1.0
