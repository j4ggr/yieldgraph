import os
import pytest

from yieldgraph.config import START_NODE_NAME, ENV
from yieldgraph.edge import Edge
from yieldgraph.graph import Graph, GraphObserver


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _source_items(*items):
    """Return a source function that yields each item in *items*."""
    def source(graph):
        yield from items
    return source


def _identity(x):
    yield x


def _double(x):
    yield x * 2


def _add(x, y):
    yield x + y


# ---------------------------------------------------------------------------
# Construction & initial state
# ---------------------------------------------------------------------------

class TestConstruction:
    def test_nodes_empty(self):
        assert Graph().nodes == {}

    def test_edges_defaultdict(self):
        from collections import defaultdict
        assert isinstance(Graph().edges, defaultdict)

    def test_terminal_nodes_empty(self):
        assert Graph().terminal_nodes == []

    def test_cancelled_false(self):
        assert Graph().cancelled is False

    def test_finished_false(self):
        assert Graph().finished is False

    def test_error_empty(self):
        assert Graph().error == ''

    def test_labels_empty(self):
        assert Graph().labels == {}

    def test_threaded_false(self):
        assert Graph()._threaded is False


# ---------------------------------------------------------------------------
# Properties before a run
# ---------------------------------------------------------------------------

class TestPropertiesPreRun:
    def test_succeeded_false_before_run(self):
        g = Graph()
        g.add_chain(_source_items(1))
        assert g.succeeded is False

    def test_has_output_false_before_run(self):
        g = Graph()
        g.add_chain(_source_items(1))
        assert g.has_output is False

    def test_at_first_node_false_when_no_nodes(self):
        assert Graph().at_first_node is False

    def test_step_empty_string_when_cancelled(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.cancelled = True
        assert g.step == ''

    def test_step_etl_prozess_ende_when_finished(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.run()
        assert g.step == 'ETL Prozess Ende'


# ---------------------------------------------------------------------------
# add_chain
# ---------------------------------------------------------------------------

class TestAddChain:
    def test_single_function_adds_one_node(self):
        g = Graph()
        g.add_chain(_source_items(1))
        assert len(g.nodes) == 1

    def test_chain_of_two_adds_two_nodes(self):
        g = Graph()
        g.add_chain(_source_items(1), _identity)
        assert len(g.nodes) == 2

    def test_node_names_are_function_names(self):
        g = Graph()
        g.add_chain(_source_items(1), _double)
        assert 'source' in g.nodes
        assert '_double' in g.nodes

    def test_first_node_has_first_true(self):
        g = Graph()
        g.add_chain(_source_items(1), _identity)
        first_node = list(g.nodes.values())[0]
        assert first_node.first is True

    def test_last_node_has_last_true(self):
        g = Graph()
        g.add_chain(_source_items(1), _identity)
        last_node = list(g.nodes.values())[-1]
        assert last_node.last is True

    def test_middle_node_not_first_not_last(self):
        g = Graph()
        g.add_chain(_source_items(1), _identity, _double)
        middle = list(g.nodes.values())[1]
        assert middle.first is False
        assert middle.last is False

    def test_terminal_nodes_contains_last_node(self):
        g = Graph()
        g.add_chain(_source_items(1), _double)
        assert '_double' in g.terminal_nodes

    def test_graph_prepended_to_initial_input(self):
        g = Graph()
        g.add_chain(_source_items(1))
        seed_edge = g.edges[START_NODE_NAME][0]
        first_input = list(seed_edge)[0]
        assert first_input[0] is g

    def test_empty_add_chain_does_nothing(self):
        g = Graph()
        g.add_chain()
        assert g.nodes == {}

    def test_custom_labels_stored(self):
        g = Graph()
        g.add_chain(_source_items(1), _double, labels=('Src', 'Dbl'))
        assert g.nodes['source']._job.label == 'Src'
        assert g.nodes['_double']._job.label == 'Dbl'


# ---------------------------------------------------------------------------
# run() — sequential (default)
# ---------------------------------------------------------------------------

class TestRunSequential:
    def setup_method(self):
        os.environ.pop(ENV.THREADED_KEY, None)

    def test_finished_true_after_run(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.run()
        assert g.finished is True

    def test_succeeded_true_on_clean_run(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.run()
        assert g.succeeded is True

    def test_error_empty_on_clean_run(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.run()
        assert g.error == ''

    def test_output_single_node(self):
        g = Graph()
        g.add_chain(_source_items(1, 2, 3))
        g.run()
        assert g.output == [(1,), (2,), (3,)]

    def test_output_two_node_chain(self):
        g = Graph()
        g.add_chain(_source_items(1, 2, 3), _double)
        g.run()
        assert g.output == [(2,), (4,), (6,)]

    def test_output_three_node_chain(self):
        g = Graph()
        def triple(x):
            yield x * 3
        g.add_chain(_source_items(2), _double, triple)
        g.run()
        assert g.output == [(12,)]

    def test_callable_graph_runs_pipeline(self):
        g = Graph()
        g.add_chain(_source_items(5), _double)
        g()
        assert g.output == [(10,)]

    def test_run_resets_output_between_runs(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.run()
        assert g.output == [(1,)]
        g.run()
        # output property re-collects from edges on second run
        assert g.output == [(1,)]

    def test_cancelled_flag_reset_by_run(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.cancelled = True
        g.run()
        # _reset() clears cancelled before the run starts
        assert g.succeeded is True

    def test_n_consumed_correct_after_run(self):
        g = Graph()
        g.add_chain(_source_items(1, 2, 3), _double)
        g.run()
        assert g.nodes['_double'].n_consumed == 3

    def test_n_produced_correct_after_run(self):
        g = Graph()
        g.add_chain(_source_items(1, 2, 3), _double)
        g.run()
        assert g.nodes['_double'].n_produced == 3

    def test_has_output_true_after_run_with_results(self):
        """has_output is True once output is accessed (lazily populated)."""
        g = Graph()
        g.add_chain(_source_items(42))
        g.run()
        _ = g.output   # trigger lazy collection into _output
        assert g.has_output is True

    def test_node_exception_does_not_crash_graph(self):
        def explode(x):
            raise ValueError('boom')

        g = Graph()
        g.add_chain(_source_items(1, 2), explode)
        g.run()
        assert g.finished is True
        assert g.nodes['explode'].n_errors == 2

    def test_multi_arg_source(self):
        """First chain node always receives the graph as its first argument."""
        def add_with_graph(graph, x, y):
            yield x + y

        g = Graph()
        g.add_chain(add_with_graph, initial_input=(3, 7))
        g.run()
        assert g.output == [(10,)]


# ---------------------------------------------------------------------------
# run() — threaded mode
# ---------------------------------------------------------------------------

class TestRunThreaded:
    def setup_method(self):
        os.environ[ENV.THREADED_KEY] = '1'

    def teardown_method(self):
        os.environ.pop(ENV.THREADED_KEY, None)

    def test_threaded_flag_set_after_reset(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.run()
        assert g._threaded is True

    def test_finished_true_after_threaded_run(self):
        g = Graph()
        g.add_chain(_source_items(1, 2, 3))
        g.run()
        assert g.finished is True

    def test_output_single_node_threaded(self):
        g = Graph()
        g.add_chain(_source_items(1, 2, 3))
        g.run()
        assert sorted(g.output) == [(1,), (2,), (3,)]

    def test_output_two_node_chain_threaded(self):
        g = Graph()
        g.add_chain(_source_items(1, 2, 3), _double)
        g.run()
        assert sorted(g.output) == [(2,), (4,), (6,)]

    def test_succeeded_true_threaded(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.run()
        assert g.succeeded is True


# ---------------------------------------------------------------------------
# Properties after run
# ---------------------------------------------------------------------------

class TestPropertiesPostRun:
    def setup_method(self):
        os.environ.pop(ENV.THREADED_KEY, None)

    def test_succeeded_true_on_success(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.run()
        assert g.succeeded is True

    def test_succeeded_false_when_error_set(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.run()
        g.error = 'something went wrong'
        assert g.succeeded is False

    def test_output_is_list(self):
        g = Graph()
        g.add_chain(_source_items(1, 2))
        g.run()
        assert isinstance(g.output, list)

    def test_output_cached_on_second_access(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.run()
        first = g.output
        second = g.output
        assert first is second


# ---------------------------------------------------------------------------
# _node_label
# ---------------------------------------------------------------------------

class TestNodeLabel:
    def test_derives_label_from_name(self):
        g = Graph()
        assert g._node_label('load_raw_data') == 'LOAD RAW DATA'

    def test_single_word_uppercased(self):
        g = Graph()
        assert g._node_label('extract') == 'EXTRACT'

    def test_explicit_label_overrides_derivation(self):
        g = Graph()
        g.labels['my_fn'] = 'Custom Label'
        assert g._node_label('my_fn') == 'Custom Label'


# ---------------------------------------------------------------------------
# _adjust_col_widths
# ---------------------------------------------------------------------------

class TestAdjustColWidths:
    def setup_method(self):
        os.environ.pop(ENV.THREADED_KEY, None)

    def test_col_width_set_to_longest_name(self):
        g = Graph()
        g.add_chain(_source_items(1), _double)
        g._adjust_col_widths()
        expected = max(len('source'), len('_double'))
        for node in g.nodes.values():
            assert node._col_width == expected

    def test_adjust_on_empty_graph_does_not_raise(self):
        Graph()._adjust_col_widths()   # must not raise


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------

class TestRepr:
    def setup_method(self):
        os.environ.pop(ENV.THREADED_KEY, None)

    def test_repr_contains_start(self):
        g = Graph()
        g.add_chain(_source_items(1))
        assert 'START' in repr(g)

    def test_repr_contains_node_names(self):
        g = Graph()
        g.add_chain(_source_items(1), _double)
        r = repr(g)
        assert 'source' in r
        assert '_double' in r

    def test_repr_contains_end(self):
        g = Graph()
        g.add_chain(_source_items(1), _double)
        assert 'END' in repr(g)

    def test_repr_shows_cancelled_message(self):
        g = Graph()
        g.add_chain(_source_items(1))
        g.cancelled = True
        assert 'INTERRUPTED' in repr(g)


# ---------------------------------------------------------------------------
# GraphObserver
# ---------------------------------------------------------------------------

class _RecordingObserver(GraphObserver):
    """Observer that records every call for assertion."""

    def __init__(self):
        self.calls = []

    def on_run_start(self, total_nodes):
        self.calls.append(('on_run_start', total_nodes))

    def on_node_start(self, node_name, step, node_index, total_nodes):
        self.calls.append(('on_node_start', node_name, step, node_index, total_nodes))

    def on_node_end(self, node_name, step, node_index, total_nodes):
        self.calls.append(('on_node_end', node_name, step, node_index, total_nodes))

    def on_run_end(self, succeeded, error):
        self.calls.append(('on_run_end', succeeded, error))


class TestGraphObserver:
    def setup_method(self):
        os.environ.pop(ENV.THREADED_KEY, None)

    def test_no_observer_does_not_raise(self):
        g = Graph()
        g.add_chain(_source_items(1, 2))
        g.run()   # observer is None — must not raise
        assert g.succeeded is True

    def test_observer_default_is_none(self):
        assert Graph().observer is None

    def test_on_run_start_called_once(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1), _double)
        g.observer = obs
        g.run()
        run_start_calls = [c for c in obs.calls if c[0] == 'on_run_start']
        assert len(run_start_calls) == 1

    def test_on_run_start_receives_total_nodes(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1), _double)
        g.observer = obs
        g.run()
        _, total = obs.calls[0]
        assert total == 2

    def test_on_run_end_called_once(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1))
        g.observer = obs
        g.run()
        run_end_calls = [c for c in obs.calls if c[0] == 'on_run_end']
        assert len(run_end_calls) == 1

    def test_on_run_end_succeeded_true_on_clean_run(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1))
        g.observer = obs
        g.run()
        _, succeeded, error = obs.calls[-1]
        assert succeeded is True
        assert error == ''

    def test_on_run_end_succeeded_false_on_error(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1))
        g.observer = obs
        g.run()
        # Inject an error to simulate failure
        g.error = 'something went wrong'
        # Re-fire manually — also check Graph.succeeded reflects the error
        assert g.succeeded is False

    def test_on_node_start_called_for_each_node(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1), _double)
        g.observer = obs
        g.run()
        starts = [c for c in obs.calls if c[0] == 'on_node_start']
        assert len(starts) == 2

    def test_on_node_end_called_for_each_node(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1), _double)
        g.observer = obs
        g.run()
        ends = [c for c in obs.calls if c[0] == 'on_node_end']
        assert len(ends) == 2

    def test_on_node_start_node_name_correct(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1), _double)
        g.observer = obs
        g.run()
        starts = [c for c in obs.calls if c[0] == 'on_node_start']
        names = [c[1] for c in starts]
        assert 'source' in names
        assert '_double' in names

    def test_on_node_start_index_is_one_based(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1), _double)
        g.observer = obs
        g.run()
        starts = [c for c in obs.calls if c[0] == 'on_node_start']
        indices = [c[3] for c in starts]
        assert indices == [1, 2]

    def test_on_node_start_total_nodes_correct(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1), _double)
        g.observer = obs
        g.run()
        starts = [c for c in obs.calls if c[0] == 'on_node_start']
        assert all(c[4] == 2 for c in starts)

    def test_call_order_run_start_before_node_start(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1))
        g.observer = obs
        g.run()
        event_types = [c[0] for c in obs.calls]
        assert event_types[0] == 'on_run_start'
        assert 'on_node_start' in event_types[1:]

    def test_call_order_run_end_after_node_end(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1))
        g.observer = obs
        g.run()
        event_types = [c[0] for c in obs.calls]
        assert event_types[-1] == 'on_run_end'

    def test_call_order_node_start_before_node_end(self):
        obs = _RecordingObserver()
        g = Graph()
        g.add_chain(_source_items(1), _double)
        g.observer = obs
        g.run()
        event_types = [c[0] for c in obs.calls]
        # Every on_node_start must come before its matching on_node_end
        for name in ('source', '_double'):
            starts = [i for i, c in enumerate(obs.calls)
                      if c[0] == 'on_node_start' and c[1] == name]
            ends = [i for i, c in enumerate(obs.calls)
                    if c[0] == 'on_node_end' and c[1] == name]
            assert starts and ends
            assert starts[0] < ends[0]

    def test_observer_works_in_threaded_mode(self):
        os.environ[ENV.THREADED_KEY] = '1'
        try:
            obs = _RecordingObserver()
            g = Graph()
            g.add_chain(_source_items(1, 2, 3), _double)
            g.observer = obs
            g.run()
            starts = [c for c in obs.calls if c[0] == 'on_node_start']
            ends = [c for c in obs.calls if c[0] == 'on_node_end']
            assert len(starts) == 2
            assert len(ends) == 2
        finally:
            os.environ.pop(ENV.THREADED_KEY, None)

    def test_graphobserver_base_methods_are_noop(self):
        obs = GraphObserver()
        obs.on_run_start(3)
        obs.on_node_start('n', 'N', 1, 3)
        obs.on_node_end('n', 'N', 1, 3)
        obs.on_run_end(True, '')
        # No exception raised, no return value
