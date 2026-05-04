import pytest

from yieldgraph.edge import Edge
from yieldgraph.node import Node, _ensure_tuple


# ---------------------------------------------------------------------------
# Helpers shared across test classes
# ---------------------------------------------------------------------------

class _MockGraph:
    """Minimal stand-in for Graph that Node reads for cancellation."""

    def __init__(self, cancelled: bool = False) -> None:
        self.cancelled = cancelled


def _make_node(fn, *, inputs_from='upstream', label='', first=True,
               last=False, cancelled=False):
    graph = _MockGraph(cancelled=cancelled)
    return Node(graph, fn, inputs_from=inputs_from, label=label,
                first=first, last=last)


def _edge(*items) -> Edge:
    """Return an Edge pre-filled with *items* (each item must be a tuple)."""
    return Edge(list(items))


# ---------------------------------------------------------------------------
# _ensure_tuple
# ---------------------------------------------------------------------------

class TestEnsureTuple:
    def test_int_wrapped(self):
        assert _ensure_tuple(42) == (42,)

    def test_string_wrapped(self):
        assert _ensure_tuple('hello') == ('hello',)

    def test_none_wrapped(self):
        assert _ensure_tuple(None) == (None,)

    def test_tuple_returned_unchanged(self):
        t = (1, 2, 3)
        assert _ensure_tuple(t) is t

    def test_empty_tuple_returned_unchanged(self):
        assert _ensure_tuple(()) == ()

    def test_nested_tuple_not_double_wrapped(self):
        assert _ensure_tuple(((1,),)) == ((1,),)

    def test_list_wrapped(self):
        lst = [1, 2]
        assert _ensure_tuple(lst) == ([1, 2],)


# ---------------------------------------------------------------------------
# Node construction
# ---------------------------------------------------------------------------

class TestNodeConstruction:
    def test_name_from_function(self):
        def my_fn(x): return x
        node = _make_node(my_fn)
        assert node.name == 'my_fn'

    def test_inputs_from_stored(self):
        node = _make_node(lambda x: x, inputs_from='source')
        assert node.inputs_from == 'source'

    def test_first_default_true(self):
        assert _make_node(lambda x: x).first is True

    def test_last_default_false(self):
        assert _make_node(lambda x: x).last is False

    def test_first_and_last_custom(self):
        node = _make_node(lambda x: x, first=False, last=True)
        assert node.first is False
        assert node.last is True

    def test_reset_called_on_init(self):
        node = _make_node(lambda x: x)
        assert node.n_consumed == 0
        assert node.n_produced == 0
        assert node.errors == []
        assert node.outputs == []


# ---------------------------------------------------------------------------
# reset()
# ---------------------------------------------------------------------------

class TestReset:
    def test_counters_zeroed(self):
        node = _make_node(lambda x: x)
        node.n_consumed = 5
        node.n_produced = 3
        node.reset()
        assert node.n_consumed == 0
        assert node.n_produced == 0

    def test_n_queued_zeroed(self):
        node = _make_node(lambda x: x)
        node.n_queued = 7
        node.reset()
        assert node.n_queued == 0

    def test_errors_cleared(self):
        node = _make_node(lambda x: x)
        node.errors.append(RuntimeError('oops'))
        node.reset()
        assert node.errors == []

    def test_outputs_cleared(self):
        node = _make_node(lambda x: x)
        node.outputs = [Edge()]
        node.reset()
        assert node.outputs == []

    def test_info_cleared(self):
        node = _make_node(lambda x: x)
        node.info = 'something'
        node.reset()
        assert node.info == ''

    def test_errors_are_fresh_list_per_instance(self):
        n1 = _make_node(lambda x: x)
        n2 = _make_node(lambda x: x)
        n1.errors.append(RuntimeError('x'))
        assert n2.errors == []


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

class TestProperties:
    def test_n_errors_zero_initially(self):
        assert _make_node(lambda x: x).n_errors == 0

    def test_n_errors_counts_errors_list(self):
        node = _make_node(lambda x: x)
        node.errors.append(ValueError('a'))
        node.errors.append(TypeError('b'))
        assert node.n_errors == 2

    def test_inputs_returns_empty_edge_when_no_inputs(self):
        node = _make_node(lambda x: x)
        result = node.inputs
        assert isinstance(result, Edge)
        assert len(result) == 0

    def test_inputs_setter_selects_first_nonempty_edge(self):
        node = _make_node(lambda x: x)
        e1 = Edge()           # empty
        e2 = _edge((1,))      # has item
        node.inputs = [e1, e2]
        assert node.inputs is e2
        assert node._active_edge_index == 1

    def test_inputs_setter_selects_first_edge_when_all_nonempty(self):
        node = _make_node(lambda x: x)
        e1 = _edge((1,))
        e2 = _edge((2,))
        node.inputs = [e1, e2]
        assert node.inputs is e1
        assert node._active_edge_index == 0

    def test_input_count_reflects_active_edge(self):
        node = _make_node(lambda x: x)
        node.inputs = [_edge((1,), (2,), (3,))]
        assert node.input_count == 3

    def test_output_count_zero_when_no_edges(self):
        node = _make_node(lambda x: x)
        # reset() leaves outputs=[], so output_count=0
        assert node.output_count == 0

    def test_output_count_equals_number_of_edges(self):
        node = _make_node(lambda x: x)
        node.outputs = [Edge(), Edge()]
        assert node.output_count == 2

    def test_outputs_empty_true_when_no_outputs(self):
        node = _make_node(lambda x: x)
        assert node.outputs_empty is True

    def test_outputs_empty_true_when_all_edges_empty(self):
        node = _make_node(lambda x: x)
        node.outputs = [Edge(), Edge()]
        assert node.outputs_empty is True

    def test_outputs_empty_false_when_edge_has_items(self):
        node = _make_node(lambda x: x)
        node.outputs = [Edge(), _edge((1,))]
        assert node.outputs_empty is False

    def test_progress_half_when_n_queued_zero(self):
        node = _make_node(lambda x: x)
        assert node.progress == 0.5

    def test_progress_fraction(self):
        node = _make_node(lambda x: x)
        node.n_queued = 4
        node.n_consumed = 2
        assert node.progress == 0.5

    def test_progress_clamped_to_one(self):
        node = _make_node(lambda x: x)
        node.n_queued = 3
        node.n_consumed = 5   # more than queued → clamp
        assert node.progress == 1.0

    def test_progress_minimum_is_point_zero_one(self):
        node = _make_node(lambda x: x)
        node.n_queued = 100
        node.n_consumed = 0
        assert node.progress == 0.01


# ---------------------------------------------------------------------------
# process()
# ---------------------------------------------------------------------------

class TestProcess:
    def test_plain_function_output_on_edge(self):
        node = _make_node(lambda x: x * 2)
        edge_out = Edge()
        node.process([_edge((3,))], [edge_out])
        assert list(edge_out) == [(6,)]

    def test_generator_function_multiple_outputs(self):
        def double_and_triple(x):
            yield x * 2
            yield x * 3

        node = _make_node(double_and_triple)
        edge_out = Edge()
        node.process([_edge((4,))], [edge_out])
        assert list(edge_out) == [(8,), (12,)]

    def test_n_consumed_matches_input_count(self):
        node = _make_node(lambda x: x)
        node.process([_edge((1,), (2,), (3,))], [Edge()])
        assert node.n_consumed == 3

    def test_n_produced_matches_output_count(self):
        node = _make_node(lambda x: x)
        edge_out = Edge()
        node.process([_edge((1,), (2,))], [edge_out])
        assert node.n_produced == 2

    def test_fan_out_to_multiple_edges(self):
        node = _make_node(lambda x: x)
        e1, e2 = Edge(), Edge()
        node.process([_edge((42,))], [e1, e2])
        assert list(e1) == [(42,)]
        assert list(e2) == [(42,)]

    def test_exception_caught_and_stored(self):
        def boom(x):
            raise ValueError('bad')

        node = _make_node(boom)
        node.process([_edge((1,))], [Edge()])
        assert node.n_errors == 1
        assert isinstance(node.errors[0], ValueError)

    def test_exception_does_not_stop_subsequent_items(self):
        call_count = [0]

        def sometimes_fails(x):
            call_count[0] += 1
            if x == 2:
                raise RuntimeError('skip 2')
            yield x

        node = _make_node(sometimes_fails)
        edge_out = Edge()
        node.process([_edge((1,), (2,), (3,))], [edge_out])
        assert node.n_consumed == 3
        assert node.n_errors == 1
        assert list(edge_out) == [(1,), (3,)]

    def test_n_consumed_incremented_even_on_exception(self):
        def always_fails(x):
            raise RuntimeError()

        node = _make_node(always_fails)
        node.process([_edge((1,), (2,))], [Edge()])
        assert node.n_consumed == 2

    def test_cancelled_graph_skips_items(self):
        call_count = [0]

        def count(x):
            call_count[0] += 1
            yield x

        node = _make_node(count, cancelled=True)
        edge_out = Edge()
        node.process([_edge((1,), (2,), (3,))], [edge_out])
        assert call_count[0] == 0
        assert len(edge_out) == 0

    def test_processing_first_flag(self):
        flags = []

        def capture(x):
            flags.append(node._processing_first)
            yield x

        node = _make_node(capture)
        node.process([_edge((1,), (2,), (3,))], [Edge()])
        assert flags[0] is True
        assert flags[1] is False
        assert flags[2] is False

    def test_processing_last_flag(self):
        flags = []

        def capture(x):
            flags.append(node._processing_last)
            yield x

        node = _make_node(capture)
        node.process([_edge((1,), (2,), (3,))], [Edge()])
        assert flags[0] is False
        assert flags[1] is False
        assert flags[2] is True

    def test_output_tuple_passthrough(self):
        """Tuples yielded by the job are not double-wrapped."""
        def make_pair(x):
            yield (x, x * 10)

        node = _make_node(make_pair)
        edge_out = Edge()
        node.process([_edge((5,))], [edge_out])
        assert list(edge_out) == [(5, 50)]


# ---------------------------------------------------------------------------
# process_streaming()
# ---------------------------------------------------------------------------

class TestProcessStreaming:
    def _run(self, fn, items, cancelled=False):
        """Build a closed input edge, run process_streaming, return output."""
        node = _make_node(fn, cancelled=cancelled)
        edge_in = Edge()
        for item in items:
            edge_in.append(item)
        edge_in.close()
        edge_out = Edge()
        node.process_streaming([edge_in], [edge_out])
        return node, list(edge_out)

    def test_consumes_all_items_from_closed_edge(self):
        _, out = self._run(lambda x: x, [(1,), (2,), (3,)])
        assert out == [(1,), (2,), (3,)]

    def test_n_consumed_correct(self):
        node, _ = self._run(lambda x: x, [(1,), (2,)])
        assert node.n_consumed == 2

    def test_n_produced_correct(self):
        node, _ = self._run(lambda x: x, [(1,), (2,)])
        assert node.n_produced == 2

    def test_cancelled_graph_stops_processing(self):
        call_count = [0]

        def count(x):
            call_count[0] += 1
            yield x

        _, out = self._run(count, [(1,), (2,), (3,)], cancelled=True)
        assert call_count[0] == 0
        assert out == []

    def test_processing_first_set_for_first_item(self):
        flags = []

        def capture(x):
            flags.append(node._processing_first)
            yield x

        node = _make_node(capture)
        edge_in = Edge([(1,), (2,)])
        edge_in.close()
        edge_out = Edge()
        node.process_streaming([edge_in], [edge_out])
        assert flags[0] is True
        assert flags[1] is False

    def test_processing_last_always_false(self):
        flags = []

        def capture(x):
            flags.append(node._processing_last)
            yield x

        node = _make_node(capture)
        edge_in = Edge([(1,), (2,), (3,)])
        edge_in.close()
        node.process_streaming([edge_in], [Edge()])
        assert all(f is False for f in flags)


# ---------------------------------------------------------------------------
# __str__ and __repr__
# ---------------------------------------------------------------------------

class TestDunder:
    def test_str_contains_name(self):
        def my_step(x): return x
        node = _make_node(my_step, inputs_from='prev')
        assert 'my_step' in str(node)

    def test_str_contains_inputs_from(self):
        node = _make_node(lambda x: x, inputs_from='prev_node')
        assert 'prev_node' in str(node)

    def test_repr_contains_counts(self):
        node = _make_node(lambda x: x)
        node.process([_edge((1,), (2,))], [Edge()])
        r = repr(node)
        # consumed=2, produced=2, errors=0
        assert '2' in r

    def test_repr_first_node_shows_inputs_from_header(self):
        node = _make_node(lambda x: x, inputs_from='__START__', first=True)
        assert '__START__' in repr(node)

    def test_repr_non_first_node_no_header(self):
        node = _make_node(lambda x: x, inputs_from='prev', first=False)
        assert 'prev' not in repr(node)

    def test_len_first_node_returns_input_edge_length(self):
        node = _make_node(lambda x: x, first=True)
        node.inputs = [_edge((1,), (2,), (3,))]
        assert len(node) == 3

    def test_len_non_first_node_returns_input_count(self):
        node = _make_node(lambda x: x, first=False)
        node.inputs = [_edge((1,), (2,))]
        assert len(node) == 2
