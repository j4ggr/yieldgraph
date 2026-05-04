import threading
import time

import pytest

from yieldgraph.edge import Edge


# ---------------------------------------------------------------------------
# Sequential / deque interface
# ---------------------------------------------------------------------------

class TestSequentialInterface:
    def test_empty_on_creation(self):
        assert len(Edge()) == 0

    def test_init_with_iterable(self):
        e = Edge([(1,), (2,), (3,)])
        assert list(e) == [(1,), (2,), (3,)]

    def test_append_and_popleft(self):
        e = Edge()
        e.append((42,))
        assert e.popleft() == (42,)

    def test_multiple_appends_preserve_order(self):
        e = Edge()
        for i in range(5):
            e.append((i,))
        assert [e.popleft() for _ in range(5)] == [(i,) for i in range(5)]

    def test_len_after_appends(self):
        e = Edge()
        e.append('a')
        e.append('b')
        assert len(e) == 2

    def test_bool_false_when_empty(self):
        assert not Edge()

    def test_bool_true_when_not_empty(self):
        e = Edge()
        e.append(1)
        assert e

    def test_iterable_protocol(self):
        e = Edge([(1,), (2,)])
        assert list(e) == [(1,), (2,)]

    def test_is_deque_subclass(self):
        from collections import deque
        assert isinstance(Edge(), deque)


# ---------------------------------------------------------------------------
# closed property & close()
# ---------------------------------------------------------------------------

class TestClose:
    def test_not_closed_initially(self):
        assert Edge().closed is False

    def test_closed_after_close(self):
        e = Edge()
        e.close()
        assert e.closed is True

    def test_close_is_idempotent(self):
        e = Edge()
        e.close()
        e.close()  # must not raise
        assert e.closed is True

    def test_close_does_not_clear_existing_items(self):
        e = Edge([(1,), (2,)])
        e.close()
        assert len(e) == 2


# ---------------------------------------------------------------------------
# put()
# ---------------------------------------------------------------------------

class TestPut:
    def test_put_appends_item(self):
        e = Edge()
        e.put((7,))
        assert len(e) == 1
        assert e.popleft() == (7,)

    def test_put_multiple_items_in_order(self):
        e = Edge()
        e.put((1,))
        e.put((2,))
        e.put((3,))
        assert list(e) == [(1,), (2,), (3,)]

    def test_put_accepts_any_type(self):
        e = Edge()
        e.put('hello')
        e.put(42)
        e.put(None)
        assert list(e) == ['hello', 42, None]


# ---------------------------------------------------------------------------
# get() — non-blocking cases (item already present or closed)
# ---------------------------------------------------------------------------

class TestGetNonBlocking:
    def test_get_returns_item_when_present(self):
        e = Edge()
        e.put((99,))
        assert e.get() == (99,)

    def test_get_drains_in_fifo_order(self):
        e = Edge([(1,), (2,), (3,)])
        assert e.get() == (1,)
        assert e.get() == (2,)
        assert e.get() == (3,)

    def test_get_returns_none_when_closed_and_empty(self):
        e = Edge()
        e.close()
        assert e.get() is None

    def test_get_drains_remaining_items_after_close(self):
        e = Edge([(1,), (2,)])
        e.close()
        assert e.get() == (1,)
        assert e.get() == (2,)
        assert e.get() is None   # now empty + closed

    def test_get_with_timeout_returns_none_on_empty_open_edge(self):
        e = Edge()
        result = e.get(timeout=0.05)
        assert result is None

    def test_get_with_timeout_returns_item_if_present(self):
        e = Edge()
        e.put((5,))
        assert e.get(timeout=1.0) == (5,)


# ---------------------------------------------------------------------------
# get() — blocking / threading cases
# ---------------------------------------------------------------------------

class TestGetThreaded:
    def test_consumer_receives_items_produced_by_thread(self):
        e = Edge()
        received = []

        def producer():
            for i in range(5):
                e.put((i,))
            e.close()

        t = threading.Thread(target=producer)
        t.start()

        while True:
            item = e.get()
            if item is None:
                break
            received.append(item)

        t.join()
        assert received == [(i,) for i in range(5)]

    def test_get_blocks_until_put_delivers_item(self):
        e = Edge()
        results = []

        def slow_producer():
            time.sleep(0.05)
            e.put(('delivered',))
            e.close()

        t = threading.Thread(target=slow_producer)
        t.start()
        item = e.get()   # should block until slow_producer wakes it
        results.append(item)
        t.join()

        assert results == [('delivered',)]

    def test_multiple_consumers_each_get_one_item(self):
        """put()/get() are exclusive — each item is consumed exactly once."""
        e = Edge()
        collected = []
        lock = threading.Lock()

        def consumer():
            item = e.get()
            if item is not None:
                with lock:
                    collected.append(item)

        e.put((1,))
        e.put((2,))

        t1 = threading.Thread(target=consumer)
        t2 = threading.Thread(target=consumer)
        t1.start(); t2.start()
        t1.join();  t2.join()

        assert sorted(collected) == [(1,), (2,)]

    def test_close_unblocks_waiting_consumer(self):
        e = Edge()
        result = []

        def consumer():
            item = e.get()   # will block until close()
            result.append(item)

        t = threading.Thread(target=consumer)
        t.start()
        time.sleep(0.05)
        e.close()
        t.join(timeout=1.0)

        assert not t.is_alive(), 'consumer thread is still blocked after close()'
        assert result == [None]

    def test_full_pipeline_two_threads(self):
        """Producer writes N items then closes; consumer drains all."""
        N = 20
        e = Edge()
        received = []

        def producer():
            for i in range(N):
                e.put((i,))
            e.close()

        def consumer():
            while True:
                item = e.get()
                if item is None:
                    break
                received.append(item)

        pt = threading.Thread(target=producer)
        ct = threading.Thread(target=consumer)
        ct.start()
        pt.start()
        pt.join()
        ct.join(timeout=2.0)

        assert not ct.is_alive()
        assert received == [(i,) for i in range(N)]
