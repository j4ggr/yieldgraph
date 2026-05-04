"""Directed queue connecting two nodes in an ETL pipeline.

An :class:`Edge` is a :class:`~collections.deque` that carries data 
items from a producer node to a consumer node.  In sequential mode the 
plain :meth:`~collections.deque.append` / 
:meth:`~collections.deque.popleft` interface works unchanged. In 
threaded mode producers call :meth:`put` and consumers call 
:meth:`get`; when a producer is done writing it calls :meth:`close` so 
consumers can drain the remaining items and stop waiting.
"""

import threading
import time

from collections import deque
from typing import Any
from typing import Optional


__all__ = ['Edge']


class Edge(deque):
    """A directed queue connecting two :class:`~yieldgraph.node.Node` 
    objects.

    Subclasses :class:`~collections.deque` so it can be used as a plain
    list-like queue in sequential mode.  Thread-safe :meth:`put`, 
    :meth:`get`, and :meth:`close` are provided for pipelined (threaded) 
    execution, where producer and consumer nodes run concurrently.

    Attributes
    ----------
    closed : bool
        ``True`` once the producer has called :meth:`close`.  No further
        items will be added.  A consumer blocked in :meth:`get` will be
        woken up and will drain any remaining items before returning 
        ``None``.

    Examples
    --------
    Sequential mode (existing interface unchanged):

    ```python
    e = Edge()
    e.append((1, 2))
    item = e.popleft()   # → (1, 2)
    ```

    Threaded mode:

    ```python
    import threading
    e = Edge()

    def producer():
        for i in range(3):
            e.put((i,))
        e.close()

    def consumer():
        while True:
            item = e.get()
            if item is None:   # closed + empty
                break
            print(item)

    t1 = threading.Thread(target=producer)
    t2 = threading.Thread(target=consumer)
    t1.start(); t2.start()
    t1.join();  t2.join()
    ```
    """

    def __init__(self, iterable=()) -> None:
        super().__init__(iterable)
        self._cond: threading.Condition = threading.Condition(threading.Lock())
        self._closed: bool = False

    # ------------------------------------------------------------------
    # Thread-safe API
    # ------------------------------------------------------------------

    def put(self, item: Any) -> None:
        """Append *item* to the right end of the queue and notify 
        waiters.

        Thread-safe alternative to :meth:`~collections.deque.append`.
        Any consumer blocked in :meth:`get` will be woken up 
        immediately.

        Parameters
        ----------
        item : Any
            The data tuple to enqueue.
        """
        with self._cond:
            self.append(item)
            self._cond.notify()

    def get(self, timeout: Optional[float] = None) -> Any:
        """Remove and return the leftmost item, blocking until one is 
        available.

        Blocks when the queue is empty and the edge is not yet 
        :attr:`closed`. Returns ``None`` when the edge is closed *and* 
        empty (producer done, all items consumed).

        Parameters
        ----------
        timeout : float, optional
            Maximum seconds to wait.  When the timeout expires without 
            an item, returns ``None`` regardless of :attr:`closed`. 
            Omit to wait indefinitely.

        Returns
        -------
        Any
            The next item from the left end, or ``None`` when the edge 
            is closed and empty (or the timeout elapsed).

        Examples
        --------
        ```python
        edge = Edge()
        edge.put((42,))
        assert edge.get() == (42,)
        edge.close()
        assert edge.get() is None   # closed + empty → None
        ```
        """
        with self._cond:
            deadline = (time.monotonic() + timeout) if timeout is not None else None
            while not self and not self._closed:
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                    self._cond.wait(remaining)
                else:
                    self._cond.wait()
            if self:
                return self.popleft()
            return None  # closed + empty

    def close(self) -> None:
        """Signal that no more items will be added to this edge.

        Sets :attr:`closed` to ``True`` and wakes all threads blocked in
        :meth:`get`.  They will drain any remaining items and then 
        receive ``None``.  Calling :meth:`close` more than once is 
        harmless.
        """
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    @property
    def closed(self) -> bool:
        """``True`` once the producer has signalled end-of-stream via 
        :meth:`close`."""
        return self._closed
