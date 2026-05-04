import pytest

from yieldgraph.job import Job, _as_generator, _wrap


# ---------------------------------------------------------------------------
# _as_generator
# ---------------------------------------------------------------------------

class TestAsGenerator:
    def test_plain_function_wraps_return_value(self):
        gen_fn = _as_generator(lambda x: x * 2)
        assert list(gen_fn(3)) == [6]

    def test_plain_function_none_return(self):
        gen_fn = _as_generator(lambda: None)
        assert list(gen_fn()) == [None]

    def test_generator_function_returned_unchanged(self):
        def gen(n):
            yield from range(n)

        assert _as_generator(gen) is gen

    def test_generator_function_produces_correct_values(self):
        def gen():
            yield 1
            yield 2

        assert list(_as_generator(gen)()) == [1, 2]

    def test_plain_function_with_multiple_args(self):
        gen_fn = _as_generator(lambda a, b: a + b)
        assert list(gen_fn(3, 4)) == [7]


# ---------------------------------------------------------------------------
# _wrap
# ---------------------------------------------------------------------------

class TestWrap:
    def _make_job(self, fn):
        """Build a bare Job shell so _wrap has a real Job to reference."""
        return Job(fn)

    def test_yields_all_values_when_not_cancelled(self):
        def gen():
            yield from range(5)

        job = self._make_job(gen)
        assert list(job()) == [0, 1, 2, 3, 4]

    def test_running_is_false_before_call(self):
        job = self._make_job(lambda: 1)
        assert not job.running

    def test_running_is_false_after_exhaustion(self):
        job = self._make_job(lambda: 1)
        list(job())
        assert not job.running

    def test_cancellation_stops_iteration(self):
        def gen():
            for i in range(100):
                yield i

        job = self._make_job(gen)
        collected = []
        for v in job():
            collected.append(v)
            if v == 2:
                job.cancelled = True

        assert collected == [0, 1, 2]
        assert not job.running

    def test_running_is_false_after_cancellation(self):
        def gen():
            yield from range(10)

        job = self._make_job(gen)
        gen_obj = job()
        next(gen_obj)
        job.cancelled = True
        list(gen_obj)   # exhaust
        assert not job.running


# ---------------------------------------------------------------------------
# Job construction & attributes
# ---------------------------------------------------------------------------

class TestJobConstruction:
    def test_name_from_function(self):
        def my_function():
            return 1

        assert Job(my_function).name == 'my_function'

    def test_name_from_lambda_is_lambda(self):
        assert Job(lambda: None).name == '<lambda>'

    def test_cancelled_starts_false(self):
        assert not Job(lambda: None).cancelled

    def test_running_starts_false(self):
        assert not Job(lambda: None).running

    def test_label_explicit(self):
        job = Job(lambda: None, label='My Label')
        assert job.label == 'My Label'

    def test_label_derived_from_name(self):
        def load_raw_data():
            return None

        assert Job(load_raw_data).label == 'LOAD RAW DATA'

    def test_label_single_word(self):
        def extract():
            return None

        assert Job(extract).label == 'EXTRACT'

    def test_label_lambda_derived(self):
        job = Job(lambda: None)
        assert job.label == '<LAMBDA>'


# ---------------------------------------------------------------------------
# Job.__call__
# ---------------------------------------------------------------------------

class TestJobCall:
    def test_plain_function_yields_one_result(self):
        job = Job(lambda x: x ** 2)
        assert list(job(4)) == [16]

    def test_generator_function_yields_all(self):
        def letters():
            yield from 'abc'

        assert list(Job(letters)()) == ['a', 'b', 'c']

    def test_call_resets_cancelled_flag(self):
        job = Job(lambda: 1)
        job.cancelled = True
        list(job())                  # new call must reset the flag
        assert not job.cancelled

    def test_call_resets_cancelled_allowing_reuse(self):
        def gen():
            yield from range(3)

        job = Job(gen)
        # first run — cancel early
        gen1 = job()
        next(gen1)
        job.cancelled = True
        list(gen1)
        # second run — should yield fully because cancelled is reset
        assert list(job()) == [0, 1, 2]

    def test_return_value_is_generator(self):
        import inspect
        job = Job(lambda: 1)
        assert inspect.isgenerator(job())

    def test_args_forwarded_correctly(self):
        def add(a, b, c):
            yield a + b + c

        assert list(Job(add)(1, 2, 3)) == [6]

    def test_multiple_calls_independent(self):
        def gen():
            yield from range(3)

        job = Job(gen)
        assert list(job()) == [0, 1, 2]
        assert list(job()) == [0, 1, 2]


# ---------------------------------------------------------------------------
# Job.__repr__
# ---------------------------------------------------------------------------

class TestJobRepr:
    def test_repr_contains_name_and_label(self):
        def do_work():
            return 1

        job = Job(do_work, label='My Work')
        r = repr(job)
        assert 'do_work' in r
        assert 'My Work' in r

    def test_repr_with_derived_label(self):
        def load_data():
            return None

        r = repr(Job(load_data))
        assert 'load_data' in r
        assert 'LOAD DATA' in r
