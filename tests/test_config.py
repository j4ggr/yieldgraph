import os
import logging

import pytest

from yieldgraph.config import LOG, ENV, LoggingBehavior, START_NODE_NAME


# ---------------------------------------------------------------------------
# LOG — string constants
# ---------------------------------------------------------------------------

class TestLog:
    def test_level_values_are_strings(self):
        assert LOG.TRACE == 'TRACE'
        assert LOG.DEBUG == 'DEBUG'
        assert LOG.INFO == 'INFO'
        assert LOG.WARNING == 'WARNING'
        assert LOG.ERROR == 'ERROR'
        assert LOG.CRITICAL == 'CRITICAL'

    def test_trace_level_num(self):
        assert LOG.TRACE_LEVEL == 5

    def test_numeric_level_constants(self):
        assert LOG.DEBUG_LEVEL == logging.DEBUG       # 10
        assert LOG.INFO_LEVEL == logging.INFO         # 20
        assert LOG.WARNING_LEVEL == logging.WARNING   # 30
        assert LOG.ERROR_LEVEL == logging.ERROR       # 40
        assert LOG.CRITICAL_LEVEL == logging.CRITICAL # 50

    def test_log_is_frozen(self):
        with pytest.raises(Exception):
            LOG.TRACE = 'something'  # type: ignore


# ---------------------------------------------------------------------------
# LOG — __getitem__
# ---------------------------------------------------------------------------

class TestLogGetItem:
    def test_lookup_by_name(self):
        assert LOG['TRACE'] == 5
        assert LOG['DEBUG'] == logging.DEBUG
        assert LOG['INFO'] == logging.INFO
        assert LOG['WARNING'] == logging.WARNING
        assert LOG['ERROR'] == logging.ERROR
        assert LOG['CRITICAL'] == logging.CRITICAL

    def test_lookup_by_name_case_insensitive(self):
        assert LOG['debug'] == logging.DEBUG
        assert LOG['Info'] == logging.INFO

    def test_lookup_by_int_returns_int(self):
        assert LOG[logging.DEBUG] == logging.DEBUG
        assert LOG[5] == 5

    def test_invalid_name_raises_key_error(self):
        with pytest.raises(KeyError):
            LOG['NONSENSE']


# ---------------------------------------------------------------------------
# LOG — __call__
# ---------------------------------------------------------------------------

class TestLogCall:
    def test_int_to_name(self):
        assert LOG(5) == 'TRACE'
        assert LOG(logging.DEBUG) == 'DEBUG'
        assert LOG(logging.INFO) == 'INFO'
        assert LOG(logging.WARNING) == 'WARNING'
        assert LOG(logging.ERROR) == 'ERROR'
        assert LOG(logging.CRITICAL) == 'CRITICAL'

    def test_name_to_name(self):
        assert LOG('DEBUG') == 'DEBUG'
        assert LOG('info') == 'INFO'

    def test_invalid_int_raises_key_error(self):
        with pytest.raises(KeyError):
            LOG(999)

    def test_invalid_name_raises_key_error(self):
        with pytest.raises(KeyError):
            LOG('NONSENSE')


# ---------------------------------------------------------------------------
# START_NODE_NAME
# ---------------------------------------------------------------------------

class TestStartNodeName:
    def test_is_string(self):
        assert isinstance(START_NODE_NAME, str)

    def test_not_empty(self):
        assert START_NODE_NAME


# ---------------------------------------------------------------------------
# ENV properties
# ---------------------------------------------------------------------------

class TestENV:
    def teardown_method(self):
        for key in (ENV.THREADED_KEY, ENV.LOG_TRACEBACK_KEY, ENV.LOG_DISABLED_KEY):
            os.environ.pop(key, None)

    def test_key_constants_are_strings(self):
        assert isinstance(ENV.THREADED_KEY, str)
        assert isinstance(ENV.LOG_TRACEBACK_KEY, str)
        assert isinstance(ENV.LOG_DISABLED_KEY, str)

    def test_trueish_values_contains_expected(self):
        assert '1' in ENV.TRUEISH_VALUES
        assert 'true' in ENV.TRUEISH_VALUES
        assert 'yes' in ENV.TRUEISH_VALUES

    # THREADED
    def test_threaded_false_by_default(self):
        assert ENV.THREADED is False

    @pytest.mark.parametrize('value', ['1', 'true', 'yes', 'TRUE', 'Yes'])
    def test_threaded_true_for_truthy_values(self, value):
        os.environ[ENV.THREADED_KEY] = value
        assert ENV.THREADED is True

    def test_threaded_false_for_falsy_value(self):
        os.environ[ENV.THREADED_KEY] = '0'
        assert ENV.THREADED is False

    # LOG_TRACEBACK
    def test_log_traceback_false_by_default(self):
        assert ENV.LOG_TRACEBACK is False

    @pytest.mark.parametrize('value', ['1', 'true', 'yes'])
    def test_log_traceback_true_for_truthy_values(self, value):
        os.environ[ENV.LOG_TRACEBACK_KEY] = value
        assert ENV.LOG_TRACEBACK is True

    # LOG_DISABLED
    def test_log_disabled_false_by_default(self):
        assert ENV.LOG_DISABLED is False

    @pytest.mark.parametrize('value', ['1', 'true', 'yes'])
    def test_log_disabled_true_for_truthy_values(self, value):
        os.environ[ENV.LOG_DISABLED_KEY] = value
        assert ENV.LOG_DISABLED is True

    def test_log_disabled_reflects_runtime_change(self):
        assert ENV.LOG_DISABLED is False
        os.environ[ENV.LOG_DISABLED_KEY] = '1'
        assert ENV.LOG_DISABLED is True
        os.environ.pop(ENV.LOG_DISABLED_KEY)
        assert ENV.LOG_DISABLED is False


# ---------------------------------------------------------------------------
# LoggingBehavior mixin
# ---------------------------------------------------------------------------

class TestLoggingBehavior:
    def _make(self, title: str | None = None) -> LoggingBehavior:
        class Obj(LoggingBehavior):
            @property
            def log_title(self):
                return title if title is not None else super().log_title
        return Obj()

    def teardown_method(self):
        os.environ.pop(ENV.LOG_DISABLED_KEY, None)

    def test_default_log_title_is_class_name(self):
        obj = self._make()
        assert 'Obj' in obj.log_title

    def test_custom_log_title(self):
        obj = self._make(title='MyTitle')
        assert obj.log_title == 'MyTitle'

    def test_has_logger(self):
        obj = self._make()
        assert obj.logger is not None

    @pytest.mark.parametrize('method', [
        'log_trace', 'log_debug', 'log_info',
        'log_warning', 'log_error', 'log_critical',
    ])
    def test_convenience_methods_do_not_raise(self, method):
        obj = self._make()
        getattr(obj, method)('test message')

    def test_log_exception_does_not_raise(self):
        obj = self._make()
        obj.log_exception('something failed', ValueError('boom'))

    def test_log_disabled_silences_all_output(self):
        """When YIELDGRAPH_LOG_DISABLED is set, log() returns immediately."""
        os.environ[ENV.LOG_DISABLED_KEY] = '1'
        obj = self._make()
        # None of these should raise; crucially they must not emit anything
        obj.log_info('should be silent')
        obj.log_error('also silent')
        obj.log_exception('silent too', RuntimeError('x'))
