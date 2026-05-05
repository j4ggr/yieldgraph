import logging

import pytest

from yieldgraph.config import LOG, ENV, LoggingBehavior, START_NODE_NAME


# ---------------------------------------------------------------------------
# LOG constants
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
        assert LOG.TRACE_LEVEL_NUM == 5

    def test_log_is_frozen(self):
        with pytest.raises(Exception):
            LOG.TRACE = 'something'  # type: ignore


# ---------------------------------------------------------------------------
# START_NODE_NAME
# ---------------------------------------------------------------------------

class TestStartNodeName:
    def test_is_string(self):
        assert isinstance(START_NODE_NAME, str)

    def test_not_empty(self):
        assert START_NODE_NAME


# ---------------------------------------------------------------------------
# LoggingBehavior.name_to_level
# ---------------------------------------------------------------------------

class TestNameToLevel:
    def test_trace(self):
        assert LoggingBehavior.name_to_level('TRACE') == LOG.TRACE_LEVEL_NUM

    def test_standard_levels_by_name(self):
        assert LoggingBehavior.name_to_level('DEBUG') == logging.DEBUG
        assert LoggingBehavior.name_to_level('INFO') == logging.INFO
        assert LoggingBehavior.name_to_level('WARNING') == logging.WARNING
        assert LoggingBehavior.name_to_level('ERROR') == logging.ERROR
        assert LoggingBehavior.name_to_level('CRITICAL') == logging.CRITICAL

    def test_standard_levels_by_int(self):
        assert LoggingBehavior.name_to_level(logging.DEBUG) == logging.DEBUG
        assert LoggingBehavior.name_to_level(logging.INFO) == logging.INFO

    def test_unknown_name_defaults_to_info(self):
        assert LoggingBehavior.name_to_level('NONSENSE') == logging.INFO

    def test_unknown_int_defaults_to_info(self):
        assert LoggingBehavior.name_to_level(999) == logging.INFO


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

    def test_default_log_title_is_class_name(self):
        obj = self._make()
        assert 'Obj' in obj.log_title

    def test_custom_log_title(self):
        obj = self._make(title='MyTitle')
        assert obj.log_title == 'MyTitle'

    def test_has_logger(self):
        obj = self._make()
        assert obj.logger is not None

    @pytest.mark.parametrize('method,level', [
        ('log_trace',    'TRACE'),
        ('log_debug',    'DEBUG'),
        ('log_info',     'INFO'),
        ('log_warning',  'WARNING'),
        ('log_error',    'ERROR'),
        ('log_critical', 'CRITICAL'),
    ])
    def test_convenience_methods_do_not_raise(self, method, level):
        obj = self._make()
        getattr(obj, method)('test message')

    def test_log_exception_does_not_raise(self):
        obj = self._make()
        obj.log_exception('something failed', ValueError('boom'))
