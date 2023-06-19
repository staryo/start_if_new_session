from datetime import datetime
from inspect import getmodule
from logging import getLogger

__all__ = [
    'Base',
]


class Base(object):

    def __init__(self, *args, logger=None, **kwargs):
        self._logger = logger or getLogger(self._dotted_name)

    @property
    def _dotted_name(self):
        return '{}.{}'.format(
            getmodule(self).__name__,
            self.__class__.__name__
        )
