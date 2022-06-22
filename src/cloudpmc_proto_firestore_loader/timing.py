import inspect
import timeit
from contextlib import ContextDecorator
from enum import Enum
from functools import wraps

from .helpers import pp
from .logger import logger


class Context(Enum):
    DECORATOR = 1
    MANAGER = 2


class Timer(ContextDecorator):
    def __init__(self, name=None):
        self._name = name or ""
        self._args = []
        self._kwargs = {}
        self._starts = None
        self._ends = None
        self._context = Context.MANAGER

    def __enter__(self):
        self._starts = timeit.default_timer()
        return self

    def __exit__(self, *args):
        self._ends = timeit.default_timer()
        elapsed = self._ends - self._starts
        _signature = self._signature()
        logger.debug(f"{_signature} " f"completes in {elapsed:.6f} sec")
        return False

    def __call__(self, func):
        if self._name in ("", None):
            self._name = func.__name__ or ""
        self._args = []
        self._kwargs = {}

        @wraps(func)
        def _decorated(*args, **kwds):
            kwargs = self._get_default_args(func)
            kwargs.update(kwds)
            self._args = args
            self._kwargs = kwargs
            self._context = Context.DECORATOR

            with self:
                return func(*args, **kwds)

        return _decorated

    def _signature(self):
        context_name = self._name
        arguments = ""

        for _ in self._args:
            arguments += (", " if arguments != "" else "") + repr(_)
        for k, v in self._kwargs.items():
            arguments += (", " if arguments != "" else "") + f"k={repr(v)}"

        if self._context == Context.MANAGER:
            return context_name
        elif self._context == Context.DECORATOR:
            return f"{context_name}({arguments})"
        else:
            error = f"Unknown context, the following are recognized {list(Context)}"
            raise ValueError(error)

    def _get_default_args(self, func):
        signature = inspect.signature(func)
        return {
            k: v.default
            for k, v in signature.parameters.items()
            if v.default is not inspect.Parameter.empty
        }
