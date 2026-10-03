"""A unittest base class with the temporary directory, patching, and output capture these
tests use, plus approximate comparison and exception checks."""

from __future__ import annotations

from collections import namedtuple
from contextlib import contextmanager
import io
import math
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any, Iterator
import unittest

_MISSING = object()
CapturedOutput = namedtuple("CapturedOutput", "out err")


class MonkeyPatch:
    """Set attributes and environment variables, undone when the test ends."""

    def __init__(self, case: unittest.TestCase) -> None:
        self._case = case

    def setattr(self, target: Any, name: str, value: Any) -> None:
        original = getattr(target, name, _MISSING)
        setattr(target, name, value)
        self._case.addCleanup(self._restore, target, name, original)

    @staticmethod
    def _restore(target: Any, name: str, original: Any) -> None:
        if original is _MISSING:
            delattr(target, name)
        else:
            setattr(target, name, original)

    def setenv(self, name: str, value: str) -> None:
        self._case.addCleanup(self._restore_env, name, os.environ.get(name))
        os.environ[name] = value

    def delenv(self, name: str, raising: bool = True) -> None:
        if name not in os.environ:
            if raising:
                raise KeyError(name)
            return
        self._case.addCleanup(self._restore_env, name, os.environ[name])
        del os.environ[name]

    @staticmethod
    def _restore_env(name: str, value: str | None) -> None:
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


class CaptureOutput:
    """Capture what the test prints to stdout and stderr."""

    def __init__(self, case: unittest.TestCase) -> None:
        self._out, self._err = io.StringIO(), io.StringIO()
        saved = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = self._out, self._err
        case.addCleanup(self._restore, saved)

    @staticmethod
    def _restore(saved: tuple) -> None:
        sys.stdout, sys.stderr = saved

    def readouterr(self) -> CapturedOutput:
        captured = CapturedOutput(self._out.getvalue(), self._err.getvalue())
        for stream in (self._out, self._err):
            stream.seek(0)
            stream.truncate()
        return captured


class Case(unittest.TestCase):
    """A test case that creates its temporary directory, patcher, and capture on first use."""

    @property
    def tmp_path(self) -> Path:
        if not hasattr(self, "_tmp_path"):
            directory = tempfile.TemporaryDirectory()
            self.addCleanup(directory.cleanup)
            self._tmp_path = Path(directory.name)
        return self._tmp_path

    @property
    def monkeypatch(self) -> MonkeyPatch:
        if not hasattr(self, "_monkeypatch"):
            self._monkeypatch = MonkeyPatch(self)
        return self._monkeypatch

    @property
    def capsys(self) -> CaptureOutput:
        if not hasattr(self, "_capsys"):
            self._capsys = CaptureOutput(self)
        return self._capsys


class approx:
    """Compares equal to numbers within a relative tolerance, also inside sequences."""

    def __init__(self, expected: Any, rel: float = 1e-6, abs: float = 1e-12) -> None:
        self.expected, self.rel, self.abs = expected, rel, abs

    def _close(self, expected: Any, actual: Any) -> bool:
        if isinstance(expected, (list, tuple)):
            return (isinstance(actual, (list, tuple)) and len(actual) == len(expected)
                    and all(self._close(e, a) for e, a in zip(expected, actual)))
        if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
            return math.isclose(actual, expected, rel_tol=self.rel, abs_tol=self.abs)
        return expected == actual

    def __eq__(self, actual: Any) -> bool:
        return self._close(self.expected, actual)

    def __repr__(self) -> str:
        return f"approx({self.expected!r})"


class _RaisedInfo:
    value: BaseException


@contextmanager
def raises(expected: type[BaseException], match: str | None = None) -> Iterator[_RaisedInfo]:
    """Require the block to raise `expected`, with a message matching `match` if given."""
    info = _RaisedInfo()
    try:
        yield info
    except expected as error:
        if match is not None and not re.search(match, str(error)):
            raise AssertionError(f"{error!r} does not match {match!r}") from error
        info.value = error
    else:
        raise AssertionError(f"{expected.__name__} was not raised")
