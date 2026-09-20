# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Process-local singleton decorator.

:func:`singleton` stores the first constructed instance in a closure
dict and returns that object on later calls. It is not thread-safe:
two concurrent first calls can both miss the dict and create two
instances. Arguments on the second and later calls are ignored, so
this is not suitable for classes that must vary by constructor args.
"""

from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Type
from typing import TypeVar

_T = TypeVar('_T')


def singleton(cls: Type[_T]) -> Callable[..., _T]:
    """Return a factory that constructs ``cls`` at most once.

    Args:
        cls: Class to wrap. The first call to the factory runs
            ``cls(*args, **kw)``; later calls return that instance
            and ignore new arguments.

    Returns:
        Callable with the same positional / keyword signature as
        ``cls.__init__`` (minus ``self``).

    Example:
        @singleton
        class DatabaseConnection:
            def __init__(self, connection_string):
                self.connection_string = connection_string

        db1 = DatabaseConnection("mysql://localhost")
        db2 = DatabaseConnection("postgres://localhost")
        assert db1 is db2
        assert db1.connection_string == "mysql://localhost"
    """
    instances = {}

    def get_instance(*args: Any, **kw: Any) -> _T:
        """
        Creates or returns the singleton instance of the decorated class.
        
        The first call creates the instance with provided arguments. Subsequent
        calls ignore the arguments and return the existing instance.
        
        Args:
            *args: Positional arguments for the class constructor (only used on first call)
            **kw: Keyword arguments for the class constructor (only used on first call)
            
        Returns:
            The singleton instance of the decorated class
            
        Note:
            If called with different arguments after first instantiation,
            the original instance will still be returned and new arguments
            will be ignored.
        """
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return get_instance
