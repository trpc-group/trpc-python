# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Generic registry-based factory utilities."""

from typing import Dict
from typing import Generic
from typing import Tuple
from typing import Type
from typing import TypeVar

T = TypeVar("T")


class Factory(Generic[T]):
    """Create registered implementations of a common template type.

    Args:
        template: Base class that every registered implementation must extend.

    Example:
        class Transport:
            pass

        class HttpTransport(Transport):
            def __init__(self, endpoint: str):
                self.endpoint = endpoint

        factory = Factory[Transport](Transport)
        factory.register("http", HttpTransport)
        transport = factory.create("http", "https://example.com")
    """

    def __init__(self, template: Type[T]) -> None:
        """Initialize the factory with its required template type.

        Args:
            template: Base class for registered implementations.

        Raises:
            TypeError: If ``template`` is not a class.
        """
        if not isinstance(template, type):
            raise TypeError("template must be a class")

        self._template = template
        self._implementations: Dict[str, Type[T]] = {}

    @property
    def template(self) -> Type[T]:
        """Return the base template accepted by this factory."""
        return self._template

    @property
    def registered_names(self) -> Tuple[str, ...]:
        """Return registered implementation names in registration order."""
        return tuple(self._implementations)

    def register(
        self,
        name: str,
        implementation: Type[T],
        *,
        replace: bool = False,
    ) -> None:
        """Register an implementation class.

        Args:
            name: Name used to create the implementation.
            implementation: Concrete subclass of the factory template.
            replace: Whether an existing registration may be replaced.

        Raises:
            TypeError: If the implementation is not a subclass of the template.
            ValueError: If the name is empty or already registered.
        """
        if not name:
            raise ValueError("factory registration name must not be empty")
        if not isinstance(implementation, type) or not issubclass(implementation, self._template):
            raise TypeError(f"implementation must be a subclass of {self._template.__name__}")

        if name in self._implementations and not replace:
            raise ValueError(f"implementation {name!r} is already registered")
        self._implementations[name] = implementation

    def get(self, name: str) -> Type[T]:
        """Return a registered implementation class.

        Args:
            name: Registered implementation name.

        Returns:
            The registered implementation class.

        Raises:
            KeyError: If the name is not registered.
        """
        try:
            return self._implementations[name]
        except KeyError:
            raise KeyError(f"implementation {name!r} is not registered") from None

    def create(self, name: str, *args, **kwargs) -> T:
        """Instantiate a registered implementation.

        Args:
            name: Registered implementation name.
            *args: Positional constructor arguments.
            **kwargs: Keyword constructor arguments.

        Returns:
            A newly created implementation instance.

        Raises:
            KeyError: If the name is not registered.
        """
        implementation = self.get(name)
        return implementation(*args, **kwargs)
