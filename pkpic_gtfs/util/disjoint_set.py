# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
from collections.abc import Hashable, Iterable, Iterator, Set
from typing import TypeVar

T_Hashable = TypeVar("T_Hashable", bound=Hashable)


class DisjointSet(Set[T_Hashable]):
    def __init__(self, elements: Iterable[T_Hashable] = []) -> None:
        self._parents = {i: i for i in elements}

    def __contains__(self, item: object) -> bool:
        return item in self._parents

    def __iter__(self) -> Iterator[T_Hashable]:
        return iter(self._parents)

    def __len__(self) -> int:
        return len(self._parents)

    def find_root(self, item: T_Hashable) -> T_Hashable:
        root = item
        while self._parents[root] != root:
            root = self._parents[root]

        while self._parents[item] != root:
            parent = self._parents[item]
            self._parents[item] = root
            item = parent

        return root

    def merge(self, x: T_Hashable, y: T_Hashable) -> None:
        x = self.find_root(x)
        y = self.find_root(y)
        self._parents[y] = x

    def get_groups(self) -> defaultdict[T_Hashable, list[T_Hashable]]:
        by_root = defaultdict[T_Hashable, list[T_Hashable]](list)
        for elem in self._parents:
            by_root[self.find_root(elem)].append(elem)
        return by_root
