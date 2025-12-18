# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
from collections.abc import Iterable, MutableMapping
from typing import cast

from impuls import DBConnection, Task, TaskRuntime


class AssignBlockIds(Task):
    def __init__(self) -> None:
        super().__init__()
        self.block_id_counter = 0

    def clear(self) -> None:
        self.block_id_counter = 0

    def execute(self, r: TaskRuntime) -> None:
        self.clear()

        trips = self.get_linked_trips(r.db)
        assigned_blocks = self.assign_block_ids(trips)
        self.save_block_ids(r.db, assigned_blocks)

    def get_next_block_id(self) -> str:
        id = str(self.block_id_counter)
        self.block_id_counter += 1
        return id

    def get_linked_trips(self, db: DBConnection) -> defaultdict[str, list[str]]:
        linked_trips = defaultdict[str, list[str]](list)
        query = cast(
            Iterable[tuple[str, str]],
            db.raw_execute(
                "SELECT from_trip_id, to_trip_id FROM transfers WHERE transfer_type = 4",
            ),
        )

        for trip_a, trip_b in query:
            linked_trips[trip_a].append(trip_b)
            linked_trips[trip_b].append(trip_a)

        return linked_trips

    def assign_block_ids(self, trips: MutableMapping[str, list[str]]) -> list[tuple[str, str]]:
        assigned_blocks = list[tuple[str, str]]()  # block_id, trip_id

        while trips:
            block = self.pick_next_block(trips)
            block_id = self.get_next_block_id()
            for trip in block:
                assigned_blocks.append((block_id, trip))

        return assigned_blocks

    def pick_next_block(self, linked_trips: MutableMapping[str, list[str]]) -> set[str]:
        trip, to_expand = linked_trips.popitem()

        queue = to_expand.copy()
        block = {trip}

        while queue:
            trip = queue.pop()
            block.add(trip)
            for linked in linked_trips.pop(trip):
                if linked not in block:
                    queue.append(linked)

        return block

    def save_block_ids(self, db: DBConnection, assigned_blocks: Iterable[tuple[str, str]]) -> None:
        with db.transaction():
            db.raw_execute_many(
                "UPDATE trips SET block_id = ? WHERE trip_id = ?",
                assigned_blocks,
            )
