# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from io import TextIOWrapper
from typing import LiteralString, Self, cast
from zipfile import ZipFile

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Date, StopTime, Transfer, Trip
from impuls.tools.temporal import DateRange, InfiniteDateRange, date_range
from impuls.tools.types import SQLNativeType, StrPath


@dataclass
class TripSlice:
    """TripSlice represents a trip with a subset of its stop-times,
    from `from_stop_sequence` to `to_stop_sequence` inclusive.
    """

    trip_id: str
    from_stop_sequence: int = 0
    to_stop_sequence: int | None = None

    def as_slice(self) -> slice:
        return slice(
            self.from_stop_sequence,
            self.to_stop_sequence + 1 if self.to_stop_sequence is not None else None,
        )

    def select_stop_times(self, db: DBConnection) -> list[StopTime]:
        query: list[LiteralString] = ["SELECT * FROM stop_times WHERE trip_id = ? "]
        args: list[SQLNativeType] = [self.trip_id]

        if self.from_stop_sequence > 0:
            query.append("AND stop_sequence >= ? ")
            args.append(self.from_stop_sequence)

        if self.to_stop_sequence is not None:
            query.append("AND stop_sequence <= ? ")
            args.append(self.to_stop_sequence)

        query.append("ORDER BY stop_sequence ASC")

        return db.typed_out_execute("".join(query), StopTime, args).all()


@dataclass
class TripStops:
    """TripStops stores a lookup table from stop_id to (smallest) stop_sequence
    for a particular trip.
    """

    trip_id: str
    stop_sequence_by_id: dict[str, int]

    def resolve_stop(self, stop: str | int) -> int:
        """Resolves a stop_id (str) into a stop_sequence (int),
        or simply returns the provided argument if it's already a stop_sequence (int).
        """
        return stop if isinstance(stop, int) else self.stop_sequence_by_id[stop]

    def stops_later(self, stop_a: str | int, stop_b: str | int) -> bool:
        """Returns true if this Trip stops at `stop_b` after stopping at `stop_a`."""
        return self.resolve_stop(stop_a) < self.resolve_stop(stop_b)

    def up_to(self, stop: str | int) -> TripSlice:
        """Returns a TripSlice of this trip from the first stop-time up to the given stop."""
        return TripSlice(self.trip_id, to_stop_sequence=self.resolve_stop(stop))

    def starting_at(self, stop: str | int) -> TripSlice:
        """Returns a TripSlice of this trip from the given stop up to the last stop-time."""
        return TripSlice(self.trip_id, from_stop_sequence=self.resolve_stop(stop))


@dataclass
class Connection:
    """Connection represents a single in-seat transfer between two trips at a specific station."""

    from_trip_id: str
    to_trip_id: str
    at_stop_id: str
    carriages: frozenset[str]

    def with_trip_id_prefix(self, prefix: str) -> Self:
        """Returns a copy of this Connection, but with prefix prepended to both trip_ids."""
        return replace(
            self,
            from_trip_id=f"{prefix}{self.from_trip_id}",
            to_trip_id=f"{prefix}{self.to_trip_id}",
        )

    def is_valid(self, trips: Mapping[str, TripStops]) -> bool:
        """Checks if this Connection is valid for a given set of trips -
        that is both from- and to- trips exists and call at at_stop_id.
        """
        return self._is_from_valid(trips) and self._is_to_valid(trips)

    def _is_from_valid(self, trips: Mapping[str, TripStops]) -> bool:
        return (
            self.from_trip_id in trips
            and self.at_stop_id in trips[self.from_trip_id].stop_sequence_by_id
        )

    def _is_to_valid(self, trips: Mapping[str, TripStops]) -> bool:
        return (
            self.to_trip_id in trips
            and self.at_stop_id in trips[self.to_trip_id].stop_sequence_by_id
        )

    @classmethod
    def read_all(
        cls,
        f: Iterable[str],
        days: DateRange = InfiniteDateRange(),
    ) -> Iterable[tuple[Date, Self]]:
        """Generates all Connections from a given PKP IC's KPD_Rozklad_Przelaczenia.csv file."""
        for row in csv.DictReader(f, delimiter=";"):
            connection = cls(
                from_trip_id=row["NrPoc1"].replace("/", "-"),
                to_trip_id=row["NrPoc2"].replace("/", "-"),
                at_stop_id=row["objectID"],
                carriages=frozenset(row["carriageNo"].split(",")),
            )

            yield from map(
                lambda day: (day, connection.with_trip_id_prefix(f"{day.isoformat()}_")),
                filter(days.__contains__, cls._parse_dates(row)),
            )

    @staticmethod
    def _parse_dates(row: Mapping[str, str]) -> Iterable[Date]:
        """Generates all dates selected by a row from PKP IC's KPD_Rozklad_Przelaczenia.csv
        by interpreting the "Timetable_no" and "mthNN" columns.
        """
        main_year = int(row["Timetable_no"].partition("/")[2])
        for month_key in range(13):
            if month_key == 0:
                year = main_year - 1
                month = 12
            else:
                year = main_year
                month = month_key
            days_str = row[f"mth{month_key:02}"]
            for day, letter in enumerate(days_str, start=1):
                if letter == "1":
                    yield Date(year, month, day)


@dataclass
class Block:
    """Block represents a continuous list of TripSlices operated by the same set of carriages;
    between which passengers can transfer by remaining seated.
    """

    legs: list[TripSlice]
    carriages: frozenset[str]

    @property
    def last_trip_id(self) -> str:
        return self.legs[-1].trip_id

    @property
    def last_from_stop_seq(self) -> int:
        return self.legs[-1].from_stop_sequence

    def copy(self) -> Self:
        """Returns a copy of this Block, including a shallow copy of the `legs` attribute."""
        return replace(self, legs=self.legs.copy())

    def issuperset(self, o: "Block") -> bool:
        """Returns True if this Block's legs contain the other Block's legs."""
        return o.issubset(self)

    def issubset(self, o: "Block") -> bool:
        """Return True if this Block's legs are contained in the other Block's legs."""
        if len(self.legs) > len(o.legs):
            return False
        return any(
            self.legs == o.legs[offset : offset + len(self.legs)]
            for offset in range(len(o.legs) - len(self.legs) + 1)
        )

    @classmethod
    def find_all_deduplicated(
        cls,
        connections: Iterable[Connection],
        trips: Mapping[str, TripStops],
    ) -> list["Block"]:
        """Finds all Blocks given a set of Connections and Trip data, then deduplicates them."""
        return cls.deduplicate(cls.find_all(connections, trips))

    @classmethod
    def find_all(
        cls,
        connections: Iterable[Connection],
        trips: Mapping[str, TripStops],
    ) -> Iterable[Self]:
        """Finds all Blocks given a set of Connections and Trip data."""
        connections_by_from_trip_id = defaultdict[str, list[Connection]](list)
        queue = list[Self]()

        # Initialize the search space
        for connection in connections:
            connections_by_from_trip_id[connection.from_trip_id].append(connection)
            block = cls(
                [
                    trips[connection.from_trip_id].up_to(connection.at_stop_id),
                    trips[connection.to_trip_id].starting_at(connection.at_stop_id),
                ],
                connection.carriages,
            )

            if block.last_trip_id not in connections_by_from_trip_id:
                # Fast path for connections without any possible continuations
                yield block
            else:
                queue.append(block)

        # Generate all blocks which can continue
        while queue:
            block = queue.pop()
            further_candidates = [
                c
                for c in connections_by_from_trip_id[block.last_trip_id]
                if c.carriages.issubset(block.carriages)
                and trips[c.from_trip_id].stops_later(block.last_from_stop_seq, c.at_stop_id)
            ]

            if further_candidates:
                # The block continues - add all continuations back to the queue
                for connection in further_candidates:
                    new = block.copy()

                    # Trim the last leg to connection.at_stop_id
                    to_idx = trips[block.last_trip_id].stop_sequence_by_id[connection.at_stop_id]
                    new.legs[-1] = replace(new.legs[-1], to_stop_sequence=to_idx)

                    # Add the leg implied by the connection
                    new.legs.append(trips[connection.to_trip_id].starting_at(connection.at_stop_id))

                    # Recalculate the carriages
                    new.carriages = new.carriages & connection.carriages

                    # Add back the block to the queue
                    queue.append(new)

            else:
                # This set of carriages isn't transferred again, yield it as a complete block
                yield block

    @staticmethod
    def deduplicate(blocks: Iterable["Block"]) -> list["Block"]:
        """Deduplicates a given set of blocks, so that no one Block is a subset of another,
        different Block.

        Note that this function has quadratic complexity and is not suitable for large inputs.
        """
        unique = list[Block]()

        for block in blocks:
            # Compare against every remembered block to see if the new block is unique
            for i, candidate in enumerate(unique):
                if block.issubset(candidate):
                    # The block is subset of candidate - no need to remember it
                    break
                elif block.issuperset(candidate):
                    # The block is superset of candidate - remember it instead of the candidate
                    unique[i] = block
                    break
            else:
                # The block was not a subset or superset of any other remembered block -
                # remember it
                unique.append(block)

        return unique


class GenerateInSeatTransfers(Task):
    """Adds in-seat transfers to the database by duplicating trips linked together
    by connections specified in KPD_Rozklad_Przelaczenia.csv file (stored in kpd_rozklad.zip)
    and joining them with both block_id and in-seat transfers.
    """

    def __init__(self) -> None:
        super().__init__()
        self.block_id_counters = defaultdict[str, int](lambda: 0)

    def clear(self) -> None:
        self.block_id_counters.clear()

    def _get_next_block_id(self, date_prefix: str = "") -> str:
        id = f"{date_prefix}_B{self.block_id_counters[date_prefix]}"
        self.block_id_counters[date_prefix] += 1
        return id

    def execute(self, r: TaskRuntime) -> None:
        pkg_path = r.resources["kpd_rozklad.zip"].stored_at
        active_days = self.get_active_date_range(r.db)
        connections_by_day = self.get_connections_by_day(pkg_path, active_days)

        blocks = [
            block
            for connections in connections_by_day.values()
            for block in self.get_blocks_for_day(r.db, connections)
        ]

        with r.db.transaction():
            for block in blocks:
                self.insert_block_trips(r.db, block)

    def get_active_date_range(self, db: DBConnection) -> DateRange:
        # fmt: off
        start_str, end_str = (
            db
            .raw_execute("SELECT min(start_date), max(end_date) FROM calendars")
            .one_must("empty calendar")
        )
        return date_range(
            Date.fromisoformat(cast(str, start_str)),
            Date.fromisoformat(cast(str, end_str)),
        )
        # fmt: on

    def get_connections_by_day(
        self,
        pkg_path: StrPath,
        days: DateRange = InfiniteDateRange(),
    ) -> defaultdict[Date, list["Connection"]]:
        by_day = defaultdict[Date, list[Connection]](list)
        with (
            ZipFile(pkg_path, "r") as pkg,
            pkg.open("KPD_Rozklad_Przelaczenia.csv") as csv_file_binary,
        ):
            csv_file = TextIOWrapper(csv_file_binary, encoding="windows-1250", newline="")
            for day, connection in Connection.read_all(csv_file, days):
                by_day[day].append(connection)
        return by_day

    def get_blocks_for_day(
        self,
        db: DBConnection,
        connections: Sequence[Connection],
    ) -> list[Block]:
        trips = self.get_trips_for_connections(db, connections)
        valid_connections = filter(lambda t: t.is_valid(trips), connections)
        return Block.find_all_deduplicated(valid_connections, trips)

    def get_trips_for_connections(
        self,
        db: DBConnection,
        connections: Iterable[Connection],
    ) -> dict[str, TripStops]:
        trips = self._get_empty_trips_for_connections(connections)
        for trip in trips.values():
            query = cast(
                Iterable[tuple[int, str]],
                db.raw_execute(
                    "SELECT stop_sequence, stop_id FROM stop_times WHERE trip_id = ?",
                    (trip.trip_id,),
                ),
            )
            for idx, stop_id in query:
                # Use setdefault to use the smallest possible stop_sequence for any given stop_id,
                # just in case a trip stops at stop_id multiple times
                trip.stop_sequence_by_id.setdefault(stop_id, idx)
        return trips

    def insert_block_trips(self, db: DBConnection, b: Block) -> None:
        date_str = b.last_trip_id.partition("_")[0]
        block_id = self._get_next_block_id(date_str)
        previous_trip_id: str | None = None
        headsign = self._get_stop_name(db, b.last_trip_id, b.legs[-1].to_stop_sequence)
        carriages = "/".join(sorted(b.carriages))

        for i, leg in enumerate(b.legs):
            is_first = i == 0
            is_last = i == len(b.legs) - 1

            trip = db.retrieve_must(Trip, leg.trip_id)
            trip.id = f"{block_id}-{i}"
            trip.block_id = block_id
            trip.headsign = headsign
            trip.set_extra_field("carriages", carriages)

            stop_times = leg.select_stop_times(db)
            stop_times[0].arrival_time = stop_times[0].departure_time
            stop_times[-1].departure_time = stop_times[-1].arrival_time

            for i, st in enumerate(stop_times):
                st.trip_id = trip.id
                st.stop_sequence = i

                if is_first:
                    # First leg of the block - forbid drop off.
                    # It's already handled by the original trip, as per:
                    # https://support.google.com/transitpartners/answer/7084064
                    st.drop_off_type = StopTime.PassengerExchange.NONE

                if is_last:
                    # First leg of the block - forbid pickup.
                    # It's already handled by the original trip, as per:
                    # https://support.google.com/transitpartners/answer/7084064
                    st.pickup_type = StopTime.PassengerExchange.NONE

            db.create(trip)
            db.create_many(StopTime, stop_times)

            if previous_trip_id:
                db.create(
                    Transfer(
                        from_stop_id=stop_times[0].stop_id,
                        to_stop_id=stop_times[0].stop_id,
                        from_trip_id=previous_trip_id,
                        to_trip_id=trip.id,
                        type=Transfer.Type.IN_SEAT,
                    )
                )

            previous_trip_id = trip.id

    @staticmethod
    def _get_empty_trips_for_connections(connections: Iterable[Connection]) -> dict[str, TripStops]:
        trips = dict[str, TripStops]()
        for c in connections:
            if c.from_trip_id not in trips:
                trips[c.from_trip_id] = TripStops(c.from_trip_id, {})
            if c.to_trip_id not in trips:
                trips[c.to_trip_id] = TripStops(c.to_trip_id, {})
        return trips

    @staticmethod
    def _get_stop_name(db: DBConnection, trip_id: str, stop_sequence: int | None = None) -> str:
        if stop_sequence is None:
            q = db.raw_execute(
                "SELECT name FROM stop_times LEFT JOIN stops USING (stop_id) "
                "WHERE trip_id = ? ORDER BY stop_sequence DESC LIMIT 1",
                (trip_id,),
            )
        else:
            q = db.raw_execute(
                "SELECT name FROM stop_times LEFT JOIN stops USING (stop_id) "
                "WHERE trip_id = ? AND stop_sequence = ?",
                (trip_id, stop_sequence),
            )
        return cast(str, q.one_must("invalid TripSlice")[0])
