# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import logging
from collections import defaultdict
from collections.abc import Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from io import TextIOWrapper
from itertools import combinations
from operator import attrgetter
from typing import Callable, LiteralString, Self, TypeVar, cast
from zipfile import ZipFile

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Date, StopTime, Transfer, Trip
from impuls.tools.temporal import DateRange, InfiniteDateRange, date_range
from impuls.tools.types import SQLNativeType, StrPath

from ..util import DisjointSet

_K = TypeVar("_K", bound=Hashable)
_V = TypeVar("_V")


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
    stop_sequence_by_id: dict[str, int] = field(default_factory=dict[str, int])

    def insert_stop(self, idx: int, stop_id: str) -> None:
        # If trip stops multiple times at the given stop_id, only record the smallest index
        self.stop_sequence_by_id.setdefault(stop_id, idx)

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
    id: int = 0

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

    @staticmethod
    def group_related(connections: Iterable["Connection"]) -> Iterable[list["Connection"]]:
        """Groups multiple Connections together if they switch the same carriages.
        Two Connections will be in the same group if their trip and carriage sets intersect.
        """
        by_conn_id = build_index(Connection.deduplicate(connections), attrgetter("id"))
        related = DisjointSet(by_conn_id)

        for a, b in combinations(by_conn_id.values(), 2):
            trips_intersect = (
                a.from_trip_id == b.from_trip_id
                or a.from_trip_id == b.to_trip_id
                or a.to_trip_id == b.from_trip_id
                or a.to_trip_id == b.to_trip_id
            )
            carriages_intersect = not a.carriages.isdisjoint(b.carriages)
            if trips_intersect and carriages_intersect:
                related.merge(a.id, b.id)

        for ids in related.get_groups().values():
            yield [by_conn_id[i] for i in ids]

    @staticmethod
    def deduplicate(connections: Iterable["Connection"]) -> list["Connection"]:
        """Ensures (from_trip_id, to_trip_id, at_stop_id) is a unique key within Connections.

        If multiple Connections link up two trips at the same stop, the returned Connection
        will have the ID of the first such connection and carriages as the union of all
        such connections.

        >>> Connection.deduplicate([Connection("T1", "T2", "S0", frozenset({"1", "2"}), 1),
        ...                         Connection("T1", "T2", "S0", frozenset({"2", "3"}), 2)])
        [Connection("T1", "T2", "S0", frozenset({'1', '2', '3'}), 1)]
        """
        unique = dict[tuple[str, str, str], tuple[int, set[str]]]()
        for c in connections:
            key = c.from_trip_id, c.to_trip_id, c.at_stop_id
            if existing := unique.get(key):
                existing[1].update(c.carriages)
            else:
                unique[key] = c.id, set(c.carriages)
        return [
            Connection(from_trip_id, to_trip_id, at_stop_id, frozenset(carriages), id)
            for (from_trip_id, to_trip_id, at_stop_id), (id, carriages) in unique.items()
        ]

    @staticmethod
    def get_disjoint_carriage_sets(connections: Iterable["Connection"]) -> DisjointSet[str]:
        """Returns a DisjointSet of all carriages used by the provided connections,
        where two carriages are in the same set if and only if they use exactly
        the same connections.
        """

        # Compute the set of used connections for each carriage
        carriage_connections = defaultdict[str, set[int]](set)
        for connection in connections:
            for carriage in connection.carriages:
                carriage_connections[carriage].add(connection.id)

        # Merge carriages with exactly the same connection set
        carriages = DisjointSet(carriage_connections)
        for (a, a_connections), (b, b_connections) in combinations(carriage_connections.items(), 2):
            if a_connections == b_connections:
                carriages.merge(a, b)
        return carriages

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
                id=int(row["ID_przelaczenia"]),
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
    def find_all(
        cls,
        connections: Iterable[Connection],
        trips: Mapping[str, TripStops],
        logger: logging.Logger = logging.getLogger("BlockResolver"),
    ) -> Iterable[Self]:
        """Finds all Blocks given a set of Connections and Trip data."""
        for connection_group in Connection.group_related(connections):
            yield from cls.resolve(connection_group, trips, logger=logger)

    @classmethod
    def resolve(
        cls,
        connections: Sequence[Connection],
        trips: Mapping[str, TripStops],
        logger: logging.Logger = logging.getLogger("BlockResolver"),
    ) -> list[Self]:
        """Resolves an independent set of connections given trip data into blocks.
        Any NonLinearBlock errors are caught and logged on the provided logger.
        """
        # Fast path for groups of one connection
        if len(connections) == 1:
            return [cls.resolve_single(connections[0], trips)]

        # Route each unique carriage group through the set of connections
        carriage_sets = Connection.get_disjoint_carriage_sets(connections).get_groups()
        blocks = list[Self]()
        for root_carriage, carriage_set in carriage_sets.items():
            try:
                block = cls.resolve_linear(
                    connections=[c for c in connections if root_carriage in c.carriages],
                    trips=trips,
                    carriages=frozenset(carriage_set),
                )
                blocks.append(block)
            except NonLinearBlock as e:
                e.log(logger)
        return blocks

    @classmethod
    def resolve_single(cls, c: Connection, trips: Mapping[str, TripStops]) -> Self:
        """Resolved a block using a single connection."""
        return cls(
            [
                trips[c.from_trip_id].up_to(c.at_stop_id),
                trips[c.to_trip_id].starting_at(c.at_stop_id),
            ],
            c.carriages,
        )

    @classmethod
    def resolve_linear(
        cls,
        connections: Sequence[Connection],
        trips: Mapping[str, TripStops],
        carriages: frozenset[str],
    ) -> Self:
        """Resolves a block from multiple connections, assuming they form a linear link.
        If that assumption does not hold, raises NonLinearBlock.
        """
        legs = list[TripSlice]()

        # Create a lookup table on from_trip_id for traversal of the block
        try:
            connections_by_from_trip_id = build_index(connections, attrgetter("from_trip_id"))
        except ValueError:
            raise NonLinearBlock.from_connections(
                "from_trip_id is not unique",
                carriages,
                connections,
            )

        # Find the very first trip
        first_trip = cls._find_initial_trip_in_linear_block(connections)
        if not first_trip:
            raise NonLinearBlock.from_connections("no initial trip", carriages, connections)

        # Create legs by following the connections forwards
        conn = connections_by_from_trip_id.pop(cls._find_initial_trip_in_linear_block(connections))
        legs.append(trips[conn.from_trip_id].up_to(conn.at_stop_id))
        while next := connections_by_from_trip_id.pop(conn.to_trip_id, None):
            t = trips[next.from_trip_id]
            start = t.stop_sequence_by_id[conn.at_stop_id]
            end = t.stop_sequence_by_id[next.at_stop_id]
            if start >= end:
                raise NonLinearBlock.from_connections(
                    f"goes backwards on trip {t.trip_id} ({start} → {end})",
                    carriages,
                    connections,
                )
            legs.append(TripSlice(t.trip_id, start, end))

            conn = next

        # Append the last leg
        legs.append(trips[conn.to_trip_id].starting_at(conn.at_stop_id))

        # Ensure we have used all of the connections
        if connections_by_from_trip_id:
            raise NonLinearBlock.from_connections("is disjoint", carriages, connections)

        return cls(legs, carriages)

    @staticmethod
    def _find_initial_trip_in_linear_block(connections: Sequence[Connection]) -> str | None:
        candidates = {i.from_trip_id for i in connections}
        for connection in connections:
            candidates.discard(connection.to_trip_id)

        if len(candidates) != 1:
            return None

        return candidates.pop()


class NonLinearBlock(ValueError):
    """Raised by Block.resolve_linear if the block doesn't appear to be linear."""

    def __init__(
        self,
        reason: str,
        carriages: Iterable[str],
        trip_ids: Iterable[str],
        connections: Iterable[int],
    ) -> None:
        self.reason = reason
        self.carriages = "/".join(sorted(carriages))
        self.trip_ids = sorted(trip_ids)
        self.connections = sorted(connections)

        super().__init__(
            f"non-linear block ({reason}): carriages={self.carriages} "
            f"trip_ids={self.trip_ids} connections={self.connections}"
        )

    @classmethod
    def from_connections(
        cls,
        reason: str,
        carriages: Iterable[str],
        connections: Iterable[Connection],
    ) -> Self:
        trip_ids = set[str]()
        connection_ids = set[int]()
        for conn in connections:
            connection_ids.add(conn.id)
            trip_ids.add(conn.from_trip_id)
            trip_ids.add(conn.to_trip_id)
        return cls(reason, carriages, trip_ids, connection_ids)

    def log(self, l: logging.Logger) -> None:
        l.error(self.args[0])


class GenerateInSeatTransfers(Task):
    """Adds in-seat transfers to the database by duplicating trips linked together
    by connections specified in KPD_Rozklad_Przelaczenia.csv file (stored in kpd_rozklad.zip)
    and joining them with both block_id and in-seat transfers.
    """

    def __init__(self) -> None:
        super().__init__()
        self.trip_copy_counters = defaultdict[str, int](lambda: 0)

    def clear(self) -> None:
        self.trip_copy_counters.clear()

    def _get_unique_trip_id(self, trip_id: str) -> str:
        id = f"{trip_id}_C{self.trip_copy_counters[trip_id]}"
        self.trip_copy_counters[trip_id] += 1
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
    ) -> Iterable[Block]:
        trips = self.get_trips_for_connections(db, connections)
        valid_connections = filter(lambda t: t.is_valid(trips), connections)
        yield from Block.find_all(valid_connections, trips)

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
                trip.insert_stop(idx, stop_id)
        return trips

    def insert_block_trips(self, db: DBConnection, b: Block) -> None:
        previous_trip_id: str | None = None
        headsign = self._get_stop_name(db, b.last_trip_id, b.legs[-1].to_stop_sequence)
        carriages = "/".join(sorted(b.carriages))

        for i, leg in enumerate(b.legs):
            is_first = i == 0
            is_last = i == len(b.legs) - 1

            trip = db.retrieve_must(Trip, leg.trip_id)
            trip.id = self._get_unique_trip_id(trip.id)
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


def build_index(items: Iterable[_V], key: Callable[[_V], _K]) -> dict[_K, _V]:
    """Builds a lookup table for items by the provided key function.

    Equivalent to `{key(i): i for i in items}`, except that ValueError is raised
    if the keys are not unique.
    """

    lookup = dict[_K, _V]()
    for v in items:
        k = key(v)
        if k in lookup:
            raise ValueError(f"duplicate key: {k} (from {lookup[k]} and {v})")
        lookup[k] = v
    return lookup
