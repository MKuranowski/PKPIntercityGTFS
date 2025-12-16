# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import dataclasses
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from io import TextIOWrapper
from typing import LiteralString, cast
from zipfile import ZipFile

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Date, StopTime, Transfer, Trip
from impuls.tools.temporal import DateRange, InfiniteDateRange, date_range
from impuls.tools.types import SQLNativeType, StrPath


@dataclass
class TripSlice:
    trip_id: str
    from_stop_sequence: int = 0
    to_stop_sequence: int | None = None

    def __repr__(self) -> str:
        parts = [repr(self.trip_id), "["]
        if self.from_stop_sequence:
            parts.append(str(self.from_stop_sequence))
        parts.append(":")
        if self.to_stop_sequence is not None:
            parts.append(str(self.to_stop_sequence))
        parts.append("]")
        return "".join(parts)

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
    id: str
    stop_sequence_by_id: dict[str, int]

    def stops_later(self, stop_a: str | int, stop_b: str | int) -> bool:
        a_idx = self.stop_sequence_by_id[stop_a] if isinstance(stop_a, str) else stop_a
        b_idx = self.stop_sequence_by_id[stop_b] if isinstance(stop_b, str) else stop_b
        return b_idx > a_idx

    def up_to(self, stop_id: str) -> TripSlice:
        return TripSlice(self.id, to_stop_sequence=self.stop_sequence_by_id[stop_id])

    def starting_at(self, stop_id: str) -> TripSlice:
        return TripSlice(self.id, from_stop_sequence=self.stop_sequence_by_id[stop_id])


@dataclass
class Block:
    legs: list[TripSlice]
    carriages: frozenset[str]

    def issuperset(self, o: "Block") -> bool:
        return o.issubset(self)

    def issubset(self, o: "Block") -> bool:
        if len(self.legs) > len(o.legs):
            return False
        return any(
            self.legs == o.legs[offset : offset + len(self.legs)]
            for offset in range(len(o.legs) - len(self.legs) + 1)
        )

    def __repr__(self) -> str:
        return " -> ".join(repr(i) for i in self.legs)


@dataclass
class InSeatTransfer:
    from_trip_id: str
    to_trip_id: str
    at_stop_id: str
    carriages: frozenset[str]

    def is_valid(self, trips: Mapping[str, TripStops]) -> bool:
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


class GenerateInSeatTransfers(Task):
    def __init__(self) -> None:
        super().__init__()
        self.block_id_counter = 0

    def clear(self) -> None:
        self.block_id_counter = 0

    def get_block_id(self, prefix: str = "") -> str:
        id = f"{prefix}{self.block_id_counter}"
        self.block_id_counter += 1
        return id

    def execute(self, r: TaskRuntime) -> None:
        active_days = self.get_active_date_range(r.db)
        transfers_by_day = self.get_transfers_by_day(
            r.resources["kpd_rozklad.zip"].stored_at,
            active_days,
        )
        blocks = [
            block
            for transfers in transfers_by_day.values()
            for block in _deduplicate_blocks(self.get_blocks_for_day(r.db, transfers))
        ]

        with r.db.transaction():
            for block in blocks:
                self.insert_block_trips(r.db, block.legs)

    def get_active_date_range(self, db: DBConnection) -> DateRange:
        # fmt: off
        start_str, end_str = (
            db.raw_execute("SELECT min(start_date), max(end_date) FROM calendars")
            .one_must("empty calendar")
        )
        return date_range(
            Date.fromisoformat(cast(str, start_str)),
            Date.fromisoformat(cast(str, end_str)),
        )
        # fmt: on

    def get_transfers_by_day(
        self,
        pkg_path: StrPath,
        days: DateRange = InfiniteDateRange(),
    ) -> defaultdict[Date, list["InSeatTransfer"]]:
        by_day = defaultdict[Date, list[InSeatTransfer]](list)
        with (
            ZipFile(pkg_path, "r") as pkg,
            pkg.open("KPD_Rozklad_Przelaczenia.csv") as csv_file_binary,
        ):
            csv_file = TextIOWrapper(csv_file_binary, encoding="windows-1250", newline="")
            for day, transfer in read_all_transfers(csv_file):
                if day in days:
                    by_day[day].append(transfer)
        return by_day

    def get_blocks_for_day(
        self,
        db: DBConnection,
        transfers: Sequence[InSeatTransfer],
    ) -> Iterable[Block]:
        trips = self.get_trips_for_transfers(db, transfers)
        valid_transfers = filter(lambda t: t.is_valid(trips), transfers)
        yield from find_all_blocks(valid_transfers, trips)

    def get_trips_for_transfers(
        self,
        db: DBConnection,
        transfers: Iterable[InSeatTransfer],
    ) -> dict[str, TripStops]:
        trips = {id: TripStops(id, {}) for id in _enumerate_interesting_trip_ids(transfers)}
        for trip in trips.values():
            query = cast(
                Iterable[tuple[int, str]],
                db.raw_execute(
                    "SELECT stop_sequence, stop_id FROM stop_times WHERE trip_id = ?",
                    (trip.id,),
                ),
            )
            for idx, stop_id in query:
                # Use setdefault to use the smallest possible stop_sequence for any given stop_id,
                # just in case a trip stops at stop_id multiple times
                trip.stop_sequence_by_id.setdefault(stop_id, idx)
        return trips

    def insert_block_trips(self, db: DBConnection, legs: Sequence[TripSlice]) -> None:
        block_id = self.get_block_id()
        previous_trip_id: str | None = None
        headsign = self.get_stop_name(db, legs[-1].trip_id, legs[-1].to_stop_sequence)
        for i, leg in enumerate(legs):
            previous_trip_id = self.insert_new_trip_slice(
                db,
                leg,
                i,
                block_id,
                headsign,
                previous_trip_id=previous_trip_id,
                is_first=i == 0,
                is_last=i == len(legs) - 1,
            )

    def insert_new_trip_slice(
        self,
        db: DBConnection,
        leg: TripSlice,
        i: int,
        block_id: str,
        headsign: str,
        previous_trip_id: str | None = None,
        is_first: bool = False,
        is_last: bool = False,
    ) -> str:
        trip = db.retrieve_must(Trip, leg.trip_id)
        trip.id = f"{leg.trip_id.partition('_')[0]}_block{block_id}-{i}"
        trip.block_id = block_id
        trip.headsign = headsign

        stop_times = leg.select_stop_times(db)
        stop_times[0].arrival_time = stop_times[0].departure_time
        stop_times[-1].departure_time = stop_times[-1].arrival_time
        for i, st in enumerate(stop_times):
            st.trip_id = trip.id
            st.stop_sequence = i

            if is_first:
                st.drop_off_type = StopTime.PassengerExchange.NONE

            if is_last:
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

        return trip.id

    @staticmethod
    def get_stop_name(db: DBConnection, trip_id: str, stop_sequence: int | None = None) -> str:
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


def read_all_transfers(f: Iterable[str]) -> Iterable[tuple[Date, InSeatTransfer]]:
    for row in csv.DictReader(f, delimiter=";"):
        from_id_suffix = row["NrPoc1"].replace("/", "-")
        to_id_suffix = row["NrPoc2"].replace("/", "-")
        at_stop_id = row["objectID"]
        carriages = frozenset(row["carriageNo"].split(","))

        for day in read_transfer_dates(row):
            from_trip_id = f"{day.isoformat()}_{from_id_suffix}"
            to_trip_id = f"{day.isoformat()}_{to_id_suffix}"
            yield day, InSeatTransfer(from_trip_id, to_trip_id, at_stop_id, carriages)


def read_transfer_dates(row: Mapping[str, str]) -> Iterable[Date]:
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


def find_all_blocks(
    transfers: Iterable[InSeatTransfer],
    trips: Mapping[str, TripStops],
) -> Iterable[Block]:
    # Initialize the search space
    transfers_by_from_trip_id = defaultdict[str, list[InSeatTransfer]](list)
    queue = list[Block]()
    for t in transfers:
        transfers_by_from_trip_id[t.from_trip_id].append(t)
        queue.append(
            Block(
                [
                    trips[t.from_trip_id].up_to(t.at_stop_id),
                    trips[t.to_trip_id].starting_at(t.at_stop_id),
                ],
                t.carriages,
            )
        )

    # Generate all possible in-seat transfer-based blocks
    while queue:
        b = queue.pop()
        further_candidates = [
            transfer
            for transfer in transfers_by_from_trip_id[b.legs[-1].trip_id]
            if transfer.carriages.issubset(b.carriages)
            # and trips[transfer.from_trip_id].stops_later(
            #     legs[-1].from_stop_sequence,
            #     transfer.at_stop_id,
            # )
        ]

        if further_candidates:
            for t in further_candidates:
                assert b.legs[-1].trip_id == t.from_trip_id
                from_stop_idx = trips[t.from_trip_id].stop_sequence_by_id[t.at_stop_id]
                assert b.legs[-1].from_stop_sequence < from_stop_idx

                queue.append(
                    Block(
                        b.legs[:-1]
                        + [
                            dataclasses.replace(b.legs[-1], to_stop_sequence=from_stop_idx),
                            trips[t.to_trip_id].starting_at(t.at_stop_id),
                        ],
                        b.carriages & t.carriages,
                    )
                )

        else:
            # This set of carriages isn't transferred again, yield it as a complete block
            yield b


def _deduplicate_blocks(blocks: Iterable[Block]) -> list[Block]:
    unique = list[Block]()

    for block in blocks:
        for i, candidate in enumerate(unique):
            if block.issuperset(candidate):
                unique[i] = block
                break
            elif block.issubset(candidate):
                break
        else:
            unique.append(block)

    return unique


def _enumerate_interesting_trip_ids(transfers: Iterable[InSeatTransfer]) -> set[str]:
    ids = set[str]()
    for t in transfers:
        ids.add(t.from_trip_id)
        ids.add(t.to_trip_id)
    return ids
