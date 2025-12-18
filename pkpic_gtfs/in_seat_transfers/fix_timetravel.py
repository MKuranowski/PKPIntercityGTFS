# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from dataclasses import dataclass, replace
from datetime import timedelta
from typing import NamedTuple, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Date, TimePoint, Transfer


class TrainKey(NamedTuple):
    date: Date
    number: str

    @staticmethod
    def extract(trip_id: str) -> "TrainKey":
        date_str, number, *_ = trip_id.split("_")
        return TrainKey(Date.from_ymd_str(date_str), number)

    def next_day(self) -> "TrainKey":
        return TrainKey(self.date.add_days(1), self.number)


@dataclass
class TimedTransfer:
    t: Transfer
    from_trip_time: TimePoint
    to_trip_time: TimePoint

    @property
    def key(self) -> tuple[TrainKey, TrainKey]:
        return (self.from_trip_key, self.to_trip_key)

    @property
    def from_trip_key(self) -> TrainKey:
        return TrainKey.extract(self.t.from_trip_id)

    @property
    def to_trip_key(self) -> TrainKey:
        return TrainKey.extract(self.t.to_trip_id)

    def time_travels(self) -> bool:
        return self.to_trip_time < self.from_trip_time


class FixTimeTravelTransfers(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        trips_used_in_from_transfers = {
            cast(str, i[0]) for i in r.db.raw_execute("SELECT from_trip_id FROM transfers")
        }

        transfers = self.get_transfers_to_process(r.db)
        transfers_by_key_pair = {t.key: t for t in transfers}

        fixed_transfers = list[Transfer]()
        trips_to_remove = list[str]()

        for t in transfers:
            # Try to find a transfer between these two trains, but on the following day.
            #
            # The assumption is that if the transfer from trip A to trip B on day X is broken,
            # the transfer from trip A' to trip B' on day X+1 is also broken, and we can re-link
            # A to B', A' to B" and so on.
            next_key = (t.from_trip_key.next_day(), t.to_trip_key.next_day())
            next_t = transfers_by_key_pair.get(next_key)

            next_would_be_valid = (
                next_t
                and t.t.from_stop_id == next_t.t.from_stop_id
                and t.t.to_stop_id == next_t.t.to_stop_id
                and t.from_trip_time < (next_t.to_trip_time + timedelta(days=1))
            )

            if next_would_be_valid:
                # Re-link the transfer
                assert next_t is not None
                self.logger.debug("Relinking %s to %s", t.t.from_trip_id, next_t.t.to_trip_id)
                fixed_transfers.append(replace(t.t, to_trip_id=next_t.t.to_trip_id))
            elif t.t.to_trip_id not in trips_used_in_from_transfers:
                # Unable to fix, and the (broken) to_trip is not used in further transfers -
                # remove the to_trip
                assert "_C" in t.t.to_trip_id
                self.logger.debug(
                    "Unable to fix %s - removing trip %s",
                    t.t.from_trip_id,
                    t.t.to_trip_id,
                )
                trips_to_remove.append(t.t.to_trip_id)
            else:
                # Unable to fix, but the (broken) to_trip is used in further transfers -
                # only remove the broken transfer
                self.logger.debug("Unable to fix %s", t.t.from_trip_id)

        self.logger.info(
            "Relinked %d / %d (%.2f %%) time travelling in-seat transfers",
            len(fixed_transfers),
            len(transfers),
            100 * len(fixed_transfers) / len(transfers),
        )

        with r.db.transaction():
            # Remove the time-travelling transfers
            r.db.raw_execute_many(
                "DELETE FROM transfers WHERE from_trip_id = ? AND to_trip_id = ?",
                ((t.t.from_trip_id, t.t.to_trip_id) for t in transfers),
            )

            # Add back the fixed transfers
            r.db.create_many(Transfer, fixed_transfers)

            # Remove unnecessary trip copies
            r.db.raw_execute_many(
                "DELETE FROM trips WHERE trip_id = ?",
                ((trip_id,) for trip_id in trips_to_remove),
            )

    def get_transfers_to_process(self, db: DBConnection) -> list[TimedTransfer]:
        to_process = list[TimedTransfer]()
        transfers = db.typed_out_execute(
            "SELECT * FROM transfers WHERE transfer_type = 4",
            Transfer,
        )

        for t in transfers:
            timed = TimedTransfer(
                t,
                from_trip_time=self._get_trip_end(db, t.from_trip_id),
                to_trip_time=self._get_trip_start(db, t.to_trip_id),
            )
            if timed.time_travels():
                to_process.append(timed)

        return to_process

    @staticmethod
    def _get_trip_start(db: DBConnection, trip_id: str) -> TimePoint:
        seconds = cast(
            int,
            db.raw_execute(
                "SELECT min(arrival_time) FROM stop_times WHERE trip_id = ?",
                (trip_id,),
            ).one_must(f"trip without times: {trip_id!r}")[0],
        )
        return TimePoint(seconds=seconds)

    @staticmethod
    def _get_trip_end(db: DBConnection, trip_id: str) -> TimePoint:
        seconds = cast(
            int,
            db.raw_execute(
                "SELECT max(departure_time) FROM stop_times WHERE trip_id = ?",
                (trip_id,),
            ).one_must(f"trip without times: {trip_id!r}")[0],
        )
        return TimePoint(seconds=seconds)

    @staticmethod
    def next_day_trip(trip_id: str) -> str:
        date_str, _, suffix = trip_id.partition("_")
        next_day = Date.fromisoformat(date_str).add_days(1)
        return f"{next_day.isoformat()}_{suffix}"
