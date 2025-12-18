# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import re
from itertools import chain
from typing import Any

from impuls.db import DBConnection
from impuls.model import Transfer, Trip
from impuls.tasks import SplitTripLegs


class SplitTripLegsRetainingTransfers(SplitTripLegs):
    def __init__(self) -> None:
        super().__init__(
            replacement_bus_short_name_pattern=re.compile(r"\bZKA\b", re.IGNORECASE),
        )

    def update_trip_with_single_leg(self, trip: Trip, data: Any, db: DBConnection) -> None:
        # Update the trip
        super().update_trip_with_single_leg(trip, data, db)

        # Replace in-seat transfers to and from a bus leg by timed transfers
        if data:
            db.raw_execute(
                "UPDATE transfers SET transfer_type = 1 "
                "WHERE transfer_type = 4 AND (from_trip_id = ? OR to_trip_id = ?)",
                (trip.id, trip.id),
            )

    def replace_trip_by_legs(
        self,
        original_trip: Trip,
        legs: list[SplitTripLegs.Leg],
        db: DBConnection,
    ) -> None:
        # Get in-seat transfers to recreate after the original trip is removed
        to_in_seat_transfers = db.typed_out_execute(
            "SELECT * FROM transfers WHERE transfer_type = 4 AND to_trip_id = ?",
            Transfer,
            (original_trip.id,),
        ).all()
        from_in_seat_transfers = db.typed_out_execute(
            "SELECT * FROM transfers WHERE transfer_type = 4 AND from_trip_id = ?",
            Transfer,
            (original_trip.id,),
        ).all()

        # Replace the trip by its legs
        super().replace_trip_by_legs(original_trip, legs, db)

        # Update (now removed) in-seat transfers to the original_trip to point to the first leg
        first_trip_id = f"{original_trip.id}_0"
        first_is_bus = legs[0][1]
        for transfer in to_in_seat_transfers:
            transfer.to_trip_id = first_trip_id
            transfer.type = Transfer.Type.TIMED if first_is_bus else Transfer.Type.IN_SEAT

        # Update (now removed) in-seat transfers from the original_trip to point from the last leg
        last_trip_id = f"{original_trip.id}_{len(legs) - 1}"
        last_is_bus = legs[-1][1]
        for transfer in from_in_seat_transfers:
            transfer.from_trip_id = last_trip_id
            transfer.type = Transfer.Type.TIMED if last_is_bus else Transfer.Type.IN_SEAT

        # Re-create the transfers
        db.create_many(Transfer, chain(to_in_seat_transfers, from_in_seat_transfers))
