# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Iterable
from math import inf, isinf
from typing import cast

from impuls import DBConnection, Task, TaskRuntime

NORMALIZATION = {
    "IC+": "IC",
}

PRIORITY = {name: idx for idx, name in enumerate(["EIP", "EIC", "IC", "TLK", "EN", "EC", "MP"])}


class SimplifyRoutes(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        all_routes = self.list_all_routes(r.db)
        with r.db.transaction():
            self.process_all_routes(r.db, all_routes)

    def list_all_routes(self, db: DBConnection) -> list[str]:
        return [cast(str, i[0]) for i in db.raw_execute("SELECT route_id FROM routes")]

    def process_all_routes(self, db: DBConnection, routes: Iterable[str]) -> None:
        for old_id in routes:
            if new_id := self.normalize_route(old_id):
                self.logger.info("Normalizing %r to %r", old_id, new_id)
                self.switch_route(db, old_id, new_id)

    def normalize_route(self, old_id: str) -> str | None:
        # Fast path for standard routes
        if " " not in old_id and old_id in PRIORITY:
            return None

        raw_parts = old_id.split()
        parts = (NORMALIZATION.get(i, i) for i in raw_parts)
        priority, best_fit = min((PRIORITY.get(i, inf), i) for i in parts)
        if isinf(priority):
            self.logger.warning("Unable to normalize route %r", best_fit)
            return None
        return best_fit if best_fit != old_id else None

    def switch_route(self, db: DBConnection, old_id: str, new_id: str) -> None:
        db.raw_execute(
            (
                "INSERT OR IGNORE INTO routes "
                "(route_id, agency_id, short_name, long_name, type) "
                "VALUES (?, '0', ?, '', 2)"
            ),
            (new_id, new_id),
        )
        db.raw_execute("UPDATE trips SET route_id = ? WHERE route_id = ?", (new_id, old_id))
        db.raw_execute("DELETE FROM routes WHERE route_id = ?", (old_id,))
