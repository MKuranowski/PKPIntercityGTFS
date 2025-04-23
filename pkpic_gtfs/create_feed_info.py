# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from zoneinfo import ZoneInfo

from impuls import Task, TaskRuntime
from impuls.model import FeedInfo

POLAND_TZ = ZoneInfo("Europe/Warsaw")


class CreateFeedInfo(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        source_timestamp = r.resources["kpd_rozklad.csv"].last_modified.astimezone(POLAND_TZ)
        r.db.create(
            FeedInfo(
                publisher_name="Mikołaj Kuranowski",
                publisher_url="https://mkuran.pl/gtfs/",
                lang="pl",
                version=source_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
