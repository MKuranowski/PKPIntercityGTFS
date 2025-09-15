# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import re
from argparse import ArgumentParser, Namespace

from impuls import App, Pipeline, PipelineOptions
from impuls.model import Agency
from impuls.resource import HTTPResource, LocalResource, ZippedResource
from impuls.tasks import (
    AddEntity,
    ExecuteSQL,
    GenerateTripHeadsign,
    ModifyRoutesFromCSV,
    SaveGTFS,
    SplitTripLegs,
)

from .create_feed_info import CreateFeedInfo
from .ftp import FTPResource
from .gtfs import GTFS_HEADERS
from .load_csv import LoadCSV
from .load_stations import LoadStationData
from .simplify_routes import SimplifyRoutes
from .load_platforms import LoadPlatformData


class PKPIntercityGTFS(App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "-o",
            "--output",
            default="pkpic.zip",
            help="path to the output GTFS file",
        )

    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        return Pipeline(
            tasks=[
                AddEntity(
                    entity=Agency(
                        id="0",
                        name="PKP Intercity",
                        url="https://intercity.pl/",
                        timezone="Europe/Warsaw",
                        lang="pl",
                        phone="+48703200200",
                    ),
                    task_name="AddAgency",
                ),
                LoadCSV(),
                ExecuteSQL(
                    statement="DELETE FROM stops WHERE stop_id = '201084'",
                    task_name="RemoveBohuminVrbice",
                ),
                LoadStationData(),
                LoadPlatformData(),
                SimplifyRoutes(),
                GenerateTripHeadsign(),
                SplitTripLegs(replacement_bus_short_name_pattern=re.compile(r"\bZKA\b", re.I)),
                ModifyRoutesFromCSV("routes.csv", must_curate_all=True, silent=True),
                CreateFeedInfo(),
                SaveGTFS(headers=GTFS_HEADERS, target=args.output, ensure_order=True),
            ],
            resources={
                "kpd_rozklad.csv": ZippedResource(
                    r=FTPResource("rozklad/KPD_Rozklad.zip"),
                    file_name_in_zip="KPD_Rozklad.csv",
                ),
                "pl_rail_map.osm": HTTPResource.get(
                    "https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm"
                ),
                "platforms.json": HTTPResource.get(
                    "https://kasmar00.github.io/osm-plk-platform-validator/platforms-list.json"
                ),
                "routes.csv": LocalResource("data/routes.csv"),
            },
            options=options,
        )
