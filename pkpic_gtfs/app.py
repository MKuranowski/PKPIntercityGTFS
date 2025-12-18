# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from argparse import ArgumentParser, Namespace

from impuls import App, Pipeline, PipelineOptions
from impuls.model import Agency
from impuls.resource import HTTPResource, LocalResource
from impuls.tasks import AddEntity, ExecuteSQL, GenerateTripHeadsign, ModifyRoutesFromCSV, SaveGTFS

from .create_feed_info import CreateFeedInfo
from .ftp import FTPResource
from .gtfs import GTFS_HEADERS
from .in_seat_transfers import AssignBlockIds, FixTimeTravelTransfers, GenerateInSeatTransfers
from .load_csv import LoadCSV
from .load_stations import LoadStationData
from .simplify_routes import SimplifyRoutes
from .split_trip_legs import SplitTripLegsRetainingTransfers


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
                ExecuteSQL(
                    statement="DELETE FROM stops WHERE stop_id = '179215'",
                    task_name="RemoveHorka",
                ),
                ExecuteSQL(
                    statement=(
                        "UPDATE trips SET short_name = "
                        r"re_sub('^(\d+)\s+Chełmoński\s+\d+\s+Saxonia$', '\1 Chełmoński', short_name)"
                    ),
                    task_name="FixChelmonskiShortName",
                ),
                ExecuteSQL(
                    statement=(
                        "UPDATE trips SET short_name = "
                        r"re_sub('^Uznam\s+(\d+)\s+Ursa$', '\1 Uznam', short_name)"
                    ),
                    task_name="FixUznamShortName",
                ),
                LoadStationData(),
                SimplifyRoutes(),
                GenerateTripHeadsign(),
                GenerateInSeatTransfers(),
                FixTimeTravelTransfers(),
                SplitTripLegsRetainingTransfers(),
                AssignBlockIds(),
                ModifyRoutesFromCSV("routes.csv", must_curate_all=True, silent=True),
                CreateFeedInfo(),
                SaveGTFS(headers=GTFS_HEADERS, target=args.output, ensure_order=True),
            ],
            resources={
                "kpd_rozklad.zip": FTPResource("rozklad/KPD_Rozklad.zip"),
                "pl_rail_map.osm": HTTPResource.get(
                    "https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm"
                ),
                "routes.csv": LocalResource("data/routes.csv"),
            },
            options=options,
        )
