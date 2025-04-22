# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from argparse import Namespace

from impuls import App, Pipeline, PipelineOptions
from impuls.model import Agency
from impuls.resource import HTTPResource, ZippedResource
from impuls.tasks import AddEntity, ExecuteSQL, GenerateTripHeadsign

from .ftp import FTPResource
from .load_csv import LoadCSV
from .load_stations import LoadStationData
from .simplify_routes import SimplifyRoutes


class PKPIntercityGTFS(App):
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
                SimplifyRoutes(),
                GenerateTripHeadsign(),
                # TODO: split bus legs
                # TODO: curate routes
                # TODO: create feed info
                # TODO: save GTFS
            ],
            resources={
                "kpd_rozklad.csv": ZippedResource(
                    r=FTPResource("rozklad/KPD_Rozklad.zip"),
                    file_name_in_zip="KPD_Rozklad.csv",
                ),
                "pl_rail_map.osm": HTTPResource.get(
                    "https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm"
                ),
            },
            options=options,
        )
