# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from argparse import Namespace

from impuls import App, Pipeline, PipelineOptions
from impuls.model import Agency
from impuls.resource import ZippedResource
from impuls.tasks import AddEntity

from .ftp import FTPResource
from .load_csv import LoadCSV


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
            ],
            resources={
                "kpd_rozklad.csv": ZippedResource(
                    r=FTPResource("rozklad/KPD_Rozklad.zip"),
                    file_name_in_zip="KPD_Rozklad.csv",
                )
            },
            options=options,
        )
