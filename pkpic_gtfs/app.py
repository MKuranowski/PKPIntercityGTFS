# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from argparse import Namespace

from impuls import App, Pipeline, PipelineOptions
from impuls.resource import ZippedResource

from .ftp import FTPResource


class PKPIntercityGTFS(App):
    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        return Pipeline(
            tasks=[],
            resources={
                "kpd_rozklad.csv": ZippedResource(
                    r=FTPResource("rozklad/KPD_Rozklad.zip"),
                    file_name_in_zip="KPD_Rozklad.csv",
                )
            },
            options=options,
        )
