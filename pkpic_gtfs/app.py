# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from argparse import Namespace

import impuls


class PKPIntercityGTFS(impuls.App):
    def prepare(self, args: Namespace, options: impuls.PipelineOptions) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[],
            options=options,
        )
