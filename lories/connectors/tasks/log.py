# -*- coding: utf-8 -*-
"""
lories.connectors.tasks.log
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from typing import Optional

import pandas as pd
from lories._core._channels import Channels  # noqa
from lories._core._connector import Connector  # noqa
from lories.connectors.tasks.write import WriteTask


class LogTask(WriteTask):
    data: Optional[pd.DataFrame]

    def __init__(self, connector: Connector, channels: Channels, data: Optional[pd.DataFrame] = None, **kwargs):
        super().__init__(connector, channels, **kwargs)
        self.data = data

    def run(self) -> None:
        self._logger.debug(
            f"Logging {len(self.channels)} channels of '{type(self.connector).__name__}': {self.connector.id}"
        )
        # Pass copied connectors instead of actual objects, including parsed logger specific connector configurations
        channels = self.channels.from_logger()

        if self.data is None:
            self._run_write(channels)
        else:
            self.connector.write(channels.to_frame(unique=True).combine_first(self.data))
