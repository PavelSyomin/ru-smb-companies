import pathlib
from typing import Optional

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from ..stages.spark_stage import SparkStage
from ..utils.spark_schemas import (smb_geocoded_schema, revexp_agg_schema,
    empl_agg_schema)


class Panelizer(SparkStage):
    SPARK_APP_NAME = "Panel Table Maker"

    def __call__(self, smb_file: str, out_file: str,
                 revexp_file: Optional[str] = None,
                 empl_file: Optional[str] = None):
        smb_data = self._read(smb_file, smb_geocoded_schema)
        if smb_data is None:
            return

        panel = (
            smb_data
            .withColumn(
                "year",
                F.explode(F.sequence(F.year("start_date"), F.year("end_date")))
            )
        )

        if revexp_file is not None:
            revexp_data = self._read(revexp_file, revexp_agg_schema)
            if revexp_data is not None:
                print("Joining with revexp data")
                panel = panel.join(revexp_data, on=["tin", "year"], how="leftouter")

        if empl_file is not None:
            empl_data = self._read(empl_file, empl_agg_schema)
            if empl_data is not None:
                print("Joining with empl data")
                panel = panel.join(empl_data, on=["tin", "year"], how="leftouter")

        panel = panel.orderBy("tin", "year")

        self._write(panel, out_file)
