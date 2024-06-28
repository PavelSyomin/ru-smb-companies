from typing import List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (coalesce, desc, first, last, lead,
    lower, lpad, row_number, upper, year)

from ..stages.spark_stage import SparkStage
from ..utils.enums import SourceDatasets, Storages
from ..utils.spark_schemas import smb_schema, revexp_schema, empl_schema


class Aggregator(SparkStage):
    INPUT_DATE_FORMAT = "dd.MM.yyyy"
    SPARK_APP_NAME = "Extracted Data Aggregator"

    def __call__(self, in_dir: str, out_file: str,
                 source_dataset: str, smb_data_file: Optional[str] = None):
        """Execute the aggregation of all datasets"""
        if source_dataset == SourceDatasets.smb.value:
            self._process_smb_registry(in_dir, out_file)
        elif source_dataset == SourceDatasets.revexp.value:
            self._process_revexp_data(in_dir, out_file, smb_data_file)
        elif source_dataset == SourceDatasets.empl.value:
            self._process_empl_data(in_dir, out_file, smb_data_file)
        else:
            raise RuntimeError(
                f"Unsupported source dataset {source_dataset}, "
                f"expected one of {[sd.value for sd in SourceDatasets]}"
            )

    def _filter_by_tins(self, table: DataFrame, smb_data_file: str) -> DataFrame:
        smb_data = self._session.read.options(header=True, escape='"').csv(smb_data_file)
        tins = smb_data.filter("kind == 1").select("tin")

        table = table.join(tins, on="tin", how="leftsemi")

        return table

    def _process_smb_registry(self, in_dir: str, out_file: str):
        """Process CSV files extacted from SMB registry archives"""
        data = self._read(in_dir, smb_schema, dateFormat=self.INPUT_DATE_FORMAT)
        if data is None:
            return

        cols_to_check_for_duplicates = [
            "kind", "category", "tin", "reg_number",
            "first_name", "last_name", "patronymic",
            "org_name", "org_short_name",
            "region_name",
            "district_name", "city_name", "settlement_name",
            "activity_code_main"
        ]
        cols_to_select = [
            "kind",
            "category",
            "tin",
            "reg_number",
            "first_name",
            "last_name",
            "patronymic",
            "org_name",
            "org_short_name",
            "region_code",
            "region_name",
            "region_type",
            "district_name",
            "district_type",
            "city_name",
            "city_type",
            "settlement_name",
            "settlement_type",
            "activity_code_main",
            "start_date",
            "end_date",
        ]
        cols_to_uppercase = [
            "first_name", "last_name", "patronymic",
            "org_name", "org_short_name",
            "region_name", "region_type",
            "district_name", "district_type",
            "city_name", "city_type",
            "settlement_name", "settlement_type",
        ]
        excluded_regions = [
            "Крым",
            "Севастополь",
            "Донецкая",
            "Луганская",
            "Запорожская",
            "Херсонская"
        ]
        excluded_regions_condition = (
            "not ("
            + " or ".join(f"region_name ilike '%{region.upper()}%'" for region in excluded_regions)
            + ")"
        )
        w_for_row_number = (
            Window
            .partitionBy(cols_to_check_for_duplicates)
            .orderBy("data_date")
        )
        w_for_end_date = w_for_row_number.rowsBetween(0, Window.unboundedFollowing)
        w_for_reg_number = (
            Window
            .partitionBy(["tin"])
            .orderBy("data_date")
            .rowsBetween(0, Window.unboundedFollowing)
        )

        table = (
            data
            .filter(excluded_regions_condition)
            .withColumns({
                colname: upper(colname)
                for colname in cols_to_uppercase
            })
            .withColumns({
                "ind_tin": lpad("ind_tin", 12, "0"),
                "org_tin": lpad("org_tin", 10, "0"),
            })
            .withColumns({
                "tin": coalesce("ind_tin", "org_tin"),
                "reg_number": coalesce("ind_number", "org_number"),
            })
            .withColumn("reg_number", first("reg_number", ignorenulls=True).over(w_for_reg_number))
            .withColumn("row_number", row_number().over(w_for_row_number))
            .withColumn("end_date", last("data_date").over(w_for_end_date))
            .filter("row_number = 1")
            .withColumnRenamed("data_date", "start_date")
            .select(*cols_to_select)
            .cache()
        )

        count_after = table.count()
        print(f"Aggregated table contains {count_after} rows")

        self._write(table, out_file)

    def _process_revexp_data(self, in_dir: str, out_file: str,
                             smb_data_file: Optional[str]):
        """Combine revexp CSV files into a single file filtering by TINs"""
        data = self._read(in_dir, revexp_schema, dateFormat=self.INPUT_DATE_FORMAT)
        if data is None:
            return

        window = Window.partitionBy("tin", "data_date").orderBy(desc("doc_date"))

        table = (
            data
            .withColumnRenamed("org_tin", "tin")
            .withColumn("tin", lpad("tin", 10, "0"))
        )

        if smb_data_file is not None:
            table = self._filter_by_tins(table, smb_data_file)

        table = (
            table
            .withColumn("row_number", row_number().over(window))
            .filter("row_number == 1")
            .select("tin", year("data_date").alias("year"), "revenue", "expenditure")
            .orderBy("tin", "year")
            .cache()
        )

        print(f"Revexp resulting table contains {table.count()} rows")

        self._write(table, out_file)

    def _process_empl_data(self, in_dir: str, out_file: str,
                           smb_data_file: Optional[str]):
        """Combine employees CSV files into a single file filtering by TINs"""
        data = self._read(in_dir, empl_schema, dateFormat=self.INPUT_DATE_FORMAT)
        if data is None:
            return

        window = Window.partitionBy("tin", "data_date").orderBy(desc("doc_date"))

        table = (
            data
            .withColumnRenamed("org_tin", "tin")
            .withColumn("tin", lpad("tin", 10, "0"))
        )

        if smb_data_file is not None:
            table = self._filter_by_tins(table, smb_data_file)

        table = (
            table
            .withColumn("row_number", row_number().over(window))
            .filter("row_number = 1")
            .select("tin", year("data_date").alias("year"), "employees_count")
            .orderBy("tin", "year")
            .cache()
        )

        print(f"Revexp resulting table contains {table.count()} rows")

        self._write(table, out_file)
