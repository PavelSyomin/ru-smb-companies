import pathlib
import shutil
import tempfile
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (coalesce, desc, first, last, lead,
    lower, lpad, row_number, upper, year)

from ..utils.spark_schemas import smb_schema, revexp_schema, empl_schema


def get_input_files(in_dir: str) -> List[str]:
    path = pathlib.Path(in_dir)
    if not path.exists():
        print(f"Input path {in_dir} not found")
        return []

    csv_files = [str(fn) for fn in path.glob("data-*.csv")]

    return csv_files


def _write(df: DataFrame, out_file: str):
    """Save Spark dataframe into a single CSV file"""
    with tempfile.TemporaryDirectory() as out_dir:
        options = dict(header=True, nullValue="NA", escape='"')
        df.coalesce(1).write.options(**options).csv(out_dir, mode="overwrite")

        # Spark writes to a folder with an arbitrary filename,
        # so we need to find and move the resulting file to the destination
        result = next(pathlib.Path(out_dir).glob("*.csv"), None)
        if result is None:
            print("Failed to save file")

        pathlib.Path(out_file).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(result, out_file)


class Aggregator:
    def __init__(self):
        self._session = None

        self._init_spark()

    def __del__(self):
        self._session.stop()

    def __call__(self, in_dir: str, out_file: str,
                 mode: str, smb_data_file: Optional[str] = None):
        """Execute the aggregation of all datasets"""
        input_files = get_input_files(in_dir)
        if len(input_files) == 0:
            print("Input directory does not contain extracted CSV files")
            return

        if mode == "smb":
            self._process_smb_registry(input_files, out_file)
        elif mode == "revexp":
            self._process_revexp_data(input_files, out_file, smb_data_file)
        elif mode == "empl":
            self._process_empl_data(input_files, out_file, smb_data_file)
        else:
            raise RuntimeError(
                f"Unsupported mode {mode}, expected one of {self.MODES}")

    def _init_spark(self):
        """Spark configuration and initialization"""
        print("Starting Spark")
        self._session = (
            SparkSession
            .builder
            .master("local")
            .appName("Data Aggregator")
            .getOrCreate()
        )

        web_url = self._session.sparkContext.uiWebUrl
        print(f"Spark session have started. You can monitor it at {web_url}")

    def _process_smb_registry(self, input_files: List[str], out_file: str):
        """Process CSV files extacted from SMB registry archives"""
        data = self._session.read.options(
            header=True, dateFormat="dd.MM.yyyy", escape='"'
        ).schema(smb_schema).csv(input_files)

        initial_count = data.count()
        print(f"Source CSV files contain {initial_count} rows")

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

        _write(table, out_file)

    def _process_revexp_data(self, input_files: List[str], out_file: str,
                             smb_data_file: Optional[str]):
        """Combine revexp CSV files into a single file filtering by TINs"""
        data = self._session.read.options(
            header=True, dateFormat="dd.MM.yyyy"
        ).schema(revexp_schema).csv(input_files)

        print(f"Revexp source CSV files contain {data.count()} rows")

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

        _write(table, out_file)

    def _process_empl_data(self, input_files: List[str], out_file: str,
                           smb_data_file: Optional[str]):
        """Combine employees CSV files into a single file filtering by TINs"""
        data = self._session.read.options(
            header=True, dateFormat="dd.MM.yyyy"
        ).schema(empl_schema).csv(input_files)
        print(f"Employees source CSV files contain {data.count()} rows")

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

        _write(table, out_file)

    def _filter_by_tins(self, table: DataFrame, smb_data_file: str) -> DataFrame:
        smb_data = self._session.read.options(header=True, escape='"').csv(smb_data_file)
        tins = smb_data.filter("kind == 1").select("tin")

        table = table.join(tins, on="tin", how="leftsemi")

        return table
