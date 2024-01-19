from pyspark.sql.types import (StructField, StructType, ByteType, DateType,
   FloatType, IntegerType, ShortType, StringType)


smb_schema = StructType([
    StructField("kind", ByteType(), False),
    StructField("category", ByteType(), False),
    StructField("reestr_date", DateType(), False),
    StructField("data_date", DateType(), False),
    StructField("ind_tin", StringType(), True),
    StructField("ind_number", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("patronymic", StringType(), True),
    StructField("org_name", StringType(), True),
    StructField("org_short_name", StringType(), True),
    StructField("org_tin", StringType(), True),
    StructField("org_number", StringType(), True),
    StructField("region_code", ByteType(), True),
    StructField("region_name", StringType(), True),
    StructField("region_type", StringType(), True),
    StructField("district_name", StringType(), True),
    StructField("district_type", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("city_type", StringType(), True),
    StructField("settlement_name", StringType(), True),
    StructField("settlement_type", StringType(), True),
    StructField("activity_code_main", StringType(), False),
    StructField("file_id", StringType(), True),
    StructField("doc_cnt", ShortType(), True),
])

revexp_schema = StructType([
    StructField("org_tin", StringType(), False),
    StructField("revenue", FloatType(), True),
    StructField("expenditure", FloatType(), True),
    StructField("data_date", DateType(), True),
    StructField("doc_date", DateType(), True),
])

empl_schema = StructType([
    StructField("org_tin", StringType(), False),
    StructField("employees_count", IntegerType(), True),
    StructField("data_date", DateType(), True),
    StructField("doc_date", DateType(), True),
])
