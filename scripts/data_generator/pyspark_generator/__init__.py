from deltalake import DeltaTable, write_deltalake
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *
import duckdb
import pandas as pd
import os
import shutil
import math
import glob

def generate_test_data_pyspark(base_path, name, current_path, input_path, delete_predicate = False, partition_column = None, mapping_mode = None):
    """
    generate_test_data_pyspark generates some test data using pyspark and duckdb

    :param current_path: the test data path
    :param input_path: the path to an input parquet file
    :return: describe what it returns
    """

    full_path = base_path + '/' + current_path
    if (os.path.isdir(full_path)):
        return

    try:
        ## SPARK SESSION
        builder = SparkSession.builder.appName("MyApp") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.driver.memory", "8g") \
            .config('spark.driver.host','127.0.0.1')

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        ## CONFIG
        delta_table_path = base_path + '/' + current_path + '/delta_lake'
        parquet_reference_path = base_path + '/' + current_path + '/parquet'

        ## CREATE DIRS
        os.makedirs(delta_table_path, exist_ok=True)
        os.makedirs(parquet_reference_path, exist_ok=True)

        ## DATA GENERATION
        # df = spark.read.parquet(input_path)
        # df.write.format("delta").mode("overwrite").save(delta_table_path)
        if (partition_column):
            spark.sql(f"CREATE TABLE test_table_{name} USING delta PARTITIONED BY ({partition_column}) LOCATION '{delta_table_path}' AS SELECT * FROM parquet.`{input_path}`")
        else:
            spark.sql(f"CREATE TABLE test_table_{name} USING delta LOCATION '{delta_table_path}' AS SELECT * FROM parquet.`{input_path}`")

        if mapping_mode == 'name' or mapping_mode == 'id':
            spark.sql(f"ALTER TABLE test_table_{name} SET TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7', 'delta.columnMapping.mode' = '{mapping_mode}');")
        elif mapping_mode is None:
            spark.sql(f"ALTER TABLE test_table_{name} SET TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7');")
        else:
            raise f"Unknown mapping mode: {mapping_mode}"

        ## CREATE
        ## CONFIGURE USAGE OF DELETION VECTORS
        if (delete_predicate):
            spark.sql(f"ALTER TABLE test_table_{name} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);")

        ## ADDING DELETES
        deltaTable = DeltaTable.forPath(spark, delta_table_path)
        if delete_predicate:
            deltaTable.delete(delete_predicate)

        ## WRITING THE PARQUET FILES
        df = spark.table(f'test_table_{name}')
        df.write.parquet(parquet_reference_path, mode='overwrite')

    except:
        if (os.path.isdir(full_path)):
            shutil.rmtree(full_path)
        raise

def generate_test_data_pyspark_by_queries(base_path, name, current_path, base_query, queries, mapping_mode = None):
    """
    schema_evolve_pyspark_deltatable generates some test data using pyspark and duckdb

    :param current_path: the test data path
    :param input_path: the path to an input parquet file
    :return: describe what it returns
    """

    full_path = base_path + '/' + current_path
    if (os.path.isdir(full_path)):
        return

    try:
        ## SPARK SESSION
        builder = SparkSession.builder.appName("MyApp") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.driver.memory", "8g") \
            .config('spark.driver.host','127.0.0.1')

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        ## CONFIG
        delta_table_path = full_path + '/delta_lake'

        ## CREATE DIRS
        os.makedirs(delta_table_path, exist_ok=True)

        ## DATA GENERATION
        # df = spark.read.parquet(input_path)
        # df.write.format("delta").mode("overwrite").save(delta_table_path)

        if mapping_mode == 'name' or mapping_mode == 'id':
            spark.sql(
                f"CREATE TABLE {name} USING delta TBLPROPERTIES ('delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5', 'delta.columnMapping.mode' = '{mapping_mode}', 'delta.enableTypeWidening' = 'true') LOCATION '{delta_table_path}' AS {base_query};")
        elif mapping_mode is None:
            spark.sql(f"CREATE TABLE {name} USING delta TBLPROPERTIES ('delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5', 'delta.enableTypeWidening' = 'true') LOCATION '{delta_table_path}' AS {base_query};")
        else:
            raise f"Unknown mapping mode: {mapping_mode}"

        for query in queries:
            spark.sql(query)

    except:
        if (os.path.isdir(full_path)):
            shutil.rmtree(full_path)
        raise


__all__ = ["generate_test_data_pyspark_by_queries", "generate_test_data_pyspark"]