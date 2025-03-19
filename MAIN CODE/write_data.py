"""
Module to write good records into PostgreSQL and bad records into a Parquet file.
"""

import logging
from pyspark.sql import DataFrame
from config import DB_URL, DB_PROPERTIES

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def write_to_parquet(input_df: DataFrame, output_path: str):
    """
    Writes a PySpark DataFrame containing bad records to a Parquet file.

    :param input_df: PySpark DataFrame containing bad records.
    :param output_path: File path to store the Parquet file.
    """
    try:
        if not input_df.isEmpty():
            record_count = input_df.count()
            logging.info("Writing %s records to parquet file at %s", record_count, output_path)
            input_df.write.mode("overwrite").parquet(output_path)
            logging.info("Bad records successfully written to: %s", output_path)
        else:
            logging.info("No bad records found, skipping Parquet write.")

    except Exception as error:
        logging.error("Failed to write bad records to Parquet: %s", str(error))
        raise

def write_to_postgres(input_df: DataFrame, table_name: str):
    """
    Writes a PySpark DataFrame to PostgreSQL.

    :param input_df: PySpark DataFrame containing good records.
    :param table_name: Name of the PostgreSQL table.
    """
    try:
        logging.info("Good Records Schema: %s", input_df.schema)
        record_count = input_df.count()
        logging.info("Good Records Count: %s", record_count)
        logging.info("Checking if table %s exists in PostgreSQL.", table_name)
        logging.info("Writing %s records to PostgreSQL table: %s", record_count, table_name)

        input_df.write \
            .jdbc(url=DB_URL, table=table_name, mode="overwrite", properties=DB_PROPERTIES)
        logging.info("Good records successfully written to PostgreSQL!")

    except Exception as error:
        logging.error("Failed to write data to PostgreSQL: %s", str(error))
        raise

if __name__ == "__main__":
    from load_data import load_data
    from quality_check import validate_data

    FILE_PATH = "/home/akshay/Iowa_Liquor_Sales.csv"
    PARQUET_OUTPUT_PATH = "/home/akshay/bad_records.parquet"
    POSTGRES_TABLE_NAME = "public.iowa_liquor_sales"

    # Load data and retrieve the SparkSession
    data_df, spark_session = load_data(FILE_PATH)

    # Validate and separate records
    good_records_df, bad_records_df = validate_data(data_df)

    # Write bad records to Parquet file
    if bad_records_df:
        write_to_parquet(bad_records_df, output_path=PARQUET_OUTPUT_PATH)

    # Write good records to PostgreSQL
    write_to_postgres(good_records_df, table_name=POSTGRES_TABLE_NAME)
