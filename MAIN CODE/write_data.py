"""
Module to write good records into PostgreSQL and bad records into a Parquet file.
"""

import logging
from pyspark.sql import DataFrame
from config import DB_URL, DB_PROPERTIES

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def write_to_parquet(df: DataFrame, output_path: str):
    """
    Writes a PySpark DataFrame containing bad records to a Parquet file.

    :param df: PySpark DataFrame containing bad records.
    :param output_path: File path to store the Parquet file.
    """
    try:
        if df.count() > 0:
            logging.info(f"Writing {df.count()} records to parquet file at {output_path}")
            df.write.mode("overwrite").parquet(output_path)
            logging.info(f"Bad records successfully written to: {output_path}")
        else:
            logging.info("No bad records found, skipping Parquet write.")

    except Exception as e:
        logging.error(f"Failed to write bad records to Parquet: {str(e)}")
        raise

def write_to_postgres(df: DataFrame, table_name: str):
    """
    Writes a PySpark DataFrame to PostgreSQL.

    :param df: PySpark DataFrame containing good records.
    :param table_name: Name of the PostgreSQL table.
    """
    try:
        logging.info(f"Good Records Schema:\n{df.printSchema()}")
        logging.info(f"Good Records Count: {df.count()}")
        logging.info(f"Checking if table {table_name} exists in PostgreSQL.")
        logging.info(f"Writing {df.count()} records to PostgreSQL table: {table_name}")

        df.write \
            .jdbc(url=DB_URL, table=table_name, mode="overwrite", properties=DB_PROPERTIES)
        logging.info("Good records successfully written to PostgreSQL!")

    except Exception as e:
        logging.error(f"Failed to write data to PostgreSQL: {str(e)}")
        raise

if __name__ == "__main__":
    from load_data import load_data
    from quality_check import validate_data

    file_path = "/home/akshay/Iowa_Liquor_Sales.csv.csv"
    parquet_output_path = "/home/akshay/bad_records.parquet"
    postgres_table_name = "public.iowa_liquor_sales"

    # Load data and retrieve the SparkSession
    df, spark = load_data(file_path)

    # Validate and separate records
    good_records_df, bad_records_df = validate_data(df)
    # Write bad records to Parquet file
    if bad_records_df:
        write_to_parquet(bad_records_df, output_path=parquet_output_path)

    # Write good records to PostgreSQL
    write_to_postgres(good_records_df, table_name=postgres_table_name)

    
