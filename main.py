"""
Main ETL script to load, validate, and store data using write_data.py.
"""

import logging
from load_data import load_data
from quality_check import validate_data
from write_data import write_to_parquet, write_to_postgres

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

if __name__ == "__main__":
    try:
        # Define file paths
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

        logging.info("ETL pipeline completed successfully!")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {str(e)}")
