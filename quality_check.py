"""
Module to perform data quality checks using Great Expectations and push the bad records to a parquet file.
"""

import great_expectations as ge
import logging
from pyspark.sql import DataFrame
from load_data import load_data  

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def validate_data(df: DataFrame):
    """
    Performs data quality checks and returns separate DataFrames for good and bad records.

    :param df: Input PySpark DataFrame.
    :return: Individual DataFrames: good_records_df and bad_records_df.
    """
    try:
        df_ge = ge.dataset.SparkDFDataset(df)

        # Defining expectations
        expectations = {
            "check_store_location": ("store_location IS NOT NULL", df_ge.expect_column_values_to_not_be_null("store_location")),
            "check_unique_invoice": ("invoice_and_item_number IS NOT NULL", df_ge.expect_column_values_to_be_unique("invoice_and_item_number")),
            "check_date_format": ("date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'", df_ge.expect_column_values_to_match_regex("date", r"^\d{4}-\d{2}-\d{2}$")),
            "check_sale_dollars": ("sale_dollars >= 0", df_ge.expect_column_values_to_be_between("sale_dollars", 0, None)),
            "check_bottles_sold": ("bottles_sold >= 1", df_ge.expect_column_values_to_be_between("bottles_sold", 1, None)),
        }

        failed_conditions = []

        # Logging each expectation result
        for check_name, (condition, result) in expectations.items():
            if result["success"]:
                logging.info(f"{check_name} check PASSED.")
            else:
                logging.warning(f"{check_name} check FAILED.")
                failed_conditions.append(condition)

        # Separate good and bad records
        if failed_conditions:
            filter_condition = " OR ".join(f"NOT ({condition})" for condition in failed_conditions)
            bad_records_df = df.filter(filter_condition)
            good_records_df = df.subtract(bad_records_df)
            logging.info(f"Bad records found. Good: {good_records_df.count()} | Bad: {bad_records_df.count()}")
        else:
            good_records_df = df
            bad_records_df = None
            logging.info("All records passed data quality checks.")

        return good_records_df, bad_records_df

    except Exception as e:
        logging.error(f"Data validation failed: {str(e)}")
        raise

if __name__ == "__main__":
    file_path = "/home/akshay/Iowa_Liquor_Sales.csv.csv"
    
    #Correctly unpack the DataFrame and SparkSession
    df, spark = load_data(file_path)

    # Run validation
    good_records_df, bad_records_df = validate_data(df)

    # Show sample records
    good_records_df.show(5)
    
    if bad_records_df:
        bad_records_df.show(5)

        #Save bad records to a single Parquet file
        bad_records_df.coalesce(1).write.mode("overwrite").parquet("/home/akshay/bad_records.parquet")
        logging.info("Bad records successfully written to Parquet.")
        logging.info("Good records are ready to be pushed to PostgreSQL database")
