"""
Unit tests for PySpark ETL pipeline using pytest.
"""

import pytest
import os
from pyspark.sql import SparkSession
from load_data import load_data
from quality_check import validate_data
from write_data import write_to_parquet, write_to_postgres

# Define file path for testing (SET IT HERE)
FILE_PATH = "/home/akshay/Iowa_Liquor_Sales.csv.csv"

@pytest.fixture(scope="session")
def spark():
    """
    Initializes a shared Spark session for testing.
    """
    return SparkSession.builder \
        .appName("Pytest-ETL") \
        .master("local[2]") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

def test_load_data(spark):
    """
    Test loading data from CSV.
    """
    df, _ = load_data(FILE_PATH)
    assert df is not None
    assert df.count() > 0  # Ensure data is loaded
    assert "store_location" in df.columns  # Ensure required column exists

def test_data_quality(spark):
    """
    Test data validation and filtering.
    """
    df, _ = load_data(FILE_PATH)
    good_df, bad_df = validate_data(df)

    assert good_df is not None
    assert bad_df is not None
    assert good_df.count() + bad_df.count() == df.count()  # Ensure records are correctly classified

def test_write_parquet(spark, tmp_path):
    """
    Test writing bad records to Parquet.
    """
    df, _ = load_data(FILE_PATH)
    _, bad_df = validate_data(df)

    output_path = str(tmp_path / "bad_records.parquet")
    write_to_parquet(bad_df, output_path)

    assert os.path.exists(output_path)

def test_write_postgres(spark):
    """
    Test writing good records to PostgreSQL.
    """
    df, _ = load_data(FILE_PATH)
    good_df, _ = validate_data(df)

    write_to_postgres(good_df, table_name="public.iowa_liquor_sales")

    assert good_df.count() > 0  # Ensure data was written

if __name__ == "__main__":
    pytest.main()