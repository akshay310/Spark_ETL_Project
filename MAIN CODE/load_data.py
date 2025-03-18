"""
Module to load CSV data into a PySpark DataFrame with automatic delimiter and encoding detection.
"""

import logging
import chardet
import csv
import os

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO) 


def detect_encoding_and_delimiter(file_path, sample_size=10000):
    """
    Detects the file encoding and delimiter.

    :param file_path: Path to the CSV file
    :param sample_size: Number of bytes to read for encoding detection
    :return: Tuple (encoding, delimiter)
    """
    try:
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            exit(1)

        # Detect encoding
        with open(file_path, "rb") as f:
            raw_data = f.read(sample_size)
            encoding_info = chardet.detect(raw_data)
            encoding = encoding_info.get("encoding", "utf-8")  # Default to UTF-8 if None

        # Detect delimiter
        with open(file_path, "r", encoding=encoding) as f:
            sample = f.readline()
            try:
                delimiter = csv.Sniffer().sniff(sample).delimiter
            except csv.Error:
                logging.warning("Could not auto-detect delimiter, defaulting to ','")
                delimiter = ","

        logging.info(f"Detected Encoding: {encoding}")
        logging.info(f"Detected Delimiter: {delimiter}")

        return encoding, delimiter
    except Exception as e:
        logging.error(f"Error detecting encoding or delimiter: {str(e)}")
        raise

def get_spark_session(app_name="ETL-Load-Data"):
    """
    Initializes and returns a Spark session.
    """
    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memoryOverhead", "1g")
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.5")
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")  # Suppress WARN messages

        logging.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {str(e)}")
        raise

def load_data(file_path):
    """
    Loads a CSV file into a PySpark DataFrame with detected encoding and delimiter.

    :param file_path: Path to the CSV file
    :return: Tuple (DataFrame, Spark Session)
    """
    try:
        encoding, delimiter = detect_encoding_and_delimiter(file_path)
        spark = get_spark_session()

        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", encoding) \
            .option("delimiter", delimiter) \
            .csv(file_path)
        
        logging.info(f'Encoding found : {encoding}')
        logging.info(f'Delimiter found : {delimiter}')
        logging.info("Data loaded successfully.")
        df.cache()
        logging.info(f"Row count: {df.count()}")  # Forces Spark to execute
        return df, spark  # Return both DataFrame and Spark session
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        raise

if __name__ == "__main__":
    file_path = "/home/akshay/Iowa_Liquor_Sales.csv.csv"

    # Debug file existence before processing
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        exit(1)

    df, spark = load_data(file_path)
    df.show(5)
