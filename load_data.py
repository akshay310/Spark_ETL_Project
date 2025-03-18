"""
Module to load CSV data into a PySpark DataFrame.
"""

from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)

def get_spark_session(app_name="ETL-Load-Data"):
    """
    Initializes and returns a Spark session.
    """
    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.executor.memory", "4g") \  
            .config("spark.driver.memory", "4g") \  
            .config("spark.executor.memoryOverhead", "1g") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.5") \
            .config("spark.jars","/opt/spark/jars/postgresql-42.6.0.jar")
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
    Loads a CSV file into a PySpark DataFrame.

    :param file_path: Path to the CSV file
    :return: Tuple (DataFrame, Spark Session)
    """
    try:
        spark = get_spark_session()
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)

        logging.info("Data loaded successfully.")
        return df, spark  #Return both DataFrame and Spark session
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        raise

if __name__ == "__main__":
    file_path = "/home/akshay/Iowa_Liquor_Sales.csv.csv"
    df, spark = load_data(file_path)
    df.show(5)
