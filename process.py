import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pathlib import Path

# Load properties from .env file
load_dotenv()

# Access the properties from the .env file
INPUT_CSV_FILE = os.getenv("INPUT_CSV_FILE")
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")


def process():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("CSV to Parquet") \
        .getOrCreate()

    # Set input and output paths
    input_path = "assets/input/random_orders.csv"
    output_path = "assets/output/random_orders.parquet"

    # Read CSV file into DataFrame
    df = spark.read.csv(input_path, header=True)

    # Create a new timestamp column from the datetime column
    df = df.withColumn("timestamp", to_timestamp(col("datetime")))

    # Write DataFrame as Parquet files partitioned by customer_id and timestamp
    df.write.partitionBy("user_id", "timestamp") \
        .parquet(OUTPUT_FOLDER, mode="overwrite")

    # Stop SparkSession
    spark.stop()

    print(f"Parquet files have been created in the '{OUTPUT_FOLDER}' folder.")
