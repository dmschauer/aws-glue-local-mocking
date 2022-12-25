import sys
import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions


def transform(spark: SparkSession, df: DataFrame, source_path: str) -> DataFrame:
    """
    Function to extract and transform dataframe columns with date
    to get day, month and year taken from file name.

    Args:
        spark (SparkSession): PySpark session object
        df (DataFrame): Dataframe object containing the data before transform

    Returns:
        DataFrame: Dataframe object containing the data after transform
    """

    # Extract date and time from filename
    date_time_str = source_path.replace(".json","")
    date_time_str = date_time_str.rsplit('_')[-1]
    date_time_obj = datetime.datetime.strptime(date_time_str, "%Y-%m-%dT%H-%M-%S")

    # Add date and time columns to DataFrame
    df = df.withColumn("year", lit(date_time_obj.year))
    df = df.withColumn("month", lit(date_time_obj.month))
    df = df.withColumn("day", lit(date_time_obj.day))
    df = df.withColumn("time", lit(date_time_obj.strftime("%H:%M:%S")))

    # if you want to partition by product_name, avoid whitespace
    # df = df.withColumn("product_name", regexp_replace("product_name", "\\s", "_"))

    return df

def process_data(spark: SparkSession, source_path: str, table_name: str):
    """
    Function to read and process data from JSON file
    and store it as parquet

    Args:
        spark (SparkSession): PySpark session object
        source_path (String): Data file path
        table_name (String): Output Table name
    """
    # Read JSON file with defined schema
    schema = StructType([
        StructField("address", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("items", ArrayType(
            StructType([
                StructField("amount", LongType(), True),
                StructField("category", StringType(), True),
                StructField("order_position", LongType(), True),
                StructField("price", DoubleType(), True),
                StructField("product_name", StringType(), True)
        ]), True), True),
        StructField("last_name", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("user_agent", StringType(), True)
    ])
    df = spark.read.json(f"s3://data-s3/{source_path}", multiLine=True, schema=schema)
        
    # Turn Items Array into separate Item rows
    df = df.select("*", explode("items").alias("item")).drop("Items")

    # Turn each nested column in the Item struct into separate column
    df = df.select("*", col("Item.*")).drop("Item")

    df_transform = transform(spark, df, source_path)

    df_transform.write.mode("append").partitionBy(
        "year", "month", "day"
    ).parquet(f"s3a://data-s3/{table_name}")

if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "table_name", "source_path"]
    )
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    spark._jsc.haddopConfiguration().set(
        "fs.s3.useRequesterPaysHeader", "True"
    )

    process_data(spark, args["source_path"], args["table_name"])

    job.commit()
