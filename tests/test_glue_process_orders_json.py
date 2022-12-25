import contextlib
import os
import boto3
import signal
import subprocess
from src.glue_process_orders_json import transform
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

SOURCE_NAME = "orders_1_2022-11-20T19-27-27.json"
TABLE_NAME = "orders"
S3_BUCKET_NAME = "data-s3"
ENDPOINT_URL = "http://127.0.0.1:5000/"

@contextlib.contextmanager
def initialize_environment(spark: SparkSession):
    """
    Context Manager to setup and initialize test case execution
    Also cleans up environment after execution

    Args:
        spark (SparkSession): PySpark session object

    Returns:
        process: Process object for the moto server that was started
        s3: S3 bucket created within moto server
    """
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "dummy-value")
    hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
    hadoop_conf.set("fs.s3a.endpoint", ENDPOINT_URL)
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    process = subprocess.Popen(
        "moto_server s3 -p5000",
        stdout=subprocess.PIPE,
        shell=True,
        preexec_fn=os.setsid,
    )

    s3 = boto3.resource(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="FakeKey",
        aws_secret_access_key="FakeSecretKey",
        aws_session_token="FakeSessionToken",
        region_name="us-east-1",
    )
    s3.create_bucket(
        Bucket=S3_BUCKET_NAME,
    )

    #values = [
    #    ("sam", "1962-05-25"),
    #    ("let", "1999-05-21"),
    #    ("nick", "1996-04-03"),
    #]
    #columns = ["name", "dt"]
    #df = spark.createDataFrame(values, columns)
    #df.write.parquet(f"s3a://{S3_BUCKET_NAME}/{SOURCE_NAME}")

    # insert test data file in S3 bucket
    filename = f"tests/data/{SOURCE_NAME}"
    bucket = s3.Bucket(S3_BUCKET_NAME)
    key=SOURCE_NAME
    bucket.upload_file(Filename=filename, Key=key)

    try:
        yield process, s3
    finally:
        #clean up AWS resources
        bucket = s3.Bucket(S3_BUCKET_NAME)
        bucket.objects.all().delete()
        bucket.delete()

        # shut down moto_server
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)

def compare_schema(schema_a: StructType, schema_b: StructType) -> bool:
    """
    Utility menthod to comapre two schema and return the results of comparison

    Args:
        schema_a (StructType): Schema for comparison
        schema_b (StructType): Schema for comparison

    Returns:
        bool: Result of schema comparison
    """
    return len(schema_a) == len(schema_b) and all(
        (a.name, a.dataType) == (b.name, b.dataType)
        for a, b in zip(schema_a, schema_b)
    )

# Test to verify data transformation
def test_transform_output_schema(glueContext: GlueContext):
    """
    Test case to test the transform function

    Args:
        glueContext (GlueContext): Test Glue context object
    """
    spark = glueContext.spark_session

    output_schema = StructType([
        StructField("first_name", StringType(), nullable=False),
        StructField("last_name", StringType(), nullable=False),
        StructField("address", StringType(), nullable=False),
        StructField("email", StringType(), nullable=False),
        StructField("user_agent", StringType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("amount", LongType(), nullable=False),
        StructField("order_position", LongType(), nullable=False),
        StructField("year", IntegerType(), nullable=False),
        StructField("month", IntegerType(), nullable=False),
        StructField("day", IntegerType(), nullable=False),
        StructField("time", StringType(), nullable=False)
    ])
    print(output_schema)    

    input_data = spark.createDataFrame(
        [("Kathryn",
        "Cook",
        "46947 Rebecca Glen Suite 265\nTomside, AK 21002",
        "ianderson@example.org",
        "Mozilla/5.0 (Windows; U; Windows 95) AppleWebKit/534.37.4 (KHTML, like Gecko) Version/5.0 Safari/534.37.4",
        "USD",
        "6d86cd7d-0a2f-4557-9a8a-7af979227e2c",
        "Apple Tea",
        1.89,
        "Tea",
        5,
        1,
        2050,
        6,
        5,
        "18-44-56")],
        ["first_name", "last_name", "address", "email", "user_agent", 
        "currency", "order_id", 
        "product_name", "price", "category", "amount", "order_position",
        "year", "month", "day", "time"]
    )
    real_output = transform(spark, input_data, SOURCE_NAME)
    real_output.printSchema()
    real_output.show()

    assert compare_schema(real_output.schema, output_schema)

# Test to verify data present in valid partioned format
def test_process_data_partitioning(glueContext: GlueContext):
    """
    Test case to test the process_data function for
    valid partitioned data output

    Args:
        glueContext (GlueContext): Test Glue context object
    """
    spark = glueContext.spark_session

    with initialize_environment(spark) as (process, s3):
        # Test code goes here
        # ...
        # process, s3 = initialize_test(spark)

        from src.glue_process_orders_json import process_data

        process_data(spark, SOURCE_NAME, TABLE_NAME)
        df = spark.read.parquet(
            f"s3a://{S3_BUCKET_NAME}/{TABLE_NAME}/year=2022/month=11/day=20"
        )
        assert isinstance(df, DataFrame)

# Test to verify number of records
def test_process_data_record_count(glueContext: GlueContext):
    """
    Test case to test the process_data function for
    number of records in input and output

    Args:
        glueContext (GlueContext): Test Glue context object
    """
    spark = glueContext.spark_session

    with initialize_environment(spark) as (process, s3):
        # Test code goes here
        # ...
        # process, s3 = initialize_test(spark)

        from src.glue_process_orders_json import process_data

        process_data(spark, SOURCE_NAME, TABLE_NAME)

        df = spark.read.parquet(f"s3a://{S3_BUCKET_NAME}/{TABLE_NAME}")

        assert df.count() == 86
