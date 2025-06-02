import findspark

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from src.main.utility.logging_config import *
import pyspark


def spark_session():
    """
    Create and return a SparkSession with custom configuration for local development and Kafka integration.
    Returns:
        SparkSession: Configured Spark session object.
    Raises:
        Exception: If Spark session creation fails.
    """
    try:
        spark = SparkSession.builder.master("local[*]") \
            .appName("shash_spark") \
            .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
            .getOrCreate()
        logger.info("spark session %s", spark)
    except Exception as e:
        logger.error("Error creating spark session: %s", e)
        raise
    return spark
