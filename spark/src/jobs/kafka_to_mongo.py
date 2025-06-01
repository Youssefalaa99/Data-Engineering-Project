import json, logging
from config.settings import KAFKA_BROKER, KAFKA_TOPIC, MONGO_URI, MONGO_DATABASE, MONGO_COLLECTION
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType



def create_spark_conn():
    try:
        spark = SparkSession.builder \
                        .appName("Kafka streaming to MongoDB") \
                        .master("local[*]") \
                        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4') \
                        .getOrCreate()
        spark.sparkContext.setLogLevel("INFO")
    except Exception as e:
        logging.error(f"Error while creating spark session: {e}")
    return spark
    

def read_from_kafka(spark):
    spark_df = None
    try:
        spark_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_BROKER) \
            .option('subscribe', KAFKA_TOPIC) \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka streaming dataframe created successfully")
    except Exception as e:
        print(f"Error kafka conn read due to: {e}")
        logging.warning(f"Kafka dataframe could not be created: {e}")
        exit(1)

    return spark_df

def transform_kafka_df(spark_df):
    schema = StructType([
        # StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", IntegerType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("registered_date", TimestampType(), False),
        StructField("phone", StringType(), False)
    ])

    spark_df = spark_df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col('value'),schema).alias('data')) \
                .select("data.*")

    return spark_df

def write_to_mongo(df):
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()


def start_job():
    spark = create_spark_conn()
    if spark is not None:
        spark_df = read_from_kafka(spark)
        transformed_df = transform_kafka_df(spark_df)
        write_to_mongo(transformed_df)
        
