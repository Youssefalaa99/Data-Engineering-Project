import json, logging
from config.settings import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType



def create_spark_conn():
    try:
        spark = SparkSession.builder \
                        .appName("Kafka streaming to MongoDB") \
                        .master("local[*]") \
                        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
                                                    "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0,"
                                                    "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.2") \
                        .getOrCreate()
        spark.sparkContext.setLogLevel("INFO")
    except Exception as e:
        logging.error(f"Error while creating spark session: {e}")
    return spark
    

def read_from_kafka(spark):
    spark_df = None
    try:
        spark_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Kafka streaming dataframe created successfully")
    except Exception as e:
        logging.error(f"Kafka dataframe could not be created: {e}")
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


# Write to MongoDB
def write_to_mongo(df):
    try:
        query = df.writeStream \
            .format("mongodb") \
            .option("spark.mongodb.connection.uri", MONGO_URI) \
            .option("spark.mongodb.database", MONGO_DATABASE) \
            .option("spark.mongodb.collection", MONGO_COLLECTION) \
            .option("checkpointLocation", "/tmp/kafka-mongo-checkpoint") \
            .outputMode("append") \
            .start()

        return query
    except Exception as e:
        logging.error(f"Error while writing to mongo: {e}")
        exit(1)

# Write to Bigquery
def write_to_bigquery(df):
    try:
        query = df.writeStream \
            .foreachBatch(write_to_bigquery_batch) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/kafka-bigquery-checkpoint") \
            .start()
        
        return query
    except Exception as e:
        logging.error(f"Error while writing to bigquery: {e}")


# For each batch biquery write
def write_to_bigquery_batch(batch_df, batch_id):
    batch_df.write \
        .format("bigquery") \
        .option("table", f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLENAME}") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()


# Used for printing
def write_to_console(df):
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
        # write_to_console(transformed_df)
        mongo_query = write_to_mongo(transformed_df)
        bq_query = write_to_bigquery(transformed_df)
        spark.streams.awaitAnyTermination()
