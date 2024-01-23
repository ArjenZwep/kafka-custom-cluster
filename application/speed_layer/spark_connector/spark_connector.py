from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, from_avro, udf, current_timestamp, avg
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.streaming import Trigger

# Assuming you have the necessary Python dependencies installed

# Loading configuration
# (Note: Make sure to replace the actual configurations with appropriate values)
settings = {
    "spark": {
        "master": "local[*]",
        "appName": "StreamProcessor",
        "shuffle_partitions": "10",  # Adjust as needed
        "max_offsets_per_trigger": "1000",
        "deprecated_offsets": "false"
    },
    "kafka": {
        "server_address": "localhost:9092",
        "topic_market": "trades",
        "min_partitions": "1"
    },
    "cassandra": {
        "host": "your_cassandra_host",
        "username": "your_cassandra_username",
        "password": "your_cassandra_password",
        "keyspace": "your_keyspace",
        "trades": "your_trades_table",
        "aggregates": "your_aggregates_table"
    },
    "schemas": {
        "trades": "path_to_your_trades_schema_file"
    }
}

# UDF for generating Cassandra UUIDs
make_uuid_udf = udf(lambda: Uuids.timeBased().toString(), StringType())

# Create Spark session
spark = SparkSession.builder \
    .master(settings["spark"]["master"]) \
    .appName(settings["spark"]["appName"]) \
    .config("spark.cassandra.connection.host", settings["cassandra"]["host"]) \
    .config("spark.cassandra.auth.username", settings["cassandra"]["username"]) \
    .config("spark.cassandra.auth.password", settings["cassandra"]["password"]) \
    .config("spark.sql.shuffle.partitions", settings["spark"]["shuffle_partitions"]) \
    .getOrCreate()

# Read streams from Kafka
input_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", settings["kafka"]["server_address"]) \
    .option("subscribe", settings["kafka"]["topic_market"]) \
    .option("minPartitions", settings["kafka"]["min_partitions"]) \
    .option("maxOffsetsPerTrigger", settings["spark"]["max_offsets_per_trigger"]) \
    .option("useDeprecatedOffsetFetching", settings["spark"]["deprecated_offsets"]) \
    .load()

# Explode the data from Avro
expanded_df = input_df \
    .withColumn("avroData", from_avro(col("value"), settings["schemas"]["trades"])) \
    .select(col("avroData.data").alias("data"), col("avroData.type").alias("type"))

# Rename columns and add proper timestamps
final_df = expanded_df \
    .withColumn("uuid", make_uuid_udf()) \
    .withColumnRenamed("data.c", "trade_conditions") \
    .withColumnRenamed("data.p", "price") \
    .withColumnRenamed("data.s", "symbol") \
    .withColumnRenamed("data.t", "trade_timestamp") \
    .withColumnRenamed("data.v", "volume") \
    .withColumn("trade_timestamp", (col("trade_timestamp") / 1000).cast(TimestampType())) \
    .withColumn("ingest_timestamp", current_timestamp())

# Write query to Cassandra for trades
query = final_df \
    .writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
                  .cassandraFormat(settings["cassandra"]["trades"], settings["cassandra"]["keyspace"]) \
                  .mode("append") \
                  .save()) \
    .outputMode("update") \
    .start()

# Another dataframe with aggregates (running averages from the last 15 seconds)
summary_df = final_df \
    .withColumn("price_volume_multiply", col("price") * col("volume")) \
    .withWatermark("trade_timestamp", "15 seconds") \
    .groupBy("symbol") \
    .agg(avg("price_volume_multiply").alias("price_volume_multiply"))

# Rename columns in dataframe and add UUIDs before inserting to Cassandra
final_summary_df = summary_df \
    .withColumn("uuid", make_uuid_udf()) \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .withColumnRenamed("price_volume_multiply", "price_volume_multiply")

# Write second query to Cassandra for aggregates
query2 = final_summary_df \
    .writeStream \
    .trigger(Trigger.ProcessingTime("5 seconds")) \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
                  .cassandraFormat(settings["cassandra"]["aggregates"], settings["cassandra"]["keyspace"]) \
                  .mode("append") \
                  .save()) \
    .outputMode("update") \
    .start()

# Let queries await termination
spark.streams.awaitAnyTermination()
