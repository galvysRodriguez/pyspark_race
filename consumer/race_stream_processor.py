from config.spark_config import get_spark_session
from consumer.schema import race_event_schema
from pyspark.sql.functions import from_json, col, max as _max, min as _min, round as _round, unix_timestamp

def main():
    spark = get_spark_session("10kRaceTracker")
    spark.sparkContext.setLogLevel("WARN")

    # Ingest from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "race_events") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON Payload
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), race_event_schema).alias("data")) \
        .select("data.*")

    # Stateful Event-Time Metrics
    leaderboard = parsed_stream \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy("bib_number", "runner_name") \
    .agg(
        _max("distance_km").alias("current_distance_km"),
        _min("timestamp").alias("start_time"),
        _max("timestamp").alias("last_checkpoint_time")
    ) \
    .withColumn(
        "elapsed_minutes", 
        _round((unix_timestamp("last_checkpoint_time") - unix_timestamp("start_time")) / 60, 2)
    ) \
    .withColumn(
        "avg_pace_min_per_km", 
        _round(col("elapsed_minutes") / col("current_distance_km"), 2)
    )

    # Write Stream to Delta Lake
    query = leaderboard.writeStream \
        .format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", "./checkpoints/race_leaderboard") \
        .option("path", "./storage/delta/race_leaderboard") \
        .trigger(processingTime="5 seconds") \
        .start()

    print("🚀 Race tracking stream processor active...")
    query.awaitTermination()

if __name__ == "__main__":
    main()