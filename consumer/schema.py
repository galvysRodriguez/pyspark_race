from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

race_event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("bib_number", IntegerType(), False),
    StructField("runner_name", StringType(), False),
    StructField("age_group", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("checkpoint_id", StringType(), False),
    StructField("distance_km", DoubleType(), False),
    StructField("timestamp", TimestampType(), False)
])