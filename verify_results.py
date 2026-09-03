from config.spark_config import get_spark_session
from pyspark.sql.functions import col

def inspect_leaderboard():
    spark = get_spark_session("RaceResultsVerifier")
    spark.sparkContext.setLogLevel("ERROR")

    print("\n🔍 Querying Delta Lake Leaderboard Table...\n")

    # Read from local Delta storage
    df = spark.read.format("delta").load("./storage/delta/race_leaderboard")

    print("📊 Top 10 Leaders (By Distance & Pace):")
    df.orderBy(col("current_distance_km").desc(), col("avg_pace_min_per_km").asc()) \
      .show(10, truncate=False)

    print("📈 Total Runners Tracked in Delta State:")
    print(f"Total: {df.count()} runners\n")

if __name__ == "__main__":
    inspect_leaderboard()