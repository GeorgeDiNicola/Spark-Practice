from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, DataFrame


INPUT_PATH = "/data/Combined_Flights_*.parquet"
OUTPUT_DIR = Path("/output")
TEMP_OUTPUT_PATH = OUTPUT_DIR / "_flight_summaries_spark_output"
FINAL_OUTPUT_PATH = OUTPUT_DIR / "flight_summaries.csv"


def rename_spark_csv_output() -> None:
    csv_files = list(TEMP_OUTPUT_PATH.glob("part-*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No Spark CSV part file found in {TEMP_OUTPUT_PATH}")

    FINAL_OUTPUT_PATH.unlink(missing_ok=True)
    shutil.move(str(csv_files[0]), FINAL_OUTPUT_PATH)
    shutil.rmtree(TEMP_OUTPUT_PATH)


def create_summary_report(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("Airline")
        .agg(
            F.count("*").alias("total_flights"),
            F.sum(F.col("Cancelled").cast("int")).alias("cancelled_flights"),
            F.sum(F.col("Diverted").cast("int")).alias("diverted_flights"),
            F.avg("DepDelayMinutes").alias("avg_departure_delay_minutes"),
            F.avg("ArrDelayMinutes").alias("avg_arrival_delay_minutes"),
            F.sum("DepDelayMinutes").alias("total_departure_delay_minutes"),
            F.sum("ArrDelayMinutes").alias("total_arrival_delay_minutes"),
            F.avg("Distance").alias("avg_distance"),
        )
        .withColumn(
            "cancellation_rate",
            F.round(F.col("cancelled_flights") / F.col("total_flights"), 4),
        )
        .withColumn(
            "diversion_rate",
            F.round(F.col("diverted_flights") / F.col("total_flights"), 4),
        )
        .orderBy("Airline")
    )


def main() -> None:
    spark = SparkSession.builder.appName("FlightAggregation").getOrCreate()

    flights = spark.read.parquet(INPUT_PATH)

    airline_summary = create_summary_report(flights)

    airline_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(TEMP_OUTPUT_PATH))
    rename_spark_csv_output()

    airline_summary.show(20, truncate=False)
    print(f"Output written to {FINAL_OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
