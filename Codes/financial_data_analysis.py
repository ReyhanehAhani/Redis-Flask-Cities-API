from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window, sum as sum_, max as max_, min as min_, count, lit

spark = SparkSession.builder \
    .appName("Financial Data Analysis with PayType Status") \
    .getOrCreate()

data_source_path = "/Users/reyhanehahani/Desktop/REF_CBS_SMS2.csv"
reference_table_path = "/Users/reyhanehahani/Desktop/Ref.csv"
data_source_df = spark.read.csv(data_source_path, header=True, inferSchema=True)
reference_table_df = spark.read.csv(reference_table_path, header=True, inferSchema=True)

# Convert DEBIT_AMOUNT_42 from Rials to Tomans and parse RECORD_DATE to a timestamp object
data_source_df = data_source_df.withColumn("revenue", col("DEBIT_AMOUNT_42") / 10) \
    .withColumn("timestamp", to_timestamp(col("RECORD_DATE"), "yyyy/MM/dd HH:mm:ss"))

joined_df = data_source_df.join(reference_table_df, data_source_df["PAYTYPE_515"] == reference_table_df["PayType"], "left") \
    .select(data_source_df["*"], reference_table_df["value"].alias("pay_type_name"))

# Calculating total revenue, maximum revenue, minimum revenue and the number of records at 15-minute intervals
interval_stats_df = joined_df.groupBy(window(col("timestamp"), "15 minutes"), "pay_type_name") \
    .agg(
        sum_("revenue").alias("total_revenue"),
        max_("revenue").alias("max_revenue"),
        min_("revenue").alias("min_revenue"),
        count("*").alias("record_count")
    )

final_result_df = interval_stats_df.select(
    col("window").start.alias("start_interval"),
    "pay_type_name",
    "total_revenue",
    "max_revenue",
    "min_revenue",
    "record_count"
)

final_result_df.show(truncate=False)

final_result_path = "/Users/reyhanehahani/Desktop/final_interval_stats" 
final_result_df.coalesce(1).write.option("header", "true").csv(final_result_path)

spark.stop()
