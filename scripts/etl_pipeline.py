from pyspark.sql import SparkSession
from pyspark.sql.functions import year, current_date, sum

# Create Spark session
spark = SparkSession.builder.appName("TRSProject").getOrCreate()

# Load CSV files
members = spark.read.csv("data/members.csv", header=True, inferSchema=True)

contributions = spark.read.csv(
    "data/contributions.csv",
    header=True,
    inferSchema=True
)

# Calculate years enrolled
members = members.withColumn(
    "years_enrolled",
    year(current_date()) - year("join_date")
)

# Aggregate contribution totals
agg_contributions = contributions.groupBy("member_id").agg(
    sum("contribution_amount").alias("total_contributions")
)

# Join datasets
final_df = members.join(
    agg_contributions,
    on="member_id",
    how="left"
)
final_df.write.mode("overwrite").parquet("output/member_summary")
# Display results
final_df.show()
