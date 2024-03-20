from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, when

# Create SparkSession
spark = SparkSession.builder \
    .appName("Assignment Part II") \
    .getOrCreate()

# Create DataFrame using a query directly
titles_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("user", "worker") \
    .option("password", "cluster") \
    .option("query", "SELECT * FROM titles WHERE title = 'Senior Engineer'") \
    .load()

# Add employment status column
titles_df = titles_df.withColumn("employment_status", 
                                 when(titles_df.to_date == "9999-01-01", "current")
                                 .otherwise("left"))

# Count senior engineers by employment status
counts = titles_df.groupBy("employment_status").count().show()

# Filter for Senior Engineers who have left
left_senior_engineers_df = titles_df.filter(titles_df.employment_status == "left")

# Write DataFrame to database with mode type errorifexists
left_senior_engineers_df.write.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "left_table") \
    .option("user", "worker") \
    .option("password", "cluster") \
    .mode("errorifexists") \
    .save()

# Create or replace temp view
left_senior_engineers_df.createOrReplaceTempView("left_tempview")

# Stop SparkSession
spark.stop()
