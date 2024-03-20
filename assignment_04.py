from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Assignment 04") \
    .getOrCreate()

employees_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "employees") \
    .option("user", "worker") \
    .option("password", "cluster") \
    .load()

print("Number of records in the DataFrame:", employees_df.count())

employees_df.printSchema()

salaries_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "(SELECT * FROM salaries ORDER BY salary DESC LIMIT 10000) as temp") \
    .option("user", "worker") \
    .option("password", "cluster") \
    .load()


# Drop the existing table
spark.sql("DROP TABLE IF EXISTS aces")

salaries_df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "aces") \
    .option("user", "worker") \
    .option("password", "cluster") \
    .save()


salaries_df.write.csv("salaries.csv", compression="snappy")
