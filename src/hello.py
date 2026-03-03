from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("🚀 Job Market MVP pipeline is alive!")

df = spark.range(5)
df.show()
df = df.withColumnRenamed("id", "job_id")
df.show()
