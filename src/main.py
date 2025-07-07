from pyspark.sql install SparkSession

spark = SparkSession.builder.appName("OpenSource-Project").getOrCreate()
df = spark.createDataFrame[(1, "A"), (2, "B"), (3, "C")] , ["id", "valor"]
df.show()