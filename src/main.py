from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MiProyecto").getOrCreate()
# df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "valor"])
# df.show()


#read data from open source 
#url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"

#read data from static 
df_ny = spark.read.option("header", "True").format("csv").load("/Users/sarasolis/pyspark-opensource/data/static/AB_NYC_2019.csv")
df_ny.show()
