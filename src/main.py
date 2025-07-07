from pyspark.sql import SparkSession
from utils.Utils import Utils
spark = SparkSession.builder.appName("MiProyecto").getOrCreate()
# df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "valor"])
# df.show()


#read data from open source 
#url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"

#read data from static 
static_path = "/Users/sarasolis/pyspark-opensource/data/static/AB_NYC_2019.csv"

# df_ny = spark.read.option("header", "True").format("csv").load(static_path)
# df_ny.show()

#read using util method
df_ny_method = Utils.loadDataFrame(spark, static_path)
df_ny_method.show()
