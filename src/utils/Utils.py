from pyspark.sql import DataFrame

class Utils:
    def loadDataFrame(spark, path:str, header:str="True", format:str="csv") -> DataFrame:
        return spark.read.option("header", header).format(format).load(path)