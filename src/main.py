from pyspark.sql import SparkSession
from utils.Utils import Utils
from pyspark.sql.functions import desc


def main():
    spark = SparkSession.builder.appName("MiProyecto").getOrCreate()
    # df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "valor"])
    # df.show()


    #read data from open source 
    #url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"

    #read data from static 
    static_path = "/Users/sarasolis/pyspark-opensource/data/static/AB_NYC_2019.csv"

    # df_ny = spark.read.option("header", "True").format("csv").load(static_path)
    # df_ny.show()

    #read file using util method
    df_ny_method = Utils.loadDataFrame(spark, static_path)
    #Show rows
    #Utils.showRows(df_ny_method, 2)
    #Count rows
    #print(Utils.countRows(df_ny_method))
    #show schema
    print(Utils.showSchema(df_ny_method))

    #stadistics
    Utils.stadistic_summary(df_ny_method).show()

    Utils.sum(df_ny_method).show()
    Utils.mean(df_ny_method).show()

    Utils.max_values(df_ny_method).show()

    df_ny_method.orderBy(desc("price")).show(10)






if __name__ == "__main__":
    main()