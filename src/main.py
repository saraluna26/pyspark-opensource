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

    df_new_column = Utils.new_column_price_category(df_ny_method)
    df_new_column.select("name", "price", "Price Category").show()

    df_window = Utils.window_price_category(df_new_column)
    df_window.select("name", "price", "Price Category", "total_num_reviews").show()

    # Using grouo by - we lose the extra information as name or price
    df_group_by = Utils.group_by_category(df_new_column)
    df_group_by.select("Price Category", "total_agg_revies").show()



#main() -- execute always the content when import it

# Clean and best practice. Only execute the content when this condition is true. This condition is only true if the file (__name__ variable) to run is this one (main).
# Doing that, we control the execution and avoid execute the code from outside. 
if __name__ == "__main__":
   # print(dir(main))
    main()