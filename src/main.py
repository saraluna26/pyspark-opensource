from pyspark.sql import SparkSession
from utils.Utils import Utils
from pyspark.sql.functions import desc, col, lit, concat

def load_df(spark) -> DataFrame:
    # df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "valor"])
    # df.show()

    #read data from open source 
    #url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"

    #read data from static 
    static_path = "/Users/sarasolis/pyspark-opensource/data/static/AB_NYC_2019.csv"

    #read file using util method
    df_ny_method = Utils.loadDataFrame(spark, static_path)
    #Show rows
    #Utils.showRows(df_ny_method, 2)
    #Count rows
    #print(Utils.countRows(df_ny_method))
    #show schema
    print(Utils.showSchema(df_ny_method))
    return df_ny_method

def stadistics_calls(df_ny_method:DataFrame) -> DataFrame:
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

    
def json_test():
    static_path_json = "/Users/sarasolis/pyspark-opensource/data/static/data_json.json"
    static_path_json2 = "/Users/sarasolis/pyspark-opensource/data/static/data_json_structure2.json"
    df_json = Utils.loadDataFrame(spark, static_path_json, True, "json")  #spark.read.format("json").load(static_path_json)
    df_json_structure2 = spark.read.format("json").option("multiline", "true").load(static_path_json) 
    
    # print (df_json_structure2)
    # df_json_structure2.show()

    # df = spark.read.option("multiline", "true").json(static_path_json2)
    # df.show()

    # print(df_json)
    # df_json.show()

    data_example = [
    {
      "policy_id": "P001",
      "customer": {
        "id": "C123",
        "name": "Sara Lee"
      },
      "premium": "1000",
      "issue_date": "2023-01-15",
      "status": "in progress"
    },
    {
      "policy_id": "P002",
      "customer": None,
      "premium": "not available",
      "issue_date": "invalid_date",
      "status": "cancelled"
    }
    ]

    df = spark.createDataFrame(data_example)
    df.show()
    df.select("customer.name", "customer.id").show()
    df_customer_valuese = df.withColumn("customer_name", col("customer.name")).withColumn("customer_id", col("customer.id"))

    
    print(Utils.showSchema(df_customer_valuese))
    df_customer_valuese.printSchema()
    df.withColumn("customer_name", lit(" name ")).show()


    df.withColumn("customer_name", concat(col("customer.name"), lit(" name "))).show()
    df.withColumn("policy_id", concat(col("policy_id"), lit (" PI"))).show()


#main() -- execute always the content when import it

# Clean and best practice. Only execute the content when this condition is true. This condition is only true if the file (__name__ variable) to run is this one (main).
# Doing that, we control the execution and avoid execute the code from outside. 
if __name__ == "__main__":
  # print(dir(main))
  spark = SparkSession.builder.appName("MiProyecto").getOrCreate()
  df_ny_method = load(spark)
  #Calls for stadistics methods
  stadistics_calls(df_ny_method)


  #Some example testing with JSON files
  json_test()
