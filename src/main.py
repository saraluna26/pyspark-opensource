from pyspark.sql import SparkSession, DataFrame
from utils.Utils import Utils
from processing.Processing import Processing
from pyspark.sql.functions import desc, col, lit, concat, when, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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

def stadistics_methods(df_ny_method:DataFrame) -> DataFrame:
    Utils.stadistic_summary(df_ny_method).show()

    # Utils.sum(df_ny_method).show()
    # Utils.mean(df_ny_method).show()

    # Utils.max_values(df_ny_method).show()

    # df_ny_method.orderBy(desc("price")).show(10)

def create_category(df_ny_method:DataFrame) -> DataFrame:
    df_new_column = Processing.new_column_price_category(df_ny_method)
    # df_new_column.select("name", "price", "Price Category").show()

    # df_window = Processing.window_price_category(df_new_column)
    # df_window.select("name", "price", "Price Category", "total_num_reviews").show()

    # # Using grouo by - we lose the extra information as name or price
    # df_group_by = Processing.group_by_category(df_new_column)
    # df_group_by.select("Price Category", "total_agg_revies").show()

    return df_new_column


def json_test():
    static_path_json = "/Users/sarasolis/pyspark-opensource/data/static/data_json.json"
    #wny the file below is not working fine?
    static_path_json2 = "/Users/sarasolis/pyspark-opensource/data/static/data_json_structure2.json"

    schema = StructType([
        StructField("policy_id", StringType(), True),
        StructField("customer", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ]), True),
        StructField("premium", StringType(), True),
        StructField("issue_date", StringType(), True),
        StructField("status", StringType(), True)
    ])


    df_json = Utils.loadDataFrame(spark, static_path_json, True, "json")  #spark.read.format("json").load(static_path_json)
    #df_json_structure2 = spark.read.format("json").option("multiline", "true").load(static_path_json) 

    df = spark.read.schema(schema).json(static_path_json)

    #Testing data_example
    # data_example = [
    # {
    #   "policy_id": "P001",
    #   "customer": {
    #     "id": "C123",
    #     "name": None
    #   },
    #   "premium": "1000",
    #   "issue_date": "2023-01-15",
    #   "status": "in progress"
    # },
    # {
    #   "policy_id": "P002",
    #   "customer": None,
    #   "premium": "not available",
    #   "issue_date": "invalid_date",
    #   "status": "cancelled"
    # }
    # ]
    #df = spark.read.schema(schema).json(data_example)
    #df = spark.createDataFrame(data_example)

  
    df.show()

    #checking schema
    # print(Utils.showSchema(df_customer_valuese))
    # df_customer_valuese.printSchema()
    

    #Testing concat() and lit()
    # df.withColumn("customer_name", concat(col("customer.name"), lit(" name "))).show()
    # df.withColumn("policy_id", concat(col("policy_id"), lit (" PI"))).show()
    #df.withColumn("customer_name", lit(" name ")).show()
    
    print("Null control in struct columns ---------------------------------------------")
    df = Processing.handle_struct_nulls(df)
    df.select("policy_id", "customer", "customer_name", "customer_id").show()
  
     

#main() -- execute always the content when import it

# Clean and best practice. Only execute the content when this condition is true. This condition is only true if the file (__name__ variable) to run is this one (main).
# Doing that, we control the execution and avoid execute the code from outside. 
if __name__ == "__main__":
  # print(dir(main))
  spark = SparkSession.builder.appName("MiProyecto").getOrCreate()
  df_ny_method = load_df(spark)
  #Calls for stadistics methods
  #stadistics_methods(df_ny_method)
  df_category = create_category(df_ny_method)
  print("DF WORKING WITH ---------------------------------------------")
  df_category.show()

  #Some example testing with JSON files --------------------------------------------- 
  json_test()


  print("Before cleaning null ---------------------------------------------")
  df_nulls_counts = Processing.count_null(df_category)
  #df_nulls_counts.show() # -- here we can see the nulls per column in a df.
  print("Total nulls before remove: " +str(sum(df_nulls_counts.collect()[0].asDict().values())))

  list_cols_with_nulls = []
  dict_cols_with_nulls = df_nulls_counts.collect()[0].asDict()

  for c, val in dict_cols_with_nulls.items():
    if val != 0:
      list_cols_with_nulls.append(c)
    
  print("List col with nulls: ")
  print(list_cols_with_nulls)


  print("After cleaning null:")
  df_no_nulls = Processing.remove_null(df_category)

  for col_with_null in list_cols_with_nulls: 
    print(f"  - Check null after cleaning df for col {col_with_null}: " + str(df_no_nulls.filter(col(col_with_null).isNull()).count()))
   
  print("After cleaning null ---------------------------------------------")
  df_nulls_counts = Processing.count_null(df_no_nulls)
  # df_nulls_counts.show()
  print("Total nulls after remove: " +str(sum(df_nulls_counts.collect()[0].asDict().values())))


  Processing.filter_by_neighbourhood(df_category, "Manhattan").show()
