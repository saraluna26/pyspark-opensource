from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import sum, mean, min, max, col, when, rank, desc
from pyspark.sql.window import Window


class Utils:
    #Load the DF by default with inferschema True and Headers true, csv format.
    def loadDataFrame(spark, path:str, header:str="True", format:str="csv", inferschema:str="True") -> DataFrame:
        return spark.read.option("header", header).format(format).option("inferschema", inferschema).load(path)

    #Show the rows defined in the variable
    def showRows(df:DataFrame, number_of_rows:str):
        df.show(number_of_rows)

    #Count total of the rows
    def countRows(df:DataFrame):
        return df.count()

    # Show the schema
    def showSchema (df:DataFrame):
        return df.schema.fields
        #return df.printSchema() - only to display ir


    #Print the stadistics summary for integer columns; show max, min and mean for price col.
    def stadistic_summary(df:DataFrame) -> DataFrame:
        return df.select(max("price").alias("Max Price"), min("price").alias("Min Price"), mean("price").alias("Mean Price"))

    def sum(df:DataFrame) -> DataFrame:
        integer_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, IntegerType)]
        df_integers = df.select(integer_cols)
        
        return df.select([sum(c).alias(c) for c in df_integers.columns])

    def mean(df:DataFrame) -> DataFrame: 
        integer_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, IntegerType)]
        df_integers = df.select(integer_cols)

        return df.select([mean(c).alias("mean") for c in df_integers.columns])

    def max_values(df:DataFrame) -> DataFrame:
        integer_cols = [c.name for c in df.schema.fields if isinstance(c.dataType, IntegerType)]
        df_integers = df.select(integer_cols)
        
        return df.select([max(c).alias(c + f' max value') for c in df_integers.columns])


    def new_column_price_category (df:DataFrame) -> DataFrame:
        return df.withColumn("Price Category", when(col("price") <= 120, "Cheap").when(col("price") <= 500, "Medium").when(col("price") > 500, "Expensive").otherwise("No"))

    def window_price_category(df:DataFrame) -> DataFrame:
        window=Window.partitionBy("Price Category").orderBy(desc("price"))
        return df.withColumn("total_num_reviews", sum("number_of_reviews").over(window))
        #.withColumn("rank", rank().over(window))
        #.withColumn("price_quintile"), nile(5).over(window)


    def group_by_category(df:DataFrame) -> DataFrame:
        return df.groupBy("Price Category").agg(sum("number_of_reviews").alias("total_agg_revies"))




