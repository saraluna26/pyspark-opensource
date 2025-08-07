from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, desc, sum, to_timestamp, lit, struct
from pyspark.sql.window import Window

class Processing:

    def new_column_price_category (df:DataFrame) -> DataFrame:
        return df.withColumn("Price Category", when(col("price") <= 120, "Cheap").when(col("price") <= 500, "Medium").when(col("price") > 500, "Expensive").otherwise("No"))

    def window_price_category(df:DataFrame) -> DataFrame:
        window=Window.partitionBy("Price Category").orderBy(desc("price"))
        return df.withColumn("total_num_reviews", sum("number_of_reviews").over(window))
        #.withColumn("rank", rank().over(window))
        #.withColumn("price_quintile"), nile(5).over(window)

    def group_by_category(df:DataFrame) -> DataFrame:
        return df.groupBy("Price Category").agg(sum("number_of_reviews").alias("total_agg_revies"))


    def remove_null(df:DataFrame)-> DataFrame:
        #example returns tuple df.dtype-> [('name', 'string'), ('age', 'int'), ('salary', 'double')]
        string_cols = [c for c, t in df.dtypes if t == 'string']
        numeric_cols = [c for c, t in df.dtypes if t in ['int', 'float', 'double']]
        dates_cols = [c for c, t in df.dtypes if t == 'date']

        # print("numeric cols ------------ " + str(numeric_cols))
        # print("string cols ------------ " + str(string_cols))
        # print("dates_cols cols ------------ " + str(dates_cols))

        # para las fechas mejor usar when y otherewise porque na.fill a veces no funciona bien con las fechas porque no hace cast
        # DateType, TimestampType, ArrayType, MapType, StructType, BinaryType, DecimalType, NullType -> does not work.
        for c in dates_cols:
            df = df.withColumn(c, when(col(c).isNull(), to_timestamp(lit('1970-01-01 00:00:00'))).otherwise(col(c)))

        df_no_nulls = df.na.fill("NA", subset=string_cols) \
             .na.fill(0, subset=numeric_cols)
        return df_no_nulls

    def count_null(df:DataFrame)-> DataFrame:
        df_nulls_counts = df.select(
            [sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
        ])
        return df_nulls_counts

    def count_null_col(df:DataFrame, col_name) -> DataFrame:
        return df.filter(col(col_name).isNull()).count()

    def handle_struct_nulls(df:DataFrame) -> DataFrame:
        #Data quality - handle nulls
        default_struct = struct(
            lit("NOIDMAP").alias("id"),
            lit("NONAMEMAP").alias("name")
        )

        return df.withColumn(
        'customer',
        when(col('customer').isNull(), default_struct).otherwise(col('customer'))
        ).withColumn(
        'customer',
        struct (
            when(col('customer.name').isNull(), lit("noName")).otherwise(col('customer.name')).alias("name"),
            when(col('customer.id').isNull(), lit("noID")).otherwise(col('customer.id')).alias("id")
        )
        ).withColumn('customer_name', col('customer.name')).withColumn('customer_id', col('customer.id'))

    def filter_by_neighbourhood(df:DataFrame, neighbourhood) -> DataFrame:
        return df.filter(col("neighbourhood_group") == neighbourhood)

    def count_rows_by_room_type(df:DataFrame, roomtype) -> DataFrame:
        pass


    

    



# Nivel 2: Filtrado y selección
# Filtra las propiedades que están en el barrio de 'Manhattan'.

# Cuenta cuántas propiedades hay por tipo de habitación (room_type).

# Selecciona solo las columnas name, neighbourhood_group, price y availability_365.

# Filtra propiedades con precio mayor a 200 dólares.

# Encuentra el precio máximo y mínimo de las propiedades en Brooklyn.



# Nivel 3: Agrupaciones y agregaciones
# Agrupa por neighbourhood_group y calcula el precio promedio.

# Encuentra el barrio con más propiedades listadas.

# Calcula el promedio de reviews por tipo de habitación.

# Encuentra el barrio con la mayor disponibilidad anual (availability_365) promedio.

# Ordena los barrios por precio promedio descendente.




# Nivel 4: Transformaciones y columnas nuevas
# Crea una nueva columna llamada price_per_night que convierta el precio a valor numérico (sin signos ni comas).

# Crea una columna price_category con valores: 'Low' (<100), 'Medium' (100-200), 'High' (>200).

# Calcula la duración promedio mínima de noches por barrio.

# Extrae el año y mes de una columna de fecha (si existe alguna, por ejemplo, last_review).

# Filtra propiedades que no tengan reviews (number_of_reviews = 0).



# Nivel 5: Joins y análisis más avanzados
# Si tienes otro dataset con información de barrios o distritos, haz un join con el dataset Airbnb.

# Agrupa por barrio y calcula la correlación entre price y number_of_reviews.

# Identifica propiedades que tengan un precio alto y pocas reviews (potenciales anomalías).

# Crea un resumen de disponibilidad por tipo de habitación y barrio.

# Escribe un DataFrame filtrado en formato parquet para su uso futuro.