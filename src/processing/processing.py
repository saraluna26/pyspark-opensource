class Processing:

    

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