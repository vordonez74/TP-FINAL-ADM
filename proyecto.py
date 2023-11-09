from pyspark.sql import SparkSession

# Crear una sesión Spark
spark = SparkSession.builder.appName("EjemploSparkSubmit").getOrCreate()

# Crear un DataFrame de ejemplo
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
df = spark.createDataFrame(data, ["Nombre", "Edad"])

# Realizar alguna operación en el DataFrame
df.show()

# Guardar el DataFrame en un formato específico (puedes ajustarlo según tus necesidades)
df.write.mode("overwrite").csv("/ruta/a/donde/guardar/resultados")

# Detener la sesión Spark
spark.stop()