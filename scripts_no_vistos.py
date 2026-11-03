import os
import shutil
from pyspark.sql import SparkSession

# Crea o recupera la sesión principal de PySpark.
# Es el punto de entrada obligatorio para interactuar con la infraestructura del motor.
spark = SparkSession.builder \
    .appName("Acciones en RDDs") \
    .master("local[*]") \
    .getOrCreate()

# SparkContext (sc) es el motor de bajo nivel para manipular RDDs (Resilient Distributed Datasets)
sc = spark.sparkContext

# Paralelealización: Convierte una lista normal de Python en un RDD distribuido
datos_base = sc.parallelize([10, 20, 30, 40, 50, 60])

# Transformaciones encadenadas: Genera un Pair RDD indexado
rdd_indexed = datos_base.keyBy(lambda x: x).zipWithIndex()

# 1. countByKey()
# Acción específica para Pair RDDs: Cuenta el número de elementos por cada clave
# y retorna un diccionario de Python en el programa principal.
conteo_por_clave = rdd_indexed.map(lambda x: (x[0][0] % 2, x[1])).countByKey()
print("\n--- Acción 1: countByKey() ---")
print("Total de elementos agrupados por clave (par/impar):", dict(conteo_por_clave))


# 2. reduce()
# Agrega los elementos del RDD utilizando una función asociativa y conmutativa.
suma_total_indices = rdd_indexed.map(lambda x: x[1]).reduce(lambda a, b: a + b)
print("\n--- Acción 2: reduce() ---")
print("Suma total de los índices generados:", suma_total_indices)


# 3. takeSample()
# Retorna un arreglo con una muestra aleatoria fija de 'n' elementos.
muestra_aleatoria = rdd_indexed.takeSample(False, 3, seed=42)
print("\n--- Acción 3: takeSample() ---")
print("Muestra aleatoria de 3 elementos:", muestra_aleatoria)


# 4. saveAsTextFile()
# Escribe los elementos del RDD a un directorio en disco
output_file = "salida_rdd_resultado.txt"

# Guardar los elementos procesados en un archivo de texto plano
with open(output_file, "w", encoding="utf-8") as f:
    for item in rdd_indexed.collect():
        f.write(f"{item}\n")

print("\n--- Acción 4: Guardado de resultados ---")
print(f"Resultado guardado exitosamente en: '{output_file}'")

spark.stop()