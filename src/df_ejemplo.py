"""
Taller Spark - Parte DataFrame (Benjamín Lagos)
-----------------------------------------------
Script inicial: crea un DataFrame de Spark leyendo el Books Dataset
(data/BooksDatasetClean.csv) y aplica los mismos pasos del EDA visto en
clases (Semana 10, EjemploEda.py), pero traducidos de pandas a PySpark:
 
    Paso 1  Librerías
    Paso 2  Carga de datos            -> spark.read.csv()
    Paso 3  Exploración inicial       -> show()            (pandas: head())
    Paso 4  Información básica        -> count/columns, printSchema(), describe()
    Paso 5  Nulos y su manejo         -> isNull() + fillna()
    Paso 6  Análisis univariante      -> histograma del precio
    Paso 7  Análisis bivariante       -> dispersión año vs precio
    Paso 8  Valores atípicos          -> media ± 2 desviaciones estándar
    Paso 9  Resumen de observaciones  -> groupBy().count() (pandas: value_counts())
    Paso 10 Guardar el DataFrame limpio
 
Las 4 funciones nuevas (no vistas en clases) las agrega María Alejandra
en la sección marcada al final del script.
 
Ejecutar desde la raíz del repo:  python src/df_ejemplo.py
"""
 
# ---------------------------------------------------------------------------
# Paso 1: Librerías necesarias
# ---------------------------------------------------------------------------
import os
 
import matplotlib.pyplot as plt          # Para visualizaciones (igual que en clases)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F   # Funciones de columna (mean, stddev, col, ...)
from pyspark.sql.window import Window    # Necesario para la función Window de M. Alejandra
 
# Rutas relativas a este archivo, así el script funciona desde cualquier carpeta
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUTA_CSV = os.path.join(BASE_DIR, "..", "data", "BooksDatasetClean.csv")
RUTA_SALIDA = os.path.join(BASE_DIR, "..", "salida", "books_limpio.csv")
 
# SparkSession: punto de entrada para trabajar con DataFrames
spark = (
    SparkSession.builder
    .appName("Taller_DataFrame_EDA_Books")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")  # oculta los logs para que la salida sea legible
 
 
def titulo(texto):
    print("\n" + "=" * 70)
    print(texto)
    print("=" * 70)
 
 
# ---------------------------------------------------------------------------
# Paso 2: Cargar los datos
# En clases: df = pd.read_csv('ventas.csv')
# En Spark:  spark.read.csv(...) crea un DataFrame distribuido.
#   header=True                 -> la primera fila son los nombres de columna
#   inferSchema=True            -> Spark deduce los tipos (double, int, string)
#   escape='"'                  -> el CSV usa "" para comillas dentro de los textos
#   ignoreLeadingWhiteSpace     -> la columna Category viene con espacios al inicio
# ---------------------------------------------------------------------------
df = spark.read.csv(
    RUTA_CSV,
    header=True,
    inferSchema=True,
    escape='"',
    ignoreLeadingWhiteSpace=True,
)
 
# Los nombres originales tienen espacios y símbolos ("Price Starting With ($)"),
# así que los renombramos a algo más cómodo de usar en el código.
df = df.toDF("titulo", "autores", "descripcion", "categoria",
             "editorial", "precio", "mes_publicacion", "anio_publicacion")
 
 
# ---------------------------------------------------------------------------
# Paso 3: Exploración inicial de los datos
# pandas: df.head()   ->   Spark: df.show(n)
# ---------------------------------------------------------------------------
titulo("Paso 3: Exploración inicial (primeras 5 filas)")
df.select("titulo", "autores", "categoria", "precio", "anio_publicacion").show(5, truncate=40)
 
 
# ---------------------------------------------------------------------------
# Paso 4: Información básica del DataFrame
# pandas: shape / columns / info() / describe()
# Spark:  (count(), len(columns)) / columns / printSchema() / describe()
# ---------------------------------------------------------------------------
titulo("Paso 4: Información básica")
total_filas = df.count()
print("Dimensiones (filas, columnas):", (total_filas, len(df.columns)))
print("Columnas:", df.columns)
 
print("\nEsquema (tipos de datos):")
df.printSchema()
 
print("Estadísticas descriptivas de las columnas numéricas:")
df.describe("precio", "anio_publicacion").show()
 
 
# ---------------------------------------------------------------------------
# Paso 5: Detección de valores nulos y su manejo
# pandas: df.isnull().sum()
# Spark:  se cuenta, por columna, cuántas filas cumplen isNull()
# ---------------------------------------------------------------------------
titulo("Paso 5: Valores nulos por columna")
nulos = df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns
])
nulos.show()
 
# Imputación (igual que en clases con fillna):
# - Numérica: precio con la media (en clases: df['precio'].fillna(media))
# - Texto: las columnas de texto con un valor por defecto
media_precio = df.select(F.mean("precio")).first()[0]
df = df.fillna({"precio": media_precio})
df = df.fillna({
    "descripcion": "Sin descripción",
    "categoria": "Sin categoría",
    "editorial": "Desconocida",
})
 
print("Nulos después de imputar:")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()
 
 
# ---------------------------------------------------------------------------
# Paso 6: Análisis univariante (1 variable)
# matplotlib no dibuja DataFrames de Spark directamente, así que traemos SOLO
# la columna necesaria a pandas con toPandas() para graficar.
# ---------------------------------------------------------------------------
titulo("Paso 6: Análisis univariante (histograma de precios)")
precios_pd = df.select("precio").toPandas()

# El 99% de los libros cuesta menos que este valor; lo usamos para hacer "zoom"
limite_99 = precios_pd["precio"].quantile(0.99)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# Izquierda: todos los precios (se ve el sesgo por los valores extremos)
ax1.hist(precios_pd["precio"], bins=50)
ax1.set_title("Distribución de precios (todos)")
ax1.set_xlabel("Precio (USD)")
ax1.set_ylabel("Frecuencia")

# Derecha: zoom al 99% de los libros, donde se aprecia la variación real
ax2.hist(precios_pd[precios_pd["precio"] <= limite_99]["precio"], bins=40)
ax2.set_title(f"Zoom: 99% de los libros (precio ≤ {limite_99:.2f} USD)")
ax2.set_xlabel("Precio (USD)")
ax2.set_ylabel("Frecuencia")

plt.tight_layout()
plt.show()
 
 
# ---------------------------------------------------------------------------
# Paso 7: Análisis bivariante (2 variables)
# ¿Los libros más antiguos son más caros? -> año de publicación vs precio
# ---------------------------------------------------------------------------
titulo("Paso 7: Análisis bivariante (año vs precio)")
anio_precio_pd = df.select("anio_publicacion", "precio").toPandas()
 
plt.scatter(anio_precio_pd["anio_publicacion"], anio_precio_pd["precio"], s=5, alpha=0.4)
plt.title("Relación año de publicación vs precio")
plt.xlabel("Año de publicación")
plt.ylabel("Precio (USD)")
plt.show()
 
 
# ---------------------------------------------------------------------------
# Paso 8: Detección de valores atípicos ('outliers')
# Mismo criterio de clases: fuera del intervalo media ± 2 * desviación estándar
# ---------------------------------------------------------------------------
titulo("Paso 8: Valores atípicos de precio")
stats = df.select(F.mean("precio").alias("media"),
                  F.stddev("precio").alias("std")).first()
p_upp = stats["media"] + 2 * stats["std"]
p_low = stats["media"] - 2 * stats["std"]
print(f"Media: {stats['media']:.2f} | Std: {stats['std']:.2f}")
print(f"Intervalo normal: [{p_low:.2f}, {p_upp:.2f}]")
 
outliers = df.filter((F.col("precio") > p_upp) | (F.col("precio") < p_low))
print("Cantidad de libros con precio atípico:", outliers.count())
outliers.select("titulo", "precio").orderBy(F.col("precio").desc()).show(10, truncate=50)
 
 
# ---------------------------------------------------------------------------
# Paso 9: Resumen de observaciones importantes
# pandas: df['producto'].value_counts()
# Spark:  df.groupBy('categoria').count().orderBy(...)
# ---------------------------------------------------------------------------
titulo("Paso 9: Libros por categoría (top 10)")
df.groupBy("categoria").count().orderBy(F.col("count").desc()).show(10, truncate=False)
 
titulo("Paso 9: Libros por editorial (top 10)")
df.groupBy("editorial").count().orderBy(F.col("count").desc()).show(10, truncate=False)
 
 
# ---------------------------------------------------------------------------
# Paso 10: Guardar el DataFrame limpio
# Se guarda con pandas para evitar los problemas de winutils/Hadoop que
# tiene df.write.csv() en Windows. Genera un único archivo CSV.
# ---------------------------------------------------------------------------
titulo("Paso 10: Guardar DataFrame limpio")
os.makedirs(os.path.dirname(RUTA_SALIDA), exist_ok=True)
df.toPandas().to_csv(RUTA_SALIDA, index=False)
print("DataFrame limpio guardado en:", os.path.normpath(RUTA_SALIDA))
 
 
# ---------------------------------------------------------------------------
# FUNCIONES NUEVAS (María Alejandra)
# Aquí se agregan las 4 funciones de DataFrame no vistas en clases, usando el DataFrame `df` ya limpio de arriba.
# ---------------------------------------------------------------------------
titulo("Nueva Función 1: summary() en columnas numéricas")
df.select("precio", "anio_publicacion").summary("count", "mean", "min", "25%", "50%", "75%", "max").show()

titulo("Nueva Función 2: approxQuantile() para evaluar cuartiles")
cuartiles = df.approxQuantile("precio", [0.25, 0.5, 0.75], 0.01)
print(f"Cuartiles de precio -> Q1: {cuartiles[0]}, Mediana: {cuartiles[1]}, Q3: {cuartiles[2]}")

titulo("Nueva Función 3: crosstab() entre Categoría y Año (2020-2023)")
df_filtrado = df.filter(F.col("anio_publicacion").between(2020, 2023))
df_filtrado.crosstab("categoria", "anio_publicacion").show(10, truncate=False)

titulo("Nueva Función 4: Window Functions + rank() (Top 2 más caros por editorial)")
window_spec = Window.partitionBy("editorial").orderBy(F.col("precio").desc())
df_ranked = df.withColumn("ranking", F.rank().over(window_spec))

df_ranked.filter(F.col("ranking") <= 2) \
         .select("editorial", "titulo", "precio", "ranking") \
         .orderBy("editorial", "ranking") \
         .show(15, truncate=False)
 
spark.stop()
spark.stop()