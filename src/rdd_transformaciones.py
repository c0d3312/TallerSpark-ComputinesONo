"""
Taller Spark - Parte RDD (Janiera UA)
-------------------------------------
Crea un RDD de prueba con ventas de una tienda y aplica 4 transformaciones
que NO se vieron en clases:

    1. keyBy()          -> genera pares (clave, registro) a partir de una función
    2. mapPartitions()  -> procesa cada partición completa de una sola vez
    3. subtractByKey()  -> quita los pares cuya clave aparece en otro RDD
    4. zipWithIndex()   -> agrega un índice correlativo a cada elemento

Recuerda: las transformaciones son "lazy" (perezosas): Spark no ejecuta nada
hasta que se llama a una acción. Este script también demuestra cuatro acciones
nuevas sobre los RDD creados para el mismo conjunto de ventas.

Ejecutar desde la raíz:  python src/rdd_transformaciones.py
"""

from pyspark import SparkConf, SparkContext

# ---------------------------------------------------------------------------
# 0. Configuración de Spark
# ---------------------------------------------------------------------------
conf = SparkConf().setAppName("Taller_RDD_Transformaciones").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("ERROR")  # oculta los logs para que la salida sea legible


def titulo(texto):
    print("\n" + "=" * 70)
    print(texto)
    print("=" * 70)


# ---------------------------------------------------------------------------
# 1. RDD de prueba
#    Cada registro: (id_venta, producto, categoria, cantidad, precio_unitario)
# ---------------------------------------------------------------------------
ventas = [
    (1, "Notebook", "Tecnologia", 2, 650000),
    (2, "Mouse", "Tecnologia", 5, 12000),
    (3, "Silla", "Muebles", 3, 89000),
    (4, "Escritorio", "Muebles", 1, 150000),
    (5, "Monitor", "Tecnologia", 2, 180000),
    (6, "Lampara", "Hogar", 4, 25000),
    (7, "Teclado", "Tecnologia", 3, 30000),
    (8, "Cojin", "Hogar", 6, 8000),
    (9, "Estante", "Muebles", 2, 70000),
    (10, "Audifonos", "Tecnologia", 4, 45000),
]

# Se reparte en 3 particiones para que mapPartitions tenga sentido
rdd_ventas = sc.parallelize(ventas, numSlices=3)

titulo("RDD ORIGINAL")
print(f"Particiones: {rdd_ventas.getNumPartitions()}")
for v in rdd_ventas.collect():
    print(v)


# ---------------------------------------------------------------------------
# TRANSFORMACIÓN 1: keyBy()
# Crea pares (clave, valor) donde la clave se calcula con una función y el
# valor es el registro completo. Equivale a map(lambda x: (f(x), x)), pero
# es más legible y deja claro que estamos "indexando" por un campo.
# ---------------------------------------------------------------------------
rdd_por_categoria = rdd_ventas.keyBy(lambda v: v[2])

titulo("TRANSFORMACIÓN 1: keyBy()  -> clave = categoría")
for par in rdd_por_categoria.collect():
    print(par)


# ---------------------------------------------------------------------------
# TRANSFORMACIÓN 2: mapPartitions()
# A diferencia de map(), que llama a la función una vez POR ELEMENTO,
# mapPartitions() la llama una vez POR PARTICIÓN y recibe un iterador.
# Es útil cuando hay un costo fijo por llamada (abrir conexión a BD,
# cargar un modelo, etc.). Aquí calculamos el total vendido por partición.
# ---------------------------------------------------------------------------
def resumen_particion(iterador):
    registros = 0
    total = 0
    for _id, _prod, _cat, cantidad, precio in iterador:
        registros += 1
        total += cantidad * precio
    # Debe devolver un iterable
    yield {"registros": registros, "total_particion": total}


rdd_resumen_particiones = rdd_ventas.mapPartitions(resumen_particion)

titulo("TRANSFORMACIÓN 2: mapPartitions()  -> total vendido por partición")
for i, r in enumerate(rdd_resumen_particiones.collect()):
    print(f"Partición {i}: {r}")


# ---------------------------------------------------------------------------
# TRANSFORMACIÓN 3: subtractByKey()
# Devuelve los pares del RDD cuya clave NO existe en el otro RDD.
# Ejemplo: excluir las categorías que están en liquidación.
# ---------------------------------------------------------------------------
rdd_liquidacion = sc.parallelize([("Hogar", "liquidacion")])

rdd_sin_liquidacion = rdd_por_categoria.subtractByKey(
    rdd_liquidacion,
    numPartitions=rdd_ventas.getNumPartitions(),
)

titulo("TRANSFORMACIÓN 3: subtractByKey()  -> se excluye la categoría 'Hogar'")
for par in sorted(rdd_sin_liquidacion.collect(), key=lambda p: p[1][0]):
    print(par)


# ---------------------------------------------------------------------------
# TRANSFORMACIÓN 4: zipWithIndex()
# Agrega a cada elemento un índice correlativo (0, 1, 2, ...) según su
# posición en el RDD. Útil para numerar registros o generar un ranking.
# Aquí numeramos las ventas ordenadas por monto total (de mayor a menor).
# ---------------------------------------------------------------------------
rdd_ranking = (
    rdd_ventas
    .map(lambda v: (v[1], v[3] * v[4]))           # (producto, monto_total)
    .sortBy(lambda x: x[1], ascending=False)
    .zipWithIndex()                                # ((producto, monto), indice)
    .map(lambda x: (x[1] + 1, x[0][0], x[0][1]))   # (puesto, producto, monto)
)

titulo("TRANSFORMACIÓN 4: zipWithIndex()  -> ranking de ventas por monto")
for puesto, producto, monto in rdd_ranking.collect():
    print(f"#{puesto:<2} {producto:<12} ${monto:,}".replace(",", "."))


# ---------------------------------------------------------------------------
# ACCIONES
# Las cuatro acciones reutilizan los RDD de ventas y categorías definidos arriba.
# ---------------------------------------------------------------------------
# 1. countByKey()
# Cuenta cuántas ventas hay por categoría y devuelve un diccionario al driver.
conteo_por_categoria = rdd_por_categoria.countByKey()

titulo("ACCIÓN 1: countByKey()  -> ventas por categoría")
print(dict(conteo_por_categoria))


# 2. reduce()
# Suma el monto total vendido en todas las ventas.
suma_ventas = rdd_ventas.map(lambda v: v[3] * v[4]).reduce(lambda a, b: a + b)

titulo("ACCIÓN 2: reduce()  -> monto total vendido")
print(f"${suma_ventas:,}".replace(",", "."))


# 3. takeSample()
# Recupera una muestra aleatoria reproducible de tres ventas.
muestra_ventas = rdd_ventas.takeSample(False, 3, seed=42)

titulo("ACCIÓN 3: takeSample()  -> muestra de 3 ventas")
for venta in muestra_ventas:
    print(venta)


# 4. aggregate()
# Acumula cantidad de ventas e importe total por partición y combina los parciales.
resumen_ventas = rdd_ventas.aggregate(
    (0, 0),
    lambda acumulado, venta: (
        acumulado[0] + 1,
        acumulado[1] + venta[3] * venta[4],
    ),
    lambda parcial_a, parcial_b: (
        parcial_a[0] + parcial_b[0],
        parcial_a[1] + parcial_b[1],
    ),
)

titulo("ACCIÓN 4: aggregate()  -> cantidad y monto total")
print(f"Ventas: {resumen_ventas[0]} | Monto total: ${resumen_ventas[1]:,}".replace(",", "."))


sc.stop()
