# Taller Spark Computines O No: Análisis con PySpark (RDD y DataFrame)

## Integrantes: 
- Adrián Mancilla Carter (Scrum Master)
- María Alejandra Astrelli
- Benjamín Díaz
- Felipe González
- Iván Herrera
- Benjamín Lagos

## Objetivo del proyecto: 
Este repositorio contiene el desarrollo del taller práctico sobre el ecosistema de Apache Spark. El objetivo principal es demostrar las diferencias, ventajas y métodos de manipulación de datos estructurados y semi-estructurados utilizando **Resilient Distributed Datasets (RDD)** y **DataFrames**.

## Distribución del trabajo:
- *Adrián Mancilla Carter:* Hacer el Github, preparar la presentación y hacer un texto explicando la lógica del trabajo completo para la presentación.
- *Benjamín Díaz:* Buscar o generar los datasets que se usará (Subir los datos al github), investigar y hacer la tabla comparativa entre RDD, DataFrame y DataSet. Debe evaluar criterios como: optimización (Catalyst Optimizer), tipado (fuerte vs débil), rendimiento, inmutabilidad y facilidad de uso.
- *Iván Herrera:* Subir el script de RDD en PySpark que genere un RDD de prueba y aplique las 4 transformaciones no vistas en clases.
- *Felipe González:* Hacer un pull del código de María Alejandra Astrelli y agregar al script las 4 acciones no vistas en clases. Asegurar que el script quede corriendo correctamente.
- *Benjamín Lagos:* Subir el script inicial donde se cree un DataFrame (leyendo un CSV o desde datos en duro) y se apliquen las funciones que el profe enseñó en clases.
- *María Alejandra Astrelli:* Actualizar el script del B. Lagos agregando las 4 nuevas funciones que no se hayan visto.

## Estructura del Repositorio

*   `/data`: Contiene el dataset utilizado para el taller.
*   `/docs`: Documentación técnica, incluyendo el cuadro comparativo.
*   `/src`: Código fuente de PySpark dividido en los scripts de RDD y DataFrame.

## Recuperación del dataset:

Para el desarrollo de los scripts en PySpark, utilizamos el **Books Dataset** obtenido de Kaggle. Este conjunto de datos proporciona información estructurada sobre libros, lo que resulta ideal para aplicar transformaciones (map, filter) y acciones en RDD, así como funciones de agregación y análisis de columnas mediante DataFrames.

*   **Fuente original:** [Kaggle - Books Dataset por Elvin Rustam](https://www.kaggle.com/datasets/elvinrustam/books-dataset)
*   **Archivo local:** `data/BooksDatasetClean.csv` (Versión procesada y limpiada para el taller).

### Estructura de los Datos
*link de recuperación:* https://www.kaggle.com/datasets/elvinrustam/books-dataset?resource=download
*licencia CCO:* Dominio Publico
*Categoría:* Librería virtual
*Anotación:* El dataset incluye dos .csv : BooksDataset.csv ; BooksDatasetClean.csv. El equipo tomó la decisión de trabajar con **BooksDatasetClean.csv** para evitar hacer el proceso de limpieza por nuestra cuenta.
