import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
# Script generado para el nodo PostgreSQL
print("Leyendo datos desde PostgreSQL...")
PostgreSQL_node1705305162915 = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "useConnectionProperties": "true",
        "url": "jdbc:postgresql://lotus-db.c3e4qkoosswu.us-east-1.rds.amazonaws.com:5432/postgres",
        "dbtable": "rds_schools",
        "user": "beto",
        "password": "pasword123",
        "customJDBCOptions": {"driver": "org.postgresql.Driver"},
        "connectionName": "Postgresql connection",
    },
    transformation_ctx="PostgreSQL_node1705305162915",
)
# Lectura de datos completada
print("Lectura de datos completada.")

# Convierte a DataFrame de Spark y muestra los resultados
df = PostgreSQL_node1705305162915.toDF()
print(df.show(truncate=False))

# Registra el DataFrame como una tabla temporal
df.createOrReplaceTempView("temp_table")

# Ejecuta una consulta SQL en la tabla temporal
consulta_sql = "SELECT name, city FROM temp_table"
df_resultado = spark.sql(consulta_sql)


# Ejecuta una consulta SQL en el DataFrame
df_resultado = spark.sql(consulta_sql)

# Muestra el resultado de la consulta
print("Resultado de la consulta SQL:")
print(df_resultado.show(truncate=False))

# Muestra información adicional
print("Número total de filas:", df.count())

# Finaliza el trabajo
job.commit()
