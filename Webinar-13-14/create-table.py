from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Создание Spark-сессии
spark = SparkSession.builder \
    .appName("create-table") \
    .enableHiveSupport() \
    .getOrCreate()

# Создание схемы данных
schema = StructType([StructField('Name', StringType(), True),
StructField('Capital', StringType(), True),
StructField('Area', IntegerType(), True),
StructField('Population', IntegerType(), True)])

# Создание датафрейма
df = spark.createDataFrame([('Австралия', 'Канберра', 7686850, 19731984), ('Австрия', 'Вена', 83855, 7700000)], schema)

# Запись датафрейма в бакет в виде таблицы countries
df.write.mode("overwrite").option("path","s3a://petr-bondarev-webinar14-bucket/countries").saveAsTable("countries")
