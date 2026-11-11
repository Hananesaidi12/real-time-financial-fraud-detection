import os
os.environ['HADOOP_HOME'] = r'C:\hadoop'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# -------------------------------
# 1️⃣ Créer la session Spark AVEC package Kafka
# -------------------------------
spark = SparkSession.builder \
    .appName("FinancialTransactionsStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# 2️⃣ Définir le schema des transactions
# -------------------------------
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("transaction_type", StringType()),
    StructField("country", StringType()),
    StructField("is_fraud_simulated", BooleanType()),
    StructField("timestamp", StringType())
])

# -------------------------------
# 3️⃣ Lire le flux depuis Kafka
# -------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# -------------------------------
# 4️⃣ Convertir la valeur en JSON et appliquer le schema
# -------------------------------
transactions = df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")

# -------------------------------
# 5️⃣ Transformer timestamp en type TimestampType
# -------------------------------
transactions = transactions.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# -------------------------------
# 6️⃣ Exemple de traitement : moyenne du montant par utilisateur chaque minute
# -------------------------------
agg_df = transactions.groupBy(
    window(col("timestamp"), "1 minute"),
    col("user_id")
).avg("amount")

# -------------------------------
# 7️⃣ Affichage en console
# -------------------------------
print("Streaming started...")
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", r"C:\tmp\spark-checkpoint") \
    .start()

query.awaitTermination()