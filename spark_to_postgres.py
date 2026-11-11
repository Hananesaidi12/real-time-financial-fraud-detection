import os
from dotenv import load_dotenv
load_dotenv()
os.environ['HADOOP_HOME'] = r'C:\hadoop'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# -------------------------------
# 1️⃣ Créer SparkSession avec JDBC driver
# -------------------------------
spark = SparkSession.builder \
    .appName("SparkToPostgres") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .config("spark.jars", r"C:\Users\HP\Downloads\fraud_detection_project\postgresql-42.7.8.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# 2️⃣ Définir le schema
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
# 3️⃣ Lire depuis Kafka
# -------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

transactions = df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# -------------------------------
# 4️⃣ Configuration PostgreSQL
# -------------------------------
postgres_url = "jdbc:postgresql://localhost:5432/financial_dw"
properties = {
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"), 
    "driver": "org.postgresql.Driver"
}

# -------------------------------
# 5️⃣ Fonction pour écrire chaque batch
# -------------------------------
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write.jdbc(
            url=postgres_url,
            table="transactions_clean",
            mode="append",
            properties=properties
        )
        print(f"✅ Batch {batch_id} écrit dans PostgreSQL ({batch_df.count()} transactions)")

# -------------------------------
# 6️⃣ Écrire dans PostgreSQL
# -------------------------------
print("💾 Écriture vers PostgreSQL démarrée...")
query = transactions.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", r"C:\tmp\spark-postgres-checkpoint") \
    .start()

query.awaitTermination()

