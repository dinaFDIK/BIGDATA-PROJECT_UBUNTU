from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Inisialisasi Spark
spark = SparkSession.builder.appName("Fraud Detection").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Membaca stream dari Kafka
df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank_topic") \
    .load()

# Skema data sesuai modul
schema = StructType([
    StructField("nama", StringType()),
    StructField("rekening", StringType()),
    StructField("jumlah", IntegerType()),
    StructField("lokasi", StringType())
])

# Parsing data JSON
df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# --- KEAMANAN & PRIVASI ---
# 1. Data Masking (Sembunyikan nomor rekening)
df = df.withColumn("rekening_masked", 
                   concat(lit("****"), col("rekening").substr(-2,2)))

# 2. Fraud Detection Logic
df = df.withColumn("status", 
                   when(col("jumlah") > 50000000, "FRAUD")
                   .when(col("lokasi") == "Luar Negeri", "FRAUD")
                   .otherwise("NORMAL"))

# 3. Encryption (Base64 untuk kolom jumlah)
df = df.withColumn("jumlah_encrypted", base64(col("jumlah").cast("string")))

# Menyimpan hasil ke folder Parquet
query = df.writeStream \
    .format("parquet") \
    .option("path", "stream_data/realtime_output/") \
    .option("checkpointLocation", "data/checkpoints/") \
    .start()

print("Spark Streaming AKTIF... Menunggu data dari Kafka...")
query.awaitTermination()
