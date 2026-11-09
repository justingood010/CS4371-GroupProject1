import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, from_json, to_json, struct
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# ---- 1. Define the UDF  ----

_nlp = None
def entities(text):
    global _nlp
    if not text:
        return []
    if _nlp is None:
        _nlp = spacy.load("en_core_web_sm")
    return [e.text for e in _nlp(text).ents]

ner = udf(entities, ArrayType(StringType()))

# ---- 2. Spark session ----
spark = (SparkSession.builder
    .appName("EntityExtraction")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0")
    .getOrCreate())

# ---- 3. Read from Kafka Topic 1 ----
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "bsky-posts")
    .option("startingOffsets", "earliest")
    .load())

# Kafka gives you a binary `value` column → cast to string
df_text = df.selectExpr("CAST(value AS STRING) AS text")

# ---- 4. Apply the UDF ----
df_entities = df_text.withColumn("entities", ner(col("text")))

# ---- 5. Explode so each entity becomes its own row ----
df_exploded = df_entities.select(explode(col("entities")).alias("entity"))

# ---- 6. Count per entity (running count within each micro-batch) ----
df_counts = (df_exploded
    .groupBy("entity")
    .count())

# ---- 7. Format as JSON and write to Topic 2 ----
df_out = df_counts.select(
    to_json(struct(col("entity"), col("count"))).alias("value")
)

query = (df_out.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "entities-counts")
    .option("checkpointLocation", "/tmp/spark-checkpoints/entities")
    .outputMode("complete")   # complete = full count table each trigger
    .start())

query.awaitTermination()