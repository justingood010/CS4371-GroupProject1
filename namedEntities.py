import spacy
import argparse                                   # ---- added for Part 2 ----
from pyspark.ml import PipelineModel               # ---- added for Part 2 ----
from pyspark.sql import functions as F             # ---- added for Part 2 ----
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


# ---- Part 2 (2.3c), added below: running count of messages per predicted class, using the saved MLlib PipelineModel on the same topic1 stream.
_parser = argparse.ArgumentParser()
_parser.add_argument("--model-path", required=True, help="path to the saved PipelineModel folder")
_parser.add_argument("--checkpoint-dir", default="./spark-checkpoints",
                     help="base folder for the class-count query checkpoint")
_parser.add_argument("--trigger-seconds", type=int, default=30)
_args = _parser.parse_args()

_model = PipelineModel.load(_args.model_path)
_pred = _model.transform(df_text)   # df_text is the topic1 text stream defined above

_labels = ["World", "Sports", "Business", "Sci/Tech"]
_label_map = F.create_map(*[x for i, l in enumerate(_labels)
                            for x in (F.lit(float(i)), F.lit(l))])

_class_counts = (_pred
    .select(_label_map[F.col("prediction")].alias("predicted_class"))
    .groupBy("predicted_class")
    .count())

query2 = (_class_counts.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", _args.checkpoint_dir + "/classes")
    .trigger(processingTime=f"{_args.trigger_seconds} seconds")
    .start())

query.awaitTermination()
