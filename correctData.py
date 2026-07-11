# --------------------------------------------------------------------
# correctData.py
# --------------------------------------------------------------------
# Spark Streaming job that:
# 1. Reads text lines from a socket (port 9999).
# 2. Sends each RDD partition to a set of inference services in a
#    round‑robin fashion.
# 3. Collects the original and corrected sentences into a DataFrame
#    and writes the result as CSV to HDFS (appending to any
#    existing data).
# --------------------------------------------------------------------

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, Row
import os, json, requests

# --------------------------------------------------------------------
# URL of the default inference service (used only for testing)
# --------------------------------------------------------------------
INFER_URL = "http://localhost:5000/correct"

# --------------------------------------------------------------------
# List of inference service endpoints.  Each partition will be
# sent to one of these URLs in a round‑robin manner.
# --------------------------------------------------------------------
INFER_SERVICES = [
    "http://master:5000/correct",
    "http://worker2:5001/correct"
]

# --------------------------------------------------------------------
# Helper: Call the inference API on a list of sentences.
# --------------------------------------------------------------------
def call_inference(sentences):
    payload = {"sentences": sentences}
    r = requests.post(INFER_URL, json=payload, timeout=3000)
    r.raise_for_status()
    return r.json().get("corrected", [])

# --------------------------------------------------------------------
# Process a single RDD partition using the specified inference URL.
# Returns an iterator of Row objects containing the wrong & corrected
# sentences.
# --------------------------------------------------------------------
def process_partition_with_url(partition, service_url):
    sentences = [s for s in partition if s]
    if not sentences:
        return []
    payload = {"sentences": sentences}
    r = requests.post(service_url, json=payload, timeout=60)
    r.raise_for_status()
    corrected = r.json().get("corrected", [])
    return [Row(wrong_sentence=s, corrected_sentence=c)
            for s, c in zip(sentences, corrected)]

# --------------------------------------------------------------------
# Spark configuration
# --------------------------------------------------------------------
sc = SparkContext("spark://master:7077", "FarsiSentenceCorrector")
ssc = StreamingContext(sc, 10)  # 10‑second micro‑batch interval
spark = SparkSession.builder.appName("FarsiSentenceCorrector").getOrCreate()
sc.setLogLevel("OFF")

# --------------------------------------------------------------------
# Create a DStream that reads lines from the TCP socket
# --------------------------------------------------------------------
lines = ssc.socketTextStream("192.168.24.105", 9999)

# --------------------------------------------------------------------
# Function applied to each RDD in the DStream
# --------------------------------------------------------------------
def process_rdd(rdd):
    if not rdd.isEmpty():
        # Repartition so that each inference service gets roughly
        # an equal share of the data
        rdd = rdd.repartition(len(INFER_SERVICES))

        # Send each partition to its assigned inference URL
        corrected = rdd.mapPartitionsWithIndex(
            lambda idx, part: process_partition_with_url(
                part,
                INFER_SERVICES[idx % len(INFER_SERVICES)]
            )
        )

        # Convert the corrected data to a DataFrame
        df = spark.createDataFrame(corrected)
        df.show(truncate=False)

        # Path to the output directory on HDFS
        hdfs_path = "hdfs://master:9000/corrected_sentences"

        # Append the new records to the existing CSV files
        df.write.mode("append").csv(hdfs_path, header=True)

# --------------------------------------------------------------------
# Register the RDD processing function
# --------------------------------------------------------------------
lines.foreachRDD(process_rdd)

# --------------------------------------------------------------------
# Start streaming and wait for termination
# --------------------------------------------------------------------
ssc.start()
ssc.awaitTermination()
