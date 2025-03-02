#!/usr/bin/env python3
# web_data_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('web-data-processor')

def create_spark_session():
    """Create and configure the Spark Session"""
    return (SparkSession
            .builder
            .appName("Web Data Processor")
            .master("spark://spark-master:7077")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0")
            .getOrCreate())

def define_schema():
    """Define the schema for incoming JSON data"""
    return StructType([
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("content", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("producer_timestamp", DoubleType(), True),
        # For public dataset type
        StructField("dataset_url", StringType(), True),
    ])

def read_from_kafka(spark, topic="web-data-stream"):
    """Read streaming data from Kafka"""
    return (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load())

def process_stream(kafka_stream, schema):
    """Process the Kafka stream with the given schema"""
    # Parse JSON from Kafka
    parsed_stream = (kafka_stream
                     .selectExpr("CAST(value AS STRING)")
                     .select(from_json(col("value"), schema).alias("data"))
                     .select("data.*"))

    # Add processing timestamp and calculate latency
    processed_stream = (parsed_stream
                        .withColumn("processing_timestamp", unix_timestamp())
                        .withColumn("latency_seconds", col("processing_timestamp") - col("producer_timestamp"))
                        .withColumn("event_time", to_timestamp(col("timestamp")))
                        .withWatermark("event_time", "10 minutes"))

    return processed_stream

def analyze_text_content(stream):
    """Analyze text content from the stream"""
    return (stream
            .filter(col("content").isNotNull() & (length(col("content")) > 0))
            .withColumn("words", explode(split(lower(col("content")), "\\s+")))
            .filter(length(col("words")) > 3)
            # Filter out common stopwords
            .filter(~col("words").isin("this", "that", "with", "from", "have", "your", "the", "and", "for"))
            .groupBy(window(col("event_time"), "10 minutes"), col("words"))
            .count()
            .orderBy(col("window"), col("count").desc()))

def calculate_stats(stream):
    """Calculate general statistics from the stream"""
    return (stream
    .withColumn("content_length", when(col("content").isNotNull(), length(col("content"))).otherwise(0))
    .groupBy(window(col("event_time"), "5 minutes"), col("source"))
    .agg(
        count("*").alias("record_count"),
        avg("latency_seconds").alias("avg_latency"),
        avg("content_length").alias("avg_content_length"),
        max("content_length").alias("max_content_length"),
        min("content_length").alias("min_content_length")
    ))

def analyze_by_source(stream, source_name):
    """Apply source-specific processing logic"""
    if source_name in ["news_api", "hackernews"]:
        # For news sources, focus on title words
        return (stream
                .filter(col("source") == source_name)
                .select("title", "content", "event_time")
                .withColumn("title_words", explode(split(col("title"), " ")))
                .filter(length(col("title_words")) > 3)
                .groupBy(col("title_words"), window(col("event_time"), "15 minutes"))
                .count()
                .orderBy(col("count").desc()))

    elif source_name == "reddit":
        # For Reddit, analyze both title and content together
        return (stream
                .filter(col("source") == source_name)
                .select("title", "content", "event_time")
                .withColumn("words", explode(split(concat(col("title"), lit(" "), col("content")), " ")))
                .filter(length(col("words")) > 3)
                .groupBy(col("words"), window(col("event_time"), "15 minutes"))
                .count()
                .orderBy(col("count").desc()))

    elif source_name == "wikipedia":
        # For Wikipedia, focus on title changes
        return (stream
                .filter(col("source") == source_name)
                .select("title", "content", "event_time")
                .withColumn("title_words", explode(split(col("title"), " ")))
                .groupBy(col("title_words"), window(col("event_time"), "15 minutes"))
                .count()
                .orderBy(col("count").desc()))

    elif source_name == "public_dataset":
        # Generic processing for public datasets
        return (stream
                .filter(col("source") == source_name)
                .groupBy(col("dataset_url"), window(col("event_time"), "30 minutes"))
                .count())

    else:
        # Default processing for any other source
        return (stream
                .filter(col("source") == source_name)
                .groupBy(window(col("event_time"), "15 minutes"))
                .count())

def main():
    """Main entry point for the web data processor"""
    try:
        # Create Spark session
        logger.info("Initializing Spark session")
        spark = create_spark_session()

        # Define schema for the incoming data
        schema = define_schema()

        # Read from Kafka
        logger.info("Setting up Kafka stream reader")
        kafka_stream = read_from_kafka(spark)

        # Process the stream
        logger.info("Processing stream")
        processed_stream = process_stream(kafka_stream, schema)

        # Create various analysis streams
        text_analysis = analyze_text_content(processed_stream)
        stats_analysis = calculate_stats(processed_stream)

        # Start queries for different sources
        sources = ["news_api", "reddit", "wikipedia", "hackernews", "public_dataset"]
        source_queries = {}

        for source in sources:
            source_stream = analyze_by_source(processed_stream, source)

            # Output query for each source
            source_queries[source] = (source_stream
                                      .writeStream
                                      .queryName(f"{source}_analysis")
                                      .outputMode("complete")
                                      .format("console")
                                      .option("truncate", "false")
                                      .trigger(processingTime="1 minute")
                                      .start())

            logger.info(f"Started query for source: {source}")

        # Start text analysis query
        text_query = (text_analysis
                      .writeStream
                      .queryName("text_analysis")
                      .outputMode("complete")
                      .format("console")
                      .option("truncate", "false")
                      .trigger(processingTime="30 seconds")
                      .start())

        # Start stats query
        stats_query = (stats_analysis
                       .writeStream
                       .queryName("stats_analysis")
                       .outputMode("complete")
                       .format("console")
                       .option("truncate", "false")
                       .trigger(processingTime="30 seconds")
                       .start())

        # Write processed data to parquet for later analysis
        parquet_query = (processed_stream
                         .writeStream
                         .queryName("parquet_sink")
                         .format("parquet")
                         .option("path", "/opt/spark-data/web-data")
                         .option("checkpointLocation", "/opt/spark-checkpoints/web-data")
                         .partitionBy("source")
                         .trigger(processingTime="1 minute")
                         .start())

        logger.info("All streaming queries started, awaiting termination")

        # Wait for all queries to terminate
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise
    finally:
        logger.info("Shutting down")
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()