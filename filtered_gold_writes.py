#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
import re

@udf('boolean')
def is_add_player(event_as_json):
    event = json.loads(event_as_json)
    m = re.match('gold',event['event_type'])
    if m:
        return True
    return False

def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    add_player_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_add_player('raw'))

    extracted_add_player_events = add_player_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_add_player_events.printSchema()
    extracted_add_player_events.show()

    extracted_add_player_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/gold')
    
if __name__ == "__main__":
    main()
