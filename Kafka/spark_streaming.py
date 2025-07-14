#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

#################################################################################################

trips_schema = StructType(
    [
        StructField('vendor_id', StringType(), True),
        StructField('tpep_pickup_datetime', TimestampType(), True),
        StructField('tpep_dropoff_datetime', TimestampType(), True),
        StructField('passenger_count', IntegerType(), True),
        StructField('trip_distance', DoubleType(), True),
        StructField('ratecode_id', IntegerType(), True),
        StructField('store_and_fwd_flag', StringType(), True),
        StructField('pulocation_id', IntegerType(), True),
        StructField('dolocation_id', IntegerType(), True),
        StructField('payment_type', IntegerType(), True),
        StructField('fare_amount', DoubleType(), True),
        StructField('extra', DoubleType(), True),
        StructField('mta_tax', DoubleType(), True),
        StructField('tip_amount', DoubleType(), True),
        StructField('tolls_amount', DoubleType(), True),
        StructField('improvement_surcharge', DoubleType(), True),
        StructField('total_amount', DoubleType(), True),
        StructField('congestion_surcharge', DoubleType()),
    ]
)

dim_columns = ['id', 'name']
payment_rows = [
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip'),
]

#################################################################################################

def _spark_connection() -> SparkSession:
    """Создание соединения с PySpark."""

    session = SparkSession \
        .builder \
        .appName('StreamingPipeline') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
        .getOrCreate()

    return session

#################################################################################################

def create_dict(spark: SparkSession, header: list[str], data: list):
    """создание словаря"""
    df = spark.createDataFrame(data=data, schema=header)
    return df

#################################################################################################

def _foreach_batch_function(dataframe: DataFrame, *args) -> None:
    dataframe.write.mode('append').json('output_report')

#################################################################################################

def main() -> None:

    fields = list(map(lambda x: f'json_message.{x.name}', trips_schema.fields))
    spark_session = _spark_connection()

    stream_df = spark_session \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:29092') \
        .option('subscribe', 'taxi') \
        .option('startingOffsets', 'latest') \
        .load() \
        .select(sf.from_json(sf.col('value').cast('string'), trips_schema).alias('json_message')) \
        .select(fields)

    datamart = stream_df \
        .groupBy('payment_type', sf.to_date('tpep_pickup_datetime').alias('dt')) \
        .agg(sf.count(sf.col('*')).alias('cnt')) \
        .join(other=create_dict(spark_session, dim_columns, payment_rows), on=sf.col('payment_type') == sf.col('id'), how='inner') \
        .select(sf.col('name'), sf.col('cnt'), sf.col('dt')) \
        .select(to_json(struct('name', 'cnt', 'dt')).alias('value'))

    writer = datamart.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:29092') \
        .option('topic', 'report') \
        .option('checkpointLocation', 'checkpoints/aggregated') \
        .outputMode('update') \
        .start()

    writer.awaitTermination()


#################################################################################################

if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        print(err)









