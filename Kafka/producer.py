#!/usr/bin/env python3

import logging
from botocore.client import BaseClient
from botocore.response import StreamingBody
from boto3.session import Session as S3Session
from kafka import KafkaProducer
from dotenv import load_dotenv
from json import dumps
from os import environ
from random import uniform
from time import sleep
from typing import Mapping, Sequence

load_dotenv()

#################################################################################################

config_log = {
    'level': logging.INFO,
    'format': "%(asctime)s | %(message)s"
}
logging.basicConfig(**config_log, datefmt='%Y-%m-%d %H:%M:%S')


#################################################################################################

S3_BUCKET = environ.get('S3_BUCKET')
BOOTSTRAP_SERVER = 'localhost:29092'
TOPIC_NAME = 'taxi'

#################################################################################################

S3 = S3Session().client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=environ.get('AWS_KEY'),
    aws_secret_access_key=environ.get('AWS_SECRET')
)

#################################################################################################

def _iter_s3_response(boto_response: StreamingBody) -> str:
    """Итерирование по данным читаемых из S3."""

    boto_body = boto_response.get('Body', False)
    if boto_body:
        for line in boto_body.iter_lines():
            yield line.decode('utf8')

#################################################################################################

def read_data_from_s3(s3: BaseClient, bucket: str, dataset: str | None = None):
    """Главная функция чтения данных из S3."""

    if dataset:
        response = s3.get_object(Bucket=bucket, Key=dataset)
        yield from _iter_s3_response(response)
    else:
        for key in s3.list_objects(Bucket=bucket)['Contents']:
            response = s3.get_object(Bucket=bucket, Key=key)
            yield from _iter_s3_response(response)

#################################################################################################

def _create_data_fields(fields: Sequence[str]) -> Mapping[str, int | float]:
    """Приведение полей данных к нужному типу."""

    return {
        'vendor_id': int(fields[0]),
        'tpep_pickup_datetime': fields[1],
        'tpep_dropoff_datetime': fields[2],
        'passenger_count': int(fields[3]),
        'trip_distance': float(fields[4]),
        'ratecode_id': int(fields[5]),
        'store_and_fwd_flag': fields[6],
        'pulocation_id': int(fields[7]),
        'dolocation_id': int(fields[8]),
        'payment_type': int(fields[9]),
        'fare_amount': float(fields[10]),
        'extra': float(fields[11]),
        'mta_tax': float(fields[12]),
        'tip_amount': float(fields[13]),
        'tolls_amount': float(fields[14]),
        'improvement_surcharge': float(fields[15]),
        'total_amount': float(fields[16]),
        'congestion_surcharge': float(fields[17]),
    }

#################################################################################################

def main() -> None:
    """Главная функция запуска производителя данных."""

    line_counter = 0
    raw_data = read_data_from_s3(S3, S3_BUCKET, 'taxi_data/yellow_tripdata_2020-04.csv')
    kafka_producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_serializer=lambda x: dumps(x).encode('utf8')
    )
    logging.info(f'==> Started to produce data to Kafka Server')
    for line in raw_data:
        sleep(uniform(0.02, 0.05))
        txt_fields = line.strip().split(',')
        try:
            valid_fields = _create_data_fields(txt_fields)
            line_counter += 1
            kafka_producer.send(topic=TOPIC_NAME, value=valid_fields)
            logging.info(f'Lines {line_counter} sent')
        except Exception as err:
            print(err)
            continue
    logging.info(f'==> Streaming job is done!')

#################################################################################################

if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        logging.info(err)
