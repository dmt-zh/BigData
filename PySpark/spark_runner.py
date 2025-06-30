#!/usr/bin/env python3.12

from argparse import ArgumentParser
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pathlib import Path


####################################################################################################################

def _create_spark_session():
    """Создание соединения с PySpark."""

    return SparkSession.builder.appName('Flights Processor').getOrCreate()

####################################################################################################################

class PySparkProcessor:
    """Класс для агрегации датафрейма с помощью PySpark."""

    def __init__(
        self,
        input_files_path: list[Path],
        output_path: Path,
        task: str,
    ) -> None:
        self._input = tuple(map(str, input_files_path))
        self._output = str(output_path)
        self._task = task
        self._run_task = {
            'task_1': self._run_task_one,
            'task_2': self._run_task_two,
            'task_3': self._run_task_three,
            'task_4': self._run_task_four,
            'task_5': self._run_task_five,
        }

    ################################################################################################################

    def _run_task_one(self, spark_df: DataFrame, session: SparkSession) -> DataFrame:
        """Получение сводной таблицы по топ 10 рейсов по коду рейса."""

        agg_table = spark_df \
            .where(spark_df['TAIL_NUMBER'].isNotNull()) \
            .groupBy(spark_df['TAIL_NUMBER']) \
            .agg(sf.count(spark_df['FLIGHT_NUMBER']).alias('count')) \
            .select(['TAIL_NUMBER', 'count']) \
            .orderBy(sf.col('count').desc()) \
            .limit(10)

        return agg_table

    ################################################################################################################

    def _run_task_two(self, spark_df: DataFrame, session: SparkSession) -> DataFrame:
        """Получение сводной таблицы по топ 10 авиамаршрутов по
        наибольшему числу рейсов, а так же среднее время в полете."""

        agg_table = spark_df \
            .groupBy(
                spark_df['ORIGIN_AIRPORT'],
                spark_df['DESTINATION_AIRPORT'],
                ) \
            .agg(
                sf.count(spark_df['TAIL_NUMBER']).alias('tail_count'),
                sf.avg(spark_df['AIR_TIME']).alias('avg_air_time'),
                ) \
            .select(['ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'tail_count', 'avg_air_time']) \
            .orderBy(sf.col('tail_count').desc()) \
            .limit(10)

        return agg_table
    
    ################################################################################################################

    def _run_task_three(self, spark_df: DataFrame, session: SparkSession) -> DataFrame:
        """Получение сводной таблицы по аэропортам, у которых
        самые большие проблемы с задержкой на вылет рейса."""

        agg_table = spark_df \
            .groupBy(spark_df['ORIGIN_AIRPORT']) \
            .agg(
                sf.avg(spark_df['DEPARTURE_DELAY']).alias('avg_delay'),
                sf.min(spark_df['DEPARTURE_DELAY']).alias('min_delay'),
                sf.max(spark_df['DEPARTURE_DELAY']).alias('max_delay'),
                sf.corr(spark_df['DEPARTURE_DELAY'], spark_df['DAY_OF_WEEK']).alias('corr_delay2day_of_week')
                ) \
            .select(['ORIGIN_AIRPORT','avg_delay','min_delay','max_delay','corr_delay2day_of_week']) \
            .where(sf.col('max_delay') > 1000) \
            .orderBy(sf.col('max_delay').desc())

        return agg_table

    ###############################################################################################################

    def _run_task_four(
        self,
        flights_df: DataFrame,
        airlines_df: DataFrame,
        airports_df: DataFrame,
        session: SparkSession
    ) -> DataFrame:
        """Формирование таблицы для дашборда с отображением выполненных рейсов."""

        al2f_df = flights_df \
            .join(airlines_df, flights_df['AIRLINE'] == airlines_df['IATA_CODE'], how='left') \
            .drop(flights_df['AIRLINE']) \
            .select(['AIRLINE', 'TAIL_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT']) \
            .withColumnRenamed('AIRLINE', 'AIRLINE_NAME')

        ap2al_df = al2f_df \
            .join(airports_df, al2f_df['ORIGIN_AIRPORT'] == airports_df['IATA_CODE'], how='inner') \
            .select(['AIRLINE_NAME', 'TAIL_NUMBER', 'COUNTRY', 'AIRPORT', 'LATITUDE', 'LONGITUDE', 'DESTINATION_AIRPORT']) \
            .withColumnsRenamed(
                {
                    'COUNTRY': 'ORIGIN_COUNTRY',
                    'AIRPORT': 'ORIGIN_AIRPORT_NAME', 
                    'LATITUDE': 'ORIGIN_LATITUDE',
                    'LONGITUDE': 'ORIGIN_LONGITUDE',
                }
            )

        joined_table = ap2al_df \
            .join(airports_df, ap2al_df['DESTINATION_AIRPORT'] == airports_df['IATA_CODE'], how='inner') \
            .select(
                [
                    'AIRLINE_NAME', 'TAIL_NUMBER', 'ORIGIN_COUNTRY', 'ORIGIN_AIRPORT_NAME',
                    'ORIGIN_LATITUDE', 'ORIGIN_LONGITUDE', 'COUNTRY', 'AIRPORT', 'LATITUDE', 'LONGITUDE']
            ) \
            .withColumnsRenamed(
                {
                    'COUNTRY': 'DESTINATION_COUNTRY',
                    'AIRPORT': 'DESTINATION_AIRPORT_NAME', 
                    'LATITUDE': 'DESTINATION_LATITUDE',
                    'LONGITUDE': 'DESTINATION_LONGITUDE',
                }
            )

        return joined_table

    ################################################################################################################

    def _run_task_five(
        self,
        flights_df: DataFrame,
        airlines_df: DataFrame,
        session: SparkSession
    ) -> DataFrame:
        """Формирование таблицы со статистикой по компаниям о возникших проблемах."""

        joined_table = flights_df \
            .join(airlines_df, flights_df['AIRLINE'] == airlines_df['IATA_CODE'], how='left') \
            .drop(flights_df['AIRLINE']) \
            .withColumnRenamed('AIRLINE', 'AIRLINE_NAME') \
            .select(['AIRLINE_NAME', 'DIVERTED', 'CANCELLED', 'CANCELLATION_REASON', 'DISTANCE', 'AIR_TIME'])

        augmented_table = joined_table \
            .withColumn('CORRECT', sf.when(joined_table['CANCELLED'] == 1, 0).when(joined_table['DIVERTED'] == 1, 0).otherwise(1)) \
            .withColumn('AIRLINE_ISSUE', sf.when(joined_table['CANCELLATION_REASON'] == 'A', 1).otherwise(0)) \
            .withColumn('WEATHER_ISSUE', sf.when(joined_table['CANCELLATION_REASON'] == 'B', 1).otherwise(0)) \
            .withColumn('SYSTEM_ISSUE', sf.when(joined_table['CANCELLATION_REASON'] == 'C', 1).otherwise(0)) \
            .withColumn('SECYRITY_ISSUE', sf.when(joined_table['CANCELLATION_REASON'] == 'D', 1).otherwise(0))

        stats_df = augmented_table \
            .groupBy(augmented_table['AIRLINE_NAME']) \
            .agg(
                sf.sum(augmented_table['CORRECT']).alias('correct_count'),
                sf.sum(augmented_table['DIVERTED']).alias('diverted_count'),
                sf.sum(augmented_table['CANCELLED']).alias('cancelled_count'),
                sf.avg(augmented_table['DISTANCE']).alias('avg_distance'),
                sf.avg(augmented_table['AIR_TIME']).alias('avg_air_time'),
                sf.sum(augmented_table['AIRLINE_ISSUE']).alias('airline_issue_count'),
                sf.sum(augmented_table['WEATHER_ISSUE']).alias('weather_issue_count'),
                sf.sum(augmented_table['SYSTEM_ISSUE']).alias('nas_issue_count'),
                sf.sum(augmented_table['SECYRITY_ISSUE']).alias('security_issue_count'),
                ) \
            .orderBy(sf.col('AIRLINE_NAME'))

        return stats_df

    ################################################################################################################

    def run(self) -> None:
        """Запуск задачи на выполнение."""

        task_runner = self._run_task.get(self._task)
        spark_session = _create_spark_session()
        if self._task == 'task_4':
            flights_df = spark_session.read.parquet(str(self._input[0]))
            airlines_df = spark_session.read.parquet(str(self._input[1]))
            airports_df = spark_session.read.parquet(str(self._input[-1]))
            output_dataframe = task_runner(flights_df, airlines_df, airports_df, spark_session)
        if self._task == 'task_5':
            flights_df = spark_session.read.parquet(str(self._input[0]))
            airlines_df = spark_session.read.parquet(str(self._input[1]))
            output_dataframe = task_runner(flights_df, airlines_df, spark_session)
        else:
            input_dataframe = spark_session.read.parquet(self._input[0])
            output_dataframe = task_runner(input_dataframe, spark_session)
        output_dataframe.write.mode('overwrite').parquet(f'{self._output}')

####################################################################################################################

def main() -> None:
    """Главная функция запуска пайплайна преобразования."""

    parser = ArgumentParser()
    parser.add_argument(
        '--flights_path',
        type=str,
        default='flights.parquet',
        help='Please set "flights" dataset path with "parquet" extension.',
    )
    parser.add_argument(
        '--airlines_path',
        type=str,
        default='airlines.parquet',
        help='Please set "airlines" dataset path with "parquet" extension.',
    )
    parser.add_argument(
        '--airports_path',
        type=str,
        default='airports.parquet',
        help='Please set "airports" dataset path with "parquet" extension.',
    )
    parser.add_argument(
        '--result_path',
        type=str,
        default='result',
        help='Please set output "results" path.'
    )
    parser.add_argument(
        '--task',
        type=str,
        default='task_5',
        choices=['task_1', 'task_3', 'task_3', 'task_4', 'task_5'],
        help='Choose task to run.'
    )
    parsed_arguments = parser.parse_args()
    input_files = (
        Path(parsed_arguments.flights_path).resolve(),
        Path(parsed_arguments.airlines_path).resolve(),
        Path(parsed_arguments.airports_path).resolve(),
    )
    pyspark_job = PySparkProcessor(
        input_files_path=input_files,
        output_path=Path(parsed_arguments.result_path).resolve(),
        task=parsed_arguments.task
    )
    pyspark_job.run()

####################################################################################################################

if __name__ == '__main__':
    main()
