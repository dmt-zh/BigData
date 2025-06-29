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
        input_file_path: Path,
        output_path: Path,
        task: str,
    ) -> None:
        self._input = str(input_file_path)
        self._output = str(output_path)
        self._task = task
        self._run_task = {
            'task_1': self._run_task_one,
            'task_2': self._run_task_two,
            'task_3': self._run_task_three,
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

    ################################################################################################################

    def run(self) -> None:
        """Запуск задачи на выполнение."""

        spark_session = _create_spark_session()
        input_dataframe = spark_session.read.parquet(self._input)
        task_runner = self._run_task.get(self._task)
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
        '--result_path',
        type=str,
        default='result',
        help='Please set output "results" path.'
    )
    parser.add_argument(
        '--task',
        type=str,
        default='task_3',
        choices=['task_1', 'task_3', 'task_3', 'task_4', 'task_5'],
        help='Choose task to run.'
    )
    parsed_arguments = parser.parse_args()
    pyspark_job = PySparkProcessor(
        input_file_path=Path(parsed_arguments.flights_path).resolve(),
        output_path=Path(parsed_arguments.result_path).resolve(),
        task=parsed_arguments.task
    )
    pyspark_job.run()

####################################################################################################################

if __name__ == '__main__':
    main()
