from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql import functions as psf
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
        }

    ################################################################################################################

    def _run_task_one(self, session: SparkSession) -> None:
        """Получение сводной таблицы по топ 10 рейсов по коду рейса."""

        flights_df = session.read.parquet(self._input)
        agg_table = flights_df \
            .where(flights_df['TAIL_NUMBER'].isNotNull()) \
            .groupBy(flights_df['TAIL_NUMBER']) \
            .agg(psf.count(flights_df['FLIGHT_NUMBER']).alias('count')) \
            .select(
                psf.col('TAIL_NUMBER'),
                psf.col('count'),
                ) \
            .orderBy(psf.col('count').desc()) \
            .limit(10)

        agg_table.write.mode('overwrite').parquet(f'{str(self._output)}')

    ################################################################################################################

    def _run_task_two(self, session: SparkSession) -> None:
        """Получение сводной таблицы по топ 10 авиамаршрутов по
        наибольшему числу рейсов, а так же среднее время в полете."""

        flights_df = session.read.parquet(self._input)
        agg_table = flights_df \
            .groupBy(
                flights_df['ORIGIN_AIRPORT'],
                flights_df['DESTINATION_AIRPORT'],
                ) \
            .agg(
                psf.count(flights_df['TAIL_NUMBER']).alias('tail_count'),
                psf.avg(flights_df['AIR_TIME']).alias('avg_air_time'),
                ) \
            .select(
                psf.col('ORIGIN_AIRPORT'),
                psf.col('DESTINATION_AIRPORT'),
                psf.col('tail_count'),
                psf.col('avg_air_time'),
                ) \
            .orderBy(psf.col('tail_count').desc()) \
            .limit(10)
        
        agg_table.show()
        agg_table.write.mode('overwrite').parquet(f'{self._output}')

    ################################################################################################################

    def run(self) -> None:
        """Запуск задачи на выполнение."""

        spark_session = _create_spark_session()
        task_runner = self._run_task.get(self._task)
        task_runner(spark_session)

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
        default='task_2',
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
