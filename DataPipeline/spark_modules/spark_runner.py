import sys
from pyspark.sql import DataFrame, SparkSession, functions as sf
from utils import EnvConfig
from uuid import uuid4

#################################################################################################

def _spark_connection(job_id: str, s3_key: str, s3_secret: str) -> SparkSession:
    """Создание соединения с PySpark."""

    prefix = uuid4().hex
    session = SparkSession \
        .builder \
        .appName(f'SparkJob_{job_id}_{prefix}') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2') \
        .config('spark.hadoop.fs.s3a.endpoint', 'https://hb.bizmrg.com') \
        .config('spark.hadoop.fs.s3a.region', 'ru-msk') \
        .config('spark.hadoop.fs.s3a.access.key', s3_key) \
        .config('spark.hadoop.fs.s3a.secret.key', s3_secret) \
        .getOrCreate()

    return session

#################################################################################################

class PySparkRunner:
    """Класс для агрегации датанных с помощью PySpark."""

    def __init__(self, config: EnvConfig, session: SparkSession) -> None:
        self._config = config
        self._spark = session
        self._run_task = {
            'lineitem': self._lineitem_processor,
            'orders': self._orders_processor,
            'customer': self._customer_processor,
            'supplier': self._supplier_processor,
            'part': self._part_processor,
        }

    ################################################################################################################

    def _lineitem_processor(self, spark: SparkSession) -> DataFrame:
        """Получение сводной сводной информации по позициям каждого заказа."""

        lineitem_df = spark.read.parquet(self._config.s3_read_path('lineitem')[0])
        lineitem_agg_table = lineitem_df \
            .groupBy('L_ORDERKEY') \
            .agg(
                sf.count('L_LINENUMBER').alias('count'),
                sf.sum('L_EXTENDEDPRICE').alias('sum_extendprice'),
                sf.avg('L_DISCOUNT').alias('mean_discount'),
                sf.avg('L_TAX').alias('mean_tax'),
                sf.avg(sf.datediff(sf.col('L_RECEIPTDATE'), sf.col('L_SHIPDATE'))).alias('delivery_days'),
                sf.sum(sf.when(sf.col('L_RETURNFLAG') == 'A', 1).otherwise(0)).alias('A_return_flags'),
                sf.sum(sf.when(sf.col('L_RETURNFLAG') == 'R', 1).otherwise(0)).alias('R_return_flags'),
                sf.sum(sf.when(sf.col('L_RETURNFLAG') == 'N', 1).otherwise(0)).alias('N_return_flags'),
            ) \
            .orderBy('L_ORDERKEY')

        return lineitem_agg_table

    ################################################################################################################

    def _orders_processor(self, spark: SparkSession) -> DataFrame:
        """Получение сводной таблицы по заказам в разрезе месяца."""

        orders_df = spark.read.parquet(self._config.s3_read_path('orders')[0])
        customer_df = spark.read.parquet(self._config.s3_read_path('orders')[1])
        nation_df = spark.read.parquet(self._config.s3_read_path('orders')[-1])

        joined_df = orders_df \
            .join(customer_df, customer_df.C_CUSTKEY == orders_df.O_CUSTKEY, how='left') \
            .join(nation_df, nation_df.N_NATIONKEY == customer_df.C_NATIONKEY, how='left')

        orders_agg_table = joined_df \
            .groupBy(
                sf.date_format('O_ORDERDATE', 'yyyy-MM').alias('O_MONTH'),
                sf.col('N_NAME'),
                sf.col('O_ORDERPRIORITY'),
            ) \
            .agg(
                sf.count('O_ORDERKEY').alias('orders_count'),
                sf.avg('O_TOTALPRICE').alias('avg_order_price'),
                sf.sum('O_TOTALPRICE').alias('sum_order_price'),
                sf.min('O_TOTALPRICE').alias('min_order_price'),
                sf.max('O_TOTALPRICE').alias('max_order_price'),
                sf.sum(sf.when(sf.col('O_ORDERSTATUS') == 'F', 1).otherwise(0)).alias('f_order_status'),
                sf.sum(sf.when(sf.col('O_ORDERSTATUS') == 'O', 1).otherwise(0)).alias('o_order_status'),
                sf.sum(sf.when(sf.col('O_ORDERSTATUS') == 'P', 1).otherwise(0)).alias('p_order_status'),
            ) \
            .orderBy('O_MONTH', 'N_NAME', 'O_ORDERPRIORITY')

        return orders_agg_table

    ################################################################################################################

    def _customer_processor(self, spark: SparkSession) -> DataFrame:
        """Получение сводной таблицы по заказам в разрезе страны, откуда
        был отправлен заказ, а также приоритету выполняемого заказа."""

        customer_df = spark.read.parquet(self._config.s3_read_path('customer')[0])
        nation_df = spark.read.parquet(self._config.s3_read_path('customer')[1])
        region_df = spark.read.parquet(self._config.s3_read_path('customer')[-1])

        joined_df = customer_df \
            .join(nation_df, customer_df.C_NATIONKEY == nation_df.N_NATIONKEY, how='left') \
            .join(region_df, region_df.R_REGIONKEY == nation_df.N_REGIONKEY, how='left')

        customer_agg_table = joined_df \
            .groupBy('R_NAME', 'N_NAME', 'C_MKTSEGMENT') \
            .agg(
                sf.countDistinct('C_CUSTKEY').alias('unique_customers_count'),
                sf.avg('C_ACCTBAL').alias('avg_acctbal'),
                sf.mean('C_ACCTBAL').alias('mean_acctbal'),
                sf.min('C_ACCTBAL').alias('min_acctbal'),
                sf.max('C_ACCTBAL').alias('max_acctbal'),
            ) \
            .orderBy('N_NAME', 'C_MKTSEGMENT')

        return customer_agg_table

    ###############################################################################################################

    def _supplier_processor(self, spark: SparkSession) -> DataFrame:
        """Получение отчет по данным о поставщиках, содержащий
        сводную информацию в разрезе страны и региона поставщика."""

        supplier_df = spark.read.parquet(self._config.s3_read_path('supplier')[0])
        nation_df = spark.read.parquet(self._config.s3_read_path('supplier')[1])
        region_df = spark.read.parquet(self._config.s3_read_path('supplier')[-1])

        joined_df = supplier_df \
            .join(nation_df, supplier_df.S_NATIONKEY == nation_df.N_NATIONKEY, how='left') \
            .join(region_df, region_df.R_REGIONKEY == nation_df.N_REGIONKEY, how='left')

        supplier_agg_table = joined_df \
            .groupBy('R_NAME', 'N_NAME') \
            .agg(
                sf.countDistinct('S_NAME').alias('unique_supplers_count'),
                sf.avg('S_ACCTBAL').alias('avg_acctbal'),
                sf.mean('S_ACCTBAL').alias('mean_acctbal'),
                sf.min('S_ACCTBAL').alias('min_acctbal'),
                sf.max('S_ACCTBAL').alias('max_acctbal'),
            ) \
            .orderBy('R_NAME', 'N_NAME')

        return supplier_agg_table

    ###############################################################################################################

    def _part_processor(self, spark: SparkSession) -> DataFrame:
        """Получение отчет по по данным о грузоперевозках, содержащий сводную
        информацию в разрезе страны поставки, типа поставки и типа контейнера."""

        part_df = spark.read.parquet(self._config.s3_read_path('part')[0])
        partsupp_df = spark.read.parquet(self._config.s3_read_path('part')[1])
        supplier_df = spark.read.parquet(self._config.s3_read_path('part')[2])
        nation_df = spark.read.parquet(self._config.s3_read_path('part')[-1])

        joined_df = part_df \
            .join(partsupp_df, part_df.P_PARTKEY == partsupp_df.PS_PARTKEY, how='inner') \
            .join(supplier_df, supplier_df.S_SUPPKEY == partsupp_df.PS_SUPPKEY, how='inner') \
            .join(nation_df, nation_df.N_NATIONKEY == supplier_df.S_NATIONKEY, how='inner')

        part_agg_table = joined_df \
            .groupBy('N_NAME', 'P_TYPE', 'P_CONTAINER') \
            .agg(
                sf.count('P_PARTKEY').alias('parts_count'),
                sf.avg('P_RETAILPRICE').alias('avg_retailprice'),
                sf.sum('P_SIZE').alias('size'),
                sf.mean('P_RETAILPRICE').alias('mean_retailprice'),
                sf.min('P_RETAILPRICE').alias('min_retailprice'),
                sf.max('P_RETAILPRICE').alias('max_retailprice'),
                sf.avg('PS_SUPPLYCOST').alias('avg_supplycost'),
                sf.mean('PS_SUPPLYCOST').alias('mean_supplycost'),
                sf.min('PS_SUPPLYCOST').alias('min_supplycost'),
                sf.max('PS_SUPPLYCOST').alias('max_supplycost')
            ) \
            .orderBy('N_NAME', 'P_TYPE', 'P_CONTAINER')

        return part_agg_table

    ###############################################################################################################

    def __call__(self, task_to_run: str) -> None:
        """Запуск задачи на выполнение."""

        spark_session = self._spark(
            job_id=task_to_run,
            s3_key=self._config.s3_key,
            s3_secret=self._config.s3_secret
        )
        execute_task = self._run_task.get(task_to_run)
        agg_table = execute_task(spark_session)
        agg_table.cache()
        agg_table.write.mode('overwrite').parquet(self._config.s3_write_path(task_to_run))
        agg_table.show(15)
        print(f'\nDataframe shape: {(agg_table.count(), len(agg_table.columns))}\n', flush=True)
        spark_session.stop()

####################################################################################################################

def main(args: list)  -> None:
    """Главная функция запуска PySpark."""

    try:
        data_processor = PySparkRunner(
            config=EnvConfig(),
            session=_spark_connection,
        )
        data_processor(args[1])
    except Exception as err:
        print(f'ERROR in data processing pipeline: {repr(err)}', flush=True)
        raise

####################################################################################################################

if __name__ == '__main__':
    main(sys.argv)
