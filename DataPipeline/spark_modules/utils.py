#################################################################################################

class EnvConfig:
    """Класс для формирования переменных окружения."""

    def __init__(self) -> None:
        self._s3_key = '<your_key>'
        self._s3_secret = '<your_secret'
        self._base_read_s3_path = 's3a://de-raw'
        self._base_write_s3_path = 's3a://de-project/d-zhigalo-18'
        self._read_tasks = {
            'lineitem': ['lineitem'],
            'orders': ['orders', 'customer', 'nation'],
            'customer': ['customer', 'nation', 'region'],
            'supplier': ['supplier', 'nation', 'region'],
            'part': ['part', 'partsupp', 'supplier', 'nation'],
        }
        self._write_tasks = {
            'lineitem': 'lineitems_report',
            'orders': 'orders_report',
            'customer': 'customers_report',
            'supplier': 'suppliers_report',
            'part': 'parts_report',
        }

    #############################################################################################

    def s3_read_path(self, task_id: str):
        """Формирование пути для чтения данных в S3."""

        dataset_names = self._read_tasks.get(task_id, [task_id])
        dataset_paths = []
        for dataset in dataset_names:
            dataset_paths.append(f'{self._base_read_s3_path}/{dataset}')
        return dataset_paths

    #############################################################################################

    def s3_write_path(self, task_id: str) -> str:
        """Формирование пути для записи данных в S3."""

        prefix = self._write_tasks.get(task_id)
        return f'{self._base_write_s3_path}/{prefix}'

    #############################################################################################

    @property
    def s3_key(self) -> str:
        return self._s3_key

    #############################################################################################

    @property
    def s3_secret(self) -> str:
        return self._s3_secret

#################################################################################################
