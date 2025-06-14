
# Практическое задание: MapReduce

---

1. Скопировать данные желтого такси за 2020 год в hadoop кластер.
2. Написать map-reduce приложение, использующее скопированные данные и вычисляющее отчет на каждый месяц 2020 года вида:

| **Month** | **Payment type** | **Tips average amount** |
| --------- | ---------------- | ----------------------- |
| 2020-01   | Credit card      | 999.99                  |

---
## Требования к отчету:

1. Количество файлов — 1
2. Формат — csv
3. Сортировка — не требуется
4. `Month` считаем по полю `tpep_pickup_datetime`
5. `Payment type` считаем по полю `payment_type`  
6. `Tips average amount` вычисляем по полю `tip_amount`  
7. Обратите внимание, в датасете будут присутствовать неверные данные  (не тот год, пустой тип оплаты) — эти данные нужно будет отсеять.
8. Маппинг типов оплат: `mapping = { 1: 'Credit card', 2: 'Cash', 3: 'No charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided trip'}`

---
## Решение:

Подключаемся к MASTERNODE:
```sh
ssh ubuntu@158.160.30.87
```

Создаем директорию на кластере:
```sh
hadoop fs -mkdir 2020
```

Копируем данные из S3 хранилища на MASTERNODE:
```sh
aws --profile=karpov-user --endpoint-url=https://storage.yandexcloud.net s3 cp --recursive s3://ny-taxi-data/ny-taxi/ ./2020
```

Переносим скопированные файлы на кластер в директорию `2020`:
```sh
hadoop fs -put /ubuntu/2020 2020
```

Создаем директорию для сохранения результатов обработки на кластере:
```sh
hadoop fs -mkdir processed-data
```

Копируем из локального окружения на MASTERNODE задание для MapReduce:
```sh
make to_hadoop_cluster 
```

На MASTERNODE запускаем задание для MapReduce:
```sh
cd /home/ubuntu/MapReduce/ && chmod +x run.sh && ./run.sh
```
