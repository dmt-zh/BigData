## Стриминговый сервис

<img src="https://github.com/dmt-zh/BigData/blob/main/Kafka/static/stream-pipeline.jpg"/>

<hr>

### Техническая инфраструктура

В качестве технической инфраструктуры будут использованы следующие инструменты^
- Озеро данных на базе S3
- Брокер сообщений Kafka
- Стриминговая обработка с помощью PySpark

<hr>

### Данные и их структура

Будем использовать данные о поездках такси Нью-Йоркского такси за 2020г.
[Описание данных](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

Схема данных:
<img src="https://github.com/dmt-zh/BigData/blob/main/Kafka/static/er_diagram.jpg"/>

<hr>

### Предварительные требования

Для начала работы нужны:
- Ubuntu 24.04 LTS
- python версии 3.12.x

<hr>

### Подготовка и настройка окружения

```sh
chmod +x environment.sh && ./environment.sh
```

### Запуск приложения

```sh
./run.sh 
```

### Посмотреть что данные поступают в Kafka можно с помощью комманд:

```sh
kcat -b localhost:29092 -t taxi -с 5
kcat -b localhost:29092 -t report -с 5
```
<img src="https://github.com/dmt-zh/BigData/blob/main/Kafka/static/kcat_log.jpg"/>
