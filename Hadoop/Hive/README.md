# Практическое задание: Hive

1. Создать БД yellow_taxi.
2. Зарегистрируйте таблицу taxi_data.
3. Подключитесь к вашему hive c NameNode
4. Выполните SQL запрос, а результаты выгрузите в файл формата result.csv (разделитель - запятая)
```sql
select 
  vendor_id, 
  avg(passenger_count) as avg_passenger_count, 
  min(trip_distance) as min_trip_distance, 
  max(trip_distance) as max_trip_distance, 
  avg(total_amount) as avg_total_amount 
from taxi_data
where vendor_id is not null and vendor_id <> ''
group by vendor_id;
```