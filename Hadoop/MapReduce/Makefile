# Копирование из локального окружения на hadoop кластер
to_hadoop_cluster:
	rsync -av --progress ./* ubuntu@89.169.180.108:/home/ubuntu/MapReduce/

copy_mapreduse_result:
	rsync -av --progress ubuntu@89.169.180.108:/home/ubuntu/MapReduce/result.csv ./