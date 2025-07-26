## Разворачиваем удаленный MLflow  сервер для трекинга ML экспериментов

<img src="https://github.com/dmt-zh/BigData/blob/main/MLflow/static/mlflow_server.jpg"/>


### Подготовка локальной машины

#### Предварительные требования

Для начала работы нужны:
- Ubuntu 24.04 LTS
- python версии 3.12.x

#### Настройка окружения:

Обновляем систему и устанавливаем необходимые пакеты:
```sh
apt-get update -y;

sudo apt -q -y install gcc swig cmake libpq-dev curl
gcc --version
swig -version
cmake -version
```

Устанавливаем пакетный менеджер [uv](https://docs.astral.sh/uv/getting-started/installation/):
```shell
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Выйти из терминала и зайти повторно. Скопировать файлы из этого репозитория, перейти в директорию, в которую будут скачаны файлы и распложен файл `pyproject.toml` и выполнить следующую команду:
```sh
uv sync
```

Создать файл `.env`, в котором определить переменные из файла `.env.example`:
```yml
POSTGRES_USER=<имя_пользователя>
POSTGRES_PASSWORD=<пароль>
POSTGRES_DB=<имя_базы_данных>
...
```

### Подготовка удаленного сервера

Устанавливаем docker и docker-compose:
```sh
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do sudo apt-get remove $pkg; done
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update -y;

sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y;
```

Запуск docker не из-под суперпользователя:
```sh
sudo groupadd docker;
sudo usermod -aG docker $USER;
```

Выйти из терминала терминала удаленного сервера и залогинится повторно. Копируем на удаленный сервер следующие файлы:
```
.gitignore
.env
Dockerfile
docker-compose.yaml
pyproject.toml
```

Запускаем MLflow приложение командой:
```
docker compose up -d
```

Чтобы убедится, что MLflow сервер успешно запустился можно посмотреть логи в контейнере:
```sh
docker logs mlflow-server --tail 8
```

После того как сервер запуститься, сменим пароль для доступа к UI MLflow, поскольку при создании сервера задается дефолтный пароль `password1234`. Cменить пароль в MLflow можно с помощью следующей команды:
```bash
curl -X PATCH "http://89.169.179.173:5050/api/2.0/mlflow/users/update-password" \
  -H "Content-Type: application/json" \
  -u "admin:password1234" \
  -d '{"username": "admin", "password": "Ve-MAKo2vmWCI-LtR02"}'
```

<img src="https://github.com/dmt-zh/BigData/blob/main/MLflow/static/mlflow_auth.jpg"/>

### Запуск трекинга ML экспериментов

В файле `.env` необходимо определить переменную `MLFLOW_S3_ENDPOINT_URL` — это ip адрес s3 хранилища с портом 9000, например:
```yml
MLFLOW_S3_ENDPOINT_URL=http://89.169.184.28:9000
```

На локальной машине необходимо сохранить [конфигурацию доступа](https://mlflow.org/docs/3.0.0rc2/auth#using-credentials-file) к удаленному MLflow серверу по пути `~/.mlflow/credentials` (директорию предварительно нужно создать)
<img src="https://github.com/dmt-zh/BigData/blob/main/MLflow/static/mlflow_creds.jpg"/>


Добавляем скрипту `experiment.py` права на исполнение:
```sh
chmod +x experiment.py
```

Запускаем тестовый эксперимент командой:
```sh
./experiment.py
```

На удаленном сервере, в UI MLflow можно убедиться, что эксперимент выполнен успешно:
<img src="https://github.com/dmt-zh/BigData/blob/main/MLflow/static/mlflow_ui.jpg"/>

