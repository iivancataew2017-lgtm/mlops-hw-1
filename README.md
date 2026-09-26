## Архитектура

1. **`interface` — Streamlit.** Читает загруженный CSV и отправляет каждую строку отдельным JSON-сообщением в `transactions`. Сохраняет `transaction_id` из файла или назначает UUID. На странице «Результаты» показывает данные из PostgreSQL.
2. **`fraud_detector` — скоринг.** Читает `transactions`, кодирует категории и применяет CatBoost. Пишет `transaction_id`, `score` и `fraud_flag` в `scores`.
3. **`kafka` и `kafka-setup` — обмен сообщениями.** Redpanda работает как Kafka-совместимый брокер. Одноразовый сервис `kafka-setup` создаёт топики при запуске.
4. **`db-writer` — запись результатов.** Отдельный потребитель топика `scores` сохраняет результаты в БД. При повторном ID обновляет существующую запись.
5. **`postgres` — хранение.** Таблица `transaction_scores` содержит ID транзакции, скор, флаг фрода и время записи. Все сервисы находятся в одной Docker-сети.

## Быстрый старт

### Требования

- Docker с поддержкой Linux-контейнеров и Docker Compose v2.
- Свободный порт `8000` для приложения.

Модель `fraud_detector/models/my_catboost.cbm` взята из проекта семинара. Вместе с ней в репе есть `preprocessing.json` со статистиками, заранее рассчитанными по `train.csv`.

### Запуск

```bash
git clone https://github.com/iivancataew2017-lgtm/mlops-hw-1.git
cd mlops-hw-1
docker compose up --build -d
docker compose ps
```

Приложение доступно по адресу [http://localhost:8000](http://localhost:8000).

Контейнер `kafka-setup` после создания топиков завершается с кодом 0 это норм :)

Дальше можно тестить так (в случае новых коммитов или еще как-то):

```bash
git pull
docker compose up --build -d
```

## Использование

### 1. Отправка транзакций

Откройте приложение, загрузите `sample_data/test.csv` и нажмите "Отправить".

Можно конечно и другой варик CSV, но формат должен быть такой:

```
transaction_time,amount,lat,lon,merchant_lat,merchant_lon,population_city,gender,merch,cat_id,one_city,us_state,jobs
2026-09-20 12:15:00,42.50,55.7558,37.6173,55.7600,37.6200,13000000,F,coffee_shop,food,1,MOW,engineer
```

### 2. Просмотр результатов

В боковом меню выберите "Результаты" и нажмите "Посмотреть результаты". На странице отображаются:

- 10 последних записей из PostgreSQL с `fraud_flag = 1`
- гистограмма скоров последних 100 транзакций

В топик `scores` поступают сообщения с тремя полями:

```json
{
  "transaction_id": "001",
  "score": 0.99,
  "fraud_flag": 1
}
```

### 3. Логи

Логи обработки:

```bash
docker compose logs --tail 100 fraud_detector db-writer
```

Некорректные сообщения пропускаются с записью причины в лог. Скорер подтверждает сообщение после отправки результата, а `db-writer` — после записи в БД.

## Структура проекта

```text
.
├── fraud_detector/
│   ├── app/app.py             # Чтение и запись Kafka
│   ├── src/preprocessing.py   # Подготовка признаков
│   ├── src/scorer.py          # Inference на CPU
│   ├── models/               # CatBoost и статистики препроцессинга
│   ├── requirements.txt
│   └── Dockerfile
├── interface/
│   ├── app.py                 # Отправка CSV
│   ├── pages/Результаты.py    # Таблица и гистограмма
│   ├── requirements.txt
│   └── Dockerfile
├── db_writer/
│   ├── app.py                 # Сохранение результатов в БД
│   ├── requirements.txt
│   └── Dockerfile
├── infra/postgres/init.sql    # Создание таблицы
├── sample_data/test.csv      # Пример транзакций
├── .env.example
├── docker-compose.yaml
└── README.md
```

## Настройки Kafka и PostgreSQL

- Топик `transactions` принимает входные транзакции, `scores` — результаты модели.
- У каждого топика 3 партиции и 1 реплика.
- Внутри Docker-сети брокер доступен как `kafka:9092`, PostgreSQL — как `postgres:5432`. Эти порты не публикуются на хосте.
- По умолчанию база, пользователь и пароль PostgreSQL — `fraud`, порог модели — `0.98`.

Для остановки проекта:

```bash
docker compose down
```

Данные сохраняются в volumes. Команда `docker compose down -v` удаляет их вместе с транзакциями и результатами.

## После словие

Вроде все ;)
