# Финансовый помощник по инструментам Московской Биржи (RAG-система)

Проект по созданию интеллектуального ассистента для ответов на вопросы о финансовых инструментах, доступных на Московской Бирже. Цель проекта — предоставить пользователям удобный и быстрый доступ к информации о акциях, облигациях, фондах и других активах с помощью RAG (Retrieval-Augmented Generation).

---

## 🎯 О проекте

Данный репозиторий содержит код и материалы для разработки сервиса, способного в режиме реального времени консультировать пользователей по вопросам, связанным с инструментами Московской Биржи. Система использует гибридный подход: поиск релевантных документов из векторной базы данных + генерация ответа с помощью языковой модели GigaChat.

---

## 📁 Структура проекта

```
masters-project-rag/
│
├── docker-compose.yaml              # Оркестрация всех сервисов
├── Dockerfile.mlflow                # Docker-образ для MLflow
├── .env.example                     # Шаблон переменных окружения
├── .gitignore
├── CHANGELOG.md
├── README.md                        # Документация проекта
│
├── src/                             # Основной код сервиса FastAPI
│   ├── Dockerfile                   # Docker-образ FastAPI-приложения
│   ├── main.py                      # Точка входа FastAPI + Telegram bot
│   ├── requirements.txt             # Python-зависимости
│   │
│   ├── config/
│   │   └── config.py                # Конфигурация приложения (pydantic-settings)
│   │
│   ├── domain/                      # Бизнес-логика (DDD)
│   │   └── query/
│   │       ├── query.py             # Модели Query и QueryResults
│   │       ├── delivery/
│   │       │   ├── controller.py    # REST API контроллеры (/forward, /history, /stats, /health, /feedback)
│   │       │   └── dto/
│   │       │       └── dto.py       # DTO для запросов/ответов API
│   │       └── usecase/
│   │           ├── i_query_usecase.py  # Интерфейс UseCase
│   │           └── query_usecase.py    # Реализация UseCase (поиск + генерация + feedback)
│   │
│   ├── infrastructure/              # Инфраструктурный слой
│   │   ├── telegram_bot.py          # Telegram-бот: ответы + inline-кнопки фидбека (👍/👎)
│   │   ├── db/
│   │   │   └── qdrand.py            # Работа с Qdrant (инициализация, вставка, эмбеддинги)
│   │   ├── di/
│   │   │   └── dependencies.py      # DI-контейнер (FastAPI Depends)
│   │   └── feedback/
│   │       └── feedback_storage.py  # Хранение фидбека для дообучения (triplet loss → JSONL)
│   │
│   └── tools/                       # Вспомогательные скрипты
│       ├── fill_qdrant.py           # Загрузка данных в Qdrant
│       ├── client.py                # Клиент для FastAPI (асинхронный HTTP)
│       ├── fine_tune_bi_encoder.py  # Дообучение bi-encoder на триплетах
│       └── metrics.py               # Расчёт метрик (Precision@k, Recall@k)
│
├── research/                        # Исследования и эксперименты
│   ├── requirements.txt             # Базовые зависимости для исследований
│   ├── requirements-mlflow-experiments.txt  # Зависимости для MLflow-экспериментов
│   ├── mlflow_setup.py              # Настройка подключения к MLflow
│   ├── create_bucket.py             # Скрипт создания bucket в MinIO
│   │
│   ├── baseline/                    # Базовые модели (baseline)
│   │   ├── bi_encoder.py            # Bi-encoder на BERT-like моделях
│   │   ├── BM25+Bi.ipynb            # Ноутбук: гибрид BM25 + Bi-encoder
│   │   ├── hybrid-retriever.py      # Гибридный ретривер
│   │   ├── rag-system.py            # RAG-система
│   │   └── README.md
│   │
│   ├── data-collection/             # Сбор данных
│   │   ├── q_a_articles.ipynb       # Парсинг статей с вопросами-ответами
│   │   ├── Q_A_articles.zip
│   │   ├── README.md
│   │   ├── additional-information/  # Дополнительные источники
│   │   ├── quantitative-data/       # Количественные данные
│   │   └── theoretical-texts/       # Теоретические тексты
│   │
│   ├── mlflow_experiments/          # Эксперименты с MLflow
│   │   ├── mlflow_config.py         # Конфигурация MLflow
│   │   ├── experiment_1_embedding_comparison.py   # Сравнение эмбеддингов
│   │   ├── experiment_2_chunking_strategies.py    # Стратегии чанкования
│   │   ├── experiment_3_chunk_overlap.py          # Перекрытие чанков
│   │   ├── run_all_experiments.py   # Запуск всех экспериментов
│   │   ├── fix_encoding.py          # Исправление кодировок
│   │   ├── README.md
│   │   └── utils/
│   │
│   └── test-outputs/                # Тестовые выходы
│       ├── test_client.py           # Тест клиента FastAPI
│       ├── test_cross_encoder.py    # Тест кросс-энкодера
│       ├── test_mocked_data.py      # Тест на мок-данных
│       └── test_real_data.py        # Тест на реальных данных
│
└── data/                            # Данные для загрузки в Qdrant
    ├── tbank_articles.json          # Статьи Т-Банка
    ├── theoretical_texts.json       # Теоретические тексты
    └── feedback/                    # Фидбек-данные для дообучения
        └── feedback.jsonl           # Триплеты (query, positive, negative)
```

---

## 🏗️ Архитектура сервисов

Проект состоит из **5 Docker-сервисов**, оркестрируемых через `docker-compose`:

| Сервис | Контейнер | Назначение |
|--------|-----------|------------|
| **app** | `fastapi_app` | FastAPI-приложение + Telegram-бот. RAG-пайплайн: эмбеддинг → поиск в Qdrant → кросс-энкодер → GigaChat |
| **qdrant** | `qdrant` | Векторная база данных для хранения и поиска эмбеддингов документов |
| **loader** | (на основе app) | Разовый скрипт для загрузки JSON-данных в Qdrant |
| **minio** | `minio` | S3-совместимое хранилище для артефактов MLflow |
| **minio-init** | `minio-init` | Инициализатор bucket'ов в MinIO (создаёт `mlflow-artifacts`) |
| **mlflow** | `mlflow` | MLflow Tracking Server для логирования экспериментов |

---

## 🔧 Технологический стек

| Компонент | Технология |
|-----------|------------|
| **Язык** | Python 3.11 |
| **Web-фреймворк** | FastAPI + Uvicorn |
| **База данных** | Qdrant (векторная БД) |
| **Эмбеддинги** | sentence-transformers (paraphrase-multilingual-MiniLM-L12-v2) |
| **Кросс-энкодер** | cross-encoder/mmarco-mMiniLMv2-L12-H384-v1 |
| **LLM** | GigaChat (Сбер) |
| **Telegram бот** | python-telegram-bot |
| **Эксперименты** | MLflow + MinIO (S3) |
| **Контейнеризация** | Docker, docker-compose |

---

## 🚀 Как запустить

### 1. Клонировать репозиторий

```bash
git clone git@github.com:bacchicsong/masters-project-rag.git
cd masters-project-rag
```

### 2. Создать файл `.env` из шаблона

```bash
cp .env.example .env
```

Заполнить в `.env` обязательные переменные:
- `GIGACHAT_AUTH_KEY` — API-ключ от GigaChat (base64 от client_id:secret)
- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота от @BotFather

### 3. Запустить все сервисы

```bash
docker-compose up --build
```

После запуска:
- **FastAPI** будет доступен по адресу: http://localhost:8088
- **Swagger UI**: http://localhost:8088/swagger
- **Qdrant**: http://localhost:6333
- **MinIO Console**: http://localhost:9001 (логин: `minioadmin`, пароль: `minioadmin`)
- **MLflow UI**: http://localhost:5000

### 4. Загрузить данные в Qdrant

При первом запуске контейнер `loader` автоматически загрузит данные из `data/` в Qdrant.
Для повторной загрузки:

```bash
docker-compose run --rm loader
```

---

## 📡 API Endpoints

Все эндпоинты доступны по префиксу `/api/v1`:

| Метод | Путь | Описание |
|-------|------|----------|
| `POST` | `/forward` | Отправить запрос в RAG-систему |
| `GET` | `/history` | Получить историю запросов |
| `GET` | `/stats` | Получить статистику по запросам |
| `GET` | `/health` | Проверка здоровья сервиса |
| `POST` | `/feedback` | Отправить фидбек (лайк/дизлайк). Тело: `{"query_id": "...", "liked": true/false}`. Сохраняет триплеты для дообучения |

---

## 🔄 Обратная связь и дообучение

### Как работает Feedback Loop

Система поддерживает цикл обратной связи, позволяющий улучшать качество поиска на основе оценок пользователей:

```
Пользователь → Оценка ответа (👍/👎) → Сохранение триплетов → Дообучение bi-encoder → Улучшение поиска
```

**Сбор фидбека:**
- В **Telegram-боте**: после каждого ответа бот показывает inline-кнопки «👍 Понравилось» / «👎 Не понравилось». Нажатие кнопки сохраняет триплет и показывает пользователю краткое подтверждение без технических деталей.
- Через **REST API**: эндпоинт `POST /api/v1/feedback` принимает JSON с `query_id` и флагом `liked`.

**Формирование триплетов:**
- При **лайке**: positive — верхний документ из reranked результатов, negative — документы с низким рейтингом из исходного поиска.
- При **дизлайке**: negative — верхние документы из reranked результатов, positive — документы из нижних позиций или указанные пользователем.
- Триплеты сохраняются в `data/feedback/feedback.jsonl` в формате JSONL.

**Дообучение bi-encoder:**
- Скрипт: `python -m src.tools.fine_tune_bi_encoder`
- Базовая модель: `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`
- Loss-функция: **CosineSimilarityLoss** (сближает positive-пары, отдаляет negative-пары)
- Минимальное количество триплетов для запуска обучения: **10**
- Эпохи: 3, batch size: 32, warmup: 100 шагов
- Результат: модель сохраняется в `models/fine_tuned_bi_encoder/`

**Активация дообученной модели:**
Для использования дообученной модели установите флаг в `src/infrastructure/db/qdrand.py`:

```python
USE_FINE_TUNED = True
```

Модель автоматически подхватится при следующем запуске сервиса. Для обновления поисковых эмбеддингов необходимо переиндексировать документы в Qdrant.

---

## 📊 MLflow эксперименты

Для запуска экспериментов с эмбеддингами, стратегиями чанкования и перекрытием:

```bash
# Установить зависимости для экспериментов
pip install -r research/requirements-mlflow-experiments.txt

# Запустить все эксперименты
cd research/mlflow_experiments
python run_all_experiments.py
```

Результаты будут доступны в MLflow UI по адресу http://localhost:5000.

---

## 👥 Команда проекта

- **Разработчики:**
  - [Охотин Даниил](https://t.me/danokil)
  - [Ващин Леонид](https://t.me/Leonid_Vaschin)
  - [Гусев Владислав](https://t.me/reverserepo)
- **Куратор:**
  - [Соборнов Тимофей](https://t.me/saintedts)

---

## 📜 Этапы работ

1. **Исследование и планирование** — постановка задачи, формирование плана
2. **Сбор данных и EDA** — парсинг статей, анализ данных
3. **Реализация бейзлайна** — BM25 + Bi-encoder, гибридный ретривер
4. **Улучшение модели** — кросс-энкодер, triplet-loss, эксперименты с MLflow
5. **Создание сервиса** — FastAPI + Telegram бот + Qdrant + GigaChat
6. **Продвинутые DL-модели** — эксперименты с современными архитектурами
7. **Доработка и обратная связь** — улучшение сервиса, документация, презентация