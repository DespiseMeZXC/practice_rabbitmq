# Простой пример работы с RabbitMQ 

## Запуск сервиса RabbitMQ

```bash
docker compose up -d
```

## Создание виртуального окружения

```bash
python -m venv .venv
```

## Установка зависимостей

```bash
pip install -r requirements.txt
```

## Активация виртуального окружения

### Linux/MacOS

```bash
source .venv/bin/activate
```

### Windows

```bash
.venv\Scripts\activate
```

## Запуск консьюмера

```bash
python consumer.py
```

## Запуск продюсера

```bash
python producer.py
```
