FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install clickhouse_driver

COPY . .

RUN ls -la /app  # Для проверки наличия файлов

CMD ["bash", "/entrypoint.sh"]  # Запускаем entrypoint.sh





