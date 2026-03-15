FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y default-jre && apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "ai_async_producer.py"]