FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends wget \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV AWS_REGION=us-east-1 \
    ATHENA_DB=pharmatrack_raw \
    ATHENA_OUTPUT=s3://resultados-athena-g6/ \
    CORS_ORIGINS="*"

EXPOSE 8086
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8086","--proxy-headers"]
