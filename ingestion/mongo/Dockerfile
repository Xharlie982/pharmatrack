FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingesta_mongo.py .
CMD ["python", "ingesta_mongo.py"]