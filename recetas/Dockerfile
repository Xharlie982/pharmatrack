FROM python:3.11-slim
# instala wget para healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
ENV MYSQL_URL=""
EXPOSE 8083
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8083"]
