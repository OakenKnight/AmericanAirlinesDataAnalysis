FROM python:3.7-slim

WORKDIR /usr/src/asvsp_project

COPY . .
RUN pip install --no-cache-dir -r req.txt

ENV KAFKA_BROKER="localhost:9092"

