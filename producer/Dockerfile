From python:3.11-slim

WORKDIR /app
COPY producer.py .
COPY input.json .

RUN pip install kafka-python

CMD ["python", "producer.py"]