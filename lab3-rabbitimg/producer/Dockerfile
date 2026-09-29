FROM python:3.9-slim

RUN pip install pika Pillow

ENV PYTHONUNBUFFERED=1

WORKDIR /usr/src/app

COPY *.py .

CMD ["python", "producer.py"]