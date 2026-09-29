# Use the Python 3.7 image as the base.
FROM python:alpine

# Install the 'pika' package to access RabbitMQ.
RUN pip install pika

# Set the environment variable to enable unbuffered mode for Python output.
ENV PYTHONUNBUFFERED=1

# Declare the working directory inside the container.
WORKDIR /usr/src/app/rabbitmqv1/producer/

# Copy the 'producer.py' file from the current directory into the container.
COPY producer.py .

# Define the command that will be executed when the container starts.
CMD [ "python", "producer.py" ]
