# Use the official Python image as the base image
FROM python:3.10

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update -y 
RUN useradd -m -d /home/tfidif_er -s /bin/bash tfidif_er


# Install Python dependencies
COPY ./requirements.txt /main/requirements.txt
RUN pip install --upgrade pip && pip install -r /main/requirements.txt
COPY ./.env main/.env

# Create and set the working directory in the container
WORKDIR /main

USER tfidif_er
ENV REDIS_HOST=redis-stack
# Copy the mainlication files into the container
COPY ./main/commands/ /main/commands/
COPY ./utils/ /main/utils/
COPY ./main.py /main/main.py
CMD ["python", "/main/main.py"]
