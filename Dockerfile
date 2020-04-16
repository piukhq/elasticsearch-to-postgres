FROM python:3.8

WORKDIR /app
ADD main.py /app/main.py

RUN apt update && apt -y install postgresql-client && \
    apt clean && rm -rf /var/lib/apt/lists

RUN pip --no-cache-dir install pipenv && pipenv install --system --deploy
