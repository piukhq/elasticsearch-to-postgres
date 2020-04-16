FROM python:3.7

WORKDIR /app
ADD main.py /app
ADD Pipfile /app
ADD Pipfile.lock /app

RUN apt update && apt -y install postgresql-client && \
    apt clean && rm -rf /var/lib/apt/lists

RUN pip --no-cache-dir install pipenv && pipenv install --system --deploy
