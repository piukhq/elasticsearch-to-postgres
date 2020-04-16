FROM python:3.7
WORKDIR /app
ADD main.py /app
ADD pyproject.toml /app
ADD poetry.lock /app

RUN apt update && apt -y install postgresql-client && \
    apt clean && rm -rf /var/lib/apt/lists

RUN pip --no-cache-dir install poetry && poetry install --no-root --no-dev
