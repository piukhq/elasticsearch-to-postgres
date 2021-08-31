FROM ghcr.io/binkhq/python:3.9
WORKDIR /app
ADD main.py /app
ADD pyproject.toml /app
ADD poetry.lock /app
ADD es_cacert.pem /app

RUN apt-get update && apt-get -y install postgresql-client && \
    apt-get clean && rm -rf /var/lib/apt/lists

RUN pip --no-cache-dir install poetry psycopg2-binary && \
    poetry config virtualenvs.create false && \
    poetry install --no-root --no-dev

CMD ["python", "main.py"]
