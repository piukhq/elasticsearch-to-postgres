FROM binkhq/python:3.8
WORKDIR /app
ADD main.py /app
ADD pyproject.toml /app
ADD poetry.lock /app
ADD es_cacert.pem /app

RUN apt update && apt -y install postgresql-client && \
    apt clean && rm -rf /var/lib/apt/lists

RUN pip --no-cache-dir install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-root --no-dev

CMD ["python", "main.py"]
