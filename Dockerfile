FROM ghcr.io/binkhq/python:3.9

WORKDIR /app
ADD main.py .
ADD Pipfile .
ADD Pipfile.lock .
ADD es_cacert.pem .

RUN apt-get update && apt-get -y install postgresql-client nano && \
    apt-get clean && rm -rf /var/lib/apt/lists && \
    pipenv install --system --deploy --ignore-pipfile

ENTRYPOINT [ "linkerd-await", "--" ]
CMD ["python", "main.py"]
