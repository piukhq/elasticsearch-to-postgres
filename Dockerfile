FROM ghcr.io/binkhq/python:3.9

WORKDIR /app
ADD main.py .
ADD settings.py .
ADD Pipfile .
ADD Pipfile.lock .
ADD es_cacert.pem .

RUN pipenv install --system --deploy --ignore-pipfile

ENTRYPOINT [ "linkerd-await", "--" ]
CMD ["python", "main.py"]
