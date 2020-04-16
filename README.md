# Postgres Database Syncer

Sync's one or more postgres databases between two database servers using `pg_dump | pg_restore` on a schedule at 2AM every day.

You can run this and pass `--now` to skip the schedule and run the sync immediately.

## Env vars:

| Name              | Required | Default  | Type   | Description                                                          |
|-------------------|:--------:|----------|--------|----------------------------------------------------------------------|
| `SOURCE_DBS`      | ✔        |          | string | Comma separated list of values e.g. `hermes,hades,harmonia`          |
| `SOURCE_DB_HOST`  | ✔        |          | string | Host / IP of the database to copy data from                          |
| `SOURCE_DB_PORT`  |          | 5432     | number | Port of the database to copy data from                               |
| `SOURCE_DB_USER`  |          | postgres | string | User of the database to copy data from                               |
| `DEST_DB_HOST`    | ✔        |          | string | Host / IP of the database to copy data to                            |
| `DEST_DB_PORT`    |          | 5432     | number | Port of the database to copy data to                                 |
| `DEST_DB_USER`    |          | postgres | string | User of the database to copy data to                                 |
| `DB_SYNC_TIMEOUT` |          | 3600     | number | How long to wait before declaring the <code>pg_dump &#124; pg_restore</code> stalled |

## Local testing

1. Install dependencies
   ```bash
   poetry install
   ```
1. Dump some data from something that looks like one of the databases in question, e.g.
   ```bash
   kubectl port-forward hermes-api-5b56bc4649-52d5k 5432 &
   pg_dump --create --clean -F custom "host=127.0.0.1 port=5432 dbname=hermes user=laadmin@bink-prod-uksouth" > hermes.dump
   kill %1
   ```
1. Run postgres containers, setup role, import data
   ```bash
   docker run --name some-postgres -p 1234:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
   docker run --name some-postgres2 -p 1235:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
   cat <<EOF>>~/.pgpass
   127.0.0.1:1234:*:postgres:mysecretpassword
   127.0.0.1:1235:*:postgres:mysecretpassword
   EOF
   psql "host=127.0.0.1 port=1234 dbname=postgres user=postgres password=mysecretpassword" -c 'CREATE ROLE laadmin'
   psql "host=127.0.0.1 port=1234 dbname=postgres user=postgres password=mysecretpassword" -c 'CREATE DATABASE hermes'
   cat hermes.dump | pg_restore -h 127.0.0.1 -p 1234 -U postgres -d hermes
   ```
1. Run script
   ```bash
   export SOURCE_DBS='hermes'
   export SOURCE_DB_HOST='127.0.0.1'
   export SOURCE_DB_PORT='1234'
   export DEST_DB_HOST='127.0.0.1'
   export DEST_DB_PORT='1235'
   pipenv run main.py --now
   ```

