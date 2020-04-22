#!/usr/bin/env python3
"""
Syncs databases from one postgres to another
"""
import argparse
import os
import logging
import re
import subprocess
import sys
from typing import cast

import psycopg2
import pylogrus
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.blocking import BlockingScheduler

logging.setLoggerClass(pylogrus.PyLogrus)

SOURCE_DB_HOST = os.environ["SOURCE_DB_HOST"]
SOURCE_DB_PORT = int(os.environ.get("SOURCE_DB_PORT", "5432"))
SOURCE_DB_USER = os.environ.get("SOURCE_DB_USER", "postgres")
SOURCE_DBS = os.environ["SOURCE_DBS"].split(",")
DEST_DB_HOST = os.environ["DEST_DB_HOST"]
DEST_DB_PORT = int(os.environ.get("DEST_DB_PORT", "5432"))
DEST_DB_USER = os.environ.get("DEST_DB_USER", "postgres")
DEST_DB_PASSWORD = os.environ.get("DEST_DB_PASSWORD")

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
DB_SYNC_TIMEOUT = int(os.environ.get("DB_SYNC_TIMEOUT", "3600"))
ACCEPTABLE_DB_REGEX = re.compile(r"[a-z]+")

KILL_CONN_SQL = """SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE pid <> pg_backend_pid()
AND datname IN ({0});"""

DROP_DB_SQL = """DROP DATABASE IF EXISTS {0};"""
CREATE_DB_SQL = """CREATE DATABASE {0};"""


logger = cast(pylogrus.PyLogrus, logging.getLogger('postgres-syncer'))


def kick_users(cursor) -> None:
    logger.info("Kicking any active users")
    # Dodgy in clause, should really use sqlalchmey
    databases = ",".join([f"'{x}'" for x in SOURCE_DBS])
    sql = KILL_CONN_SQL.format(databases)
    # print(sql)
    cursor.execute(sql)
    logger.info("Kicked users")


def drop_create_db(cursor, dbname: str) -> None:
    logger.withFields({'dbname': dbname}).info("Dropping and creating database")
    cursor.execute(DROP_DB_SQL.format(dbname))
    cursor.execute(CREATE_DB_SQL.format(dbname))
    logger.withFields({'dbname': dbname}).info(f"Dropped and created database")


def sync_data(dbname: str, timeout: int = DB_SYNC_TIMEOUT) -> None:
    logger.withFields({'dbname': dbname}).info("Starting database sync")
    p1 = subprocess.Popen(
        (
            "pg_dump",
            "--create",
            "--clean",
            "-F",
            "custom",
            f"host={SOURCE_DB_HOST} port={SOURCE_DB_PORT} dbname={dbname} user={SOURCE_DB_USER}",
        ),
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    )
    p2 = subprocess.Popen(
        ("pg_restore", "-h", DEST_DB_HOST, "-p", str(DEST_DB_PORT), "-d", dbname, "-U", DEST_DB_USER),
        stdout=sys.stdout,
        stderr=sys.stderr,
        stdin=p1.stdout,
    )
    try:
        ret_code = p2.wait(timeout)
        if ret_code == 0:
            logger.withFields({'dbname': dbname}).info("Finished database sync")
        else:
            logger.withFields({'dbname': dbname}).error("Failed database sync")
    except subprocess.TimeoutExpired:
        logger.withFields({'dbname': dbname}).error("Database sync timed out")
        p2.terminate()
        try:
            p2.wait(2)
        except subprocess.TimeoutExpired:
            p2.kill()


def dump_tables() -> None:
    logger.info("Syncing databases")

    with psycopg2.connect(f"host={DEST_DB_HOST} user={DEST_DB_USER} dbname=postgres port={DEST_DB_PORT}") as conn:
        conn.autocommit = True

        with conn.cursor() as cur:
            kick_users(cur)

            for db in SOURCE_DBS:
                drop_create_db(cur, db)
                sync_data(db)

    logger.info("Finished syncing databases")


def main() -> None:
    logger.setLevel(LOG_LEVEL)
    formatter = pylogrus.TextFormatter(datefmt='Z', colorize=False)
    # formatter = pylogrus.JsonFormatter()  # Can switch to json if needed
    ch = logging.StreamHandler()
    ch.setLevel(LOG_LEVEL)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.info('Started postgres syncer')

    parser = argparse.ArgumentParser()
    parser.add_argument("--now", action="store_true", help="Run database sync now")
    args = parser.parse_args()

    if SOURCE_DB_HOST == DEST_DB_HOST and SOURCE_DB_PORT == DEST_DB_PORT:
        logger.critical("Cant sync data to the same place")
        sys.exit(1)
    if SOURCE_DBS == [""]:
        logger.critical("DBs must be provided")
        sys.exit(1)
    for db in SOURCE_DBS:
        if not ACCEPTABLE_DB_REGEX.match(db):
            logger.withFields({'dbname': db}).critical(f"DB not an acceptable db name")
            sys.exit(1)

    if DEST_DB_PASSWORD:
        filename = os.path.expanduser("~/.pgpass")
        with open(filename, "w") as fp:
            fp.write(f"{DEST_DB_HOST}:{DEST_DB_PORT}:*:{DEST_DB_USER}:{DEST_DB_PASSWORD}\n")
        os.chmod(filename, 0o0600)

    logger.withFields({'host': SOURCE_DB_HOST, 'port': SOURCE_DB_PORT, 'user': SOURCE_DB_USER, 'databases': SOURCE_DBS}).info('Source DB Info')
    logger.withFields({'host': DEST_DB_HOST, 'port': DEST_DB_PORT, 'user': DEST_DB_USER}).info('Destination DB Info')

    if args.now:
        dump_tables()
    else:
        scheduler = BlockingScheduler()
        scheduler.add_job(dump_tables, trigger=CronTrigger.from_crontab("0 2 * * *"))
        scheduler.start()


if __name__ == "__main__":
    main()
