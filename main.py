#!/usr/bin/env python3
"""
Syncs databases from one postgres to another
"""
import argparse
import datetime
import os
import logging
import re
import redis
import subprocess
import ssl
import sys
import socket
import time
import requests
from typing import cast

import dateutil.parser
import elasticsearch
import psycopg2
import pylogrus
import elasticsearch.helpers
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    String,
    MetaData,
    Date,
    Integer,
    Float,
    Boolean,
    DateTime,
)
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.blocking import BlockingScheduler

logging.setLoggerClass(pylogrus.PyLogrus)

# Leader Election
redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")

SOURCE_DB_HOST = os.environ["SOURCE_DB_HOST"]
SOURCE_DB_PORT = int(os.environ.get("SOURCE_DB_PORT", "5432"))
SOURCE_DBS = os.environ["SOURCE_DBS"].split(",")
DEST_DB_HOST = os.environ["DEST_DB_HOST"]
DEST_DB_PORT = int(os.environ.get("DEST_DB_PORT", "5432"))
DEST_DB_USER = os.environ.get("DEST_DB_USER", "postgres")
DEST_DB_PASSWORD = os.environ.get("DEST_DB_PASSWORD")
ES_HOST = os.environ.get("ES_HOST", "elasticsearch.uksouth.bink.host")
if DEST_DB_PASSWORD is not None:
    DEST_DB_PASSWORD = DEST_DB_PASSWORD.strip()


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
DB_SYNC_TIMEOUT = int(os.environ.get("DB_SYNC_TIMEOUT", "21600"))
ACCEPTABLE_DB_REGEX = re.compile(r"[a-z]+")

KILL_CONN_SQL = """SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE pid <> pg_backend_pid()
AND datname IN ({0});"""

DROP_DB_SQL = """DROP DATABASE IF EXISTS {0};"""
CREATE_DB_SQL = """CREATE DATABASE {0};"""
REMOVE_HASHES_SQL = """UPDATE payment_card_paymentcardaccount SET hash = NULL;"""


logger = cast(pylogrus.PyLogrus, logging.getLogger("postgres-syncer"))

dest_db_apistats = create_engine(
    f"postgresql://{DEST_DB_USER}:{DEST_DB_PASSWORD}@{DEST_DB_HOST}:{DEST_DB_PORT}/apistats"
)
meta = MetaData(dest_db_apistats)
stats_table = Table(
    "api_stats_v2",
    meta,
    Column("id", String, primary_key=True),
    Column("date", Date),
    Column("method", String),
    Column("host", String),
    Column("path", String),
    Column("status", Integer),
    Column("response_time", Float),
    Column("user_agent", String),
)
dd_stats_table = Table(
    "dd_api_stats_v2",
    meta,
    Column("id", String, primary_key=True),
    Column("date", DateTime),
    Column("url", String),
    Column("error", Boolean),
    Column("total_time", Float),
)


def is_leader():
    r = redis.Redis.from_url(redis_url)
    lock_key = "harmonia-reporter-lock"
    hostname = socket.gethostname()
    is_leader = False

    with r.pipeline() as pipe:
        try:
            pipe.watch(lock_key)
            leader_host = pipe.get(lock_key)
            if leader_host in (hostname.encode(), None):
                pipe.multi()
                pipe.setex(lock_key, 60, hostname)
                pipe.execute()
                is_leader = True
        except redis.WatchError:
            pass
    return is_leader


def teams_notify(message):
    """
    Sends `message` to the 'Prototype' channel on Microsoft Teams.
    """
    TEAMS_WEBHOOK_URL = "https://hellobink.webhook.office.com/webhookb2/bf220ac8-d509-474f-a568-148982784d19@a6e2367a-92ea-4e5a-b565-723830bcc095/IncomingWebhook/edc46bc8aa4948428302736a978cc819/bba71e03-172e-4d07-8ee4-aad029d9031d"  # noqa: E501
    template = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "1A1F71",
        "summary": "A database sync has failed",
        "Sections": [
            {
                "activityTitle": "Database Sync Error",
                "facts": [{"name": "Message", "value": message},],
                "markdown": False,
            }
        ],
    }
    return requests.post(TEAMS_WEBHOOK_URL, json=template)


def kick_users(cursor) -> None:
    logger.info("Kicking any active users")
    # Dodgy in clause, should really use sqlalchmey
    databases = ",".join([f"'{x.split('|')[0]}'" for x in SOURCE_DBS])
    sql = KILL_CONN_SQL.format(databases)
    # print(sql)
    cursor.execute(sql)
    logger.info("Kicked users")


def drop_hashes(cursor) -> None:
    logger.info("Removing hashes")
    # print(sql)
    cursor.execute(REMOVE_HASHES_SQL)
    logger.info("Kicked users")


def drop_create_db(cursor, dbname: str) -> None:
    logger.withFields({"dbname": dbname}).info("Dropping and creating database")
    cursor.execute(DROP_DB_SQL.format(dbname))
    cursor.execute(CREATE_DB_SQL.format(dbname))
    logger.withFields({"dbname": dbname}).info(f"Dropped and created database")


def sync_data(dbname: str, dbuser: str, timeout: int = DB_SYNC_TIMEOUT) -> bool:
    logger.withFields({"dbname": dbname}).info("Starting database sync")
    sync_command = [
        "pg_dump",
        "--create",
        "--clean",
        "-F",
        "custom",
        f"host={SOURCE_DB_HOST} port={SOURCE_DB_PORT} dbname={dbname} user={dbuser}",
    ]
    if dbname == "atlas":
        sync_command = [
            "pg_dump",
            "--create",
            "--clean",
            "--table",
            "transactions_*",
            "-F",
            "custom",
            f"host={SOURCE_DB_HOST} port={SOURCE_DB_PORT} dbname={dbname} user={dbuser}",
        ]

    p1 = subprocess.Popen(sync_command, stdout=subprocess.PIPE, stderr=sys.stderr,)
    p2 = subprocess.Popen(
        (
            "pg_restore",
            "-h",
            DEST_DB_HOST,
            "-p",
            str(DEST_DB_PORT),
            "-d",
            dbname,
            "-U",
            DEST_DB_USER,
            "--no-owner",
            "--no-privileges",
        ),
        stdout=sys.stdout,
        stderr=sys.stderr,
        stdin=p1.stdout,
    )
    try:
        ret_code = p2.wait(timeout)
        if ret_code == 0:
            logger.withFields({"dbname": dbname}).info("Finished database sync")
            return True
        else:
            logger.withFields({"dbname": dbname}).error("Failed database sync")
            teams_notify(dbname + " database has failed to sync")
    except subprocess.TimeoutExpired:
        logger.withFields({"dbname": dbname}).error("Database sync timed out")
        teams_notify(dbname + " database sync has timed out")
        p2.terminate()
        try:
            p2.wait(2)
        except subprocess.TimeoutExpired:
            p2.kill()

    logger.info("Retrying in 5s")
    time.sleep(5)

    return False


def dump_tables() -> None:
    if is_leader():
        logger.info("Syncing databases")

        conn = None
        try:
            # psycopg2 changed how it works, so using a with resources statement automatically starts a transaction
            conn = psycopg2.connect(f"host={DEST_DB_HOST} user={DEST_DB_USER} dbname=postgres port={DEST_DB_PORT}")
            conn.autocommit = True

            for db in SOURCE_DBS:
                db, dbuser = db.split("|", 1)
                attempt = 0
                while attempt < 5:
                    with conn.cursor() as cur:
                        kick_users(cur)
                    with conn.cursor() as cur:
                        drop_create_db(cur, db)
                    if sync_data(db, dbuser) and attempt > 0:
                        teams_notify(db + "database sync succeeded on retry")
                        break
                    elif sync_data(db, dbuser):
                        break
                    attempt += 1
                    time.sleep(5)
        finally:
            if conn:
                conn.close()

        with psycopg2.connect(f"host={DEST_DB_HOST} user={DEST_DB_USER} dbname=hermes port={DEST_DB_PORT}") as conn:
            with conn.cursor() as cur:
                drop_hashes(cur)

        logger.info("Finished syncing databases")


def dump_dd_stats() -> None:
    if is_leader():
        es = elasticsearch.Elasticsearch(["starbug-elasticsearch"])
        # es = elasticsearch.Elasticsearch(["localhost"])

        now = datetime.datetime.utcnow()
        yesterday = (now - datetime.timedelta(days=1)).date()

        start_date = yesterday.strftime("%Y-%m-%dT00:00:00.000Z")
        end_date = now.strftime("%Y-%m-%dT00:00:00.000Z")

        index_scan = elasticsearch.helpers.scan(
            es,
            index="synthetics",
            query={
                "query": {
                    "bool": {
                        "filter": {
                            "range": {
                                "timestamp": {"gte": start_date, "lt": end_date, "format": "strict_date_optional_time",}
                            }
                        },
                    }
                }
            },
        )

        # Attempt to make tabes
        logger.info("Connecting to dd stats db")
        with dest_db_apistats.connect() as conn:
            logger.info("Creating tables")
            meta.create_all()
            logger.info("Insterting results")
            counter = 0
            for item in index_scan:
                data = item["_source"]

                try:
                    result = {
                        "id": item["_id"],
                        "date": dateutil.parser.parse(data["timestamp"]),
                        "url": data["url"],
                        "error": False if not data["error"] else True,
                        "total_time": data["total"],
                    }
                except Exception:
                    continue

                stmt = dd_stats_table.insert().values(**result)
                try:
                    conn.execute(stmt)
                except Exception:
                    pass
                counter += 1

                if counter % 100 == 0:
                    logger.info(f"Inserted {counter} values")

            logger.info("Insterted results")


def dump_es_api_stats() -> None:
    if is_leader():
        ctx = ssl.create_default_context(cafile="es_cacert.pem")
        ctx.check_hostname = False if ES_HOST == "localhost" else True
        ctx.verify_mode = ssl.CERT_NONE if ES_HOST == "localhost" else ssl.CERT_REQUIRED

        es = elasticsearch.Elasticsearch(
            [ES_HOST], http_auth=("starbug", "PPwu7*Cq%H2JOEj2lE@O3423vVSNgybd"), scheme="https", ssl_context=ctx,
        )

        now = datetime.datetime.utcnow()
        yesterday = (now - datetime.timedelta(days=1)).date()

        kube_cluster = "prod"
        start_date = yesterday.strftime("%Y-%m-%dT00:00:00.000Z")
        end_date = now.strftime("%Y-%m-%dT00:00:00.000Z")

        index_scan = elasticsearch.helpers.scan(
            es,
            index="nginx-*",
            query={
                "query": {
                    "bool": {
                        "must": [],
                        "filter": [
                            {
                                "bool": {
                                    "should": [
                                        {"match": {"kubernetes.env.keyword": kube_cluster}},
                                        {"match": {"nginx.http_user_agent": "Apache"}},
                                    ],
                                    "minimum_should_match": 2,
                                }
                            },
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": start_date,
                                        "lt": end_date,
                                        "format": "strict_date_optional_time",
                                    }
                                }
                            },
                        ],
                        "should": [],
                        "must_not": [],
                    }
                }
            },
        )

        # Attempt to make tabes
        logger.info("Connecting to api stats db")
        with dest_db_apistats.connect() as conn:
            logger.info("Creating tables")
            meta.create_all()
            logger.info("Insterting results")

            counter = 0
            for item in index_scan:
                data = item["_source"]

                try:
                    result = {
                        "id": item["_id"],
                        "date": dateutil.parser.parse(data["@timestamp"]),
                        "method": data["nginx"]["method"],
                        "host": data["nginx"]["vhost"],
                        "path": data["nginx"]["path"],
                        "status": data["nginx"]["status"],
                        "response_time": data["nginx"]["upstream_response_time"],
                        "user_agent": data["nginx"]["http_user_agent"],
                    }
                except Exception:
                    continue

                stmt = stats_table.insert().values(**result)
                try:
                    conn.execute(stmt)
                except Exception:
                    pass
                counter += 1

                if counter % 100 == 0:
                    logger.info(f"Inserted {counter} values")

            logger.info("Insterted results")


def main() -> None:
    logger.setLevel(LOG_LEVEL)
    formatter = pylogrus.TextFormatter(datefmt="Z", colorize=False)
    # formatter = pylogrus.JsonFormatter()  # Can switch to json if needed
    ch = logging.StreamHandler()
    ch.setLevel(LOG_LEVEL)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.info("Started postgres syncer")

    parser = argparse.ArgumentParser()
    parser.add_argument("--now", action="store_true", help="Run database sync now")
    parser.add_argument("--es", action="store_true", help="Run elasticsearch dump now")
    parser.add_argument("--es-dd", action="store_true", help="Run elasticsearch dump now")
    args = parser.parse_args()

    if SOURCE_DB_HOST == DEST_DB_HOST and SOURCE_DB_PORT == DEST_DB_PORT:
        logger.critical("Cant sync data to the same place")
        sys.exit(1)
    if SOURCE_DBS == [""]:
        logger.critical("DBs must be provided")
        sys.exit(1)
    for db in SOURCE_DBS:
        if not ACCEPTABLE_DB_REGEX.match(db):
            logger.withFields({"dbname": db}).critical(f"DB not an acceptable db name")
            sys.exit(1)

    if DEST_DB_PASSWORD:
        filename = os.path.expanduser("~/.pgpass")
        with open(filename, "w") as fp:
            fp.write(f"{DEST_DB_HOST}:{DEST_DB_PORT}:*:{DEST_DB_USER}:{DEST_DB_PASSWORD}\n")
        os.chmod(filename, 0o0600)

    logger.withFields({"host": SOURCE_DB_HOST, "port": SOURCE_DB_PORT, "databases": SOURCE_DBS}).info("Source DB Info")
    logger.withFields({"host": DEST_DB_HOST, "port": DEST_DB_PORT, "user": DEST_DB_USER}).info("Destination DB Info")

    if args.now:
        dump_tables()
    elif args.es:
        dump_es_api_stats()
    elif args.es_dd:
        dump_dd_stats()
    else:
        scheduler = BlockingScheduler()
        scheduler.add_job(dump_tables, trigger=CronTrigger.from_crontab("0 4 * * *"))
        scheduler.add_job(dump_es_api_stats, trigger=CronTrigger.from_crontab("0 1 * * *"))
        scheduler.add_job(dump_dd_stats, trigger=CronTrigger.from_crontab("0 3 * * *"))
        scheduler.start()


if __name__ == "__main__":
    main()
