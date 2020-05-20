#!/usr/bin/env python3
"""
Syncs databases from one postgres to another
"""
import argparse
import datetime
import os
import logging
import re
import subprocess
import ssl
import sys
from typing import cast, List, Dict, Any

import elasticsearch
import psycopg2
import pylogrus
from sqlalchemy import create_engine, Table, Column, String, MetaData, Date, JSON
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
ES_HOST = os.environ.get("ES_HOST", "elasticsearch.uksouth.bink.sh")
if DEST_DB_PASSWORD is not None:
    DEST_DB_PASSWORD = DEST_DB_PASSWORD.strip()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
DB_SYNC_TIMEOUT = int(os.environ.get("DB_SYNC_TIMEOUT", "3600"))
ACCEPTABLE_DB_REGEX = re.compile(r"[a-z]+")

KILL_CONN_SQL = """SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE pid <> pg_backend_pid()
AND datname IN ({0});"""

DROP_DB_SQL = """DROP DATABASE IF EXISTS {0};"""
CREATE_DB_SQL = """CREATE DATABASE {0};"""


logger = cast(pylogrus.PyLogrus, logging.getLogger("postgres-syncer"))

dest_db_apistats = create_engine(f"postgres://{DEST_DB_USER}:{DEST_DB_PASSWORD}@{DEST_DB_HOST}:{DEST_DB_PORT}/apistats")
meta = MetaData(dest_db_apistats)
stats_table = Table("api_stats", meta, Column("date", Date, primary_key=True), Column("api_data", JSON))


def kick_users(cursor) -> None:
    logger.info("Kicking any active users")
    # Dodgy in clause, should really use sqlalchmey
    databases = ",".join([f"'{x}'" for x in SOURCE_DBS])
    sql = KILL_CONN_SQL.format(databases)
    # print(sql)
    cursor.execute(sql)
    logger.info("Kicked users")


def drop_create_db(cursor, dbname: str) -> None:
    logger.withFields({"dbname": dbname}).info("Dropping and creating database")
    cursor.execute(DROP_DB_SQL.format(dbname))
    cursor.execute(CREATE_DB_SQL.format(dbname))
    logger.withFields({"dbname": dbname}).info(f"Dropped and created database")


def sync_data(dbname: str, timeout: int = DB_SYNC_TIMEOUT) -> None:
    logger.withFields({"dbname": dbname}).info("Starting database sync")
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
            logger.withFields({"dbname": dbname}).info("Finished database sync")
        else:
            logger.withFields({"dbname": dbname}).error("Failed database sync")
    except subprocess.TimeoutExpired:
        logger.withFields({"dbname": dbname}).error("Database sync timed out")
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


def dump_es_api_stats() -> None:
    ctx = ssl.create_default_context(cafile="es_cacert.pem")
    ctx.check_hostname = False if ES_HOST == "localhost" else True
    ctx.verify_mode = ssl.CERT_NONE if ES_HOST == "localhost" else ssl.CERT_REQUIRED

    es = elasticsearch.Elasticsearch(
        [ES_HOST], http_auth=("starbug", "PPwu7*Cq%H2JOEj2lE@O3423vVSNgybd"), scheme="https", ssl_context=ctx
    )

    now = datetime.datetime.utcnow()
    yesterday = (now - datetime.timedelta(days=1)).date()

    kube_cluster = "sandbox.uksouth.bink.sh"
    start_date = yesterday.strftime("%Y-%m-%dT00:00:00.000Z")
    end_date = now.strftime("%Y-%m-%dT00:00:00.000Z")

    data = es.search(
        index="nginx-*",
        body={
            "size": 0,
            "query": {
                "bool": {
                    "must": [],
                    "filter": [
                        {
                            "bool": {
                                "should": [{"match": {"kubernetes.cluster": kube_cluster}}],
                                "minimum_should_match": 1,
                            }
                        },
                        {
                            "range": {
                                "@timestamp": {"gte": start_date, "lt": end_date, "format": "strict_date_optional_time"}
                            }
                        },
                    ],
                    "should": [],
                    "must_not": [],
                }
            },
            "aggs": {
                "by_uri": {
                    "terms": {"field": "nginx.path.keyword"},
                    "aggs": {
                        "by_method": {
                            "terms": {"field": "nginx.method.keyword"},
                            "aggs": {
                                "stats_response_time": {"stats": {"field": "nginx.upstream_response_time"}},
                                "percentile_response_time": {
                                    "percentiles": {"field": "nginx.upstream_response_time", "percents": [95, 99]}
                                },
                                "response_distribution": {"terms": {"field": "nginx.response.keyword"}},
                                "availability_date_histogram": {
                                    "date_histogram": {"field": "@timestamp", "fixed_interval": "1m"},
                                    "aggs": {"response_count": {"terms": {"field": "nginx.response.keyword"}}},
                                },
                            },
                        }
                    },
                }
            },
        },
    )

    def get_500_count(buckets: List[Dict[str, Any]]) -> int:
        other = 0
        _5xx_count = 0
        for item in buckets:
            if item["key"].startswith("5"):
                _5xx_count += item["doc_count"]
            else:
                other += item["doc_count"]
        # If we have valid results getting returned in this 1m bucket, then the api is working
        return 0 if other else _5xx_count

    def window_response_agg(buckets: List[Dict[str, Any]]) -> float:
        total = len(buckets)
        items = [get_500_count(item["response_count"]["buckets"]) for item in buckets]

        # Any 5minute sliding window with
        sla_breach = [int(all(items[i : i + 5])) for i in range(0, total - 5)]
        availability = (((total - 5) - sum(sla_breach)) / (total - 5)) * 100
        return availability

    result = []
    aggs = data["aggregations"]["by_uri"]["buckets"]
    for uri_bucket in aggs:
        url = uri_bucket["key"]
        for method_bucket in uri_bucket["by_method"]["buckets"]:
            method = method_bucket["key"]

            result.append(
                {
                    "key": f"{method}_{url}",
                    "url": url,
                    "method": method,
                    "total_hits": method_bucket["doc_count"],
                    "max_response_time": method_bucket["stats_response_time"]["max"],
                    "min_response_time": method_bucket["stats_response_time"]["min"],
                    "sum_response_time": method_bucket["stats_response_time"]["sum"],
                    "avg_response_time": method_bucket["stats_response_time"]["sum"] / method_bucket["doc_count"],
                    "95_response_time": method_bucket["percentile_response_time"]["values"]["95.0"],
                    "99_response_time": method_bucket["percentile_response_time"]["values"]["99.0"],
                    "availability": window_response_agg(method_bucket["availability_date_histogram"]["buckets"]),
                }
            )

    # Attempt to make tabes
    logger.info("Connecting to api stats db")
    with dest_db_apistats.connect() as conn:
        logger.info("Creating tables")
        meta.create_all()
        logger.info("Insterting results")
        stmt = stats_table.insert().values(date=yesterday, api_data=result)
        conn.execute(stmt)
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

    logger.withFields(
        {"host": SOURCE_DB_HOST, "port": SOURCE_DB_PORT, "user": SOURCE_DB_USER, "databases": SOURCE_DBS}
    ).info("Source DB Info")
    logger.withFields({"host": DEST_DB_HOST, "port": DEST_DB_PORT, "user": DEST_DB_USER}).info("Destination DB Info")

    if args.now:
        dump_tables()
    elif args.es:
        dump_es_api_stats()
    else:
        scheduler = BlockingScheduler()
        scheduler.add_job(dump_tables, trigger=CronTrigger.from_crontab("0 2 * * *"))
        scheduler.add_job(dump_es_api_stats, trigger=CronTrigger.from_crontab("0 1 * * *"))
        scheduler.start()


if __name__ == "__main__":
    main()
