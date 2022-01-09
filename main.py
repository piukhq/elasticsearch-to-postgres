#!/usr/bin/env python3
"""
Syncs databases from one postgres to another
"""
import argparse
import datetime
import logging
import socket
import ssl

import dateutil.parser
import elasticsearch
import elasticsearch.helpers
import redis
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from pythonjsonlogger import jsonlogger
from sqlalchemy import Column, Date, Float, Integer, MetaData, String, Table, create_engine

from settings import settings

logger = logging.getLogger()
logHandler = logging.StreamHandler()
logFmt = jsonlogger.JsonFormatter(timestamp=True)
logHandler.setFormatter(logFmt)
logger.addHandler(logHandler)

dest_db_apistats = create_engine(settings.pg_connection_string.replace("/postgres?", "/apistats?"))
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


def is_leader():
    if settings.leader_election_enabled:
        r = redis.Redis.from_url(settings.redis_connection_string)
        lock_key = "elasticsearch-to-postgres-lock"
        hostname = socket.gethostname()
        is_leader = False

        with r.pipeline() as pipe:
            try:
                pipe.watch(lock_key)
                leader_host = pipe.get(lock_key)
                if leader_host in (hostname.encode(), None):
                    pipe.multi()
                    pipe.setex(lock_key, 10, hostname)
                    pipe.execute()
                    is_leader = True
            except redis.WatchError:
                pass
    else:
        is_leader = True
    return is_leader


def dump_es_api_stats() -> None:
    if not is_leader():
        return

    ctx = ssl.create_default_context(cafile="es_cacert.pem")
    ctx.check_hostname = False if settings.elasticsearch_host == "localhost" else True
    ctx.verify_mode = ssl.CERT_NONE if settings.elasticsearch_host == "localhost" else ssl.CERT_REQUIRED

    es = elasticsearch.Elasticsearch(
        [settings.elasticsearch_host],
        http_auth=(settings.elasticseasch_user, settings.elasticsearch_pass),
        scheme="https",
        ssl_context=ctx,
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
    logging.warning("Connecting to api stats db")
    with dest_db_apistats.connect() as conn:
        logging.warning("Creating tables")
        meta.create_all()
        logging.warning("Insterting results")

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
                logging.warning(f"Inserted {counter} values")

        logging.warning("Insterted results")


def main() -> None:
    logging.warning("Started postgres syncer")

    parser = argparse.ArgumentParser()
    parser.add_argument("--now", action="store_true", help="Run elasticsearch dump now")
    args = parser.parse_args()

    if args.now:
        dump_es_api_stats()
    else:
        scheduler = BlockingScheduler()
        scheduler.add_job(dump_es_api_stats, trigger=CronTrigger.from_crontab("0 1 * * *"))
        scheduler.start()


if __name__ == "__main__":
    main()
