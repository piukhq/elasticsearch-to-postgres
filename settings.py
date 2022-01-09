from typing import Optional

from pydantic import BaseSettings, PostgresDsn
from pydantic.networks import RedisDsn


class Settings(BaseSettings):
    pg_connection_string: PostgresDsn
    leader_election_enabled: bool = False
    redis_connection_string: Optional[RedisDsn]
    elasticsearch_host: str = "elasticsearch.uksouth.bink.host"
    elasticseasch_user: str = "starbug"
    elasticsearch_pass: str = "PPwu7*Cq%H2JOEj2lE@O3423vVSNgybd"


settings = Settings()
