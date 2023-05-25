from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable

import requests
import sqlalchemy as sa

from datagovindia import config, db
from datagovindia import metadata as meta
from datagovindia.logger import log
from datagovindia.metadata import ResourceMetadata

config.DATAGOVINDIA_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def generate_swagger_client(index_name: str, lang: str = "python") -> dict:
    """NOTE: this doesn't work yet."""

    url = f"https://data.gov.in/backend/dataapi/v1/swagger/{index_name}"
    log.info(f"Generating API for {url}")
    payload = {"swaggerUrl": url}
    headers = {"Content-Type": "application/json"}
    r = requests.post(
        f"https://generator.swagger.io/api/gen/clients/{lang}",
        json=payload,
        headers=headers,
    )

    return r.json()


def get_resource(
    index_name: str,
    api_key: str | None = None,
    format: str = "json",
    offset: int | None = None,
    limit: int | None = None,
    filters: dict[str, str] | None = None,
    fields: list[str] | None = None,
) -> dict:
    """Get data from the API"""

    api_key = api_key or config.DATAGOVINDIA_API_KEY

    if api_key is None:
        log.warn(f"Using sample API key with limitations")
        api_key = config.DATAGOVINDIA_SAMPLE_API_KEY

    url = f"https://api.data.gov.in/resource/{index_name}?api-key={api_key}&format={format}"

    if offset is not None:
        url += f"&offset={offset}"

    if limit is not None:
        url += f"&limit={limit}"

    if filters is not None:
        for k, v in filters.items():
            url += f"&filter[{k}]={v}"

    if fields is not None:
        url += f"&fields={','.join(fields)}"

    log.info(f"Getting data from {url}")
    r = requests.get(url)
    return r.json()


@dataclass
class ResourceApi:
    resource: ResourceMetadata
    total: int
    api_key: str | None = config.DATAGOVINDIA_API_KEY

    def __post_init__(self):
        if self.api_key is None:
            log.warn(f"Using sample API key with limitations")
            self.api_key = config.DATAGOVINDIA_SAMPLE_API_KEY

    @classmethod
    def from_index_name(cls, index_name: str, api_key: str | None = None):
        data = get_resource(index_name=index_name, api_key=api_key, limit=0, fields=[])
        resource = ResourceMetadata.from_raw(data)
        total = data["total"]

        return cls(resource=resource, total=total, api_key=api_key)

    def generate_swagger_client(self, lang: str = "python") -> dict:
        return generate_swagger_client(index_name=self.resource.index_name, lang=lang)

    def stream_records(
        self,
        offset: int = 0,
        batch_size: int = 10,
        filters: dict[str, str] | None = None,
        fields: list[str] | None = None,
    ) -> Iterable[dict]:
        results = []
        while True:
            results = get_resource(
                index_name=self.resource.index_name,
                api_key=self.api_key,
                offset=offset,
                limit=batch_size,
                filters=filters,
                fields=fields,
            )["records"]
            offset += batch_size

            if not results:
                break

            for result in results:
                yield result


@dataclass
class DataGovIndia:
    conn: sa.Connection = field(default_factory=db.engine.connect)
    api_key: str | None = None
    auto_refresh: bool = True
    refresh_interval: timedelta = field(default_factory=lambda: timedelta(hours=1))
    last_refreshed_at: datetime | None = None

    def __post_init__(self):
        self.api_key = self.api_key or config.DATAGOVINDIA_API_KEY
        if self.api_key is None:
            log.warn(f"Using sample API key with limitations")
            self.api_key = config.DATAGOVINDIA_SAMPLE_API_KEY

        if self.auto_refresh:
            if db.path.exists():
                self.refresh()
            else:
                self.refresh(full=True)
            self.last_refreshed_at = datetime.now()

    def refresh(self, full=False):
        """Refresh local metadata db."""
        meta.refresh(self.conn, full=full)
        self.last_refreshed_at = datetime.now()

    def should_refresh(self) -> bool:
        if self.last_refreshed_at is None:
            return True
        return datetime.now() - self.last_refreshed_at > self.refresh_interval

    def refresh_on_interval(self):
        if self.should_refresh():
            self.refresh()

    def list_orgtypes(self) -> list[str]:
        if self.auto_refresh:
            self.refresh_on_interval()
        return meta.list_org_types(self.conn)

    def list_orgnames(self) -> list[str]:
        if self.auto_refresh:
            self.refresh_on_interval()
        return meta.list_orgs(self.conn)

    def list_sectors(self) -> list[str]:
        if self.auto_refresh:
            self.refresh_on_interval()
        return meta.list_sectors(self.conn)

    def list_sources(self) -> list[str]:
        if self.auto_refresh:
            self.refresh_on_interval()
        return meta.list_sources(self.conn)

    def list_all_attributes(self) -> dict:
        if self.auto_refresh:
            self.refresh_on_interval()
        return meta.list_all_attributes(self.conn)

    def list_recently_updated(
        self, days=7, max_results=10, fields: list[str] | None = None
    ) -> list[dict]:
        if self.auto_refresh:
            self.refresh_on_interval()
        return meta.list_recently_updated(
            self.conn, days=days, max_results=max_results, fields=fields
        )

    def list_recently_created(
        self, days=7, max_results=10, fields: list[str] | None = None
    ) -> list[dict]:
        if self.auto_refresh:
            self.refresh_on_interval()
        return meta.list_recently_created(
            self.conn, days=days, max_results=max_results, fields=fields
        )

    def search(
        self,
        title=None,
        desc=None,
        org=None,
        org_type=None,
        sector=None,
        source=None,
        max_results=10,
        fields: list[str] | None = None,
    ) -> list[dict]:
        if self.auto_refresh:
            self.refresh_on_interval()
        return meta.search(
            self.conn,
            title=title,
            desc=desc,
            org=org,
            org_type=org_type,
            sector=sector,
            source=source,
            max_results=max_results,
            fields=fields,
        )

    def get_resource_api(self, index_name: str) -> ResourceApi:
        if self.auto_refresh:
            self.refresh_on_interval()

        return ResourceApi.from_index_name(index_name=index_name, api_key=self.api_key)

    def __del__(self):
        self.conn.close()
