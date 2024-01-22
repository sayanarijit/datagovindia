import json
import multiprocessing
import re
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Literal

import requests
import sqlalchemy as sa
from tqdm import tqdm

from datagovindia import db
from datagovindia.db import Resources, ResourcesFTS
from datagovindia.logger import log


@dataclass
class ResourceMetadata:
    index_name: str
    title: str
    desc: str
    org: list[str]
    org_type: str
    source: str
    sector: list[str]
    field: list[str]
    created: datetime | None
    updated: datetime | None

    @classmethod
    def from_raw(cls, raw: dict):
        return ResourceMetadata(
            index_name=raw["index_name"],
            title=clean_text(raw.get("title", "")),
            desc=clean_text(raw.get("desc", "")),
            created=get_timestamp(raw.get("created", "")),
            updated=get_timestamp(raw.get("updated", "")),
            org=list({clean_text(o) for o in raw.get("org", [])}),
            org_type=raw.get("org_type", "").lower(),
            source=raw.get("source", "data.gov.in").lower(),
            sector=list({clean_text(s) for s in raw.get("sector", [])}),
            field=list({r["id"] for r in raw.get("field", [])}),
        )


@dataclass
class FetchedBatch:
    resources: list[ResourceMetadata]

    @classmethod
    def from_raw_records(cls, raw: list[dict]):
        resources = []

        for r in raw:
            index_name = r["index_name"]
            if len(index_name) != 36:
                log.warn(f"Invalid index_name {repr(index_name)}, skipping...")
                continue

            rec = ResourceMetadata.from_raw(r)
            resources.append(rec)

        return cls(resources=resources)

    @classmethod
    def new(
        cls,
        batch: tuple[int, int],
        sort_by: Literal["created"] | Literal["updated"],
        sort_order: Literal["desc"] | Literal["asc"],
    ):
        offset, limit = batch
        url = gen_url(offset, limit, sort_by=sort_by, sort_order=sort_order)
        log.info(f"Fetching {url}")
        resp = fetch(url)
        try:
            recs = resp.json()["records"]
            return cls.from_raw_records(recs)
        except Exception as e:
            url = resp.request.url
            text = resp.text
            log.warn(f"Failed to parse response for url {url}: {e}: {text}")
            return cls(resources=[])

    @classmethod
    def recently_created(cls, batch: tuple[int, int]):
        return cls.new(batch, sort_by="created", sort_order="desc")

    @classmethod
    def recently_updated(cls, batch: tuple[int, int]):
        return cls.new(batch, sort_by="updated", sort_order="desc")


def gen_url(
    offset: int,
    limit: int,
    format: Literal["json", "csv", "xml"] = "json",
    sort_by: Literal["updated"] | Literal["created"] = "updated",
    sort_order: Literal["desc"] | Literal["asc"] = "desc",
) -> str:
    return f"https://api.data.gov.in/lists?format={format}&notfilters[source]=visualize.data.gov.in&filters[active]=1&sort[{sort_by}]={sort_order}&offset={offset}&limit={limit}"


def fetch(url: str, timeout: int = 30) -> requests.Response:
    return requests.get(url, timeout=(timeout, timeout + 15))


def clean_text(text):
    text = re.sub(" +", " ", text).strip()
    return text


def get_timestamp(t: int | str) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(t))
    except Exception:
        pass

    try:
        return datetime.fromtimestamp(int(t) / 1000)
    except Exception:
        log.warn(f"Invalid timestamp: {repr(t)}")
        return None


def list_org_types(conn: sa.Connection) -> list[str]:
    q = sa.select(Resources.org_type).distinct()
    result = conn.execute(q).fetchall()
    return [r for r, in result]


def list_orgs(conn: sa.Connection) -> list[str]:
    q = sa.select(Resources.org).distinct()
    results = set()
    for (r,) in conn.execute(q).fetchall():
        results.update(r)

    return list(results)


def list_sectors(conn: sa.Connection) -> list[str]:
    q = sa.select(Resources.sector).distinct()
    results = set()
    for (r,) in conn.execute(q).fetchall():
        results.update(r)

    return list(results)


def list_sources(conn: sa.Connection) -> list[str]:
    q = sa.select(Resources.source).distinct()
    result = conn.execute(q).fetchall()
    return [r for r, in result]


def list_all_attributes(conn: sa.Connection) -> dict:
    return {
        "sectors": list_sectors(conn),
        "sources": list_sources(conn),
        "org_types": list_org_types(conn),
        "orgs": list_orgs(conn),
    }


def list_recently_updated(
    conn: sa.Connection,
    days=7,
    max_results=10,
    fields: list[str] | None = None,
) -> list[dict]:
    if not fields:
        fields = Resources.Table.columns.keys()

    cols = [c for c in Resources.Table.columns if c.name in fields]

    now = datetime.now()
    past = now - timedelta(days=days)

    q = (
        sa.select(*cols)
        .where(Resources.updated.between(past, now))
        .order_by(Resources.updated.desc())
        .limit(max_results)
    )

    results = conn.execute(q).mappings()
    return [{f: r[f] for f in fields} for r in results]


def list_recently_created(
    conn: sa.Connection, days=7, max_results=10, fields: list[str] | None = None
) -> list[dict]:
    if not fields:
        fields = Resources.Table.columns.keys()
    cols = [c for c in Resources.Table.columns if c.name in fields]

    now = datetime.now()
    past = now - timedelta(days=days)

    q = (
        sa.select(*cols)
        .where(Resources.created.between(past, now))
        .order_by(Resources.created.desc())
        .limit(max_results)
    )

    results = conn.execute(q).mappings()
    return [{f: r[f] for f in fields} for r in results]


def search(
    conn: sa.Connection,
    title: str | None = None,
    desc: str | None = None,
    org: str | None = None,
    org_type: str | None = None,
    sector: str | None = None,
    source: str | None = None,
    max_results: int = 10,
    fields: list[str] | None = None,
) -> list[dict]:
    if not fields:
        fields = Resources.Table.columns.keys()
    cols = [c for c in Resources.Table.columns if c.name in fields]
    where = []

    if title:
        where.append(ResourcesFTS.title.match(title))
    if desc:
        where.append(ResourcesFTS.desc.match(desc))
    if org:
        where.append(ResourcesFTS.org.match(org))
    if org_type:
        where.append(ResourcesFTS.org_type.match(org_type))
    if sector:
        where.append(ResourcesFTS.sector.match(sector))
    if source:
        where.append(ResourcesFTS.source.match(source))

    q = (
        sa.select(ResourcesFTS.index_name)
        .where(sa.and_(*where))
        .order_by(ResourcesFTS.rank)
        .limit(max_results)
    )

    ids = [r for r, in conn.execute(q).fetchall()]
    if not ids:
        return []

    q = sa.select(*cols).where(Resources.index_name.in_(ids))
    results = conn.execute(q).mappings()

    return [{f: r[f] for f in fields} for r in results]


def get_resource_info(
    conn: sa.Connection,
    index_name: str,
    fields: list[str] | None = None,
) -> dict:
    if not fields:
        fields = Resources.Table.columns.keys()
    cols = [c for c in Resources.Table.columns if c.name in fields]

    q = sa.select(*cols).where(Resources.index_name == index_name)
    r = conn.execute(q).fetchone()

    if not r:
        raise ValueError(f"Resource {index_name} not found")

    return {f: r._mapping[f] for f in fields}


def _refresh_resources_full(conn: sa.Connection):
    fields = {}

    db.drop_all()
    db.create_all()
    log.info(f"Created metadata records database at {db.path}")

    pool_size = multiprocessing.cpu_count()
    limit = 5000

    total_apis_available = fetch(gen_url(0, 1)).json()["total"]
    batches = []
    for offset in range(0, total_apis_available, limit):
        if offset + limit > total_apis_available:
            limit = total_apis_available % limit
        batches.append((offset, limit))

    total_batches = len(batches)

    inserted = 0
    with ProcessPoolExecutor(max_workers=pool_size) as executor:
        log.info(f"Using {pool_size} processes to fetch {total_batches} batches")
        futures = executor.map(FetchedBatch.recently_updated, batches)
        futures = tqdm(
            futures, total=total_batches, desc="Fetching & updating resources"
        )

        for batch in futures:
            for r in batch.resources:
                q = sa.insert(db.Resources.Table).values(asdict(r))
                conn.execute(q)
            conn.commit()
            inserted += len(batch.resources)
        conn.commit()

        if inserted != total_apis_available:
            log.warn(
                f"Total APIs available: {total_apis_available}, but fetched: {inserted}"
            )

        log.info(f"Valid resources: {inserted}/{total_apis_available}")

    log.info("Creating Full Text Search table")
    fts_table = db.ResourcesFTS.Table.name
    fts_columns = ",".join(c.name for c in db.ResourcesFTS.Table.columns)
    queries = f"""
        DROP TABLE IF EXISTS {fts_table};
        CREATE VIRTUAL TABLE {fts_table} USING FTS5({fts_columns}, tokenize="trigram");
        INSERT INTO {fts_table}({fts_columns}) SELECT {fts_columns} FROM {db.Resources.Table.name};
    """
    for q in queries.split(";"):
        conn.execute(sa.text(q))
    conn.commit()
    log.info(f"Created Full Test Search table: {fts_table}")

    return list(fields.values())


def _refresh_resources_incremental(conn: sa.Connection):
    q = sa.select(Resources.index_name, Resources.updated)
    ids = {id_: upd for id_, upd in conn.execute(q).fetchall()}
    log.info(f"Found {len(ids)} existing resources")

    offset = 0
    limit = 10

    fts_cols = [c.name for c in db.ResourcesFTS.Table.columns]

    updated: list[ResourceMetadata] = []
    created: list[ResourceMetadata] = []

    log.info(f"Fetching recently updated resources")
    all_updated = False
    while not all_updated:
        batch = FetchedBatch.recently_updated((offset, limit))
        if not batch.resources:
            break
        for r in batch.resources:
            if r.index_name in ids and ids[r.index_name] >= r.updated:
                all_updated = True
                break
        if all_updated:
            break
        updated.extend(batch.resources)
        offset += limit
        limit = min(5000, limit * 2)
    log.info(f"Found {len(updated)} recently updated resources")

    log.info(f"Fetching recently created resources")
    all_updated = False
    while not all_updated:
        batch = FetchedBatch.recently_created((offset, limit))
        if not batch.resources:
            break
        for r in batch.resources:
            if r.index_name in ids:
                all_updated = True
                break
        if all_updated:
            break

        created.extend(batch.resources)
        offset += limit
        limit = min(5000, limit * 2)
    log.info(f"Found {len(created)} recently created resources")

    # Sort to make the operation atomic
    updated.sort(key=lambda r: r.updated or datetime.max)
    created.sort(key=lambda r: r.created or datetime.max)

    for r in tqdm(updated + created, desc="Updating resources"):
        res = asdict(r)
        fts = {
            c: res[c] if isinstance(res[c], str) else json.dumps(res[c])
            for c in fts_cols
            if c in res
        }
        if r.index_name in ids:
            rq = (
                sa.update(db.Resources.Table)
                .where(Resources.index_name == r.index_name)
                .values(res)
            )
            fq = (
                sa.update(db.ResourcesFTS.Table)
                .where(ResourcesFTS.index_name == r.index_name)
                .values(fts)
            )

        else:
            rq = sa.insert(db.Resources.Table).values(res)
            fq = sa.insert(db.ResourcesFTS.Table).values(fts)
        conn.execute(rq)
        conn.execute(fq)

        ids[r.index_name] = r.updated
        conn.commit()


def refresh(conn: sa.Connection, full=False):
    if full:
        _refresh_resources_full(conn)
    else:
        _refresh_resources_incremental(conn)
