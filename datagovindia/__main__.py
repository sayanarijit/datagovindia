import csv
import json
import sys

import typer

from datagovindia import api, db, metadata

app = typer.Typer()


@app.command()
def refresh(full: bool = False):
    """Refresh local metadata db"""

    with db.engine.connect() as conn:
        metadata.refresh(conn, full=full)


@app.command()
def org_types():
    """List all organization types"""

    with db.engine.connect() as conn:
        orgtypes = metadata.list_org_types(conn)

    for orgtype in orgtypes:
        typer.echo(orgtype)


@app.command()
def orgs():
    """List all organization names"""

    with db.engine.connect() as conn:
        orgnames = metadata.list_orgs(conn)

    for orgname in orgnames:
        typer.echo(orgname)


@app.command()
def sectors():
    """List all sectors"""

    with db.engine.connect() as conn:
        sectors = metadata.list_sectors(conn)

    for sector in sectors:
        typer.echo(sector)


@app.command()
def sources():
    """List all sources"""

    with db.engine.connect() as conn:
        sources = metadata.list_sources(conn)

    for source in sources:
        typer.echo(source)


@app.command()
def recently_updated(
    days: int = 7, max_results: int = 10, fields: list[str] | None = None
):
    """List recently updated resources"""

    with db.engine.connect() as conn:
        results = metadata.list_recently_updated(
            conn, days=days, max_results=max_results, fields=fields
        )

    typer.echo(json.dumps(results, indent=4, default=str))


@app.command()
def recently_created(
    days: int = 7, max_results: int = 10, fields: list[str] | None = None
):
    """List recently created resources"""

    with db.engine.connect() as conn:
        results = metadata.list_recently_created(
            conn, days=days, max_results=max_results, fields=fields
        )

    typer.echo(json.dumps(results, indent=4, default=str))


@app.command()
def search(
    title: str | None = None,
    desc: str | None = None,
    org: str | None = None,
    org_type: str | None = None,
    sector: str | None = None,
    source: str | None = None,
    max_results: int = 10,
    fields: list[str] | None = None,
):
    """Search resources"""

    if not any([title, desc, org, org_type, sector, source]):
        typer.echo("At least one search parameter is required", err=True)
        raise typer.Exit(code=1)

    with db.engine.connect() as conn:
        results = metadata.search(
            conn,
            title=title,
            desc=desc,
            org=org,
            org_type=org_type,
            sector=sector,
            source=source,
            max_results=max_results,
            fields=fields,
        )

    typer.echo(json.dumps(results, indent=4, default=str))


@app.command()
def info(
    index_name: str,
    format: str = "json",
):
    """Get data for a resource"""

    result = api.get_resource(
        index_name=index_name,
        offset=0,
        limit=0,
        format=format,
    )

    result.pop("field")
    result.pop("records")

    if format == "json":
        typer.echo(json.dumps(result, indent=4, default=str))
    else:
        typer.echo(result)


@app.command()
def preview(
    index_name: str,
    offset: int = 0,
    limit: int = 10,
    format: str = "json",
    filter: list[str] | None = None,
    fields: list[str] | None = None,
):
    """Preview data for a resource"""

    filters = {k: v for k, v in (x.split("=") for x in filter)} if filter else None

    resource = api.get_resource(
        index_name=index_name,
        offset=offset,
        limit=limit,
        format="json",
        filters=filters,
        fields=fields,
    )
    data = resource["records"]

    if format == "json":
        typer.echo(json.dumps(data, indent=4, default=str))
    else:
        typer.echo(data)


@app.command()
def stream(
    index_name: str,
    offset: int = 0,
    batch_size: int = 10,
    filter: list[str] | None = None,
    fields: list[str] | None = None,
    output: typer.FileTextWrite | None = None,
):
    """Download data for a resource as CSV"""

    resource = api.ResourceApi.from_index_name(index_name)
    filters = {k: v for k, v in (x.split("=") for x in filter)} if filter else None
    records = resource.stream_records(
        offset=offset, batch_size=batch_size, filters=filters, fields=fields
    )

    try:
        row = next(records)
    except StopIteration:
        return

    file = output or sys.stdout
    writer = csv.DictWriter(file, fieldnames=row.keys())
    writer.writeheader()
    writer.writerow(row)

    for row in records:
        writer.writerow(row)


def main():
    if not db.path.exists():
        refresh(full=True)

    app()


if __name__ == "__main__":
    main()
