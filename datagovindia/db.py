import sqlalchemy as sa
from sqla_fancy_core import TableFactory

from datagovindia.config import DATAGOVINDIA_CACHE_DIR

path = DATAGOVINDIA_CACHE_DIR / "metadata.db"

meta = sa.MetaData()
engine = sa.create_engine(f"sqlite:///{path}")

tf = TableFactory()


class Resources:
    index_name = tf.string("index_name", primary_key=True, nullable=False)
    title = tf.string("title")
    desc = tf.string("desc")
    org = tf.json("org")
    org_type = tf.string("org_type")
    source = tf.string("source")
    sector = tf.json("sector")
    field = tf.json("field")
    created = tf.datetime("created")
    updated = tf.datetime("updated")

    Table = tf("resources", meta)


class ResourcesFTS(Resources):
    index_name = tf.string("index_name")
    title = tf.string("title")
    desc = tf.string("desc")
    org = tf.string("org")
    org_type = tf.string("org_type")
    source = tf.string("source")
    sector = tf.string("sector")
    field = tf.string("field")

    rank = sa.text("rank")  # Hidden column for MATCH operation

    Table = tf("resources_fts", meta)


def drop_all():
    meta.drop_all(engine)


def create_all():
    meta.create_all(engine)
