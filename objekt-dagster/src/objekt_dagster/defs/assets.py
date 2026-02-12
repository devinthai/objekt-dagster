import dagster as dg
from objekt_api.objekt_api import ObjektApi
import objekt_api.models
from dagster import AssetExecutionContext, ResourceParam
from sqlalchemy import Select, or_
from sqlalchemy.orm import Session 
from sqlalchemy.dialects.postgresql import insert
from db.models.slugs import Slugs 
from db.models.metadata_snapshot import MetadataSnapshot
from datetime import datetime as dt
from datetime import UTC
import logging

@dg.asset
def collection(objekt_api: ResourceParam[ObjektApi]) -> list[objekt_api.models.Slug]:
    logging.info("getting objekt collection")
    collection = objekt_api.get_collection()
    return collection

@dg.asset(
    deps=[collection]
)
def sqlalchemy_slugs_dict_list(collection):
    """
    convert the collection (list of slug data models) into a list of dicts
    """
    slugs_list = []

    for slug in collection:
        current_slug = slug.model_dump()
        current_slug['workBatch'] = hash(current_slug['slugId']) % 4
        slugs_list.append(current_slug)

    return slugs_list

@dg.asset(
    deps=[
        sqlalchemy_slugs_dict_list
    ]
)
def postgresql_slugs_upsert(sqlalchemy_slugs_dict_list: list[dict], objekt_db_session: ResourceParam[Session]):
    """
    upsert the list of dicts into the postgresql server
    """
    insert_stmt = insert(Slugs).values(sqlalchemy_slugs_dict_list)

    # excluded gets the values that would have been inserted had there been no conflict
    excluded = insert_stmt.excluded

    # create a dict of the updated cols and their values as long as they're not the unique identifiers
    update_cols = {
        col.name: getattr(excluded, col.name)
        for col in Slugs.__table__.columns
        if col.name not in ["slugId", "id"]
    }

    # only update the rows that had a change to at least one of their values
    where_clause = or_(*[
        getattr(Slugs, col).is_distinct_from(getattr(excluded, col)) for col in update_cols
    ])

    on_conflict_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[Slugs.__table__.c.slugId],
        set_ = update_cols,
        where = where_clause,
    )

    with objekt_db_session as session:
        session.execute(on_conflict_stmt)
        session.commit()

    return

def process_metadata_list(metadata_list):
    now = dt.now(UTC)

    metadata_dicts = [
        {
            **data['metadata'].model_dump(),
            "slug": data['slug'],
            "snapshotTimestamp" : now
        }
        for data in metadata_list
    ]

    return metadata_dicts

@dg.asset()
def get_metadata_snapshot_batch0(objekt_api: ResourceParam[ObjektApi], objekt_db_session: ResourceParam[Session]):
    res = objekt_db_session.execute(Select(Slugs).where(Slugs.workBatch == 0))
    rows = res.all()

    slugs_list = [row.Slugs.slugString for row in rows]
    metadata_list = objekt_api.get_bulk_metadata(slugs_list)
    metadata_dicts = process_metadata_list(metadata_list)
    return metadata_dicts


@dg.asset()
def get_metadata_snapshot_batch1(objekt_api: ResourceParam[ObjektApi], objekt_db_session: ResourceParam[Session]):
    res = objekt_db_session.execute(Select(Slugs).where(Slugs.workBatch == 1))
    rows = res.all()

    slugs_list = [row.Slugs.slugString for row in rows]
    metadata_list = objekt_api.get_bulk_metadata(slugs_list)
    metadata_dicts = process_metadata_list(metadata_list)
    return metadata_dicts


@dg.asset()
def get_metadata_snapshot_batch2(objekt_api: ResourceParam[ObjektApi], objekt_db_session: ResourceParam[Session]):
    res = objekt_db_session.execute(Select(Slugs).where(Slugs.workBatch == 2))
    rows = res.all()

    slugs_list = [row.Slugs.slugString for row in rows]
    metadata_list = objekt_api.get_bulk_metadata(slugs_list)
    metadata_dicts = process_metadata_list(metadata_list)
    return metadata_dicts


@dg.asset()
def get_metadata_snapshot_batch3(objekt_api: ResourceParam[ObjektApi], objekt_db_session: ResourceParam[Session]):
    res = objekt_db_session.execute(Select(Slugs).where(Slugs.workBatch == 3))
    rows = res.all()

    slugs_list = [row.Slugs.slugString for row in rows]
    metadata_list = objekt_api.get_bulk_metadata(slugs_list)
    metadata_dicts = process_metadata_list(metadata_list)
    return metadata_dicts


@dg.asset(deps=[
    get_metadata_snapshot_batch0,
    get_metadata_snapshot_batch1,
    get_metadata_snapshot_batch2,
    get_metadata_snapshot_batch3
])
def insert_metadata_snapshot(
    get_metadata_snapshot_batch0,
    get_metadata_snapshot_batch1,
    get_metadata_snapshot_batch2,
    get_metadata_snapshot_batch3,
    objekt_db_session: ResourceParam[Session]
):
    for metadata_list in [get_metadata_snapshot_batch0, get_metadata_snapshot_batch1, get_metadata_snapshot_batch2, get_metadata_snapshot_batch3]:
        insert_stmt = insert(MetadataSnapshot).values(metadata_list)

        with objekt_db_session as session:
            session.execute(insert_stmt)
            session.commit()

