import dagster as dg
from objekt_api.objekt_api import ObjektApi
import objekt_api.models
from dagster import AssetExecutionContext, ResourceParam
from sqlalchemy import Select
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

    on_conflict_stmt = insert_stmt.on_conflict_do_nothing(
         index_elements=[Slugs.__table__.c.slugId]
    )

    with objekt_db_session as session:
        session.execute(on_conflict_stmt)
        session.commit()

    return

@dg.asset()
def get_metadata_snapshot(context: AssetExecutionContext, objekt_api: ResourceParam[ObjektApi], objekt_db_session: ResourceParam[Session]):
    res = objekt_db_session.execute(Select(Slugs))
    rows = res.all()

    metadata_list = []
    for row in rows:
        # print(f"Getting metadata for {row.Slugs.slugString}.")
        fetch_string = f"Getting metadata for {row.Slugs.slugString}"
        context.log.info(fetch_string)
        metadata = objekt_api.get_metadata(row.Slugs.slugString)
        metadata_dict = metadata.model_dump()
        metadata_dict['snapshotTimestamp'] = dt.now(UTC)
        metadata_list.append(metadata_dict)

    return metadata_list

@dg.asset(deps=[
    get_metadata_snapshot
])
def insert_metadata_snapshot(get_metadata_snapshot: list[dict], objekt_db_session: ResourceParam[Session]):
    insert_stmt = insert(MetadataSnapshot).values(get_metadata_snapshot)

    with objekt_db_session as session:
        session.execute(insert_stmt)
        session.commit()

