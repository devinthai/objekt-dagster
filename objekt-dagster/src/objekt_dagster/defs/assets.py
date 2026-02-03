import dagster as dg
from objekt_api.objekt_api import ObjektApi
import objekt_api.models
from dagster import ResourceParam

@dg.asset
def collection(objekt_api: ResourceParam[ObjektApi]) -> list[objekt_api.models.Slug]:
    collection = objekt_api.get_collection()
    return collection
