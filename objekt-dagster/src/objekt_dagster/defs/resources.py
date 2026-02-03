import dagster as dg
from objekt_api.objekt_api import ObjektApi

def objekt_api_resource() -> ObjektApi:
    objekt_api = ObjektApi()

    return objekt_api


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "objekt_api": objekt_api_resource(),
        }
    )
