import dagster as dg
from objekt_api.objekt_api import ObjektApi
from db.engine import engine
from sqlalchemy.orm import sessionmaker

def objekt_api_resource() -> ObjektApi:
    objekt_api = ObjektApi()

    return objekt_api

def objekt_db_session():
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "objekt_api": objekt_api_resource(),
            "objekt_db_session": objekt_db_session(),
        }
    )
