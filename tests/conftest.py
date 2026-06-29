from pathlib import Path
import pytest
import sqlalchemy
from sqlalchemy import create_engine, text
import os

import oeamdb

@pytest.fixture(scope="session")
def shared_tmp_path(tmp_path_factory) -> Path:
    # one dir for the whole pytest run (all files)
    return tmp_path_factory.mktemp("download_cache")

@pytest.fixture(scope="session")
def persistent_tmp_path(request) -> Path:
    return Path(request.config.cache.makedir("persistent_download_cache"))


@pytest.fixture()
def chembl_engine():
    engine_url = os.environ.get("CHEMBL_ENGINE",None)
    if engine_url is not None:
        return create_engine(
            engine_url
        )
    return None

dbtype_list = [
"sqlite",
"postgres",

]


POSTGRES_USER=os.environ.get('PYTEST_POSTGRES_USER','postgres')

@pytest.fixture(params=dbtype_list)
def dbtype(request):
    return request.param

@pytest.fixture(params=dbtype_list,scope="session")
def testengine(request,shared_tmp_path):
    if request.param in ['sqlite','duckdb']:
        db_path = shared_tmp_path / f"_test_oeamdb.{request.param}.db"
        url = f"{request.param}:///{db_path.as_posix()}"
    elif request.param =="postgres":
        url= f"postgresql+psycopg://{POSTGRES_USER}@localhost:5432/_test_oeamdb_{POSTGRES_USER}"
    else:
        url= f"{request.param}:///_test_oeamdb.{request.param}"
    return create_engine(url)
