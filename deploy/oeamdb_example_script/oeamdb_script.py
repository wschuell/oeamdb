import os
from sqlalchemy import create_engine

from oeamdb import Oeamdb


chembl_version = os.getenv("CHEMBL_VERSION")
if chembl_version == "latest":
    chembl_version = None

chembl_db_url = os.getenv("OEAMDB_WEB_CHEMBL_DATABASE_URL")

db = Oeamdb(
    data_folder=os.getenv("OEAMDB_WEB_DATAFOLDER"),
    engine_url=os.getenv("OEAMDB_WEB_DATABASE_ADMIN_URL"),
    # chembl_download=False,
    # chembl_version=chembl_version,
    chembl_engine=create_engine(chembl_db_url)
    )

db.import_all()

db.refresh_course_pivot()