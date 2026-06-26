
import os
import pytest
import oeamdb
from sqlalchemy import create_engine, text
import time


def test_atc(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_geoloc_queries=5,
        max_pubchem_queries=10,
    )
    time.sleep(0.2)
    db.get_atc_corrections()
    db.apply_atc_corrections()

def test_subst_atc(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_geoloc_queries=5,
        max_pubchem_queries=10,
    )
    time.sleep(0.2)
    db.resolve_substance_atc()