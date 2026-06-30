"""Tests for the oeamdb database connections."""

import os
import pytest
import oeamdb
from sqlalchemy import create_engine, text
import time

def test_chembl(testengine, persistent_tmp_path, chembl_engine):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        chembl_engine=chembl_engine,
        max_chembl_queries=3,
        max_geoloc_queries=3,
    )
    if chembl_engine is None:
        pytest.skip()
    db.get_chembl_atc_info()
    db.get_chembl_mol_info()
    db.resolve_chembl()
    db.get_chembl_mol_atc()

def test_pubchem(testengine, persistent_tmp_path, chembl_engine):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        chembl_engine=chembl_engine,
        max_geoloc_queries=3,
        max_chembl_queries=3,
        max_pubchem_queries=3,
    )
    time.sleep(0.2)
    db.resolve_pubchem()