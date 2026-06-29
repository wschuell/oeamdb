
import os
import pytest
import oeamdb
from sqlalchemy import create_engine, text
import time
from pathlib import Path


def test_atc(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_geoloc_queries=5,
        max_pubchem_queries=10,
    )
    time.sleep(0.2)
    db.get_atc_corrections()
    db.get_atc_corrections(filepath=Path(oeamdb.__file__).parent
        / "data"
        / "atc_corr.json")
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

def test_course(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_geoloc_queries=5,
        max_pubchem_queries=10,
    )
    db.import_course_material(
        filepath=Path(__file__).parent
        / "test_data"
        / "course_material.json"
        )

def test_categories(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_geoloc_queries=5,
        max_pubchem_queries=10,
    )
    db.import_category_corrections(
        filepath=Path(__file__).parent
        / "test_data"
        / "category.json"
        )

def test_stats(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_geoloc_queries=5,
        max_pubchem_queries=10,
    )
    db.get_stats()