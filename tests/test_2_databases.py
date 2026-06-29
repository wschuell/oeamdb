"""Tests for the oeamdb database connections."""

import os
import pytest
import oeamdb
from sqlalchemy import create_engine, text


def test_connect(testengine):
    with testengine.connect() as conn:
        result = conn.execute(text("SELECT 'engine_works';"))
        res_list = list(result)
        assert len(res_list) == 1
        assert "engine_works" == res_list[0][0]


def test_create(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_geoloc_queries=3,
    )


def test_drop(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_geoloc_queries=3,
    )
    db.drop_all()


def test_import(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_geoloc_queries=3,
    )
    db.download_basg()
    db.import_basg()

def test_reimport(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_geoloc_queries=3,
    )

    db.download_basg()
    db.import_basg()

def test_geolocate(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_docs_queries=3,
    )

    db.geolocate()

def test_docs(testengine, persistent_tmp_path):
    db = oeamdb.Oeamdb(
        engine=testengine,
        data_folder=persistent_tmp_path / "basg_dl",
        max_docs_queries=3,
    )
    db.resolve_docs()
