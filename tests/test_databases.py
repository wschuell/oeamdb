"""Tests for the oeamdb database connections."""

import os
import pytest
import oeamdb
from sqlalchemy import create_engine, text
def test_connect(testengine):
    with testengine.connect() as conn:
        result=conn.execute(text("SELECT 'engine_works';"))
        res_list = list(result)
        assert len(res_list) == 1
        assert "engine_works" == res_list[0][0]


def test_create(testengine):
    db = oeamdb.Oeamdb(
        engine=testengine,
        )

def test_drop(testengine):
    db = oeamdb.Oeamdb(
        engine=testengine,
        )
    db.drop_all()

def test_import(testengine):
    db = oeamdb.Oeamdb(
        engine=testengine,
        )
    db.import_all()
