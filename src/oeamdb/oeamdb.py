
import sqlite3
from pathlib import Path
import os

from sqlalchemy import (
    create_engine,
    text,
)
from .sql_model import Base

from .downloaders import BasgDownloader

import xxhash
import csv
import json
from itertools import islice

from Levenshtein import distance as levenshtein

def compute_hash(text_data: str | bytes) -> str:
    return xxhash.xxh3_64(text_data).hexdigest()

def compute_filehash(filepath: str | Path) -> str:
    with open(filepath,"rb") as f:
        return compute_hash(f.read())

def chunked(iterable, size):
    it = iter(iterable)
    while (batch := list(islice(it, size))):
        yield batch

def check_hash(engine,filehash,filepath):
    skip = False
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT 1 FROM _file
                WHERE filehash=:filehash
                ;"""),
            {"filename":filepath.name,
             "filehash":filehash},
        ).fetchone()
        if result:
            skip = True
    return skip

def register_hash(engine,filehash,filepath):
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO _file(name,filehash)
                SELECT :filename,:filehash
                ;"""),
            {"filename":filepath.name,
             "filehash":filehash},
        )
        conn.commit()



def match_lists(en, de, product_key):
    """
    Returns list of (en_string, de_string) pairs.
    Uses Needleman-Wunsch-style alignment minimizing total edit distance.
    When one side has an extra element, both sides get the same string (copied).
    """
    n, m = len(en), len(de)

    # Fast path: equal length & already well-ordered enough -> still align,
    # but the trivial 1-1 case is covered by the DP below anyway.
    if n == 0 and m == 0:
        return []
    if n == 1 and m == 1:
        return [{
        "name_en":en[0],
        "name_de":de[0],
        "product_key":product_key,
        }]


    # Gap penalty: cost of leaving an element unmatched (then copied).
    # Set high enough that we only insert a gap when matching would be worse.
    GAP = 1000

    # DP over alignment
    INF = float('inf')
    dp = [[INF] * (m + 1) for _ in range(n + 1)]
    back = [[None] * (m + 1) for _ in range(n + 1)]
    dp[0][0] = 0
    for i in range(n + 1):
        for j in range(m + 1):
            if dp[i][j] == INF:
                continue
            # match en[i] with de[j]
            if i < n and j < m:
                c = dp[i][j] + levenshtein(en[i], de[j])
                if c < dp[i+1][j+1]:
                    dp[i+1][j+1] = c
                    back[i+1][j+1] = (i, j, 'M')
            # extra en[i] (de side copies it)
            if i < n:
                c = dp[i][j] + GAP
                if c < dp[i+1][j]:
                    dp[i+1][j] = c
                    back[i+1][j] = (i, j, 'EN')
            # extra de[j] (en side copies it)
            if j < m:
                c = dp[i][j] + GAP
                if c < dp[i][j+1]:
                    dp[i][j+1] = c
                    back[i][j+1] = (i, j, 'DE')

    # backtrack
    pairs = []
    i, j = n, m
    while (i, j) != (0, 0):
        pi, pj, op = back[i][j]
        if op == 'M':
            pairs.append({
                "name_en":en[i-1],
                "name_de":de[j-1],
                "product_key":product_key,
                })
        elif op == 'EN':
            pairs.append({
                "name_en":en[i-1],
                "name_de":en[i-1],
                "product_key":product_key,
                })
        else:
            pairs.append({
                "name_en":de[j-1],
                "name_de":de[j-1],
                "product_key":product_key,
                })
        i, j = pi, pj
    pairs.reverse()
    return pairs

class Importer:
    def __init__(self,query,chunk_size=10**4,param_processor=None,autocommit=True,param_split=False):
        self.chunk_size = chunk_size
        self.query = query
        self.param_processor = param_processor
        self.param_split = param_split
        self.autocommit = autocommit

    def import_all(self,engine,params):
        with engine.connect() as conn:
            param_iter = iter(params)
            while (chunk := list(islice(param_iter, self.chunk_size))):
                if self.param_processor is not None:
                    if self.param_split:
                        chunk_tmp = []
                        for p in chunk:
                            chunk_tmp += self.param_processor(p)
                        chunk = chunk_tmp
                    else:
                        chunk = [self.param_processor(p) for p in chunk]
                if chunk:
                    self.import_chunk(conn=conn,params=chunk)
            if self.autocommit:
                conn.commit()

    def import_chunk(self,conn,params):
        if isinstance(self.query,str):
            conn.execute(
                text(self.query),
                params,
            )
        elif isinstance(self.query,list):
            for q in self.query:
                conn.execute(
                    text(q),
                    params,
                )
        else:
            msg = f"Unsupported query type:{type(self.query)}"
            raise NotImplementedError(msg)



class Oeamdb:
    """Wrapper class."""


    sql_base = Base

    def __init__(
        self,
        engine_url: str ="sqlite:///oeamdb.db",
        engine=None,
        data_folder: "str | Path | None" = None,
    ):
        self.engine_url = engine_url
        if engine is not None:
            self.engine=engine
        else:
            self.init_engine()
        self.sql_base.metadata.create_all(self.engine,checkfirst=True)

        if data_folder is None:
            self.data_folder = Path.home() / ".oeamdb_data"
        else:
            self.data_folder = Path(data_folder)


    def download_basg(self,force=False):
        bdl = BasgDownloader(data_folder=self.data_folder)
        bdl.download(force=force)

    def init_engine(self )-> None:
        """Initiating the SQLAlchemy engine if not existing."""
        if not hasattr(self, "engine"):
            if self.engine_url.startswith("sqlite:"):
                connect_args = {
                    "detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
                }
            else:
                connect_args = {}
            self.engine_cfg = {
                "url": self.engine_url, "connect_args": connect_args,
            }
            self.engine = create_engine(**self.engine_cfg)

    def drop_all(self)-> None:
        """Clean slate for the database."""
        self.sql_base.metadata.drop_all(self.engine)

    def import_all(self):
        self.download_basg()
        self.import_basg()

    def import_basg(self):
        self.import_basg_json()
        self.import_basg_csv()

    def import_basg_json(self,force=False):
        filepath = self.data_folder / "basg.json"
        filehash = compute_filehash(filepath)


        skip = check_hash(engine=self.engine,filehash=filehash,filepath=filepath)

        with filepath.open(mode="r") as f:
            json_content = json.loads(f.read())

        def param_processor(data):
            shortage = data.get("drugShortage",None)
            if isinstance(shortage,str):
                shortage = not (shortage.lower()[0] in ('f','n'))
            data["drugShortage"] = shortage

            prescr = data.get("requiresPrescription",None)
            if isinstance(prescr,str):
                prescr = not (prescr.lower()[0] in ('f','n'))
            data["requiresPrescription"] = prescr

            data["mrpDcpNumber"] = data.get("mrpDcpNumber",None)
            return data

        def subst_processor(data):
            s_de = data['substances']
            s_en = data['substances_en']
            product_key = data["authNumber"]
            if s_de is None:
                s_de = []
            if s_en is None:
                s_en = []
            return match_lists(de=s_de,en=s_en,product_key=product_key)

        importers =[
                Importer(
                    query="""
                    INSERT INTO company(full_text)
                    SELECT CAST(:approvalHolder AS TEXT)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM company
                        WHERE full_text=:approvalHolder
                        )
                    ;""",param_processor=param_processor),
                Importer(
                    query="""
                    INSERT INTO product(
                            product_key,
                            name,
                            requires_prescription,
                            shortage,
                            mrp_dcp,
                            approval_holder
                            )
                    SELECT CAST(:authNumber AS TEXT),
                            :name,
                            :requiresPrescription,
                            :drugShortage,
                            :mrpDcpNumber,
                            (
                                SELECT id FROM company c
                                WHERE c.full_text=:approvalHolder
                            )
                    WHERE NOT EXISTS (
                        SELECT 1 FROM product
                        WHERE product_key=:authNumber
                        )
                    ;""",param_processor=param_processor),
                Importer(
                    query="""
                    INSERT INTO substance(name_de,name_en)
                    SELECT CAST(:name_de AS TEXT),CAST(:name_en AS TEXT)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM substance
                        WHERE name_de=:name_de
                        )
                    ;""",param_processor=subst_processor,param_split=True),
                Importer(
                    query="""
                    INSERT INTO product_substances(product_id,substance_id)
                    SELECT p.id,s.id
                    FROM product p
                    INNER JOIN substance s
                    ON p.product_key=:product_key
                    AND s.name_de=:name_de
                    ON CONFLICT DO NOTHING
                    ;""",param_processor=subst_processor,param_split=True),
                ]
        for importer in importers:
            importer.import_all(engine=self.engine,params=json_content)

        if not skip:
            register_hash(engine=self.engine,filehash=filehash,filepath=filepath)

    def import_basg_csv(self):
        csv_path = self.data_folder / "basg.csv"
        csv_hash = compute_filehash(csv_path)
        with csv_path.open(mode="r") as f:
           reader = csv.reader(f)