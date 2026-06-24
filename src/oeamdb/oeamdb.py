import sqlite3
from pathlib import Path
import os

from sqlalchemy import (
    create_engine,
    text,
    JSON,
    bindparam,
)

from sqlalchemy.dialects.postgresql import JSONB

from .sql_model import Base

from .downloaders import BasgDownloader

import xxhash
import csv
import json
from itertools import islice

from Levenshtein import distance as levenshtein

import polars as pl
import copy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

import logging
import pubchempy as pcp
import time

from bs4 import BeautifulSoup
import requests

import pymupdf
import pymupdf4llm
import multiprocessing as mp

logger = logging.getLogger(__name__)


def _fetch_worker(args):
    i, meta = args
    dl_info = fetch_and_parse(url=meta["url"])
    return i, meta, dl_info

def compute_hash(text_data: str | bytes) -> str:
    return xxhash.xxh3_64(text_data).hexdigest()


def compute_filehash(filepath: str | Path) -> str:
    with open(filepath, "rb") as f:
        return compute_hash(f.read())


def chunked(iterable, size=10**4):
    it = iter(iterable)
    while batch := list(islice(it, size)):
        yield batch


def check_hash(engine, filehash, filepath):
    skip = False
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT 1 FROM _file
                WHERE filehash=:filehash
                ;"""
            ),
            {"filename": filepath.name, "filehash": filehash},
        ).fetchone()
        if result:
            skip = True
    return skip


def register_hash(engine, filehash, filepath):
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO _file(name,filehash)
                SELECT :filename,:filehash
                ;"""
            ),
            {"filename": filepath.name, "filehash": filehash},
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
        return [
            {
                "name_en": en[0],
                "name_de": de[0],
                "product_key": product_key,
            }
        ]

    pairs = []
    enc = copy.deepcopy(en)
    dec = copy.deepcopy(de)
    same = set(en) & set(dec)
    for e in same:
        pairs.append(
                {
                    "name_en": e,
                    "name_de": e,
                    "product_key": product_key,
                }
            )
        enc.remove(e)
        dec.remove(e)
        n -= 1
        m -= 1

    while n < m:
        _, d = max([(min([(levenshtein(d, e), e) for e in enc]), d) for d in dec])
        enc.append(d)
        n += 1
    while n > m:
        _, e = max([(min([(levenshtein(d, e), d) for d in dec]), e) for e in enc])
        dec.append(e)
        m += 1
    for d in dec:
        l_e = [(levenshtein(d, e), e) for e in enc]
        _, e = min(l_e)
        pairs.append(
            {
                "name_en": e,
                "name_de": d,
                "product_key": product_key,
            }
        )
        enc.remove(e)
    return pairs

def parse_pdf(content: bytes, embed_images: bool = True):
    doc = pymupdf.open(stream=content, filetype="pdf")
    md = pymupdf4llm.to_markdown(doc, embed_images=embed_images)
    return md

def fetch_and_parse(url, max_attempts=5):
    delay = 1
    for _ in range(max_attempts):
        try:
            r = requests.get(url, timeout=10)
        except requests.RequestException:
            time.sleep(delay); delay += 1; continue
        if r.ok:
            return {"content": r.content,
                    "text_content": parse_pdf(r.content),
                    "download_success": True}
        if r.status_code == 404:
            return {"content": None,
                    "text_content": None, "download_success": False}
        if r.status_code == 104:
            time.sleep(delay); delay += 1; continue
        raise IOError(f"Failed download status {r.status_code}: {url}")
    return None


class Importer:
    def __init__(
        self,
        query,
        chunk_size=10**4,
        param_processor=None,
        autocommit=True,
        param_split=False,
    ):
        self.chunk_size = chunk_size
        self.query = query
        self.param_processor = param_processor
        self.param_split = param_split
        self.autocommit = autocommit

    def import_all(self, engine, params):
        with engine.connect() as conn:
            param_iter = iter(params)
            while chunk := list(islice(param_iter, self.chunk_size)):
                if self.param_processor is not None:
                    if self.param_split:
                        chunk_tmp = []
                        for p in chunk:
                            chunk_tmp += self.param_processor(p)
                        chunk = chunk_tmp
                    else:
                        chunk = [self.param_processor(p) for p in chunk]
                if chunk:
                    self.import_chunk(conn=conn, params=chunk)
            if self.autocommit:
                conn.commit()

    def import_chunk(self, conn, params):
        if isinstance(self.query, str):
            conn.execute(
                text(self.query),
                params,
            )
        elif isinstance(self.query, list):
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

    doc_types_default = {
        "usage_info": ("Gebrauchsinformation", "GI gültig seit"),
        "technical_info": ("Fachinformation", "FI gültig seit"),
        "report_info": ("Public Assessment Report (PAR)", "PAR gültig seit"),
        "risk_info": ("Risk Management Plan (RMP Summary)", "RMP gültig seit"),
    }

    sql_base = Base

    def __init__(
        self,
        engine_url: str = "sqlite:///oeamdb.db",
        engine=None,
        data_folder: "str | Path | None" = None,
        filter_vet: bool = True,
        enforce_data_check: bool = True,
        doc_types: "dict | None" = None,
        chembl_engine=None,
        max_geoloc_queries=None,
        geolocator=None,
        max_pubchem_queries=None,
        max_docs_queries=None,
        workers=None,
        commit_batch=100,
    ):
        self.engine_url = engine_url
        if engine is not None:
            self.engine = engine
        else:
            self.init_engine()
        self.sql_base.metadata.create_all(self.engine, checkfirst=True)

        if data_folder is None:
            self.data_folder = Path.home() / ".oeamdb_data"
        else:
            self.data_folder = Path(data_folder)
        self.filter_vet = filter_vet
        self.enforce_data_check = enforce_data_check
        if doc_types is None:
            self.doc_types = copy.deepcopy(self.doc_types_default)
        else:
            self.doc_types = copy.deepcopy(doc_types)
        self.chembl_engine = chembl_engine
        self.max_geoloc_queries = max_geoloc_queries
        if geolocator is None:
            self.locator = Nominatim(user_agent="oeamdb_geolocator", timeout=5)
        else:
            self.locator = geolocator
        self.geocode = RateLimiter(self.locator.geocode, min_delay_seconds=1.5)
        self.max_pubchem_queries = max_pubchem_queries
        self.max_docs_queries = max_docs_queries
        self.commit_batch = commit_batch
        if workers is None:
            self.workers = max(1,mp.cpu_count()-1)
        else:
            self.workers = workers

    def download_basg(self, force=False):
        bdl = BasgDownloader(data_folder=self.data_folder)
        bdl.download(force=force)

    def init_engine(self) -> None:
        """Initiating the SQLAlchemy engine if not existing."""
        if not hasattr(self, "engine"):
            if self.engine_url.startswith("sqlite:"):
                connect_args = {
                    "detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
                }
            else:
                connect_args = {}
            self.engine_cfg = {
                "url": self.engine_url,
                "connect_args": connect_args,
            }
            self.engine = create_engine(**self.engine_cfg)

    def drop_all(self) -> None:
        """Clean slate for the database."""
        self.sql_base.metadata.drop_all(self.engine)

    def import_all(self):
        self.download_basg()
        self.import_basg()
        self.geolocate()

    def import_basg(self):
        self.import_basg_csv()
        self.import_basg_json()

    def import_basg_json(self, force=False, update_mode=True):
        filepath = self.data_folder / "basg.json"
        filehash = compute_filehash(filepath)

        skip = check_hash(engine=self.engine, filehash=filehash, filepath=filepath)

        if skip and not force:
            return

        with filepath.open(mode="r") as f:
            json_content = json.loads(f.read())

        def param_processor(data):
            shortage = data.get("drugShortage", None)
            if isinstance(shortage, str):
                shortage = not (shortage.lower()[0] in ("f", "n"))
            data["drugShortage"] = shortage

            prescr = data.get("requiresPrescription", None)
            if isinstance(prescr, str):
                prescr = not (prescr.lower()[0] in ("f", "n"))
            data["requiresPrescription"] = prescr

            data["mrpDcpNumber"] = data.get("mrpDcpNumber", None)
            return data

        def subst_processor(data):
            s_de = data["substances"]
            s_en = data["substances_en"]
            product_key = data["authNumber"]
            if s_de is None:
                s_de = []
            if s_en is None:
                s_en = []
            return match_lists(de=s_de, en=s_en, product_key=product_key)

        importers = [
            Importer(
                query="""
                    INSERT INTO company(full_text)
                    SELECT CAST(:approvalHolder AS TEXT)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM company
                        WHERE full_text=:approvalHolder
                        )
                    ;""",
                param_processor=param_processor,
            ),
        ]

        if update_mode:
            importers += [
                Importer(
                    query="""
                    UPDATE product
                    SET requires_prescription=:requiresPrescription,
                        shortage=:drugShortage,
                        mrp_dcp=:mrpDcpNumber,
                        updated_at=CURRENT_TIMESTAMP,
                        approval_holder=(
                                SELECT id FROM company c
                                WHERE c.full_text=:approvalHolder
                            )
                    WHERE product_key=:authNumber
                    ;""",
                    param_processor=param_processor,
                ),
            ]

        else:
            importers += [
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
                    ;""",
                    param_processor=param_processor,
                ),
            ]

        importers += [
            Importer(
                query="""
                    INSERT INTO substance(name_de,name_en)
                    SELECT UPPER(CAST(:name_de AS TEXT)),UPPER(CAST(:name_en AS TEXT))
                    FROM product p
                    WHERE p.product_key=:product_key
                    AND NOT EXISTS (
                        SELECT 1 FROM substance
                        WHERE name_en=UPPER(:name_en)
                        )
                    ;""",
                param_processor=subst_processor,
                param_split=True,
            ),
            Importer(
                query="""
                    INSERT INTO product_substances(product_id,substance_id)
                    SELECT p.id,s.id
                    FROM product p
                    INNER JOIN substance s
                    ON p.product_key=:product_key
                    AND s.name_de=:name_de
                    ON CONFLICT DO NOTHING
                    ;""",
                param_processor=subst_processor,
                param_split=True,
            ),
        ]
        for importer in importers:
            importer.import_all(engine=self.engine, params=json_content)

        if not skip:
            register_hash(engine=self.engine, filehash=filehash, filepath=filepath)

        if self.enforce_data_check:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(
                        """
                        SELECT COUNT(*) FROM product
                        WHERE shortage IS NULL
                        ;"""
                    ),
                )
                cnt = result.fetchone()[0]
                if cnt > 0:
                    raise ValueError(
                        f"After JSON update, {cnt} rows are still incomplete. JSON and CSV data might be from different timestamps."
                    )

    def import_basg_csv(self, force=False):
        filepath = self.data_folder / "basg.csv"
        filehash = compute_filehash(filepath)
        skip = check_hash(engine=self.engine, filehash=filehash, filepath=filepath)
        if skip and not force:
            return

        with filepath.open(mode="r") as f:
            csv_content = list(pl.read_csv(f).iter_rows(named=True))

        def param_processor(data):
            processed_data = {
                "approval_date": data["Zulassungsdatum"],
                "product_key": data["Zulassungsnummer"],
                "name": data["Name"],
                "target_usage": data["Verwendung"],
                "human_usage": data["Verwendung"] == "Human",
                "vet_usage": data["Verwendung"] == "Veterinär",
                "category": data["Arzneimittelkategorie"],
            }

            if self.filter_vet and not processed_data["human_usage"]:
                return []
            else:
                return [processed_data]

        def atc_processor(data):
            product_key = data["Zulassungsnummer"]
            target_usage = data["Verwendung"]
            if data["ATC Code"] is None:
                ans = []
            elif self.filter_vet and not target_usage == "Human":
                ans = []
            else:
                ans = [
                    {
                        "atc_code": ac.strip(" "),
                        "target_usage": target_usage,
                        "product_key": product_key,
                    }
                    for ac in data["ATC Code"].split(",")
                    if len(ac)
                ]
            return ans

        def doc_processor(data):
            product_key = data["Zulassungsnummer"]
            ans = []
            for doc_type, doc_info in self.doc_types.items():
                doc_row, date_row = doc_info
                if data[doc_row]:
                    ans.append(
                        {
                            "product_key": product_key,
                            "doc_type": doc_type,
                            "doc_url": data[doc_row],
                            "doc_date": data[date_row],
                        }
                    )
            return ans

        importers = [
            Importer(
                query="""
                    INSERT INTO product(
                            product_key,
                            approval_date,
                            name,
                            human_usage,
                            vet_usage,
                            orig_category
                            )
                    SELECT CAST(:product_key AS TEXT),
                            :approval_date,
                            :name,
                            :human_usage,
                            :vet_usage,
                            :category
                    WHERE NOT EXISTS (
                        SELECT 1 FROM product
                        WHERE product_key=:product_key)
                    ;""",
                param_processor=param_processor,
                param_split=True,
            ),
            Importer(
                query="""
                    INSERT INTO atc_code(atc_code)
                    SELECT :atc_code
                    ON CONFLICT DO NOTHING
                    ;""",
                param_processor=atc_processor,
                param_split=True,
            ),
            Importer(
                query="""
                    INSERT INTO product_atc(product_id,atc_code)
                    SELECT p.id,:atc_code
                    FROM product p
                    WHERE p.product_key=:product_key
                    ON CONFLICT DO NOTHING
                    ;""",
                param_processor=atc_processor,
                param_split=True,
            ),
            Importer(
                query="""
                    INSERT INTO document(
                        product_id,
                        doc_type,
                        valid_since,
                        url
                        )
                    SELECT p.id,:doc_type,:doc_date,:doc_url
                    FROM product p
                    WHERE p.product_key=:product_key
                    ON CONFLICT DO NOTHING
                    ;""",
                param_processor=doc_processor,
                param_split=True,
            ),
        ]
        for importer in importers:
            importer.import_all(engine=self.engine, params=csv_content)

        if not skip:
            register_hash(engine=self.engine, filehash=filehash, filepath=filepath)

    def get_chembl_mol_info(self):
        if self.chembl_engine is None:
            raise NotImplementedError(
                "Chembl REST API queries not implemented yet. Please provide a Chembl DB engine."
            )
        with self.chembl_engine.connect() as chembl_conn:
            molecules_query = chembl_conn.execute(
                text(
                    """
                SELECT
                    md.pref_name,
                    ms.synonyms,
                    md.molregno,
                    md.chembl_id,
                    cs.canonical_smiles,
                    cs.standard_inchi,
                    cs.standard_inchi_key,
                    md.molecule_type,
                    md.structure_type
                FROM molecule_dictionary md
                LEFT OUTER JOIN molecule_synonyms ms
                    ON md.molregno=ms.molregno
                LEFT OUTER JOIN compound_structures cs
                    ON cs.molregno=md.molregno
                WHERE md.pref_name is not NULL
                ORDER BY md.molregno
                """
                )
            )
            molecules = [m._mapping for m in molecules_query]
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                            CREATE TEMP TABLE IF NOT EXISTS chembl_mols(
                                name TEXT PRIMARY KEY,
                                mol_regno INTEGER,
                                chembl_id TEXT,
                                canonical_smiles TEXT,
                                standard_inchi TEXT,
                                standard_inchi_key TEXT,
                                molecule_type TEXT,
                                structure_type TEXT
                                );
                            """
                )
            )
            def split_synonyms(mol_dict_list):
                previous_molreg = None
                for m in mol_dict_list:
                    if m["molregno"] is None or m["molregno"] != previous_molreg:
                        yield m
                        previous_molreg = m["molregno"]
                    if m["synonyms"] is not None:
                        md = dict(m)
                        md["pref_name"] = m["synonyms"]
                        yield md

            conn.execute(
                text(
                    """
                            INSERT INTO chembl_mols(
                                name,
                                mol_regno,
                                chembl_id,
                                canonical_smiles,
                                standard_inchi,
                                standard_inchi_key,
                                molecule_type,
                                structure_type
                                )
                            VALUES(upper(:pref_name)
                                ,:molregno,
                                :chembl_id,
                                :canonical_smiles,
                                :standard_inchi,
                                :standard_inchi_key,
                                :molecule_type,
                                :structure_type)
                            ON CONFLICT DO NOTHING
                            ;
                            """
                ),
                list(split_synonyms(molecules)),
            )
            conn.commit()

    def get_chembl_atc_info(self):
        if self.chembl_engine is None:
            raise NotImplementedError(
                "Chembl REST API queries not implemented yet. Please provide a Chembl DB engine."
            )
        with self.chembl_engine.connect() as chembl_conn:
            atc_query = chembl_conn.execute(
                text(
                    """
                SELECT
                    who_name,
                    level1,
                    level1_description,
                    level2,
                    level2_description,
                    level3,
                    level3_description,
                    level4,
                    level4_description,
                    level5
                FROM atc_classification
                """
                )
            )
            atc_codes = [a._mapping for a in atc_query]
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                            INSERT INTO atc_code(
                    atc_code,
                    who_name,
                    level1,
                    level1_description,
                    level2,
                    level2_description,
                    level3,
                    level3_description,
                    level4,
                    level4_description,
                    level5
                                )
                            VALUES(
                    :level5,
                    :who_name,
                    :level1,
                    :level1_description,
                    :level2,
                    :level2_description,
                    :level3,
                    :level3_description,
                    :level4,
                    :level4_description,
                    :level5)
                            ON CONFLICT(atc_code) DO UPDATE SET
                    who_name=EXCLUDED.who_name,
                    level1=EXCLUDED.level1,
                    level1_description=EXCLUDED.level1_description,
                    level2=EXCLUDED.level2,
                    level2_description=EXCLUDED.level2_description,
                    level3=EXCLUDED.level3,
                    level3_description=EXCLUDED.level3_description,
                    level4=EXCLUDED.level4,
                    level4_description=EXCLUDED.level4_description,
                    level5=EXCLUDED.level5
                            ;
                            """
                ),
                atc_codes,
            )
            conn.commit()
            conn.execute(
                text(
                    """
                            INSERT INTO atc_code(
                    atc_code,
                    who_name,
                    level1,
                    level1_description,
                    level2,
                    level2_description,
                    level3,
                    level3_description,
                    level4,
                    level4_description
                                )
                           SELECT
                    ac.level4,
                    ac.level4_description,
                    ac.level1,
                    ac.level1_description,
                    ac.level2,
                    ac.level2_description,
                    ac.level3,
                    ac.level3_description,
                    ac.level4,
                    ac.level4_description
                    FROM atc_code ac
                    WHERE ac.who_name IS NOT NULL
                    GROUP BY
                    ac.level1,
                    ac.level1_description,
                    ac.level2,
                    ac.level2_description,
                    ac.level3,
                    ac.level3_description,
                    ac.level4,
                    ac.level4_description
                            ON CONFLICT(atc_code) DO UPDATE SET
                    who_name=EXCLUDED.who_name,
                    level1=EXCLUDED.level1,
                    level1_description=EXCLUDED.level1_description,
                    level2=EXCLUDED.level2,
                    level2_description=EXCLUDED.level2_description,
                    level3=EXCLUDED.level3,
                    level3_description=EXCLUDED.level3_description,
                    level4=EXCLUDED.level4,
                    level4_description=EXCLUDED.level4_description
                            ;
                            """
                ),
            )
            conn.execute(
                text(
                    """
                            INSERT INTO atc_code(
                    atc_code,
                    who_name,
                    level1,
                    level1_description,
                    level2,
                    level2_description,
                    level3,
                    level3_description
                                )
                           SELECT
                    ac.level3,
                    ac.level3_description,
                    ac.level1,
                    ac.level1_description,
                    ac.level2,
                    ac.level2_description,
                    ac.level3,
                    ac.level3_description
                    FROM atc_code ac
                    WHERE ac.who_name IS NOT NULL
                    GROUP BY
                    ac.level1,
                    ac.level1_description,
                    ac.level2,
                    ac.level2_description,
                    ac.level3,
                    ac.level3_description
                            ON CONFLICT(atc_code) DO UPDATE SET
                    who_name=EXCLUDED.who_name,
                    level1=EXCLUDED.level1,
                    level1_description=EXCLUDED.level1_description,
                    level2=EXCLUDED.level2,
                    level2_description=EXCLUDED.level2_description,
                    level3=EXCLUDED.level3,
                    level3_description=EXCLUDED.level3_description
                            ;
                            """
                ),
            )
            conn.execute(
                text(
                    """
                            INSERT INTO atc_code(
                    atc_code,
                    who_name,
                    level1,
                    level1_description,
                    level2,
                    level2_description
                                )
                           SELECT
                    ac.level2,
                    ac.level2_description,
                    ac.level1,
                    ac.level1_description,
                    ac.level2,
                    ac.level2_description
                    FROM atc_code ac
                    WHERE ac.who_name IS NOT NULL
                    GROUP BY
                    ac.level1,
                    ac.level1_description,
                    ac.level2,
                    ac.level2_description
                            ON CONFLICT(atc_code) DO UPDATE SET
                    who_name=EXCLUDED.who_name,
                    level1=EXCLUDED.level1,
                    level1_description=EXCLUDED.level1_description,
                    level2=EXCLUDED.level2,
                    level2_description=EXCLUDED.level2_description
                            ;
                            """
                ),
            )
            conn.execute(
                text(
                    """
                            INSERT INTO atc_code(
                    atc_code,
                    who_name,
                    level1,
                    level1_description
                                )
                           SELECT
                    ac.level1,
                    ac.level1_description,
                    ac.level1,
                    ac.level1_description
                    FROM atc_code ac
                    WHERE ac.who_name IS NOT NULL
                    GROUP BY
                    ac.level1,
                    ac.level1_description
                            ON CONFLICT(atc_code) DO UPDATE SET
                    who_name=EXCLUDED.who_name,
                    level1=EXCLUDED.level1,
                    level1_description=EXCLUDED.level1_description
                            ;
                            """
                ),
            )
            conn.commit()

    def get_atc_corrections(self, force=False):
        filepath = self.data_folder / "atc_corrections_fhi.no.json"
        if not filepath.exists() or force:
            r = requests.get("https://atcddd.fhi.no/atc_ddd_alterations__cumulative/atc_alterations/",timeout=5)

            soup = BeautifulSoup(r.content,"html.parser")
            div_table = soup.find_all("div",attrs={"class":"listtable"})[0]
            rows = [
                        [(
                            rr.text.split("\xa0")[0].strip("\n"),
                            (rr.a["title"].strip("\n") if rr.find("a") else None),
                            ) for rr in
                        r.find_all("td")
                        ]
                    for r in div_table.find_all("tr")
                    if len(r.find_all("td"))>1
                    ]
            atc_corrections = [{    "submitted_by":None,
                                    "atc_from":r[0][0],
                                    "name":r[1][0],
                                    "atc_to":r[2][0],
                                    "year":r[3][0],
                                    "footnotes":
                                            {
                                            "footnote_from":r[0][1],
                                            "footnote_name":r[1][1],
                                            "footnote_to":r[2][1],
                                            "footnote_year":r[3][1],
                                             },
                                } for r in rows
                                ]
            with filepath.open(mode="w") as f:
                f.write(json.dumps(atc_corrections))
        else:
            with filepath.open(mode="r") as f:
                atc_corrections = json.load(f)
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                            INSERT INTO atc_correction(
                                submitted_by,
                                replacement_year,
                                name,
                                old_code,
                                new_code,
                                footnotes
                                )
                            VALUES(
                                :submitted_by,
                                :year,
                                :name,
                                :atc_from,
                                :atc_to,
                                :footnotes
                                )
                            ON CONFLICT DO NOTHING
                            ;
                            """
                ).bindparams(
                            bindparam(
                                "footnotes",
                                type_=JSON(
                                    none_as_null=True
                                ),  # ().with_variant(JSONB, "postgresql"),
                            )
                        ),
                atc_corrections,
            )
            conn.commit()

    def apply_atc_corrections(self):
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                    WITH ranked_corrections AS (
                        SELECT
                            corr.old_code,
                            corr.name,
                            corr.replacement_year,
                            target.level1,
                            target.level1_description,
                            target.level2,
                            target.level2_description,
                            target.level3,
                            target.level3_description,
                            target.level4,
                            target.level4_description,
                            target.level5,
                            ROW_NUMBER() OVER (
                                PARTITION BY corr.old_code
                                ORDER BY corr.replacement_year DESC
                            ) as rn
                        FROM atc_correction corr
                        LEFT OUTER JOIN atc_code target
                            ON target.atc_code = corr.new_code
                    )
                    UPDATE atc_code
                    SET
                        who_name = r.name,
                        level1 = r.level1,
                        level1_description = r.level1_description,
                        level2 = r.level2,
                        level2_description = r.level2_description,
                        level3 = r.level3,
                        level3_description = r.level3_description,
                        level4 = r.level4,
                        level4_description = r.level4_description,
                        level5 = r.level5,
                        replacement_year = r.replacement_year,
                        has_been_replaced = true
                    FROM ranked_corrections r
                    WHERE atc_code.atc_code = r.old_code
                      AND r.rn = 1
                ;
                """
                ),
            )
            conn.commit()


    def resolve_chembl(self):
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                    UPDATE substance AS s
                    SET chembl_id = cm.chembl_id,
                    chembl_name=cm.name,
                    standard_inchi=cm.standard_inchi,
                    standard_inchi_key=cm.standard_inchi_key,
                    canonical_smiles=cm.canonical_smiles,
                    mol_type=cm.molecule_type,
                    struct_type=cm.structure_type
                    FROM chembl_mols AS cm
                    WHERE s.name_en=cm.name
                    ;"""))
            conn.commit()


    def geolocate(self, max_queries=None):
        if max_queries is None:
            max_queries = self.max_geoloc_queries

        query_count = 0
        with sqlite3.connect(
            self.data_folder / "oeamdb_geloloc.db"
        ) as geoloc_cache_conn, self.engine.connect() as conn:
            geoloc_cache_conn.execute(
                """
                CREATE TABLE IF NOT EXISTS company(
                    full_text TEXT PRIMARY KEY,
                    address TEXT,
                    latitude REAL,
                    longitude REAL,
                    geoloc_success BOOL,
                    raw_data JSON,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """
            )
            companies = conn.execute(
                text(
                    """SELECT
                        id,
                        full_text,
                        COUNT(*) OVER () AS total_count
                     FROM company
                    WHERE geoloc_success IS NULL
                            """
                )
            )
            for i, c_info in enumerate(companies):
                cid, c, c_cnt = c_info
                logger.info(f"Geolocating address {i+1} (over {c_cnt} total missing)")
                ca = ",".join(c.split(",")[1:]) if "," in c else c
                gq = geoloc_cache_conn.execute(
                    """SELECT address,latitude,longitude,raw_data,geoloc_success FROM company
                        WHERE full_text=:full_text
                            ;""",
                    {"full_text": c},
                ).fetchone()
                if not gq:
                    if max_queries is not None and query_count >= max_queries:
                        loc_data = None
                        logger.info(
                            f"Skipped address {i+1} (max queries to OSM reached)"
                        )
                    else:
                        loc = self.geocode(
                            ca,
                            addressdetails=True,
                            namedetails=True,
                        )
                        query_count += 1
                        if loc is not None:
                            loc_data = {
                                "full_text": c,
                                "address": loc.address,
                                "latitude": loc.latitude,
                                "longitude": loc.longitude,
                                "cid": cid,
                                "raw_data": loc.raw,
                                "success": True,
                            }
                        else:
                            loc_data = {
                                "full_text": c,
                                "address": None,
                                "latitude": None,
                                "longitude": None,
                                "cid": cid,
                                "raw_data": None,
                                "success": False,
                            }
                            logger.info(f"Failed address {i+1} (OSM return None)")
                        geoloc_cache_conn.execute(
                            """INSERT INTO company(full_text,address,latitude,longitude,raw_data,geoloc_success)
                            SELECT
                                :full_text,
                                :address,
                                :latitude,
                                :longitude,
                                :raw_data,
                                :success
                                    ;""",
                            loc_data,
                        )
                        geoloc_cache_conn.commit()
                else:
                    match gq[3]:
                        case None:
                            raw_data = None
                        case "null":
                            raw_data = None
                        case str():
                            raw_data = json.loads(gq[3])
                        case _:
                            raw_data = gq[3]
                    loc_data = {
                        "full_text": c,
                        "address": gq[0],
                        "latitude": gq[1],
                        "longitude": gq[2],
                        "cid": cid,
                        "raw_data": raw_data,
                        "success": bool(gq[4]),
                    }
                    logger.info(f"Retrieved address {i+1} from local cache")
                if loc_data:
                    conn.execute(
                        text(
                            """UPDATE company
                        SET address=:address,
                        latitude=:latitude,
                        longitude=:longitude,
                        geoloc_success=:success,
                        json_location=:raw_data
                        WHERE id=:cid
                                ;"""
                        ).bindparams(
                            bindparam(
                                "raw_data",
                                type_=JSON(
                                    none_as_null=True
                                ),  # ().with_variant(JSONB, "postgresql"),
                            )
                        ),
                        loc_data,
                    )
                    conn.commit()

    def resolve_pubchem(self,max_queries=None):
        if max_queries is None:
            max_queries = self.max_pubchem_queries
        query_count = 0
        last_query = 0
        with sqlite3.connect(
            self.data_folder / "oeamdb_pubchemc.db"
        ) as pubchem_cache_conn, self.engine.connect() as conn:
            pubchem_cache_conn.execute(
                """
                CREATE TABLE IF NOT EXISTS compound(
                    search_text TEXT PRIMARY KEY,
                    canonical_smiles TEXT,
                    standard_inchi TEXT,
                    standard_inchi_key TEXT,
                    pubchem_cid TEXT,
                    pubchem_sid TEXT,
                    success BOOL,
                    raw_data JSON,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """
            )
            missing_smiles = conn.execute(
                text(
                    """SELECT
                        id,
                        COALESCE(chembl_name,name_en),
                        COUNT(*) OVER () AS total_count
                     FROM substance
                    WHERE canonical_smiles IS NULL AND
                    (
                        (chembl_id IS NOT NULL AND struct_type != 'SEQ')
                        OR chembl_id IS NULL
                    )
                            """
                )
            )
            for i, s_info in enumerate(missing_smiles):
                sid, s, s_cnt = s_info
                logger.info(f"Querying Pubchem element {i+1} (over {s_cnt} total missing SMILES)")
                sq = pubchem_cache_conn.execute(
                    """SELECT   canonical_smiles,
                                standard_inchi,
                                standard_inchi_key,
                                pubchem_cid,
                                pubchem_sid,
                                success,
                                raw_data FROM compound
                        WHERE search_text=:search_text
                            ;""",
                    {"search_text": s},
                ).fetchone()
                if not sq:
                    if max_queries is not None and query_count >= max_queries:
                        s_data = None
                        logger.info(
                            f"Skipped element {i+1} (max queries to PubChem reached)"
                        )

                    else:
                        delay = time.time() - last_query
                        if delay < 0.5:
                            time.sleep(delay)
                        comp = pcp.get_compounds(s,"name")
                        query_count += 1
                        last_query = time.time()
                        if comp:
                            lengths = {
                                len({cp.canonical_smiles for cp in comp}),
                                len({cp.inchikey for cp in comp}),
                                }
                            if lengths != {1}:
                                msg = f"{len(comp)} non-matching elements found for {s}: {comp}"
                                logger.info(msg)

                                s_data = {
                                    "search_text":s,
                                    "pubchem_cid":None,
                                    "pubchem_sid":None,
                                    "canonical_smiles":None,
                                    "standard_inchi":None,
                                    "standard_inchi_key":None,
                                    "raw_data": [cp.record for cp in comp],
                                    "success": False,
                                    "sid": sid,
                                }
                            else:
                                c = comp[0]
                                s_data = {
                                    "search_text":s,
                                    "pubchem_cid":c.cid,
                                    "pubchem_sid":None,
                                    "canonical_smiles":c.canonical_smiles,
                                    "standard_inchi":c.inchi,
                                    "standard_inchi_key":c.inchikey,
                                    "raw_data": c.record,
                                    "success": True,
                                    "sid": sid,
                                }
                        else:
                            s_data = {
                                "search_text":s,
                                "pubchem_cid":None,
                                "pubchem_sid":None,
                                "canonical_smiles":None,
                                "standard_inchi":None,
                                "standard_inchi_key":None,
                                "raw_data": None,
                                "success": False,
                                "sid": sid,
                            }
                            logger.info(f"Failed address {i+1} (OSM return None)")
                        pubchem_cache_conn.execute(
                            """INSERT INTO compound(
                                search_text,
                                canonical_smiles,
                                standard_inchi,
                                standard_inchi_key,
                                pubchem_cid,
                                pubchem_sid,
                                success,
                                raw_data)
                            SELECT
                                :search_text,
                                :canonical_smiles,
                                :standard_inchi,
                                :standard_inchi_key,
                                :pubchem_cid,
                                :pubchem_sid,
                                :success,
                                :raw_data
                                    ;""",
                            s_data,
                        )
                        pubchem_cache_conn.commit()
                else:
                    match sq[3]:
                        case None:
                            raw_data = None
                        case "null":
                            raw_data = None
                        case str():
                            raw_data = json.loads(sq[6])
                        case _:
                            raw_data = sq[6]
                    s_data = {
                                "canonical_smiles": sq[0],
                                "standard_inchi": sq[1],
                                "standard_inchi_key": sq[2],
                                "pubchem_cid": sq[3],
                                "pubchem_sid": sq[4],
                                "success": sq[5],
                                "raw_data": raw_data,
                                "sid": sid,
                    }
                    logger.info(f"Retrieved element {i+1} from local cache")
                if s_data:
                    conn.execute(
                        text(
                            """UPDATE substance
                        SET
                                canonical_smiles=:canonical_smiles,
                                standard_inchi=:standard_inchi,
                                standard_inchi_key=:standard_inchi_key,
                                pubchem_cid=:pubchem_cid,
                                pubchem_sid=:pubchem_sid
                        WHERE id=:sid
                                ;"""
                        )
                        ,
                        s_data,
                    )
                    conn.commit()


    def resolve_docs(self,max_queries=None):
        separator = "._."

        if max_queries is None:
            max_queries = self.max_docs_queries
        query_count = 0
        delay = 1
        with sqlite3.connect(
            self.data_folder / "oeamdb_docs.db"
        ) as docs_cache_conn, self.engine.connect() as conn:
            docs_cache_conn.execute(
                """
                CREATE TABLE IF NOT EXISTS document(
                    url TEXT PRIMARY KEY,
                    product_key TEXT,
                    doc_type TEXT,
                    valid_since INTEGER,
                    content BLOB,
                    text_content TEXT,
                    success BOOL,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """
            )
            missing_doc = conn.execute(
                text(
                    """SELECT
                        d.url,
                        d.valid_since,
                        p.product_key,
                        d.doc_type,
                        COUNT(*) OVER () AS total_count
                     FROM document d
                     INNER JOIN product p
                        ON p.id=d.product_id
                     WHERE download_success IS NULL
                     LIMIT 1
                            """
                )
            ).fetchone()
            if missing_doc:
                dq = docs_cache_conn.execute(
                    """SELECT  url,text_content,success FROM document
                            ;""",
                    )
                for chunk in chunked(
                    iterable=({
                        "url":r[0],
                        "text_content":r[1],
                        "success":bool(r[2]),
                        } for r in dq.fetchall()),
                    size = 10**3,
                    ):
                    conn.execute(
                        text("""
                            UPDATE document
                                SET text_content=:text_content,
                                download_success=:success
                            WHERE download_success IS NULL
                            AND url=:url
                            ;"""
                            ),
                        chunk
                        )
                conn.commit()

                missing_docs_query = conn.execute(
                                    text(
                                        """SELECT
                                            d.url,
                                            d.valid_since,
                                            p.product_key,
                                            d.doc_type,
                                            COUNT(*) OVER () AS total_count
                                         FROM document d
                                         INNER JOIN product p
                                            ON p.id=d.product_id
                                         WHERE download_success IS NULL
                                                """
                                    )
                                )
                missing_docs = [
                    (i,dict(d._mapping))
                    for (i,d) in enumerate(missing_docs_query.fetchall())
                    ]


                if max_queries is not None:
                    if len(missing_docs) > max_queries:
                        fetch_tasks = missing_docs[:max_queries]
                        logger.info(f"Skipping {len(missing_docs)} documents (to stay below max downloads)")
                else:
                    fetch_tasks = missing_docs

                with mp.Pool(self.workers) as pool:
                    n = 0
                    for i, meta, dl_info in pool.imap_unordered(_fetch_worker, fetch_tasks):
                        if dl_info is None:
                            logger.info(f"Failed/skipped document {i+1}")
                            continue

                        doc_data = {**meta, **dl_info}

                        # SQLite cache write (parent owns the connection)
                        docs_cache_conn.execute(
                                """INSERT INTO document(
                                    url,
                                    text_content,
                                    valid_since,
                                    doc_type,
                                    product_key,
                                    success)
                                SELECT
                                    :url,
                                    :text_content,
                                    :valid_since,
                                    :doc_type,
                                    :product_key,
                                    :download_success
                                        ;""",
                                doc_data,
                                )

                        # main DB update
                        conn.execute(
                            text(
                                """UPDATE document
                            SET
                                    text_content=:text_content,
                                    download_success=:download_success
                            FROM product p
                            WHERE p.product_key=:product_key
                                AND p.id=product_id
                                AND valid_since=:valid_since
                                AND doc_type=:doc_type
                                    ;"""
                            )
                            ,
                            doc_data,
                            )
                        n += 1
                        if n % self.commit_batch == 0:
                            docs_cache_conn.commit()
                            conn.commit()
                docs_cache_conn.commit()
                conn.commit()
