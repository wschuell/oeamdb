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

logger = logging.getLogger(__name__)


def compute_hash(text_data: str | bytes) -> str:
    return xxhash.xxh3_64(text_data).hexdigest()


def compute_filehash(filepath: str | Path) -> str:
    with open(filepath, "rb") as f:
        return compute_hash(f.read())


def chunked(iterable, size):
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
                    WHERE NOT EXISTS (
                        SELECT 1 FROM substance
                        WHERE name_de=UPPER(:name_de)
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
            molsyn_query = chembl_conn.execute(
                text(
                    """
                SELECT
                    molregno,
                    syn_type,
                    molsyn_id,
                    synonyms
                FROM molecule_synonyms ms
                """
                )
            )
            molsyns = [m._mapping for m in molsyn_query]
            molecules_query = chembl_conn.execute(
                text(
                    """
                SELECT
                    md.pref_name,
                    md.molregno,
                    md.chembl_id,
                    cs.canonical_smiles,
                    cs.standard_inchi,
                    cs.standard_inchi_key,
                    md.molecule_type,
                    md.structure_type
                FROM molecule_dictionary md
                LEFT OUTER JOIN compound_structures cs
                    ON cs.molregno=md.molregno
                WHERE md.pref_name is not NULL
                """
                )
            )
            molecules = [m._mapping for m in molecules_query]
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                            CREATE TABLE IF NOT EXISTS chembl_mol_syns(
                                molsyn_id INTEGER PRIMARY KEY,
                                mol_regno INTEGER,
                                syn_type TEXT,
                                synonym TEXT
                                );
                            """
                )
            )
            conn.execute(
                text(
                    """
                            CREATE TABLE IF NOT EXISTS chembl_mols(
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
            conn.execute(
                text(
                    """
                            INSERT INTO chembl_mol_syns(
                                molsyn_id,
                                mol_regno,
                                syn_type,
                                synonym
                                )
                            VALUES(:molsyn_id
                                ,:molregno,
                                :syn_type,
                                UPPER(:synonyms))
                            ON CONFLICT DO NOTHING
                            ;
                            """
                ),
                molecules,
            )
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
                molecules,
            )
            conn.commit()

    def get_chembl_atc_info(self):
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
                    md.molregno,
                    md.chembl_id,
                    cs.canonical_smiles,
                    cs.standard_inchi,
                    cs.standard_inchi_key
                FROM molecule_dictionary md 
                LEFT OUTER JOIN compound_structures cs
                    ON cs.molregno=md.molregno
                WHERE md.pref_name is not NULL
                """
                )
            )
            molecules = [m._mapping for m in molecules_query]
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                            CREATE TABLE IF NOT EXISTS chembl_mols(
                                name TEXT PRIMARY KEY,
                                mol_regno INTEGER,
                                chembl_id TEXT,
                                canonical_smiles TEXT,
                                standard_inchi TEXT,
                                standard_inchi_key TEXT
                                );
                            """
                )
            )
            conn.execute(
                text(
                    """
                            INSERT INTO chembl_mols(
                                name,
                                mol_regno,
                                chembl_id,
                                canonical_smiles,
                                standard_inchi,
                                standard_inchi_key
                                )
                            VALUES(upper(:pref_name),:molregno,:chembl_id,:canonical_smiles,:standard_inchi,:standard_inchi_key)
                            ON CONFLICT DO NOTHING
                            ;
                            """
                ),
                molecules,
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
                    canonical_smiles=cm.canonical_smiles
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
                    raw_data JSON
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
