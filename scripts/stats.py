from enum import Enum, auto
from sqlalchemy import text


class Query_Ret(Enum):
    SCALAR = auto()
    LIST = auto()
    DICT = auto()


def to_scalar(rows):
    return rows[0][0] if rows else None


def to_list(rows):
    return [r[0] for r in rows] if rows else None


def to_dict(rows):
    return {r[0]: r[1] for r in rows} if rows else None


HANDLERS = {
    Query_Ret.SCALAR: to_scalar,
    Query_Ret.LIST: to_list,
    Query_Ret.DICT: to_dict,
}


def collect_stats(stats_engine, stats_queries: Dict[str, str]):
    res = dict()
    with engine.connect() as conn:
        for name, (q_type, sql) in stats_queries.items():
            res[name] = HANDLERS[q_type](conn.execute(text(sql)).fetchall())
    return res


if __name__ == "__main__":
    import os
    from sqlalchemy import create_engine

    stats_queries = {
        "n_sub": (
            Query_Ret.SCALAR,
            """
                  SELECT COUNT(*) from substance
                  """,
        ),
        "n_geloc": (
            Query_Ret.SCALAR,
            """
                   SELECT COUNT(address) FROM public.company
                   """,
        ),
        "n_sub_struct": (
            Query_Ret.SCALAR,
            """
                         SELECT COUNT(*) FROM public.substance
                         WHERE canonical_smiles IS NOT NULL OR struct_type = 'SEQ'
                         """,
        ),
        "n_sub_DBid": (
            Query_Ret.SCALAR,
            """
                       SELECT COUNT(*) FROM public.substance s
                       WHERE COALESCE(pubchem_cid, pubchem_sid, chembl_id) IS NOT NULL
                       """,
        ),
    }

    POSTGRES_USER = os.environ.get("PYTEST_POSTGRES_USER", "postgres")
    url = f"postgresql+psycopg://{POSTGRES_USER}@localhost:5432/_test_oeamdb_{POSTGRES_USER}"
    engine = create_engine(url)
    print(collect_stats(engine, stats_queries))
