from sqlalchemy import text


def collect_stats(stats_engine, stats_queries: Dict[str, str]):
    res = dict()
    with engine.connect() as conn:
        for name, query in stats_queries.items():
            res[name] = conn.execute(text(query)).fetchone()[0]
    return res


if __name__ == "__main__":
    import os
    from sqlalchemy import create_engine

    stats_queries = {
        "n_sub": """
        SELECT COUNT(*) from substance
        """,
        "n_geloc": """
        SELECT COUNT(address) FROM public.company
        """,
        "n_sub_struct": """
        SELECT COUNT(*) FROM public.substance
        WHERE canonical_smiles IS NOT NULL OR struct_type = 'SEQ'
        """,
        "n_sub_DBid": """
        SELECT COUNT(*) FROM public.substance s
        WHERE COALESCE(pubchem_cid, pubchem_sid, chembl_id) IS NOT NULL
        """,
    }

    POSTGRES_USER = os.environ.get("PYTEST_POSTGRES_USER", "postgres")
    url = f"postgresql+psycopg://{POSTGRES_USER}@localhost:5432/_test_oeamdb_{POSTGRES_USER}"
    engine = create_engine(url)
    print(collect_stats(engine, stats_queries))
