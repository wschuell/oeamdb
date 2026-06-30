from sqlalchemy import text, create_engine
import json
import os


STATS_QUERIES = {
        "geoloc": (
            lambda x,y: {"total":x,"covered":y,"coverage":y*1./max(x,1.)},
            """
                   SELECT COUNT(*),COUNT(address) FROM company
                   ;""",
        ),
        "substances": (
            lambda tot,sm,ss,c,cp: {"total":tot,
                "with_smiles":sm,
                "with_smiles_ratio":1. if tot==0 else sm/(max(1.,tot)),
                "with_smiles_or_seq":sm,
                "with_smiles_or_seq_ratio":1. if tot==0 else ss/(max(1.,tot)),
                "with_chemblid":c,
                "with_chemblid_ratio":1. if tot==0 else c/(max(1.,tot)),
                "with_chembl_or_pubchemid":cp,
                "with_chembl_or_pubchemid_ratio":1. if tot==0 else cp/(max(1.,tot)),
                } ,
            """
                         SELECT COUNT(*),
                         COUNT(canonical_smiles),
                         COUNT(
                            COALESCE(canonical_smiles,
                                CASE struct_type
                                    WHEN 'SEQ' THEN 'SEQ'
                                    ELSE NULL END)
                                ),
                         COUNT(chembl_id),
                         COUNT(COALESCE(chembl_id,pubchem_cid,pubchem_sid))
                         FROM substance
                         ;""",
        ),
        "substances_atc": (
            lambda tot,a,a5,a4,a3,a2: {"total":tot,
                "with_atc":a,
                "with_atc_ratio":1. if tot==0 else a/(max(1.,tot)),
                "with_atc5":a5,
                "with_atc5_ratio":1. if tot==0 else a5/(max(1.,tot)),
                "with_atc4":a4,
                "with_atc4_ratio":1. if tot==0 else a4/(max(1.,tot)),
                "with_atc3":a3,
                "with_atc3_ratio":1. if tot==0 else a3/(max(1.,tot)),
                "with_atc2":a2,
                "with_atc2_ratio":1. if tot==0 else a2/(max(1.,tot)),
                } ,
            """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(
                                CASE WHEN atc>0 THEN 1
                                ELSE NULL
                            END) AS with_atc,
                        COUNT(
                                CASE WHEN atc5>0 THEN 1
                                ELSE NULL
                            END) AS with_atc5,
                        COUNT(
                                CASE WHEN atc4>0 THEN 1
                                ELSE NULL
                            END) AS with_atc4,
                        COUNT(
                                CASE WHEN atc3>0 THEN 1
                                ELSE NULL
                            END) AS with_atc3,
                        COUNT(
                                CASE WHEN atc2>0 THEN 1
                                ELSE NULL
                            END) AS with_atc2
                    FROM (
                         SELECT
                            s.id,
                            COUNT(sa.atc_code) as atc,
                            COUNT(
                                CASE WHEN CHAR_LENGTH(sa.atc_code)>=7 THEN 1
                                ELSE NULL
                            END
                                ) as atc5,
                            COUNT(
                                CASE WHEN CHAR_LENGTH(sa.atc_code)>=5 THEN 1
                                ELSE NULL
                            END
                                ) as atc4,
                            COUNT(
                                CASE WHEN CHAR_LENGTH(sa.atc_code)>=4 THEN 1
                                ELSE NULL
                            END
                                ) as atc3,
                            COUNT(
                                CASE WHEN CHAR_LENGTH(sa.atc_code)>=3 THEN 1
                                ELSE NULL
                            END
                                ) as atc2
                         FROM substance s
                         LEFT OUTER JOIN substance_atc sa
                         ON s.id = sa.substance_id
                         GROUP BY s.id
                        )
                         ;""",
        ),
        "product_atc_substance": (
            lambda prod,patc,patc_lk,patcs_lk: {
                "products":prod,
                "products_with_atc":patc,
                "products_with_atc_ratio":1. if prod==0 else patc/max(1.,prod),
                "products_atc_links":patc_lk,
                "products_atc_substance_links":patcs_lk,
                "products_atc_substance_links_ratio":1. if patc_lk==0 else patcs_lk/max(-1.,patc_lk),
                },
            """
                       SELECT
                            (SELECT COUNT(*) FROM product) AS products,
                            (SELECT COUNT(DISTINCT product_id)
                                FROM product_atc pa) AS products_with_atc,
                             (SELECT COUNT(*) FROM
                                (SELECT DISTINCT product_id,
                                    COALESCE(ac.level5,
                                        ac.atc_code) AS atc_code
                                    FROM product_atc pa
                                    INNER JOIN atc_code ac
                                    ON ac.atc_code =pa.atc_code)
                                ) AS p_atc_links,
                             (SELECT COUNT(*) FROM
                                ( SELECT p.id AS product_id, ac2.atc_code
                                    FROM product p
                                    INNER JOIN product_atc pa
                                        ON pa.product_id = p.id
                                    INNER JOIN atc_code ac
                                        ON pa.atc_code = ac.atc_code
                                    INNER JOIN atc_code ac2
                                        ON ac2.atc_code = COALESCE(
                                            ac.level5,
                                            ac.atc_code)
                                    INNER JOIN substance_atc sa
                                        ON sa.atc_code = ac.atc_code
                                    INNER JOIN substance s
                                        ON s.id = sa.substance_id
                                UNION
                                    SELECT p.id, ac2.atc_code
                                    FROM product p
                                    INNER JOIN product_atc pa
                                        ON pa.product_id = p.id
                                    INNER JOIN atc_code ac
                                        ON pa.atc_code = ac.atc_code
                                    INNER JOIN atc_code ac2
                                        ON ac2.atc_code = COALESCE(
                                            ac.level5,
                                            ac.atc_code)
                                    INNER JOIN substance_atc sa
                                        ON sa.atc_code = ac2.atc_code
                                    INNER JOIN substance s
                                        ON s.id = sa.substance_id)
                                ) AS p_atc_s_links
                       ;""",
        ),
        "ATC": (
            lambda  a1,
                    a1_missing,
                    a1_deleted,
                    a2,
                    a2_missing,
                    a2_deleted,
                    a3,
                    a3_missing,
                    a3_deleted,
                    a4,
                    a4_missing,
                    a4_deleted,
                    a5,
                    a5_missing,
                    a5_deleted,
                    : {
                    "a1":a1,
                    "a1_missing":a1_missing,
                    "a1_deleted":a1_deleted,
                    "a1_coverage":1. if a1==0 else (a1-a1_missing)/max(1.,a1),
                    "a2":a2,
                    "a2_missing":a2_missing,
                    "a2_deleted":a2_deleted,
                    "a2_coverage":1. if a2==0 else (a2-a2_missing)/max(1.,a2),
                    "a3":a3,
                    "a3_missing":a3_missing,
                    "a3_deleted":a3_deleted,
                    "a3_coverage":1. if a3==0 else (a3-a3_missing)/max(1.,a3),
                    "a4":a4,
                    "a4_missing":a4_missing,
                    "a4_deleted":a4_deleted,
                    "a4_coverage":1. if a4==0 else (a4-a4_missing)/max(1.,a4),
                    "a5":a5,
                    "a5_missing":a5_missing,
                    "a5_deleted":a5_deleted,
                    "a5_coverage":1. if a5==0 else (a5-a5_missing)/max(1.,a5),
                },
            """
                       SELECT
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=1) AS a1,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=1
                            AND level1 IS NULL) AS a1_missing,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=1
                            AND level5='deleted') AS a1_deleted,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=3) AS a2,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=3
                            AND level2 IS NULL) AS a2_missing,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=3
                            AND level5='deleted') AS a2_deleted,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=4) AS a3,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=4
                            AND level3 IS NULL) AS a3_missing,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=4
                            AND level5='deleted') AS a3_deleted,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=5) AS a4,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=5
                            AND level4 IS NULL) AS a4_missing,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)=5
                            AND level5='deleted') AS a4_deleted,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)>=7) AS a5,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)>=7
                            AND level5 IS NULL) AS a5_missing,
                        (SELECT COUNT(*) FROM atc_code
                            WHERE CHAR_LENGTH(atc_code)>=7
                            AND level5='deleted') AS a5_deleted
            ;""",
        ),
    }


def collect_stats(engine, stats_queries: dict[str, str]=STATS_QUERIES):
    res = dict()
    with engine.connect() as conn:
        for name, (q_parser, sql) in stats_queries.items():
            q_res = list(conn.execute(text(sql)).fetchone())
            res[name] = None if q_res is None else q_parser(*q_res)
    return res