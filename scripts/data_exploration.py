#!/usr/bin/env python3

import polars as pl
import seaborn as sns
from matplotlib import pyplot as plt
from pprint import pprint


def compare_cols(csv_col, json_col):
    print(csv_df[csv_col])
    print(json_df[json_col])


## Columns
## ['Name', 'Typ', 'Bezeichnung', 'Zulassungsnummer', 'MR/DC/CP Nummer', 'Inhaber:in', 'Zulassungsdatum', 'Wirkstoff(e)', 'ATC Code',
## 'relevant gemäß Suchtgift VO', 'relevant gemäß Psychotropen VO', 'Chargenfreigabepflicht', 'Einstellung des In-Verkehr-Bringens gemeldet',
## 'Ausnahme Chargenprüfung', 'zugelassen in Liechtenstein', 'Rezeptpflichtstatus', 'Abgabestatus', 'Verwendung', 'Arzneimittelkategorie', 'Stärke',
## 'Einheit Stärke', 'Darreichungsform', 'Allergen', 'Impfstoff', 'Rezeptpflichtig', 'zusätzliche Überwachung', 'Zieltierart', 'Tierkategorie', 'Anwendungsart',
## 'Dosierung', 'Zielgewebe', 'Wartezeit', 'Einheit', 'Besonderheiten', 'Gebrauchsinformation', 'GI gültig seit', 'Fachinformation', 'FI gültig seit', 'Public Assessment Report (PAR)',
## 'PAR gültig seit', 'Risk Management Plan (RMP Summary)', 'RMP gültig seit']
## ['id', 'authNumber', 'lang', 'substances', 'name', 'approvalHolder', 'requiresPrescription', 'packageLeaflet', 'fachInformation', 'mrpDcpNumber', 'drugShortage']
csv_map = {
    "Name": "name",
    "Inhaber:in": "approvalHolder",
    "Wirkstoff(e)": "substances",  # Wirkstoffe is a string and substances is a list of strings
    "Rezeptpflichtstatus": "requiresPrescription",  # "Arzneimittel zur Abgabe ohne aerztliche Verschreibung" vs "Nein"
    "Zulassungsnummer": "authNumber",
    "ATC Code": "atc_code",
}
de_atc_map = {"ATC-Code": "atc_code"}
csv_df = (
    pl.read_csv("test_data/medicinal-products.csv")
    .rename(csv_map)
    .with_columns(
        pl.col("atc_code")
        .str.split(",")
        .list.eval(pl.element().str.strip_chars())
    )
    .explode("atc_code")
)

clean_csv_df = csv_df.filter(
    pl.col("atc_code").str.contains(
        pattern=r"(^[a-zA-Z]{1,2}\d{2}[a-zA-Z]{2}\d{2,3}$)"
    )
).drop_nulls(subset="atc_code")
json_df = pl.read_json("test_data/export.json")
scrape_atc_df = (
    pl.read_csv("test_data/WHO ATC-DDD 2026-04-25.csv")
    .with_columns(
        pl.col("atc_code")
        .str.split(",")
        .list.eval(pl.element().str.strip_chars())
    )
    .explode("atc_code")
    .filter(
        pl.col("atc_code").str.contains(
            pattern=r"(^[a-zA-Z]{1,2}\d{2}[a-zA-Z]{2}\d{2,3}$)"
        )
    )
    .drop_nulls(subset="atc_code")
)


de_atc_df = (
    pl.read_excel("test_data/ATC GKV-AI 2026.xlsx", sheet_id=2)
    .rename(de_atc_map)
    .with_columns(
        pl.col("atc_code")
        .str.split(",")
        .list.eval(pl.element().str.strip_chars())
    )
    .explode("atc_code")
    .filter(
        pl.col("atc_code").str.contains(
            pattern=r"(^[a-zA-Z]{1,2}\d{2}[a-zA-Z]{2}\d{2,3}$)"
        )
    )
    .drop_nulls(subset="atc_code")
)


print(de_atc_df.head())

lens = clean_csv_df.select(pl.col("atc_code").str.len_chars().alias("len"))


all_count = csv_df.shape[0]
clean_count = clean_csv_df.shape[0]
all_short = csv_df.filter(pl.col("atc_code").str.len_chars() < 7)
all_unique_short = csv_df.filter(pl.col("atc_code").str.len_chars() < 7)[
    "atc_code"
].n_unique()
all_unique = csv_df["atc_code"].n_unique()
n_clean_unique = clean_csv_df["atc_code"].n_unique()
short_count = csv_df.filter(pl.col("atc_code").str.len_chars() < 7).shape[0]
null_count = csv_df["atc_code"].null_count()

print(
    f"all={all_count}\n"
    f"clean={clean_count}\n"
    f"all_unique={all_unique}\n"
    f"n_clean_unique={n_clean_unique}\n"
    f"all shorter than 7 chars={short_count}\n"
    f"all unique shorter than 7 chars={all_unique_short}\n"
    f"n_nulls={null_count}\n"
)

csv_scrape_inner = set(
    clean_csv_df.join(scrape_atc_df, on=["atc_code"], how="inner")[
        "atc_code"
    ].to_list()
)
csv_set = set(clean_csv_df["atc_code"].to_list())
scrape_set = set(scrape_atc_df["atc_code"].to_list())
de_set = set(de_atc_df["atc_code"].to_list())

print(
    f"{len(csv_set & scrape_set)=}\n",
    f"{len(csv_set - scrape_set)=}\n",
    f"{len(csv_set & de_set)=}\n",
    f"{len(csv_set - de_set)=}\n",
    f"{len((csv_set & de_set) - (csv_set & scrape_set))=}\n",
    f"{len((csv_set - de_set) & (csv_set - scrape_set))=}\n",
)
# csv_scrape_anti = clean_csv_df.join(scrape_atc_df, on=["atc_code"], how="anti")

# csv_de_inner = clean_csv_df.join(de_atc_df, on=["atc_code"], how="inner")
# csv_de_anti = clean_csv_df.join(de_atc_df, on=["atc_code"], how="anti")

# inner_inner_anti = csv_scrape_inner.join(
#     csv_de_inner, on=["atc_code"], how="anti"
# )
# anti_anti_inner = csv_scrape_anti.join(
#     csv_de_anti, on=["atc_code"], how="inner"
# )
# print(csv_de_anti["atc_code"])
# print(inner_inner_anti["atc_code"])


# print(
#     f"csv scrape intersection={csv_scrape_inner.shape[0]}\n"
#     f"csv - scrape ={csv_scrape_anti.shape[0]}\n"
#     f"csv - scrape unique={csv_scrape_anti['atc_code'].n_unique()}\n"
#     f"csv de intersection={csv_de_inner.shape[0]}\n"
#     f"csv - de ={csv_de_anti.shape[0]}\n"
#     f"csv - de unique={csv_de_anti['atc_code'].n_unique()}\n",
#     f"inner inner anti = {inner_inner_anti.shape[0]}\n",
#     f"inner inner anti  unique= {inner_inner_anti['atc_code'].n_unique()}\n",
#     f"anti anti intersection= {anti_anti_inner.shape[0]}\n",
#     f"anti anti intersection unique= {anti_anti_inner['atc_code'].n_unique()}\n",
#     f"anti anti intersection= {anti_anti_inner.shape[0]}\n",
#     f"anti anti intersection unique= {anti_anti_inner['atc_code'].n_unique()}\n",
# )
