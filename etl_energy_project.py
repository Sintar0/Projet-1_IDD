import numpy as np
import pandas as pd
import sys
import os
import plotly.express as px
import json

sys.stdout.reconfigure(encoding='utf-8')

# -----------------------
# Config chemins
# -----------------------
PATH_CONSO_REGION = "./data/Consommation_brute_2025-11-17_16-30.csv"
PATH_PROD_REGION = "./data/energie_produite_region_mensuel_source.csv"
PATH_CONSO_ANNEE = "./data/consommation-annuelle-d-electricite-et-gaz-par-region.csv"

OUT_DIR = "./output/"
OUT_INFO_REGION = OUT_DIR + "infos_region.csv"
OUT_DATASET = OUT_DIR + "dataset_region_conso_prod.csv"
JSON_REGION = "./data/regions.geojson"

# -----------------------
# Helpers
# -----------------------
def to_float_fr(x):
    """Convertit '1,7' -> 1.7 ; garde NaN si vide."""
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return np.nan
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return np.nan


def safe_int(x):
    if pd.isna(x):
        return np.nan
    try:
        return int(float(x))
    except Exception:
        return np.nan


def region_info():
    """
     Extraction des informations régionales (code_region, nom_region, nb_sites, habitants)
    """
    if not os.path.exists(PATH_CONSO_ANNEE):
        raise FileNotFoundError(f"Le fichier {PATH_CONSO_ANNEE} est introuvable.")
    df = pd.read_csv(PATH_CONSO_ANNEE, sep="\t", encoding="latin1")

    df = df.rename(
        columns={
            "Code Région": "code_region",
            "Nom Région": "nom_region",
            "Nb sites" : "nb_sites",
            "Nombre d'habitants": "habitants"
        }
    )

    df["nb_sites"] = pd.to_numeric(df["nb_sites"], errors="coerce").fillna(0).astype(int)
    df["habitants"] = pd.to_numeric(df["habitants"], errors="coerce").fillna(0).astype(int)

    colonnes = [
        "code_region", "nom_region", "nb_sites", "habitants"
    ]

    df = df[colonnes].copy()

    df = (
        df.groupby("code_region", as_index=False)
        .agg(
            nom_region=("nom_region", "first"),
            nb_sites=("nb_sites", "sum"),
            habitants=("habitants", "sum")
        )
    )
    df.to_csv(OUT_INFO_REGION, index=False)
    return df 


def merge_conso_prod_region_info():
    if not os.path.exists(OUT_INFO_REGION):
        region_info()
    if not os.path.exists(PATH_CONSO_REGION):
        raise FileNotFoundError(f"Le fichier {PATH_CONSO_REGION} est introuvable.")
    if not os.path.exists(PATH_PROD_REGION):
        raise FileNotFoundError(f"Le fichier {PATH_PROD_REGION} est introuvable.")
    
    info_region = pd.read_csv(OUT_INFO_REGION, sep=",", encoding="utf-8-sig")
    conso_region = pd.read_csv(PATH_CONSO_REGION, sep=";", encoding="utf-8-sig")
    prod_region = pd.read_csv(PATH_PROD_REGION, sep=";", encoding="utf-8-sig")

    conso_region.columns = conso_region.columns.str.strip()
    prod_region.columns = prod_region.columns.str.strip()
    
    ## renommage colonnes pour merge
    conso_region = conso_region.rename(
        columns={
            "Région": "nom_region",
            "Filière": "filiere_conso",
            "Valeur (TWh)": "conso_twh"
        }
    )
    conso_region["conso_twh"] = (
        conso_region["conso_twh"]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .astype(float)
    )
    conso_region["Date"] = pd.to_datetime(conso_region["Date"])

    prod_region = prod_region.rename(
            columns={
                "Région": "nom_region",
                "Filière": "filiere_prod",
                "Valeur (TWh)": "prod_twh"
            }
        )
    prod_region["prod_twh"] = (
        prod_region["prod_twh"]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .astype(float)
    )

    prod_region["Date"] = pd.to_datetime(prod_region["Date"])

    conso_region = conso_region.merge(info_region, on="nom_region", how="left")
    prod_region = prod_region.merge(info_region, on="nom_region", how="left")


    ## merge conso et prod
    merged = conso_region.merge(
        prod_region,
        on=["code_region", "nom_region", "nb_sites", "habitants", "Date"],
        how="outer"
    )

    graphe_builder(conso_region)
    graphe_builder(prod_region)

    df = prod_cons_stat(merged)
    df.to_csv(OUT_DATASET, index=False)
    return df

def graphe_builder(dataset):
    df = dataset.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    valeur = df.columns[-1]
    df_region = (
        df.groupby(["Date", "nom_region"], as_index=False)
        .agg({valeur: "sum"})
    )
    title = ""
    if valeur == "conso_twh":
        titre = "Consommation Électrique par Région"
        legende = "Consommation (TWh)"
    else :
        titre = "Production Électrique par Région"
        legende = "Production (TWh)"

    fig = px.line(
        df_region,
        x="Date",
        y=valeur,
        color="nom_region",
        title=titre,
        labels={
            valeur: legende,
            "Date": "Date",
            "nom_region": "Région"
        }
    )
   
    fig.write_html(f"{valeur}.html")
    return 

def prod_cons_stat(dataframe):
    df = dataframe.copy()
    df["Date"] = pd.to_datetime(df["Date"])

    df["conso_region"] = df.groupby("nom_region")["conso_twh"].transform("sum")
    df["prod_region"] = df.groupby("nom_region")["prod_twh"].transform("sum")

    # Indice pondéré
    df["indice_pondere"] = (
        (df["prod_region"] - df["conso_region"])
        / df["conso_region"]
    )
    df["indice_par_habitant"] = (
        (df["prod_twh"] - df["conso_twh"]) * 1e6
        / df["habitants"]
    )
    df["classe"] = pd.cut(
        df["indice_pondere"],
        bins=[-1, -0.5, -0.1, 0.1, 0.5, 2],
        labels=["Très déficitaire", "Déficitaire", "Équilibrée", "Excédentaire", "Très excédentaire"]
    )
    return df 


def final_graphe():
    if not os.path.exists(OUT_DATASET):
        merge_conso_prod_region_info()
    df = pd.read_csv(OUT_DATASET, sep=",", encoding="utf-8-sig")

    df["Date"] = pd.to_datetime(df["Date"])
    # Sécuriser les colonnes numériques
    for col in ["conso_twh", "prod_twh"]:
        df[col] = (
            df[col]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .astype(float)
        )
    df_long = df.melt(
        id_vars=["Date", "nom_region"],
        value_vars=["conso_twh", "prod_twh"],
        var_name="type",
        value_name="twh"
    )
    fig = px.line(
        df_long,
        x="Date",
        y="twh",
        color="type",
        line_dash="type",
        facet_col="nom_region",
        facet_col_wrap=3,
        title="Comparaison consommation vs production par région",
        labels={
            "twh": "Énergie (TWh)",
            "type": "Type"
        }
    )

    fig.write_html("comparaison_conso_prod_regions.html")


def indice_contribution_graphe():
    if not os.path.exists(OUT_DATASET):
        raise FileNotFoundError(f"Le fichier {OUT_DATASET} est introuvable.")
    
    df_region = pd.read_csv(OUT_DATASET, sep=",", encoding="utf-8-sig")
    
    with open(JSON_REGION, encoding="utf-8") as f:
        geojson = json.load(f)
    
    fig = px.choropleth(
        df_region,
        geojson=geojson,
        locations="code_region",
        featureidkey="properties.code",
        color="indice_pondere",
        color_continuous_scale="RdYlGn",
        range_color=(-1, 1),
        hover_name="nom_region",
        hover_data={
            "indice_pondere": ":.2f",
            "conso_twh": ":.1f",
            "prod_twh": ":.1f"
        },
        title="Indice pondéré de contribution énergétique par région"
    )

    fig.update_geos(
        fitbounds="locations",
        visible=False
    )

    fig.write_html("carte_indice_pondere_regions.html")
    fig.show()


def main():
    merge_conso_prod_region_info()
    final_graphe()
    indice_contribution_graphe()

if __name__ == "__main__":
    main()
