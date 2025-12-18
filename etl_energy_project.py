import numpy as np
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

# -----------------------
# Config chemins
# -----------------------
PATH_ELECDOM = "./data/Fichiers de données conso annuelles_V1.csv"
PATH_CONSO_REGION = "./data/consommation-annuelle-d-electricite-et-gaz-par-region.csv"
PATH_PROD_IRIS = "./data/production-electrique-annuelle-par-filiere-a-la-maille-iris.csv"

OUT_FACT_CONSO = "fact_consumption_region_year.csv"
OUT_FACT_PROD = "fact_production_region_year_filiere.csv"
OUT_FACT_ELECDOM = "fact_elecdom_usage_household.csv"
OUT_MART = "mart_region_year_energy.csv"


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


# -----------------------
# 1) ElecDom (usage résidentiel)
# -----------------------
def build_fact_elecdom():
    df = pd.read_csv(PATH_ELECDOM, sep=";", encoding="latin-1")

    # Colonnes attendues :
    # 'Appareil suivi', 'ID logement', 'Consommation annuelle  AN1', 'Consommation annuelle AN2', 'Type'
    df = df.rename(
        columns={
            "Appareil suivi": "appareil_suivi",
            "ID logement": "id_logement",
            "Consommation annuelle  AN1": "conso_an1_kwh",
            "Consommation annuelle AN2": "conso_an2_kwh",
            "Type": "type",
        }
    )

    df["id_logement"] = df["id_logement"].apply(safe_int)
    df["conso_an1_kwh"] = df["conso_an1_kwh"].apply(to_float_fr)
    df["conso_an2_kwh"] = df["conso_an2_kwh"].apply(to_float_fr)

    # Fact
    fact = df[
        ["id_logement", "appareil_suivi", "type", "conso_an1_kwh", "conso_an2_kwh"]
    ].copy()
    fact.to_csv(OUT_FACT_ELECDOM, index=False)
    return fact


# -----------------------
# 2) Consommation régionale élec/gaz
# -----------------------
def build_fact_consumption_region_year():
    df = pd.read_csv(PATH_CONSO_REGION, sep=";", encoding="utf-8")

    df = df.rename(
        columns={
            "Année": "year",
            "Code Région": "region_code",
            "Nom Région": "region_name",
            "FILIERE": "carrier",
            "Nb sites": "nb_sites",
            "Conso totale (MWh)": "conso_totale_mwh",
            "Nombre d'habitants": "habitants",
        }
    )

    # Nettoyages
    df["year"] = df["year"].apply(safe_int)
    df["region_code"] = df["region_code"].apply(safe_int)

    df["nb_sites"] = df["nb_sites"].apply(to_float_fr)
    df["conso_totale_mwh"] = df["conso_totale_mwh"].apply(to_float_fr)
    df["habitants"] = df["habitants"].apply(to_float_fr)

    # On garde utile
    keep = [
        "year",
        "region_code",
        "region_name",
        "carrier",
        "nb_sites",
        "conso_totale_mwh",
        "habitants",
    ]
    df = df[keep].copy()

    # Agrégation (car plusieurs opérateurs/secteurs)
    fact = df.groupby(
        ["year", "region_code", "region_name", "carrier"], as_index=False
    ).agg(
        {
            "conso_totale_mwh": "sum",
            "nb_sites": "sum",
            "habitants": "max",  # habitants identique => max pour garder une valeur
        }
    )

    fact.to_csv(OUT_FACT_CONSO, index=False)
    return fact


# -----------------------
# 3) Production par filière (IRIS -> région/année)
# -----------------------
def build_fact_production_region_year_filiere():
    df = pd.read_csv(PATH_PROD_IRIS, sep=",", encoding="utf-8")

    # Colonnes de production (wide -> long)
    filiere_cols = {
        "energie_produite_annuelle_photovoltaique_enedis_mwh": "photovoltaique",
        "energie_produite_annuelle_eolien_enedis_mwh": "eolien",
        "energie_produite_annuelle_hydraulique_enedis_mwh": "hydraulique",
        "energie_produite_annuelle_bio_energie_enedis_mwh": "bio_energie",
        "energie_produite_annuelle_cogeneration_enedis_mwh": "cogeneration",
        "energie_produite_annuelle_autres_filieres_enedis_mwh": "autres_filieres",
    }

    df = df.rename(
        columns={
            "annee": "year",
            "code_region": "region_code",
            "nom_region": "region_name",
        }
    )

    df["year"] = df["year"].apply(safe_int)
    df["region_code"] = df["region_code"].apply(safe_int)

    # Convert numerics
    for c in filiere_cols.keys():
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Mise au format long (filiere, production_mwh)
    long_df = df.melt(
        id_vars=["year", "region_code", "region_name"],
        value_vars=list(filiere_cols.keys()),
        var_name="filiere_col",
        value_name="production_mwh",
    )
    long_df["filiere"] = long_df["filiere_col"].map(filiere_cols)
    long_df = long_df.drop(columns=["filiere_col"])

    # Agrégation IRIS -> région
    fact = long_df.groupby(
        ["year", "region_code", "region_name", "filiere"], as_index=False
    ).agg({"production_mwh": "sum"})

    fact.to_csv(OUT_FACT_PROD, index=False)
    return fact


# -----------------------
# 4) Data mart final
# -----------------------
def build_mart_region_year_energy(fact_conso, fact_prod):
    # Pivot consommation : 1 ligne par (region, year)
    conso_piv = fact_conso.pivot_table(
        index=["year", "region_code", "region_name"],
        columns="carrier",
        values="conso_totale_mwh",
        aggfunc="sum",
    ).reset_index()

    # Renommage colonnes selon libellés présents
    # (souvent "Electricité" et "Gaz")
    conso_cols = {
        c: c.lower().replace("é", "e").replace(" ", "_")
        for c in conso_piv.columns
        if c not in ["year", "region_code", "region_name"]
    }
    conso_piv = conso_piv.rename(columns=conso_cols)

    # Habitants (une valeur par région/année)
    hab = fact_conso.groupby(
        ["year", "region_code", "region_name"], as_index=False
    ).agg({"habitants": "max"})
    conso_piv = conso_piv.merge(
        hab, on=["year", "region_code", "region_name"], how="left"
    )

    # Production : total + parts
    prod_tot = fact_prod.groupby(
        ["year", "region_code", "region_name"], as_index=False
    ).agg(production_totale_mwh=("production_mwh", "sum"))
    prod_piv = fact_prod.pivot_table(
        index=["year", "region_code", "region_name"],
        columns="filiere",
        values="production_mwh",
        aggfunc="sum",
    ).reset_index()

    mart = conso_piv.merge(
        prod_tot, on=["year", "region_code", "region_name"], how="left"
    )
    mart = mart.merge(prod_piv, on=["year", "region_code", "region_name"], how="left")

    # Calculs d’indicateurs
    # Conso électricité (nom colonne variable -> on essaye de la trouver)
    elec_col = None
    for candidate in [
        "electricite",
        "électricité",
        "electricite_mwh",
        "électricité_mwh",
    ]:
        if candidate in mart.columns:
            elec_col = candidate
            break
    # Dans ton CSV, le pivot crée souvent "Electricité" -> "electricite"
    if elec_col is None and "electricite" in mart.columns:
        elec_col = "electricite"

    if elec_col is not None:
        mart["conso_elec_par_habitant_kwh"] = (mart[elec_col] * 1000) / mart[
            "habitants"
        ]
        mart["ratio_prod_sur_conso_elec"] = (
            mart["production_totale_mwh"] / mart[elec_col]
        )

    # Parts par filière (%)
    filieres = [
        "photovoltaique",
        "eolien",
        "hydraulique",
        "bio_energie",
        "cogeneration",
        "autres_filieres",
    ]
    for f in filieres:
        if f in mart.columns:
            mart[f"part_{f}_pct"] = (mart[f] / mart["production_totale_mwh"]) * 100

    mart.to_csv(OUT_MART, index=False)
    return mart

#Main
def main():
    fact_elecdom = build_fact_elecdom()
    fact_conso = build_fact_consumption_region_year()
    fact_prod = build_fact_production_region_year_filiere()
    mart = build_mart_region_year_energy(fact_conso, fact_prod)

    print("OK. Fichiers générés :")
    print(" -", OUT_FACT_ELECDOM)
    print(" -", OUT_FACT_CONSO)
    print(" -", OUT_FACT_PROD)
    print(" -", OUT_MART)


if __name__ == "__main__":
    main()
