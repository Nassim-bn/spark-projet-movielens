"""Pipeline data du projet Jour 4 — jeu de données MovieLens.

Architecture cible (vue en cours) :
    brut (bronze) -> nettoyé (silver, Parquet) -> agrégé (gold, résultats)

Les méthodes sont remplies une par une, chacune dans sa branche, puis fusionnées dans la branchedev2 une fois testées.
Puis la branche dev2 est fusionnée dans main pour produire le livrable final.
"""

import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_session import get_spark

# --------------------------------------------------------------------------- #
# Chemins (adaptés à MovieLens)
# --------------------------------------------------------------------------- #
RATINGS_RAW = "data/datasets/ml-latest-small/ratings.csv"   # données brutes
MOVIES_RAW  = "data/datasets/ml-latest-small/movies.csv"
SORTIE_SILVER = "data/output/clean"                          # couche nettoyée (Parquet)
SORTIE_GOLD   = "data/output/analyses"                       # résultats d'analyses


def ingestion(spark):
    """Étape 1a : lire les données brutes (ratings + movies) avec schéma explicite.

    TODO (branche feat/ingestion) :
    - Lire les CSV avec un SCHÉMA EXPLICITE (StructType), pas inferSchema.
    - Inspecter : printSchema(), show(5), count().
    - Retourner les DataFrames bruts.
    """
    raise NotImplementedError("TODO ingestion : lire ratings et movies (schéma explicite).")


def nettoyage(df):
    """Étape 1b : typer, dériver des colonnes, nettoyer (bronze -> silver).

    TODO (branche feat/nettoyage) :
    - Dériver les colonnes utiles (ex. annee depuis le timestamp).
    - Filtrer les aberrants (note hors 0.5–5.0), gérer manquants (na.drop),
      retirer les doublons (dropDuplicates). Utiliser & | ~ et parenthéser.
    """
    raise NotImplementedError("TODO nettoyage : dérivez les colonnes et filtrez les aberrants.")


def ecrire_silver(df):
    """Étape 1c : écrire la couche nettoyée en Parquet.

    TODO (branche feat/ecriture-silver) :
    - write.mode("overwrite").parquet(SORTIE_SILVER).
    - partitionBy sur une colonne à FAIBLE cardinalité (ex. annee).
    """
    df.write.mode("overwrite").parquet(SORTIE_SILVER)
    print("Couche silver écrite dans", SORTIE_SILVER)


def transformation_et_analyses(spark):
    """Étape 2 : relire le propre, puis 3 analyses (silver -> gold).

    TODO : produire AU MOINS TROIS analyses :
    - une AGRÉGATION (groupBy + agg) ;
    - une JOINTURE (join, idéalement avec F.broadcast sur la petite table) ;
    - une WINDOW FUNCTION (Window.partitionBy(...).orderBy(...), row_number/rank/lag).
    Et au moins UNE OPTIMISATION justifiée : broadcast, cache, ou repartition.
    """
    df = spark.read.parquet(SORTIE_SILVER)

    # Optimisation cache : utile UNIQUEMENT si df est réutilisé par plusieurs analyses.
    df = df.cache()
    df.count()  # matérialise le cache

    # --- Analyse 1 : agrégation ---
    analyse_1 = None

    # --- Analyse 2 : jointure ---
    analyse_2 = None

    # --- Analyse 3 : window function ---
    analyse_3 = None

    if analyse_1 is None or analyse_2 is None or analyse_3 is None:
        raise NotImplementedError(
            "TODO analyses : produisez 3 analyses (agrégation, jointure, window)."
        )

    return {"analyse_1": analyse_1, "analyse_2": analyse_2, "analyse_3": analyse_3}


def ecrire_gold(resultats):
    """Étape 3 : écrire les résultats de synthèse.

    coalesce(1) est acceptable ICI car les résultats agrégés sont PETITS.
    Ne jamais coalesce(1) un gros DataFrame.
    """
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        df.coalesce(1).write.mode("overwrite").parquet(chemin)
        print("Résultat écrit :", chemin)


def main():
    spark = get_spark("Projet MovieLens")
    print("Spark UI disponible sur http://localhost:4040")

    # Étape 1 : ingestion et nettoyage (bronze -> silver)
    brut = ingestion(spark)
    propre = nettoyage(brut)
    ecrire_silver(propre)

    # Étape 2 : transformation et analyses (silver -> gold)
    resultats = transformation_et_analyses(spark)

    # Étape 3 : finalisation
    ecrire_gold(resultats)

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except NotImplementedError as e:
        print()
        print("Pipeline incomplet :", e)
        print("Complétez les sections TODO dans pipeline.py.")
        sys.exit(1)