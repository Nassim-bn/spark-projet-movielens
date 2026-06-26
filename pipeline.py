import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, LongType, StringType,
)

from spark_session import get_spark

# ----- Chemins des entrées et sorties de MovieLens

RATINGS_RAW = "data/datasets/ml-latest-small/ratings.csv"
MOVIES_RAW  = "data/datasets/ml-latest-small/movies.csv"
SORTIE_SILVER = "data/output/clean"      # parquet de ratings et movies après nettoyage
SORTIE_GOLD   = "data/output/analyses"   # résultats des aggregations, jointure et window functions

# Raings max et min des films
RATING_MIN, RATING_MAX = 0.5, 5.0


# ----- Schémas explicites pour l'ingestion de ratings et movies

SCHEMA_RATINGS = StructType([
    StructField("userId",    IntegerType(), True),
    StructField("movieId",   IntegerType(), True),
    StructField("rating",    DoubleType(),  True),   # note entre 0.5 et 5.0
    StructField("timestamp", LongType(),    True),   # secondes depuis 1970 (unix timestamp)
])

SCHEMA_MOVIES = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title",   StringType(),  True),
    StructField("genres",  StringType(),  True),     # genres des films séparés par des '|'
])



# INGESTION
def ingestion(spark):
    """
    - Lit ratings et movies en CSV avec un SCHÉMA EXPLICITE au lieu de inferSchema.
    - On impose les types nous-mêmes pour éviter des surprises (ex: movieId = string au lieu d'int).
    """
    ratings = (
        spark.read
        .option("header", True)          # 1re ligne pour noms de colonnes
        .schema(SCHEMA_RATINGS)
        .csv(RATINGS_RAW)
    )
    movies = (
        spark.read
        .option("header", True)
        .schema(SCHEMA_MOVIES)
        .csv(MOVIES_RAW)
    )

    print("\n INGESTION \n")
    print("Schéma ratings :")
    ratings.printSchema()
    print("Aperçu ratings :")
    ratings.show(5) # pas de show pour movies car show() est une action et déclenche un job Spark
    print(f"ratings -> lignes : {ratings.count()} | colonnes : {len(ratings.columns)}")
    print(f"movies  -> lignes : {movies.count()} | colonnes : {len(movies.columns)}")

    return ratings, movies # on retorune 2 DataFrames (ratings, movies) au lieu d'un


# NETTOYAGE
def nettoyage(ratings, movies):
    """
    - Nettoie ratings et movies pour la couche silver et enrichit avec des nouvelles colonnes utiles.
     - ratings : Extrait l'année, retire les valeurs manquantes et aberrantes, retire les doublons.
     - movies  : Retire les valeurs manquantes et doublons, marque les films sans genre avec une colonne genres_present=False
    Retourne (ratings_propre, movies_propre).
    """
    print("\n NETTOYAGE :\n")

    # 1/ RATINGS 
    # On cree une colonne annee à partir du timestamp puis on la cast en entier pour avoir juste l'année
    ratings = ratings.withColumn(
        "annee", F.year(F.col("timestamp").cast("timestamp"))
    )
    # Nombre de lignes avant nettoyage
    r_avant = ratings.count()
    # Compte des doublons AVANT suppression
    r_doublons = r_avant - ratings.dropDuplicates(["userId", "movieId"]).count()
    ratings_propre = (
        ratings
        .na.drop(subset=["userId", "movieId", "rating"])                 #  valeurs manquantes
        .filter((F.col("rating") >= RATING_MIN) & (F.col("rating") <= RATING_MAX))  # ratings aberrants )
        .dropDuplicates(["userId", "movieId"])                          # Drop les spectateurs qui ont noté plusieurs fois le même film
    )
    r_apres = ratings_propre.count()

    print("----- ratings -----")
    print(f"  Nombre de lignes avant : {r_avant}")
    print(f"  Nombre de doublons détectés : {r_doublons}")
    print(f"  Nombre de lignes après : {r_apres}")
    print(f"  Nombre de lignes supprimées : {r_avant - r_apres}")

    # 2/ MOVIES 
    # Nombre de movies avant nettoyage
    m_avant = movies.count()
    # Compte des doublons AVANT suppression
    m_doublons = m_avant - movies.dropDuplicates(["movieId"]).count()
    movies_propre = (
        movies
        .na.drop(subset=["movieId", "title"])      # un film sans id/titre ---> inutile doncc drop
        .dropDuplicates(["movieId"])               # un seul enregistrement par film ---> drop les doublons
        # Marque les films sans genre SANS les supprimer
        .withColumn(
            "genres_present",
            (F.col("genres").isNotNull()) & (F.col("genres") != "(no genres listed)")
        )
    )
    # Nombre de movies après nettoyage
    m_apres = movies_propre.count()
    # Nombre de films sans genre (genres_present=False)
    m_sans_genre = movies_propre.filter(~F.col("genres_present")).count()

    print("----- movies -----")
    print(f"  Nombre de lignes avant : {m_avant}")
    print(f"  Nombre de doublons détectés : {m_doublons}")
    print(f"  Nombre de lignes après : {m_apres}")
    print(f"  Nombre de films sans genre : {m_sans_genre} (genres_present=False)")

    return ratings_propre, movies_propre # on retorune 2 df au lieu d'un



# ----- ÉCRITURE SILVER

def ecrire_silver(ratings_propre, movies_propre):
    """
    - Écrit la couche la partie silver (ratings et movies) en parquet
    - ratings : partitionné par annee car faible cardinalité
    - movies  : table de référence légère et pas beacoup de lignes, on écrit un seul fichier parquet (coalesce(1)) et pas de partionnement
    """
    print("\n ÉCRITURE SILVER \n")

    # Écriture des fichiers parquet dans le répertoire SORTIE_SILVER
    ratings_propre.write.mode("overwrite").partitionBy("annee").parquet(f"{SORTIE_SILVER}/ratings")
    print(f"ratings silver -> {SORTIE_SILVER}/ratings partitionné par annee")

    # Écriture movies en un seul fichier parquet dans le répertoire SORTIE_SILVER)
    movies_propre.coalesce(1).write.mode("overwrite").parquet(f"{SORTIE_SILVER}/movies")
    print(f"movies silver -> {SORTIE_SILVER}/movies sans partitionnement")






# --------------------------------------------------------------------------- #
# Étapes suivantes : EN TODO (remplies dans d'autres branches)
# --------------------------------------------------------------------------- #
def transformation_et_analyses(spark):
    raise NotImplementedError("TODO : analyses (autre branche).")


def ecrire_gold(resultats):
    raise NotImplementedError("TODO : écriture gold (autre branche).")


# --------------------------------------------------------------------------- #
def main():
    spark = get_spark("Projet MovieLens")
    print("Spark UI disponible sur http://localhost:4040")

    # ===== ÉTAPE 1 : ingestion -> nettoyage -> silver (ce qu'on teste ici) =====
    ratings, movies = ingestion(spark)
    ratings_propre, movies_propre = nettoyage(ratings, movies)
    ecrire_silver(ratings_propre, movies_propre)

    spark.stop()


if __name__ == "__main__":
    main()