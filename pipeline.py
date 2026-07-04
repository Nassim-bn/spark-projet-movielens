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







# ----- Analyses
def transformation_et_analyses(spark):
    """Étape 2 : on relit les fichiers propres (silver) et on fait nos analyses."""

    # On relit ratings et movies séparément (ils sont dans 2 dossiers différents)
    ratings = spark.read.parquet(f"{SORTIE_SILVER}/ratings")
    movies  = spark.read.parquet(f"{SORTIE_SILVER}/movies")

    # On garde ratings en mémoire vu qu'on va s'en servir dans plusieurs analyses,
    # ça evite à Spark de le relire depuis le disque à chaque fois
    ratings = ratings.cache()
    ratings.count()  # ça force le cache à se remplir tout de suite

    # --- Analyse 1 : les films les mieux notés ---
    # Question: Quels sont les films les mieux notés ?
    # D'abord regrouper toutes les notes d'un même film pour avoir sa moyenne + combien de gens l'ont noté.
    # On garde que les films avec au moins 50 notes, sinon un film noté 1 seule fois à 5/5 
    # se retrouverait en haut du classement alors que ça veut rien dire.
    analyse_1 = (
        ratings
        .groupBy("movieId")
        .agg(
            F.count("*").alias("nb_votes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne"),
        )
        .filter(F.col("nb_votes") >= 50)
        .orderBy(F.desc("note_moyenne"))   # du mieux noté au moins bien noté
    )

    # --- Analyse 2 : jointure pour récupérer les titres ---
    # Question: Quels sont les meilleurs films avec leur titre et leur genre ?
    # On va chercher le titre + les genres dans la table movies, en collant les
    # deux tables sur movieId. Comme movies est une petite table, on la "broadcast" 
    # au lieu de faire un gros brassage entre les machines (shuffle), on l'envoie à
    # tout le monde, comme ca la jointure va beaucoup  plus vite.
    analyse_2 = (
        analyse_1
        .join(F.broadcast(movies), on="movieId", how="left")
        .select("movieId", "title", "genres", "nb_votes", "note_moyenne")
        .orderBy(F.desc("note_moyenne"))
    )

    # --- Analyse 3 : window function - top films par genre ---
    # Question : Quels sont les meilleurs films par genre ?
    # Problème : un film a plusieurs genres collés ("Action|Crime|Drama"). On doit
    # d'abord éclater ça pour avoir une ligne par (film, genre).
    films_genres = (
        analyse_2
        .filter(F.col("genres") != "(no genres listed)")   # on retire les films sans genre
        .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
    )

    # On classe les films à l'intérieur de chaque genre, du mieux noté au moins bien.
    # partitionBy = on fait un classement séparé PAR genre
    # orderBy = dans chaque genre, on trie par note
    fenetre = Window.partitionBy("genre").orderBy(F.desc("note_moyenne"))

    analyse_3 = (
        films_genres
        .withColumn("rang", F.row_number().over(fenetre))   # numéro dans son genre
        .filter(F.col("rang") <= 3)                         # on garde le top 3 par genre
        .select("genre", "rang", "title", "note_moyenne", "nb_votes")
        .orderBy("genre", "rang")
    )

    return {"analyse_1": analyse_1, "analyse_2": analyse_2, "analyse_3": analyse_3}

# ----- Ecriture des résultats des analyses en Parquet (couche gold)
def ecrire_gold(resultats):
    """Écrit les résultats des 3 analyses en Parquet (couche gold).
    """
    print("\n ÉCRITURE DANS GOLD \n")
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        df.coalesce(1).write.mode("overwrite").parquet(chemin)
        print(f"  {nom} -> {chemin}")


# --------------------------------------------------------------------------- #
def main():
    spark = get_spark("Projet MovieLens")
    print("Spark UI disponible sur http://localhost:4040")

    ratings, movies = ingestion(spark)
    ratings_propre, movies_propre = nettoyage(ratings, movies)
    ecrire_silver(ratings_propre, movies_propre)

    resultats = transformation_et_analyses(spark)
    ecrire_gold(resultats)

    # Pause pour explorer la Spark UI avant que la session se ferme
    input("\n>>> Spark UI ouverte sur http://localhost:4040 — appuie sur Entrée pour quitter <<<\n")

    spark.stop()


if __name__ == "__main__":
    main()