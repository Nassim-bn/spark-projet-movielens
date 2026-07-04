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

# Seuil minimum de votes pour qu'un film entre dans le classement "mieux notés" (analyse jointure).
# Déterminé empiriquement : sur ml-latest-small, le 95e percentile de n_votes est ~46 (mean=10.4,
# median=3, 90e percentile=26). En dessous de 50 votes, le top est dominé par des films à 1-2 notes
# (moyenne instable) ; à 50, le classement se stabilise sur des films reconnus (Shawshank, Godfather,
# Fight Club...) tout en gardant 450 films (4.6% du catalogue noté) éligibles.
SEUIL_VOTES_FILM = 50

# Seuil minimum de votes pour qu'un film entre dans le classement par genre (analyse window).
# Déterminé empiriquement sur les 19 genres présents : à 20 votes, chaque genre garde au moins
# TOP_N=5 films éligibles (le pire cas, Documentary, tombe pile à 5) ; à 30, Documentary n'en a
# plus que 4 (top-N incomplet). 20 est donc le seuil le plus strict qui garantit un top-5 partout.
SEUIL_VOTES_GENRE = 20

# Nombre de films classés par genre dans l'analyse window.
TOP_N = 5


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






# ----- ANALYSE 2 : JOINTURE

def analyse_jointure(spark):
    """
    Analyse 2 - jointure : films les mieux notés, enrichis avec titre et genres.
    - Relit la couche silver (jamais le brut).
    - Agrège ratings par movieId (nombre de votes, note moyenne).
    - Joint avec movies (broadcast, car movies est une petite table de référence).
    """
    print("\n ANALYSE 2 : JOINTURE (films les mieux notés) \n")

    ratings_silver = spark.read.parquet(f"{SORTIE_SILVER}/ratings")
    movies_silver = spark.read.parquet(f"{SORTIE_SILVER}/movies")

    stats_films = (
        ratings_silver
        .groupBy("movieId")
        .agg(
            F.count("*").alias("n_votes"),
            F.round(F.avg("rating"), 3).alias("note_moyenne"),
        )
    )

    # Sans seuil, le top est dominé par des films à 1-2 votes (moyenne non significative).
    # On filtre sur SEUIL_VOTES_FILM (voir justification à la définition de la constante).
    avant_seuil = stats_films.count()
    stats_films = stats_films.filter(F.col("n_votes") >= SEUIL_VOTES_FILM)
    apres_seuil = stats_films.count()
    print(f"Films avant seuil : {avant_seuil} | après seuil (>= {SEUIL_VOTES_FILM} votes) : {apres_seuil}")

    top_films = (
        stats_films
        .join(F.broadcast(movies_silver), on="movieId", how="inner")
        .select("movieId", "title", "genres", "n_votes", "note_moyenne")
        .orderBy(F.desc("note_moyenne"), F.desc("n_votes"))
    )

    # Vérification de cardinalité : movies est dédupliqué sur movieId en amont (nettoyage),
    # donc le join ne doit pas dupliquer de lignes.
    print(f"Films agrégés (après seuil) : {stats_films.count()} | après jointure : {top_films.count()}")
    print("Plan d'exécution (vérifier la présence d'un BroadcastHashJoin) :")
    top_films.explain()
    print("Top 15 films les mieux notés :")
    top_films.show(15, truncate=False)

    return top_films


# ----- ANALYSE 3 : WINDOW FUNCTION

def analyse_fenetre(spark):
    """
    Analyse 3 - window function : top-N films les mieux notés, par genre.
    - Relit la couche silver (jamais le brut).
    - Explose la colonne genres (un film multi-genres apparaît dans chacun de ses genres).
    - Agrège ratings par (genre, movieId, title), filtre sur un seuil de votes minimum.
    - Classe les films au sein de chaque genre (Window.partitionBy) par note décroissante.
    """
    print("\n ANALYSE 3 : WINDOW FUNCTION (top films par genre) \n")

    ratings_silver = spark.read.parquet(f"{SORTIE_SILVER}/ratings")
    movies_silver = spark.read.parquet(f"{SORTIE_SILVER}/movies")

    genres_par_film = (
        movies_silver
        .filter(F.col("genres_present"))
        .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
        .select("movieId", "title", "genre")
    )

    stats_genre_film = (
        ratings_silver
        .join(genres_par_film, "movieId")
        .groupBy("genre", "movieId", "title")
        .agg(
            F.count("*").alias("n_votes"),
            F.round(F.avg("rating"), 3).alias("note_moyenne"),
        )
    )

    # Sans seuil, le rang 1 de chaque genre serait souvent un film à 1-2 votes.
    avant_seuil = stats_genre_film.count()
    stats_genre_film = stats_genre_film.filter(F.col("n_votes") >= SEUIL_VOTES_GENRE)
    apres_seuil = stats_genre_film.count()
    print(f"Paires (genre, film) avant seuil : {avant_seuil} | après seuil (>= {SEUIL_VOTES_GENRE} votes) : {apres_seuil}")

    fenetre_genre = Window.partitionBy("genre").orderBy(F.desc("note_moyenne"), F.desc("n_votes"))
    top_par_genre = (
        stats_genre_film
        .withColumn("rang", F.row_number().over(fenetre_genre))
        .filter(F.col("rang") <= TOP_N)
        .select("genre", "rang", "title", "note_moyenne", "n_votes")
        .orderBy("genre", "rang")
    )

    n_genres = top_par_genre.select("genre").distinct().count()
    print(f"Genres couverts : {n_genres} | lignes du classement (attendu <= {n_genres} * {TOP_N}) : {top_par_genre.count()}")
    print("Plan d'exécution (vérifier la présence d'un Window/Exchange partitionné par genre) :")
    top_par_genre.explain()
    print("Top 5 pour quelques genres :")
    top_par_genre.filter(F.col("genre").isin("Drama", "Comedy", "Action")).show(15, truncate=False)

    return top_par_genre


# --------------------------------------------------------------------------- #
def transformation_et_analyses(spark):
    resultats = {}
    resultats["jointure_top_films"] = analyse_jointure(spark)
    resultats["window_top_par_genre"] = analyse_fenetre(spark)
    # --- Analyse 1 : agrégation (TODO, branche feature/analyse-agregation-gold) ---
    return resultats


def ecrire_gold(resultats):
    """Écrit chaque résultat d'analyse en CSV (1 fichier, avec header) sous SORTIE_GOLD."""
    print("\n ÉCRITURE GOLD \n")
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(chemin)
        print(f"{nom} -> {chemin}")


# --------------------------------------------------------------------------- #
def main():
    spark = get_spark("Projet MovieLens")
    print("Spark UI disponible sur http://localhost:4040")

    # ===== ÉTAPE 1 : ingestion -> nettoyage -> silver (ce qu'on teste ici) =====
    ratings, movies = ingestion(spark)
    ratings_propre, movies_propre = nettoyage(ratings, movies)
    ecrire_silver(ratings_propre, movies_propre)

    # ===== ÉTAPE 2 : silver -> gold (analyses) =====
    resultats = transformation_et_analyses(spark)
    ecrire_gold(resultats)

    spark.stop()


if __name__ == "__main__":
    main()