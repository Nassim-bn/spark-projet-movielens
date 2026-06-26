"""
analysis.py — Couche silver -> gold du projet MovieLens (ml-latest-small).

Étape 2 de l'énoncé : relire la couche Parquet propre (jamais le brut), produire
trois analyses (agrégation, jointure, window), mesurer une optimisation
(broadcast), écrire les résultats de synthèse en CSV (petits fichiers).

Analyses :
  1. Agrégation : popularité (volume) vs qualité (note moyenne) par genre.
  2. Jointure   : films les mieux notés, seuil de votes — broadcast(movies).
  3. Window     : top-N films par genre (classement intra-genre).
  (+ bonus léger : notes par utilisateur.)
"""

import glob
import os
import shutil
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

# --------------------------------------------------------------------------- #
RATINGS_SILVER = "data/silver/ratings"
MOVIES_SILVER = "data/silver/movies"
GOLD_DIR = "data/gold"

SEUIL_VOTES_FILM = 50      # films "les mieux notés" : minimum de votes
SEUIL_VOTES_GENRE = 20     # classement intra-genre : minimum de votes
TOP_N = 3                  # top-N par genre

KEEP_ALIVE = False         # True -> pause finale pour capturer la Spark UI (4040)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MovieLens - analyses")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
# Utilitaires
# --------------------------------------------------------------------------- #
def write_csv_single(df: DataFrame, path: str) -> None:
    """Écrit un DataFrame en UN seul CSV nommé (synthèse lisible).

    Spark ne nomme pas ses fichiers : on écrit en coalesce(1) dans un dossier
    temporaire, puis on renomme le part-*.csv unique vers le nom voulu.
    Valide en mode local (système de fichiers local).
    """
    tmp = path + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp)
    part = glob.glob(os.path.join(tmp, "part-*.csv"))[0]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    os.replace(part, path)
    shutil.rmtree(tmp)
    print(f"  -> synthèse écrite : {path}")


def genres_long(movies: DataFrame) -> DataFrame:
    """movies -> (movieId, title, genre), un genre par ligne.

    has_genres écarte les "(no genres listed)" (marqués en silver, pas supprimés).
    C'est ici, en gold, que le filtrage métier des genres s'applique.
    """
    return (
        movies.filter(F.col("has_genres"))
        .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
        .select("movieId", "title", "genre")
    )


# --------------------------------------------------------------------------- #
# Optimisation : broadcast — mesure par le plan (le chrono est du bruit à 100k)
# --------------------------------------------------------------------------- #
def mesure_broadcast(ratings: DataFrame, movies: DataFrame, spark: SparkSession) -> None:
    print("\n===== OPTIMISATION : broadcast(movies) =====")
    stats = ratings.groupBy("movieId").agg(F.count("*").alias("n_votes"))

    # 1) Forcer le sort-merge join (broadcast désactivé) pour comparer.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    print("\n--- Plan SANS broadcast (SortMergeJoin attendu) ---")
    stats.join(movies, "movieId").explain()

    # 2) Broadcast explicite de la petite table.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
    print("\n--- Plan AVEC broadcast (BroadcastHashJoin attendu) ---")
    stats.join(broadcast(movies), "movieId").explain()

    # 3) Timing indicatif (VOLUME FAIBLE : à lire avec prudence, bruit JVM).
    def timed(df, runs=5):
        df.count()                                  # warm-up
        t0 = time.perf_counter()
        for _ in range(runs):
            df.count()
        return (time.perf_counter() - t0) / runs

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    t_smj = timed(stats.join(movies, "movieId"))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
    t_bhj = timed(stats.join(broadcast(movies), "movieId"))
    print(f"\nTiming indicatif (5 runs) : sort-merge {t_smj*1000:.0f} ms | "
          f"broadcast {t_bhj*1000:.0f} ms")
    print("(à 100k lignes, l'écart est dans le bruit ; la preuve est le PLAN.)")


# --------------------------------------------------------------------------- #
# Analyse 1 — AGRÉGATION : popularité vs qualité par genre
# --------------------------------------------------------------------------- #
def analyse_agg_genre(ratings: DataFrame, movies: DataFrame) -> DataFrame:
    g = genres_long(movies)
    return (
        ratings.join(g, "movieId")
        .groupBy("genre")
        .agg(
            F.count("*").alias("n_notes"),
            F.round(F.avg("rating"), 3).alias("note_moy"),
            F.countDistinct("movieId").alias("n_films"),
        )
        .orderBy(F.desc("n_notes"))
    )


# --------------------------------------------------------------------------- #
# Analyse 2 — JOINTURE : films les mieux notés (seuil de votes) + broadcast
# --------------------------------------------------------------------------- #
def analyse_top_films(ratings: DataFrame, movies: DataFrame) -> DataFrame:
    stats = (
        ratings.groupBy("movieId")
        .agg(
            F.count("*").alias("n_votes"),
            F.round(F.avg("rating"), 3).alias("note_moy"),
        )
        .filter(F.col("n_votes") >= SEUIL_VOTES_FILM)
    )
    # broadcast de movies (petite) dans la jointure avec stats (issue du gros ratings)
    return (
        stats.join(broadcast(movies), "movieId")
        .select("title", "n_votes", "note_moy")
        .orderBy(F.desc("note_moy"), F.desc("n_votes"))
    )


# --------------------------------------------------------------------------- #
# Analyse 3 — WINDOW : top-N films par genre
# --------------------------------------------------------------------------- #
def analyse_window_genre(ratings: DataFrame, movies: DataFrame) -> DataFrame:
    g = genres_long(movies)
    par_film_genre = (
        ratings.join(g, "movieId")
        .groupBy("genre", "movieId", "title")
        .agg(
            F.count("*").alias("n_votes"),
            F.round(F.avg("rating"), 3).alias("note_moy"),
        )
        .filter(F.col("n_votes") >= SEUIL_VOTES_GENRE)
    )
    w = Window.partitionBy("genre").orderBy(F.desc("note_moy"), F.desc("n_votes"))
    return (
        par_film_genre.withColumn("rang", F.row_number().over(w))
        .filter(F.col("rang") <= TOP_N)
        .select("genre", "rang", "title", "note_moy", "n_votes")
        .orderBy("genre", "rang")
    )


# --------------------------------------------------------------------------- #
# Bonus léger — notes par utilisateur (agrégation)
# --------------------------------------------------------------------------- #
def analyse_par_user(ratings: DataFrame) -> DataFrame:
    return (
        ratings.groupBy("userId")
        .agg(
            F.count("*").alias("n_notes"),
            F.round(F.avg("rating"), 3).alias("note_moy"),
        )
        .orderBy(F.desc("n_notes"))
    )


# --------------------------------------------------------------------------- #
def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Relire la couche Parquet PROPRE (pas le brut).
        ratings = spark.read.parquet(RATINGS_SILVER)
        movies = spark.read.parquet(MOVIES_SILVER)

        # ratings est réutilisé par les 3 analyses -> cache (optimisation secondaire).
        ratings = ratings.cache()
        print("Lignes ratings (silver) :", ratings.count())

        # --- Optimisation mesurée ---
        mesure_broadcast(ratings, movies, spark)

        # --- Analyse 1 : agrégation par genre ---
        print("\n===== ANALYSE 1 — agrégation : par genre =====")
        a1 = analyse_agg_genre(ratings, movies)
        a1.show(truncate=False)
        write_csv_single(a1, f"{GOLD_DIR}/synthese_genres.csv")

        # --- Analyse 2 : jointure, top films (seuil de votes) ---
        print(f"\n===== ANALYSE 2 — jointure : top films (>= {SEUIL_VOTES_FILM} votes) =====")
        a2 = analyse_top_films(ratings, movies)
        a2.show(15, truncate=False)
        write_csv_single(a2, f"{GOLD_DIR}/top_films.csv")

        # --- Analyse 3 : window, top-N par genre ---
        print(f"\n===== ANALYSE 3 — window : top-{TOP_N} par genre (>= {SEUIL_VOTES_GENRE} votes) =====")
        a3 = analyse_window_genre(ratings, movies)
        a3.show(60, truncate=False)
        write_csv_single(a3, f"{GOLD_DIR}/top_par_genre.csv")

        # --- Bonus : notes par utilisateur ---
        print("\n===== BONUS — notes par utilisateur =====")
        a4 = analyse_par_user(ratings)
        a4.show(10, truncate=False)
        write_csv_single(a4, f"{GOLD_DIR}/notes_par_user.csv")

        print("\nAnalyses terminees. Synthèses sous", GOLD_DIR)

        if KEEP_ALIVE:
            input("\n[Spark UI sur http://localhost:4040 — Entrée pour fermer] ")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()