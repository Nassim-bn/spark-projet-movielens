"""
pipeline.py — Couche bronze -> silver du projet MovieLens (ml-latest-small).

Étape 1 de l'énoncé : lecture CSV brut avec schéma explicite (StructType, jamais
inferSchema), inspection (printSchema / show / count / describe), typage,
dérivations, nettoyage (manquants, aberrants, doublons), écriture de la couche
silver en Parquet PARTITIONNÉE (les deux tables).

Deux tables silver :
  - data/silver/ratings : partitionnée par annee (faible cardinalité, support pushdown).
  - data/silver/movies  : à plat (table de dimension, 9742 lignes ; future candidate
                          broadcast — orthogonal au partitionnement disque).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, LongType, StringType,
)

# --------------------------------------------------------------------------- #
# Chemins
# --------------------------------------------------------------------------- #
RATINGS_RAW = "data/raw/ml-latest-small/ratings.csv"
MOVIES_RAW = "data/raw/ml-latest-small/movies.csv"
RATINGS_SILVER = "data/silver/ratings"
MOVIES_SILVER = "data/silver/movies"

# Bornes métier (README MovieLens)
RATING_MIN, RATING_MAX = 0.5, 5.0
YEAR_MIN, YEAR_MAX = 1995, 2025          # garde-fou "dates incohérentes"

# --------------------------------------------------------------------------- #
# Schémas explicites (pas d'inferSchema)
# --------------------------------------------------------------------------- #
SCHEMA_RATINGS = StructType([
    StructField("userId",    IntegerType(), True),
    StructField("movieId",   IntegerType(), True),
    StructField("rating",    DoubleType(),  True),   # convention Spark (cf. moyennes gold)
    StructField("timestamp", LongType(),    True),   # secondes epoch UTC
])

SCHEMA_MOVIES = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title",   StringType(),  True),
    StructField("genres",  StringType(),  True),     # pipe-separated
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Projet MovieLens")
        .master("local[*]")
        # Timestamps UTC (README) : F.year dépend du fuseau de session.
        # Sans ça, un rating proche de minuit UTC bascule d'année.
        .config("spark.sql.session.timeZone", "UTC")
        # Volume modeste : 8 partitions de shuffle suffisent (64 sur-dimensionné).
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
# RATINGS : bronze -> silver
# --------------------------------------------------------------------------- #
def ingest_ratings(spark: SparkSession) -> None:
    print("\n##### RATINGS : bronze -> silver #####")

    bronze = (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")           # README : fichiers UTF-8
        .schema(SCHEMA_RATINGS)
        .csv(RATINGS_RAW)
    )

    # --- Inspection bronze (printSchema, show, count, describe) ---
    bronze.printSchema()
    bronze.show(5)
    print("Lignes :", bronze.count())
    print("Utilisateurs distincts :", bronze.select(F.countDistinct("userId")).first()[0])
    print("Films notés distincts  :", bronze.select(F.countDistinct("movieId")).first()[0])
    # describe sur les colonnes numériques : confirme rating dans [0.5,5.0]
    # et donne la plage de timestamp.
    bronze.describe("userId", "movieId", "rating", "timestamp").show()

    # --- Enrichissement : année de notation ---
    # cast long(epoch) -> timestamp (secondes), puis année. Session en UTC.
    enrichi = bronze.withColumn("annee", F.year(F.col("timestamp").cast("timestamp")))

    # --- Diagnostic en UNE passe (narrow) ---
    # Causes comptées sans enchaîner 5 actions. NB : diagnostic PAR CAUSE,
    # pas cumulatif -> recoupements possibles. Les doublons exigent un shuffle :
    # ils ne sont pas ici, déduits plus bas par différence.
    diag = enrichi.select(
        F.count("*").alias("brut"),
        F.sum((F.col("userId").isNull() | F.col("movieId").isNull()
               | F.col("rating").isNull()).cast("int")).alias("manquants"),
        F.sum(((F.col("rating") < RATING_MIN) | (F.col("rating") > RATING_MAX))
              .cast("int")).alias("aberrants"),
        F.sum(((F.col("annee") < YEAR_MIN) | (F.col("annee") > YEAR_MAX))
              .cast("int")).alias("dates_incoherentes"),
    ).first()

    # --- Nettoyage : VALIDER d'abord, DEDUPLIQUER en dernier ---
    valide = (
        enrichi
        .na.drop(subset=["userId", "movieId", "rating"])                            # manquants
        .filter((F.col("rating") >= RATING_MIN) & (F.col("rating") <= RATING_MAX))  # aberrants
        .filter((F.col("annee") >= YEAR_MIN) & (F.col("annee") <= YEAR_MAX))        # dates
    ).cache()
    n_valide = valide.count()

    silver = (
        valide
        .dropDuplicates(["userId", "movieId"])                                      # doublons (dernier)
        .select("userId", "movieId", "rating", "timestamp", "annee")
    ).cache()
    n_final = silver.count()

    # --- Écriture silver : repartition(annee) -> un fichier par année ---
    (
        silver.repartition("annee")
        .write.mode("overwrite")
        .partitionBy("annee")
        .parquet(RATINGS_SILVER)
    )

    # --- Bilan (preuve de nettoyage pour le rapport) ---
    print("\n----- Bilan nettoyage ratings (par cause) -----")
    print(f"  brut                : {diag['brut']}")
    print(f"  manquants           : {diag['manquants']}")
    print(f"  aberrants (note)    : {diag['aberrants']}")
    print(f"  dates incoherentes  : {diag['dates_incoherentes']}")
    print(f"  doublons ecartes    : {n_valide - n_final}   (n_valide - n_final)")
    print(f"  silver ecrite       : {n_final} lignes -> {RATINGS_SILVER}")

    valide.unpersist()
    silver.unpersist()


# --------------------------------------------------------------------------- #
# MOVIES : bronze -> silver
# --------------------------------------------------------------------------- #
def ingest_movies(spark: SparkSession) -> None:
    print("\n##### MOVIES : bronze -> silver #####")

    bronze = (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .schema(SCHEMA_MOVIES)
        .csv(MOVIES_RAW)
    )

    # --- Inspection bronze ---
    bronze.printSchema()
    bronze.show(5, truncate=False)
    print("Films au catalogue :", bronze.count())
    # describe : movieId est la seule numérique au stade brut (identifiant, peu
    # informatif) ; release_year sera décrite après dérivation.
    bronze.describe("movieId").show()

    # --- Diagnostic en une passe (narrow) ---
    diag = bronze.select(
        F.count("*").alias("brut"),
        F.sum((F.col("movieId").isNull() | F.col("title").isNull())
              .cast("int")).alias("manquants"),
        F.sum((F.col("genres") == "(no genres listed)").cast("int")).alias("sans_genre"),
    ).first()

    # --- Nettoyage : valider puis dédupliquer ---
    valide = bronze.na.drop(subset=["movieId", "title"]).cache()
    n_valide = valide.count()

    # --- Dérivations utiles ---
    # release_year : année extraite du titre "... (YYYY)" en fin de chaine.
    #   IMPORTANT : regexp_extract renvoie "" (chaine vide), PAS null, quand le
    #   motif ne matche pas (13 films sans annee dans ml-latest-small). En mode
    #   ANSI (defaut Spark 3+/4), cast('' AS INT) LEVE une exception au lieu de
    #   renvoyer null -> on neutralise "" en null AVANT le cast.
    # has_genres : marque les "(no genres listed)" SANS les supprimer (filtrage
    #   métier reporté en gold, au moment d'exploser les genres).
    silver = (
        valide
        .dropDuplicates(["movieId"])
        .withColumn("year_str", F.regexp_extract(F.col("title"), r"\((\d{4})\)\s*$", 1))
        .withColumn(
            "release_year",
            F.when(F.col("year_str") == "", None)        # "" -> null avant cast
             .otherwise(F.col("year_str"))
             .cast(IntegerType()),
        )
        .withColumn(
            "has_genres",
            (F.col("genres").isNotNull()) & (F.col("genres") != "(no genres listed)"),
        )
        .select("movieId", "title", "genres", "release_year", "has_genres")
    ).cache()
    n_final = silver.count()

    # describe sur la vraie numérique informative
    silver.describe("release_year").show()

    # --- Écriture silver : à plat (table de dimension), un seul fichier ---
    # Pas de partitionnement : 9742 lignes ne justifient pas un decoupage, qui
    # ne produirait que des micro-fichiers. La competence "partition" est
    # demontree sur ratings, ou elle a un sens reel (pushdown).
    silver.coalesce(1).write.mode("overwrite").parquet(MOVIES_SILVER)

    # --- Bilan ---
    print("\n----- Bilan nettoyage movies (par cause) -----")
    print(f"  brut                  : {diag['brut']}")
    print(f"  manquants             : {diag['manquants']}")
    print(f"  doublons ecartes      : {n_valide - n_final}   (n_valide - n_final)")
    print(f"  '(no genres listed)'  : {diag['sans_genre']}  (marques has_genres=false, conserves)")
    print(f"  silver ecrite         : {n_final} lignes -> {MOVIES_SILVER}")

    valide.unpersist()
    silver.unpersist()


# --------------------------------------------------------------------------- #
def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ingest_ratings(spark)
        ingest_movies(spark)
        print("\nIngestion terminee. Couche silver disponible sous data/silver/")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()