from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType, StringType

spark = (
    SparkSession.builder
    .appName("Projet MovieLens")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "64")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- Schéma explicite de ratings ---
schema_ratings = StructType([
    StructField("userId",    IntegerType(), True),
    StructField("movieId",   IntegerType(), True),
    StructField("rating",    FloatType(),   True),
    StructField("timestamp", LongType(),    True),
])

ratings = (
    spark.read
    .option("header", True)
    .schema(schema_ratings)
    .csv("data/raw/ml-latest-small/ratings.csv")
)

ratings.printSchema()
ratings.show(5)
print("Lignes :", ratings.count())
print("Utilisateurs distincts :", ratings.select(F.countDistinct("userId")).collect()[0][0])
print("Films notés distincts   :", ratings.select(F.countDistinct("movieId")).collect()[0][0])


# --- Schéma explicite de movies ---
schema_movies = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title",   StringType(),  True),
    StructField("genres",  StringType(),  True),
])

movies = (
    spark.read
    .option("header", True)
    .schema(schema_movies)
    .csv("data/raw/ml-latest-small/movies.csv")
)

movies.printSchema()
movies.show(5, truncate=False)
print("Films au catalogue :", movies.count())



# === Nettoyage : bronze -> silver ===
# MovieLens est un jeu académique déjà propre, mais on valide les 3 axes
# attendus (doublons, manquants, aberrants) pour garantir des analyses fiables
avant = ratings.count()

ratings_propre = (
    ratings
    # Doublons : un utilisateur ne note qu'une fois un film donné
    # Une paire (userId, movieId) répétée = erreur d'import -> on la retire
    .dropDuplicates(["userId", "movieId"])

    # Manquants : une note sans user, film ou valeur est inexploitable
    # subset volontairement limité aux colonnes critiques (pas le timestamp)
    .na.drop(subset=["userId", "movieId", "rating"])

    # Aberrants : l'échelle MovieLens va de 0.5 à 5.0. Hors plage = erreur
    .filter((F.col("rating") >= 0.5) & (F.col("rating") <= 5.0))
)

apres = ratings_propre.count()
# Trace de contrôle : sert de preuve de nettoyage pour le rapport (bloc 2).
print("Avant :", avant, "| Après :", apres, "| Écartées :", avant - apres)



# === Enrichissement : dériver des colonnes utiles ===
# Le timestamp Unix (secondes depuis 1970) est illisible : on en extrait l'année utile pour analyser l'évolution des notes dans le temps
ratings_propre = ratings_propre.withColumn(
    "annee", F.year(F.from_unixtime(F.col("timestamp")))
)

ratings_propre.select("userId", "movieId", "rating", "annee").show(5)



# === Écriture de la couche silver (Parquet, partitionnée) ===
# Partition par année : faible cardinalité et clé naturelle

ratings_propre.write.mode("overwrite").partitionBy("annee").parquet("data/silver")

print("Couche silver écrite dans data/silver")