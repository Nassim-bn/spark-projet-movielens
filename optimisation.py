"""Mesure de l'optimisation broadcast sur la jointure ratings/movies.

Fichier à part (ne modifie pas pipeline.py). On compare une vraie jointure SANS
broadcast (SortMergeJoin + shuffle) contre une jointure AVEC broadcast
(BroadcastHashJoin). On force Spark à ne pas broadcaster tout seul pour voir la
vraie différence de plan.

Lancement : python optimisation.py
"""

import time
from pyspark.sql import functions as F

from spark_session import get_spark
from pipeline import SORTIE_SILVER


def mesurer_temps(df, nom):
    """Chronomètre un count() sur df. Le count force le calcul (Spark est paresseux).
    On lance une fois 'à blanc' avant pour enlever le coût de démarrage à froid."""
    df.count()                      # tour de chauffe (pas chronométré)
    debut = time.time()
    df.count()                      # tour mesuré
    duree = time.time() - debut
    print(f"  {nom} : {duree:.3f} s")
    return duree


def main():
    spark = get_spark("Mesure optimisation")

    ratings = spark.read.parquet(f"{SORTIE_SILVER}/ratings")
    movies  = spark.read.parquet(f"{SORTIE_SILVER}/movies")

    # Agrégation = point de départ de la jointure
    agg = (
        ratings.groupBy("movieId")
        .agg(
            F.count("*").alias("nb_votes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne"),
        )
        .filter(F.col("nb_votes") >= 50)
    )

    # --- SANS broadcast : on empêche Spark de broadcaster tout seul ---
    # -1 désactive le broadcast automatique -> Spark fait un vrai SortMergeJoin
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    jointure_sans = agg.join(movies, on="movieId", how="left")

    # --- AVEC broadcast : on force explicitement le broadcast de movies ---
    jointure_avec = agg.join(F.broadcast(movies), on="movieId", how="left")

    print("\n=== Jointure : sans broadcast vs avec broadcast ===")
    temps_sans = mesurer_temps(jointure_sans, "sans broadcast (SortMergeJoin)")
    temps_avec = mesurer_temps(jointure_avec, "avec broadcast (BroadcastHashJoin)")
    print(f"\n  gain : {temps_sans - temps_avec:.3f} s")

    print("\n--- Plan SANS broadcast (doit montrer SortMergeJoin + Exchange) ---")
    jointure_sans.explain()
    print("\n--- Plan AVEC broadcast (doit montrer BroadcastHashJoin) ---")
    jointure_avec.explain()

    spark.stop()


if __name__ == "__main__":
    main()