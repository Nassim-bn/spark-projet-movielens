"""Exploration (piste : pushdown mesuré).

Sur la couche Parquet partitionnée (ratings, partitionné par annee), on prouve :
  1. le partition pruning : filtrer sur annee -> Spark n'ouvre que le bon dossier
  2. le predicate pushdown : filtrer sur une colonne interne (rating) -> Spark
     pousse le filtre dans la lecture du fichier

"""

from pyspark.sql import functions as F

from spark_session import get_spark
from pipeline import SORTIE_SILVER


def main():
    spark = get_spark("Exploration pushdown")

    ratings = spark.read.parquet(f"{SORTIE_SILVER}/ratings")

    # 1) PARTITION PRUNING : filtre sur la colonne de partition (annee)
    print("\n 1) PARTITION PRUNING (filtre sur annee)")

    sans_filtre = ratings
    avec_annee  = ratings.filter(F.col("annee") == 2018)

    print(f"toutes les années : {sans_filtre.count()} lignes")
    print(f"année 2018 seule  : {avec_annee.count()} lignes")

    print("\n  --- Plan : on cherche PartitionFilters = [annee = 2018] ---")
    avec_annee.explain()

    # 2) PREDICATE PUSHDOWN : filtre sur une colonne interne (rating)
    print("\n 2) PREDICATE PUSHDOWN (filtre sur rating) ")

    avec_rating = ratings.filter(F.col("rating") >= 4.5)
    print(f"notes >= 4.5 : {avec_rating.count()} lignes")

    print("\n  --- Plan : on cherche PushedFilters = [rating >= 4.5] ---")
    avec_rating.explain()

    # 3) Pour CHIFFRER LES OCTETS LUS : voir la Spark UI (onglet SQL)
    input("\nEntrée pour quitter ..")

    spark.stop()


if __name__ == "__main__":
    main()