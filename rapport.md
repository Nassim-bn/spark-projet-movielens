## 1. Jeu de données et schéma cible

- Source et volume : MovieLens ml-latest-small (GroupLens). 100 836 notes,
  610 utilisateurs, 9 724 films notés. 4 fichiers CSV (ratings, movies, tags, links).
- Schéma cible (colonnes retenues, types) :
  - ratings : userId (int), movieId (int), rating (float), timestamp (long)
  - movies  : movieId (int), title (string), genres (string, séparés par |)
- Questions métier visées : [à compléter une fois les 3 analyses choisies]


## 2. Pipeline (bronze -> silver -> gold)

- Nettoyage appliqué :
  - Doublons : dropDuplicates sur (userId, movieId) — un seul vote par user/film.
  - Manquants : na.drop sur les colonnes critiques (userId, movieId, rating).
  - Aberrants : filtre des notes hors de l'échelle officielle 0.5–5.0.
- Lignes brutes : 100 836 | après nettoyage : 100 836 | écartées : 0 %
  (jeu MovieLens déjà sain : le nettoyage confirme la qualité, sans rejet).
- Partitionnement de la silver : par `annee` (faible cardinalité, 23 valeurs de
  1996 à 2018). Clé naturelle pour les analyses temporelles ; permet le partition
  pruning (lire une seule année sans scanner tout le jeu).