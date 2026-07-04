# Rapport de projet - Pipeline Spark


- **Équipe** : **Nassim Benchikh**, **Harold Lerner**
- **Jeu de données** : **MovieLens**
- **Date** : 04/07/2026

---
## 1. Jeu de données et schéma cible

* Source et volume : MovieLens ml-latest-small (GroupLens). 100 836 notes, 610 utilisateurs, 9 724 films notés (sur 9 742 au catalogue). 4 fichiers CSV ; on exploite ratings et movies (tags et links non utilisés pour le socle).
* Schéma cible (colonnes retenues, types) :
   * ratings : userId (int), movieId (int), rating (double), timestamp (long), annee (int, dérivée)
   * movies : movieId (int), title (string), genres (string, séparés par |), genres_present (bool, dérivée)
* Questions métier visées :
   * Quels sont les films les mieux notés du catalogue ? (agrégation)
   * Quels titres portent ces films les mieux notés ? (jointure)
   * Quels sont les 3 meilleurs films dans chaque genre ? (window)
## 2. Pipeline (bronze -> silver -> gold)

* Nettoyage ratings :
   * Manquants : na.drop sur (userId, movieId, rating).
   * Aberrants : notes filtrées hors de l'échelle 0.5–5.0.
   * Doublons : dropDuplicates sur (userId, movieId).
   * Lignes brutes : 100 836 | après : 100 836 | écartées : 0 % (jeu déjà sain).
* Nettoyage movies :
   * Manquants (movieId, title) et doublons (movieId) retirés.
   * Films sans genre marqués (genres_present=false) plutôt que supprimés : 34 films.
* Enrichissement : colonne `annee` dérivée du timestamp (ratings).
* Partitionnement silver :
   * ratings : partitionné par `annee` (faible cardinalité, ~23 valeurs) → permet le partition pruning.
   * movies : un seul fichier (coalesce(1)), table de référence légère.
---

## 3. Analyses

### Analyse 1 - agrégation


- Question : Quels sont les films les mieux notés du catalogue ?

- Code clé :
```python
analyse_1 = (
    ratings
    .groupBy("movieId")
    .agg(
        F.count("*").alias("nb_votes"),
        F.round(F.avg("rating"), 2).alias("note_moyenne"),
    )
    .filter(F.col("nb_votes") >= 50)      # au moins 50 votes pour une moyenne fiable
    .orderBy(F.desc("note_moyenne"))
)
```

- Résultat (extrait) :
```
+-------+--------+------------+
|movieId|nb_votes|note_moyenne|
+-------+--------+------------+
|318    |317     |4.43        |
|858    |192     |4.29        |
|2959   |218     |4.27        |
|1276   |57      |4.27        |
|750    |97      |4.27        |
|904    |84      |4.26        |
|1221   |129     |4.26        |
|1213   |126     |4.25        |
|48516  |107     |4.25        |
|912    |100     |4.24        |
+-------+--------+------------+
```
- Lecture métier : On regroupe toutes les notes par film pour obtenir sa note
  moyenne et son nombre de votes. Le seuil de 50 votes est important : sans lui,
  un film noté une seule fois à 5.0 remonterait en tête alors que sa moyenne
  n'est pas représentative. Le classement fait ressortir des films largement
  reconnus (le movieId 318 correspond à The Shawshank Redemption, 858 au Parrain),
  tous bien notés ET très vus. Les titres sont ajoutés dans l'analyse 2 (jointure).

### Analyse 2 - jointure

- Question : Quels sont les titres des films les mieux notés avec leur titre et genres ?

- Code clé :
```python
analyse_2 = (
    analyse_1
    .join(F.broadcast(movies), on="movieId", how="left")
    .select("movieId", "title", "genres", "nb_votes", "note_moyenne")
    .orderBy(F.desc("note_moyenne"))
)
```

- Résultat (extrait) :
```
+-------+---------------------------------------------------------------------------+---------------------------+--------+------------+
|movieId|title                                                                      |genres                     |nb_votes|note_moyenne|
+-------+---------------------------------------------------------------------------+---------------------------+--------+------------+
|318    |Shawshank Redemption, The (1994)                                           |Crime|Drama                |317     |4.43        |
|858    |Godfather, The (1972)                                                      |Crime|Drama                |192     |4.29        |
|2959   |Fight Club (1999)                                                          |Action|Crime|Drama|Thriller|218     |4.27        |
|1276   |Cool Hand Luke (1967)                                                      |Drama                      |57      |4.27        |
|750    |Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1964)|Comedy|War                 |97      |4.27        |
|904    |Rear Window (1954)                                                         |Mystery|Thriller           |84      |4.26        |
|1221   |Godfather: Part II, The (1974)                                             |Crime|Drama                |129     |4.26        |
|1213   |Goodfellas (1990)                                                          |Crime|Drama                |126     |4.25        |
|48516  |Departed, The (2006)                                                       |Crime|Drama|Thriller       |107     |4.25        |
|912    |Casablanca (1942)                                                          |Drama|Romance              |100     |4.24        |
+-------+---------------------------------------------------------------------------+---------------------------+--------+------------+
```
- Lecture métier : L'analyse 1 donnait les meilleurs films par leur numéro
  (movieId). Ici on joint la table **movies** sur movieId pour récupérer le titre et
  les genres, ce qui rend le classement lisible. On utilise **F.broadcast** sur movies
  car c'est une petite table (9 742 lignes) : Spark l'envoie à tous les workers au
  lieu de faire un shuffle coûteux, ce qui accélère la jointure. Le résultat
  confirme la qualité du classement (des grands classiques du cinéma remontent),
  et on remarque que le genre Crime|Drama domine le haut du tableau.

### Analyse 3 - window function

- Question : Quels sont les 3 meilleurs films dans chaque genre ?

- Code clé :
```python
# Un film a plusieurs genres collés ("Action|Crime") : on éclate en une ligne par genre
films_genres = (
    analyse_2
    .filter(F.col("genres") != "(no genres listed)")
    .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
)

# Classement séparé par genre, trié par note
fenetre = Window.partitionBy("genre").orderBy(F.desc("note_moyenne"))

analyse_3 = (
    films_genres
    .withColumn("rang", F.row_number().over(fenetre))
    .filter(F.col("rang") <= 3)      # top 3 de chaque genre
    .select("genre", "rang", "title", "note_moyenne", "nb_votes")
    .orderBy("genre", "rang")
)
```


- Résultat (extrait) :
```
+-----------+----+---------------------------------------------------------------------------+------------+--------+
|genre      |rang|title                                                                      |note_moyenne|nb_votes|
+-----------+----+---------------------------------------------------------------------------+------------+--------+
|Action     |1   |Fight Club (1999)                                                          |4.27        |218     |
|Action     |2   |Dark Knight, The (2008)                                                    |4.24        |149     |
|Action     |3   |Star Wars: Episode IV - A New Hope (1977)                                  |4.23        |251     |
|Adventure  |1   |Star Wars: Episode IV - A New Hope (1977)                                  |4.23        |251     |
|Adventure  |2   |Princess Bride, The (1987)                                                 |4.23        |142     |
|Adventure  |3   |Star Wars: Episode V - The Empire Strikes Back (1980)                      |4.22        |211     |
|Animation  |1   |Spirited Away (Sen to Chihiro no kamikakushi) (2001)                       |4.16        |87      |
|Animation  |2   |Toy Story 3 (2010)                                                         |4.11        |55      |
|Animation  |3   |WALL·E (2008)                                                              |4.06        |104     |
|Children   |1   |Toy Story 3 (2010)                                                         |4.11        |55      |
|Children   |2   |WALL·E (2008)                                                              |4.06        |104     |
|Children   |3   |Wallace & Gromit: The Wrong Trousers (1993)                                |4.04        |56      |
|Comedy     |1   |Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1964)|4.27        |97      |
|Comedy     |2   |Princess Bride, The (1987)                                                 |4.23        |142     |
|Comedy     |3   |Pulp Fiction (1994)                                                        |4.2         |307     |
|Crime      |1   |Shawshank Redemption, The (1994)                                           |4.43        |317     |
|Crime      |2   |Godfather, The (1972)                                                      |4.29        |192     |
|Crime      |3   |Fight Club (1999)                                                          |4.27        |218     |
|Documentary|1   |Bowling for Columbine (2002)                                               |3.78        |58      |
|Documentary|2   |Super Size Me (2004)                                                       |3.51        |50      |
|Drama      |1   |Shawshank Redemption, The (1994)                                           |4.43        |317     |
|Drama      |2   |Godfather, The (1972)                                                      |4.29        |192     |
|Drama      |3   |Cool Hand Luke (1967)                                                      |4.27        |57      |
|Fantasy    |1   |Princess Bride, The (1987)                                                 |4.23        |142     |
|Fantasy    |2   |Brazil (1985)                                                              |4.18        |59      |
|Fantasy    |3   |Spirited Away (Sen to Chihiro no kamikakushi) (2001)                       |4.16        |87      |
|Film-Noir  |1   |Chinatown (1974)                                                           |4.21        |59      |
|Film-Noir  |2   |L.A. Confidential (1997)                                                   |4.06        |97      |
|Film-Noir  |3   |Sin City (2005)                                                            |3.86        |84      |
|Horror     |1   |Silence of the Lambs, The (1991)                                           |4.16        |279     |
+-----------+----+---------------------------------------------------------------------------+------------+--------+
```
- Lecture métier : On veut le top des films par genre, mais  groupBy écraserait
  tout en une ligne par genre. La window function garde chaque film et lui donne un
  rang À L'INTÉRIEUR de son genre (partitionBy genre + row_number). Comme un film a
  plusieurs genres, on éclate d'abord la colonne genres avec split + explode : un
  film comme Star Wars se retrouve classé à la fois en Action et en Adventure. Le
  résultat fait remonter les références de chaque genre (Spirited Away en Animation,
  Chinatown en Film-Noir), ce qui confirme la pertinence du classement.

---

## 4. Optimisation

* Optimisation choisie : le broadcast, sur la jointure entre les films agrégés et la table movies.

* Pourquoi : movies est une petite table (9 742 lignes). Normalement, pour joindre
  deux tables, Spark doit déplacer les données entre les machines pour rapprocher
  les lignes qui ont le même movieId (c'est le shuffle, l'étape la plus lente vue
  en cours). Avec le broadcast, on envoie la petite table à toutes les machines,
  du coup chacune peut faire la jointure toute seule, sans avoir à déplacer les
  données. On évite donc le shuffle.

* Mesure avant / après :
```
sans broadcast : 0.378 s
avec broadcast : 0.254 s   (environ 33 % plus rapide)
```

* (On lance chaque jointure une première fois "à vide" avant de chronométrer, pour
  ne pas mesurer le temps de démarrage de Spark et avoir un temps plus juste.)

* Ce que ça change : sans broadcast, Spark déplace et trie les deux tables (on le
  voit dans le plan avec deux étapes "Exchange", qui sont les shuffles). Avec
  broadcast, il n'y a plus ce déplacement, d'où le gain de temps.

* Remarque : sur notre petit jeu de données, le gain est faible. Mais sur une
  grosse table, éviter le shuffle ferait une grosse différence, car déplacer des
  millions de lignes entre les machines coûte très cher.

---

## 5. Lecture de la Spark UI

- Job observé : [...]
- Où se produit le shuffle (`Exchange`) : [...]
- Nombre de stages et de tasks : [...]
- Capture(s) : [insérer]
- Commentaire : [...]

---

## 6. Exploration au-delà du cours

- Piste choisie : [AQE et partitions / skew et salting / UDF vs pandas_udf / table gérée et upsert /
  spark-submit / pushdown mesuré / benchmark formats / streaming ou MLlib]
- Question : [...]
- Protocole (ce qu'on a fait varier, ce qui reste fixe) : [...]
- Mesures :
```
[...]
```
- Conclusion (même si négative ou contre-intuitive) : [...]

---

## 7. Ce qu'on a appris et limites

- Ce qui a marché : [...]
- Ce qui a bloqué : [...]
- Ce qu'on ferait avec plus de temps : [...]
