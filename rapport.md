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

__________________________________________________________________________________________________________________________________________

  # Rapport de projet — Pipeline Spark (Jour 4)

* Équipe : [noms]
* Jeu de données : MovieLens (ml-latest-small)
* Date : [...]

---

## 1. Jeu de données et schéma cible

* **Source et volume** : MovieLens ml-latest-small (GroupLens). 100 836 notes,
  610 utilisateurs. **9742 films au catalogue** (`movies.csv`) dont **9724 notés**
  (les 18 restants n'ont que des tags). 4 fichiers CSV ; on retient `ratings` et
  `movies` (les analyses portent sur eux ; `tags`/`links` non nécessaires au socle).
* **Schéma cible** (explicite, `StructType`, pas d'`inferSchema`) :
  * `ratings` : userId (int), movieId (int), **rating (double)**, timestamp (long, epoch UTC)
  * `movies`  : movieId (int), title (string), genres (string, séparés par `|`)
  * Dérivées : `annee` (ratings, depuis timestamp) ; `release_year` + `has_genres` (movies)
* **Questions métier visées** :
  1. Quels genres concentrent le visionnage, et sont-ils les mieux notés ? (agrégation)
  2. Quels films sont les mieux notés, à seuil minimal de votes ? (jointure)
  3. Top-N films par genre (classement intra-genre) ? (window)

---

## 2. Pipeline (bronze -> silver -> gold)

```
brut (bronze)  ->  nettoyé (silver, Parquet)  ->  agrégé (gold)
```

* **Nettoyage appliqué** (diagnostic en une passe par cause, puis filtres) :
  * Manquants : `na.drop` sur colonnes critiques (userId, movieId, rating ; movieId, title).
  * Aberrants : notes hors échelle officielle 0.5–5.0.
  * Dates incohérentes : garde-fou `annee` dans [1995, 2025].
  * Doublons : `dropDuplicates` en **dernier** — sur (userId, movieId) pour ratings,
    (movieId) pour movies. Ordre voulu : valider d'abord, dédupliquer ensuite (sinon
    `dropDuplicates` peut conserver une ligne aberrante).
* **Choix techniques notables** :
  * Fuseau session forcé à **UTC** : `F.year` dépend du fuseau ; sans ça, un rating
    proche de minuit UTC bascule d'année et fausse `annee`.
  * `release_year` extrait du titre `(YYYY)`. `regexp_extract` renvoie `""` (pas null)
    sur les 13 films sans année ; en **mode ANSI** (défaut Spark 4), `cast('' AS INT)`
    lève une exception → on neutralise `""` en null avant le cast.
  * Les 34 films `(no genres listed)` sont **marqués** (`has_genres=false`), pas supprimés :
    ils portent de vraies notes ; le filtrage genre est reporté en gold.

* **Lignes** :
  * ratings — brutes : 100 836 | après nettoyage : 100 836 | écartées : **0 %**
  * movies  — bruts  : 9 742   | après nettoyage : 9 742   | écartées : **0 %**
  * Jeu MovieLens déjà sain : le nettoyage **confirme** la qualité sans rejet. C'est une
    limite assumée (cf. §7), pas un défaut du pipeline.

* **Partitionnement de la silver** :
  * `ratings` : partitionnée par **`annee`** (faible cardinalité, 23 valeurs 1996→2018).
    Clé naturelle pour l'axe temporel ; support du **partition pruning** (cf. §6).
  * `movies` : **à plat** (non partitionnée). 9742 lignes ne justifient pas un découpage,
    qui ne produirait que des micro-fichiers ; c'est une table de dimension. La compétence
    « partitionnement » est démontrée sur ratings, là où elle a un effet réel.

---

## 3. Analyses

### Analyse 1 — agrégation

* **Question** : popularité (volume de notes) vs qualité (note moyenne) par genre.
* **Code clé** :



* **Résultat (extrait)** : `[à exécuter — coller la sortie de show()]`
* **Lecture métier** : `[à compléter — attendu : les genres les plus volumineux`
  `(Drama, Comedy) ne sont pas les mieux notés ; les niches (Film-Noir, War,`
  `Documentary) ont peu de notes mais des moyennes hautes]`

### Analyse 2 — jointure

* **Question** : films les mieux notés, avec un seuil minimal de votes (les titres
  exigent la jointure `ratings ⋈ movies`).
* **Code clé** :



* **Résultat (extrait)** : `[à exécuter]`
* **Lecture métier** : `[à compléter — sans seuil, le top serait pollué par des`
  `films à 1 ou 2 votes notés 5★ ; le seuil corrige ce biais classique du dataset]`

### Analyse 3 — window function

* **Question** : top-N films par genre (classement intra-genre).
* **Code clé** :



* **Résultat (extrait)** : `[à exécuter]`
* **Lecture métier** : `[à compléter — le seuil de votes évite qu'un film à note`
  `unique trône en tête de son genre]`

---

## 4. Optimisation

* **Optimisation choisie** : **broadcast** de `movies` dans la jointure avec `ratings`.
* **Pourquoi** : `movies` est petite (9742 lignes) ; la diffuser supprime le shuffle
  (`Exchange` + `SortMergeJoin`) côté `ratings` (la grosse table).
* **Mesure — par le plan, pas par le chrono** : sur 100 836 lignes, l'écart de temps
  est noyé dans le bruit JVM. On compare donc les **plans d'exécution** :

```
# sans broadcast (forcer le sort-merge pour comparer) :
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
ratings.join(movies, "movieId").explain()   # -> SortMergeJoin (+ 2 Exchange)

# avec broadcast :
ratings.join(broadcast(movies), "movieId").explain()  # -> BroadcastHashJoin (0 Exchange sur ratings)
```

* **Extrait de plan** : `[à insérer — les deux explain()]`
* **Ce que ça change** : suppression du shuffle de la grande table → moins d'I/O réseau
  et pas de tri. Gain structurel visible au plan, indépendant du volume.

---

## 5. Lecture de la Spark UI

* **Job observé** : `[à compléter — l'agrégation par genre, ou la window]`
* **Où se produit le shuffle (`Exchange`)** : `[à compléter — au groupBy("genre"),`
  `repartition par clé de regroupement]`
* **Nombre de stages et de tasks** : `[à relever sur localhost:4040 pendant le run]`
* **Capture(s)** : `[insérer le DAG montrant l'Exchange + le découpage stages/tasks]`
* **Commentaire** : `[à compléter]`
* *Rappel : ouvrir 4040 PENDANT l'exécution (l'UI meurt à la fin de la session locale).*

---

## 6. Exploration au-delà du cours

* **Piste choisie** : **pushdown mesuré** (partition pruning) sur la silver `ratings`
  partitionnée par `annee`.
* **Question** : un filtre sur `annee` permet-il à Spark de ne lire qu'une partition
  au lieu de scanner tout le jeu ?
* **Protocole** (un seul réglage varie, le reste fixe) :
  * Lecture sans filtre : `spark.read.parquet(".../ratings").agg(...)`.
  * Lecture avec filtre : `... .filter(F.col("annee") == 2018).agg(...)`.
  * On compare l'**input size / nombre de fichiers lus** dans la Spark UI.
* **Mesures** : `[à insérer — input size avec et sans filtre ; nombre de partitions lues]`
* **Conclusion** : `[à compléter — attendu : le filtre déclenche le pruning, Spark`
  `saute les dossiers annee=... non concernés ; octets lus très inférieurs]`

---

## 7. Ce qu'on a appris et limites

* **Ce qui a marché** :
  * Schéma explicite + diagnostic en une passe : ingestion propre, traçable.
  * Fuseau UTC explicite : cohérence de l'année (piège silencieux évité).
* **Ce qui a bloqué** :
  * Mode ANSI : `cast('' AS INT)` sur les 13 films sans année plante (au lieu de
    renvoyer null en mode legacy). Corrigé en neutralisant `""` avant le cast.
    Bon signal : l'échec bruyant a révélé un cas qu'un cast silencieux aurait masqué.
  * CRLF des CSV d'origine Windows : sans incidence côté Spark (parser tolérant),
    mais piège côté outils annexes.
* **Limites** :
  * Dataset déjà curé → le nettoyage est un **garde-fou**, il n'écarte aucune ligne.
    Sur un jeu sale, diagnostic par cause ≠ décompte cumulatif (recoupements).
  * `ml-latest-small` (100k lignes) : optimisation et exploration prouvées **par le
    plan / les octets lus**, pas par le chrono (trop bruité à ce volume).
* **Avec plus de temps** : rejouer optimisation et pushdown sur `ml-latest` (33 M de
  notes) pour des mesures de temps lisibles ; ajouter une analyse sur `tags` (bonus).
```