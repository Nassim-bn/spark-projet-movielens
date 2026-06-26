# Rapport de projet - Pipeline Spark (Jour 4)


- **Équipe** : **Nassim Benchikh**, **Harold Lerner**
- **Jeu de données** : **MovieLens**
- **Date** : [...]

---
## 1. Jeu de données et schéma cible

- Source et volume : MovieLens ml-latest-small (GroupLens). 100 836 notes,
  610 utilisateurs, 9 724 films notés (sur 9 742 au catalogue). 4 fichiers CSV ;
  on exploite ratings et movies (tags et links non utilisés pour le socle).
- Schéma cible (colonnes retenues, types) :
  - ratings : userId (int), movieId (int), rating (double), timestamp (long), annee (int, dérivée)
  - movies  : movieId (int), title (string), genres (string, séparés par |), has_genres (bool, dérivée)
- Questions métier visées : [à compléter une fois les 3 analyses choisies]

## 2. Pipeline (bronze -> silver -> gold)

- Nettoyage ratings :
  - Manquants : na.drop sur (userId, movieId, rating).
  - Aberrants : notes filtrées hors de l'échelle 0.5–5.0.
  - Doublons : dropDuplicates sur (userId, movieId).
  - Lignes brutes : 100 836 | après : [TON CHIFFRE] | écartées : [X] %
- Nettoyage movies :
  - Manquants (movieId, title) et doublons (movieId) retirés.
  - Films sans genre marqués (has_genres=false) plutôt que supprimés : [N] films.
- Enrichissement : colonne `annee` dérivée du timestamp (ratings).
- Partitionnement silver :
  - ratings : partitionné par `annee` (faible cardinalité, ~23 valeurs) →
    permet le partition pruning.
  - movies : un seul fichier (coalesce(1)), table de référence légère.

---

## 3. Analyses

### Analyse 1 - agrégation

- Question : [...]
- Code clé :
```python
[...]
```
- Résultat (extrait) :
```
[...]
```
- Lecture métier : [...]

### Analyse 2 - jointure

- Question : [...]
- Code clé :
```python
[...]
```
- Résultat (extrait) :
```
[...]
```
- Lecture métier : [...]

### Analyse 3 - window function

- Question : [...]
- Code clé :
```python
[...]
```
- Résultat (extrait) :
```
[...]
```
- Lecture métier : [...]

---

## 4. Optimisation

- Optimisation choisie : [broadcast / cache / repartition]
- Pourquoi : [...]
- Mesure avant/après ou extrait de plan :
```
avant : [...] s   |   après : [...] s
(ou extrait de explain() montrant le changement)
```
- Ce que ça change : [...]

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
