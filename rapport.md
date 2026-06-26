# Rapport de projet - Pipeline Spark (Jour 4)


- **Équipe** : **Nassim Benchikh**, **Harold Lerner**
- **Jeu de données** : **MovieLens**
- **Date** : [...]

---

## 1. Jeu de données et schéma cible

- Source et volume : [...]
- Schéma cible (colonnes retenues, types) : [...]
- Questions métier visées : [...]

---

## 2. Pipeline (bronze -> silver -> gold)

```
brut (bronze)  ->  nettoyé (silver, Parquet)  ->  agrégé (gold)
```

- Nettoyage appliqué (filtres, manquants, doublons) : [...]
- Lignes brutes : [...] | après nettoyage : [...] | écartées : [...] %
- Partitionnement de la silver (colonne, pourquoi) : [...]

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
