# Projet Spark - Pipeline MovieLens 

Pipeline data de bout en bout en PySpark (mode local), sur le jeu de données
**MovieLens**. Architecture **bronze -> silver -> gold** : lecture des CSV bruts,
nettoyage et écriture en Parquet, puis trois analyses, une optimisation, une
lecture de la Spark UI et une exploration.

Le livrable détaillé est le fichier [`rapport.md`](rapport.md).

---

## Prérequis

- Python 3.11
- Java 17 ou 21 (nécessaire pour Spark 4)
- Les dépendances Python listées dans `requirements.txt` (PySpark)

## Installation

```bash
# 1. Créer et activer un environnement virtuel
python -m venv env
source env/bin/activate          # sous Windows : .venv\Scripts\activate

# 2. Installer les dépendances
pip install -r requirements.txt

# 3. Télécharger les données MovieLens (dans data/datasets/)
bash get_data.sh
```

Le script `get_data.sh` télécharge le jeu `ml-latest-small` depuis GroupLens et le
décompresse dans `data/datasets/ml-latest-small/`. Les données ne sont pas versionnées
sur Git (voir `.gitignore`) : il faut donc lancer ce script après avoir cloné le repo.

## Lancement

Toujours exécuter **depuis la racine du projet** (les chemins sont relatifs).

```bash
# Pipeline complet : ingestion -> nettoyage -> silver -> analyses -> gold
python pipeline.py

# Mesure de l'optimisation (jointure sans/avec broadcast : temps + plan)
python optimisation.py

# Exploration (partition pruning + predicate pushdown sur la couche Parquet)
python exploration.py
```

Le pipeline crée automatiquement les dossiers de sortie :
- `data/output/clean/` : couche silver (Parquet nettoyé)
- `data/output/analyses/` : couche gold (résultats des 3 analyses)

Pendant l'exécution, la Spark UI est disponible sur http://localhost:4040.

---

## Structure du projet 

```
spark-projet-movielens/
├── README.md              # ce fichier
├── rapport.md             # le rapport détaillé (livrable noté)
├── requirements.txt       # dépendances Python (PySpark)
├── get_data.sh            # téléchargement des données MovieLens
├── spark_session.py       # helper : création de la SparkSession
├── pipeline.py            # ingestion, nettoyage, silver, analyses, gold
├── optimisation.py        # mesure de l'optimisation broadcast
├── exploration.py         # exploration : partition pruning + predicate pushdown
├── docs/
│   └── captures/          # captures d'écran de la Spark UI (pour le rapport)
└── data/                  # données et sorties (NON versionnées)
    ├── datasets/          # CSV bruts (créés par get_data.sh)
    └── output/            # silver + gold (créés par pipeline.py)
```

---

## Répartition du travail

Projet réalisé par **Nassim Benchikh** et **Harold Lerner**.

| Partie                                        | Réalisée par                          |
| --------------------------------------------- | ------------------------------------- |
| Ingestion                                     | Nassim                                |
| Nettoyage + silver                            | Nassim & Harold                       |
| Les 3 analyses (agrégation, jointure, window) | Harold                                |
| Optimisation (broadcast)                      | Nassim                                |
| Lecture de la Spark UI                        | Nassim                                |
| Exploration                                   | Nassim                                |
| Rapport                                       | Nassim & Harold (chacun ses sections) |

Chacun a rédigé les sections du rapport correspondant à sa partie.