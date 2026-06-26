#!/usr/bin/env bash
# Telecharge le jeu MovieLens (ml-latest-small) dans data/datasets/.
set -e

mkdir -p data/datatsets

echo "== Telechargement de MovieLens ml-latest-small =="
curl -fSL https://files.grouplens.org/datasets/movielens/ml-latest-small.zip \
  -o data/datasets/ml-latest-small.zip

echo "== Decompression =="
cd data/datasets
unzip -o ml-latest-small.zip
cd ../..

echo ""
echo "Termine. Contenu :"
ls -lh data/datasets/ml-latest-small/