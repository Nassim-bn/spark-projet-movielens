#!/usr/bin/env bash
# Telecharge le jeu MovieLens (ml-latest-small) dans data/datasets/.
set -e

mkdir -p data/raw

echo "== Telechargement de MovieLens ml-latest-small =="
curl -fSL https://files.grouplens.org/datasets/movielens/ml-latest-small.zip \
  -o data/raw/ml-latest-small.zip

echo "== Decompression =="
cd data/raw
unzip -o ml-latest-small.zip
cd ../..

echo ""
echo "Termine. Contenu :"
ls -lh data/raw/ml-latest-small/