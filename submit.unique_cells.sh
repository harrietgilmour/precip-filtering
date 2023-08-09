#!/bin/bash
#SBATCH --mem=0000
#SBATCH --ntasks=4
#SBATCH --time=10

#Extract args from command line
tracks_file=$1

echo "$tracks_file"

python unique_cells.py ${tracks_file}
