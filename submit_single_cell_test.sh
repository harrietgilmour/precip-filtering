#!/bin/bash
#SBATCH --mem=0000
#SBATCH --ntasks=4
#SBATCH --time=10

#Extract args from command line
mask_file=$1
precip_file=$2
tracks_file=$3
cell=$4

echo "$mask_file"
echo "$precip_file"
echo "$tracks_file"
echo "$cell"

python single_cell_loop.py ${mask_file} ${precip_file} ${tracks_file} ${cell}
