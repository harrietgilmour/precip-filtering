#!/bin/sh -l
#
# This script submits the jobs to run the single_cell_loop.py script
#
# Usage: submit_all.single_cell_test.sh <year> <month>
#

# Check that the correct no of args has been passed
if [ $# -ne 2 ]; then
    echo "Usage: submit_all.single_cell_test.bash <year> <month>"
    exit 1
fi

# Extract the year and month from the command line
year=$1
month=$2

# load the txt file
txt_file_path="/data/users/hgilmour/precip-filtering/unique_cell_files"
txt_file_name = "unique_cells_${year}_${month}.txt"
# form the file path
txtfile= txt_file_path + txt_file_name

# Check that this file exists
if [ ! -f $txtfile ]; then
    echo "File not found!"
    exit 1
fi

# Set up the extractor script 
EXTRACTOR="/data/users/hgilmour/precip-filtering/submit.single_cell_test.sh"

# Set the output directory for the lOTUS OUTPUT
# Set up the output directory
OUTPUT_DIR="/data/users/hgilmour/precip-filtering/lotus_output/single_cell_test"
mkdir -p $OUTPUT_DIR

# We want to extract the array of values from the txtfile
unique_values_array = $(cat $txtfile)

# check that this array looks good
echo "array of unique values: ${unique_values_array}"

# set up the mask, precip and tracks file
mask_dir="/data/users/hgilmour/tracking/tobac/code/tobac_sensitivity/Save"
mask_file="mask_${year}_${month}.nc"

precip_dir="/data/users/hgilmour/total_precip/precip_1h"
precip_file="precip_${year}_${month}.nc"

tracks_dir="/data/users/hgilmour/tracking/tobac/code/tobac_sensitivity/Save"
tracks_file="tracks_${year}_${month}.h5"

# form the file paths
mask=mask_dir + mask_file
precip=precip_dir + precip_file
tracks=tracks_dir + tracks_file

# loop over the cells
for cell in ${unique_values_array[@]}; do

    echo $cell
    echo $mask
    echo $precip
    echo $tracks

    # FIND THE SYNTAX FOR SBBATCH ON MET OFFICE PAGE

    # Set up the output files
    OUTPUT_FILE="$OUTPUT_DIR/all_single_cells_test.$year.$month.$cell.out"
    ERROR_FILE="$OUTPUT_DIR/all_single_cells_test.$year.$month.$cell.err"

    sbatch --mem=1000 --ntasks=4 --time=20 --output=$OUTPUT_FILE --error=$ERROR_FILE $EXTRACTOR $mask $precip $tracks $cell

done



