# Python script to generate array of unique cell values in a month of tobac tracking
#
# <USAGE> python unique_cells.py <TRACKS_FILE>
#
# <EXAMPLE> python unique_cells.py /data/users/hgilmour/tracking/code/tobac_sensitivity/Save/Track_precip_test.h5
#


# Import local packages
import os
import sys
import glob

# Import third party packages
import numpy as np
import pandas as pd

# Import and set up warnings
import warnings
warnings.filterwarnings('ignore')


# Write a function which will check the number of arguements passed
def check_no_args(args):
    """Check the number of arguements passed"""
    if len(args) != 2:
        print('Incorrect number of arguements')
        print('Usage: python unqiue_cells.py <TRACKS_FILE>')
        print('Example: python UNIQUE_CELLS.py /data/users/hgilmour/tracking/code/tobac_sensitivity/Save/Track_precip_test.h5')
        sys.exit(1)

# Write a function which loads the file
def open_dataset(tracks_file):
    """Load specified files"""

    #Load tracks file
    tracks = pd.read_hdf(tracks_file, 'table')

    return tracks


# Create a function to remove cells which are not part of a track
# i.e. these will have a cell value of -1
def remove_non_track_cells(tracks):
    """Removes cells which are not part of a track"""

    # Remove cells which are not part of a track
    tracks = tracks[tracks.cell >= 0]
    #print(tracks)

    return tracks


# Create a function which finds all of the unique track cell values
# within the tracks dataframe
def find_unique_cells(tracks):
    """Finds unique cell values within the tracks dataframe"""

    # Find the unique cell values within the tracks dataframe
    unique_cells = np.unique(tracks.cell.values)

    # Print the shape of the unique cells array
    print("The shape of the unique cells array is: ", np.shape(unique_cells))

    # Print the type of the unique cells array
    print("The type of the unique cells array is: ", type(unique_cells))
    
    # Print the unique cells array
    #print("The unique cells array is: ", unique_cells)

    return unique_cells


#Define the main function / filerting loop:
def main():
    """Main function."""

    # First extract the arguements:
    tracks_file = str(sys.argv[1])

    #check the number of arguements
    check_no_args(sys.argv)

    #first open the tracks dataset for 1 month
    tracks = open_dataset(tracks_file)

    # remove cells that do not form a track (i.e. have a value of -1)
    tracks = remove_non_track_cells(tracks)

    # create an array of unique cells witin the tracks dataframe
    unique_cells = find_unique_cells(tracks) 

    # Print the unique cells array
    print("The unique cells array is: ", unique_cells)

    # Save the unique cells array in the unique_cell_files directory
    np.savetxt('/data/users/hgilmour/precip-filtering/unique_cell_files/unique_cell_TEST.txt', unique_cells)
    print('Saved unique cell array for file {}'.format(tracks_file))


#Run the main function
if __name__ == "__main__":
    main()


