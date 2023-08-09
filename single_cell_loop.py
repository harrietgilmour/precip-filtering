# Python script for filtering individual unique cells based on precipitation thresholds
#
# <USAGE> python single_cell_loop.py <MASK_FILE> <PRECIP_FILE> <TRACKS_FILE> <CELL>
#
# <EXAMPLE> python single_cell_loop.py /data/users/hgilmour/tracking/code/tobac_sensitivity/Save/Mask_Segmentation_tb_precip_test.nc /data/users/hgilmour/total_precip/precip_instant/precip_jan_2005.nc /data/users/hgilmour/tracking/code/tobac_sensitivity/Save/Track_precip_test.h5 12
#


# Import local packages
import os
import sys
import glob

# Import third party packages
import iris
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import iris.quickplot as qplt
import iris.plot as iplt
import datetime
import shutil
from six.moves import urllib
from pathlib import Path
import trackpy
from iris.time import PartialDateTime
import functions
import dask
import dask.array as da
import dask.distributed as dd
from dask import delayed
import cartopy.crs as ccrs
import xarray as xr
import netCDF4 as nc
import scipy
from scipy import ndimage
from scipy.ndimage import label, generate_binary_structure
import tobac #tobac package cloned from https://github.com/tobac-project/tobac.git


# Import and set up warnings
import warnings
warnings.filterwarnings('ignore', category=UserWarning, append=True)
warnings.filterwarnings('ignore', category=RuntimeWarning, append=True)
warnings.filterwarnings('ignore', category=FutureWarning, append=True)
warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)
warnings.filterwarnings('ignore')

# Define the usr directory for the dictoinaries
sys.path.append("/data/users/hgilmour/precip-filtering")

# Import the functions and dictionaries
import dictionaries as dic


# Write a function which will check the number of arguements passed
def check_no_args(args):
    """Check the number of arguements passed"""
    if len(args) != 5:
        print('Incorrect number of arguements')
        print('Usage: python single_cell_loop.py <YEAR> <MONTH> <CELL>')
        print('Example: python single_cell_loop.py 1998 01 12')
        sys.exit(1)


# #Find the files for the given month and year
# def find_files(mask_file, precip_file, tracks_file):
#     """Find the files for the given year and month"""

#     #JUST TESTING THIS FOR JAN 2005
#     mask_file = dic.mask_file
#     precip_file = dic.precip_file
#     tracks_file = dic.tracks_file

#     #Check that the files exist
#     if not os.path.exists(mask_file, precip_file, tracks_file):
#         print("File does not exist")
#         sys.exit(1)

#         return mask_file, precip_file, tracks_file


# Write a function which loads the files
def open_datasets(mask_file, precip_file, tracks_file):
    """Load specified files"""

    #Load mask file
    mask = xr.open_dataset(mask_file)
    mask = mask.segmentation_mask

    #Load precip file
    precip = xr.open_dataset(precip_file)
    precip = precip.stratiform_rainfall_flux

    #Load tracks file
    tracks = pd.read_hdf(tracks_file, 'table')

    return mask, precip, tracks


#Create a function to copy the tracks file
def copy_tracks_file(tracks):
    tracks = tracks.copy()
    return tracks

# Create a function which adds precip columns to the tracks dataframe
def add_precip_columns(tracks):
    """Adds columns to the tracks dataframe for the total precip, heavy precip and extreme precip"""

    # Add columns to the tracks_precip dataframe ready to append data to later
    tracks['total_precip'] = 0
    tracks['rain_flag'] = 0
    tracks['convective_precip'] = 0
    tracks['heavy_precip'] = 0
    tracks['extreme_precip'] = 0
    tracks['heavy_rain_flag'] = 0
    tracks['extreme_rain_flag'] = 0
    tracks['max_precip'] = 0
    tracks['mean_precip'] = 0

    return tracks

# Create a function to remove cells which are not part of a track
# i.e. these will have a cell value of -1
def remove_non_track_cells(tracks):
    """Removes cells which are not part of a track"""

    # Remove cells which are not part of a track
    tracks = tracks[tracks.cell >= 0]
    #print(tracks)

    return tracks


def check_unique_cell_number(cell):
    """Check the unique cell number to be used in the loop"""
    cell = cell
    print(cell)

    return cell


def select_subset(tracks, cell):
    """Select a subset of the tracks dataframe that just keeps rows for 1 cell"""

    subset = tracks[tracks.cell == int(cell)] #NEED TO CHANGE THIS BACK TO 'CELL' RATHER THAN 12
    print(subset)

    return subset

# Create a function for finding the corresponding frames
# Within the mask and precip datasets
def find_corresponding_frames(mask, precip, frame):

    # Find the segmentation mask which occurs in the same frame
    # as the new value
    seg = mask[frame, :, :]

    # Find the precipitation which occurs in the same frame
    # as the new value
    prec = precip[frame, :, :]

    return seg, prec


# Define a function for assigning the feature id's
def assign_feature_id(subset, frame):

    # Assign the feature id to the subset features at each frame
    # of the cells lifetime
    feature_id = subset.feature[subset.frame == frame].values[0]

    return feature_id

# Define a function for the image processing using ndimage
def image_processing_ndimage(seg, s):

    # Use the ndimage package to label the segmentation mask
    # Generating an array of numbers to find connected regions
    labels, num_labels = ndimage.label(seg, structure = s)

    return labels, num_labels


# Define a function for selecting the segmentation mask area
# which is assigned to the feature id
def select_area(labels, feature_id, seg):

    # Select the label which corresponds to the feature id
    # This is unique to each cell area at a single timestep
    label = np.unique(labels[seg == feature_id])[0]

    # Select the segmentation mask area which corresponds to the feature id
    seg_mask = seg.where(labels == label)

    return seg_mask


# Define a function to create coordinates for the selected segmentation mask
# area
def create_coordinates(seg_mask):

    # Set up lon and lat co-ords
    seg_mask.coords['mask'] = (('latitude', 'longitude'), seg_mask.data)

    return seg_mask

# Define a function which finds the precipitation values
# within the selected segmentation mask area
# and converts this into a dask array
# with no nan values
def find_precip_values(seg_mask, prec):

    # Apply the mask to the precipitation data
    precip_values = prec.where(seg_mask.coords['mask'].values > 0)

    # Convert the precip values into a 1D array
    # Converted from kg m-2 s-1 to mm hr-1
    precip_values_array = precip_values.values.flatten() * 3600

    # Convert the precip values array into a dask array
    #precip_values_array = da.from_array(precip_values_array, chunks = 100)

    # Remove any nan values from the array
    precip_values = precip_values_array[~np.isnan(precip_values_array)]

    return precip_values

# Create a function to find the total precip and rain features
# and set them to the tracks dataframe
def find_total_precip_and_rain_features(subset, precip_values, feature_id, frame, precip_threshold):

    # Find the total precip for the feature
    # First, values of 0 are removed to only consider precipitating pixels. # Then np.nansum is used to compute the sum of all precipitating values # within the mask.
    total_precip = np.nansum(precip_values[precip_values > 0])

    # Find the number of rain pixels within the mask which meet //
    # the threshold for rain - 1 mm hr-1
    rain_features = precip_values[precip_values >= precip_threshold].shape[0]

    # Assign these to the tracks dataframe for the corresponding values
    # of cell frame and feature id
    subset['total_precip'][(subset.feature == feature_id) & (subset.frame == frame)] = total_precip

    # And for the rain features
    subset['rain_flag'][(subset.feature == feature_id) & (subset.frame == frame)] = rain_features

    return subset, rain_features

# Create a function to find the total rainfall and area
# from convective, heavy and extreme
# precipitation types
def find_precipitation_types(subset, precip_values, feature_id, frame, precip_threshold, heavy_precip_threshold, extreme_precip_threshold):

    # Set up the tracks dataframe columns for convective precip
    # heavy precip and extreme precip
    subset['convective_precip'][(subset.feature == feature_id) & (subset.frame == frame)] = np.nansum(precip_values[precip_values >= precip_threshold])

    # For heavy precip threshold
    subset['heavy_precip'][(subset.feature == feature_id) & (subset.frame == frame)] = np.nansum(precip_values[precip_values >= heavy_precip_threshold])

    # Count the number of heavy precip pixels for the heavy rain flag
    subset['heavy_rain_flag'][(subset.feature == feature_id) & (subset.frame == frame)] = precip_values[precip_values >= heavy_precip_threshold].shape[0]

    # For extreme precip threshold
    subset['extreme_precip'][(subset.feature == feature_id) & (subset.frame == frame)] = np.nansum(precip_values[precip_values >= extreme_precip_threshold])

    # Count the number of extreme precip pixels for the extreme rain flag
    subset['extreme_rain_flag'][(subset.feature == feature_id) & (subset.frame == frame)] = precip_values[precip_values >= extreme_precip_threshold].shape[0]

    # Max precip within the cell at that timestep
    subset['max_precip'][(subset.feature == feature_id) & (subset.frame == frame)] = np.max(precip_values)

    # Mean precip within the cell at that timestep
    subset['mean_precip'][(subset.feature == feature_id) & (subset.frame == frame)] = np.mean(precip_values)

    return subset



# Create a function for the conditional image processing
def image_processing(cell, subset, precip, mask, subset_feature_frame, precip_threshold, heavy_precip_threshold, extreme_precip_threshold, s, precip_area, precipitation_flag):
    """Conditional image processing statement"""

    # Add in the for loop here
    for frame in subset_feature_frame:
        print('frame', frame)

        # If the mask shape is equal to the precip shape
        if mask.shape == precip.shape:
            print("The mask shape is equal to the precip shape")

            # Find the segmentation mask which occurs in the same frame
            seg, prec = find_corresponding_frames(mask, precip, frame)

            # Assign the feature id to the subset features at each frame
            # of the cells lifetime
            feature_id = assign_feature_id(subset, frame)

            # Process the image using ndimage
            # Generate a binary structure
            labels, num_labels = image_processing_ndimage(seg, s)

            # Check whether the feature_id is in the segmentation mask for that frame/timestep
            if int(feature_id) not in seg:
                print("feature_id not in seg")
                # Keep the loop running until matching feature_id is found
                continue
            else:
                # A match has been found for the feature_id
                # Select the segmentation mask area which 
                # corresponds to the feature id
                seg_mask = select_area(labels, feature_id, seg)

                # Create coordinates for the selected segmentation mask area
                seg_mask = create_coordinates(seg_mask)

                # Find the precipitation values within the selected segmentation mask area
                precip_values = find_precip_values(seg_mask, prec)

                # Find the total precip and rain features
                subset, rain_features = find_total_precip_and_rain_features(subset, precip_values, feature_id, frame, precip_threshold)

                # Find the precipitation types
                # add them to the tracks dataframe
                subset = find_precipitation_types(subset, precip_values, feature_id, frame, precip_threshold, heavy_precip_threshold, extreme_precip_threshold)

                # Checking whether the number of precipitating pixels
                # exceeds the minimum area threshold for rain
                # If it does, then the precipitation flag is set to increase
                # the number of rain features

                if rain_features >= precip_area:
                    precipitation_flag.append(rain_features)

    return subset, precipitation_flag


#Define the main function / filerting loop:
def main():
    """Main function."""

    # First extract the arguements:
    mask_file = str(sys.argv[1])
    precip_file = str(sys.argv[2])
    tracks_file = str(sys.argv[3])
    cell = str(sys.argv[4])

    #check the number of arguements
    check_no_args(sys.argv)

    #first check the cell number being used in the loop
    cell = check_unique_cell_number(cell)

    #removed tracks set to 0
    removed_tracks = 0

    #first find the file
    mask, precip, tracks = open_datasets(mask_file, precip_file, tracks_file)

    # make a copy of the tracks dataframe
    tracks = copy_tracks_file(tracks)

    #add precip columns to tracks dataframe
    tracks = add_precip_columns(tracks)

    #remove non-tracked cells from the dataframe
    tracks = remove_non_track_cells(tracks)

    # Select a subset of the dataframe for the cell
    subset = select_subset(tracks, cell)

    # Extract the features from the subset
    subset_features = subset.feature.values

    # Set the precipitation flag to 0
    precipitation_flag = []

    # Loop over the feature values within the subset
    # Which is set by the current cell
    for feature in subset_features:
        # Set up the frame of the feature within the subset
        subset_feature_frame = subset.frame[subset.feature == feature]

        # Do the image processing for each subset feature frame
        subset, precipitation_flag = image_processing(cell, subset, precip, mask, subset_feature_frame, dic.precip_threshold, dic.heavy_precip_threshold, dic.extreme_precip_threshold, dic.s, dic.precip_area, precipitation_flag)

    # Take the sum of the array
    precipitation_flag = np.sum(precipitation_flag)

    # If the precipitation flag is equal to zero
    # Then there is no precipitation within the cell
    if precipitation_flag == 0:
        # Remove the cell from the tracks dataframe
        #tracks = tracks.drop(tracks[tracks.cell == cell].index)
        subset = subset.drop(subset[subset.cell == cell].index)

        # Count the number of cells which have been removed
        removed_tracks += 1

    # Print the number of cells which have been removed
    print("The number of cells which have been removed is: ", removed_tracks)

    subset.to_hdf('/data/users/hgilmour/precip-filtering/single_cell_hdf_files/jan_2005_precip_cell_{}'.format(cell), 'table')
    print('Saved file for cell {}'.format(cell))

#Run the main function
if __name__ == "__main__":
    main()









    
