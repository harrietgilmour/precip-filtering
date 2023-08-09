# Functions for the main program
# Import local packages
import os
import sys
import glob

# Third party packages
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
import cartopy.crs as ccrs
import xarray as xr
import netCDF4 as nc
import scipy
import dask as ds
import dask.dataframe as dd
import dask.array as da
from dask import delayed
from scipy import ndimage
from scipy.ndimage import label, generate_binary_structure
import tobac #tobac package cloned from https://github.com/tobac-project/tobac.git

# Write a function which opens the datasets
def open_datasets(mask_file, precip_file, tracks_file):
    """Opens the mask, precipitation and tracks datasets"""

    # Open the mask file with chunks of time
    mask = xr.open_dataset(mask_file, chunks={'time': 100})
    # Set the variable within the mask file
    # to be the segmentation mask
    mask = mask.segmentation_mask

    # Open the precipitation file with chunks of time
    precip = xr.open_dataset(precip_file, chunks={'time': 100})
    # Set the variable as stratiform precipitation
    precip = precip.stratiform_rainfall_flux

    # Open the tracks file using pandas first
    tracks = pd.read_hdf(tracks_file, 'table')
    # Convert the pandas dataframe to a dask dataframe
    # tracks = dd.from_pandas(tracks, npartitions=100)

    return mask, precip, tracks

# Create a function to create a copy of the tracks file
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

    return tracks

# Create a function to remove cells which are not part of a track
# i.e. these will have a cell value of -1
def remove_non_track_cells(tracks):
    """Removes cells which are not part of a track"""

    # Remove cells which are not part of a track
    tracks = tracks[tracks.cell >= 0]

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
    print("The unique cells array is: ", unique_cells)

    return unique_cells

# Create a function for finding the corresponding frames
# Within the mask and precip datasets
def find_corresponding_frames(tracks, mask, precip, frame):

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
    feature_id = subset.feature[subset.frame == frame]

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
    precip_values_array = da.from_array(precip_values_array, chunks = 100)

    # Remove any nan values from the array
    precip_values = precip_values_array[~da.isnan(precip_values_array)]

    return precip_values


# Create a function to find the total precip and rain features
# and set them to the tracks dataframe
def find_total_precip_and_rain_features(tracks, precip_values, feature_id, frame, precip_threshold, cell):

    # Find the total precip for the feature
    # First, values of 0 are removed to only consider precipitating pixels. # Then np.nansum is used to compute the sum of all precipitating values # within the mask.
    total_precip = np.nansum(precip_values[precip_values > 0])

    # Find the number of rain pixels within the mask which meet //
    # the threshold for rain - 1 mm hr-1
    rain_features = precip_values[precip_values >= precip_threshold].shape[0]

    # Assign these to the tracks dataframe for the corresponding values
    # of cell frame and feature id
    tracks['total_precip'][(tracks.feature == feature_id) & (tracks.frame == frame) & (tracks.cell == cell)] = total_precip

    # And for the rain features
    tracks['rain_flag'][(tracks.feature == feature_id) & (tracks.frame == frame) & (tracks.cell == cell)] = rain_features

    return tracks, rain_features


# Create a function to find the total rainfall and area
# from convective, heavy and extreme
# precipitation types
def find_precipitation_types(tracks, precip_values, feature_id, frame, precip_threshold, heavy_precip_threshold, extreme_precip_threshold, cell):

    # Set up the tracks dataframe columns for convective precip
    # heavy precip and extreme precip
    tracks['convective_precip'][(tracks.feature == feature_id) & (tracks.frame == frame) & (tracks.cell == cell)] = np.nansum(precip_values[precip_values >= precip_threshold])

    # For heavy precip threshold
    tracks['heavy_precip'][(tracks.feature == feature_id) & (tracks.frame == frame) & (tracks.cell == cell)] = np.nansum(precip_values[precip_values >= heavy_precip_threshold])

    # Count the number of heavy precip pixels for the heavy rain flag
    tracks['heavy_rain_flag'][(tracks.feature == feature_id) & (tracks.frame == frame) & (tracks.cell == cell)] = precip_values[precip_values >= heavy_precip_threshold].shape[0]

    # For extreme precip threshold
    tracks['extreme_precip'][(tracks.feature == feature_id) & (tracks.frame == frame) & (tracks.cell == cell)] = np.nansum(precip_values[precip_values >= extreme_precip_threshold])

    # Count the number of extreme precip pixels for the extreme rain flag
    tracks['extreme_rain_flag'][(tracks.feature == feature_id) & (tracks.frame == frame) & (tracks.cell == cell)] = precip_values[precip_values >= extreme_precip_threshold].shape[0]

    return tracks


# Create a function for the conditional image processing
def image_processing(cell, tracks, precip, mask, subset, feature, subset_feature_frame, precip_threshold, heavy_precip_threshold, extreme_precip_threshold, s, precip_area):
    """Conditional image processing statement"""

    
    
    # Add in the for loop here
    for frame in subset_feature_frame:
        
        print(np.shape(frame))
        print(np.shape(subset_feature_frame))

        # If the mask shape is equal to the precip shape
        if mask.shape == precip.shape:
            # print("The mask shape is equal to the precip shape")

            # Find the segmentation mask which occurs in the same frame
            seg, prec = find_corresponding_frames(tracks, mask, precip, frame)

            # Assign the feature id to the subset features at each frame
            # of the cells lifetime
            feature_id = assign_feature_id(subset, frame)

            # Process the image using ndimage
            # Generate a binary structure
            labels, num_labels = image_processing_ndimage(seg, s)

            
            print("shape of feature id:", np.shape(feature_id))
            print("shape of seg:", np.shape(seg))
            
            # Check whether the feature_id is in the segmentation mask for that frame/timestep
            if feature_id not in seg:
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
                tracks, rain_features = find_total_precip_and_rain_features(tracks, precip_values, feature_id, frame, precip_threshold, cell)

                # Find the precipitation types
                # add them to the tracks dataframe
                tracks = find_precipitation_types(tracks, precip_values, feature_id, frame, precip_threshold, heavy_precip_threshold, extreme_precip_threshold, cell)

                # Checking whether the number of precipitating pixels
                # exceeds the minimum area threshold for rain
                # If it does, then the precipitation flag is set to increase
                # the number of rain features
                if rain_features >= precip_area:
                    # Set the flag to add rain pixels
                    precipitation_flag += rain_features

                    # return the tracks dataframe and the precipitation flag
                    return tracks, precipitation_flag


# Create a function for the main filtering loop
def precip_filtering_loop(tracks, precip, mask, unique_cells, precip_threshold, heavy_precip_threshold, extreme_precip_threshold, s, precip_area, removed_tracks, tracks_filtered_output):
    """Main precipitation filtering loop"""

    # First loop over the unique cells in the tracks dataframe
    # These are equivalent to unique MCSs
    for cell in unique_cells:

        # Select a subset of the dataframe for the current cell
        subset = tracks[tracks.cell == cell]

        # Extract the features from the subset
        subset_features = subset.feature.values

        # Set the precipitation flag to 0
        precipitation_flag = 0

        # Loop over the feature values within the subset
        # Which is set by the current cell
        for feature in subset_features:

            # Set up the frame of the feature within the subset
            subset_feature_frame = subset.frame[subset.feature == feature]

            # Do the image processing for each subset feature frame
            tracks, precipitation_flag = image_processing(cell, tracks, precip, mask, subset, feature, subset_feature_frame, precip_threshold, heavy_precip_threshold, extreme_precip_threshold, s, precip_area)

        # If the precipitation flag is equal to zero
        # Then there is no precipitation within the cell
        if precipitation_flag == 0:
            # Remove the cell from the tracks dataframe
            tracks = tracks.drop(tracks[tracks.cell == cell].index)

            # Count the number of cells which have been removed
            removed_tracks += 1

    # Print the number of cells which have been removed
    print("The number of cells which have been removed is: ", removed_tracks)

    # Save the tracks post precipitation filtering
    tracks.to_hdf(tracks_filtered_output, 'table')

    return tracks, removed_tracks

# Define a precipitation filtering function for dask
# which takes the cell
def precip_filtering_loop_cell(cell, tracks, precip, mask, unique_cells, precip_threshold, heavy_precip_threshold, extreme_precip_threshold, s, precip_area, removed_tracks, tracks_filtered_output, tracks_cell_output_dir):
    """Main precipitation filtering loop"""

    # Select a subset of the dataframe for the current cell
    subset = tracks[tracks.cell == cell]

    # Extract the features from the subset
    subset_features = subset.feature.values

    # Set the precipitation flag to 0
    precipitation_flag = 0

    # Loop over the feature values within the subset
    # Which is set by the current cell
    for feature in subset_features:

        # Set up the frame of the feature within the subset
        subset_feature_frame = subset.frame[subset.feature == feature]

        # Do the image processing
        tracks, precipitation_flag = image_processing(cell, tracks, precip, mask, subset, feature, subset_feature_frame, precip_threshold, heavy_precip_threshold, extreme_precip_threshold, s, precip_area)
        
    # If the precipitation flag is equal to zero
    # Then there is no precipitation within the cell
    if precipitation_flag == 0:
        # Remove the cell from the tracks dataframe
        tracks = tracks.drop(tracks[tracks.cell == cell].index)

        # Count the number of cells which have been removed
        removed_tracks += 1

    # Print the number of cells which have been removed
    print("The number of cells which have been removed is: ", removed_tracks)

    # Set up the name for the tracks output
    # depending on the cell number
    # Track_precip_test_filtered_jan_2005.h5"
    tracks_filtered_output = tracks_cell_output_dir + "/" + "tracks_precip_test_filtered_jan_2005_cell_" + str(cell) + ".h5"

    # Save the tracks post precipitation filtering
    tracks.to_hdf(tracks_filtered_output, 'table')

    return tracks, removed_tracks

