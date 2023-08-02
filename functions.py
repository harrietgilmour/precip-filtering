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
    tracks = dd.from_pandas(tracks, npartitions=100)

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

    return tracks

# Create a function to remove cells which are not part of a track
# i.e. these will have a cell value of -1
def remove_non_track_cells(tracks):
    """Removes cells which are not part of a track"""

    # Remove cells which are not part of a track
    tracks = tracks[tracks.cell >= 0]

    return tracks


