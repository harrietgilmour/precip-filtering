# Contains the dictionaries for use in precipitation filtering
# ---- These are all individual files, valid for January 2005 ----
# Import packages
from scipy.ndimage import label, generate_binary_structure

# Mask file for January 2005
mask_file = "/data/users/hgilmour/tracking/code/tobac_sensitivity/Save/Mask_Segmentation_tb_precip_test.nc"

# Precip file for Jan 2005
precip_file = "/data/users/hgilmour/total_precip/precip_instant/precip_jan_2005.nc"

# Tracks file for Jan 2005
tracks_file = "/data/users/hgilmour/tracking/code/tobac_sensitivity/Save/Track_precip_test.h5"

# Filtered output for jan 2005
tracks_filtered_output = "/data/users/hgilmour/tracking/code/tobac_sensitivity/Save/Track_precip_test_filtered_jan_2005.h5"

tracks_cell_output_dir = "/data/users/hgilmour/tracking/code/tobac_sensitivity/Save"

# Rainfall thresholds to use within filtering loop
## rainfall thresholds to use within filtering loop ##
precip_threshold = 1 #mm/hr
heavy_precip_threshold = 10 # mm/hr
extreme_precip_threshold = 50 # mm/hr (based on Marengo, J. A., Ambrizzi, T., Alves, L. M., Barreto, N. J., Simões Reboita, M., & Ramos, A. M. (2020). Changing trends in rainfall extremes in the metropolitan area of São Paulo: causes and impacts. Frontiers in Climate, 2, 3.)

precip_area = 25 # threshold for the minimum number of grid points that must be precipitating for a track to remain (and not be dropped from the tracks dataset)

## other parameters that need to be defined before loop ##
s = generate_binary_structure(2,2) # need this in loop later on
removed_tracks = 0 # need this for loop later on