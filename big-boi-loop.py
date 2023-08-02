## starting precipitation filtering loop ##
for cell in np.unique(tracks.cell.values):
    subset = tracks[tracks.cell == cell]
    precipitation_flag = 0

    for feature in subset.feature.values: #find all the feature values for that unique cell / track (the feature value is a unique value for each feature in a frame /timestep)
        #print(feature)
        for frame in subset.frame[subset.feature == feature]: #find the frame / timestep that corresponds to the feature number
        #  print(frame)
            if mask.shape == precip.shape:
                seg = mask[frame,:,:] #printing the segmentation mask which occurs in the same frame as the feature value
                #print(seg)
                prec = precip[frame,:,:] #printing the precip timesteps which occurs in the same frame as the feature value
                #print(prec)

                featureid = subset.feature[subset.frame == frame].values[0] #find the feature number at each timestep / frame of the cells lifetime (it changes over time and doesn't stay constant)
                #print('featureid: {}'.format(featureid)) #we now know all the feature numbers that belong to a single cell over its lifetime
            
            
                labels, nr = ndimage.label(seg, structure = s) #this line uses ndimage package for image processing. It generates arrays of numbers and decides what are joined together and what aren't.
                # In other words, it does image segmentation tasks, such as finding connected components and labeling objects in an image.
                # (i.e. it generates the locations of all contiguous fields of the segmentation mask that belong to a specific cell at a specific timestep and gives it a label. The number of labels is also recorded (the number of segmented areas in the timestep))

                if featureid not in seg: #check that the feature id number at each timestep is within the segmentation mask, if not, it is ignored and we continue
                    continue
                else:

                    label = np.unique(labels[seg == featureid])[0] #28/07/23: CURRENTLY STUCK ON WHAT THIS LINE IS DOING. START FROM HERE NEXT TIME
                    seg_mask = seg.where(labels == label)
                    #print(seg_mask)

                    #create coordinates from mask
                    seg_mask.coords['mask'] = (('longitude', 'latitude'), seg_mask.data)
                    #print(seg_mask.coords['mask'])
                    #apply mask to precip dataset
                    precip_values = prec.where(seg_mask.coords['mask'].values > 0) # creating a new dataset called 'precip_values' with only the precip values where the seg_mask pixel is labelled as greater than 0 (i.e. the MCS region)
                    #print('precip values: {}'.format(precip_values))
                    array = precip_values.values.flatten() * 3600 # precip values are converted to 1D numpy array and multiplied by 3600 to convert from kg m-2 s-1 to mm / hr
                    values = array[~np.isnan(array)] #removes NaNs from the precip array for further calculations
                    #print(values)

                    total_precip = np.nansum(values[values > 0]) #working out the total precip associated with the mask. First, values of 0 are removed to only consider precipitating pixels. Then np.nansum is used to compute the sum of all precipitating values within the mask.
                    #print('total precip: {}'.format(total_precip))
                    tracks['total_precip'][(tracks.feature == featureid) & (tracks.frame == frame)  & (tracks.cell == cell)] = total_precip

                    rain_features = values[values >= precip_threshold].shape[0] #number of pixels within the mask that meet the 1 mm/hr precip threshold
                    #print('rain features: {}'.format(rain_features))
                    tracks['rain_flag'][(tracks.feature == featureid) & (tracks.frame == frame) & (tracks.cell == cell)] = rain_features

                    tracks['convective_precip'][(tracks.feature == featureid) & (tracks.frame == frame) & (tracks.cell == cell)] = np.nansum(values[values >= precip_threshold]) #total rain from all pixel where the rainfall threshold of 1 mm/hr is met

                    tracks['heavy_precip'][(tracks.feature == featureid) & (tracks.frame == frame) & (tracks.cell == cell)] = np.nansum(values[values >= heavy_precip_threshold]) #total rain from all pixel where the heavy rainfall threshold of 10 mm/hr is met
                    rain_features_heavy = values[values >= heavy_precip_threshold].shape[0] #number of pixels within the mask that meet the heavy rainfall threshold of 10 mm/hr

                    tracks['extreme_precip'][(tracks.feature == featureid) & (tracks.frame == frame) & (tracks.cell == cell)] = np.nansum(values[values >= extreme_precip_threshold]) #total rain from all pixel where the extreme rainfall threshold of 50 mm/hr is met
                    rain_features_extreme = values[values >= extreme_precip_threshold].shape[0] #number of pixels within the mask that meet the extreme rainfall threshold of 50 mm/hr
                

                    if rain_features >= precip_area: # if the number of precipitating pixels exceeds the miniumum pixel number... 
                        precipitation_flag += rain_features # add rain pixels to the precipitation flag

    if precipitation_flag == 0: #if the minumum precipitating pixel thresholds aren't met...
        #remove corresponding cell from the tracks dataframe
        tracks = tracks.drop(tracks[tracks.cell == cell].index)
        removed += 1 #print the number of tracks that have been removed from the original dataset

# save precip track files
tracks.to_hdf('Save/Tracks_precip_Jan_2005_test.h5', 'table')