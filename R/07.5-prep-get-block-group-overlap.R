#######################
source("R/01-setup.R")
#######################
#####Please check data/07.5-prep-get-block-group-overlap to see if these have all been created already.  

#This code downloads, transforms, and writes shapefiles for block groups in the USA for multiple years, including 2022, 2021, 2020, and 2019. It then proceeds to download block group data for earlier years (from 2018 to 2008) for the entire USA by merging state-level data, ensuring a comprehensive dataset for block groups spanning these years.

#block groups are fully formed for the nation from the US Census Bureau via tigris package
download_transform_write_shapefiles(2022)
download_transform_write_shapefiles(2021)
download_transform_write_shapefiles(2020)
download_transform_write_shapefiles(2019)

#No national blockgroups after 2019.  So we download each state block group and combined into one map 
download_each_state_and_merge_to_nation_block_groups(2018)
download_each_state_and_merge_to_nation_block_groups(2017)
download_each_state_and_merge_to_nation_block_groups(2016)
download_each_state_and_merge_to_nation_block_groups(2015)
download_each_state_and_merge_to_nation_block_groups(2014)
download_each_state_and_merge_to_nation_block_groups(2013)
download_each_state_and_merge_to_nation_block_groups(2012)
download_each_state_and_merge_to_nation_block_groups(2011)
download_each_state_and_merge_to_nation_block_groups(2010)
download_each_state_and_merge_to_nation_block_groups(2009)
download_each_state_and_merge_to_nation_block_groups(2008)
