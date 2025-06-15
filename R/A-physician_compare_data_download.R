#######################
source("R/01-setup.R")
#######################

# https://www.openicpsr.org/openicpsr/project/149961/version/V1/view?path=/openicpsr/149961/fcr:versions/V1&type=project
# You have to download this file manually.
source("R/01-setup.R")

data_dir <- "/Volumes/Video Projects Muffly 1/physician_compare"

unzip("/Volumes/Video Projects Muffly 1/physician_compare/149961-V1.zip", 
      exdir = data/dir)

list.files("/Volumes/Video Projects Muffly 1/physician_compare/unzipped_files")