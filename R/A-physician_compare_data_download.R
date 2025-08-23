#######################
source("R/01-setup.R")
#######################

# https://www.openicpsr.org/openicpsr/project/149961/version/V1/view?path=/openicpsr/149961/fcr:versions/V1&type=project
# You have to download this file manually.
source("R/01-setup.R")

# File Path Constants ----
PHYSICIAN_COMPARE_BASE_DIR <- "/Volumes/Video Projects Muffly 1/physician_compare"
PHYSICIAN_COMPARE_ZIP <- file.path(PHYSICIAN_COMPARE_BASE_DIR, "149961-V1.zip")
PHYSICIAN_COMPARE_UNZIP_DIR <- file.path(PHYSICIAN_COMPARE_BASE_DIR, "unzipped_files")

data_dir <- PHYSICIAN_COMPARE_BASE_DIR

unzip(PHYSICIAN_COMPARE_ZIP,
      exdir = PHYSICIAN_COMPARE_UNZIP_DIR)

list.files(PHYSICIAN_COMPARE_UNZIP_DIR)
