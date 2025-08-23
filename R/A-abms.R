#######################
source("R/01-setup.R")
#######################

# File Path Constants ----
ABMS_DATA_DIR <- "data/SMBA"
ABMS_LEADS_FILE <- file.path(ABMS_DATA_DIR, "SBMA_leads.xlsx")

#ABMS
source("R/01-setup.R")


library(tidyverse)
library(readxl)

abms <- readxl::read_xlsx(ABMS_LEADS_FILE)
names(abms)
glimpse(abms)
View(abms)
