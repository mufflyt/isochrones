#######################
source("R/01-setup.R")
#######################

#ABMS


library(tidyverse)
library(readxl)

abms <- readxl::read_xlsx("data/SMBA/SBMA_leads.xlsx")
names(abms)
glimpse(abms)
View(abms)
