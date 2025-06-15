#ABMS
source("R/01-setup.R")


library(tidyverse)
library(readxl)

abms <- readxl::read_xlsx("data/SMBA/SBMA_leads.xlsx")
names(abms)
glimpse(abms)
View(abms)
