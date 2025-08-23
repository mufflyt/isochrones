#######################
source("R/01-setup.R")
#######################

#' -----------------------------------------------------------------------------
#' Physician Retirement Year Confirmation Script
#' -----------------------------------------------------------------------------
#' 
#' This script combines and analyzes physician retirement data from multiple sources:
#' 
#' 1. NPPES Deactivation Records - When provider NPIs are marked as deactivated.  This is most likely to be correct IF #' a physician is 
#' 2. Medicare Part D Prescribing - Last year of prescription records.  This is good for people who prescribe to #' #' #' patients over 65 years old.  So if may work for GO, Urgyn, and general OBGYNs.  Not great for REI or MFM or young #' OBGYNs only doing OB.  
#' 
#' 3. Hospital Affiliations - End dates of hospital privileges/employment?  
#' 4. NIPS Dataset - Custom processed National Provider Identifier data
#' 
#' The purpose is to determine the most accurate retirement year for each physician
#' by triangulating data from these independent sources. The script allows for 
#' different methodologies (earliest, latest, or mode) to resolve conflicts when
#' sources disagree.
#' 
#' Workflow:
#' 1. Loads data from each source
#' 2. Extracts retirement year information
#' 3. Standardizes data format across sources
#' 4. Combines sources with appropriate resolution strategy
#' 5. Outputs a standardized dataset with consistent column names
#' 
#' Output format:
#' - calculated_retirement_npi: Provider identifier
#' - calculated_retirement_year_of_retirement: Determined retirement year
#' - calculated_retirement_data_source: Source of the retirement date
#'
#' Author: Tyler Muffly, MD
#' Last updated: April 2025
#' -----------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Script: physician_retirement_analysis.R
#
# Description:
# This script analyzes trends in physician retirement using three key datasets:
#
#   1. **NPPES Deactivation Report**:
#      - Lists National Provider Identifiers (NPIs) that have been deactivated.
#      - Used to estimate when a physician may have retired or stopped practicing.
#
#   2. **Medicare Part D Prescriber Data**:
#      - Tracks the last year a physician prescribed medications under Medicare Part D.
#      - Provides a proxy for when a physician may have retired based on prescribing activity.
#
#   3. **Custom NIPS Dataset**:
#      - A cleaned or enriched internal dataset that includes manually recorded or modeled
#        retirement years for physicians.
#
# The script performs the following tasks:
# -------------------------------------------------------------------------------
#   ✓ Loads and processes each dataset
#   ✓ Extracts retirement years from dates or existing columns
#   ✓ Summarizes retirement year counts
#   ✓ Visualizes retirement trends over time for each data source
#   ✓ Combines all sources for comparison
#   ✓ Creates comparative charts (histogram and time series)
#   ✓ Analyzes overlaps and inconsistencies between the sources
#
# Outputs:
#   📁 All summary plots and tables are saved to the 'analysis' folder within the main
#      working directory.
#
# Why this is useful:
#   ➤ This analysis helps identify trends in physician retirement
#     (e.g., early retirement, pandemic-related exits, or regional shortages).
#   ➤ It also allows validation and triangulation between independent data sources.
#
# Author: Tyler Muffly, MD
# Updated: April 2025
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# Script: physician_retirement_analysis.R
# Purpose: Analyze physician retirement using NPPES, Medicare Part D, and NIPS data
# ------------------------------------------------------------------------------

invisible(gc())

# Load custom functions
source("R/bespoke_functions.R")  # loads process_nppes_data()

# Load required libraries
library(data.table)
library(dplyr)
library(readr)
library(ggplot2)
library(lubridate)
library(tidyr)
library(tibble)
library(scales)
library(knitr)
library(assertthat)
library(logger)
library(janitor)
library(gridExtra)
library(readxl)

# ------------------------------------------------------------------------------
# SETTINGS
# ------------------------------------------------------------------------------

DATA_DIR <- "/Volumes/Video Projects Muffly 1/retirement"
ANALYSIS_DIR <- file.path(DATA_DIR, "analysis")
VERBOSE <- TRUE

if (!dir.exists(ANALYSIS_DIR)) {
  dir.create(ANALYSIS_DIR, recursive = TRUE)
  logger::log_info("Created analysis directory: {ANALYSIS_DIR}")
}

logger::log_threshold(INFO)
logger::log_info("Starting retirement data analysis")

# ------------------------------------------------------------------------------
# HELPER FUNCTIONS
# ------------------------------------------------------------------------------

safe_read_csv <- function(file_path, required = FALSE) {
  if (!file.exists(file_path)) {
    msg <- ifelse(required, "Required", "Optional")
    logger::log_warn("{msg} file not found: {file_path}")
    return(NULL)
  }
  
  tryCatch({
    csv_data <- fread(file_path, data.table = TRUE)
    return(csv_data)
  }, error = function(e) {
    logger::log_error("Failed to read {file_path}: {e$message}")
    return(NULL)
  })
}

standardize_npi_column <- function(npi_data) {
  if (is.null(npi_data)) return(NULL)
  npi_data <- setDT(npi_data)
  if ("NPI" %in% names(npi_data) && !"npi" %in% names(npi_data)) {
    setnames(npi_data, "NPI", "npi")
  }
  return(npi_data)
}

# ------------------------------------------------------------------------------
# 1. NPPES Deactivation Data
# ------------------------------------------------------------------------------

nppes_path <- file.path("/Volumes/Video Projects Muffly 1/nppes_deactivation_reports", "NPPES Deactivated NPI Report 20250414.xlsx")
logger::log_info("Reading NPPES file: {nppes_path}")

# NPPES Deactivation Data
npi_data <- read_xlsx(
  nppes_path,
  skip = 1,  # Skip the title row
  col_names = TRUE,  # Use the first row after skipping as column names
  col_types = c("text", "text"),  # Specify column types
  .name_repair = "unique"  # Ensure column names are unique
) %>%
  # Clean up column names
  clean_names() %>%
  # Rename columns to standard names
  rename(
    npi = 1,
    nppes_deactivation_date = 2
  ) %>%
  # Process the date column
  mutate(
    deactivation_date = as.Date(nppes_deactivation_date, format = "%m/%d/%Y"),
    retirement_year = year(deactivation_date)
  ) %>%
  select(npi, retirement_year); npi_data

# Summarize NPPES Deactivation Data
nppes_summary <- npi_data %>%
  distinct(npi, .keep_all = TRUE) %>%
  dplyr::filter(!is.na(retirement_year)) %>%
  count(retirement_year, name = "count") %>%
  arrange(retirement_year)

cat("\nNPPES Deactivation of All Physicians (not just OBGYNs)- Retirement Summary:\n")
print(kable(head(nppes_summary, 10)))

# Visualization of NPPES Deactivation Data
ggplot(npi_data %>% dplyr::filter(!is.na(retirement_year)), aes(x = retirement_year)) +
  geom_histogram(binwidth = 1, fill = "#1B9E77", color = "white") +
  labs(title = "NPPES Deactivation of All Physicians (not just OBGYNs)",
       x = "Retirement Year", y = "Number of Physicians") +
  theme_minimal() -> p_nppes; p_nppes

ggsave(file.path(ANALYSIS_DIR, "nppes_deactivation_retirement_years.png"), p_nppes, width = 10, height = 6)

# ------------------------------------------------------------------------------
# 2. Medicare Part D Prescribers
# ------------------------------------------------------------------------------

medicare_path <- file.path(DATA_DIR, "end_Medicare_part_D_prescribers_last_consecutive_year.csv")
logger::log_info("Reading Medicare Part D file: {medicare_path}")

medicare_data <- read_delim(medicare_path, delim = ",", quote = "\"", na = "", trim_ws = TRUE,
                            locale = locale(encoding = "UTF-8", tz = "America/Denver", grouping_mark = ","))

assert_that(is.data.frame(medicare_data))

medicare_data <- type_convert(medicare_data) %>%
  mutate(retirement_year = as.numeric(last_consecutive_year_Medicare_part_D_prescribers)) %>%
  rename(npi = PRSCRBR_NPI)

medicare_summary <- medicare_data %>%
  dplyr::filter(!is.na(retirement_year)) %>%
  count(retirement_year, name = "count") %>%
  arrange(desc(retirement_year))

cat("\nMedicare Part D - Last Consecutive Year Summary:\n")
print(kable(head(medicare_summary, 10)))

# Visualization
ggplot(medicare_data %>% dplyr::filter(!is.na(retirement_year)), aes(x = retirement_year)) +
  geom_histogram(binwidth = 1, fill = "#1B9E77", color = "white") +
  labs(title = "Medicare Part D - Last Consecutive Year of Prescribing",
       x = "Year", y = "Number of Physicians") +
  theme_minimal() -> p_medicare; p_medicare

ggsave(file.path(ANALYSIS_DIR, "medicare_last_consecutive_years.png"), p_medicare, width = 10, height = 6)

# ------------------------------------------------------------------------------
# 3. Updated Processed NIPS
# ------------------------------------------------------------------------------

nips_path <- file.path(DATA_DIR, "updated_processed_nips.csv")
nips_data <- safe_read_csv(nips_path)
nips_data <- standardize_npi_column(nips_data)

if (!is.null(nips_data) && "year_of_retirement" %in% names(nips_data)) {
  nips_data <- mutate(nips_data, retirement_year = as.numeric(as.character(year_of_retirement)))
  
  nips_summary <- nips_data %>%
    dplyr::filter(!is.na(retirement_year)) %>%
    count(retirement_year, name = "count") %>%
    arrange(desc(retirement_year))
  
  cat("\nNIPS - Retirement Year Summary:\n")
  print(kable(head(nips_summary, 10)))
  
  # Visualization
  ggplot(nips_data %>% dplyr::filter(!is.na(retirement_year)), aes(x = retirement_year)) +
    geom_histogram(binwidth = 1, fill = "#7570B3", color = "white") +
    labs(title = "NIPS Data - Retirement Years", x = "Year", y = "Number of Physicians") +
    theme_minimal() -> p_nips
  
  ggsave(file.path(ANALYSIS_DIR, "nips_retirement_years.png"), p_nips, width = 10, height = 6)
}

# ------------------------------------------------------------------------------
# 4. Combine and Compare Sources
# ------------------------------------------------------------------------------

conflicted::conflicts_prefer(dplyr::bind_rows)
nppes_data <- npi_data

nppes_data$npi <- as.numeric(nppes_data$npi)
medicare_data$npi <- as.numeric(medicare_data$npi)
nips_data$npi <- as.numeric(nips_data$npi)


combined_data <- bind_rows(
  if (!is.null(nppes_data)) nppes_data %>% select(npi, retirement_year) %>% mutate(source = "NPPES"),
  if (!is.null(medicare_data)) medicare_data %>% select(npi, retirement_year) %>% mutate(source = "Medicare Part D"),
  if (!is.null(nips_data)) nips_data %>% select(npi, retirement_year) %>% mutate(source = "NIPS")
) %>% dplyr::filter(!is.na(retirement_year))

# Combined plot
ggplot(combined_data, aes(x = retirement_year, fill = source)) +
  geom_histogram(binwidth = 1, position = "dodge", alpha = 0.7) +
  scale_fill_brewer(palette = "Set1") +
  labs(title = "Comparison of Retirement Years Across Sources",
       subtitle = paste("Based on", nrow(combined_data), "records"),
       x = "Retirement Year", y = "Number of Physicians") +
  theme_minimal() +
  theme(legend.position = "bottom") -> p_combined

ggsave(file.path(ANALYSIS_DIR, "combined_retirement_years.png"), p_combined, width = 12, height = 8)

# Time series
yearly_counts <- combined_data %>%
  group_by(retirement_year, source) %>%
  summarize(count = n(), .groups = "drop")

ggplot(yearly_counts, aes(x = retirement_year, y = count, color = source)) +
  geom_line(size = 1.2) +
  geom_point(size = 3) +
  scale_color_brewer(palette = "Set1") +
  labs(title = "Retirement Trends by Source", x = "Year", y = "Number of Physicians") +
  theme_minimal() +
  theme(legend.position = "bottom") -> p_timeseries

ggsave(file.path(ANALYSIS_DIR, "retirement_trends_by_source.png"), p_timeseries, width = 12, height = 8)

logger::log_info("✅ Analysis complete! Outputs saved in: {ANALYSIS_DIR}")
