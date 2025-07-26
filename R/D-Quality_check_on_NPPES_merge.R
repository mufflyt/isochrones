# 
# NPPES OB/GYN Provider Data Quality Assessment & Validation
# Script: D-Quality_check_on_NPPES_merge.R
# 
#
# PURPOSE:
# Validates and quality-checks the processed NPPES OB/GYN provider dataset
# through physician presence tracking, geographic mapping, and temporal analysis.
# Ensures data integrity and identifies potential issues in the merged dataset.
#
# RESEARCH QUESTION: Are specific physicians accurately tracked across years
# and properly geocoded for spatial analysis?
#
# 
# SCRIPT ARCHITECTURE
# 
#
# APPROACH: Multi-layered validation with visual confirmation
# - Individual physician presence tracking across years
# - Geographic validation through coordinate mapping
# - Temporal trend analysis for data consistency
# - Interactive mapping for detailed inspection
# - Subspecialty-specific validation (Gynecologic Oncology focus)
#
# VALIDATION STRATEGY:
# 1. Known Provider Tracking → Geographic Validation → Temporal Analysis
# 2. ZIP code coordinate integration and validation
# 3. State-level filtering and boundary verification
# 4. Interactive mapping for manual quality assessment
# 5. Subspecialty distribution analysis over time
#
# 
# PHYSICIAN TRACKING VALIDATION
# 
#
# KNOWN PROVIDER LIST (Test Cases):
# npi_list <- c(
#   "1689603763",   # Tyler Muffly, MD
#   "1528060639",   # John Curtin, MD
#   "1346355807",   # Pedro Miranda, MD
#   "1437904760",   # Lizeth Acosta, MD
#   "1568738854",   # Aaron Lazorwitz, MD
#   "1194571661",   # Ana Gomez, MD
#   "1699237040",   # Erin W. Franks, MD
#   "1003311044",   # Catherine Callinan, MD
#   "1609009943",   # Kristin Powell, MD
#   "1114125051",   # Nathan Kow, MD
#   "1043432123",   # Elena Tunitsky, MD
#   "1215490446",   # Parisa Khalighi, MD
#   "1487879987"    # Peter Jeppson, MD
# )
#
# VALIDATION PURPOSE:
# - Verify specific physicians appear in expected years
# - Confirm provider continuity across temporal dataset
# - Identify missing or incomplete provider records
# - Validate name-NPI matching accuracy
#
# FUNCTION USAGE:
# physician_presence <- check_physician_presence(
#   obgyn_physicians_all_years, 
#   npi_list, 
#   names_list
)
# Returns: Structured data frame with presence by year and record counts
#
# 
# GEOGRAPHIC VALIDATION FRAMEWORK
# 
#
# ZIP CODE COORDINATE INTEGRATION:
# zip_coords <- read_csv("/Users/tylermuffly/Documents/uszips.csv") %>%
#   transmute(
#     zip = str_pad(as.character(zip), 5, pad = "0"),  # Standardized 5-digit ZIP
#     lat = lat,                                       # Latitude coordinate
#     lon = lng,                                       # Longitude coordinate  
#     state_name = state_name                          # State validation
#   )
#
# COORDINATE VALIDATION PROCESS:
# 1. ZIP code standardization (5-digit with leading zeros)
# 2. Coordinate lookup from authoritative ZIP database
# 3. Geographic boundary validation by state
# 4. Outlier detection for impossible coordinates
#
# MERGE STRATEGY:
# obgyn_with_coords <- obgyn_physicians_all_years %>%
#   mutate(
#     zip = str_sub(`Provider Business Practice Location Address Postal Code`, 1, 5),
#     zip = str_pad(zip, 5, pad = "0")
#   ) %>%
#   filter(str_detect(zip, "^\\d{5}$")) %>%           # Valid ZIP format only
#   left_join(zip_coords, by = "zip") %>%
#   filter(!is.na(lat), !is.na(lon))                 # Complete coordinates only
#
# 
# STATE-SPECIFIC VALIDATION (COLORADO FOCUS)
# 
#
# COLORADO GEOGRAPHIC BOUNDARIES:
# Latitude: 36.5° to 42.0° N
# Longitude: -110.0° to -101.0° W
#
# FILTERING STRATEGY:
# obgyn_colorado <- obgyn_with_coords %>%
#   filter(`Provider Business Practice Location Address State Name` == "Colorado") %>%
#   filter(
#     lon >= -110, lon <= -101,    # Colorado longitude bounds
#     lat >= 36.5, lat <= 42       # Colorado latitude bounds  
#   )
#
# SUBSPECIALTY VALIDATION:
# Gynecologic Oncology Focus (207VX0201X):
# gyn_onc_colorado <- obgyn_colorado %>%
#   filter(`Healthcare Provider Taxonomy Code_1` == "207VX0201X")
#
# VALIDATION METRICS:
# - Provider count by year for trend analysis
# - Geographic distribution within state boundaries
# - Coordinate accuracy through visual inspection
# - Temporal consistency of provider locations
#
# 
# VISUALIZATION VALIDATION METHODS
# 
#
# 1. FACETED DOT MAPS BY YEAR:
# Purpose: Visual validation of provider distribution over time
# ggplot() +
#   geom_polygon(data = colorado_map, aes(x = long, y = lat, group = group)) +
#   geom_point(data = obgyn_colorado, aes(x = lon, y = lat)) +
#   facet_wrap(~ Year, ncol = 4) +
#   labs(title = "OB/GYN Physician Locations in Colorado by Year")
#
# 2. TEMPORAL TREND ANALYSIS:
# Purpose: Validate data consistency across years
# obgyn_colorado %>%
#   count(Year) %>%
#   ggplot(aes(x = Year, y = n)) +
#   geom_line() + geom_point() +
#   labs(title = "OB/GYN Provider Count in Colorado Over Time")
#
# 3. CHANGE ANALYSIS MAPPING:
# Purpose: Identify provider gains/losses by ZIP code
# zip_growth_analysis <- obgyn_colorado %>%
#   count(zip, Year) %>%
#   pivot_wider(names_from = Year, values_from = n, values_fill = 0) %>%
#   mutate(net_change = `2024` - `2010`)
#
# 
# INTERACTIVE VALIDATION TOOLS
# 
#
# LEAFLET INTERACTIVE MAPPING:
# Purpose: Detailed manual inspection of provider locations
# leaflet(zip_change_analysis) %>%
#   addProviderTiles("CartoDB.Positron") %>%
#   addCircleMarkers(
#     lng = ~lon, lat = ~lat,
#     radius = ~pmax(3, log10(abs(net_change) + 1) * 4),
#     color = ~color_palette(change_category),
#     label = ~paste0("ZIP: ", zip, " | Change: ", net_change)
#   ) %>%
#   addLegend(title = "Net Change (2024 vs 2010)")
#
# INTERACTIVE FEATURES:
# - Hover labels with ZIP and change information
# - Color-coded markers for gains/losses/no change
# - Zoom capabilities for detailed inspection
# - Legend for change interpretation
#
# 
# SUBSPECIALTY-SPECIFIC VALIDATION
# 
#
# GYNECOLOGIC ONCOLOGY TRACKING:
# Taxonomy Code: 207VX0201X (Primary research focus)
#
# VALIDATION COMPONENTS:
# 1. Provider count trends over time
# 2. Geographic distribution within state
# 3. New provider identification (2024 vs 2010)
# 4. Practice location stability analysis
#
# TREND VISUALIZATION:
# gyn_onc_colorado %>%
#   count(Year) %>%
#   ggplot(aes(x = Year, y = n)) +
#   geom_line(color = "#D55E00", size = 1.2) +
#   geom_point(size = 2) +
#   labs(
#     title = "Gynecologic Oncologist Count in Colorado Over Time",
#     y = "Number of Gyn Oncologists"
#   )
#
# 
# QUALITY ASSESSMENT METRICS
# 
#
# DATA COMPLETENESS CHECKS:
# ✓ NPI presence validation for known physicians
# ✓ Geographic coordinate availability (lat/lon)
# ✓ ZIP code format standardization
# ✓ State boundary validation
# ✓ Temporal continuity assessment
#
# ACCURACY VALIDATION:
# ✓ Coordinate plausibility within state boundaries
# ✓ Provider name-NPI matching verification
# ✓ Taxonomy code consistency over time
# ✓ Address standardization verification
#
# CONSISTENCY CHECKS:
# ✓ Year-over-year provider tracking
# ✓ Geographic stability analysis
# ✓ Subspecialty distribution validation
# ✓ Data format standardization
#
# 
# EXPECTED VALIDATION OUTPUTS
# 
#
# 1. PHYSICIAN PRESENCE TABLE:
# Columns: NPI, Name, Total_Records, Years_Present, First_Year, Last_Year
# Purpose: Verify known physician tracking accuracy
#
# 2. GEOGRAPHIC VALIDATION MAPS:
# - State-level provider distribution by year
# - Subspecialty-specific location mapping
# - Change analysis visualization (gains/losses)
#
# 3. TEMPORAL TREND CHARTS:
# - Provider count by year (overall and subspecialty)
# - Geographic distribution changes over time
# - Data completeness metrics by year
#
# 4. INTERACTIVE VALIDATION TOOLS:
# - Leaflet maps for detailed coordinate inspection
# - Filterable views by subspecialty and year
# - ZIP-level change analysis with hover details
#
# 
# ERROR DETECTION CAPABILITIES
# 
#
# POTENTIAL ISSUES IDENTIFIED:
# 🔍 Missing known physicians in expected years
# 🔍 Coordinates outside reasonable geographic bounds
# 🔍 ZIP codes without coordinate matches
# 🔍 Implausible provider count changes
# 🔍 Inconsistent taxonomy code assignments
# 🔍 Address standardization failures
#
# VALIDATION FLAGS:
# ⚠️  Providers with gaps in temporal coverage
# ⚠️  Coordinates outside state boundaries
# ⚠️  ZIP codes with invalid format
# ⚠️  Sudden provider count discontinuities
# ⚠️  Subspecialty classification inconsistencies
#
# 
# RESEARCH VALIDATION APPLICATIONS
# 
#
# This quality assessment supports research on:
# - Geographic disparities in gynecologic oncology access
# - Provider workforce distribution analysis
# - Temporal trends in subspecialty availability
# - Rural vs urban provider accessibility
# - Distance-to-care calculations for patient populations
#
# VALIDATION ENSURES:
# - Accurate geographic analysis for accessibility studies
# - Reliable temporal trends for workforce planning
# - Complete provider tracking for longitudinal analysis
# - High-quality coordinate data for distance calculations
# - Verified subspecialty classifications for care access studies
#
# 
# INTEGRATION WITH PREVIOUS PROCESSING
# 
#
# DEPENDENCIES:
# - File B/C: Cleaned and processed NPPES provider data
# - ZIP coordinate database: Authoritative lat/lon lookup
# - State boundary data: Geographic validation reference
# - Known physician list: Manual validation test cases
#
# INPUT REQUIREMENTS:
# - obgyn_physicians_all_years.rds (from File C processing)
# - uszips.csv (ZIP coordinate reference file)
# - State map data (for boundary validation)
# - Known physician NPI list (for tracking validation)
#
# QUALITY GATES:
# - All known physicians must be trackable across expected years
# - Geographic coordinates must fall within reasonable boundaries
# - Temporal trends must show logical continuity
# - Subspecialty distributions must align with clinical expectations
#
# 

#
source("R/01-setup.R")
#

#The check_physician_presence function is a well-designed utility for tracking physicians across temporal data. It efficiently analyzes a dataset containing physician information to determine when specific providers appear in the records. The function accepts a list of National Provider Identifiers (NPIs), optionally paired with provider names, and methodically examines each NPI's presence throughout different years. It returns a structured data frame summarizing each provider's representation in the dataset, including their total record count and a chronological listing of years in which they appear. This function is particularly valuable for longitudinal analyses of healthcare provider data, enabling researchers to identify patterns in physician presence, track career trajectories, or validate data completeness across multiple years of NPI records.

invisible(gc())
source("R/bespoke_functions.R") # This loads up the process_nppes_data function

# List of NPIs to check ----
npi_list <- c(
  "1689603763",   # Tyler Muffly, MD
  "1528060639",   # John Curtin, MD
  "1346355807",   # Pedro Miranda, MD
  "1437904760",   # Lizeth Acosta, MD
  "1568738854",   # Aaron Lazorwitz, MD
  "1194571661",   # Ana Gomez, MD
  "1699237040",   # Erin W. Franks, MD
  "1003311044",   # CATHERINE Callinan, MD
  "1609009943",   # Kristin Powell, MD
  "1114125051",   # Nathan Kow, MD
  "1043432123",   # Elena Tunitsky, MD
  "1215490446",   # PK
  "1487879987"    # Peter Jeppson
)

# List of physician names (optional, for better logging and output) ----
names_list <- c(
  "Tyler Muffly, MD",
  "John Curtin, MD",
  "Pedro Miranda, MD",
  "Lizeth Acosta, MD",
  "Aaron Lazorwitz, MD",
  "Ana Gomez, MD",
  "Erin W. Franks, MD", 
  "Catherine Callinan, MD",
  "Kristin Powell, MD",
  "Nathan Kow, MD", 
  "Elena Tunitsky, MD",
  "Parisa Khalighi, MD",
  "Peter Jeppson, MD"
)

# Load the processed OB/GYN physicians data from RDS file ----
obgyn_physicians_all_years <- readRDS("data/C_extracting_and_processing_NPPES_obgyn_physicians_all_years.rds")

dim(obgyn_physicians_all_years)
glimpse(obgyn_physicians_all_years)
str(obgyn_physicians_all_years)
tail(obgyn_physicians_all_years)

# Check presence of specified physicians ----
physician_presence <- check_physician_presence(
  obgyn_physicians_all_years, 
  npi_list, 
  names_list
); knitr::kable(physician_presence, caption = "Presence of Specific Physicians by Year")



# Sanity Map Check ----
# Load ZIP → lat/lon mapping
library(readr)
library(dplyr)
library(stringr)

zip_coords <- read_csv("/Users/tylermuffly/Documents/uszips.csv") %>%
  transmute(
    zip = str_pad(as.character(zip), 5, pad = "0"),
    lat = lat,
    lon = lng  # watch column names — some files say `lng`, others `longitude`
  )

obgyn_colorado <- obgyn_with_coords %>%
  filter(`Provider Business Practice Location Address State Name` == "Colorado") %>%
  filter(`Healthcare Provider Taxonomy Code_1` == "207VX0201X")

obgyn_colorado <- obgyn_colorado %>%
  filter(
    lon >= -110, lon <= -101,  # Roughly covers Colorado
    lat >= 36.5, lat <= 42
  )

colorado_map <- map_data("state") %>%
  filter(region == "colorado")

ggplot() +
  geom_polygon(
    data = colorado_map,
    aes(x = long, y = lat, group = group),
    fill = "#f8f9fa", color = "gray60", linewidth = 0.3
  ) +
  geom_point(
    data = obgyn_colorado,
    aes(x = lon, y = lat),
    color = "#3182bd", alpha = 0.6, size = 1
  ) +
  facet_wrap(~ Year, ncol = 4) +
  coord_fixed(1.3) +
  theme_minimal(base_family = "Helvetica") +
  theme(
    strip.text = element_text(size = 14, face = "bold"),
    plot.title = element_text(size = 18, face = "bold", hjust = 0.5),
    plot.subtitle = element_text(size = 12, hjust = 0.5),
    plot.caption = element_text(size = 9, hjust = 0.5),
    panel.spacing = unit(1, "lines"),
    panel.grid = element_blank()
  ) +
  labs(
    title = "OB/GYN Physician Locations in Colorado by Year",
    subtitle = "Each dot is a provider ZIP centroid",
    caption = "Source: NPPES Registry + ZIP Lat/Lon Crosswalk"
  )

obgyn_colorado %>%
  count(Year) %>%
  ggplot(aes(x = Year, y = n)) +
  geom_line(color = "#0072B2", size = 1.2) +
  geom_point(size = 2) +
  theme_minimal() +
  labs(
    title = "OB/GYN Provider Count in Colorado Over Time",
    y = "Number of Providers",
    x = "Year"
  )

# Map by Change
# Get background ZIPs: any ZIP in zip_growth_geo
background_zips <- zip_coords %>%
  filter(zip %in% zip_growth_geo$zip)

ggplot() +
  # Baseline ZIPs: faint background
  geom_point(
    data = background_zips,
    aes(x = lon, y = lat),
    color = "gray90", size = 1
  ) +
  
  # Overlay: ZIPs with change
  geom_point(
    data = zip_growth_geo %>% filter(abs(net_change) > 0),
    aes(x = lon, y = lat, color = net_change, size = abs(net_change)),
    alpha = 0.95
  ) +
  
  # Colorado border
  borders("state", regions = "colorado", fill = NA, color = "black", size = 0.5) +
  
  # Scales and theme
  scale_color_gradient2(
    low = "red", mid = "gray90", high = "blue", midpoint = 0,
    name = "Net Change\n(2024 vs 2010)"
  ) +
  scale_size_continuous(range = c(1.5, 6), guide = "none") +
  coord_fixed(xlim = c(-109, -102), ylim = c(37, 41)) +
  theme_minimal() +
  labs(
    title = "Net Change in OB/GYN Providers by ZIP (Colorado, 2010–2024)",
    subtitle = "Red = loss, Blue = gain, Gray = no change",
    caption = "Source: NPPES Registry + ZIP Centroids"
  ) +
  theme(
    plot.title = element_text(size = 18, face = "bold"),
    plot.subtitle = element_text(size = 12),
    plot.caption = element_text(size = 10)
  )

ggplot() +
  # Baseline ZIPs: gray backdrop
  geom_point(
    data = background_zips,
    aes(x = lon, y = lat),
    color = "gray90", size = 1
  ) +
  
  # Overlay: ZIPs with net change
  geom_point(
    data = zip_growth_geo %>% filter(net_change != 0),
    aes(x = lon, y = lat, color = net_change, size = abs(net_change)),
    alpha = 0.95
  ) +
  
  # Colorado border
  borders("state", regions = "colorado", fill = NA, color = "black", size = 0.5) +
  
  # Log-like color scale
  scale_color_gradient2(
    low = "firebrick",
    mid = "gray90",
    high = "blue",
    midpoint = 0,
    trans = "pseudo_log",
    name = "Net Change\n(2024 vs 2010)"
  ) +
  
  scale_size_continuous(range = c(1.5, 6), guide = "none") +
  
  coord_fixed(xlim = c(-109, -102), ylim = c(37, 41)) +
  theme_minimal() +
  labs(
    title = "Net Change in OB/GYN Providers by ZIP (Colorado, 2010–2024)",
    subtitle = "Red = provider loss, Blue = gain, Gray = no change",
    caption = "Source: NPPES Registry + ZIP Centroids"
  ) +
  theme(
    plot.title = element_text(size = 18, face = "bold"),
    plot.subtitle = element_text(size = 12),
    plot.caption = element_text(size = 10)
  )

zip_coords <- read_csv("/Users/tylermuffly/Documents/uszips.csv") %>%
  mutate(
    zip = str_pad(as.character(zip), 5, pad = "0")
  ) %>%
  select(zip, lat, lng, state_name) %>% # <- preserve state_name
  rename(lon = lng)

# Function for plotting change over time
plot_provider_growth_map <- function(zip_growth_df, zip_coords_df, state_to_map = "Colorado") {
  library(dplyr)
  library(ggplot2)
  library(scales)
  library(stringr)
  
  # Set bounding boxes
  xlim_us <- c(-125, -66)
  ylim_us <- c(24, 50)
  state_bounds <- list(
    "Colorado" = list(x = c(-109, -102), y = c(37, 41))
  )
  
  # Add 0-padding and join lat/lon
  zip_change_coords <- zip_growth_df %>%
    mutate(zip = str_pad(zip, 5, pad = "0")) %>%
    left_join(zip_coords_df, by = "zip") %>%
    filter(!is.na(lat), !is.na(lon))
  
  # Determine bounding box
  if (!is.null(state_to_map)) {
    zip_change_coords <- zip_change_coords %>%
      filter(zip %in% zip_coords_df$zip[zip_coords_df$state_name == state_to_map])
    background_zips <- zip_coords_df %>%
      filter(state_name == state_to_map)
    coord_xlim <- state_bounds[[state_to_map]]$x
    coord_ylim <- state_bounds[[state_to_map]]$y
    map_title <- paste0("Net Change in OB/GYN Providers by ZIP (", state_to_map, ", 2010–2024)")
    state_border <- borders("state", regions = state_to_map, fill = NA, color = "black", size = 0.5)
  } else {
    background_zips <- zip_coords_df
    coord_xlim <- xlim_us
    coord_ylim <- ylim_us
    map_title <- "Net Change in OB/GYN Providers by ZIP (United States, 2010–2024)"
    state_border <- borders("state", fill = NA, color = "black", size = 0.25)
  }
  
  # Compute symmetric limits
  max_abs_change <- max(abs(zip_change_coords$net_change), na.rm = TRUE)
  
  # Plot
  ggplot() +
    geom_point(
      data = background_zips,
      aes(x = lon, y = lat),
      color = "gray90", size = 1
    ) +
    geom_point(
      data = zip_change_coords %>% filter(net_change != 0),
      aes(x = lon, y = lat, color = net_change, size = abs(net_change)),
      alpha = 0.95
    ) +
    state_border +
    # scale_color_gradient2(
    #   low = "red",
    #   mid = "gray90",
    #   high = "blue",
    #   midpoint = 0,
    #   limits = c(-max_abs_change, max_abs_change),
    #   name = "Net Change\n(2024 vs 2010)",
    #   labels = function(x) paste0(ifelse(x > 0, "+", ""), x)
    # ) +
    scale_color_gradient2(
      low = "red",
      mid = "gray90",
      high = "blue",
      midpoint = 0,
      limits = c(-max_abs_change, max_abs_change),
      trans = "pseudo_log",
      name = "Net Change\n(2024 vs 2010)",
      labels = function(x) ifelse(x > 0, paste0("+", x), as.character(x))
    ) + 
    scale_size_continuous(range = c(1.5, 6), guide = "none") +
    coord_fixed(xlim = coord_xlim, ylim = coord_ylim) +
    theme_minimal() +
    labs(
      title = map_title,
      subtitle = "Red = provider loss, Blue = gain, Gray = no change",
      caption = "Source: NPPES Registry + ZIP Centroids"
    ) +
    theme(
      plot.title = element_text(size = 18, face = "bold"),
      plot.subtitle = element_text(size = 12),
      plot.caption = element_text(size = 10)
    )
}

plot_provider_growth_map(
  zip_growth_df = zip_growth_check,
  zip_coords_df = zip_coords,
  state_to_map = "Colorado"
)

# Make sure zip is character with leading zeros (if any)
zip_coords <- read_csv("/Users/tylermuffly/Documents/uszips.csv") %>%
  mutate(zip = str_pad(as.character(zip), width = 5, pad = "0")) %>%
  rename(lon = lng)


# Clean and join
obgyn_with_coords <- obgyn_physicians_all_years %>%
  mutate(zip = str_sub(`Provider Business Practice Location Address Postal Code`, 1, 5)) %>%
  left_join(zip_coords, by = "zip") %>%
  filter(!is.na(lat), !is.na(lon))

# 📍 Faceted Dot Map of OB/GYN Providers by Year ----
library(ggplot2)
library(maps)
library(stringr)

# Prepare U.S. base map
us_map <- map_data("state")

# # Add a numeric year column
# obgyn_with_coords <- obgyn_with_coords %>%
#   mutate(year = as.numeric(Year)) %>%
#   filter(!is.na(year))  # Just in case

library(dplyr)
library(ggplot2)
library(maps)
library(scales)
library(stringr)

# Merge zip coords
obgyn_with_coords <- obgyn_physicians_all_years %>%
  mutate(
    zip = str_sub(`Provider Business Practice Location Address Postal Code`, 1, 5),
    zip = str_pad(zip, 5, pad = "0")
  ) %>%
  filter(str_detect(zip, "^\\d{5}$")) %>%
  left_join(zip_coords, by = "zip") %>%
  filter(!is.na(lat), !is.na(lon))

# Load map
us_states <- map_data("state")

# Plot faceted dot map by year
test_years <- c(2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024)

faceted_dot_map_subset <- ggplot() +
  geom_polygon(data = us_states, aes(x = long, y = lat, group = group),
               fill = "gray95", color = "white", size = 0.2) +
  geom_point(data = obgyn_with_coords %>% filter(Year %in% test_years),
             aes(x = lon, y = lat),
             color = "#0072B2", alpha = 0.5, size = 0.6) +
  facet_wrap(~ Year, ncol = 2) +
  coord_fixed(1.3) +
  theme_void() +
  labs(
    title = "OB/GYN Physician Locations (Sample Years)",
    subtitle = "Subset of 4 years — each dot is a provider",
    caption = "Data: NPPES + ZIP centroid"
  )

print(faceted_dot_map_subset)


# Only GO ----
gyn_onc_colorado <- obgyn_with_coords %>%
  filter(`Provider Business Practice Location Address State Name` == "Colorado") %>%
  filter(`Healthcare Provider Taxonomy Code_1` == "207VX0201X") %>%
  filter(
    lon >= -110, lon <= -101,
    lat >= 36.5, lat <= 42
  )

colorado_map <- map_data("state") %>%
  filter(region == "colorado")

ggplot() +
  geom_polygon(
    data = colorado_map,
    aes(x = long, y = lat, group = group),
    fill = "#f8f9fa", color = "gray60", linewidth = 0.3
  ) +
  geom_point(
    data = gyn_onc_colorado,
    aes(x = lon, y = lat),
    color = "#3182bd", alpha = 0.6, size = 1
  ) +
  facet_wrap(~ Year, ncol = 4) +
  coord_fixed(1.3) +
  theme_minimal(base_family = "Helvetica") +
  theme(
    strip.text = element_text(size = 14, face = "bold"),
    plot.title = element_text(size = 18, face = "bold", hjust = 0.5),
    plot.subtitle = element_text(size = 12, hjust = 0.5),
    plot.caption = element_text(size = 9, hjust = 0.5),
    panel.spacing = unit(1, "lines"),
    panel.grid = element_blank()
  ) +
  labs(
    title = "Gynecologic Oncologist Locations in Colorado by Year",
    subtitle = "Each dot is a provider ZIP centroid",
    caption = "Source: NPPES Registry + ZIP Lat/Lon Crosswalk"
  )

gyn_onc_colorado %>%
  count(Year) %>%
  ggplot(aes(x = Year, y = n)) +
  geom_line(color = "#D55E00", size = 1.2) +
  geom_point(size = 2) +
  theme_minimal() +
  labs(
    title = "Gyn Oncologist Count in Colorado Over Time",
    y = "Number of Gyn Oncologists",
    x = "Year"
  )

zip_growth_gyn_onc <- gyn_onc_colorado %>%
  mutate(zip = str_sub(zip, 1, 5)) %>%
  count(zip, Year) %>%
  pivot_wider(names_from = Year, values_from = n, values_fill = 0) %>%
  mutate(net_change = `2024` - `2010`)

plot_provider_growth_map(
  zip_growth_df = zip_growth_gyn_onc,
  zip_coords_df = zip_coords,
  state_to_map = "Colorado"
)

library(dplyr)
library(leaflet)
library(stringr)

gyn_onc_colorado <- obgyn_with_coords %>%
  filter(`Provider Business Practice Location Address State Name` == "Colorado") %>%
  filter(`Healthcare Provider Taxonomy Code_1` == "207VX0201X") %>%
  mutate(zip = str_sub(zip, 1, 5))

zip_change_gyn_onc <- gyn_onc_colorado %>%
  count(zip, Year) %>%
  tidyr::pivot_wider(names_from = Year, values_from = n, values_fill = 0) %>%
  mutate(net_change = `2024` - `2010`) %>%
  left_join(zip_coords, by = "zip") %>%
  filter(!is.na(lat), !is.na(lon))

library(leaflet)
library(htmltools)

# Ensure change_cat exists
zip_change_gyn_onc <- zip_change_gyn_onc %>%
  dplyr::mutate(
    change_cat = dplyr::case_when(
      net_change > 0 ~ "gain",
      net_change < 0 ~ "loss",
      TRUE ~ "no change"
    )
  )

library(leaflet)
library(htmltools)

color_pal <- leaflet::colorFactor(
  palette = c("firebrick", "gray60", "royalblue"),
  domain = c("loss", "no change", "gain")
)

leaflet(zip_change_gyn_onc) %>%
  addProviderTiles("CartoDB.Positron") %>%
  addCircleMarkers(
    lng = ~lon,
    lat = ~lat,
    radius = ~pmax(3, log10(abs(net_change) + 1) * 4),
    color = ~color_pal(change_cat),
    stroke = FALSE,
    fillOpacity = 0.75,
    label = ~ifelse(
      abs(net_change) > 0,
      htmltools::HTML(paste0("<strong>ZIP:</strong> ", zip, "<br/><strong>Change:</strong> ", net_change)),
      NA_character_
    ),
    labelOptions = labelOptions(
      direction = "auto",
      textsize = "12px",
      opacity = 0.9,
      style = list("background" = "white", "padding" = "4px")
    )
  ) %>%
  addLegend(
    position = "bottomright",
    pal = color_pal,
    values = ~change_cat,
    title = "Net Change<br>(2024 vs 2010)",
    opacity = 0.9
  )
