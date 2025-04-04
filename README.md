Gynecologic Oncology Accessibility Project
================
Tyler Muffly, MD
2025-02-09

<!-- README.md is generated from README.Rmd. Please edit that file -->

## Project Overview

This project analyzes nationwide access to gynecologic oncologists and
other OBGYN subspecialists using drive time isochrones, demographic
data, and geospatial analysis. The analysis examines how accessibility
varies across different geographic areas, demographic groups, and time
periods (2013-2023).

Using the HERE Maps API and census data, we calculate drive time
isochrones around gynecologic oncologists’ locations and analyze the
demographics of populations within each drive time threshold. The
project examines changes in accessibility over time and retirement
patterns among specialists.

### Subspecialists Analyzed

- Female Pelvic Medicine and Reconstructive Surgery/Urogynecology
- Gynecologic Oncology
- Maternal-Fetal Medicine
- Reproductive Endocrinology and Infertility

## File Organization

### Core Analysis Workflow

#### Data Gathering

- `Postico_database_pull.R` - Extracts physician data from PostgreSQL
  database, enabling year-by-year analysis of physician practice
  locations from 2013 to 2022. Pulls “GYNECOLOGIC ONCOLOGY” from the
  Primary Specialty. For urogyn, we will need NPIs to go retrospectively
  to look for people.  
- `getting_isochrones_trying.R` - Alternative method for isochrone
  generation
- `retirement_adjusted.R` - Enhanced retirement analysis using multiple
  data sources (NPI deactivation, Medicare data, and board certification
  status) to improve workforce accuracy.
- `subspecialists over time.R` - Analysis of subspecialist trends
- `visualize_fips_inters_isochr.R` - Visualizes FIPS code intersections
- `fips_blocks_female_proportion.R` - Analyzes female population in FIPS
  blocks
- `fips_isochrones_population_intersect.R` - Examines population within
  isochrones

#### 1. Setup and Data Preparation

- `000-control.R` - Auxiliary script for data compilation
- `01-setup.R` - Loads packages, sets API keys, defines helper
  functions, initializes directory structure
- `02-search_taxonomy.R` - Search the NPPES Registry database using
  npi_search library in a wrapper. Taxonomy description from the NUCC:
  <https://taxonomy.nucc.org/>. Note recent change in FPMRS to URPS.  
- `02.5-subspecialists_over_time.R` - Analyzes subspecialist trends over
  multiple years
- `03-search_and_process_npi.R` - Processes National Provider Identifier
  (NPI) data
- `03a-search_and_process_extra.R` - Additional NPI processing for edge
  cases
- `04-geocode.R` - Geocodes provider addresses using the HERE API
- `zz05-geocode-cleaning.R` - Old technique with postmaster pulling
  apart the address.

#### 2. Isochrone Generation and Analysis

- `06-isochrones.R` - Generates drive time isochrones (30, 60, 120, 180
  min)
- `07-isochrone-mapping.R` - Maps isochrones and performs spatial joins
- `07.5-prep-get-block-group-overlap.R` - Prepares census block group
  data
- `08-get-block-group-overlap.R` - Calculates overlap between isochrones
  and census blocks
- `08.5-prep-the-census-variables.R` - Prepares demographic variables
  from Census
- `09-get-census-population.R` - Calculates population within/outside
  isochrones

#### 3. Results and Analysis

- `10-calculate-polygon-demographcs.R` - Analyzes demographic
  characteristics

- `10-make-region.R` - Creates regional maps and analyses

- `analyze_isochrone_data.R` - Framework for analyzing isochrone data

- `calculate_population_in_isochrones_by_race.R` - Analyzes population
  by race within isochrones

- `walker_isochrone_maps.R` - Visualizes isochrone changes over time

- `Access_Data.csv` - Data from Tannous that he arranged and is held in
  `data/`

### R Markdown Documents

- `GO_access_analysis_code.Rmd` - Statistical analysis of gynecologic
  oncology access
- `for_every_year_script_rmd.Rmd` - Year-by-year analysis of
  accessibility trends
- `isochrones.Rmd` - Tutorial on creating and analyzing isochrones

## Execution Order

For a complete analysis, the files should be executed in approximately
this order:

### Setup Phase

1.  `01-setup.R`
2.  `Postico_database_pull.R` (if external hardrive with the Positico
    database access is connected)

### Data Collection Phase

3.  `02-search_taxonomy.R`
4.  `02.5-subspecialists_over_time.R`
5.  `03-search_and_process_npi.R` - When did physicians start
    practicing?
6.  `03a-search_and_process_extra.R`
7.  `04-geocode.R`
8.  `05-geocode-cleaning.R`
9.  `retirement.R`/`retirement_adjusted.R` - When did physicians retire?
    (if physician retirement analysis is needed)

### Isochrone Analysis Phase

9.  `06-isochrones.R`
10. `07-isochrone-mapping.R`
11. `07.5-prep-get-block-group-overlap.R`
12. `08-get-block-group-overlap.R`
13. `08.5-prep-the-census-variables.R`
14. `09-get-census-population.R`

### Results and Additional Analysis Phase

15. `10-calculate-polygon-demographcs.R`

16. `10-make-region.R`

17. `script2025.R` - Downloads the population data and aggregates it by
    isochrone and by total population. Creates tables of women within
    isochrones and total women.  

``` r
> access_merged
# A tibble: 240 × 6
   year  range category              count     total percent
   <chr> <int> <chr>                 <dbl>     <dbl>   <dbl>
 1 2013   1800 total_female       72362517 162649954    44.5
 2 2013   1800 total_female_white 46553359 119180751    39.1
```

19. `analyze_isochrone_data.R` - Measures the slope for access from
    start 2013 to finish 2022. Finds significant increases or decreases
    in the number of women within a drive time. Some notable trends:

- Total female white population shows significant declines in access
  across all time thresholds
- Some categories like `total_female_asian` show significant increases
  in longer-time thresholds

``` r
> isochrone_results$trend_analysis
                 category        time_threshold         slope    r_squared     p_value start_year end_year start_value
year         total_female  access_30min_cleaned -7.363956e+05 1.126669e-01 0.343029928       2013     2022    72362517
year1        total_female  access_60min_cleaned -6.301068e+05 7.221735e-02 0.452792176       2013     2022    98337640
year2        total_female access_120min_cleaned -1.340719e+05 4.509434e-03 0.853764926       2013     2022   133049027
year3        total_female access_180min_cleaned -3.015709e+03 2.336200e-06 0.996656494       2013     2022   148447072
year4   total_female_aian  access_30min_cleaned  8.242703e+03 1.559138e-01 0.258786159       2013     2022      316937
year5   total_female_aian  access_60min_cleaned  1.109027e+04 1.747050e-01 0.229364327       2013     2022      453807
```

``` r
library(knitr)
knitr::include_graphics("figures/mean_access_by_group.png")
```

<img src="figures/mean_access_by_group.png" width="3000" />

``` r
knitr::include_graphics("figures/access_over_time.png")
```

<img src="figures/access_over_time.png" width="3000" />

``` r
knitr::include_graphics("figures/access_distribution.png")
```

<img src="figures/access_distribution.png" width="3000" />

22. `GO_access_analysis_code.Rmd`

23. `walker_isochrone_maps.R` - Creates a faceted map of the US, HI, AK,
    and PR with the isochrones in place.

24. `zzzcalculate_population_in_isochrones_by_race.R` - I’m unsure if it
    is needed.  

25. `zzzfor_every_year_script_rmd.Rmd` - This is THE SAME MAP THAT
    WALKER DID IN `walker_isochrone_maps.R` BUT HE DID IT BETTER.
    Creates a map of the isochrones for every year.

## Methods

### Data Sources

- Physician Data: National Plan and Provider Enumeration System (NPPES)
  files from 2013 to 2023
- Population Data: American Community Survey 5-year estimates and
  decennial census data
- Geographic Analysis: Used block groups rather than counties for finer
  data resolution
- Geographic Regions: American College of Obstetricians and
  Gynecologists (ACOG) Districts

### Analysis Approach

- Drive time isochrones (30, 60, 120, 180 minutes) calculated using HERE
  API
- Isochrones generated for the third Friday in October at 9:00 AM for
  each year
- Demographic analysis by race/ethnicity (White, Black, Asian or Pacific
  Islander, American Indian/Alaska Native)
- Comparison of urban vs. rural accessibility

## Prerequisites

- R 4.0.0 or higher
- Required R packages (listed in `01-setup.R`)
- HERE Maps API key
- Census API key
- PostgreSQL database (optional, for historical physician data)

## Tools and Data Management

### HERE API

- Used for geocoding and isochrone generation
- Geocoding and Search: \$0.83 per 1,000 searches after 30,000 free
  geocodes
- Isoline Routing: \$5.50 per 1,000 after 2,500 free isoline routings

### Data Storage

- GitHub LFS (Large File Storage) for managing large files
- DuckDB for efficient data querying
- PostgreSQL database for year-specific physician data

### Auxiliary Tools

- tyler package: Custom package for project-specific functions
- Exploratory.io: Used for data wrangling

## Key Outputs

- Drive time isochrones at multiple thresholds (30, 60, 120, 180
  minutes)
- Population statistics within/outside isochrones
- Demographic analysis by race/ethnicity
- Temporal trends in accessibility (2013-2023)
- Visualizations of geographic access patterns

## Data Sources

For downloading NPPES files:

``` bash
wget -P "/Volumes/Video Projects Muffly 1/nppes_historical_downloads" "https://download.cms.gov/nppes/NPPES_Data_Dissemination_April_2024.zip"
```

## License

\[Specify your license here\]

## Contact

Tyler Muffly, MD - \[Add contact information if appropriate\]
