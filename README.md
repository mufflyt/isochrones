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

## Materials and Methods

### Data Sources

**Physician Provider Data**: National Plan and Provider Enumeration
System (NPPES) files from 2013 to 2023 were obtained from the National
Bureau of Economic Research (NBER) cumulative dataset, which provides
deduplicated historical provider records spanning April 2007 to present,
addressing limitations in official CMS offerings that lack historical
provider details. The dataset is deduplicated based on variable changes
(excluding mere updates in file names or years), and includes separate
core and multiplicative variable files to manage large file sizes
efficiently.

Additional validation data sources included: - Medicare Part D
prescriber data (2013-2022) for providers treating Medicare patients
over 65 years old - CMS facility affiliation data (2014-present) serving
as the “gold standard” for current practice locations  
- CMS Open Payments data (2013-present) for cross-verification of
provider activity status - NPI deactivation data for tracking provider
retirement, though may lag real-time by 1-2 years due to self-reporting

**Population and Demographic Data**: American Community Survey (ACS)
5-year estimates and decennial census data provided demographic and
population information. Census block groups were used as the primary
geographic unit rather than counties to achieve higher spatial
resolution for accessibility calculations.

**Geographic Data**: Census TIGER/Line shapefiles provided geographic
boundaries. Rural-Urban Commuting Area (RUCA) codes from the USDA
Economic Research Service were used for rural-urban classification.

### Provider Identification and Classification

Gynecologic oncologists and other OBGYN subspecialists were identified
using National Uniform Claim Committee (NUCC) Healthcare Provider
Taxonomy codes. The analysis searched all taxonomy columns in each
year’s dataset (not just the primary taxonomy) to capture physicians
with multiple subspecialty certifications.

``` r
# Taxonomy codes for OBGYN subspecialties analyzed
obgyn_taxonomy_codes <- c(
  "207V00000X",    # Obstetrics & Gynecology
  "207VX0201X",    # Gynecologic Oncology (PRIMARY FOCUS)
  "207VE0102X",    # Reproductive Endocrinology
  "207VG0400X",    # Gynecology
  "207VM0101X",    # Maternal & Fetal Medicine
  "207VF0040X",    # Female Pelvic Medicine/Urogynecology
  "207VB0002X",    # Bariatric Medicine
  "207VC0200X",    # Critical care medicine
  "207VC0040X", 
  "207VC0300X",    # Complex family planning
  "207VH0002X",    # Palliative care
  "207VX0000X"     # Obstetrics only
)
```

### Quality Control and Data Validation

The `check_physician_presence` function was implemented as a quality
control utility for tracking physicians across temporal data. This
function efficiently analyzes datasets containing physician information
to determine when specific providers appear in records, accepting
National Provider Identifiers (NPIs) and methodically examining each
NPI’s presence throughout different years. It returns structured data
summarizing each provider’s representation, including total record count
and chronological listing of years present.

``` r
# Example validation list of known NPIs for quality control
npi_list <- c(
  "1689603763",   # Tyler Muffly, MD
  "1528060639",   # John Curtin, MD
  "1346355807",   # Pedro Miranda, MD
  # ... additional validation cases
)
```

### Geocoding and Address Standardization

Provider practice addresses were geocoded using the HERE Maps Geocoding
API (version 6.2). Addresses were standardized and cleaned prior to
geocoding, with quality scoring and manual review of poor matches.
Coordinate validation included bounds checking and outlier detection.

### Isochrone Generation and Spatial Analysis

Drive time isochrones were calculated using the HERE Maps Isoline
Routing API (version 7.2) with the following specifications:

- **Time Thresholds**: 30, 60, 120, and 180 minutes
- **Reference Time**: Third Friday of October at 9:00 AM local time for
  each analysis year (2013-2023)  
- **Vehicle Type**: Car
- **Traffic Conditions**: Real-time traffic enabled
- **Route Quality**: Highest quality setting

The reference time was standardized across all years to ensure temporal
consistency, selected to represent typical weekday travel conditions
while avoiding rush hour peaks.

### Demographic and Population Analysis

Population within each isochrone was calculated through spatial
intersection of drive time polygons with census block groups.
Area-weighted population estimates were computed for partial block group
overlaps.

**Primary Analysis Population**: Female population, with demographic
stratification by: - Race/ethnicity: White alone, Black or African
American alone, Asian alone, American Indian and Alaska Native alone -
Geographic regions: American College of Obstetricians and Gynecologists
(ACOG) Districts - Rural-urban classification: RUCA codes

### Statistical Analysis

Temporal trends in accessibility were analyzed using linear regression
to measure changes from 2013 to 2022. Statistical significance was
assessed at p \< 0.05. Slope calculations, R-squared values, and
p-values were computed for each demographic category and drive time
threshold.

## Results

### Overall Access to Gynecologic Oncologists

**Baseline Access (2013)**: Among the total female population, 72.4
million women (44.5% of total population) had access to gynecologic
oncologists within a 30-minute drive time. Access increased with longer
drive times: 98.3 million women (60.4%) within 60 minutes, 133.0 million
women (81.8%) within 120 minutes, and 148.4 million women (91.3%) within
180 minutes.

**Current Access (2022)**: Access levels remained relatively stable,
with 71.6 million women having 30-minute access, 97.7 million women
having 60-minute access, 132.9 million women having 120-minute access,
and 148.4 million women having 180-minute access.

``` r
# Example access data structure
> access_merged
# A tibble: 240 × 6
   year  range category              count     total percent
   <chr> <int> <chr>                 <dbl>     <dbl>   <dbl>
 1 2013   1800 total_female       72362517 162649954    44.5
 2 2013   1800 total_female_white 46553359 119180751    39.1
```

### Temporal Trends in Access (2013-2022)

Statistical analysis of temporal trends revealed significant variations
by demographic group:

**Total Female Population**: Showed declining trends across all drive
time thresholds, though not all reached statistical significance: -
30-minute access: -736,396 women per year (R² = 0.113, p = 0.343) -
60-minute access: -630,107 women per year (R² = 0.072, p = 0.453)  
- 120-minute access: -134,072 women per year (R² = 0.005, p = 0.854) -
180-minute access: -3,016 women per year (R² \< 0.001, p = 0.997)

**Racial/Ethnic Disparities**: - **White women**: Significant declines
in access observed across all time thresholds - **Asian women**:
Significant increases in longer drive time access (120+ minutes) -
**American Indian/Alaska Native women**: Mixed trends with some
increases in 30- and 60-minute access categories, though starting from
lower baseline access levels

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

### Geographic Patterns

Analysis revealed persistent rural-urban divides and regional variations
in access to gynecologic oncology care. Geographic regions with highest
access density included major metropolitan areas and academic medical
centers, while rural and frontier areas showed significantly reduced
access across all time thresholds.

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

### Subspecialist Workforce Trends

The analysis of subspecialist workforce over time revealed relatively
stable provider numbers but shifting geographic distribution. Retirement
pattern analysis using multiple data sources (NPI deactivation, Medicare
prescribing data, and board certification status) improved accuracy of
active workforce estimates.

## File Organization

#### Data Gathering

- `bespoke_functions.R` - Data for the duckDB connection and functions
  for the Data Gathering lettered code below. The National Bureau of
  Economic Research (NBER) provides a cumulative NPI/NPPES dataset
  created from monthly CMS files spanning from April 2007 to the
  present, addressing limitations in official CMS offerings that lack
  historical provider details. The dataset is deduplicated based on
  variable changes (excluding mere updates in file names or years), and
  includes separate core and multiplicative variable files to manage the
  large file sizes efficiently.

| **Dataset/Script Name** | **Available Years** | **Notes** |
|:---|:---|:---|
| **`A_nppes_download.R`** | 2007–2022 | NBER cumulative NPPES file (monthly updates deduplicated; includes changes back to 2007). |
| **`A-NPI_deactivation_download.R`** | Varies, slightly delayed | Based on self-reported deactivation. CMS updates irregularly; may lag real-time by 1–2 years. |
| **`A-Medicare_part_d_prescribers_data_downloaded`** | 2013–2022 | Released annually by CMS; first available dataset based on 2013 prescribing data. |
| **`A-download_physician_compare_download`** | ~2013–2020 (downloaded files) | ICPSR archive has files through ~2020 (Physician Compare closed in Dec 2020). Must manually log in. |
| **`A-facility_affiliation_download.R`** | 2014–present | First Facility Affiliation data started ~2014; yearly updates (CMS Physician Compare Affiliation data). |
| **`A-open_payments_download.R`** | 2013–present | Open Payments (Sunshine Act) started reporting payments in 2013. Annual updates. |

- `A_nppes_download.R` - Downloads NPPES data for back years from NBER
  (National Board Economic Research). The National Bureau of Economic
  Research (NBER) provides a cumulative NPI/NPPES dataset created from
  monthly CMS files spanning from April 2007 to 2022, addressing
  limitations in official CMS offerings that lack historical provider
  details. The dataset is deduplicated based on variable changes
  (excluding mere updates in file names or years), and includes separate
  core and multiplicative variable files to manage the large file sizes
  efficiently.

- `A-NPI_deactivation_download.R` - NPPES data may be a few years behind
  and is all based on personal report.

- `A-Medicare_part_d_prescribers_data_downloaded` - Good for docs who
  prescribe drugs to Medicare patients over 65 years old. Started 2013
  to 2022.

- `A-download_physician_compare_download` - To get old physician
  compare/national downloadable files we need to log in manually to OPEN
  ICPSR:
  <https://www.openicpsr.org/openicpsr/project/149961/version/V1/view?path=/openicpsr/149961/fcr:versions/V1&type=project>.
  You have to login with google account to download it.

- `A-facility_affiliation_download.R` - Gold standard.

- `A-open_payments_download.R` - Open Payments.

- `B-read_in_csv_file_to_duckDB_database.R` - Reads in the NPPES CSV
  fils from the NBER to the duckDB database. The primary action occurs
  via the process_nppes_data function (defined in an external script
  bespoke_functions.R), which reads and processes a large CSV file
  containing historical NPI data (spanning May 2005 to October 2020).
  The processed data is stored in a DuckDB database file.

- `C-Extracting_and_Processing_NPPES_Provider_Data.R` - Function for
  processing OBGYNs from NPPES data with exact file names for years 2010
  to 2022. Automatically identifies and maps database tables to their
  corresponding years, enabling efficient extraction of provider data
  across different time periods. Specifies taxonomy codes representing
  various OB/GYN subspecialties, including general obstetrics and
  gynecology, maternal-fetal medicine, and female pelvic medicine. Looks
  at all taxonomy columns in each year’s dataset (not just the first
  one). Checks if any of your specified codes appear in any of those
  columns. Includes a physician in the results if there’s a match in any
  column. Retrieves provider records matching specified OB/GYN taxonomy
  codes from each year, standardizing and combining the data into a
  unified dataset.

- `D-Quality_check_on_NPPES_merge.R` - The check_physician_presence
  function is a well-designed utility for tracking physicians across
  temporal data. It efficiently analyzes a dataset containing physician
  information to determine when specific providers appear in the
  records. The function accepts a list of National Provider Identifiers
  (NPIs), optionally paired with provider names, and methodically
  examines each NPI’s presence throughout different years. It returns a
  structured data frame summarizing each provider’s representation in
  the dataset, including their total record count and a chronological
  listing of years in which they appear. This function is particularly
  valuable for longitudinal analyses of healthcare provider data,
  enabling researchers to identify patterns in physician presence, track
  career trajectories, or validate data completeness across multiple
  years of NPI records.

- `E-Medicare_part_d_prescribers_data_processing.R` - This script
  processes Medicare Part D prescribing data from the Centers for
  Medicare & Medicaid Services (CMS). Process each table filtering
  “Prscrbr_Type” only OBGYN and “Gynecological Oncology”. It identifies
  providers’ prescribing patterns, cleans data by removing outlier
  records (claim counts over 50,000), annotates records by year, and
  merges multiple years into a single standardized dataset.
  Additionally, it calculates the last consecutive year each provider
  actively prescribed medications under Medicare Part D, facilitating
  analysis of provider activity and continuity over time. NOT GOOD FOR
  DOCS WHO DO NOT TREAT PATIENTS \>65 years old.

- `F-retirement_year_confirmation.R` - Download the massive data files
  to the external hard drive for this with one set of code then then can
  run E-retirement_year_confirmation.R.

Retirement Year Data download: `NPPES_deactivated_download.R` - Best
source but may be late.  
`Medicare_part_d_prescribers_data_processing` - People who prescribed to
\>65 year old women.  
`download_physician_compare_data.R` - Includes data for people who see
Medicare.  
facility affiliation. - Does not include a year for the facility
affiliation so it is not helpful.  
ABMS - scraped.

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
- `zzzPostico.R` - Used Postico originally. Able to use duckDB later
  on.  
- `Postico_database_pull.R` - Extracts physician data from PostgreSQL
  database, enabling year-by-year analysis of physician practice
  locations from 2013 to 2022. Pulls “GYNECOLOGIC ONCOLOGY” from the
  Primary Specialty. For urogyn, we will need NPIs to go retrospectively
  to look for people.

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
18. `analyze_isochrone_data.R` - Measures the slope for access from
    start 2013 to finish 2022. Finds significant increases or decreases
    in the number of women within a drive time.
19. `GO_access_analysis_code.Rmd` - Comprehensive statistical analysis
    report
20. `walker_isochrone_maps.R` - Creates a faceted map of the US, HI, AK,
    and PR with the isochrones in place.

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

MIT License

## Contact

Tyler Muffly, MD - <tyler.muffly@dhha.org>
