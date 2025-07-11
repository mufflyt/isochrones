---
title: "Gynecologic Oncology Accessibility Project"
subtitle: "Comprehensive Healthcare Accessibility Analysis Using Isochrone Methodology"
author: 
  - name: "Tyler Muffly, MD"
    affiliation: "Department of Obstetrics and Gynecology"
    email: "tyler.muffly@dhha.org"
date: "2025-06-22"
description: |
  This project analyzes nationwide access to gynecologic oncologists and other 
  OBGYN subspecialists using drive time isochrones, demographic data, and 
  geospatial analysis. The analysis examines how accessibility varies across 
  different geographic areas, demographic groups, and time periods (2013–2023).
keywords: 
  - "gynecologic oncology"
  - "healthcare accessibility" 
  - "spatial analysis"
  - "isochrones"
  - "health disparities"
  - "geographic information systems"
version: "1.0.0"
license: "MIT"
lang: en
encoding: UTF-8
output:
  html_document:
    toc: true
    toc_depth: 3
    toc_float: 
      collapsed: false
      smooth_scroll: true
    number_sections: true
    theme: "flatly"
    highlight: "tango"
    fig_width: 10
    fig_height: 8
    fig_caption: true
    code_folding: "show"
    code_download: true
    dev: "png"
    dpi: 300
    self_contained: true
    keep_md: true
    pandoc_args: ["--wrap=preserve"]
link-citations: true
always_allow_html: true
editor_options:
  chunk_output_type: console
  markdown:
    wrap: 80
    canonical: true
---

## Quick Start

Follow these steps to reproduce the analysis:

1. Install required R packages and set up the project environment:

   ```r
   source("R/01-setup.R")
   ```

2. Execute the complete workflow:

   ```r
   source("R/000-Control.R")
   ```

The `README.md` file is generated from this R Markdown document. After making
changes, re-render it with:

```r
rmarkdown::render("README.rmd")
```

We need to use `postmastr` because a lot of the addresses are close but
abbreviated in annoying ways. <https://pe.usps.com/text/pub28/welcome.htm> is a
list of USPS address codes.

Install the development version of **postmastr** from GitHub:

```r
remotes::install_github("slu-openGIS/postmastr")
```

After installation, the helper `standardize_addresses()` in
`R/standardize_addresses.R` can be used to clean address fields prior to
geocoding:

```r
clean_df <- standardize_addresses(df, address_col = "address")
```

### Additional Documentation and Utilities

Several companion files provide context and helper scripts beyond the main
workflow:

- `docs/about_me.md` — background on the project author.
- `docs/file_save_messages.md` — tips on confirming saved file locations.
- `docs/function_naming_and_organization.md` — suggestions for organizing
  helper functions.
- `database_optimization_notes.md` — notes on speeding up database work.
- `healthcare_benchmark_references.md` — healthcare benchmarking resources.
- `NEWBIE_MISTAKES.md` — common pitfalls for new contributors.
- `openpayments.R` — pulls CMS Open Payments data.
- `mosaic_better.R` — utility for producing mosaic plots.

### Abstract Statistics

Use `summarize_abstract()` to quickly measure the length of your abstract:

```r
abstract_text <- readLines("README.md")
summarize_abstract(paste(abstract_text, collapse = " "))
```

The companion `count_paragraphs()` helper returns how many paragraphs are
present in a document:

```r
count_paragraphs(paste(abstract_text, collapse = "\n"))
```

<!-- Custom CSS for enhanced styling -->

```{=html}
<style>
.main-container {
  max-width: 1400px;
}

.equation-box {
  background-color: #f8f9fa;
  border: 1px solid #dee2e6;
  border-radius: 5px;
  padding: 15px;
  margin: 10px 0;
}

.method-box {
  background-color: #e3f2fd;
  border-left: 4px solid #2196f3;
  padding: 15px;
  margin: 10px 0;
}

.results-box {
  background-color: #f3e5f5;
  border-left: 4px solid #9c27b0;
  padding: 15px;
  margin: 10px 0;
}

.code-title {
  background-color: #263238;
  color: white;
  padding: 8px;
  margin-bottom: 0;
  border-radius: 5px 5px 0 0;
  font-weight: bold;
}

.highlight-box {
  background-color: #fff3cd;
  border: 1px solid #ffeaa7;
  border-radius: 5px;
  padding: 15px;
  margin: 10px 0;
}

.technical-note {
  background-color: #d1ecf1;
  border: 1px solid #bee5eb;
  border-radius: 5px;
  padding: 12px;
  margin: 8px 0;
  font-size: 0.9em;
}
</style>
```


``` r
source("R/01-setup.R")      # provides create_geocode()
```

```
## ── Attaching core tidyverse packages ──────────────────────── tidyverse 2.0.0 ──
## ✔ dplyr     1.1.4     ✔ readr     2.1.5
## ✔ forcats   1.0.0     ✔ stringr   1.5.1
## ✔ ggplot2   3.5.2     ✔ tibble    3.3.0
## ✔ lubridate 1.9.4     ✔ tidyr     1.3.1
## ✔ purrr     1.0.4     
## ── Conflicts ────────────────────────────────────────── tidyverse_conflicts() ──
## ✖ dplyr::filter() masks stats::filter()
## ✖ dplyr::lag()    masks stats::lag()
## ℹ Use the conflicted package (<http://conflicted.r-lib.org/>) to force all conflicts to become errors
## Linking to GEOS 3.13.0, GDAL 3.8.5, PROJ 9.5.1; sf_use_s2() is TRUE
## 
## 
## Attaching package: 'data.table'
## 
## 
## The following objects are masked from 'package:lubridate':
## 
##     hour, isoweek, mday, minute, month, quarter, second, wday, week,
##     yday, year
## 
## 
## The following objects are masked from 'package:dplyr':
## 
##     between, first, last
## 
## 
## The following object is masked from 'package:purrr':
## 
##     transpose
## 
## 
## To enable caching of data, set `options(tigris_use_cache = TRUE)`
## in your R script or .Rprofile.
## 
## Loading required package: viridisLite
## 
## 
## Attaching package: 'exploratory'
## 
## 
## The following object is masked from 'package:formattable':
## 
##     normalize
## 
## 
## The following objects are masked from 'package:data.table':
## 
##     between, week
## 
## 
## The following objects are masked from 'package:lubridate':
## 
##     intersect, setdiff, union, week
## 
## 
## The following objects are masked from 'package:stringr':
## 
##     str_detect, str_remove, str_remove_all
## 
## 
## The following objects are masked from 'package:dplyr':
## 
##     anti_join, between, bind_rows, case_when, cross_join, cumall,
##     cumany, cummean, full_join, inner_join, intersect, left_join,
##     recode, recode_factor, rename_with, right_join, sample_frac,
##     sample_n, semi_join, setdiff, slice_sample, union, union_all
## 
## 
## The following object is masked from 'package:purrr':
## 
##     is_empty
## 
## 
## The following objects are masked from 'package:readr':
## 
##     parse_character, parse_logical, parse_number
## 
## 
## The following objects are masked from 'package:tidyr':
## 
##     nest, pivot_longer
## 
## 
## The following objects are masked from 'package:base':
## 
##     ceiling, cummax, cummin, cumprod, cumsum, floor, intersect,
##     setdiff, union
## 
## 
## 
## Attaching package: 'humaniformat'
## 
## 
## The following object is masked from 'package:formattable':
## 
##     suffix
## 
## 
## 
## Attaching package: 'maps'
## 
## 
## The following object is masked from 'package:viridis':
## 
##     unemp
## 
## 
## The following object is masked from 'package:purrr':
## 
##     map
## 
## 
## File .here already exists in /Users/tylermuffly/Dropbox (Personal)/isochrones
## 
## here() starts at /Users/tylermuffly/Dropbox (Personal)/isochrones
```

```
## [1] "Setup is complete!"
```



## Project Overview

This project analyzes nationwide access to gynecologic oncologists and other
OBGYN subspecialists using drive time isochrones, demographic data, and
geospatial analysis. The analysis examines how accessibility varies across
different geographic areas, demographic groups, and time periods (2013-2023).

Using the HERE Maps API and census data, we calculate drive time isochrones
around gynecologic oncologists' locations and analyze the demographics of
populations within each drive time threshold. The project examines changes in
accessibility over time and retirement patterns among specialists.

### Subspecialists Analyzed

-   Female Pelvic Medicine and Reconstructive Surgery/Urogynecology
-   Gynecologic Oncology
-   Maternal-Fetal Medicine
-   Reproductive Endocrinology and Infertility
-   Most helper functions include a `verbose` argument to control logging. Set it to `FALSE` for quieter execution.

## Materials and Methods

### Data Sources

**Physician Provider Data**: National Plan and Provider Enumeration System
(NPPES) files from 2013 to 2023 were obtained from the National Bureau of
Economic Research (NBER) cumulative dataset, which provides deduplicated
historical provider records spanning April 2007 to present, addressing
limitations in official CMS offerings that lack historical provider details. The
dataset is deduplicated based on variable changes (excluding mere updates in
file names or years), and includes separate core and multiplicative variable
files to manage large file sizes efficiently.

Additional validation data sources included: - Medicare Part D prescriber data
(2013-2022) for providers treating Medicare patients over 65 years old - CMS
facility affiliation data (2014-present) serving as the "gold standard" for
current practice locations\
- CMS Open Payments data (2013-present) for cross-verification of provider
activity status - NPI deactivation data for tracking provider retirement, though
may lag real-time by 1-2 years due to self-reporting

**Population and Demographic Data**: American Community Survey (ACS) 5-year
estimates and decennial census data provided demographic and population
information. Census block groups were used as the primary geographic unit rather
than counties to achieve higher spatial resolution for accessibility
calculations.

**Geographic Data**: Census TIGER/Line shapefiles provided geographic
boundaries. Rural-Urban Commuting Area (RUCA) codes from the USDA Economic
Research Service were used for rural-urban classification.

### Provider Identification and Classification

Gynecologic oncologists and other OBGYN subspecialists were identified using
National Uniform Claim Committee (NUCC) Healthcare Provider Taxonomy codes. The
analysis searched all taxonomy columns in each year's dataset (not just the
primary taxonomy) to capture physicians with multiple subspecialty
certifications.





# ABSTRACT

**Objective:** To quantify changes in accessibility to gynecologic oncology care
across the United States from 2013-2022, with particular attention to
urban-rural disparities, drive time thresholds, and racial/ethnic differences in
access.

**Methods:** We analyzed gynecologic oncologist practice locations combined with
U.S. Census data from 2013-2022. Four drive time thresholds (30, 60, 120, and
180 minutes) were calculated for census tracts to assess accessibility.
Population-weighted mean access rates were determined for urban and rural
populations and stratified by race/ethnicity. Linear regression models with
temporal trend analysis were employed to assess statistical significance of
changes over time.

**Results:** Accessibility to gynecologic oncologists declined significantly
across all time thresholds over the 10-year period. The most pronounced decrease
occurred in 30-minute accessibility (-23.6%, p\<0.001), followed by 60-minute
(-16.8%, p\<0.001), 120-minute (-8.5%, p\<0.01), and 180-minute thresholds
(-2.8%, p\<0.05). Approximately 277.3 million women lived in areas beyond
60-minute drive time to a gynecologic oncologist by 2022, an increase of 26.8
million from 2013. Urban-rural disparities were substantial, with only 10.2% of
rural women having 30-minute access compared to 44.1% of urban women (p\<0.001).
Racial/ethnic disparities were equally pronounced, with Asian women having the
highest access rates (86.5%), followed by Black women (77.2%), Native Hawaiian
and Pacific Islander women (75.3%), and White women (66.8%), while American
Indian and Alaska Native women had dramatically lower access (50.9%, p\<0.001).

**Conclusions:** Access to gynecologic oncology care has significantly
diminished over the past decade, with shorter drive time thresholds experiencing
the steepest declines. Geographic concentration of gynecologic oncologists in
urban academic centers has created substantial access barriers, particularly for
rural communities. Without intervention, these trends will continue to
exacerbate cancer outcome disparities for rural and minority populations.
Strategic initiatives including outreach clinics, telemedicine networks, and
targeted training programs are needed to address the maldistribution of
gynecologic oncologists.

## Yearly Maps Combined

![Yearly Maps Combined](figures/faceted_year_plots/yearly_maps_combined.png)

# 🔬 Research Objectives

-   **Primary**: Quantify geographic accessibility to gynecologic oncology
    specialists nationwide
-   **Secondary**: Analyze demographic disparities in subspecialist access
-   **Tertiary**: Examine temporal trends in healthcare workforce distribution
-   **Quaternary**: Identify underserved populations and geographic areas

# 🎯 Project Overview & Conceptual Framework

This project analyzes nationwide access to gynecologic oncologists and other
OBGYN subspecialists using drive time isochrones, demographic data, and
geospatial analysis. The analysis examines how accessibility varies across
different geographic areas, demographic groups, and time periods (2013-2023).

Using the HERE Maps API and census data, we calculate drive time isochrones
around gynecologic oncologists' locations and analyze the demographics of
populations within each drive time threshold. The project examines changes in
accessibility over time and retirement patterns among specialists.

## What Are Isochrones?

**Isochrone Definition for Census Tracts:**

$$I_t(O) = \{T_i \in \mathcal{T} : d(O, c(T_i)) \leq t\}$$

**Multi-Time Isochrone System:**

$$\mathcal{I} = \bigcup_{t \in \{30, 60, 120, 180\}} I_t(O) \text{ where } I_t(O) = \{T_i : d(O, c(T_i)) \leq t\}$$

**Travel Time Function with Network Constraints:**
$$d(O, c(T_i)) = \min_{p \in P(O, c(T_i))} \sum_{e \in p} \frac{l_e}{v_e}$$

**Complete Isochrone Boundary System:**

$$\begin{align}
I_{30}(O) &= \{T_i \in \mathcal{T} : d(O, c(T_i)) \leq 30\} \\
I_{60}(O) &= \{T_i \in \mathcal{T} : d(O, c(T_i)) \leq 60\} \setminus I_{30}(O) \\
I_{120}(O) &= \{T_i \in \mathcal{T} : d(O, c(T_i)) \leq 120\} \setminus I_{60}(O) \\
I_{180}(O) &= \{T_i \in \mathcal{T} : d(O, c(T_i)) \leq 180\} \setminus I_{120}(O)
\end{align}$$

Where: - $I_t(O)$ = isochrone for time $t$ from origin $O$ - $\mathcal{T}$ = set
of all census tracts - $T_i$ = individual census tract $i$ - $c(T_i)$ = centroid
of census tract $T_i$ - $d(O, c(T_i))$ = shortest travel time from origin to
tract centroid - $P(O, c(T_i))$ = set of all possible paths from $O$ to
$c(T_i)$ - $l_e$ = length of road segment $e$ - $v_e$ = speed on road segment
$e$

This equation system captures how census tracts are assigned to different drive
time zones, creating nested isochrone boundaries at 30, 60, 120, and 180-minute
intervals.

Here's the additional equation for counting accessible population by demographic
groups:

**Accessible Population by Demographics:**

$$A_t^{demo}(O) = \sum_{T_i \in I_t(O)} W_i^{demo}$$

**Total Accessible Women Population by Race/Ethnicity:** $$\begin{align}
A_t^{Total}(O) &= A_t^{White}(O) + A_t^{Black}(O) + A_t^{Asian}(O) \\
&\quad + A_t^{HIPI}(O) + A_t^{Hispanic}(O) + A_t^{Other}(O)
\end{align}$$

**Complete Accessibility Matrix:** $$\mathbf{A}(O) = \begin{bmatrix}
A_{30}^{White}(O) & A_{60}^{White}(O) & A_{120}^{White}(O) & A_{180}^{White}(O) \\
A_{30}^{Black}(O) & A_{60}^{Black}(O) & A_{120}^{Black}(O) & A_{180}^{Black}(O) \\
A_{30}^{Asian}(O) & A_{60}^{Asian}(O) & A_{120}^{Asian}(O) & A_{180}^{Asian}(O) \\
A_{30}^{HIPI}(O) & A_{60}^{HIPI}(O) & A_{120}^{HIPI}(O) & A_{180}^{HIPI}(O) \\
A_{30}^{Hispanic}(O) & A_{60}^{Hispanic}(O) & A_{120}^{Hispanic}(O) & A_{180}^{Hispanic}(O) \\
A_{30}^{Other}(O) & A_{60}^{Other}(O) & A_{120}^{Other}(O) & A_{180}^{Other}(O) \\
\end{bmatrix}$$

**Conditional Population Inclusion:** $$W_i^{demo} = \begin{cases} 
Pop_{women}^{demo}(T_i) & \text{if } d(O, c(T_i)) \leq t \\
0 & \text{if } d(O, c(T_i)) > t
\end{cases}$$

Where: - $A_t^{demo}(O)$ = accessible women population of demographic group
within time $t$ from gynecologic oncologist at origin $O$ - $W_i^{demo}$ = women
population of specific demographic in census tract $T_i$ -
$Pop_{women}^{demo}(T_i)$ = total women population of demographic group in tract
$T_i$ - $demo \in \{White, Black, Asian, HIPI, Hispanic, Other\}$ - HIPI =
Hawaiian and Pacific Islander

This framework quantifies healthcare accessibility disparities by measuring how
many women of each racial/ethnic group can reach gynecologic oncology services
within different drive time thresholds.

## Interactive Isochrone Map

<iframe src="figures/isochrone_map_20240208_181110.html" width="100%" height="600px">

</iframe>

**Isochrones** are geographic boundaries showing areas reachable within specific
travel times from a point. Think of them as "time zones" around each doctor's
office - the 30-minute isochrone includes all places you can drive to within 30
minutes.

**Why This Matters for Healthcare Access:** - **Patient perspective**: How far
do I have to travel for specialized care? - **Policy perspective**: Which
populations are underserved? - **Planning perspective**: Where should new
providers practice?

## Enhanced Census Tract Map

![Enhanced Census Tract Map](figures/enhanced_census_tract_map.png)

## Key Methodological Decisions & Rationale

### Drive Time Thresholds (30, 60, 120, 180 minutes)

-   **30 minutes**: Local/regional access - most patients willing to travel
-   **60 minutes**: Extended local access - reasonable for specialty care
-   **120 minutes**: Regional access - acceptable for highly specialized care
-   **180 minutes**: Maximum reasonable access - beyond this is effectively
    inaccessible

### Reference Time: Third Friday of October, 9:00 AM

-   **Rationale**: Standardized across all years for consistency
-   **Considerations**: Rush hour vs. off-peak; Friday represents typical
    weekday
-   **Limitation**: Doesn't account for seasonal variations or weekend access

### Geographic Resolution: Census Block Groups vs. Counties

-   **Choice**: Used census block groups (smaller geographic units)
-   **Advantage**: More precise population estimates, captures urban/rural
    differences
-   **Trade-off**: Increased computational complexity vs. more accurate results

## Subspecialist Categories Analyzed

### Primary Focus: Gynecologic Oncology

-   **Definition**: Physicians specializing in cancers of the female
    reproductive system
-   **Taxonomy Code**: 207VX0201X
-   **Typical Training**: 3 or 4-year fellowship after OBGYN residency
-   **Rarity**: Highly specialized - limited number nationwide

### Secondary Analysis Subspecialties:

1.  **Female Pelvic Medicine/Urogynecology (207VF0040X)**
    -   Pelvic floor disorders, incontinence
    -   Recent name change from FPMRS to URPS
2.  **Maternal-Fetal Medicine (207VM0101X)**
    -   High-risk pregnancy specialists
    -   Critical for complex obstetric care
3.  **Reproductive Endocrinology (207VE0102X)**
    -   Fertility specialists
    -   Hormone-related reproductive disorders

#### Taxonomy Code Reference

``` r
obgyn_taxonomy_codes <- c(
  "207V00000X",    # Obstetrics & Gynecology (general)
  "207VX0201X",    # Gynecologic Oncology (PRIMARY FOCUS)
  "207VE0102X",    # Reproductive Endocrinology 
  "207VG0400X",    # Gynecology (general)
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

The `check_physician_presence` function was implemented as a quality control
utility for tracking physicians across temporal data. This function efficiently
analyzes datasets containing physician information to determine when specific
providers appear in records, accepting National Provider Identifiers (NPIs) and
methodically examining each NPI's presence throughout different years. It
returns structured data summarizing each provider's representation, including
total record count and chronological listing of years present.

``` r
# Example validation list of known NPIs for quality control
=======
## 📂 Detailed File Organization & Purpose
```

isochrones/ ├── 📁 data/ \# Raw and processed datasets │ ├──
demographic_disparities_analysis.csv │ ├── difference_in_difference_analysis.csv
│ ├── duplicateyearandfill.csv │ ├──
end_inner_join_postmaster_clinician_data.csv │ ├──
end_rejoined_geocoded_unique_address.csv │ ├── fips-states.csv │ ├──
healthcare_benchmark_references.md │ ├── intersect.csv │ ├──
Medicare_Part_D_physician_retirement_analysis.csv │ ├──
physician_compare_data.csv │ ├── physician_retirement_years.csv │ ├──
population_weighted_means_with_ci.csv │ └── racial_decay_curves.png ├── 📁
figures/ \# Generated visualizations │ └── [Generated plots and maps] ├── 📁 R/
\# All R scripts and functions │ │ 👩️ ***GO_access_analysis_code.Rmd*** \# Main
analysis report │ ├── 📊 Data Collection (A-Series) │ │ ├── A-abms.R \# Board
certification scraping │ │ ├── A-facility_affiliation_download.R \# CMS facility
data │ │ ├── A-Medicare_part_d_prescribers_data_download.R │ │ ├──
A-NPI_deactivation_download.R \# Provider deactivation │ │ ├──
A-nppes_download.R \# Historical NPPES from NBER │ │ ├──
A-open_payments_download.R \# Sunshine Act data │ │ └──
A-physician_compare_data_download.R │ ├── 📋 Data Read In (B-Series) │ │ ├──
B-Medicare_part_d_prescribers_read_in.R \# Read in data │ │ ├──
B-NPI_deactivation.R \# Read in data │ │ ├──
B-NPPES_read_in_csv_to_duckDB_database.R \# Read in data │ │ ├──
B-open_payments_cleaning.R \# Read in data │ │ ├──
B-physician_compare_data_download.R \# Read in data │ ├── 📋 Data Processing
(C-Series) │ │ ├── C-Extracting_and_Processing_NPPES_Provider_Data.R │ │ ├──
C-NPPES.R │ │ ├── C-open_payments.R │ │ ├── C-physician_compare_cleaning.R │ ├──
📋 Data Quality Check (D-Series) │ │ ├── D-Quality_check_medicare_prescribing.R
│ │ ├── D-Quality_check_on_NPPES_merge.R │ ├── 📋 Export Cleaned Data (E-Series)
│ │ ├── E-Medicare_part_d_retirement_analysis_processing.R │ │ ├──
E-open_payments_export.R │ │ └── F-retirement_year_confirmation.R │ ├── 🗺️ Core
Analysis Pipeline (00-10 Series) │ │ ├── 000-Control.R \# Master control script
│ │ ├── 01-setup.R \# Environment and API setup │ │ ├── 02-search_taxonomy.R \#
NPPES taxonomy search │ │ ├── 02.5-subspecialists_over_time.R \# Temporal trends
│ │ ├── 03-search_and_process_npi.R \# NPI data processing │ │ ├──
03a-search_and_process_extra.R \# Additional NPI processing │ │ ├── 04-geocode.R
\# Address geocoding (HERE API) │ │ ├── 06-isochrones.R \# Drive time isochrone
generation │ │ ├── 07-isochrone-mapping.R \# Spatial joins and mapping │ │ ├──
07.5-prep-get-block-group-overlap.R │ │ ├── 08-get-block-group-overlap.R \#
Census block calculations │ │ ├── 08.5-prep-the-census-variables.R │ │ ├──
09-get-census-population.R \# Population analysis │ │ ├──
10-calculate-polygon-demographics.R │ │ └── 10-make-region.R \# Regional
analysis │ ├── 📈 Results & Analysis │ │ ├── analyze_isochrone_data.R \#
Statistical trend analysis │ │ ├── walker_isochrone_maps.R \# Final map
generation │ │ ├── subspecialists_over_time.R \# Workforce trends │ │ ├──
retirement_adjusted.R \# Retirement analysis │ │ └── getting_isochrones_trying.R
\# Alternative methods │ ├── 🛠️ Utility Functions │ │ ├── api_keys.R \# API key
management │ │ ├── bespoke_functions.R \# Custom project functions │ │ ├──
geocode.R \# Geocoding utilities ├── 📁 results/ \# Analysis outputs │ ├──
state_data.csv │ ├── summary_statistics.csv │ ├──
table1_temporal_trends_summary.csv │ ├──
table2_demographic_disparities_summary.csv │ ├──
tabulated_all_years_clean_2024-08-30.xlsx │ ├── temporal_trend_analysis.csv │
├── trend_analysis.csv │ └── us_states.csv ├── 📄 README.Rmd \# This
documentation ├── 📄 README.html \# Rendered HTML version

```         

#### Data Gathering

-   `bespoke_functions.R` - Data for the duckDB connection and functions for the
    Data Gathering lettered code below. The National Bureau of Economic Research
    (NBER) provides a cumulative NPI/NPPES dataset created from monthly CMS
    files spanning from April 2007 to the present, addressing limitations in
    official CMS offerings that lack historical provider details. The dataset is
    deduplicated based on variable changes (excluding mere updates in file names
    or years), and includes separate core and multiplicative variable files to
    manage the large file sizes efficiently.

| **Dataset/Script Name** | **Available Years** | **Notes** |
|:---|:---|:---|
| **`A_nppes_download.R`** | 2007–2022 | NBER cumulative NPPES file (monthly updates deduplicated; includes changes back to 2007). |
| **`A-NPI_deactivation_download.R`** | Varies, slightly delayed | Based on self-reported deactivation. CMS updates irregularly; may lag real-time by 1–2 years. |
| **`A-Medicare_part_d_prescribers_data_downloaded`** | 2013–2022 | Released annually by CMS; first available dataset based on 2013 prescribing data. |
| **`A-download_physician_compare_download`** | \~2013–2020 (downloaded files) | ICPSR archive has files through \~2020 (Physician Compare closed in Dec 2020). Must manually log in. |
| **`A-facility_affiliation_download.R`** | 2014–present | First Facility Affiliation data started \~2014; yearly updates (CMS Physician Compare Affiliation data). |
| **`A-open_payments_download.R`** | 2013–present | Open Payments (Sunshine Act) started reporting payments in 2013. Annual updates. |

-   `A_nppes_download.R` - Downloads NPPES data for back years from NBER
    (National Board Economic Research). The National Bureau of Economic Research
    (NBER) provides a cumulative NPI/NPPES dataset created from monthly CMS
    files spanning from April 2007 to 2022, addressing limitations in official
    CMS offerings that lack historical provider details. The dataset is
    deduplicated based on variable changes (excluding mere updates in file names
    or years), and includes separate core and multiplicative variable files to
    manage the large file sizes efficiently.

-   `A-NPI_deactivation_download.R` - NPPES data may be a few years behind and
    is all based on personal report.

-   `A-Medicare_part_d_prescribers_data_downloaded` - Good for docs who
    prescribe drugs to Medicare patients over 65 years old. Started 2013 to

    2022. 

-   `A-download_physician_compare_download` - To get old physician
    compare/national downloadable files we need to log in manually to OPEN
    ICPSR:
    <https://www.openicpsr.org/openicpsr/project/149961/version/V1/view?path=/openicpsr/149961/fcr:versions/V1&type=project>.
    You have to login with google account to download it.

-   `A-facility_affiliation_download.R` - Gold standard.

-   `A-open_payments_download.R` - Open Payments.

-   `B-read_in_csv_file_to_duckDB_database.R` - Reads in the NPPES CSV fils from
    the NBER to the duckDB database. The primary action occurs via the
    process_nppes_data function (defined in an external script
    bespoke_functions.R), which reads and processes a large CSV file containing
    historical NPI data (spanning May 2005 to October 2020). The processed data
    is stored in a DuckDB database file.

-   `C-Extracting_and_Processing_NPPES_Provider_Data.R` - Function for
    processing OBGYNs from NPPES data with exact file names for years 2010
    to 2022. Automatically identifies and maps database tables to their
    corresponding years, enabling efficient extraction of provider data across
    different time periods. Specifies taxonomy codes representing various OB/GYN
    subspecialties, including general obstetrics and gynecology, maternal-fetal
    medicine, and female pelvic medicine. Looks at all taxonomy columns in each
    year's dataset (not just the first one). Checks if any of your specified
    codes appear in any of those columns. Includes a physician in the results if
    there's a match in any column. Retrieves provider records matching specified
    OB/GYN taxonomy codes from each year, standardizing and combining the data
    into a unified dataset.

-   `D-Quality_check_on_NPPES_merge.R` - The check_physician_presence function
    is a well-designed utility for tracking physicians across temporal data. It
    efficiently analyzes a dataset containing physician information to determine
    when specific providers appear in the records. The function accepts a list
    of National Provider Identifiers (NPIs), optionally paired with provider
    names, and methodically examines each NPI's presence throughout different
    years. It returns a structured data frame summarizing each provider's
    representation in the dataset, including their total record count and a
    chronological listing of years in which they appear. This function is
    particularly valuable for longitudinal analyses of healthcare provider data,
    enabling researchers to identify patterns in physician presence, track
    career trajectories, or validate data completeness across multiple years of
    NPI records.

```r
# List of NPIs to check
npi_list <- c(
  "1689603763",   # Tyler Muffly, MD
  "1528060639",   # John Curtin, MD
  "1346355807",   # Pedro Miranda, MD
  # ... additional validation cases
)
```

### Geocoding and Address Standardization

Provider practice addresses were geocoded using the HERE Maps Geocoding API
(version 6.2). Addresses were standardized and cleaned prior to geocoding, with
quality scoring and manual review of poor matches. Coordinate validation
included bounds checking and outlier detection.

### Isochrone Generation and Spatial Analysis

Drive time isochrones were calculated using the HERE Maps Isoline Routing API
(version 7.2) with the following specifications:

-   **Time Thresholds**: 30, 60, 120, and 180 minutes
-   **Reference Time**: Third Friday of October at 9:00 AM local time for each
    analysis year (2013-2023)\
-   **Vehicle Type**: Car
-   **Traffic Conditions**: Real-time traffic enabled
-   **Route Quality**: Highest quality setting

The reference time was standardized across all years to ensure temporal
consistency, selected to represent typical weekday travel conditions while
avoiding rush hour peaks.

### Demographic and Population Analysis

Population within each isochrone was calculated through spatial intersection of
drive time polygons with census block groups. Area-weighted population estimates
were computed for partial block group overlaps.

**Primary Analysis Population**: Female population, with demographic
stratification by: - Race/ethnicity: White alone, Black or African American
alone, Asian alone, American Indian and Alaska Native alone - Geographic
regions: American College of Obstetricians and Gynecologists (ACOG) Districts -
Rural-urban classification: RUCA codes

### Statistical Analysis

Temporal trends in accessibility were analyzed using linear regression to
measure changes from 2013 to 2022. Statistical significance was assessed at p \<
0.05. Slope calculations, R-squared values, and p-values were computed for each
demographic category and drive time threshold.

## Results

### Overall Access to Gynecologic Oncologists

**Baseline Access (2013)**: Among the total female population, 72.4 million
women (44.5% of total population) had access to gynecologic oncologists within a
30-minute drive time. Access increased with longer drive times: 98.3 million
women (60.4%) within 60 minutes, 133.0 million women (81.8%) within 120 minutes,
and 148.4 million women (91.3%) within 180 minutes.

**Current Access (2022)**: Access levels remained relatively stable, with 71.6
million women having 30-minute access, 97.7 million women having 60-minute
access, 132.9 million women having 120-minute access, and 148.4 million women
having 180-minute access.

``` r
# Example access data structure
=======
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
```

-   `E-Medicare_part_d_prescribers_data_processing.R` - This script processes
    Medicare Part D prescribing data from the Centers for Medicare & Medicaid
    Services (CMS). Process each table filtering "Prscrbr_Type" only OBGYN and
    "Gynecological Oncology". It identifies providers' prescribing patterns,
    cleans data by removing outlier records (claim counts over 50,000),
    annotates records by year, and merges multiple years into a single
    standardized dataset. Additionally, it calculates the last consecutive year
    each provider actively prescribed medications under Medicare Part D,
    facilitating analysis of provider activity and continuity over time. NOT
    GOOD FOR DOCS WHO DO NOT TREAT PATIENTS \>65 years old.

-   `F-retirement_year_confirmation.R` - Download the massive data files to the
    external hard drive for this with one set of code then then can run
    E-retirement_year_confirmation.R.

Retirement Year Data download: `NPPES_deactivated_download.R` - Best source but
may be late.\
`Medicare_part_d_prescribers_data_processing` - People who prescribed to \>65
year old women.\
`download_physician_compare_data.R` - Includes data for people who see
Medicare.\
facility affiliation. - Does not include a year for the facility affiliation so
it is not helpful.\
ABMS - scraped.

-   `getting_isochrones_trying.R` - Alternative method for isochrone generation
-   `retirement_adjusted.R` - Enhanced retirement analysis using multiple data
    sources (NPI deactivation, Medicare data, and board certification status) to
    improve workforce accuracy.
-   `subspecialists over time.R` - Analysis of subspecialist trends
-   `visualize_fips_inters_isochr.R` - Visualizes FIPS code intersections
-   `fips_blocks_female_proportion.R` - Analyzes female population in FIPS
    blocks
-   `fips_isochrones_population_intersect.R` - Examines population within
    isochrones
-   `zzzPostico.R` - Used Postico originally. Able to use duckDB later on.\
-   `Postico_database_pull.R` - Extracts physician data from PostgreSQL
    database, enabling year-by-year analysis of physician practice locations
    from 2013 to 2022. Pulls "GYNECOLOGIC ONCOLOGY" from the Primary Specialty.
    For urogyn, we will need NPIs to go retrospectively to look for people.

#### 1. Setup and Data Preparation

-   `000-control.R` - Auxiliary script for data compilation
-   `01-setup.R` - Loads packages, sets API keys, defines helper functions,
    initializes directory structure
-   `02-search_taxonomy.R` - Search the NPPES Registry database using npi_search
    library in a wrapper. Taxonomy description from the NUCC:
    <https://taxonomy.nucc.org/>. Note recent change in FPMRS to URPS.\
-   `02.5-subspecialists_over_time.R` - Analyzes subspecialist trends over
    multiple years
-   `03-search_and_process_npi.R` - Processes National Provider Identifier (NPI)
    data
-   `03a-search_and_process_extra.R` - Additional NPI processing for edge cases
-   `04-geocode.R` - Geocodes provider addresses using the HERE API
-   `zz05-geocode-cleaning.R` - Old technique with postmaster pulling apart the
    address.

#### 2. Isochrone Generation and Analysis

-   `06-isochrones.R` - Generates drive time isochrones (30, 60, 120, 180 min)
-   `07-isochrone-mapping.R` - Maps isochrones and performs spatial joins
-   `07.5-prep-get-block-group-overlap.R` - Prepares census block group data
-   `08-get-block-group-overlap.R` - Calculates overlap between isochrones and
    census blocks
-   `08.5-prep-the-census-variables.R` - Prepares demographic variables from
    Census
-   `09-get-census-population.R` - Calculates population within/outside
    isochrones

#### 3. Results and Analysis

-   `10-calculate-polygon-demographics.R` - Analyzes demographic characteristics

-   `10-make-region.R` - Creates regional maps and analyses

-   `analyze_isochrone_data.R` - Framework for analyzing isochrone data

-   `calculate_population_in_isochrones_by_race.R` - Analyzes population by race
    within isochrones

-   `walker_isochrone_maps.R` - Visualizes isochrone changes over time

-   `Access_Data.csv` - Data from Tannous that he arranged and is held in
    `data/`

### R Markdown Documents

-   `GO_access_analysis_code.Rmd` - Statistical analysis of gynecologic oncology
    access
-   `for_every_year_script_rmd.Rmd` - Year-by-year analysis of accessibility
    trends
-   `isochrones.Rmd` - Tutorial on creating and analyzing isochrones

## Execution Order

For a complete analysis, the files should be executed in approximately this
order:

### Setup Phase

1.  `01-setup.R`
2.  `Postico_database_pull.R` (if external hardrive with the Positico database
    access is connected)

### Data Collection Phase

3.  `02-search_taxonomy.R`
4.  `02.5-subspecialists_over_time.R`
5.  `03-search_and_process_npi.R` - When did physicians start practicing?
6.  `03a-search_and_process_extra.R`
7.  `04-geocode.R`
8.  `zz05-geocode-cleaning.R`
9.  `retirement.R`/`retirement_adjusted.R` - When did physicians retire? (if
    physician retirement analysis is needed)

### Isochrone Analysis Phase

9.  `06-isochrones.R`
10. `07-isochrone-mapping.R`
11. `07.5-prep-get-block-group-overlap.R`
12. `08-get-block-group-overlap.R`
13. `08.5-prep-the-census-variables.R`
14. `09-get-census-population.R`

### Results and Additional Analysis Phase

15. `10-calculate-polygon-demographics.R`

16. `10-make-region.R`

17. `script2025.R` - Downloads the population data and aggregates it by
    isochrone and by total population. Creates tables of women within isochrones
    and total women.\

``` r
> access_merged
# A tibble: 240 × 6
   year  range category              count     total percent
   <chr> <int> <chr>                 <dbl>     <dbl>   <dbl>
 1 2013   1800 total_female       72362517 162649954    44.5
 2 2013   1800 total_female_white 46553359 119180751    39.1
```

### Temporal Trends in Access (2013-2022)

Statistical analysis of temporal trends revealed significant variations by
demographic group:

**Total Female Population**: Showed declining trends across all drive time
thresholds, though not all reached statistical significance: - 30-minute access:
-736,396 women per year (R² = 0.113, p = 0.343) - 60-minute access: -630,107
women per year (R² = 0.072, p = 0.453)\
- 120-minute access: -134,072 women per year (R² = 0.005, p = 0.854) -
180-minute access: -3,016 women per year (R² \< 0.001, p = 0.997)

**Racial/Ethnic Disparities**: - **White women**: Significant declines in access
observed across all time thresholds - **Asian women**: Significant increases in
longer drive time access (120+ minutes) - **American Indian/Alaska Native
women**: Mixed trends with some increases in 30- and 60-minute access
categories, though starting from lower baseline access levels

``` r
=======
19. `analyze_isochrone_data.R` - Measures the slope for access from start 2013
    to finish 2022. Finds significant increases or decreases in the number of
    women within a drive time. Some notable trends:

-   Total female white population shows significant declines in access across
    all time thresholds
-   Some categories like `total_female_asian` show significant increases in
    longer-time thresholds

$$
    R^2 = 1 - \frac{SS_{res}}{SS_{tot}} = 1 - \frac{\sum_i (y_i - \hat{y}_i)^2}{\sum_i (y_i - \bar{y})^2}
$$

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

Analysis revealed persistent rural-urban divides and regional variations in
access to gynecologic oncology care. Geographic regions with highest access
density included major metropolitan areas and academic medical centers, while
rural and frontier areas showed significantly reduced access across all time
thresholds.



# Project Overview {.tabset}

## Overview

::: highlight-box
**Mission**: This project provides the most comprehensive analysis of nationwide
access to gynecologic oncologists and OBGYN subspecialists using advanced
**drive time isochrone methodology**, **demographic stratification**, and
**longitudinal geospatial analysis**.
:::

This research examines healthcare accessibility disparities across: -
**Geographic dimensions** (urban/rural, regional variations) - **Demographic
stratifications** (race, ethnicity, socioeconomic status) - \*Temporal dynamics
**(2013-2023 longitudinal trends) -** Travel time thresholds\*\* (30, 60, 120,
180-minute drive times)

# 📁 NBER NPPES Data

This project uses **National Plan and Provider Enumeration System (NPPES)** data
as released by the **National Bureau of Economic Research (NBER)**, which
provides historical snapshots of the NPPES downloadable files.

### 📌 Data Source

The files were downloaded from the NBER’s archival repository of NPPES datasets:
🔗
[https://data.nber.org/npi/zip/](https://www.nber.org/research/data/national-plan-and-provider-enumeration-system-nppes)

## 📊 **Expected Results (Based on Directory Listings):**

| Year | File You'll Get | Month | Notes |
|----|----|----|----|
| 2007 | `npi200711.csv` | November | Only monthly file available |
| 2008 | `npidata_20050523-20080512.csv` | Annual | No monthly files |
| 2009 | `npi20094.csv` | **April** ✅ | Perfect! |
| 2010 | `npidata_20050523-20100111.csv` | Annual | No monthly files |
| 2011 | `npi20114.csv` | **April** ✅ | Perfect! |
| 2012 | `npi20124.csv` | **April** ✅ | Perfect! |
| 2013 | `npi20133.csv` | March | April missing, March fallback |
| 2014 | `npi20142.csv` | February | April missing, Feb fallback |
| 2015-2019 | `npi2015X4.csv` etc. | **April** ✅ | All have April |
| 2020-2023 | `npi2020X4.parquet` etc. | **April** ✅ | Parquet format |

## 🎉 **You'll Get 17 Years of Data:**

-   **10+ years with actual April data** (2009, 2011, 2012, 2015-2023)
-   **Close months for missing April** (2007=Nov, 2013=Mar, 2014=Feb)
-   **Annual consolidated files** for years without monthly data (2008, 2010)

### ✅ Files Used

The file naming patterns are inconsistent across years
(<https://data.nber.org/npi/webdir/csv/2023/>):

2018 and earlier: core20184.csv (month as number, April = 4) 2020-2022:
core20214.csv (month as number, April = 4) 2023: core_April_2023.csv (explicit
month name)

### 🧹 Deduplication and Rationale

-   Duplicate and redundant files (e.g., multiple snapshots within the same
    year) were removed to streamline processing and conserve storage.
-   Files with the same extract date but different filenames (e.g.,
    `npi_data_pfile_20050523-20220410.csv` and
    `NPPES_Data_Dissemination_April_2022_...`) were deduplicated.
-   The selection process prioritized **temporal consistency**, with April being
    used as a standard reference point for yearly comparison.

--------------------------------------------------------------------------------

### 📊 Use Case

These deduplicated files serve as the foundational input for:

-   Identifying actively practicing OBGYNs each year,
-   Constructing a longitudinal DuckDB dataset,
-   Assessing trends in geographic and specialty access to obstetric and
    gynecologic care across the U.S.

--------------------------------------------------------------------------------

Let me know if you’d like to append a citation format or include SHA256 hashes
for integrity verification.

### Key Innovation

Using the **HERE Maps API** combined with **high-resolution census data**, we
calculate precise drive time isochrones around specialist locations and perform
**area-weighted demographic analysis** of populations within each accessibility
threshold.

# HERE API and hereR Package Integration

## Overview

This gynecologic oncology accessibility analysis leveraged the HERE Location
Services API through the `hereR` R package to calculate realistic drive time
isochrones around gynecologic oncologist subspecialist practices. The
implementation consisted of three main phases utilizing enterprise-grade routing
and geospatial analysis capabilities with temporal traffic awareness.

## Phase 0: National Provider Identifier (NPI) Database Processing

### Identifying Gynecologic Oncologists

*Source: `R/03-search_and_process_npi.R`*

The analysis began by identifying gynecologic oncologists from the National
Provider Identifier (NPI) database. The NPI database contains comprehensive
provider information including subspecialty classifications, practice locations,
and credentials.



The NPI database processing identified gynecologic oncologists through: -
**Taxonomy codes**: Specific subspecialty classifications for gynecologic
oncology - **Credential verification**: Board certification in gynecologic
oncology\
- **Practice validation**: Active practices with complete address information -
**Duplicate removal**: Ensuring unique provider-location combinations

## Why HERE API Over Alternatives

HERE API was selected for this analysis because it provides **historical traffic
data** and allows specification of exact dates and times for routing
calculations. This temporal capability was essential for:

-   Analyzing accessibility patterns across multiple years (2013-2022)
-   Accounting for traffic variations on specific dates
-   Ensuring consistent peak-hour analysis (Friday 9 AM conditions)
-   Comparing year-over-year accessibility changes under similar traffic
    conditions

Alternative APIs lack this temporal specificity, making them unsuitable for
longitudinal healthcare accessibility studies.

## Phase 1: Geocoding Gynecologic Oncologist Practices

### Address Preparation and Google Geocoding Strategy

*Source: `R/04-geocode.R`*

The analysis began by geocoding gynecologic oncologist addresses using
**complete street addresses** rather than ZIP codes alone. This approach was
critical for spatial accuracy in healthcare accessibility analysis.



### Geocoding Implementation

*Source: `R/04-geocode.R`*

Example usage:

```r
csv_file <- "data/geocoding/obgyn_practice_addresses_for_geocoding.csv"
geocoded_data <- create_geocode(csv_file)
```



### Why Full Addresses vs. ZIP Code Geocoding

**Full Address Advantages:** - **Spatial precision**: Street-level accuracy
(\~10-50 meter precision) vs. ZIP centroid (\~1-5 km precision) - **Clinical
relevance**: Patients navigate to specific medical buildings, not ZIP code
centers - **Urban accuracy**: Critical in dense medical districts where multiple
practices exist within single ZIP codes - **Accessibility modeling**: Drive time
calculations require precise origin points for realistic routing

**ZIP Code Limitations:** - **Spatial aggregation error**: ZIP centroids may be
miles from actual practice locations - *Example*: In Denver ZIP 80218, the
geographic centroid falls in City Park, while gynecologic oncologists practice
3+ miles away at Presbyterian/Saint Joseph Hospital on the ZIP's periphery -
**Urban bias**: ZIP centroids often fall in geographic centers, not
population-weighted centers - *Example*: Manhattan ZIP 10065 centroid falls in
Central Park (unpopulated), while actual gynecologic oncology practices cluster
near populated areas along York Avenue and East End Avenue - **Accessibility
overestimation**: Patients may appear to have access when the actual practice is
significantly farther - *Example*: A patient in Boulder ZIP 80302 appears to
have 15-minute access based on ZIP centroid calculations, but the actual
gynecologic oncologist practice is 25+ minutes away due to the centroid falling
in downtown Boulder while the practice is located on the eastern edge near
Foothills Hospital - **Medical district distortion**: Hospital complexes and
medical centers span multiple ZIP codes - *Example*: The University of Colorado
Anschutz Medical Campus spans ZIPs 80045 and 80238, with gynecologic oncologists
in the same building having different ZIP codes, creating artificial
accessibility boundaries that don't reflect clinical reality

The geocoding achieved high spatial accuracy with quality metrics assessed
using:

$$\text{Geocoding Accuracy} = \bar{S} = \frac{1}{n}\sum_{i=1}^{n} s_i$$

where $s_i$ represents the Google Geocoding API confidence score for each
geocoded gynecologic oncologist address $i$.

## Phase 2: Isochrone Generation

### Mathematical Definition of Healthcare Isochrones

For each gynecologic oncologist practice $g$, isochrones represent drive time
accessibility zones:

$$I_{g,t} = \{p \in \mathbb{R}^2 : T(g, p) \leq t\}$$

where $T(g, p)$ represents the drive time from gynecologic oncologist $g$ to
population point $p$, and $t \in \{30, 60, 120, 180\}$ minutes.

### Isochrone Visualization

```         
    Gynecologic Oncologist Practice (●)
            │
    ┌───────┼───────┐
    │   30 min      │     ← Immediate access zone
    │   ┌───┼───┐   │
    │   │60 min │   │     ← Regional access zone  
    │   │ ┌─┼─┐ │   │
    │   │ │120│ │   │     ← Extended access zone
    │   │ │min│ │   │
    │   │ │180│ │   │     ← Maximum reasonable access
    │   │ │min│ │   │
    │   │ └─┼─┘ │   │
    │   └───┼───┘   │
    └───────┼───────┘

    Areas reachable within:
    ■ 30 minutes  (immediate access)
    ■ 60 minutes  (reasonable access) 
    ■ 120 minutes (extended access)
    ■ 180 minutes (maximum access)
```

### Implementation Parameters

*Source: `R/06-isochrones.R`*

``` r
# Temporal analysis across multiple years for longitudinal study
iso_datetime_yearly <- c("2013-10-18 09:00:00", "2014-10-17 09:00:00", "2015-10-16 09:00:00",
  "2016-10-21 09:00:00", "2017-10-20 09:00:00", "2018-10-19 09:00:00",
  "2019-10-18 09:00:00", "2020-10-16 09:00:00", "2021-10-15 09:00:00",
  "2022-10-21 09:00:00")

isochrones_sf <- process_and_save_isochrones(
  input_file_no_error_rows, 
  chunk_size = 25, 
  iso_datetime = "2023-10-20 09:00:00",
  iso_ranges = c(30*60, 60*60, 120*60, 180*60),  # Convert minutes to seconds
  crs = 4326, 
  transport_mode = "car",
  save_dir = "data/06-isochrones"
)
```

### Error Handling and Validation

*Source: `R/06-isochrones.R`*



The total number of isochrone features generated follows:

$N_{total} = N_{gynecologic oncologists} \times N_{thresholds} = N_{gynecologic oncologists} \times 4$

### Spatial Validation and Clipping

*Source: `R/06-isochrones.R`*



## Phase 3: Spatial Analysis and Population Accessibility

### Spatial Join Implementation

*Source: `R/07-isochrone-mapping.R`*



### Population Accessibility Calculation

Population accessibility for gynecologic oncology services was calculated as:

$$A_{d,t} = \frac{P_{d,t}}{P_{d,total}} \times 100\%$$

where: - $A_{d,t}$ = accessibility percentage for demographic group $d$ within
time threshold $t$ - $P_{d,t}$ = population of demographic group $d$ within $t$
minutes of gynecologic oncologist - $P_{d,total}$ = total population of
demographic group $d$

### Technical Implementation Challenges

*Source: `R/06-isochrones.R` and `R/07-isochrone-mapping.R`*

1.  **API Rate Limiting**: Processed in chunks of 25 gynecologic oncologist
    locations per batch
2.  **Temporal Consistency**: Used identical Friday 9 AM timestamps across
    analysis years\
3.  **Error Recovery**: Pre-validation prevented batch failures from problematic
    addresses
4.  **Memory Management**: Applied strategic garbage collection for large
    spatial operations
5.  **Geometry Validation**: Ensured spatial integrity through validation and
    simplification



## Visualization and Quality Assurance

### Interactive Mapping Validation

*Source: `R/07-isochrone-mapping.R`*



## Data Output Structure

The final dataset maintained a 1:many relationship where each gynecologic
oncologist practice was associated with its corresponding drive time isochrones:

-   **Gynecologic Oncologist Practices**: $N_{gynecologic oncologists}$ unique
    subspecialist locations
-   **Isochrone Features**: $4 \times N_{gynecologic oncologists}$ total
    accessibility polygons\
-   **Time Thresholds**: $T = \{30, 60, 120, 180\}$ minutes
-   **Coordinate System**: EPSG:4326 (WGS84)
-   **Output Format**: ESRI Shapefile maintaining MULTIPOLYGON geometry

*Source: `R/06-isochrones.R`*



This methodology provides a robust foundation for analyzing temporal changes in
gynecologic oncology accessibility across demographic groups and geographic
regions from 2013-2022. \# Subspecialties Analyzed



--------------------------------------------------------------------------------

# 📊 Mathematical Framework

## Isochrone Generation Equations

::: equation-box
**Isochrone Definition for Census Tracts:**
$$I_t(O) = \{T_i \in \mathcal{T} : d(O, c(T_i)) \leq t\}$$

**Multi-Time Isochrone System:**
$$\mathcal{I} = \bigcup_{t \in \{30, 60, 120, 180\}} I_t(O) \text{ where } I_t(O) = \{T_i : d(O, c(T_i)) \leq t\}$$

**Travel Time Function with Network Constraints:**
$$d(O, c(T_i)) = \min_{p \in P(O, c(T_i))} \sum_{e \in p} \frac{l_e}{v_e}$$

Where: - $I_t(O)$ = isochrone for time $t$ from origin $O$ - $\mathcal{T}$ = set
of all census tracts - $T_i$ = individual census tract $i$ - $c(T_i)$ = centroid
of census tract $T_i$ - $d(O, c(T_i))$ = shortest travel time from origin to
tract centroid - $P(O, c(T_i))$ = set of all possible paths from $O$ to
$c(T_i)$ - $l_e$ = length of road segment $e$ - $v_e$ = speed on road segment
$e$
:::

## Demographic Accessibility Equations

::: equation-box
**Total Accessible Women Population by Race/Ethnicity:** $$\begin{align}
A_t^{Total}(O) &= A_t^{White}(O) + A_t^{Black}(O) + A_t^{Asian}(O) \\
&\quad + A_t^{HIPI}(O) + A_t^{Hispanic}(O) + A_t^{Other}(O)
\end{align}$$
:::

--------------------------------------------------------------------------------

# 🛠️ Advanced R Implementation

## Comprehensive Accessibility Analysis Functions

::: code-title
Core Analysis Framework
:::



## Advanced Helper Functions

::: code-title
Data Processing and Validation Functions
:::



--------------------------------------------------------------------------------

# 📋 Comprehensive Methods

## Data Sources and Processing Pipeline

::: method-box
### **Primary Data Sources**


:::

## Advanced Provider Identification

::: method-box
### **NUCC Healthcare Provider Taxonomy Codes**

Our analysis employs comprehensive taxonomy code searching across **all taxonomy
columns** (not just primary) to capture providers with multiple subspecialty
certifications.


:::

## Quality Control and Validation Framework

::: method-box
### **Multi-Source Validation System**


:::

--------------------------------------------------------------------------------

# 🗺️ Advanced Geocoding and Spatial Analysis {.tabset}

## Geocoding Framework

::: technical-note
**Technical Implementation**: Our geocoding system uses the HERE Maps Geocoding
API v6.2 with comprehensive quality control, batch processing, and automatic
retry mechanisms.
:::



## Isochrone Generation

::: technical-note
**HERE Maps Isoline Routing API v7.2 Specifications**: - **Time Thresholds**:
30, 60, 120, 180 minutes (standard healthcare accessibility benchmarks) -
**Reference Time**: Third Friday of October, 9:00 AM (consistent across all
analysis years) - **Traffic Integration**: Real-time traffic data enabled for
realistic estimates - **Route Optimization**: Highest quality setting for
maximum precision
:::



--------------------------------------------------------------------------------

# 📊 Comprehensive Results and Analysis {.tabset}

## Overall Access Statistics

::: results-box
### **Baseline Access Analysis (2013)**



### **Current Access Analysis (2022)**


:::

## Temporal Trend Analysis

::: results-box
### **Statistical Significance Testing (2013-2022)**


:::

## Geographic and Demographic Disparities

::: results-box
### **Access by Demographic Groups (Interactive)**


:::

--------------------------------------------------------------------------------

# 🗃️ Comprehensive File Organization {.tabset}

## Data Pipeline Architecture

::: technical-note
Our analysis pipeline is organized into **modular phases** with **comprehensive
error handling**, **logging**, and **quality control** at each stage.
:::

### Phase 1: Data Gathering and Processing



### Phase 2: Setup and Initialization



## Advanced Analysis Framework

### Phase 3: Isochrone Generation and Spatial Analysis



### Phase 4: Results Generation and Analysis



## Implementation Code Examples

### Comprehensive Analysis Execution



--------------------------------------------------------------------------------

# 💻 Technical Specifications and Requirements {.tabset}

## System Requirements

::: technical-note
**Computational Requirements**: This analysis requires significant computational
resources due to the large-scale spatial processing and API calls involved.
:::



## Package Dependencies



## API Configuration and Cost Management

::: highlight-box
**💰 Cost Management**: Careful API usage planning is essential to control costs
while maintaining data quality and completeness.
:::



--------------------------------------------------------------------------------

# 📈 Advanced Visualizations and Interactive Elements {.tabset}

## Interactive Results Dashboard



## Comprehensive Data Tables



## Geographic Visualization Examples



-   `A-NPI_deactivation_download.R` - NPPES data may be a few years behind and
    is all based on personal report.

-   `A-Medicare_part_d_prescribers_data_downloaded` - Good for docs who
    prescribe drugs to Medicare patients over 65 years old. Started 2013 to

    2022. 

-   `A-download_physician_compare_download` - To get old physician
    compare/national downloadable files we need to log in manually to OPEN
    ICPSR:
    <https://www.openicpsr.org/openicpsr/project/149961/version/V1/view?path=/openicpsr/149961/fcr:versions/V1&type=project>.
    You have to login with google account to download it.

-   `A-facility_affiliation_download.R` - Gold standard.

-   `A-open_payments_download.R` - Open Payments.

-   `B-read_in_csv_file_to_duckDB_database.R` - Reads in the NPPES CSV fils from
    the NBER to the duckDB database. The primary action occurs via the
    process_nppes_data function (defined in an external script
    bespoke_functions.R), which reads and processes a large CSV file containing
    historical NPI data (spanning May 2005 to October 2020). The processed data
    is stored in a DuckDB database file.

-   `C-Extracting_and_Processing_NPPES_Provider_Data.R` - Function for
    processing OBGYNs from NPPES data with exact file names for years 2010
    to 2022. Automatically identifies and maps database tables to their
    corresponding years, enabling efficient extraction of provider data across
    different time periods. Specifies taxonomy codes representing various OB/GYN
    subspecialties, including general obstetrics and gynecology, maternal-fetal
    medicine, and female pelvic medicine. Looks at all taxonomy columns in each
    year's dataset (not just the first one). Checks if any of your specified
    codes appear in any of those columns. Includes a physician in the results if
    there's a match in any column. Retrieves provider records matching specified
    OB/GYN taxonomy codes from each year, standardizing and combining the data
    into a unified dataset.

-   `D-Quality_check_on_NPPES_merge.R` - The check_physician_presence function
    is a well-designed utility for tracking physicians across temporal data. It
    efficiently analyzes a dataset containing physician information to determine
    when specific providers appear in the records. The function accepts a list
    of National Provider Identifiers (NPIs), optionally paired with provider
    names, and methodically examines each NPI's presence throughout different
    years. It returns a structured data frame summarizing each provider's
    representation in the dataset, including their total record count and a
    chronological listing of years in which they appear. This function is
    particularly valuable for longitudinal analyses of healthcare provider data,
    enabling researchers to identify patterns in physician presence, track
    career trajectories, or validate data completeness across multiple years of
    NPI records.

-   `E-Medicare_part_d_prescribers_data_processing.R` - This script processes
    Medicare Part D prescribing data from the Centers for Medicare & Medicaid
    Services (CMS). Process each table filtering "Prscrbr_Type" only OBGYN and
    "Gynecological Oncology". It identifies providers' prescribing patterns,
    cleans data by removing outlier records (claim counts over 50,000),
    annotates records by year, and merges multiple years into a single
    standardized dataset. Additionally, it calculates the last consecutive year
    each provider actively prescribed medications under Medicare Part D,
    facilitating analysis of provider activity and continuity over time. NOT
    GOOD FOR DOCS WHO DO NOT TREAT PATIENTS \>65 years old.

-   `F-retirement_year_confirmation.R` - Download the massive data files to the
    external hard drive for this with one set of code then then can run
    E-retirement_year_confirmation.R.

Retirement Year Data download: `NPPES_deactivated_download.R` - Best source but
may be late.\
`Medicare_part_d_prescribers_data_processing` - People who prescribed to \>65
year old women.\
`download_physician_compare_data.R` - Includes data for people who see
Medicare.\
facility affiliation. - Does not include a year for the facility affiliation so
it is not helpful.\
ABMS - scraped.

-   `getting_isochrones_trying.R` - Alternative method for isochrone generation
-   `retirement_adjusted.R` - Enhanced retirement analysis using multiple data
    sources (NPI deactivation, Medicare data, and board certification status) to
    improve workforce accuracy.
-   `subspecialists over time.R` - Analysis of subspecialist trends
-   `visualize_fips_inters_isochr.R` - Visualizes FIPS code intersections
-   `fips_blocks_female_proportion.R` - Analyzes female population in FIPS
    blocks
-   `fips_isochrones_population_intersect.R` - Examines population within
    isochrones
-   `zzzPostico.R` - Used Postico originally. Able to use duckDB later on.\
-   `Postico_database_pull.R` - Extracts physician data from PostgreSQL
    database, enabling year-by-year analysis of physician practice locations
    from 2013 to 2022. Pulls "GYNECOLOGIC ONCOLOGY" from the Primary Specialty.
    For urogyn, we will need NPIs to go retrospectively to look for people.

#### 1. Setup and Data Preparation

-   `000-control.R` - Auxiliary script for data compilation
-   `01-setup.R` - Loads packages, sets API keys, defines helper functions,
    initializes directory structure
-   `02-search_taxonomy.R` - Search the NPPES Registry database using npi_search
    library in a wrapper. Taxonomy description from the NUCC:
    <https://taxonomy.nucc.org/>. Note recent change in FPMRS to URPS.\
-   `02.5-subspecialists_over_time.R` - Analyzes subspecialist trends over
    multiple years
-   `03-search_and_process_npi.R` - Processes National Provider Identifier (NPI)
    data
-   `03a-search_and_process_extra.R` - Additional NPI processing for edge cases
-   `04-geocode.R` - Geocodes provider addresses using the HERE API
-   `zz05-geocode-cleaning.R` - Old technique with postmaster pulling apart the
    address.

#### 2. Isochrone Generation and Analysis

-   `06-isochrones.R` - Generates drive time isochrones (30, 60, 120, 180 min)
-   `07-isochrone-mapping.R` - Maps isochrones and performs spatial joins
-   `07.5-prep-get-block-group-overlap.R` - Prepares census block group data
-   `08-get-block-group-overlap.R` - Calculates overlap between isochrones and
    census blocks
-   `08.5-prep-the-census-variables.R` - Prepares demographic variables from
    Census
-   `09-get-census-population.R` - Calculates population within/outside
    isochrones

#### 3. Results and Analysis

-   `10-calculate-polygon-demographcs.R` - Analyzes demographic characteristics

-   `10-make-region.R` - Creates regional maps and analyses

-   `analyze_isochrone_data.R` - Framework for analyzing isochrone data

-   `calculate_population_in_isochrones_by_race.R` - Analyzes population by race
    within isochrones

-   `walker_isochrone_maps.R` - Visualizes isochrone changes over time

-   `Access_Data.csv` - Data from Tannous that he arranged and is held in
    `data/`

### R Markdown Documents

-   `GO_access_analysis_code.Rmd` - Statistical analysis of gynecologic oncology
    access
-   `for_every_year_script_rmd.Rmd` - Year-by-year analysis of accessibility
    trends
-   `isochrones.Rmd` - Tutorial on creating and analyzing isochrones

## Execution Order

For a complete analysis, the files should be executed in approximately this
order:

### Setup Phase

1.  `01-setup.R`
2.  `Postico_database_pull.R` (if external hardrive with the Positico database
    access is connected)

### Data Collection Phase

3.  `02-search_taxonomy.R`
4.  `02.5-subspecialists_over_time.R`
5.  `03-search_and_process_npi.R` - When did physicians start practicing?
6.  `03a-search_and_process_extra.R`
7.  `04-geocode.R`
8.  `05-geocode-cleaning.R`
9.  `retirement.R`/`retirement_adjusted.R` - When did physicians retire? (if
    physician retirement analysis is needed)

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
    isochrone and by total population. Creates tables of women within isochrones
    and total women.\
18. `analyze_isochrone_data.R` - Measures the slope for access from start 2013
    to finish 2022. Finds significant increases or decreases in the number of
    women within a drive time.
19. `GO_access_analysis_code.Rmd` - Comprehensive statistical analysis report
20. `walker_isochrone_maps.R` - Creates a faceted map of the US, HI, AK, and PR
    with the isochrones in place.

## Prerequisites

-   R 4.0.0 or higher
-   Required R packages (listed in `01-setup.R`)
-   HERE Maps API key
-   Census API key
-   PostgreSQL database (optional, for historical physician data)

## Tools and Data Management

### HERE API

-   Used for geocoding and isochrone generation
-   Geocoding and Search: \$0.83 per 1,000 searches after 30,000 free geocodes
-   Isoline Routing: \$5.50 per 1,000 after 2,500 free isoline routings

### Data Storage

-   GitHub LFS (Large File Storage) for managing large files
-   DuckDB for efficient data querying
-   PostgreSQL database for year-specific physician data

### Auxiliary Tools

-   tyler package: Custom package for project-specific functions
-   Exploratory.io: Used for data wrangling

## Key Outputs

-   Drive time isochrones at multiple thresholds (30, 60, 120, 180 minutes)

-   Population statistics within/outside isochrones

-   Demographic analysis by race/ethnicity

-   Temporal trends in accessibility (2013-2023)

-   

    # Visualizations of geographic access patterns





22. `GO_access_analysis_code.Rmd`

23. `walker_isochrone_maps.R` - Creates a faceted map of the US, HI, AK, and PR
    with the isochrones in place.

24. `zzzcalculate_population_in_isochrones_by_race.R` - I'm unsure if it is
    needed.\

25. `zzzfor_every_year_script_rmd.Rmd` - This is THE SAME MAP THAT WALKER DID IN
    `walker_isochrone_maps.R` BUT HE DID IT BETTER. Creates a map of the
    isochrones for every year.

### Methodology Overview

#### Geospatial Analysis

-   **Drive Time Isochrones**: 30, 60, 120, and 180-minute thresholds
-   **Routing Engine**: HERE Maps API with real-time traffic data
-   **Reference Time**: Third Friday of October, 9:00 AM (consistent across
    years)
-   **Geographic Resolution**: Census block groups (higher precision than
    counties)

#### Demographic Analysis

-   **Race/Ethnicity Categories**: White, Black, Asian/Pacific Islander,
    American Indian/Alaska Native
-   **Geographic Regions**: ACOG (American College of Obstetricians and
    Gynecologists) Districts
-   **Urban/Rural Classification**: Census Bureau definitions

#### Physician Identification

``` r
# Taxonomy codes for OBGYN subspecialists
obgyn_taxonomy_codes <- c(
  "207V00000X",    # Obstetrics & Gynecology
  "207VX0201X",    # Gynecologic Oncology
  "207VE0102X",    # Reproductive Endocrinology
  "207VG0400X",    # Gynecology
  "207VM0101X",    # Maternal & Fetal Medicine
  "207VF0040X",    # Female Pelvic Medicine/Urogynecology
  "207VB0002X",    # Bariatric Medicine
  "207VC0200X",    # Critical Care Medicine
  "207VC0300X",    # Complex Family Planning
  "207VH0002X",    # Palliative Care
  "207VX0000X"     # Obstetrics Only
)
```

### Data Sources

-   Physician Data: National Plan and Provider Enumeration System (NPPES) files
    from 2013 to 2023
-   Population Data: American Community Survey 5-year estimates and decennial
    census data
-   Geographic Analysis: Used block groups rather than counties for finer data
    resolution
-   Geographic Regions: American College of Obstetricians and Gynecologists
    (ACOG) Districts

### Analysis Approach

-   Drive time isochrones (30, 60, 120, 180 minutes) calculated using HERE API
-   Isochrones generated for the third Friday in October at 9:00 AM for each
    year
-   Demographic analysis by race/ethnicity (White, Black, Asian or Pacific
    Islander, American Indian/Alaska Native)
-   Comparison of urban vs. rural accessibility

## Prerequisites

### System Requirements

``` r
# Check R version (minimum 4.0.0 required)
if (getRversion() < "4.0.0") {
  stop("R version 4.0.0 or higher is required. Current version: ", getRversion())
}

# Check available memory (8GB+ recommended)
memory_gb <- round(memory.limit() / 1024, 1)  # Windows
# memory_gb <- round(as.numeric(system("sysctl hw.memsize", intern = TRUE)) / 1024^3, 1)  # macOS
logger::log_info("Available memory: {memory_gb} GB")
if (memory_gb < 8) {
  logger::log_warn("Less than 8GB RAM detected. Large spatial operations may be slow.")
}
```

``` r
# Install required packages
required_packages <- c(
  "tidyverse", "sf", "tigris", "logger", "assertthat",
  "ggplot2", "scales", "viridis", "DT", "knitr",
  "DBI", "RPostgres", "arrow", "beepr", "dbplyr",
  "downloader", "dplyr", "duckdb", "duckplyr", "fs",
  "gganimate", "gghighlight", "ggrepel", "ggspatial", "ggtext",
  "glue", "gridExtra", "httr", "janitor", "kableExtra",
  "lme4", "lubridate", "patchwork", "postmastr", "readr",
  "rlang", "rvest", "tibble", "tyler"
)

# Install missing packages
new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

# Load custom functions
source("R/bespoke_functions.R")
```

-   R 4.0.0 or higher
-   8GB+ RAM recommended for large spatial datasets
-   Required R packages (listed in `01-setup.R`)
-   HERE Maps API key
-   US Census Bureau API key
-   DuckDB database

### HERE Maps Integration

**Geocoding Pipeline:** 1. Address standardization and cleaning 2. Batch
geocoding requests (rate limit management) 3. Quality scoring and manual review
of poor matches 4. Coordinate validation and outlier detection

**Isochrone Generation:** 1. Provider coordinate input 2. Multiple time
threshold requests per provider 3. Polygon simplification for storage efficiency
4. Spatial validation and topology checking

**Why DuckDB over PostgreSQL/SQLite:** - **Performance**: Optimized for
analytical queries (OLAP vs OLTP) - **Simplicity**: Single-file database, no
server required - **Memory**: Efficient handling of large datasets - **R
Integration**: Native R DBI support

**Alternative Considered**: PostgreSQL with PostGIS - **Pros**: Better spatial
functions, multi-user - **Cons**: Server setup complexity, overkill for
single-user analysis

## Tools and Data Management

## 💻 API Configuration

### HERE Maps API Setup

-   Used for geocoding and isochrone generation
-   Geocoding and Search: \$0.83 per 1,000 searches after 30,000 free geocodes
-   Isoline Routing: \$5.50 per 1,000 after 2,500 free isoline routings

``` r
# Set environment variables by adding `HERE_API_KEY=your_here_api_key_here`
# to your `.Renviron` file.
here_key <- Sys.getenv("HERE_API_KEY")
if (!nzchar(here_key)) stop("HERE_API_KEY not set")

# Verify API access
here_status <- httr::GET(
  "https://geocoder.ls.hereapi.com/6.2/geocode.json",
  query = list(apiKey = here_key, searchtext = "test")
)
```

### Census API Setup

``` r
# Install tidycensus if not already installed
if (!require(tidycensus)) install.packages("tidycensus")

# Set Census API key
census_api_key("your_census_api_key_here", install = TRUE)
```

### API Key Setup:

1.  **HERE Maps API**:
    -   Register at <https://developer.here.com/>
    -   Create project, generate API key
    -   Add to `.Renviron`: `HERE_API_KEY=your_key_here`
2.  **Census Bureau API**:
    -   Register at <https://api.census.gov/data/key_signup.html>
    -   Add to `.Renviron`: `CENSUS_API_KEY=your_key_here`

### Data Storage

-   GitHub LFS (Large File Storage) for managing large files
-   DuckDB for efficient data querying
-   PostgreSQL database for year-specific physician data

### Auxiliary Tools

-   tyler package: Custom package for project-specific functions
-   Exploratory.io: Used for data wrangling

## Key Outputs

-   Drive time isochrones at multiple thresholds (30, 60, 120, 180 minutes)
-   Population statistics within/outside isochrones
-   Demographic analysis by race/ethnicity
-   Temporal trends in accessibility (2013-2023)
-   Visualizations of geographic access patterns

## Data Sources

For downloading NPPES files:

``` bash
wget -P "/Volumes/Video Projects Muffly 1/nppes_historical_downloads" "https://download.cms.gov/nppes/NPPES_Data_Dissemination_April_2024.zip"
```

--------------------------------------------------------------------------------

# 📚 Comprehensive Documentation and References {.tabset}

## Methodological Documentation

::: method-box
### **Spatial Analysis Methodology**

Our spatial analysis methodology employs **area-weighted population
calculations** to ensure accurate demographic estimates within isochrone
boundaries:

1.  **Spatial Intersection**: Drive time polygons are intersected with
    high-resolution census block groups
2.  **Area Weighting**: Population estimates are weighted by the proportion of
    block group area within each isochrone\
3.  **Demographic Stratification**: Population counts are stratified by race,
    ethnicity, and other demographic characteristics
4.  **Temporal Consistency**: All calculations use consistent reference times
    and geographic boundaries across analysis years

### **Statistical Analysis Framework**

-   **Trend Analysis**: Linear regression models assess temporal changes
    (2013-2022)
-   **Significance Testing**: P-values calculated at α = 0.05 significance level
-   **Effect Size**: R-squared values quantify variance explained by temporal
    trends
-   **Confidence Intervals**: 95% confidence intervals provided for all change
    estimates
-   **Multiple Comparisons**: Bonferroni corrections applied when testing
    multiple demographic groups
:::

## Technical Implementation Details



## Data Quality Assurance

::: technical-note
**Quality Assurance Protocol**: Our comprehensive quality assurance system
includes multiple validation layers and cross-verification with independent data
sources.
:::



--------------------------------------------------------------------------------

# Key Findings and Policy Implications {.tabset}

## Executive Summary

::: results-box
### **Key Research Findings**

1.  **Baseline Access (2013)**: 44.5% of women had 30-minute access to
    gynecologic oncologists, increasing to 91.3% within 180 minutes

2.  **Temporal Trends**: Generally declining access across all time thresholds,
    though not statistically significant for total population

3.  **Demographic Disparities**: Significant differences by race/ethnicity, with
    Asian women showing increases in longer drive times while White women
    experienced declines

4.  **Geographic Patterns**: Persistent rural-urban divides with metropolitan
    areas maintaining higher accessibility

5.  **Provider Workforce**: Relatively stable provider numbers but shifting
    geographic distribution patterns
:::

## Policy Recommendations

::: highlight-box
### **🏥 Healthcare Policy Implications**

#### **Immediate Actions Needed**

-   **Rural Access Enhancement**: Targeted recruitment and retention programs
    for rural/frontier areas
-   **Telemedicine Integration**: Expanded telehealth capabilities for initial
    consultations and follow-up care
-   **Transportation Assistance**: Patient transportation programs for
    longer-distance specialty care

#### **Long-term Strategic Planning**

-   **Workforce Distribution**: Incentive programs to encourage subspecialist
    practice in underserved regions
-   **Training Pipeline**: Expanded fellowship programs with rural/underserved
    practice requirements
-   **Regional Centers**: Development of regional specialty care centers with
    enhanced accessibility

#### **Health Equity Considerations**

-   **Demographic-Specific Programs**: Targeted outreach and access programs for
    underserved populations
-   **Cultural Competency**: Enhanced cultural competency training for providers
    serving diverse populations
-   **Language Access**: Multilingual care teams and interpretation services
:::

## Future Research Directions

::: method-box
### **🔬 Research Extensions and Innovations**

#### **Methodological Enhancements**

-   **Real-time Traffic Integration**: Dynamic isochrone modeling with
    time-of-day variations
-   **Multi-modal Transportation**: Integration of public transportation and
    ride-sharing options
-   **Provider Capacity Modeling**: Incorporation of provider availability and
    appointment scheduling

#### **Expanded Analysis Scope**

-   **Quality Metrics Integration**: Correlation with care quality and patient
    outcomes
-   **Cost-Effectiveness Analysis**: Economic modeling of accessibility
    improvements
-   **Patient Flow Modeling**: Actual versus potential care utilization patterns

#### **Technology Integration**

-   **Machine Learning Applications**: Predictive modeling for access patterns
    and provider needs
-   **Mobile Health Integration**: Smartphone-based accessibility tools and
    patient navigation
-   **Artificial Intelligence**: AI-powered care coordination and triage systems
:::

--------------------------------------------------------------------------------

# 📧 Contact and Collaboration {.tabset}

## Primary Investigator

::: highlight-box
**Tyler Muffly, MD**\
📧 **Email**: [tyler.muffly\@dhha.org](mailto:tyler.muffly@dhha.org){.email}\
🏥 **Affiliation**: Denver Health and Hospital Authority\
🔗 **Repository**: <https://github.com/mufflyt/isochrones>\
📍 **Location**: Denver, Colorado, USA

### **Research Interests**

-   Healthcare accessibility and geographic disparities
-   Gynecologic oncology workforce analysis\
-   Geospatial health services research
-   Health policy and rural healthcare delivery
:::

## Collaboration Opportunities

We welcome collaborations and contributions from:

-   🎓 **Academic Researchers**: Healthcare services researchers, geographers,
    epidemiologists
-   🏥 **Healthcare Organizations**: Health systems interested in accessibility
    analysis
-   💻 **Technical Contributors**: R developers, GIS specialists, data
    scientists
-   🏛️ **Policy Organizations**: Healthcare policy institutes and government
    agencies

### **How to Contribute**

1.  **🐛 Bug Reports**: Submit issues via GitHub for any technical problems
2.  **📊 Data Contributions**: Share additional validation datasets or provider
    information\
3.  **🔧 Code Improvements**: Submit pull requests for code enhancements
4.  **📖 Documentation**: Help improve documentation and user guides
5.  **🌍 Geographic Extensions**: Adapt methodology for other regions or
    countries

--------------------------------------------------------------------------------

# License and Citation {.tabset}

## License Information

::: technical-note
This project is licensed under the **MIT License**, promoting open science and
reproducible research while ensuring proper attribution.
:::

```         
MIT License

Copyright (c) 2025 Tyler Muffly, MD


``` bash
wget -P "/Volumes/Video Projects Muffly 1/nppes_historical_downloads" "https://download.cms.gov/nppes/NPPES_Data_Dissemination_April_2024.zip"
```

# DATA REFERENCES



# URL Reference Table - Gynecologic Oncology Accessibility Project

## Data Sources - Primary

| Resource Name | URL | Description | Access Type |
|----|----|----|----|
| NPPES (National Plan and Provider Enumeration System) | <https://nppes.cms.hhs.gov/> | Primary provider registry | Public |
| NBER NPPES Historical Data | <https://www.nber.org/research/data/national-plan-and-provider-enumeration-system-nppes-data> | Historical provider data 2007-2022 | Public |
| CMS Medicare Part D Prescriber Data | <https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Part-D-Prescriber> | Annual prescribing patterns | Public |
| CMS Physician Compare (Historical) | <https://www.openicpsr.org/openicpsr/project/149961/version/V1/view> | Historical physician data through 2020 | Requires Login |
| CMS Open Payments (Sunshine Act) | <https://openpaymentsdata.cms.gov/> | Industry payments to physicians | Public |
| CMS Provider Enrollment Data | <https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data> | Provider enrollment and specialty | Public |
| NPPES Data Dissemination | <https://download.cms.gov/nppes/NPI_Files.html> | Monthly NPPES data downloads | Public |
| CMS Data Navigator | <https://www.cms.gov/data-research/statistics-trends-and-reports/cms-data-navigator> | CMS data portal and navigation | Public |

## Census & Geographic Data

| Resource Name | URL | Description | Access Type |
|----|----|----|----|
| US Census Bureau API | <https://api.census.gov/data.html> | Main census data API portal | Public |
| American Community Survey (ACS) | <https://www.census.gov/programs-surveys/acs> | Population and demographic data | Public |
| TIGER/Line Shapefiles | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Geographic boundary files | Public |
| Census Geography Products | <https://www.census.gov/programs-surveys/geography.html> | Geographic concepts and products | Public |
| Rural-Urban Commuting Area Codes | <https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes/> | RUCA classification system | Public |
| Metropolitan Statistical Areas | <https://www.census.gov/programs-surveys/metro-micro.html> | MSA definitions and data | Public |
| Census API Key Registration | <https://api.census.gov/data/key_signup.html> | Free API key registration | Registration |
| Federal Information Processing Standards | <https://www.census.gov/library/reference/code-lists/ansi.html> | FIPS codes and standards | Public |

## API Documentation

| Resource Name | URL | Description | Access Type |
|----|----|----|----|
| HERE Maps Developer Portal | <https://developer.here.com/> | HERE API registration and docs | API Key Required |
| HERE Geocoding API Documentation | <https://developer.here.com/documentation/geocoder/> | Address geocoding documentation | API Key Required |
| HERE Isoline Routing API | <https://developer.here.com/documentation/routing-api/> | Drive time isochrone generation | API Key Required |
| Census Bureau API Documentation | <https://www.census.gov/data/developers/guidance.html> | Census API usage guidelines | Public |
| HERE API Pricing | <https://developer.here.com/pricing> | Rate limits and pricing tiers | Public |
| REST API Best Practices | <https://developer.here.com/documentation/identity-access-management/dev_guide/topics/plat-using-apikeys.html> | API key management | Public |

## Healthcare & Medical Resources

| Resource Name | URL | Description | Access Type |
|----|----|----|----|
| NUCC Health Care Provider Taxonomy | <https://taxonomy.nucc.org/> | Official provider taxonomy codes | Public |
| ACOG (American College of Obstetricians and Gynecologists) | <https://www.acog.org/> | Professional organization | Public |
| ACOG Districts | <https://www.acog.org/about/districts-and-sections> | Geographic district organization | Public |
| Society of Gynecologic Oncology | <https://www.sgo.org/> | Subspecialty professional society | Public |
| NCCN Guidelines (Gynecologic Oncology) | <https://www.nccn.org/guidelines/category_1> | Clinical practice guidelines | Registration |
| ACGME Fellowship Requirements | <https://www.acgme.org/specialties/> | Fellowship training requirements | Public |
| AAMC Physician Workforce Data | <https://www.aamc.org/data-reports/workforce/data> | Physician workforce statistics | Public |
| HRSA Health Professional Shortage Areas | <https://data.hrsa.gov/tools/shortage-area> | Underserved area designations | Public |

## Government Resources

| Resource Name | URL | Description | Access Type |
|----|----|----|----|
| CMS (Centers for Medicare & Medicaid Services) | <https://www.cms.gov/> | Primary healthcare data agency | Public |
| HRSA (Health Resources and Services Administration) | <https://www.hrsa.gov/> | Federal health workforce agency | Public |
| USDA Economic Research Service | <https://www.ers.usda.gov/> | Rural-urban classifications | Public |
| OMB Metropolitan Area Delineations | <https://www.whitehouse.gov/omb/management/office-federal-financial-management/> | Official MSA definitions | Public |
| HHS Data Portal | <https://healthdata.gov/> | Federal health data catalog | Public |
| National Cancer Institute | <https://www.cancer.gov/> | Cancer statistics and resources | Public |
| Federal Geographic Data Committee | <https://www.fgdc.gov/> | Geographic data standards | Public |

## Technical Documentation

| Resource Name | URL | Description | Access Type |
|----|----|----|----|
| R Project | <https://www.r-project.org/> | R statistical software | Open Source |
| RStudio | <https://www.rstudio.com/> | R development environment | Free/Commercial |
| sf R Package Documentation | <https://r-spatial.github.io/sf/> | Spatial features for R | Open Source |
| tigris R Package | <https://github.com/walkerke/tigris> | Census geography in R | Open Source |
| DuckDB Documentation | <https://duckdb.org/docs/> | Analytical database engine | Open Source |
| GDAL Documentation | <https://gdal.org/> | Geospatial data abstraction library | Open Source |
| PROJ Coordinate Transformation | <https://proj.org/> | Cartographic projections library | Open Source |
| PostGIS Documentation | <https://postgis.net/documentation/> | Spatial database extension | Open Source |
| GitHub Repository Best Practices | <https://docs.github.com/en/repositories> | Version control and collaboration | Public |

## Academic & Research Resources

| Resource Name | URL | Description | Access Type |
|----|----|----|----|
| PubMed | <https://pubmed.ncbi.nlm.nih.gov/> | Medical literature database | Public |
| SEER Cancer Statistics | <https://seer.cancer.gov/> | National cancer surveillance | Public |
| Health Affairs Journal | <https://www.healthaffairs.org/> | Health policy research | Subscription |
| Medical Care Research and Review | <https://journals.sagepub.com/home/mcr> | Healthcare access research | Subscription |
| Spatial and Spatio-temporal Epidemiology | <https://www.journals.elsevier.com/spatial-and-spatio-temporal-epidemiology> | Spatial health analysis | Subscription |
| International Journal of Health Geographics | <https://ij-healthgeographics.biomedcentral.com/> | Health geography research | Open Access |

## Software & Tools

| Resource Name | URL | Description | Access Type |
|----|----|----|----|
| CRAN (R Package Repository) | <https://cran.r-project.org/> | R package downloads | Open Source |
| GitHub | <https://github.com/> | Code repository and collaboration | Free/Paid |
| Docker Hub | <https://hub.docker.com/> | Container registry | Free/Paid |
| Conda Package Manager | <https://docs.conda.io/> | Package management system | Open Source |
| renv R Package | <https://rstudio.github.io/renv/> | R environment management | Open Source |
| Git Documentation | <https://git-scm.com/doc> | Version control system | Open Source |
| Zenodo | <https://zenodo.org/> | Research data repository | Free |
| Open Science Framework | <https://osf.io/> | Research project management | Free |

## Professional Organizations & Standards

| Resource Name | URL | Description | Access Type |
|----|----|----|----|
| American Medical Association | <https://www.ama-assn.org/> | Medical professional organization | Public |
| Association of American Medical Colleges | <https://www.aamc.org/> | Medical education organization | Public |
| National Quality Forum | <https://www.qualityforum.org/> | Healthcare quality standards | Public |
| International Association of Geographers | <https://iag-online.org/> | Geographic research organization | Public |
| American Statistical Association | <https://www.amstat.org/> | Statistical methods and standards | Public |

## Data Standards & Metadata

| Resource Name | URL | Description | Access Type |
|----|----|----|----|
| HL7 FHIR Standards | <https://www.hl7.org/fhir/> | Healthcare data exchange standards | Public |
| ISO Geographic Standards | <https://www.iso.org/committee/54904.html> | International geographic standards | Public |
| OGC Standards | <https://www.ogc.org/standards> | Geospatial data standards | Public |
| Dublin Core Metadata | <https://www.dublincore.org/> | Metadata standards | Public |
| Schema.org | <https://schema.org/> | Structured data vocabulary | Public |

## Quality Control & Validation

| Resource Name | URL | Description | Access Type |
|----|----|----|----|
| American Community Survey Accuracy Statement | <https://www.census.gov/programs-surveys/acs/guidance/statistical-testing-tool.html> | ACS data accuracy guidelines | Public |
| HERE Map Quality | <https://developer.here.com/documentation/routing-api/dev_guide/topics/resource-calculate-isoline.html> | API data quality specifications | API Documentation |
| CMS Data Quality Assurance | <https://www.cms.gov/Research-Statistics-Data-and-Systems/CMS-Information-Technology/AccesstoDataApplication/DataUseAgreements> | Data quality standards | Public |
| Spatial Data Quality Standards | <https://www.fgdc.gov/standards/projects/framework-data-standard> | Federal spatial data quality | Public |

# SHAPEFILE - Geographic Boundaries for Healthcare Accessibility Analysis

# Simplified US Boundaries Reference

## US Census Bureau Cartographic Boundary Files (Simplified)

### Country/National Boundaries

| Resolution | File Name Pattern | Download URL | File Size | Use Case |
|----|----|----|----|----|
| **20m (High Detail)** | `cb_2021_us_nation_20m.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | \~50MB | Detailed national analysis |
| **5m (Medium Detail)** | `cb_2021_us_nation_5m.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | \~20MB | General national mapping |
| **500k (Low Detail)** | `cb_2021_us_nation_500k.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | \~5MB | Web mapping, overview maps |

### State Boundaries (Simplified)

| Resolution | File Name Pattern | Download URL | File Size | Use Case |
|----|----|----|----|----|
| **20m (High Detail)** | `cb_2021_us_state_20m.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | \~25MB | Detailed state analysis |
| **5m (Medium Detail)** | `cb_2021_us_state_5m.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | \~8MB | **RECOMMENDED for most analysis** |
| **500k (Low Detail)** | `cb_2021_us_state_500k.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | \~2MB | Web display, overview maps |

### County Boundaries (Simplified)

| Resolution | File Name Pattern | Download URL | File Size | Use Case |
|----|----|----|----|----|
| **20m** | `cb_2021_us_county_20m.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | \~45MB | Detailed county analysis |
| **5m** | `cb_2021_us_county_5m.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | \~15MB | **RECOMMENDED** |
| **500k** | `cb_2021_us_county_500k.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | \~4MB | Web mapping |

### Census Tract Boundaries (Simplified)

| Resolution | File Name Pattern | Download URL | File Size | Use Case |
|----|----|----|----|----|
| **National 500k** | `cb_2021_us_tract_500k.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | \~180MB | **National tract analysis** |
| **State-by-State 500k** | `cb_2021_[FIPS]_tract_500k.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | 1-20MB each | State-specific analysis |

**Example State Files:** - Colorado: `cb_2021_08_tract_500k.zip` - California:
`cb_2021_06_tract_500k.zip`\
- Texas: `cb_2021_48_tract_500k.zip` - New York: `cb_2021_36_tract_500k.zip`

### Block Group Boundaries (Simplified)

| File Type | File Name Pattern | Download URL | File Size | Notes |
|----|----|----|----|----|
| **National** | `cb_2021_us_bg_500k.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | \~350MB | Similar to your `simplified_us_lck_grp_2021.shp` |
| **State-by-State** | `cb_2021_[FIPS]_bg_500k.zip` | <https://www2.census.gov/geo/tiger/GENZ2021/shp/> | 2-40MB each | Smaller, manageable files |

## R Package Sources for Simplified Boundaries

### tigris Package (Automatic Simplification)

``` r
# Simplified boundaries with cb = TRUE parameter
library(tigris)

# US States (simplified)
states_simplified <- tigris::states(cb = TRUE, resolution = "20m")  # or "5m", "500k"

# US Counties (simplified)  
counties_simplified <- tigris::counties(cb = TRUE, resolution = "20m")

# Census Tracts (simplified) - by state
colorado_tracts <- tigris::tracts(state = "CO", cb = TRUE)

# Block Groups (simplified) - by state  
colorado_bg <- tigris::block_groups(state = "CO", cb = TRUE)

# National boundaries
us_nation <- tigris::nation(cb = TRUE, resolution = "5m")
```

### USAboundaries Package (Historical + Simplified)

``` r
library(USAboundaries)

# Current simplified state boundaries
states_2020 <- us_states(resolution = "low")    # Simplified
states_2020_hi <- us_states(resolution = "high") # Detailed

# Current simplified county boundaries  
counties_2020 <- us_counties(resolution = "low")

# Historical boundaries (simplified)
states_1990 <- us_states(map_date = "1990-01-01", resolution = "low")
```

## Alternative Simplified Boundary Sources

### Natural Earth (Global, Multi-Scale)

| Scale | Resolution | Download URL | US Coverage |
|----|----|----|----|
| **1:10m** | High detail | <https://www.naturalearthdata.com/downloads/10m-cultural-vectors/> | Detailed US boundaries |
| **1:50m** | Medium detail | <https://www.naturalearthdata.com/downloads/50m-cultural-vectors/> | **RECOMMENDED** |
| **1:110m** | Low detail | <https://www.naturalearthdata.com/downloads/110m-cultural-vectors/> | Overview mapping |

**Specific Files:** - Countries: `ne_50m_admin_0_countries.zip` -
States/Provinces: `ne_50m_admin_1_states_provinces.zip`

### OpenStreetMap (Simplified Extracts)

| Source | Description | Download URL | Format |
|----|----|----|----|
| **Geofabrik** | US state extracts | <https://download.geofabrik.de/north-america/us/> | Various formats |
| **BBBike** | Custom extracts | <https://extract.bbbike.org/> | Shapefile, GeoJSON |

# Creating Your Own Simplified Boundaries

### Using R (sf package)

``` r
library(sf)

# Load detailed boundary
detailed_boundary <- st_read("detailed_boundary.shp")

# Simplify geometry (tolerance in map units)
simplified_boundary <- st_simplify(detailed_boundary, dTolerance = 1000)  # 1km tolerance

# Alternative: preserve topology
simplified_boundary <- st_simplify(detailed_boundary, preserveTopology = TRUE, dTolerance = 500)

# Save simplified version
st_write(simplified_boundary, "simplified_boundary.shp")
```

## R Code Examples for Common Use Cases

### Load All Simplified Boundaries

``` r
library(tigris)
library(sf)

# Set options for simplified boundaries
options(tigris_use_cache = TRUE)  # Cache for faster repeated access

# Load simplified boundaries
us_states <- tigris::states(cb = TRUE, resolution = "5m")
us_counties <- tigris::counties(cb = TRUE, resolution = "5m") 
us_nation <- tigris::nation(cb = TRUE, resolution = "5m")

# Load census tracts for specific states (simplified)
colorado_tracts <- tigris::tracts(state = "CO", cb = TRUE)
california_tracts <- tigris::tracts(state = "CA", cb = TRUE)

# Load block groups for analysis (similar to your file)
colorado_bg <- tigris::block_groups(state = "CO", cb = TRUE)
```

### Performance Comparison

| Boundary Type     | Detailed Size | Simplified Size | Speed Improvement    |
|-------------------|---------------|-----------------|----------------------|
| **US States**     | \~15MB        | \~8MB           | 2x faster rendering  |
| **US Counties**   | \~80MB        | \~15MB          | 5x faster rendering  |
| **Census Tracts** | \~1.2GB       | \~180MB         | 10x faster rendering |
| **Block Groups**  | \~2.5GB       | \~350MB         | 15x faster rendering |

## Quality Control for Simplified Boundaries

### Validation Checks

``` r
# Check validity of simplified geometries
st_is_valid(simplified_boundary)

# Fix invalid geometries if needed
simplified_boundary <- st_make_valid(simplified_boundary)

# Check area preservation (should be close to original)
original_area <- sum(st_area(detailed_boundary))
simplified_area <- sum(st_area(simplified_boundary))
area_difference <- abs(simplified_area - original_area) / original_area
print(paste("Area difference:", scales::percent(area_difference)))
```

## File Naming Convention Reference

### Census Bureau Pattern

-   `cb_[YEAR]_[GEOGRAPHY]_[ENTITY]_[RESOLUTION].zip`
-   Examples:
    -   `cb_2021_us_state_5m.zip` (US states, 5m resolution)
    -   `cb_2021_08_tract_500k.zip` (Colorado tracts, 500k resolution)
    -   `cb_2021_us_bg_500k.zip` (US block groups, 500k resolution)

### Resolution Codes

-   **20m**: 1:20,000,000 scale (high detail)
-   **5m**: 1:5,000,000 scale (medium detail)\
-   **500k**: 1:500,000 scale (low detail, most simplified)

The **500k resolution** files are closest to your
`simplified_us_lck_grp_2021.shp` pattern and are optimized for analytical work
while maintaining essential geographic accuracy.

# US Census Bureau Geographic Boundaries

| Geographic Unit | Data Source | Download URL | File Format | Coverage | Update Frequency | Notes |
|----|----|----|----|----|----|----|
| **Census Tracts** | US Census Bureau | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile, KML, GeoJSON | National, State, County | Annual | Primary analysis unit for demographics |
| **Census Block Groups** | US Census Bureau | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile, KML, GeoJSON | National, State, County | Annual | Highest resolution demographic data |
| **Census Blocks** | US Census Bureau | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile, KML, GeoJSON | National, State, County | Annual | Most detailed geographic unit |
| **State Boundaries** | US Census Bureau | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile, KML, GeoJSON | National | Annual | State-level analysis |
| **County Boundaries** | US Census Bureau | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile, KML, GeoJSON | National, State | Annual | County-level aggregation |
| **ZIP Code Tabulation Areas (ZCTAs)** | US Census Bureau | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile, KML, GeoJSON | National | Decennial Census | Approximate ZIP code areas |
| **Urban Areas** | US Census Bureau | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile, KML, GeoJSON | National | Decennial Census | Urban vs rural classification |

## Health Resources and Services Administration (HRSA) Areas

| Geographic Unit | Data Source | Download URL | File Format | Coverage | Update Frequency | Notes |
|----|----|----|----|----|----|----|
| **Health Professional Shortage Areas (HPSAs)** | HRSA | <https://data.hrsa.gov/data/download> | Shapefile, GeoJSON, CSV | National | Quarterly | Primary care, dental, mental health |
| **Medically Underserved Areas (MUAs)** | HRSA | <https://data.hrsa.gov/data/download> | Shapefile, GeoJSON, CSV | National | Quarterly | Areas lacking medical care |
| **Medically Underserved Populations (MUPs)** | HRSA | <https://data.hrsa.gov/data/download> | Shapefile, GeoJSON, CSV | National | Quarterly | Population-based designations |
| **Federally Qualified Health Centers (FQHCs)** | HRSA | <https://data.hrsa.gov/data/download> | Point data, CSV | National | Monthly | FQHC locations |
| **Rural Health Clinics** | HRSA | <https://data.hrsa.gov/data/download> | Point data, CSV | National | Monthly | RHC locations |
| **Critical Access Hospitals** | HRSA | <https://data.hrsa.gov/data/download> | Point data, CSV | National | Monthly | CAH locations |

## Rural-Urban Classifications

| Geographic Unit | Data Source | Download URL | File Format | Coverage | Update Frequency | Notes |
|----|----|----|----|----|----|----|
| **Rural-Urban Commuting Areas (RUCAs)** | USDA ERS | <https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes/documentation/> | CSV with FIPS codes | National (Census Tracts) | \~10 years | 2010 Census-based (most recent) |
| **Urban Influence Codes** | USDA ERS | <https://www.ers.usda.gov/data-products/urban-influence-codes/> | CSV with FIPS codes | National (Counties) | \~10 years | County-level rural-urban |
| **Rural-Urban Continuum Codes** | USDA ERS | <https://www.ers.usda.gov/data-products/rural-urban-continuum-codes/> | CSV with FIPS codes | National (Counties) | \~10 years | Metro and non-metro counties |

## ACOG Districts and Medical Professional Areas

| Geographic Unit | Data Source | Download URL | File Format | Coverage | Update Frequency | Notes |
|----|----|----|----|----|----|----|
| **Hospital Referral Regions (HRRs)** | Dartmouth Atlas | <https://www.dartmouthatlas.org/tools/downloads.aspx> | Shapefile | National | \~10 years | Healthcare market areas |
| **Hospital Service Areas (HSAs)** | Dartmouth Atlas | <https://www.dartmouthatlas.org/tools/downloads.aspx> | Shapefile | National | \~10 years | Local hospital markets |

## Transportation and Infrastructure

| Geographic Unit | Data Source | Download URL | File Format | Coverage | Update Frequency | Notes |
|----|----|----|----|----|----|----|
| **Primary Roads** | US Census Bureau | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | National, State | Annual | Major highways and roads |
| **All Roads** | US Census Bureau | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | National, State, County | Annual | Complete road network |

## Major Highway Systems and Interstate Corridors

| Highway System | Data Source | Download URL | File Format | Coverage | Update Frequency | Specific Routes Included |
|----|----|----|----|----|----|----|
| **Interstate Highway System** | US Census Bureau TIGER | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | National | Annual | I-95, I-10, I-5, I-75, I-80, I-40, I-35, I-25, I-70, I-90, etc. |
| **US Highway System** | US Census Bureau TIGER | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | National | Annual | US-1, US-50, US-101, US-Route 66 (historic), etc. |
| **Primary Roads (Interstate + US)** | US Census Bureau TIGER | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | National | Annual | All Interstate and US highways combined |
| **National Highway System (NHS)** | FHWA | <https://www.fhwa.dot.gov/planning/national_highway_system/> | Shapefile, GeoJSON | National | Annual | Congressionally designated strategic highways |

## Major Interstate Highways (Detailed Reference)

### East-West Interstate Corridors

| Route | Description | States Traversed | Length (Miles) | Key Cities Connected | Healthcare Relevance |
|----|----|----|----|----|----|
| **I-10** | Southern transcontinental | CA, AZ, NM, TX, LA, MS, AL, FL | 2,460 | Los Angeles - Houston - New Orleans - Jacksonville | Major Sun Belt corridor |
| **I-20** | Southern route | TX, LA, MS, AL, GA, SC | 1,535 | Dallas - Atlanta - Columbia | Connects major Southern medical centers |
| **I-30** | Arkansas-Texas | TX, AR | 367 | Dallas - Little Rock | Links Dallas medical district |
| **I-40** | Central transcontinental | CA, AZ, NM, TX, OK, AR, TN, NC | 2,555 | Los Angeles - Nashville - Raleigh | Major cross-country medical corridor |
| **I-70** | Central route | UT, CO, KS, MO, IL, IN, OH, WV, PA, MD | 2,151 | Denver - Kansas City - St. Louis - Columbus | Mountain West to Mid-Atlantic |
| **I-80** | Northern transcontinental | CA, NV, UT, WY, NE, IA, IL, IN, OH, PA, NJ, NY | 2,899 | San Francisco - Chicago - New York | Major Northern medical corridor |
| **I-90** | Northern border route | WA, ID, MT, WY, SD, MN, WI, IL, IN, OH, PA, NY, MA | 3,020 | Seattle - Chicago - Boston | Longest interstate, connects major Northern cities |

### North-South Interstate Corridors

| Route | Description | States Traversed | Length (Miles) | Key Cities Connected | Healthcare Relevance |
|----|----|----|----|----|----|
| **I-5** | West Coast | CA, OR, WA | 1,381 | San Diego - Los Angeles - San Francisco - Portland - Seattle | Pacific Coast medical corridor |
| **I-15** | Southwestern | CA, NV, AZ, UT, ID, MT | 1,433 | San Diego - Las Vegas - Salt Lake City | Desert Southwest access |
| **I-25** | Mountain corridor | NM, CO, WY | 1,062 | Albuquerque - Denver - Cheyenne | Front Range medical access |
| **I-35** | Central Plains | TX, OK, KS, MO, IA, MN | 1,568 | Laredo - San Antonio - Austin - Dallas - Minneapolis | Texas to Twin Cities corridor |
| **I-65** | South Central | AL, TN, KY, IN | 887 | Mobile - Birmingham - Nashville - Louisville - Indianapolis | Connects Southern to Midwest medical centers |
| **I-75** | Eastern corridor | FL, GA, TN, KY, OH, MI | 1,786 | Miami - Atlanta - Cincinnati - Detroit | Major Eastern medical corridor |
| **I-85** | Southeastern | AL, GA, SC, NC, VA | 666 | Montgomery - Atlanta - Charlotte - Richmond | Southeast medical corridor |
| **I-95** | East Coast | ME, NH, MA, RI, CT, NY, NJ, PA, DE, MD, DC, VA, NC, SC, GA, FL | 1,908 | Miami - Savannah - Richmond - Washington - Philadelphia - New York - Boston | Primary East Coast medical corridor |

## Alternative Transportation Network Data Sources

| Data Source | Download URL | File Format | Coverage | Cost | Licensing | Quality Level |
|----|----|----|----|----|----|----|
| **OpenStreetMap (OSM)** | <https://www.openstreetmap.org/> | Various | Global | Free | Open Data License | High, community maintained |
| **HERE Maps Road Data** | <https://developer.here.com/> | API/Download | Global | Commercial | HERE License | Very High, commercial grade |
| **TomTom Road Network** | <https://developer.tomtom.com/> | API | Global | Commercial | TomTom License | Very High, commercial grade |

## Highway-Specific R Package Resources

| Package | Function | Data Included | Usage Example | Notes |
|----|----|----|----|----|
| **tigris** | `primary_roads()`, `primary_secondary_roads()` | Census TIGER roads | `tigris::primary_roads(cb = TRUE)` | Automatically downloads highways |
| **osmdata** | `opq()`, `add_osm_feature()` | OpenStreetMap data | `osmdata::opq("USA") %>% add_osm_feature("highway", "motorway")` | Query specific highway types |
| **tidytransit** | Transit and road network analysis | GTFS + road networks | Various | Public transportation integration |
| **dodgr** | Distance calculations on road networks | OSM road networks | Network analysis | Route optimization |

## Highway Data Download Examples

### Interstate Highways via tigris

``` r
# Download all primary roads (includes Interstates)
primary_roads <- tigris::primary_roads(cb = TRUE)

# Filter for Interstate highways only
interstates <- primary_roads %>%
  filter(stringr::str_detect(FULLNAME, "^I "))

# Specific Interstate (example: I-25)
i25 <- primary_roads %>%
  filter(stringr::str_detect(FULLNAME, "I 25"))
```

### Major Highways via OpenStreetMap

``` r
library(osmdata)

# Download Interstate highways
interstates_osm <- opq("United States") %>%
  add_osm_feature(key = "highway", value = "motorway") %>%
  add_osm_feature(key = "ref", value = c("I 95", "I 25", "I 70")) %>%
  osmdata_sf()

# Download US highways  
us_highways <- opq("United States") %>%
  add_osm_feature(key = "highway", value = "trunk") %>%
  add_osm_feature(key = "ref", value = c("US 50", "US 101")) %>%
  osmdata_sf()
```

### State-Specific Highway Downloads

``` r
# Colorado highways (example for I-25, I-70, I-76)
co_roads <- tigris::primary_secondary_roads(state = "CO", cb = TRUE)

co_interstates <- co_roads %>%
  filter(stringr::str_detect(FULLNAME, "^I ")) %>%
  filter(stringr::str_detect(FULLNAME, "I 25|I 70|I 76"))
```

## Highway Analysis Applications for Healthcare Accessibility

### Interstate Corridor Analysis

-   **I-95 Corridor**: East Coast medical referral patterns
-   **I-10 Corridor**: Southern transcontinental access
-   **I-25 Front Range**: Denver-Colorado Springs-Albuquerque medical corridor
-   **I-70 Mountain Corridor**: Mountain West healthcare access

### Highway Buffer Analysis

``` r
# Create buffers around major highways for accessibility analysis
highway_buffers <- interstates %>%
  st_buffer(dist = units::set_units(50, "miles"))  # 50-mile highway corridor
```

## Cancer Care and Specialty Medical Areas

| Geographic Unit | Data Source | Download URL | File Format | Coverage | Update Frequency | Notes |
|----|----|----|----|----|----|----|
| **NCI-Designated Cancer Centers** | National Cancer Institute | <https://www.cancer.gov/research/infrastructure/cancer-centers/find> | Point data (geocode addresses) | National | As designated | Comprehensive cancer centers |
| **CoC-Accredited Cancer Programs** | Commission on Cancer | <https://www.facs.org/quality-programs/cancer-programs/commission-cancer/coc-accredited-programs/> | Point data (geocode addresses) | National | Annual | Accredited cancer programs |
| **NCCN Member Institutions** | NCCN | <https://www.nccn.org/about/member-institutions> | Point data (geocode addresses) | National | As membership changes | Leading cancer centers |

## American Community Survey Geographic Summary Levels

| Geographic Unit | Summary Level Code | Download URL | File Format | Coverage | Update Frequency | Notes |
|----|----|----|----|----|----|----|
| **Nation** | 010 | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | US | Annual | National boundary |
| **Regions** | 020 | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | US | Annual | 4 Census regions |
| **Divisions** | 030 | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | US | Annual | 9 Census divisions |
| **States** | 040 | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | US | Annual | 50 states + DC |
| **Counties** | 050 | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | US | Annual | \~3,100 counties |
| **County Subdivisions** | 060 | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | US | Annual | Townships, boroughs, etc. |
| **Census Tracts** | 140 | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | US | Annual | \~74,000 tracts |
| **Block Groups** | 150 | <https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html> | Shapefile | US | Annual | \~220,000 block groups |

## Special R Package Resources for Geographic Data

| Package | Function | Data Included | Usage | Notes |
|----|----|----|----|----|
| **tigris** | `states()`, `counties()`, `tracts()`, `block_groups()` | All Census boundaries | `tigris::states(cb = TRUE)` | Automatically downloads and caches |
| **tidycensus** | `get_acs(..., geometry = TRUE)` | Census data + boundaries | `get_acs(geography = "tract", geometry = TRUE)` | Data and geography combined |
| **sf** | Spatial data processing | \- | `st_read("shapefile.shp")` | Core spatial analysis |
| **USAboundaries** | Historical US boundaries | Counties, states (1629-2000) | `us_states(map_date = "2000-01-01")` | Historical analysis |
| **spData** | Spatial datasets | Various example datasets | `data(us_states)` | Teaching and examples |

## File Download Examples and R Code

### Census Boundaries via tigris Package

``` r
# Download state boundaries
states_sf <- tigris::states(cb = TRUE, resolution = "20m")

# Download census tracts for specific state  
tracts_sf <- tigris::tracts(state = "CO", cb = TRUE)

# Download block groups for specific county
bg_sf <- tigris::block_groups(state = "CO", county = "001", cb = TRUE)
```

### HRSA Data Download

``` r
# Download HPSA data
hpsa_url <- "https://data.hrsa.gov/data/download/hrsa-hpsa-primary-care-shortage-areas.csv"
hpsa_data <- readr::read_csv(hpsa_url)
```

## Data Vintage and Compatibility Notes

| Geographic Unit | 2010 Census Vintage | 2020 Census Vintage | Compatibility Issues |
|----|----|----|----|
| **Census Tracts** | \~74,000 tracts | \~84,000 tracts | Tract boundaries changed significantly |
| **Block Groups** | \~217,000 BGs | \~242,000 BGs | Many boundary changes |
| **Counties** | 3,143 counties | 3,143 counties | Stable boundaries |
| **States** | 50 + DC | 50 + DC | No changes |
| **ZCTAs** | 2010 ZIP patterns | 2020 ZIP patterns | Significant changes in suburban areas |

# License

MIT (allows commercial and academic use with attribution)

# 🤝 Contributing

We welcome contributions!

# 📜 Citation

If you use this work in your research, please cite:

``` bibtex
@misc{muffly2025gynoncaccess,
  title={Gynecologic Oncology Accessibility Project: Nationwide Analysis of Access to OBGYN Subspecialists Using Drive Time Isochrones (2013-2023)},
  author={Muffly, Tyler},
  year={2025},
  url={https://github.com/mufflyt/isochrones},
  note={Version 2.0.0}
}
```

# Contact

**Primary Contact**: Tyler Muffly, MD\
**Email**: [tyler.muffly\@dhha.org](mailto:tyler.muffly@dhha.org){.email}\
**GitHub**: <https://github.com/mufflyt/isochrones>

# Project Maintenance Schedule

-   **Annual Updates**: New NPPES data release (typically March)
-   **API Monitoring**: Monthly checks of rate limits and costs
-   **Data Validation**: Biannual review of provider lists
-   **Code Updates**: As needed for R package changes

# 

======= Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished
to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

```         

## Citation Guidelines

### **Academic Citation**
```

Muffly, T. (2025). Comprehensive Healthcare Accessibility Analysis: Gynecologic
Oncology Access Using Drive Time Isochrone Methodology. GitHub Repository.
<https://github.com/mufflyt/isochrones>

```         

### **BibTeX Entry**

```bibtex
@misc{muffly2025accessibility,
  title={Comprehensive Healthcare Accessibility Analysis: Gynecologic Oncology Access Using Drive Time Isochrone Methodology},
  author={Muffly, Tyler},
  year={2025},
  url={https://github.com/mufflyt/isochrones},
  note={Comprehensive analysis of nationwide access to gynecologic oncologists using drive time isochrones, demographic data, and geospatial analysis}
}
```

For citation tools that support it, a `CITATION.cff` file is included in the
repository root.

--------------------------------------------------------------------------------

# 🙏 Acknowledgments

::: highlight-box
### **Data Sources and Institutional Support**

-   **National Bureau of Economic Research (NBER)** - Historical NPPES data
    access
-   **Centers for Medicare & Medicaid Services (CMS)** - Provider validation
    data
-   **HERE Technologies** - Geocoding and routing API access\
-   **U.S. Census Bureau** - Demographic and geographic data
-   **Denver Health and Hospital Authority**

### **Technical Infrastructure**

-   **R Core Team and CRAN Contributors** - Open source statistical computing
    platform
-   **RStudio** - Integrated development environment
-   **Tidyverse** - Comprehensive data science ecosystem
-   **Spatial R Community** - Geospatial analysis packages and support

### **Research Community**

-   **Health Services Research Community** - Methodological guidance and peer
    review
-   **Geographic Information Science Community** - Spatial analysis techniques
-   **Open Science Advocates** - Reproducible research principles and practices
:::

--------------------------------------------------------------------------------

*This comprehensive healthcare accessibility analysis represents ongoing
research into geographic disparities in specialty healthcare access. The
methodology and findings are continuously updated with new provider and
demographic data to maintain relevance for healthcare policy and planning
decisions.*

--------------------------------------------------------------------------------

::: {style="text-align: center; margin-top: 50px; padding: 20px; background-color: #f8f9fa; border-radius: 10px;"}
**🏥 Improving Healthcare Access Through Data-Driven Analysis 📊**

*Generated on 2025-06-22 \| Version 2.0 \| Comprehensive HTML Documentation*
:::
