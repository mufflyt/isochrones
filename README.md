### 01-setup
[Setup file](https://youtu.be/4eStfNf9qjk)

### 02-search_taxonomy
[Setup file](https://youtu.be/4eStfNf9qjk)

### 02.5-subspecialists_over_time
[subspecialists over time](https://youtu.be/-WfEeK8gpH8)

### 03-search_and_process_npi
[search_and_process_npi](https://youtu.be/AZVw-7y01BE)

### 04-geocode
[04-geocode](https://youtu.be/S2vXh8-fzKs)

### 05-geocode-cleaning
[05-geocode-cleaning](https://youtu.be/CwUaD5tRWo4)

### 06-isochrones
[isochrones](https://youtu.be/crZuELWtrTA)

### 07-isochrone-mapping
[07-isochrone-mapping](https://youtu.be/Hs2Nab7Imoc)

### 07.5-prep-get-block-group-overlap
[07.5-prep-get-block-group-overlap]()

### 08-get-block-group-overlap

### 08.5-prep-the census-variables

### 09-get-census-population

### 10-calculate-polygon-demographcs

# Geography

#TODO: 
1) 01-setup: Setup will need to be updated with any changes you make to the bespoke functions.  Teh get_census_data function is not working.  
2) 02.5-subspecialists_over_time: Year-specific physicians should be contained in "data/02.5-subspecialists_over_time/distinct_year_by_year_nppes_data_validated_npi.csv".  This file was run on a different machine with a Postico database so the file does not need to be run.  
3) 03-search_and_process_npi.R should not need to be run.  
4) 04-geocode: We need to geocode all the physicians from "distinct_year_by_year_nppes_data_validated_npi.csv."  I would like to avoid doing too much geocoding as using the hereR package can get expensive, so I would prefer we only code distinct addresses.
5) 05-geocode-cleaning: This file is a very hacky workaround to parse then match the address parts, and now that we have a unique identifier going in and coming out of the 06-isochrones.R file.  
5) 05-geocode-cleaning: Running this file may not be necessary.
6) 06-isochrones: Please fix the process_and_save_isochrones function to give it multiple dates (`iso_datetime_yearly`).  
7) 07-isochrone-mapping: To make year-specific physicians, we must figure out the "subspecialists_lat_long" variable.  
7) 07.5-prep-get-block-group-overlap: This contains the year-specific block groups and should work well as it is.  
8) 08-get-block-group-overlap: We need to look at the entire USA.  Currently, the code reads "Colorado" as a smaller state for the toy example.
8) 08.5-prep-the census-variables: Please download all races ("BLACK", "American Indian/Alaska Native (AI/AN)", "ASIAN", "NHPI" = "Native Hawaiian/Pacific Islander", "WHITE") of the total female population per block group for all years.  This is going to need some work.  We will need to find the American Community Survey (ACS) variables associated with the races.  

I am interested in looking at the availability of access to over 5,000 OBGYN subspecialists over time (2013 to 2022) in the United States.  The subspecialists include: Female Pelvic Medicine and Reconstructive Surgery/Urogynecology, Gynecologic Oncology, Maternal-Fetal Medicine, and Reproductive Endocrinology and Infertility.  The topic is physician workforce planning, where there is the greatest need for more physicians to serve women 18 years and older.  Patients will be driving by car to create the isochrones.  I do not have access to ArcGIS due to cost and so I would like to create everything using R.  Ideally we would use the “sf” package as “sp” is being deprecated.  I'm hiring because I have too much work to do alone. I would like to start work next week.  

What I am providing:
1) A github repository with the scripts and the data needed to create the code: www.github.com/mufflyt.  Most of this project is based off of two projects by Hannah Recht.  www.github.com/hrecht

Deliverables:
1) Source code in R
2) Scripts that create year-specific isochrones and year-specific physicians and year specific block groups
3) Gather Census data for each of the years for all the block groups.  
4) Create a static map of each year (2013 to 2022) plotting the year-specific physicians and year-specific isochrones
a. Isochrones of different colors for 15 minutes, 30 minutes, 60 minutes, 180 minutes, and 240 minutes
b. Physicians as dots with different colors based on specialty (sub1)
c. ACOG Districts outlined with no fill
d. A compass rose and a scale bar in miles
e. Legend of isochrones ordered from 15 minutes on top to 240 minutes on bottom
f. Title at the top center of the map
g. Saved as a TIFF file
5) Leaflet map with the same features as above please
6) Counts of the number of women (Total, White, Black , Asian, American Indian/Alaska Native, Native Hawaiian/Pacific Islander) who are within each of the 15 minutes, 30 minutes, 60 minutes, 180 minutes, and 240 minutes from each subspecialty(Female Pelvic Medicine and Reconstructive Surgery/Urogynecology, Gynecologic Oncology, Maternal-Fetal Medicine, and Reproductive Endocrinology and Infertility.)

Sample Abstract

Geographic Disparities in Potential Accessibility to Gynecologic Oncologists in the United States from 2013 to 2022.  

Objective: To use a spatial modeling approach to capture potential disparities of gynecologic oncologist accessibility in the United States at the block group level between 2013 and 2022.

Methods: Physician registries identified the 2013 to 2022 gynecologic oncology workforce and were aggregated to each county.  The at-risk cohort (women aged 18 years or older) was stratified by race and rurality demographics.  We computed the distance from at-risk women to physicians.  We set drive time to 30, 60, 180, and 240 minutes.  

Results: Between 2013 and 2022, the gynecologic oncology workforce increased.  By 2022, there were x active physicians, and x% practiced in urban block groups.  Geographic disparities were identified, with x physicians per 100,000 women in urban areas compared with 0.1 physicians per 100,000 women in rural areas.  In total, x block groups (x million at-risk women) lacked a gynecologic oncologist.  Additionally, there was no increase in rural physicians, with only x% practicing in rural areas in 2013-2015 relative to ??% in 2016-2022 (p=?).  Women in racial minority populations exhibited the lowest level of access to physicians across all periods.  For example, xx% of American Indian or Alaska Native women did not have access to a physician with a 3-hour drive from 2013-2015, which did not improve over time.  Black women experience an increase in relative accessibility, with a ??% increase by 2016-2022.  However, Asian or Pacific Islander women exhibited significantly better access than ???  women across all periods.  

Conclusion:  Although the US gynecologic oncologist workforce has increased steadily over 20 years, this has not translated into evidence of improved access for many women from rural and underrepresented areas.  However, distance and availability may not influence healthcare utilization and cancer outcomes only.  



Methods:
There are subspecialists in obstetrics and gynecology across the United States in four categories: Gynecologic Oncology, Female Pelvic Medicine and Reconstructive Surgery, Maternal-Fetal Medicine, and Reproductive Endocrinology and Infertility.  We gathered data from the National Plan and Provider Enumeration System (NPPES) to extract each obstetrician-gynecologist and their address of practice (available at https://www.nber.org/research/data/national-plan-and-provider-enumeration-system-nppes).  The NPPES file contains a list of every physician practicing in the United States and is closest to a physician data clearinghouse.  This data also includes their graduation and license year.  Therefore, we identified the approximate year that each subspecialist entered the workforce as a physician.  We had data for NPPES files from 2013 to 2023.  

Utilization data for each physician were unavailable, so our analysis focused on at-risk women aged 18 years or older in the United States at the block group level (Appendix 0).  Analysis was also done at the American College of Obstetricians and Gynecologists District level (Appendix 1).  We collected American Community Survey 5-year estimates and decennial census data.  Along with the total adult female population in each cross-section, we also collected population on race to identify potential disparities among women in minority populations (Appendix 2).  The race categories included White, Black, Asian or Pacific Islander, and American Indian or Alaska Native.  

To measure the distance between the patient location and the point of care (hospital or emergency department), the shortest or "straight-line" distance (i.e., the geodetic or great circle distance) is commonly used because it can be readily calculated (e.g., through statistical software programs such as SAS®).  An alternative distance metric is the driving distance or driving times obtained from various mapping software such as HERE, Google Maps, MapQuest, OpenStreetMaps, and ArcGIS Network Analyst.  Multiple organizations, such as the Veteran’s Administration and the Department of Transportation, prefer drive time for measuring access.  We use the HERE API to calculate optimal routes and directions for driving with traffic on the third Friday in October at 0900 (Appendix 3). 

Additionally, our analysis examined potential access to obstetrician-gynecologist subspecialists across the United States.  We used drive time isochrones.  

Appendix 0:
Choosing block groups over counties for data analysis could be driven by several factors, depending on the nature of the analysis you are conducting:

•	Resolution of Data: Block groups provide a finer data resolution than counties. 
•	Local Trends and Patterns: Block groups are small enough to reveal local trends and patterns that might be obscured when data is aggregated at the county level.
•	Socioeconomic Analysis: Block groups can provide more precise information about demographic and economic conditions for studies involving socioeconomic factors since they represent smaller communities.
•	Policy Impact Assessment: Block groups may be more appropriate when assessing the impact of local policies or interventions because policies might vary significantly within a county.
•	Spatial Analysis: For spatial analyses that require precision, such as identifying hotspots or conducting proximity analysis, the smaller geographic units of block groups are more valuable.

We utilized the tigris package to get the block group geometries for different years.  This required downloading each state’s data and joining them into one national file for years 2018 and earlier.  

Appendix 1: The American College of Obstetricians and Gynecologists (ACOG) is a professional organization representing obstetricians and gynecologists in the United States.  ACOG divides its membership into various geographical regions known as "ACOG Districts."

```r
State	        ACOG_District 	State_Abbreviations
Alabama	District VII	AL
Alaska		District VIII	AK
Arizona	District VIII	AZ
Arkansas	District VII	AR
California	District IX	CA
Colorado	District VIII	CO
Connecticut	District I	CT
District of Columbia	District IV	DC
Florida		District XII	FL
Georgia	District IV	GA
Hawaii		District VIII	HI
Illinois		District VI	IL
Indiana	District V	IN
Iowa		District VI	IA
Kansas		District VII	KS
Kentucky	District V	KY
Louisiana	District VII	LA
Maryland	District IV	MD
Massachusetts	District I	MA
Michigan	District V	MI
Minnesota	District VI	MN
Mississippi	District VII	MS
Missouri	District VII	MO
Nebraska	District VI	NE
Nevada	District VIII	NV
New Hampshire District I	NH
New Jersey	District III	NJ
New Mexico	District VIII	NM
New York	District II	NY
North Carolina	District IV	NC
North Dakota	District VI	ND
Ohio		District V	OH
Oklahoma	District VII	OK
Oregon	District VIII	OR
Pennsylvania	District III	PA
Puerto Rico	District IV	PR
Rhode Island	District I	RI
South Carolina	District IV	SC
South Dakota	District VI	SD
Tennessee	District VII	TN
Texas		District XI	TX
Utah		District VIII	UT
Vermont	District I	VT
Virginia	District IV	VA
Washington	District VIII	WA
West Virginia	District IV	WV
Wisconsin	District VI	WI
```

Appendix 2: US Census Bureau Codes Decennial Census and the American Community Survey.
Decennial Census – Demographic and Housing Characteristics File (API variables: https://api.census.gov/data/2020/dec/dhc/variables.html)

(https://www2.census.gov/programs-surveys/decennial/2020/technical-documentation/complete-tech-docs/demographic-and-housing-characteristics-file-and-demographic-profile/2020census-demographic-and-housing-characteristics-file-and-demographic-profile-techdoc.pdf )


American Community Survey 
(API variables: https://api.census.gov/data/2022/acs/acs1/variables.html )
```r
B01001_026E  Estimate _Total _Female
    female_10_to_14 = "B01001_029",
    female_15_to_17 = "B01001_030",
    female_18_to_19 = "B01001_031",
    female_20years = "B01001_032",
    female_21years = "B01001_033",
    female_22_to_24 = "B01001_034",
    female_25_to_29 = "B01001_035",
    female_30_to_34 = "B01001_036",
    female_35_to_39 = "B01001_037",
    female_40_to_44 = "B01001_038",
    female_45_to_49 = "B01001_039",
    female_50_to_54 = "B01001_040",
    female_55_to_59 = "B01001_041",
    female_60_to_61 = "B01001_042",
    female_62_to_64 = "B01001_043",
    female_65_to_66 = "B01001_044",
    female_67_to_69 = "B01001_045",
    female_70_to_74 = "B01001_046",
    female_75_to_79 = "B01001_047",
    female_80_to_84 = "B01001_048",
    female_85_over = "B01001_049"
````

## US Decennial Census Demographic and Housing
```r
    name	label	concept
P12Y_026N	Female:	AI/AN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, NOT HISPANIC OR LATINO)
P12Z_026N	Female:	ASIAN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, NOT HISPANIC OR LATINO)
P12X_026N	Female:	BLACK OR AFRICAN AMERICAN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, NOT HISPANIC OR LATINO)
P12AA_026N	Female:	NHPI ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, NOT HISPANIC OR LATINO)
P12W_026N	Female:	WHITE ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES, NOT HISPANIC OR LATINO)
```

Appendix 3: Characteristics of Isochrones
The bespoke R code generates individual maps for each drive time, visually representing the accessible areas on a map.  The function shapefiles are geospatial data files used for storing geographic information, including the boundaries of the reachable areas.  The HERE API (here.com) was utilized because traffic and time could be standardized yearly.  Each year, the isochrones are built on the third Friday in October at 0900, defined as “posix_time”.  We imagined that patients would see their primary care provider at this time of year for an influenza vaccination or other issues.  The hereR package (https://github.com/munterfi/hereR/ ) is a wrapper around the R code that calls the HERE REST API for isoline routing (platform.here.com) and returns it as an sf object.  There is a cost of $5.50 for every 1,000 isolines created (https://www.here.com/get-started/pricing#here---platform---pricing---page-title ).  

```r
•	October 18, 2013
•	October 17, 2014
•	October 16, 2015
•	October 21, 2016
•	October 20, 2017
•	October 19, 2018
•	October 18, 2019
•	October 16, 2020
•	October 15, 2021
•	October 21, 2022

c("2013-10-18 09:00:00", "2014-10-17 09:00:00", "2015-10-16 09:00:00",
  "2016-10-21 09:00:00", "2017-10-20 09:00:00", "2018-10-19 09:00:00",
  "2019-10-18 09:00:00", "2020-10-16 09:00:00", "2021-10-15 09:00:00",
  "2022-10-21 09:00:00")
```

R code utilizing the hereR package with the isoline library.  The range of isochrones was 30 minutes, 60 minutes, 120 minutes, and 180 minutes.  
```r
isochrones_sf <- process_and_save_isochrones(input_file_no_error_rows, 
                                             chunk_size = 25, 
                                             iso_datetime = "2023-10-20 09:00:00",
                                             iso_ranges = c(30*60, 60*60, 120*60, 180*60),
                                             crs = 4326, 
                                             transport_mode = "car",
                                             file_path_prefix = "data/06-isochrones/isochrones_")
```

![image](https://github.com/mufflyt/Geography/assets/44621942/d246f85e-4b77-463e-9cb9-69c0e6623e2e)


####
# Tools

We used github LFS (large file storage).  2GB is the largest that LFS can handle.  Do NOT exceed. Upload ONE directory at a time after checking the size.  

```r
git lfs install
git config http.postBuffer 524288000
git lfs track "*.dbf" "*.shp"
git ls-tree -r -t -l HEAD | sort -k 4 -n -r | head -n 10
git add .gitattributes
git commit -m "Track large files with Git LFS"
git push origin main
```

```r
git lfs track "bg_shp_joined_intersect_pct.rds"
git add .gitattributes
git commit -m "Track bg_shp_joined_intersect_pct.rds with Git LFS"
git push origin main
```

```r
git add .gitattributes
git add *.shp
git add *.dbf
git add *.shx
git add *.prj
git commit -m "Reconfigure Git LFS for large files"
git push origin main
```

# Postico Database
We needed a database program to house each of the NPI files from each year due to RAM restrictions.  Postico is a client for PostgreSQL, and it allows you to execute raw SQL queries, including DDL statements._full_
<img width="1440" alt="Screenshot 2024-01-13 at 8 25 35 PM" src="https://github.com/mufflyt/isochrones/assets/44621942/86e54895-3358-4ff7-96d6-0e3e9b120a46">

# tyler package
[tyler_1.1.0.tar.gz](https://github.com/mufflyt/isochrones/files/13914538/tyler_1.1.0.tar.gz)

