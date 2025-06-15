#This code is part of a data preprocessing and cleaning workflow for geocoded clinician data. It starts by reading in a CSV file named "end_completed_clinician_data_geocoded_addresses_12_8_2023.csv" into the `geocoded_data` data frame. Then, it performs several data transformation steps on this data, including cleaning and formatting the address information. The cleaned data is written to a CSV file named "geocoded_data_to_match_house_number.csv." In the second part of the code, it reads another CSV file named "for_street_matching_with_HERE_results_clinician_data.csv" into the `clinician_data_for_matching` data frame and applies a series of data parsing and cleaning operations using the "postmastr" package to extract and standardize various address components. The result is stored in the `clinician_data_postmastr_parsed` data frame and written to a CSV file named "end_postmastr_clinician_data.csv." The code then performs an inner join between the postmastr processed clinician data and the geocoded data based on specific columns, creating an `inner_join_postmastr_clinician_data` data frame. Finally, it generates ACOG (American College of Obstetricians and Gynecologists) districts using the `tyler` package and stores the result in the `acog_districts_sf` object.

#TODO: Please assess if we need this hacky way to match the geocoded results by a parsed address if we can pass a unique_id variable to create_geocode in order to match the original dataframe with the geocoded output.  

#######################
source("R/01-setup.R")
#######################

#**************************
#* STEP A TO CLINICIAN_DATA TO GEOCODED_DATA 
#**************************
# 
# Get data formatted the same so we can match it more simply
geocoded_data <- read_csv("data/04-geocode/end_completed_clinician_data_geocoded_addresses_12_8_2023.csv")

geocoded_data_to_match_house_number <- geocoded_data %>%
  mutate(address_cleaned = exploratory::str_remove(address, regex(", United States$", ignore_case = TRUE), remove_extra_space = TRUE), .after = ifelse("address" %in% names(.), "address", last_col())) %>%
  mutate(address_cleaned = exploratory::str_remove_after(address_cleaned, sep = "\\-")) %>%
  mutate(address_cleaned = str_replace(address_cleaned, "([A-Z]{2}) ([0-9]{5})", "\\1, \\2")) %>%
  readr::write_csv(., "data/05-geocode-cleaning/geocoded_data_to_match_house_number.csv")


#**************************
#* STEP B TO MATCH CLINICIAN_DATA TO GEOCODED_DATA
#**************************
#*  https://slu-opengis.github.io/postmastr/index.html
#postmastr’s functionality rests on an order of operations that must be followed to ensure correct parsing:
# * prep
# * postal code
# * state
# * city
# * unit
# * house number
# * ranged house number
# * fractional house number
# * house suffix
# * street directionals
# * street suffix
# * street name
# * reconstruct

# Prep
clinician_data_for_matching <- readr::read_csv("data/04-geocode/for_street_matching_with_HERE_results_clinician_data.csv")
clinician_data_postmastr <- clinician_data_for_matching %>% postmastr::pm_identify(var = "address")
clinician_data_postmastr_min <- postmastr::pm_prep(clinician_data_postmastr, var = "address", type = "street") %>%
  mutate(pm.address = stringr::str_squish(pm.address))

# removes the second line address with the word "suite" and the suite number.  postmastr needs a little help at the start
# clinician_data_postmastr_min <- clinician_data_postmastr_min %>%
#   mutate(postmastr::pm.address = stringr::str_replace_all(pm.address, "\\bSUITE \\d+\\b", ""))

# Postal code parsing
# Postal code: Once we have our data prepared, we can begin working our way down the order of operations list.
clinician_data_postmastr_min <- postmastr::pm_postal_parse(clinician_data_postmastr_min)

# State parsing
# Create state directory
stateDict <- postmastr::pm_dictionary(locale = "us", type = "state", case = c("title", "upper")); stateDict
postmastr::pm_state_all(clinician_data_postmastr_min, dictionary = stateDict) #Checks to make sure that all states have matches in the dataset with the created dictionary
clinician_data_postmastr_min <- postmastr::pm_state_parse(clinician_data_postmastr_min, dictionary = stateDict)

# City parsing
# City dictionary
# city <- pm_dictionary(type = "city", locale = "us", filter = c("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"), case = c("title", "upper", "lower")) %>%
# write_rds("data/city.rds")

# This is provided in the repository in the ""data/05-geocode-cleaning" path.  
#This takes a while so a hard copy is stored in data/05-geocode-cleaning
city <- readr::read_rds("data/05-geocode-cleaning/city.rds")

postmastr::pm_city_all(clinician_data_postmastr_min, dictionary = city) #Checks to make sure that all cities have matches
clinician_data_postmastr_min <- postmastr::pm_city_parse(clinician_data_postmastr_min, dictionary = city, locale = "us") # may take some time

# House number parse
postmastr::pm_house_all(clinician_data_postmastr_min)
clinician_data_postmastr_min <- postmastr::pm_house_parse(clinician_data_postmastr_min, locale = "us")
clinician_data_postmastr_min <- postmastr::pm_streetSuf_parse(clinician_data_postmastr_min, locale = "us")
clinician_data_postmastr_min <- postmastr::pm_streetDir_parse(clinician_data_postmastr_min, locale = "us") 

# Street Parse
clinician_data_postmastr_min <- postmastr::pm_street_parse(clinician_data_postmastr_min, ordinal = TRUE, drop = TRUE, locale = "us")

ACOG_Districts <- tyler::ACOG_Districts #Also found on READ.ME as an appendix.  

# Writes the postmastr data back to the original dataframe
clinician_data_postmastr_parsed <- postmastr::pm_replace(street = clinician_data_postmastr_min, 
                                                         source = clinician_data_postmastr) %>%
  exploratory::left_join(`ACOG_Districts`, by = join_by(`pm.state` == `State_Abbreviations`))
#View(clinician_data_postmastr_parsed)

# Reorder factor levels
clinician_data_postmastr_parsed$ACOG_District <- factor(
  clinician_data_postmastr_parsed$ACOG_District,
  levels = c("District I", "District II", "District III", "District IV", "District V",
             "District VI", "District VII", "District VIII", "District IX",
             "District XI", "District XII"))


readr::write_csv(clinician_data_postmastr_parsed, "data/05-geocode-cleaning/end_postmastr_clinician_data.csv")

# Now do a join between the postmastr file `postmastr_clinician_data.csv` and the geocoded results file `geocoded_data_to_match_house_number`


#**********************************************
# INNER JOIN BY HOUSE_NUMBER AND STATE
#**********************************************
# Provenance from 05-geocode-cleaning.R at the top of the script
geocoded_data_to_match_house_number <- readr::read_csv("data/05-geocode-cleaning/geocoded_data_to_match_house_number.csv")

# Provenance
clinician_data_postmastr_parsed <- readr::read_csv("data/05-geocode-cleaning/postmastr_clinician_data.csv")

inner_join_postmastr_clinician_data <- clinician_data_postmastr_parsed %>%
  rename(house_number = pm.house, street = pm.street) %>%
  exploratory::left_join(`geocoded_data_to_match_house_number`, by = c("house_number" = "house_number", "pm.state" = "state_code"), exclude_target_columns = TRUE, target_columns = c("country_code", "district", "state", "country"), ignorecase=TRUE) %>%
  distinct(pm.uid, .keep_all = TRUE) %>%
  filter(!is.na(score)) 

# Define the number of ACOG districts
num_acog_districts <- 11

# Create a custom color palette using viridis.  I like using the viridis palette because it is ok for color blind folks.  
district_colors <- viridis::viridis(num_acog_districts, option = "viridis")

# Generate ACOG districts with geometry borders in sf using tyler::generate_acog_districts_sf()
acog_districts_sf <- tyler::generate_acog_districts_sf()


#**********************************************
# SANITY CHECK
#**********************************************

leaflet::leaflet(data = inner_join_postmastr_clinician_data) %>%
  leaflet::addCircleMarkers(
    data = inner_join_postmastr_clinician_data,
    lng = ~long,
    lat = ~lat,
    radius = 3,         # Adjust the radius as needed
    stroke = TRUE,      # Add a stroke (outline)
    weight = 1,         # Adjust the outline weight as needed
    color = district_colors[as.numeric(inner_join_postmastr_clinician_data$ACOG_District)],   # Set the outline color to black
    fillOpacity = 0.8#,  # Fill opacity
    #popup = as.formula(paste0("~", popup_var))  # Popup text based on popup_var argument
  ) %>%
  # Add ACOG district boundaries
  leaflet::addPolygons(
    data = acog_districts_sf,
    color = district_colors[as.numeric(inner_join_postmastr_clinician_data$ACOG_District)],      # Boundary color
    weight = 2,         # Boundary weight
    fill = TRUE,       # No fill
    opacity = 0.1,      # Boundary opacity
    popup = ~acog_districts_sf$ACOG_District   # Popup text
  ) %>%
  #Add a legend
  leaflet::addLegend(
    position = "bottomright",   # Position of the legend on the map
    colors = district_colors,   # Colors for the legend
    labels = levels(inner_join_postmastr_clinician_data$ACOG_District),   # Labels for legend items
    title = "ACOG Districts"   # Title for the legend
  )
