library(tidyverse)

# Define the original dataset using tribble for clear, row-wise data definition.
# Includes various attributes such as NPI number, update year, personal and professional details.
readr::read_csv("end_sp_duckdb_npi_all.csv")

# Generate a data frame containing all years from 2005 to 2020 for time-series analysis.
years <- tibble(year = 2005:2020)

# Expand the dataset to include an entry for each year from 2005 to 2020 for each unique NPI number.
# This involves cross-joining the distinct NPI numbers and last update years with the years data frame.
expanded_df <- toy_df %>%
  distinct(npi, lastupdatestr) %>%
  cross_join(years) %>%
  group_by(npi) %>%
  # Assign the maximum lastupdatestr for each npi up to the current year.
  mutate(lastupdatestr = max(lastupdatestr[lastupdatestr <= year], na.rm = TRUE)) %>%
  ungroup()

# Merge the expanded data frame with the original dataset based on NPI and lastupdatestr.
# This provides complete records for each year, incorporating all available details.
expanded_df <- expanded_df %>%
  left_join(toy_df, by = c("npi", "lastupdatestr")) %>%
  arrange(npi, year)

# Further process the data to apply conditional logic and fill down methods.
dataframe_final <- expanded_df %>%
  group_by(npi) %>%
  # Identify rows that signify an update or a change in data for an NPI.
  mutate(
    is_update = if_else(is.na(lag(lastupdatestr)) | lastupdatestr != lag(lastupdatestr), TRUE, FALSE),
    # Fill NA values for character and numeric columns to ensure no data gaps.
    across(where(is.character), ~replace_na(., "")),
    across(where(is.numeric), ~replace_na(., 0))
  ) %>%
  # Propagate the last known values downward to fill gaps in data.
  fill(everything(), .direction = "down") %>%
  # Only retain rows marked as updates to reduce data redundancy.
  filter(is_update) %>%
  select(-is_update) %>%
  ungroup() %>%
  arrange(npi, year)

# Output the final processed dataframe to review the transformation results.
print(dataframe_final)
