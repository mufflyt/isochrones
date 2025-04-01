library(tidyverse)
conflicted::conflicts_prefer(dplyr::lag)

# Define the original data
toy_df <- tribble(
  ~npi, ~lastupdatestr, ~penumdatestr, ~pfname, ~plname, ~address, ~plocline1, ~ploccityname, ~plocstatename, ~ploczip, ~ploctel, ~pmailline1, ~pmailcityname, ~pmailstatename, ~pmailzip, ~pgender, ~pcredential, ~ptaxcode1, ~ptaxcode2, ~soleprop,
  1528060639, 2005, 2005, "TY", "CURTIN", "160 E 34TH ST, NEW YORK, NY, 10016", "160 E 34TH ST", "NEW YORK", "NY", 10016, 2127315180, "160 E 34TH ST", "NEW YORK", "NY", 10016, "Male", "MD", "207VX0201X", NA, NA,
  1528060639, 2007, 2005, "MATT", "CURTIN", "160 E 34TH ST, NEW YORK, NY, 10016", "160 E 34TH ST", "NEW YORK", "NY", 10016, 2127315180, "160 E 34TH ST", "NEW YORK", "NY", 10016, "Male", "MD", "207VX0201X", "X", "X",
  1528060639, 2015, 2005, "STACY", "CURTIN", "160 E 34TH ST, NEW YORK, NY, 10016", "160 E 34TH ST", "NEW YORK", "NY", 10016, 2127315180, "160 E 34TH ST", "NEW YORK", "NY", 10016, "Male", "MD", "207VX0201X", NA, "N"
)

# Create a data frame with all years from 2005 to 2020
years <- tibble(year = 2005:2020)

# Expand the toy_df to cover all years
expanded_df <- crossing(years, toy_df %>% select(npi, lastupdatestr) %>% distinct()) %>%
  group_by(npi) %>%
  mutate(
    lastupdatestr = max(lastupdatestr[lastupdatestr <= year], na.rm = TRUE)
  ) %>%
  ungroup() %>%
  left_join(toy_df, by = c("npi", "lastupdatestr"))

# Apply conditions for lastupdatestr and fill data accordingly
dataframe_final <- expanded_df %>%
  group_by(npi) %>%
  mutate(
    lastupdatestr = if_else(year >= lastupdatestr, lastupdatestr, NA_real_),
    is_update = if_else(is.na(lag(lastupdatestr)) | lastupdatestr != lag(lastupdatestr), TRUE, FALSE)
  ) %>%
  fill(everything(), .direction = "down") %>%
  filter(is_update) %>%
  select(-is_update) %>%
  ungroup() %>%
  arrange(npi, year)

# Print the final dataframe
print(dataframe_final)
