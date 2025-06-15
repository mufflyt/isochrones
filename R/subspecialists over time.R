#######################
source("R/01-setup.R")
#######################

library(tidyverse)
library(lubridate)

#TODO: I need to run 2023 data scraper on this one works.R

#2012-2013 Urogyn gets boarded


####Brought in from exploratory.io for ~Recent_Grads_GOBA_NPI_2022 dataframe.
# Load the data
data <- read_csv("data/Subspecialists_over_time/2_Recent_Grads_GOBA_NPI_2022.csv") # Replace with your file path

# Convert 'startDate' to year format and find the maximum 'Year' for each 'userid'
data <- data %>%
  mutate(startYear = year(ymd(startDate)),
         userid = as.factor(userid)) %>%
  group_by(userid) %>%
  mutate(endYear = max(Year)) %>%
  ungroup() %>%
  filter(!is.na(startYear) & !is.na(endYear)) %>%
  mutate(as.character(Year))

# Create a list of all years each physician practiced
all_years <- map2(data$startYear, data$endYear, seq)

# Unlist and create a data frame
years_df <- data.frame(Year = unlist(all_years))

# Filter years between 2012 and 2022
filtered_years_df <- filter(years_df, Year > 2011 & Year < 2023)

# Count the occurrences of each year
year_counts <- filtered_years_df %>%
  count(Year) %>%
  arrange(Year)

year_counts$Year <- as.factor(year_counts$Year)

# Plot
# Plot with each bar labeled with the year and color changing by Year
ggplot(year_counts, aes(x = Year, y = n, fill = as.factor(Year))) +
  geom_col() +
  geom_text(aes(label = Year), vjust = -0.5) +
  scale_fill_viridis_d() + # You can change this to any color scale you prefer
  labs(title = "Number of Practicing Physicians per Year (2012-2022)",
       x = "Year",
       y = "Number of Physicians") +
  theme_minimal() +
  theme(legend.position = "none") # Remove legend if not needed

